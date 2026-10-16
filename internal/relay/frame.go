package relay

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	opContinuation = 0
	opText         = 1
	opBinary       = 2
	opClose        = 8
	opPing         = 9
	opPong         = 10

	maxControlPayload = 125
	frameReadBuffer   = 4096
	frameWriteBuffer  = 16 << 10
	frameReadChunk    = 16 << 10
	controlWriteWait  = time.Second
)

var (
	writeBuffers    = sync.Pool{New: func() any { b := make([]byte, 0, frameWriteBuffer); return &b }}
	readChunks      = sync.Pool{New: func() any { b := make([]byte, frameReadChunk); return &b }}
	errWriteTimeout = errors.New("websocket: write lock timeout")
	errProtocol     = errors.New("websocket: protocol error")
)

type frameHeader struct {
	fin    bool
	op     byte
	length int64
	mask   [4]byte
}

func (h frameHeader) control() bool {
	return h.op >= opClose
}

type dataConn struct {
	nc        net.Conn
	br        *bufio.Reader
	readLimit int64
	lock      chan struct{}
	wbuf      *[]byte
	writeErr  error
	closeSent bool
}

func newDataConn(nc net.Conn, readLimit int64) *dataConn {
	return &dataConn{nc: nc, br: bufio.NewReaderSize(nc, frameReadBuffer), readLimit: readLimit, lock: make(chan struct{}, 1)}
}

func (c *dataConn) acquire(deadline time.Time) bool {
	select {
	case c.lock <- struct{}{}:
		return true
	default:
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case c.lock <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

func (c *dataConn) release() {
	<-c.lock
}

func putHeader(b []byte, op byte, fin bool, n int) []byte {
	b[0] = op
	if fin {
		b[0] |= 0x80
	}
	switch {
	case n < 126:
		b[1] = byte(n)
		return b[:2]
	case n < 1<<16:
		b[1] = 126
		binary.BigEndian.PutUint16(b[2:], uint16(n))
		return b[:4]
	default:
		b[1] = 127
		binary.BigEndian.PutUint64(b[2:], uint64(n))
		return b[:10]
	}
}

func (c *dataConn) appendFrame(op byte, fin bool, payload []byte, deadline time.Time) error {
	if c.writeErr != nil {
		return c.writeErr
	}
	if c.closeSent {
		return websocket.ErrCloseSent
	}
	var scratch [10]byte
	header := putHeader(scratch[:], op, fin, len(payload))
	if c.wbuf == nil {
		c.wbuf = writeBuffers.Get().(*[]byte)
	}
	if len(*c.wbuf)+len(header)+len(payload) <= cap(*c.wbuf) {
		*c.wbuf = append(append(*c.wbuf, header...), payload...)
		return nil
	}
	_ = c.nc.SetWriteDeadline(deadline)
	buffers := net.Buffers{*c.wbuf, header, payload}
	_, c.writeErr = buffers.WriteTo(c.nc)
	*c.wbuf = (*c.wbuf)[:0]
	return c.writeErr
}

func (c *dataConn) flushLocked(deadline time.Time) error {
	if c.wbuf == nil {
		return c.writeErr
	}
	b := *c.wbuf
	if len(b) > 0 && c.writeErr == nil {
		_ = c.nc.SetWriteDeadline(deadline)
		_, c.writeErr = c.nc.Write(b)
	}
	*c.wbuf = b[:0]
	writeBuffers.Put(c.wbuf)
	c.wbuf = nil
	return c.writeErr
}

func (c *dataConn) flush(deadline time.Time) error {
	if !c.acquire(deadline) {
		return errWriteTimeout
	}
	defer c.release()
	return c.flushLocked(deadline)
}

func (c *dataConn) writeControl(op byte, payload []byte, deadline time.Time) error {
	if !c.acquire(deadline) {
		return errWriteTimeout
	}
	defer c.release()
	if err := c.appendFrame(op, true, payload, deadline); err != nil {
		return err
	}
	if op == opClose {
		c.closeSent = true
	}
	return c.flushLocked(deadline)
}

func (c *dataConn) sendClose(m closeMsg, deadline time.Time) error {
	return c.writeControl(opClose, websocket.FormatCloseMessage(m.code, m.reason), deadline)
}

func (c *dataConn) protocolError(message string) error {
	_ = c.sendClose(closeMsg{websocket.CloseProtocolError, message}, time.Now().Add(controlWriteWait))
	return errProtocol
}

func (c *dataConn) readHeader() (frameHeader, error) {
	var h frameHeader
	b, err := c.br.Peek(2)
	if err != nil {
		return h, err
	}
	masked := b[1]&0x80 != 0
	size := 2
	if masked {
		size += 4
	}
	switch b[1] & 127 {
	case 126:
		size += 2
	case 127:
		size += 8
	}
	if b, err = c.br.Peek(size); err != nil {
		return h, err
	}
	h.fin = b[0]&0x80 != 0
	h.op = b[0] & 0x0f
	h.length = int64(b[1] & 127)
	p := 2
	switch h.length {
	case 126:
		h.length = int64(binary.BigEndian.Uint16(b[2:]))
		p = 4
	case 127:
		h.length = int64(binary.BigEndian.Uint64(b[2:]) & (1<<63 - 1))
		p = 10
	}
	rsv := b[0] & 0x70
	_, _ = c.br.Discard(size)
	if !masked {
		return h, c.protocolError("incorrect mask flag")
	}
	copy(h.mask[:], b[p:p+4])
	if rsv != 0 {
		return h, c.protocolError("unexpected reserved bits")
	}
	if h.control() && (h.length > maxControlPayload || !h.fin) {
		return h, c.protocolError("invalid control frame")
	}
	return h, nil
}

func maskBytes(mask [4]byte, pos int, b []byte) int {
	words := 0
	if len(b) >= 16 {
		var k [8]byte
		for i := range k {
			k[i] = mask[(pos+i)&3]
		}
		key := binary.LittleEndian.Uint64(k[:])
		words = len(b) &^ 7
		for i := 0; i < words; i += 8 {
			binary.LittleEndian.PutUint64(b[i:], binary.LittleEndian.Uint64(b[i:])^key)
		}
	}
	for i := words; i < len(b); i++ {
		b[i] ^= mask[(pos+i)&3]
	}
	return (pos + len(b)) & 3
}

func (c *dataConn) readControl(h frameHeader) (pong bool, err error) {
	payload := make([]byte, h.length)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return false, err
	}
	maskBytes(h.mask, 0, payload)
	deadline := time.Now().Add(controlWriteWait)
	switch h.op {
	case opPing:
		_ = c.writeControl(opPong, payload, deadline)
		return false, nil
	case opPong:
		return true, nil
	case opClose:
		code, text := websocket.CloseNoStatusReceived, ""
		if len(payload) == 1 {
			return false, c.protocolError("invalid close frame")
		}
		if len(payload) >= 2 {
			code = int(binary.BigEndian.Uint16(payload))
			text = string(payload[2:])
			if !validReceivedCloseCode(code) {
				return false, c.protocolError("bad close code")
			}
			if !utf8.ValidString(text) {
				return false, c.protocolError("invalid utf8 payload in close frame")
			}
		}
		_ = c.writeControl(opClose, websocket.FormatCloseMessage(code, ""), deadline)
		return false, &websocket.CloseError{Code: code, Text: text}
	}
	return false, c.protocolError("unknown opcode")
}

func validReceivedCloseCode(code int) bool {
	switch code {
	case 1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013:
		return true
	}
	return code >= 3000 && code <= 4999
}
