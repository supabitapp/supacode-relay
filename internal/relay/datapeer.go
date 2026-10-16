package relay

import (
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supaterm-relay/internal/diagnostics"
)

type dataPeer struct {
	s         *Server
	conn      *dataConn
	traffic   *atomic.Int64
	cause     atomic.Pointer[closeMsg]
	trace     *diagnostics.Trace
	bytes     atomic.Int64
	messages  atomic.Int64
	heartbeat *time.Timer
	stopped   atomic.Bool
}

type destinationError struct{ err error }

func (e destinationError) Error() string { return e.err.Error() }

func (e destinationError) Unwrap() error { return e.err }

func (s *Server) newDataPeer(conn *dataConn, traffic *atomic.Int64) *dataPeer {
	p := &dataPeer{s: s, conn: conn, traffic: traffic}
	_ = conn.nc.SetReadDeadline(time.Now().Add(s.cfg.pongTimeout()))
	return p
}

func (p *dataPeer) fail(c closeMsg) {
	if p.cause.CompareAndSwap(nil, &c) {
		p.trace.Event("peer.failed", "close_code", c.code, "reason", diagnostics.CloseReason(c.reason))
	}
	_ = p.conn.nc.Close()
}

func (p *dataPeer) ping() {
	if p.stopped.Load() {
		return
	}
	err := p.conn.writeControl(opPing, nil, time.Now().Add(p.s.cfg.DeliveryTimeout))
	if errors.Is(err, websocket.ErrCloseSent) {
		return
	}
	if err != nil {
		p.fail(closeMsg{websocket.CloseGoingAway, "peer unreachable"})
		return
	}
	if !p.stopped.Load() {
		p.heartbeat.Reset(p.s.cfg.Heartbeat)
	}
}

func (p *dataPeer) readData(pr *pair) {
	p.trace.Event("peer.waiting_for_pair")
	defer func() {
		p.trace.Event("peer.ended", "forwarded_bytes", p.bytes.Load(), "forwarded_messages", p.messages.Load())
	}()
	defer p.s.forgetData(p.conn)
	defer func() {
		p.stopped.Store(true)
		p.heartbeat.Stop()
	}()
	p.heartbeat = time.AfterFunc(p.s.cfg.Heartbeat, p.ping)
	select {
	case <-pr.ready:
		p.trace.Event("peer.forwarding.started")
	case <-pr.done:
		p.trace.Event("peer.pair.closed_before_ready")
		return
	}
	p.s.mu.Lock()
	out := pr.hostPeer
	if p == pr.hostPeer {
		out = pr.client
	}
	p.s.mu.Unlock()
	err := p.forward(out)
	var dest destinationError
	if errors.As(err, &dest) {
		p.trace.Failure("peer.forwarding.failed", dest.err, "side", "destination")
		pr.close(closeMsg{websocket.CloseTryAgainLater, "peer write timeout"})
		return
	}
	p.trace.Failure("peer.read.ended", err, "close_code", diagnostics.CloseCode(err))
	pr.close(p.closeCause(err))
}

func (p *dataPeer) forward(out *dataPeer) error {
	src, dst := p.conn, out.conn
	pongTimeout := p.s.cfg.pongTimeout()
	var lastExtend time.Time
	extend := func() {
		if now := time.Now(); now.Sub(lastExtend) > pongTimeout/8 {
			lastExtend = now
			_ = src.nc.SetReadDeadline(now.Add(pongTimeout))
		}
	}
	deadline := func() time.Time { return time.Now().Add(p.s.cfg.DeliveryTimeout) }
	var messageOp byte
	var messageLength int64
	started := false
	emit := func(fin bool, b []byte) error {
		op := byte(opContinuation)
		if !started {
			op = messageOp
		}
		started = true
		if !dst.acquire(deadline()) {
			return destinationError{errWriteTimeout}
		}
		err := dst.appendFrame(op, fin, b, deadline())
		dst.release()
		if err != nil {
			return destinationError{err}
		}
		return nil
	}
	for {
		if src.br.Buffered() == 0 {
			if err := dst.flush(deadline()); err != nil {
				return destinationError{err}
			}
		}
		h, err := src.readHeader()
		if err != nil {
			return err
		}
		if h.control() {
			pong, err := src.readControl(h)
			if err != nil {
				return err
			}
			if pong {
				lastExtend = time.Now()
				_ = src.nc.SetReadDeadline(lastExtend.Add(pongTimeout))
			}
			continue
		}
		switch {
		case h.op == opContinuation && messageOp == 0:
			return src.protocolError("continuation after final message frame")
		case h.op != opContinuation && messageOp != 0:
			return src.protocolError("message start before final message frame")
		case h.op != opContinuation && h.op != opText && h.op != opBinary:
			return src.protocolError("unknown opcode")
		case h.op != opContinuation:
			messageOp, messageLength, started = h.op, 0, false
		}
		messageLength += h.length
		if src.readLimit > 0 && messageLength > src.readLimit {
			_ = src.sendClose(closeMsg{websocket.CloseMessageTooBig, ""}, time.Now().Add(controlWriteWait))
			return websocket.ErrReadLimit
		}
		if h.length == 0 && h.fin {
			if err := emit(true, nil); err != nil {
				return err
			}
		}
		remaining, pos := h.length, 0
		for remaining > 0 {
			var b []byte
			var chunk *[]byte
			if src.br.Buffered() == 0 && remaining >= frameReadBuffer {
				chunk = readChunks.Get().(*[]byte)
				n, err := src.br.Read((*chunk)[:min(remaining, frameReadChunk)])
				if err != nil {
					readChunks.Put(chunk)
					return err
				}
				b = (*chunk)[:n]
			} else {
				if _, err := src.br.Peek(1); err != nil {
					return err
				}
				b, _ = src.br.Peek(int(min(remaining, int64(src.br.Buffered()))))
			}
			pos = maskBytes(h.mask, pos, b)
			remaining -= int64(len(b))
			extend()
			err := emit(h.fin && remaining == 0, b)
			if chunk != nil {
				readChunks.Put(chunk)
			} else {
				_, _ = src.br.Discard(len(b))
			}
			if err != nil {
				return err
			}
		}
		if !h.fin {
			continue
		}
		p.s.forwardedMessages.Add(1)
		p.s.forwardedBytes.Add(messageLength)
		out.traffic.Add(messageLength)
		p.bytes.Add(messageLength)
		if p.messages.Add(1) == 1 {
			p.trace.Event("peer.message.first", "message_type", int(messageOp), "bytes", messageLength)
		}
		messageOp = 0
	}
}

func (p *dataPeer) closeCause(err error) closeMsg {
	if c := p.cause.Load(); c != nil {
		return *c
	}
	var ce *websocket.CloseError
	switch {
	case errors.As(err, &ce) && ce.Code == websocket.CloseNoStatusReceived:
		return closeMsg{websocket.CloseNormalClosure, ""}
	case errors.As(err, &ce) && sendableCode(ce.Code):
		return closeMsg{ce.Code, ce.Text}
	case errors.Is(err, websocket.ErrReadLimit):
		return closeMsg{websocket.CloseMessageTooBig, "message too big"}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return closeMsg{websocket.CloseGoingAway, "peer timeout"}
	}
	return closeMsg{websocket.CloseGoingAway, "peer disconnected"}
}
