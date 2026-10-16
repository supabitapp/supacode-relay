package e2e

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSingleFrameStreamsBeforePayloadCompletes(t *testing.T) {
	const size = 32 << 20
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=33554432", "RELAY_HEARTBEAT_MS=1000")
	client, host, _ := pairUp(t, r, register(t, r))
	header := binary.BigEndian.AppendUint64([]byte{0x82, 0xff}, size)
	header = append(header, 0, 0, 0, 0)
	block := bytes.Repeat([]byte{0xa5}, 64<<10)
	_ = client.SetWriteDeadline(time.Now().Add(deadline))
	if _, err := client.UnderlyingConn().Write(append(header, block...)); err != nil {
		t.Fatal(err)
	}
	pings := make(chan struct{}, 1)
	host.SetPingHandler(func(string) error {
		select {
		case pings <- struct{}{}:
		default:
		}
		return nil
	})
	_ = host.SetReadDeadline(time.Now().Add(deadline))
	_, reader, err := host.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 32<<10)
	if _, err := io.ReadFull(reader, prefix); err != nil || !bytes.Equal(prefix, block[:len(prefix)]) {
		t.Fatalf("single-frame prefix: %v", err)
	}
	received := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, reader, size-int64(len(prefix)))
		if err == nil {
			var tail [1]byte
			_, err = reader.Read(tail[:])
			if err == io.EOF {
				err = nil
			}
		}
		received <- err
	}()
	select {
	case <-pings:
	case err := <-received:
		t.Fatalf("unfinished source frame completed: %v", err)
	case <-time.After(deadline):
		t.Fatal("heartbeat blocked behind source frame")
	}
	for range size/len(block) - 1 {
		if _, err := client.UnderlyingConn().Write(block); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-received; err != nil {
		t.Fatal(err)
	}
}

func TestSendersActuallyStopUnderBackpressure(t *testing.T) {
	for _, pending := range []bool{true, false} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=65536")
			h := register(t, r)
			client, ev := connect(t, r, h)
			var host *websocket.Conn
			if !pending {
				host = accept(t, h, ev)
			}
			var sent atomic.Uint64
			done := make(chan struct{})
			go func() {
				defer close(done)
				block := make([]byte, 64<<10)
				for {
					binary.BigEndian.PutUint64(block, sent.Load())
					if client.WriteMessage(websocket.BinaryMessage, block) != nil {
						return
					}
					sent.Add(1)
				}
			}()
			t.Cleanup(func() { client.Close(); <-done })
			end := time.Now().Add(3 * time.Second)
			stopped := false
			for time.Now().Before(end) {
				before := sent.Load()
				time.Sleep(150 * time.Millisecond)
				if before > 0 && sent.Load() == before {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatalf("sender never stopped: %d messages", sent.Load())
			}
			if pending {
				host = accept(t, h, ev)
			}
			before := sent.Load()
			for next := uint64(0); next <= before; next++ {
				_ = host.SetReadDeadline(time.Now().Add(deadline))
				_, data, err := host.ReadMessage()
				if err != nil || len(data) < 8 || binary.BigEndian.Uint64(data) != next {
					t.Fatalf("resumed message %d: %v", next, err)
				}
			}
		})
	}
}

func residentBytes(t *testing.T, r *relay) int64 {
	t.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(r.cmd.Process.Pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n * 1024
}

func TestConcurrentStreamingRetainsBoundedProcessMemory(t *testing.T) {
	const count, size = 6, 32 << 20
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=33554432")
	h := register(t, r)
	var clients, hosts []*websocket.Conn
	for range count {
		client, host, _ := pairUp(t, r, h)
		clients, hosts = append(clients, client), append(hosts, host)
	}
	baseline := residentBytes(t, r)
	done := make(chan error, count)
	for i := range count {
		go func() {
			writer, err := clients[i].NextWriter(websocket.BinaryMessage)
			if err == nil {
				block := make([]byte, 64<<10)
				for n := 0; n < size/len(block) && err == nil; n++ {
					_, err = writer.Write(block)
				}
				if err == nil {
					err = writer.Close()
				}
			}
			done <- err
		}()
	}
	readers := make([]io.Reader, count)
	for i, host := range hosts {
		_ = host.SetReadDeadline(time.Now().Add(deadline))
		_, reader, err := host.NextReader()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.CopyN(io.Discard, reader, 1<<20); err != nil {
			t.Fatal(err)
		}
		readers[i] = reader
	}
	if growth := residentBytes(t, r) - baseline; growth > 64<<20 {
		t.Fatalf("concurrent streams retained %d bytes (limit 64 MiB)", growth)
	}
	for _, reader := range readers {
		if n, err := io.Copy(io.Discard, reader); err != nil || n != size-(1<<20) {
			t.Fatalf("remaining stream %d: %v", n, err)
		}
	}
	for range count {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
