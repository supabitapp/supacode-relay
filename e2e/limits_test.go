package e2e

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func TestPendingBufferDrainsInOrder(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_QUEUE_MESSAGES=8", "RELAY_MAX_QUEUE_BYTES=65536", "RELAY_MAX_MESSAGE_BYTES=4096")
	h := register(t, r)
	client, ev := connect(t, r, h)
	var sent []message
	for i := range 8 {
		m := message{websocket.TextMessage, []byte(fmt.Sprintf("early-%d", i))}
		if i%2 == 1 {
			m = message{websocket.BinaryMessage, randomBytes(100 + i)}
		}
		send(t, client, m)
		sent = append(sent, m)
	}
	host := accept(t, h, ev)
	for _, m := range sent {
		expectMessage(t, host, m)
	}
	late := message{websocket.TextMessage, []byte("late")}
	send(t, client, late)
	expectMessage(t, host, late)
}

func TestPendingBufferPausesSender(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_QUEUE_MESSAGES=8", "RELAY_MAX_QUEUE_BYTES=4096", "RELAY_MAX_MESSAGE_BYTES=4096")
	h := register(t, r)

	client, ev := connect(t, r, h)
	var sent []message
	for i := range 9 {
		sent = append(sent, message{websocket.TextMessage, []byte(fmt.Sprint(i))})
	}
	for range 3 {
		sent = append(sent, message{websocket.BinaryMessage, randomBytes(2000)})
	}
	for _, m := range sent {
		send(t, client, m)
	}
	time.Sleep(200 * time.Millisecond)
	if m := r.metrics(); m["pendingPairs"] != 1 {
		t.Fatalf("pending pair over its buffer was closed instead of paused: %v", m)
	}

	host := accept(t, h, ev)
	for _, m := range sent {
		expectMessage(t, host, m)
	}
}

func TestPairTimeoutReleasesState(t *testing.T) {
	r := startRelay(t, "RELAY_PAIR_TIMEOUT_MS=150")
	h := register(t, r)
	client, ev := connect(t, r, h)
	send(t, client, message{websocket.TextMessage, []byte("buffered then dropped")})
	ce := expectClose(t, client, websocket.CloseTryAgainLater)
	if ce.Text != "pair timeout" {
		t.Fatalf("unexpected reason %q", ce.Text)
	}
	if closed := nextEvent(t, h, "closed"); closed.ConnectionID != ev.ConnectionID {
		t.Fatalf("closed event mismatch %+v", closed)
	}
	r.waitIdle()
}

func TestOversizeMessages(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=1024")
	h := register(t, r)

	client, host, _ := pairUp(t, r, h)
	exact := message{websocket.BinaryMessage, randomBytes(1024)}
	send(t, client, exact)
	expectMessage(t, host, exact)
	send(t, client, message{websocket.BinaryMessage, randomBytes(1025)})
	expectClose(t, client, websocket.CloseMessageTooBig)
	expectClose(t, host, websocket.CloseMessageTooBig)
	nextEvent(t, h, "closed")

	client2, host2, _ := pairUp(t, r, h)
	send(t, host2, message{websocket.TextMessage, make([]byte, 4096)})
	expectClose(t, host2, websocket.CloseMessageTooBig)
	expectClose(t, client2, websocket.CloseMessageTooBig)
	r.waitIdle()
}

func TestSlowReaderIsolation(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=65536", "RELAY_MAX_QUEUE_BYTES=262144", "RELAY_MAX_QUEUE_MESSAGES=64", "RELAY_DELIVERY_TIMEOUT_MS=1000")
	h := register(t, r)
	stalledClient, stalledHost, _ := pairUp(t, r, h)
	healthyClient, healthyHost, _ := pairUp(t, r, h)
	other := register(t, r)
	otherClient, otherHost, _ := pairUp(t, r, other)

	floodDone := make(chan error, 1)
	go func() {
		chunk := randomBytes(65536)
		for {
			_ = stalledClient.SetWriteDeadline(time.Now().Add(deadline))
			if err := stalledClient.WriteMessage(websocket.BinaryMessage, chunk); err != nil {
				floodDone <- err
				return
			}
		}
	}()

	for i := range 200 {
		for _, p := range [][2]*websocket.Conn{{healthyClient, healthyHost}, {otherClient, otherHost}} {
			m := message{websocket.TextMessage, []byte(fmt.Sprintf("ping-%d", i))}
			send(t, p[0], m)
			expectMessage(t, p[1], m)
			send(t, p[1], m)
			expectMessage(t, p[0], m)
		}
	}

	ce := expectClose(t, stalledClient, websocket.CloseTryAgainLater)
	t.Logf("stalled pair closed: %d %q", ce.Code, ce.Text)
	select {
	case <-floodDone:
	case <-time.After(deadline):
		t.Fatal("flood writer did not stop")
	}
	stalledHost.Close()
	r.waitMetrics("stalled pair released", func(m map[string]float64) bool { return m["activePairs"] == 2 })
	m := message{websocket.BinaryMessage, []byte("after")}
	send(t, healthyClient, m)
	expectMessage(t, healthyHost, m)
}

func TestSlowReaderPausesSender(t *testing.T) {
	const chunk = 64 << 10
	const queue = 4 * chunk
	r := startRelay(t, fmt.Sprintf("RELAY_MAX_MESSAGE_BYTES=%d", chunk), fmt.Sprintf("RELAY_MAX_QUEUE_BYTES=%d", queue), "RELAY_MAX_QUEUE_MESSAGES=64")
	client, host, _ := pairUp(t, r, register(t, r))

	stop := make(chan struct{})
	total := make(chan uint64, 1)
	go func() {
		payload := randomBytes(chunk)
		var n uint64
		defer func() { total <- n }()
		for {
			select {
			case <-stop:
				return
			default:
			}
			binary.BigEndian.PutUint64(payload, n)
			_ = client.SetWriteDeadline(time.Now().Add(4 * deadline))
			if err := client.WriteMessage(websocket.BinaryMessage, payload); err != nil {
				return
			}
			n++
		}
	}()

	r.waitMetrics("stalled delivery active", func(m map[string]float64) bool { return m["activePairs"] == 1 })
	time.Sleep(time.Second)
	if m := r.metrics(); m["activePairs"] != 1 {
		t.Fatalf("stalled pair was closed instead of paused: %v", m)
	}

	received := make(chan uint64, 1)
	go func() {
		var next uint64
		defer func() { received <- next }()
		for {
			_ = host.SetReadDeadline(time.Now().Add(deadline))
			_, data, err := host.ReadMessage()
			if err != nil {
				return
			}
			if got := binary.BigEndian.Uint64(data); got != next {
				t.Errorf("message %d arrived as %d", next, got)
				return
			}
			next++
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	sent := <-total
	// A close follows every accepted message through the same source socket.
	_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"), time.Now().Add(deadline))
	if got := <-received; got != sent {
		t.Fatalf("host received %d of %d messages", got, sent)
	}
}

func TestStreamingMessageStartsBeforeFinalFragment(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=33554432")
	client, host, _ := pairUp(t, r, register(t, r))
	writer, err := client.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	payload := randomBytes(64 << 10)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	// No final fragment has been sent. The receiver must already see bytes.
	_ = host.SetReadDeadline(time.Now().Add(deadline))
	typ, reader, err := host.NextReader()
	if err != nil || typ != websocket.BinaryMessage {
		t.Fatalf("reader type=%d err=%v", typ, err)
	}
	prefix := make([]byte, 32<<10)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prefix, payload[:len(prefix)]) {
		t.Fatal("prefix changed")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(rest, payload[len(prefix):]) {
		t.Fatalf("tail changed: %v", err)
	}
}

func TestTruncatedMessageIsNeverCompleted(t *testing.T) {
	r := startRelay(t)
	client, host, _ := pairUp(t, r, register(t, r))
	writer, err := client.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(randomBytes(64 << 10)); err != nil {
		t.Fatal(err)
	}
	_ = host.SetReadDeadline(time.Now().Add(deadline))
	_, reader, err := host.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(io.Discard, reader, 32<<10); err != nil {
		t.Fatal(err)
	}
	client.Close() // Abrupt EOF with no final fragment.
	if _, err := io.Copy(io.Discard, reader); err == nil {
		t.Fatal("truncated message was finalized")
	}
	r.waitIdle()
}

func TestConnectionLimits(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_CLIENTS=3", "RELAY_MAX_CLIENTS_PER_HOST=2", "RELAY_MAX_PENDING_PER_HOST=1", "RELAY_MAX_HOSTS=2")
	h1, h2 := register(t, r), register(t, r)
	_, _, resp, err := endpoint.DialControl(r.base, endpoint.B64(newKey(t).Public().(ed25519.PublicKey)))
	expectStatus(t, resp, err, 503)
	_, ev1 := connect(t, r, h1)
	_, resp, err = endpoint.Connect(r.base, h1.ID)
	expectStatus(t, resp, err, 503)
	accept(t, h1, ev1)
	connect(t, r, h1)
	_, resp, err = endpoint.Connect(r.base, h1.ID)
	expectStatus(t, resp, err, 503)
	connect(t, r, h2)
	_, resp, err = endpoint.Connect(r.base, h2.ID)
	expectStatus(t, resp, err, 503)
	_, resp, err = endpoint.Connect(r.base, endpoint.EndpointID(randomBytes(32)))
	expectStatus(t, resp, err, 404)
	if m := r.metrics(); m["activePairs"] != 1 || m["pendingPairs"] != 2 || m["rejectedConnections"] < 4 {
		t.Fatalf("unexpected metrics %v", m)
	}
}

func TestClosingStalledPairReleasesItsAdmissionSlot(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_CLIENTS=1", "RELAY_MAX_CLIENTS_PER_HOST=1", "RELAY_MAX_PENDING_PER_HOST=1", "RELAY_MAX_MESSAGE_BYTES=65536", "RELAY_MAX_QUEUE_BYTES=262144", "RELAY_DELIVERY_TIMEOUT_MS=30000")
	h := register(t, r)
	nextHost := register(t, r)
	client, _, _ := pairUp(t, r, h)
	stopped := make(chan struct{})
	var sent atomic.Uint64
	go func() {
		defer close(stopped)
		payload := randomBytes(65536)
		for {
			_ = client.SetWriteDeadline(time.Now().Add(deadline))
			if client.WriteMessage(websocket.BinaryMessage, payload) != nil {
				return
			}
			sent.Add(1)
		}
	}()
	r.waitMetrics("stalled delivery owns memory", func(m map[string]float64) bool { return sent.Load() >= 9 && m["forwardedMessages"] >= 8 })
	h.Close()
	r.waitMetrics("closing pair released admission", func(m map[string]float64) bool { return m["clientSlots"] == 0 })
	<-stopped
	nextClient, nextPeer, _ := pairUp(t, r, nextHost)
	m := message{websocket.TextMessage, []byte("capacity recovered")}
	send(t, nextClient, m)
	expectMessage(t, nextPeer, m)
}

func TestInterruptedStreamingMessageNeverCompletes(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{"oversized", []string{"RELAY_MAX_MESSAGE_BYTES=65536"}},
		{"source inactivity", []string{"RELAY_HEARTBEAT_MS=100"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := startRelay(t, tc.env...)
			client, host, _ := pairUp(t, r, register(t, r))
			writer, err := client.NextWriter(websocket.BinaryMessage)
			if err != nil {
				t.Fatal(err)
			}
			_ = client.SetWriteDeadline(time.Now().Add(deadline))
			_, _ = writer.Write(make([]byte, 128<<10))
			// Leave the fragmented message unfinished, including when the source
			// exceeds its read limit or stops answering heartbeats.
			_ = host.SetReadDeadline(time.Now().Add(deadline))
			_, reader, err := host.NextReader()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.CopyN(io.Discard, reader, 16<<10); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, reader); err == nil {
				t.Fatal("interrupted fragments became a complete message")
			}
		})
	}
}

func TestLargeStreamingMessageKeepsHeapBounded(t *testing.T) {
	const size = 32 << 20
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=33554432")
	client, host, _ := pairUp(t, r, register(t, r))
	baseline := r.metrics()["heapAllocBytes"]
	block := bytes.Repeat([]byte{0xa5}, 64<<10)
	written := make(chan error, 1)
	go func() {
		writer, err := client.NextWriter(websocket.BinaryMessage)
		if err != nil {
			written <- err
			return
		}
		for range size / len(block) {
			if _, err := writer.Write(block); err != nil {
				written <- err
				return
			}
		}
		written <- writer.Close()
	}()
	_, reader, err := host.NextReader()
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(block))
	// Pause the receiver within a message much larger than the relay buffers.
	// A store-and-forward implementation still retains its complete 32 MiB here.
	for range (8 << 20) / len(block) {
		if _, err := io.ReadFull(reader, buffer); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buffer, block) {
			t.Fatal("stream changed")
		}
	}
	if growth := r.metrics()["heapAllocBytes"] - baseline; growth > 4<<20 {
		t.Fatalf("relay heap grew %.0f bytes while streaming a %d byte message", growth, size)
	}
	for range (size - (8 << 20)) / len(block) {
		if _, err := io.ReadFull(reader, buffer); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buffer, block) {
			t.Fatal("stream changed")
		}
	}
	if n, err := reader.Read(buffer); n != 0 || err != io.EOF {
		t.Fatalf("message end n=%d err=%v", n, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	r.waitMetrics("large message counted", func(m map[string]float64) bool { return m["forwardedBytes"] == size })
}
