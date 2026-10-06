package e2e

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
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

	if reserved := settledIngress(r); reserved == 0 || reserved > queue+chunk {
		t.Fatalf("relay buffered %v bytes for a stalled reader, want between 1 and %d", reserved, queue+chunk)
	}
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
	r.waitMetrics("paused queue drained", func(m map[string]float64) bool { return m["ingressReservedBytes"] == 0 })
	host.Close()
	if got := <-received; got != sent {
		t.Fatalf("host received %d of %d messages", got, sent)
	}
}

func TestIngressBudgetLimit(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=4096", "RELAY_MAX_QUEUE_BYTES=65536", "RELAY_INGRESS_BUDGET_BYTES=4096", "RELAY_INGRESS_WEIGHT=1")
	h := register(t, r)
	client, ev := connect(t, r, h)
	send(t, client, message{websocket.BinaryMessage, randomBytes(4096)})
	send(t, client, message{websocket.BinaryMessage, []byte("next")})
	ce := expectClose(t, client, websocket.CloseTryAgainLater)
	if ce.Text != "ingress capacity exceeded" {
		t.Fatalf("unexpected close reason %q", ce.Text)
	}
	if closed := nextEvent(t, h, "closed"); closed.ConnectionID != ev.ConnectionID {
		t.Fatalf("closed event for wrong pair %+v", closed)
	}
	r.waitMetrics("ingress budget release", func(m map[string]float64) bool { return m["ingressReservedBytes"] == 0 })
}

func TestIngressBudgetEvictsHeaviestPair(t *testing.T) {
	const chunk = 64 << 10
	const budget = 8 * chunk
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=65536", "RELAY_MAX_QUEUE_BYTES=4194304", "RELAY_MAX_QUEUE_MESSAGES=1024",
		fmt.Sprintf("RELAY_INGRESS_BUDGET_BYTES=%d", budget), "RELAY_INGRESS_WEIGHT=1", "RELAY_WRITE_TIMEOUT_MS=30000")
	hogClient, hogHost, _ := pairUp(t, r, register(t, r))

	data := randomBytes(chunk)
	for reserved := 0.0; reserved < budget; {
		send(t, hogClient, message{websocket.BinaryMessage, data})
		reserved = settledIngress(r)
	}

	client, host, _ := pairUp(t, r, register(t, r))
	ping := message{websocket.TextMessage, []byte("ping")}
	send(t, client, ping)
	expectMessage(t, host, ping)
	if ce := expectClose(t, hogClient, websocket.CloseTryAgainLater); ce.Text != "ingress capacity exceeded" {
		t.Fatalf("unexpected close reason %q", ce.Text)
	}
	send(t, host, ping)
	expectMessage(t, client, ping)
	r.waitMetrics("eviction recorded", func(m map[string]float64) bool {
		return m["ingressEvictions"] == 1 && m["activePairs"] == 1
	})

	hogHost.Close()
	r.waitMetrics("hog reservations released", func(m map[string]float64) bool { return m["ingressReservedBytes"] == 0 })
}

func settledIngress(r *relay) float64 {
	r.t.Helper()
	last := r.metrics()["ingressReservedBytes"]
	for {
		time.Sleep(25 * time.Millisecond)
		now := r.metrics()["ingressReservedBytes"]
		if now == last {
			return now
		}
		last = now
	}
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
