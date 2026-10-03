package e2e

import (
	"crypto/ed25519"
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

func TestPendingBufferLimits(t *testing.T) {
	r := startRelay(t, "RELAY_MAX_QUEUE_MESSAGES=8", "RELAY_MAX_QUEUE_BYTES=4096", "RELAY_MAX_MESSAGE_BYTES=4096")
	h := register(t, r)

	client, ev := connect(t, r, h)
	for i := range 9 {
		send(t, client, message{websocket.TextMessage, []byte(fmt.Sprint(i))})
	}
	expectClose(t, client, websocket.CloseTryAgainLater, websocket.CloseMessageTooBig)
	if closed := nextEvent(t, h, "closed"); closed.ConnectionID != ev.ConnectionID {
		t.Fatalf("closed event for wrong pair %+v", closed)
	}

	client2, ev2 := connect(t, r, h)
	for range 3 {
		send(t, client2, message{websocket.BinaryMessage, randomBytes(2000)})
	}
	expectClose(t, client2, websocket.CloseTryAgainLater, websocket.CloseMessageTooBig)
	nextEvent(t, h, "closed")
	_, resp, err := h.Accept(ev2)
	expectStatus(t, resp, err, 404)
	r.waitIdle()
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
	r := startRelay(t, "RELAY_MAX_MESSAGE_BYTES=65536", "RELAY_MAX_QUEUE_BYTES=262144", "RELAY_MAX_QUEUE_MESSAGES=64", "RELAY_WRITE_TIMEOUT_MS=1000")
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
