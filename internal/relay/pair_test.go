package relay

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

type handlerBarrier struct {
	entered  chan struct{}
	returned chan struct{}
	block    func()
	release  func()
	claimed  atomic.Bool
}

func newHandlerBarrier(t *testing.T) *handlerBarrier {
	t.Helper()
	entered, released := make(chan struct{}), make(chan struct{})
	b := &handlerBarrier{
		entered:  entered,
		returned: make(chan struct{}),
		block: sync.OnceFunc(func() {
			close(entered)
			<-released
		}),
		release: sync.OnceFunc(func() { close(released) }),
	}
	t.Cleanup(b.release)
	return b
}

type readErrorConn struct {
	net.Conn
	barrier *handlerBarrier
}

func (c *readErrorConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.barrier.block()
	}
	return n, err
}

type barrierResponseWriter struct {
	http.ResponseWriter
	barrier *handlerBarrier
}

func (w barrierResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &readErrorConn{Conn: conn, barrier: w.barrier}, rw, nil
}

func TestClosingPairKeepsItsAdmissionSlot(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{
		"RELAY_MAX_CLIENTS":          "1",
		"RELAY_MAX_CLIENTS_PER_HOST": "1",
		"RELAY_MAX_PENDING_PER_HOST": "1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	connect, accept := newHandlerBarrier(t), newHandlerBarrier(t)
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b *handlerBarrier
		switch r.URL.Path {
		case "/v1/connect":
			b = connect
		case "/v1/accept":
			b = accept
		}
		if b != nil && b.claimed.CompareAndSwap(false, true) {
			defer close(b.returned)
			w = barrierResponseWriter{ResponseWriter: w, barrier: b}
		}
		s.http.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(listener.Close)
	base := "ws" + strings.TrimPrefix(listener.URL, "http")
	h, nextHost := registerHost(t, base), registerHost(t, base)
	client, host := openPair(t, h)
	expectPairMessage(t, client, host)
	h.Close()
	waitForHandler(t, connect.entered, "connect terminal read")
	waitForHandler(t, accept.entered, "accept terminal read")
	expectClientCapacityReached(t, nextHost)
	expectClosingPairMetrics(t, s, 1)
	connect.release()
	waitForHandler(t, connect.returned, "connect handler exit")
	expectClientCapacityReached(t, nextHost)
	expectClosingPairMetrics(t, s, 1)
	accept.release()
	waitForHandler(t, accept.returned, "accept handler exit")
	expectClosingPairMetrics(t, s, 0)
	client, host = openPair(t, nextHost)
	expectPairMessage(t, client, host)
}

func waitForHandler(t *testing.T, ch <-chan struct{}, milestone string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", milestone)
	}
}

func expectClosingPairMetrics(t *testing.T, s *Server, want int) {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleMetrics(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var counts struct {
		ActivePairs  int
		PendingPairs int
		ClosingPairs int
		ClientSlots  int
	}
	if err := json.Unmarshal(w.Body.Bytes(), &counts); err != nil {
		t.Fatal(err)
	}
	if counts.ActivePairs != 0 || counts.PendingPairs != 0 || counts.ClosingPairs != want || counts.ClientSlots != want {
		t.Fatalf("expected %d closing pairs and slots, got %+v", want, counts)
	}
}

func registerHost(t *testing.T, base string) *endpoint.Host {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	h, err := endpoint.Register(base, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

func openPair(t *testing.T, h *endpoint.Host) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	client, _, err := endpoint.Connect(h.Base, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ev, err := h.Next(5 * time.Second)
	if err != nil || ev.Type != "incoming" {
		t.Fatalf("incoming: %+v %v", ev, err)
	}
	host, _, err := h.Accept(ev)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { host.Close() })
	return client, host
}

func expectPairMessage(t *testing.T, client, host *websocket.Conn) {
	t.Helper()
	_ = client.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := client.WriteMessage(websocket.TextMessage, []byte("paired")); err != nil {
		t.Fatal(err)
	}
	_ = host.SetReadDeadline(time.Now().Add(5 * time.Second))
	typ, data, err := host.ReadMessage()
	if err != nil || typ != websocket.TextMessage || string(data) != "paired" {
		t.Fatalf("forwarded message: type=%d data=%q err=%v", typ, data, err)
	}
}

func expectClientCapacityReached(t *testing.T, h *endpoint.Host) {
	t.Helper()
	ws, resp, err := endpoint.Connect(h.Base, h.ID)
	if err == nil {
		ws.Close()
		t.Fatal("closing pair did not retain admission: connection upgraded")
	}
	status := "no response"
	if resp != nil {
		if resp.StatusCode == http.StatusServiceUnavailable {
			return
		}
		status = resp.Status
	}
	t.Fatalf("expected HTTP 503, got %s (%v)", status, err)
}
