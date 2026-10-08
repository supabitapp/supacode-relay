package relay

import (
	"bufio"
	"crypto/ed25519"
	"crypto/rand"
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

type readErrorBarrier struct {
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}
	claimed  atomic.Bool
}

type readErrorConn struct {
	net.Conn
	barrier *readErrorBarrier
	once    sync.Once
}

func (c *readErrorConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if err != nil {
		c.once.Do(func() {
			close(c.barrier.entered)
			<-c.barrier.release
		})
	}
	return n, err
}

type barrierResponseWriter struct {
	http.ResponseWriter
	barrier *readErrorBarrier
}

func (w barrierResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &readErrorConn{Conn: conn, barrier: w.barrier}, rw, nil
}

func TestClosingPairKeepsItsAdmissionSlot(t *testing.T) {
	cfg, err := LoadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxClients, cfg.MaxClientsPerHost, cfg.MaxPendingPerHost = 1, 1, 1
	cfg.AdmissionRate = 100000
	s := New(cfg)
	barriers := map[string]*readErrorBarrier{}
	for _, path := range []string{"/v1/connect", "/v1/accept"} {
		b := &readErrorBarrier{entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
		barriers[path] = b
		t.Cleanup(func() {
			select {
			case <-b.release:
			default:
				close(b.release)
			}
		})
	}
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b := barriers[r.URL.Path]; b != nil && b.claimed.CompareAndSwap(false, true) {
			defer close(b.returned)
			w = barrierResponseWriter{ResponseWriter: w, barrier: b}
		}
		s.http.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(listener.Close)
	base := "ws" + strings.TrimPrefix(listener.URL, "http")
	wait := func(ch <-chan struct{}, milestone string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", milestone)
		}
	}
	register := func() *endpoint.Host {
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
	pairUp := func(h *endpoint.Host) (*websocket.Conn, *websocket.Conn) {
		t.Helper()
		client, _, err := endpoint.Connect(base, h.ID)
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
	exchange := func(client, host *websocket.Conn) {
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
	h, nextHost := register(), register()
	client, host := pairUp(h)
	exchange(client, host)
	h.Close()
	for path, b := range barriers {
		wait(b.entered, path+" terminal read")
	}
	assertFull := func() {
		t.Helper()
		ws, resp, err := endpoint.Connect(base, nextHost.ID)
		if ws != nil {
			ws.Close()
		}
		if resp != nil && resp.Body != nil {
			defer resp.Body.Close()
		}
		if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("closing pair did not retain admission: response=%v err=%v", resp, err)
		}
	}
	assertFull()
	connect := barriers["/v1/connect"]
	close(connect.release)
	wait(connect.returned, "connect handler exit")
	assertFull()
	accept := barriers["/v1/accept"]
	close(accept.release)
	wait(accept.returned, "accept handler exit")
	client, host = pairUp(nextHost)
	exchange(client, host)
}
