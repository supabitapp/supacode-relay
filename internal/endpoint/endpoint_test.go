package endpoint

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHostEventQueueOverflowClosesConnection(t *testing.T) {
	host, disconnected := registerEventHost(t, 1025)
	select {
	case <-host.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("full event queue kept the host reader alive")
	}
	if host.Err == nil || host.Err.Error() != "control event queue full" {
		t.Fatalf("unexpected overflow error: %v", host.Err)
	}
	if len(host.Events) != 1024 {
		t.Fatalf("expected a full event queue, got %d events", len(host.Events))
	}
	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("overflow left the control socket open")
	}
	host.Close()
}

func TestHostCloseClosesEventStream(t *testing.T) {
	host, _ := registerEventHost(t, 1)
	event, err := host.Next(3 * time.Second)
	if err != nil || event.Type != "incoming" {
		t.Fatalf("unexpected event: %+v, %v", event, err)
	}
	host.Close()
	select {
	case _, ok := <-host.Events:
		if ok {
			t.Fatal("unexpected event after closing the host")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("closing the host left its event stream open")
	}
	for range 10 {
		if event, err := host.Next(time.Second); err == nil || event != (Event{}) {
			t.Fatalf("closed host returned a successful event: %+v, %v", event, err)
		}
	}
}

func registerEventHost(t *testing.T, eventCount int) (*Host, <-chan struct{}) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id := EndpointID(key.Public().(ed25519.PublicKey))
	disconnected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(disconnected)
		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		if err := ws.WriteJSON(Event{Type: "challenge", Nonce: "nonce"}); err != nil {
			return
		}
		if _, _, err := ws.ReadMessage(); err != nil {
			return
		}
		if err := ws.WriteJSON(Event{Type: "registered", EndpointID: id}); err != nil {
			return
		}
		for range eventCount {
			if err := ws.WriteJSON(Event{Type: "incoming"}); err != nil {
				return
			}
		}
		_, _, _ = ws.ReadMessage()
	}))
	t.Cleanup(server.Close)
	host, err := Register("ws"+strings.TrimPrefix(server.URL, "http"), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Control.Close() })
	return host, disconnected
}
