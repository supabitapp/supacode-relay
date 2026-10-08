package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCloseNowPongsCannotExtendDrain(t *testing.T) {
	server := New(Config{Heartbeat: time.Second, WriteTimeout: time.Second})
	finished := make(chan time.Duration, 1)
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, ok := server.upgrade(w, r, 1024)
		if !ok {
			return
		}
		server.newPeer(ws, nil, nil)
		start := time.Now()
		server.closeNow(ws, closeMsg{websocket.CloseGoingAway, "closing"})
		finished <- time.Since(start)
	}))
	defer listener.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(listener.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetCloseHandler(func(int, string) error { return nil })
	go func() {
		for {
			if _, _, err := client.ReadMessage(); err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case elapsed := <-finished:
			if elapsed > 2*time.Second {
				t.Fatalf("pongs extended close drain to %s", elapsed)
			}
			return
		case <-ticker.C:
			_ = client.WriteControl(websocket.PongMessage, nil, time.Now().Add(time.Second))
		case <-timeout.C:
			t.Fatal("pongs kept the close drain alive")
		}
	}
}
