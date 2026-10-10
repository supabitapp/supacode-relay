package relay

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
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

func TestNodeOnlyInheritsClusterRouterTraces(t *testing.T) {
	const suppliedTrace = "ABCDabcd1234-_56"
	for _, test := range []struct {
		name    string
		cluster bool
		trusted bool
	}{
		{"standalone", false, true},
		{"trusted_router", true, true},
		{"untrusted_peer", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{}
			if test.cluster {
				cfg.Routers = []string{"ws://router"}
			}
			if test.trusted {
				cfg.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
			}
			s := New(cfg)
			request := httptest.NewRequest(http.MethodGet, "/unsupported", nil)
			request.RemoteAddr = "127.0.0.1:1234"
			request.Header.Set(diagnostics.Header, suppliedTrace)
			request.Header.Set("X-Forwarded-For", "203.0.113.2")
			response := httptest.NewRecorder()
			s.http.Handler.ServeHTTP(response, request)
			trace := response.Header().Get(diagnostics.Header)
			if len(trace) != 16 || (trace == suppliedTrace) != (test.cluster && test.trusted) {
				t.Fatalf("incorrect node trace trust: %q", trace)
			}
		})
	}
}
