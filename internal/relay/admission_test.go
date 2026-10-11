package relay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func TestHostClientQuotaIsolatesNetworks(t *testing.T) {
	for _, test := range []struct {
		name, first, same, other string
	}{
		{"IPv4", "192.0.2.1", "192.0.2.1", "192.0.2.2"},
		{"IPv6", "2001:db8:1:2::1", "2001:db8:1:2::2", "2001:db8:1:3::1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, base := newClientQuotaServer(t)
			h, otherHost := registerHost(t, base), registerHost(t, base)
			client, event := connectFrom(t, h, test.first)
			for _, phase := range []string{"pending", "active"} {
				ws, response, err := websocket.DefaultDialer.Dial(base+"/v1/connect?endpointId="+h.ID, http.Header{"X-Forwarded-For": {test.same}})
				if err == nil {
					ws.Close()
					t.Fatalf("%s client exceeded host quota", phase)
				}
				if response == nil || response.StatusCode != http.StatusTooManyRequests {
					t.Fatalf("%s rejection: response=%v error=%v", phase, response, err)
				}
				response.Body.Close()
				if phase == "pending" {
					peer, _, err := h.Accept(event)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { peer.Close() })
					expectPairMessage(t, client, peer)
				}
			}
			connectFrom(t, h, test.other)
			connectFrom(t, otherHost, test.same)
			s.mu.Lock()
			groups := len(s.hosts[h.ID].clientCounts)
			s.mu.Unlock()
			if groups != 2 {
				t.Fatalf("host accounts for %d client networks, want 2", groups)
			}
		})
	}
}

func TestHostClientQuotaReleasedBeforePairing(t *testing.T) {
	for _, scenario := range []string{"upgrade failure", "pair timeout"} {
		t.Run(scenario, func(t *testing.T) {
			s, base := newClientQuotaServer(t)
			h := registerHost(t, base)
			if scenario == "upgrade failure" {
				r := httptest.NewRequest(http.MethodGet, "/v1/connect?endpointId="+h.ID, nil)
				r.RemoteAddr = "192.0.2.1:1234"
				s.http.Handler.ServeHTTP(httptest.NewRecorder(), r)
			} else {
				client, event := connectFrom(t, h, "192.0.2.1")
				s.mu.Lock()
				p := s.pairs[event.ConnectionID]
				s.mu.Unlock()
				p.expire()
				_ = client.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := client.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
					t.Fatalf("pair timeout: %v", err)
				}
			}
			deadline := time.After(time.Second)
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				s.mu.Lock()
				groups := len(s.hosts[h.ID].clientCounts)
				s.mu.Unlock()
				if groups == 0 {
					break
				}
				select {
				case <-tick.C:
				case <-deadline:
					t.Fatal("client quota retained after pair cleanup")
				}
			}
			if scenario == "pair timeout" {
				if event, err := h.Next(time.Second); err != nil || event.Type != "closed" {
					t.Fatalf("close event: %+v, %v", event, err)
				}
			}
			connectFrom(t, h, "192.0.2.1")
		})
	}
}

func newClientQuotaServer(t *testing.T) (*Server, string) {
	t.Helper()
	cfg, err := LoadConfig(env(map[string]string{
		"RELAY_MAX_CLIENTS_PER_HOST_PER_IP": "1",
		"RELAY_TRUSTED_PROXIES":             "127.0.0.1/32",
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard)
	listener := httptest.NewServer(s.http.Handler)
	t.Cleanup(listener.Close)
	t.Cleanup(func() { closeLifecycleSockets(s) })
	return s, "ws" + strings.TrimPrefix(listener.URL, "http")
}

func connectFrom(t *testing.T, h *endpoint.Host, ip string) (*websocket.Conn, endpoint.Event) {
	t.Helper()
	ws, response, err := websocket.DefaultDialer.Dial(h.Base+"/v1/connect?endpointId="+h.ID, http.Header{"X-Forwarded-For": {ip}})
	if err != nil {
		t.Fatalf("connect from %s: response=%v error=%v", ip, response, err)
	}
	t.Cleanup(func() { ws.Close() })
	event, err := h.Next(time.Second)
	if err != nil || event.Type != "incoming" {
		t.Fatalf("incoming event: %+v, %v", event, err)
	}
	return ws, event
}
