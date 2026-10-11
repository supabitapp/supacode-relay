package relay

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/dashboard"
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

func TestRelayIgnoresCallerTraceHeaders(t *testing.T) {
	const suppliedTrace = "ABCDabcd1234-_56"
	for _, trusted := range []bool{false, true} {
		cfg := Config{}
		if trusted {
			cfg.TrustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
		}
		s := New(cfg)
		request := httptest.NewRequest(http.MethodGet, "/unsupported", nil)
		request.RemoteAddr = "127.0.0.1:1234"
		request.Header.Set(diagnostics.Header, suppliedTrace)
		response := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(response, request)
		trace := response.Header().Get(diagnostics.Header)
		if len(trace) != 16 || trace == suppliedTrace {
			t.Fatalf("incorrect relay trace: %q", trace)
		}
	}
}

func TestDashboardIsServedOutsideRequestTracing(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{"RELAY_PRIVATE_ADDR": "127.0.0.1:0"}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard)
	serve := func(path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		s.http.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		return response
	}
	if response := serve("/dashboard"); response.Code/100 != 3 || response.Header().Get("Location") != "/dashboard/" {
		t.Errorf("/dashboard: status %d location %q", response.Code, response.Header().Get("Location"))
	}
	for path, status := range map[string]int{
		"/dashboard/":       http.StatusOK,
		"/dashboard/stats":  http.StatusOK,
		"/dashboard/static": http.StatusNotFound,
		"/metrics":          http.StatusOK,
	} {
		response := serve(path)
		traced := response.Header().Get(diagnostics.Header) != ""
		if response.Code != status || traced != (path == "/metrics") {
			t.Errorf("%s: status %d traced %v", path, response.Code, traced)
		}
	}
}

func TestDashboardStatsMatchMetrics(t *testing.T) {
	cfg, err := LoadConfig(env(map[string]string{"RELAY_MAX_HOSTS": "7", "RELAY_MAX_CLIENTS": "9", "RELAY_MAX_CONNS": "11"}))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg)
	s.events = diagnostics.Logger(io.Discard)
	listener := httptest.NewServer(s.http.Handler)
	defer listener.Close()
	openPair(t, registerHost(t, "ws"+strings.TrimPrefix(listener.URL, "http")))
	offline, err := http.Get(listener.URL + "/v1/connect?endpointId=" + strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	offline.Body.Close()
	response := httptest.NewRecorder()
	s.handleMetrics(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var metrics struct {
		Draining                                                   bool
		ActiveHosts, ActivePairs, PendingPairs, ControlConnections int
		ClientSlots, OpenSockets, OpenConnections                  int
		RejectedConnections, ForwardedMessages, ForwardedBytes     int64
		RejectedConnectionsByReason                                map[string]int64
	}
	if err := json.Unmarshal(response.Body.Bytes(), &metrics); err != nil {
		t.Fatal(err)
	}
	want := dashboard.Stats{
		Draining:           metrics.Draining,
		ActiveHosts:        metrics.ActiveHosts,
		ActivePairs:        metrics.ActivePairs,
		PendingPairs:       metrics.PendingPairs,
		ControlConnections: metrics.ControlConnections,
		ClientSlots:        metrics.ClientSlots,
		OpenSockets:        metrics.OpenSockets,
		OpenConnections:    metrics.OpenConnections,
		MaxHosts:           7,
		MaxClients:         9,
		MaxConnections:     11,
		ForwardedMessages:  metrics.ForwardedMessages,
		ForwardedBytes:     metrics.ForwardedBytes,

		RejectedConnections:         metrics.RejectedConnections,
		RejectedConnectionsByReason: metrics.RejectedConnectionsByReason,
	}
	if got := s.dashboardStats(); !reflect.DeepEqual(got, want) || got.ActiveHosts != 1 || got.ClientSlots != 1 || got.RejectedConnections != 1 {
		t.Fatalf("dashboard stats = %+v, want %+v", got, want)
	}
}
