package relay

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const closeGrace = time.Second

type Server struct {
	cfg      Config
	http     *http.Server
	upgrader websocket.Upgrader
	limiter  *ipLimiter
	budget   *byteBudget
	draining atomic.Bool

	forwardedMessages   atomic.Int64
	forwardedBytes      atomic.Int64
	rejectedConnections atomic.Int64

	mu       sync.Mutex
	hosts    map[string]*host
	controls int
	pending  int
	active   int
	conns    map[*websocket.Conn]struct{}
}

func New(cfg Config) *Server {
	s := &Server{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			WriteBufferPool: &sync.Pool{},
			CheckOrigin:     func(*http.Request) bool { return true },
		},
		limiter: newIPLimiter(cfg.AdmissionRate, cfg.admissionBurst()),
		budget:  newByteBudget(int64(cfg.IngressBudgetBytes), int64(cfg.IngressWeight)),
		hosts:   map[string]*host{},
		conns:   map[*websocket.Conn]struct{}{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/control", s.handleControl)
	mux.HandleFunc("GET /v1/connect", s.handleConnect)
	mux.HandleFunc("GET /v1/accept", s.handleAccept)
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) Serve(ln net.Listener) error {
	return s.http.Serve(ln)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	m := map[string]any{
		"activeHosts":        len(s.hosts),
		"activePairs":        s.active,
		"pendingPairs":       s.pending,
		"controlConnections": s.controls,
		"openSockets":        len(s.conns),
	}
	s.mu.Unlock()
	m["forwardedMessages"] = s.forwardedMessages.Load()
	m["forwardedBytes"] = s.forwardedBytes.Load()
	m["rejectedConnections"] = s.rejectedConnections.Load()
	m["rateLimiterEntries"] = s.limiter.size()
	m["goroutines"] = runtime.NumGoroutine()
	m["draining"] = s.draining.Load()
	m["ingressReservedBytes"] = s.budget.bytes.Load()
	m["ingressReservedWeightedBytes"] = s.budget.used.Load()
	m["ingressBudgetBytes"] = s.cfg.IngressBudgetBytes
	m["ingressWeight"] = s.cfg.IngressWeight
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) reject(w http.ResponseWriter, status int, msg string) {
	s.rejectedConnections.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) admit(w http.ResponseWriter, r *http.Request, duringDrain bool) bool {
	if !duringDrain && s.draining.Load() {
		s.reject(w, http.StatusServiceUnavailable, "draining")
		return false
	}
	if !s.limiter.allow(clientIP(r, s.cfg.TrustedProxies), time.Now()) {
		s.reject(w, http.StatusTooManyRequests, "rate limited")
		return false
	}
	return true
}

func (s *Server) upgrade(w http.ResponseWriter, r *http.Request, limit int64) (*websocket.Conn, bool) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, false
	}
	ws.SetReadLimit(limit)
	s.mu.Lock()
	s.conns[ws] = struct{}{}
	s.mu.Unlock()
	return ws, true
}

func (s *Server) closeNow(ws *websocket.Conn, c closeMsg) {
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.code, c.reason), time.Now().Add(closeGrace))
	_ = ws.SetReadDeadline(time.Now().Add(closeGrace))
	for {
		if _, _, err := ws.NextReader(); err != nil {
			break
		}
	}
	s.forget(ws)
}

func (s *Server) forget(ws *websocket.Conn) {
	_ = ws.Close()
	s.mu.Lock()
	delete(s.conns, ws)
	s.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func textFrame(v any) frame {
	b, _ := json.Marshal(v)
	return frame{typ: websocket.TextMessage, data: b}
}

func randomB64(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
