package relay

import (
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/admission"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

const (
	closeGrace       = time.Second
	topHostsReported = 20
)

type Server struct {
	cfg      Config
	http     *http.Server
	private  *http.Server
	upgrader websocket.Upgrader
	limiter  *admission.Limiter
	draining atomic.Bool
	clock    directory.Clock

	forwardedMessages   atomic.Int64
	forwardedBytes      atomic.Int64
	rejectedConnections atomic.Int64
	superseded          atomic.Int64
	evicted             atomic.Int64

	mu          sync.Mutex
	hosts       map[string]*host
	pairs       map[string]*pair
	controls    int
	pending     int
	active      int
	clientSlots int
	conns       map[*websocket.Conn]struct{}
	dirStreams  map[*dirStream]struct{}
}

func New(cfg Config) *Server {
	s := &Server{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			HandshakeTimeout: cfg.WriteTimeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  16 * 1024,
			WriteBufferPool:  &sync.Pool{},
			CheckOrigin:      func(*http.Request) bool { return true },
		},
		limiter:    admission.NewLimiter(cfg.AdmissionRate, cfg.admissionBurst()),
		hosts:      map[string]*host{},
		pairs:      map[string]*pair{},
		conns:      map[*websocket.Conn]struct{}{},
		dirStreams: map[*dirStream]struct{}{},
	}
	admin := http.NewServeMux()
	admin.HandleFunc("GET /healthz", s.handleHealth)
	admin.HandleFunc("GET /metrics", s.handleMetrics)
	mux := http.NewServeMux()
	if cfg.PrivateAddr == "" {
		mux.Handle("GET /healthz", admin)
		mux.Handle("GET /metrics", admin)
	}
	mux.HandleFunc("GET /v1/control", s.handleControl)
	mux.HandleFunc("GET /v1/connect", s.handleConnect)
	mux.HandleFunc("GET /v1/accept", s.handleAccept)
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.private = &http.Server{Handler: admin, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) Serve(ln net.Listener) error {
	return s.http.Serve(admission.FilterListener(ln, s.cfg.AllowedPeers))
}

func (s *Server) ServePrivate(ln net.Listener) error {
	return s.private.Serve(admission.FilterListener(ln, s.cfg.PrivatePeers))
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
		"clientSlots":        s.clientSlots,
		"closingPairs":       s.clientSlots - s.active - s.pending,
		"pendingPairs":       s.pending,
		"controlConnections": s.controls,
		"openSockets":        len(s.conns),
		"directoryStreams":   len(s.dirStreams),
		"topHosts":           s.topHosts(),
	}
	s.mu.Unlock()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	m["heapAllocBytes"] = memory.HeapAlloc
	m["heapInuseBytes"] = memory.HeapInuse
	m["heapSysBytes"] = memory.HeapSys
	m["heapReleasedBytes"] = memory.HeapReleased
	m["stackInuseBytes"] = memory.StackInuse
	m["runtimeSysBytes"] = memory.Sys
	m["totalAllocatedBytes"] = memory.TotalAlloc
	m["gcCycles"] = memory.NumGC
	m["nodeId"] = s.cfg.NodeID
	m["supersededRegistrations"] = s.superseded.Load()
	m["evictedRegistrations"] = s.evicted.Load()
	m["forwardedMessages"] = s.forwardedMessages.Load()
	m["forwardedBytes"] = s.forwardedBytes.Load()
	m["rejectedConnections"] = s.rejectedConnections.Load()
	m["rateLimiterEntries"] = s.limiter.Size()
	m["goroutines"] = runtime.NumGoroutine()
	m["draining"] = s.draining.Load()
	m["dataBufferBytesPerSocket"] = 4096 + 16*1024
	writeJSON(w, http.StatusOK, m)
}

type hostTraffic struct {
	Endpoint string `json:"endpoint"`
	BytesIn  int64  `json:"bytesIn"`
	BytesOut int64  `json:"bytesOut"`
	Pairs    int    `json:"pairs"`
}

func (s *Server) topHosts() []hostTraffic {
	hosts := make([]hostTraffic, 0, len(s.hosts))
	for id, h := range s.hosts {
		in, out := h.bytesIn.Load(), h.bytesOut.Load()
		if in+out == 0 {
			continue
		}
		hosts = append(hosts, hostTraffic{Endpoint: id[:16], BytesIn: in, BytesOut: out, Pairs: len(h.pairs)})
	}
	slices.SortFunc(hosts, func(a, b hostTraffic) int {
		return cmp.Compare(b.BytesIn+b.BytesOut, a.BytesIn+a.BytesOut)
	})
	return hosts[:min(len(hosts), topHostsReported)]
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
	if !s.limiter.Allow(admission.ClientIP(r, s.cfg.TrustedProxies, s.cfg.ClientIPHeader), time.Now()) {
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
	deadline := time.Now().Add(closeGrace)
	ws.SetPongHandler(nil)
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.code, c.reason), deadline)
	_ = ws.SetReadDeadline(deadline)
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
