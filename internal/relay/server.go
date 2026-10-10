package relay

import (
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"log"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/admission"
	"github.com/supabitapp/supacode-relay/internal/diagnostics"
)

const (
	closeGrace       = time.Second
	topHostsReported = 20
)

type Server struct {
	cfg            Config
	http           *http.Server
	private        *http.Server
	upgrader       websocket.Upgrader
	limiter        *admission.Limiter
	metricsLimiter *admission.Limiter
	gate           *admission.Gate
	events         *slog.Logger
	draining       atomic.Bool

	forwardedMessages   atomic.Int64
	forwardedBytes      atomic.Int64
	rejectedConnections atomic.Int64
	superseded          atomic.Int64

	mu          sync.Mutex
	hosts       map[string]*host
	pairs       map[string]*pair
	controls    int
	pending     int
	active      int
	clientSlots int
	conns       map[*websocket.Conn]struct{}
}

func New(cfg Config) *Server {
	s := &Server{
		cfg:    cfg,
		events: diagnostics.Logger(log.Writer()),
		upgrader: websocket.Upgrader{
			HandshakeTimeout: cfg.WriteTimeout,
			ReadBufferSize:   4096,
			WriteBufferSize:  16 * 1024,
			WriteBufferPool:  &sync.Pool{},
			CheckOrigin:      func(*http.Request) bool { return true },
		},
		limiter:        admission.NewLimiter(cfg.AdmissionRate, cfg.admissionBurst()),
		metricsLimiter: admission.NewLimiter(cfg.AdmissionRate, cfg.admissionBurst()),
		gate:           admission.NewGate(cfg.MaxConnections, cfg.MaxConnectionsPerIP),
		hosts:          map[string]*host{},
		pairs:          map[string]*pair{},
		conns:          map[*websocket.Conn]struct{}{},
	}
	admin := http.NewServeMux()
	admin.HandleFunc("GET /healthz", s.handleHealth)
	admin.HandleFunc("GET /metrics", s.handleMetrics)
	mux := http.NewServeMux()
	if cfg.PrivateAddr == "" {
		mux.Handle("GET /healthz", admin)
		mux.Handle("GET /metrics", admin)
	} else {
		mux.HandleFunc("GET /metrics", s.handlePublicMetrics)
	}
	mux.HandleFunc("GET /v1/control", s.limitConnections(s.handleControl))
	mux.HandleFunc("GET /v1/connect", s.limitConnections(s.handleConnect))
	mux.HandleFunc("GET /v1/accept", s.limitConnections(s.handleAccept))
	s.http = &http.Server{Handler: s.traceRequests(mux), ReadHeaderTimeout: 5 * time.Second}
	s.private = &http.Server{Handler: admin, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) traceRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace, request := diagnostics.Request(s.events, r, diagnostics.RequestOptions{
			TrustedProxies: s.cfg.TrustedProxies,
			ClientIPHeader: s.cfg.ClientIPHeader,
		})
		trace.Event("request.begin")
		response := &diagnostics.Response{ResponseWriter: w}
		response.Header().Set(diagnostics.Header, trace.ID())
		defer func() {
			trace.Event("request.end", "status", response.Status, "upgraded", response.Hijacked)
		}()
		next.ServeHTTP(response, request)
	})
}

func (s *Server) Serve(ln net.Listener) error {
	s.events.Info("relay.started", "auth_timeout_ms", s.cfg.AuthTimeout.Milliseconds(),
		"pair_timeout_ms", s.cfg.PairTimeout.Milliseconds(), "delivery_timeout_ms", s.cfg.DeliveryTimeout.Milliseconds(),
		"heartbeat_ms", s.cfg.Heartbeat.Milliseconds(), "max_hosts", s.cfg.MaxHosts,
		"max_clients", s.cfg.MaxClients, "max_clients_per_host", s.cfg.MaxClientsPerHost,
		"max_pending_per_host", s.cfg.MaxPendingPerHost)
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
	s.writeMetrics(w, true)
}

func (s *Server) handlePublicMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.metricsLimiter.Allow(admission.ClientIP(r, s.cfg.TrustedProxies, s.cfg.ClientIPHeader), time.Now()) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
		return
	}
	s.writeMetrics(w, false)
}

func (s *Server) writeMetrics(w http.ResponseWriter, detailed bool) {
	s.mu.Lock()
	m := map[string]any{
		"activeHosts":        len(s.hosts),
		"activePairs":        s.active,
		"clientSlots":        s.clientSlots,
		"closingPairs":       s.clientSlots - s.active - s.pending,
		"pendingPairs":       s.pending,
		"controlConnections": s.controls,
		"openSockets":        len(s.conns),
	}
	if detailed {
		m["topHosts"] = s.topHosts()
	}
	s.mu.Unlock()
	m["openConnections"], m["openConnectionIPs"] = s.gate.Stats()
	m["maxConnections"] = s.cfg.MaxConnections
	m["maxConnectionsPerClient"] = s.cfg.MaxConnectionsPerIP
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
	m["supersededRegistrations"] = s.superseded.Load()
	m["forwardedMessages"] = s.forwardedMessages.Load()
	m["forwardedBytes"] = s.forwardedBytes.Load()
	m["rejectedConnections"] = s.rejectedConnections.Load()
	m["rateLimiterEntries"] = s.limiter.Size()
	m["goroutines"] = runtime.NumGoroutine()
	m["draining"] = s.draining.Load()
	m["dataBufferBytesPerSocket"] = 4096 + 16*1024
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) limitConnections(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := admission.ClientIP(r, s.cfg.TrustedProxies, s.cfg.ClientIPHeader)
		switch s.gate.Acquire(ip) {
		case admission.GlobalFull:
			s.reject(w, r, http.StatusServiceUnavailable, "connection capacity reached")
			return
		case admission.PerIPFull:
			s.reject(w, r, http.StatusTooManyRequests, "client connection capacity reached")
			return
		}
		defer s.gate.Release(ip)
		next(w, r)
	}
}

type hostTraffic struct {
	Endpoint string `json:"endpoint"`
	TraceTag string `json:"traceTag"`
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
		hosts = append(hosts, hostTraffic{Endpoint: id[:16], TraceTag: diagnostics.Tag(id), BytesIn: in, BytesOut: out, Pairs: len(h.pairs)})
	}
	slices.SortFunc(hosts, func(a, b hostTraffic) int {
		return cmp.Compare(b.BytesIn+b.BytesOut, a.BytesIn+a.BytesOut)
	})
	return hosts[:min(len(hosts), topHostsReported)]
}

func (s *Server) reject(w http.ResponseWriter, r *http.Request, status int, msg string) {
	s.rejectedConnections.Add(1)
	diagnostics.From(r).Event("request.rejected", "status", status, "reason", msg)
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) admit(w http.ResponseWriter, r *http.Request, duringDrain bool) bool {
	if !duringDrain && s.draining.Load() {
		s.reject(w, r, http.StatusServiceUnavailable, "draining")
		return false
	}
	if !s.limiter.Allow(admission.ClientIP(r, s.cfg.TrustedProxies, s.cfg.ClientIPHeader), time.Now()) {
		s.reject(w, r, http.StatusTooManyRequests, "rate limited")
		return false
	}
	return true
}

func (s *Server) upgrade(w http.ResponseWriter, r *http.Request, limit int64) (*websocket.Conn, bool) {
	var headers http.Header
	if trace := diagnostics.From(r); trace != nil {
		headers = http.Header{diagnostics.Header: {trace.ID()}}
	}
	ws, err := s.upgrader.Upgrade(w, r, headers)
	if err != nil {
		diagnostics.From(r).Failure("socket.upgrade.failed", err)
		return nil, false
	}
	ws.SetReadLimit(limit)
	s.mu.Lock()
	s.conns[ws] = struct{}{}
	s.mu.Unlock()
	diagnostics.From(r).Event("socket.upgraded")
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
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(v)
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
