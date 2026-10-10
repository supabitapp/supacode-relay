package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/admission"
	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
	"github.com/supabitapp/supacode-relay/internal/healthcheck"
)

type router struct {
	cfg            config
	log            *log.Logger
	events         *slog.Logger
	table          *directory.Table
	limiter        *admission.Limiter
	metricsLimiter *admission.Limiter
	gate           *admission.Gate
	proxy          *httputil.ReverseProxy
	upgrader       websocket.Upgrader
	draining       atomic.Bool
	sessions       atomic.Uint64

	requests          [3]atomic.Int64
	active            [3]atomic.Int64
	retries           [3]atomic.Int64
	misses            [3]atomic.Int64
	upstreamErrors    atomic.Int64
	rejectedRate      atomic.Int64
	rejectedCapacity  atomic.Int64
	rejectedDraining  atomic.Int64
	directoryRejected atomic.Int64
	refreshes         atomic.Int64
	rounds            atomic.Int64
	roundTimeouts     atomic.Int64
	evictions         atomic.Int64

	mu        sync.Mutex
	streams   map[string]*stream
	controls  map[string]int
	nextRound *round
	syncing   bool
}

var sensitive = regexp.MustCompile(`\?[^\s"'<>]*|[0-9a-fA-F]{64}`)

func redact(s string) string {
	return sensitive.ReplaceAllString(s, "[redacted]")
}

type redactingWriter struct {
	w io.Writer
}

func (r redactingWriter) Write(p []byte) (int, error) {
	if _, err := io.WriteString(r.w, redact(string(p))); err != nil {
		return 0, err
	}
	return len(p), nil
}

func newRouter(cfg config, logOut io.Writer) *router {
	rt := &router{
		cfg:            cfg,
		log:            log.New(redactingWriter{logOut}, "", log.LstdFlags),
		events:         diagnostics.Logger(redactingWriter{logOut}, "router", ""),
		table:          directory.NewTable(cfg.acceptGrace),
		limiter:        admission.NewLimiter(cfg.admissionRate, cfg.admissionBurst),
		metricsLimiter: admission.NewLimiter(cfg.admissionRate, cfg.admissionBurst),
		gate:           admission.NewGate(cfg.maxConns, cfg.maxConnsPerIP),
		upgrader:       websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096},
		streams:        map[string]*stream{},
		controls:       map[string]int{},
	}
	rt.proxy = rt.newProxy()
	return rt
}

func (rt *router) private() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", rt.handleHealth)
	mux.HandleFunc("GET /metrics", rt.handleMetrics)
	mux.HandleFunc("GET "+directory.Path, rt.handleDirectory)
	return mux
}

func (rt *router) handleHealth(w http.ResponseWriter, _ *http.Request) {
	nodes := len(rt.table.Candidates())
	switch {
	case rt.draining.Load():
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining", "nodes": nodes})
	case nodes == 0:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "no nodes", "nodes": 0})
	default:
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "nodes": nodes})
	}
}

func (rt *router) handlePublicMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	ip := admission.ClientIP(r, rt.cfg.trustedProxies, "")
	if !rt.metricsLimiter.Allow(ip, time.Now()) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "rate limited"})
		return
	}
	rt.handleMetrics(w, r)
}

func (rt *router) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	rt.mu.Lock()
	controls := map[string]int{}
	for k, v := range rt.controls {
		controls[k] = v
	}
	streams := len(rt.streams)
	rt.mu.Unlock()
	var nodes []map[string]any
	for _, n := range rt.table.Nodes() {
		nodes = append(nodes, map[string]any{"id": n.ID, "ready": n.Ready, "draining": n.Draining, "endpoints": n.Entries, "controlSockets": controls[n.ID]})
	}
	perRoute := func(c *[3]atomic.Int64) map[string]int64 {
		out := map[string]int64{}
		for i := range c {
			out[route(i).String()] = c[i].Load()
		}
		return out
	}
	total, ips := rt.gate.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"heapAllocBytes":              memory.HeapAlloc,
		"heapInuseBytes":              memory.HeapInuse,
		"heapSysBytes":                memory.HeapSys,
		"heapReleasedBytes":           memory.HeapReleased,
		"stackInuseBytes":             memory.StackInuse,
		"runtimeSysBytes":             memory.Sys,
		"totalAllocatedBytes":         memory.TotalAlloc,
		"gcCycles":                    memory.NumGC,
		"copyBufferBytesPerDirection": copyBufferBytes,

		"nodes":                   nodes,
		"endpoints":               rt.table.Len(),
		"directoryStreams":        streams,
		"openConnections":         total,
		"openConnectionIPs":       ips,
		"requests":                perRoute(&rt.requests),
		"activeConnections":       perRoute(&rt.active),
		"retries":                 perRoute(&rt.retries),
		"misses":                  perRoute(&rt.misses),
		"upstreamErrors":          rt.upstreamErrors.Load(),
		"rejectedRateLimited":     rt.rejectedRate.Load(),
		"rejectedCapacity":        rt.rejectedCapacity.Load(),
		"rejectedDraining":        rt.rejectedDraining.Load(),
		"directoryRejected":       rt.directoryRejected.Load(),
		"refreshes":               rt.refreshes.Load(),
		"refreshRounds":           rt.rounds.Load(),
		"refreshRoundTimeouts":    rt.roundTimeouts.Load(),
		"evictionsSent":           rt.evictions.Load(),
		"rateLimiterEntries":      rt.limiter.Size(),
		"goroutines":              runtime.NumGoroutine(),
		"draining":                rt.draining.Load(),
		"directoryClock":          rt.table.Clock(),
		"activeUpgradedSockets":   rt.activeTotal(),
		"maxConnections":          rt.cfg.maxConns,
		"maxConnectionsPerClient": rt.cfg.maxConnsPerIP,
	})
}

func (rt *router) activeTotal() int64 {
	var n int64
	for i := range rt.active {
		n += rt.active[i].Load()
	}
	return n
}

func main() {
	if code, handled := diagnostics.Command(os.Args, os.Stdout, os.Stderr); handled {
		os.Exit(code)
	}
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "router: invalid configuration:", err)
		os.Exit(2)
	}
	if code, ok := healthcheck.Command(os.Args, cfg.privateAddr, os.Stdout, os.Stderr); ok {
		os.Exit(code)
	}
	rt := newRouter(cfg, os.Stderr)
	rt.events.Info("router.started", "dial_timeout_ms", cfg.dialTimeout.Milliseconds(),
		"header_timeout_ms", cfg.headerTimeout.Milliseconds(), "refresh_timeout_ms", cfg.refreshTimeout.Milliseconds(),
		"heartbeat_ms", cfg.heartbeat.Milliseconds(), "max_connections", cfg.maxConns,
		"max_connections_per_client", cfg.maxConnsPerIP, "admission_rate", cfg.admissionRate)
	lc := net.ListenConfig{KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: keepAliveIdle, Interval: 5 * time.Second, Count: 3}}
	ln, err := lc.Listen(context.Background(), "tcp", cfg.addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "router: listen:", err)
		os.Exit(1)
	}
	pln, err := lc.Listen(context.Background(), "tcp", cfg.privateAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "router: listen private:", err)
		os.Exit(1)
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	baseContext := func(net.Listener) context.Context { return base }
	srv := &http.Server{Handler: rt, ReadHeaderTimeout: 5 * time.Second, ErrorLog: rt.log, BaseContext: baseContext}
	psrv := &http.Server{Handler: rt.private(), ReadHeaderTimeout: 5 * time.Second, ErrorLog: rt.log, BaseContext: baseContext}
	for _, l := range []struct {
		ln   net.Listener
		name string
	}{{ln, "public"}, {pln, "private"}} {
		line, _ := json.Marshal(map[string]string{"event": "listening", "address": l.ln.Addr().String(), "listener": l.name})
		fmt.Println(string(line))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	errc := make(chan error, 2)
	go func() { errc <- srv.Serve(ln) }()
	go func() { errc <- psrv.Serve(admission.FilterListener(pln, cfg.privatePeers)) }()
	select {
	case err := <-errc:
		rt.log.Println("router: serve:", err)
		os.Exit(1)
	case <-ctx.Done():
	}
	rt.shutdown(srv, psrv, cancel)
}

func (rt *router) shutdown(srv, psrv *http.Server, cancel context.CancelFunc) {
	start := time.Now()
	rt.draining.Store(true)
	srv.SetKeepAlivesEnabled(false)
	rt.log.Printf("router: draining %d connections", rt.activeTotal())
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := start.Add(rt.cfg.drainTimeout)
	for rt.activeTotal() > 0 && time.Now().Before(deadline) {
		<-tick.C
	}
	remaining := rt.activeTotal()
	_ = srv.Close()
	cancel()
	rt.closeStreams()
	_ = psrv.Close()
	end := time.Now().Add(time.Second)
	for rt.activeTotal() > 0 && time.Now().Before(end) {
		<-tick.C
	}
	rt.log.Printf("router: stopped after %s with %d connections cut", time.Since(start).Round(time.Millisecond), remaining)
}
