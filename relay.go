package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	signaturePrefix  = "supacode-relay-v1\n"
	controlReadLimit = 4096
	closeGrace       = time.Second
)

type pairState int

const (
	pending pairState = iota
	accepting
	active
	closed
)

type server struct {
	cfg      config
	upgrader websocket.Upgrader
	limiter  *ipLimiter
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

type host struct {
	id      string
	ctrl    *peer
	pairs   map[string]*pair
	pending int
	gone    bool
}

type pair struct {
	s         *server
	id        string
	token     string
	host      *host
	state     pairState
	cause     closeMsg
	announced bool
	timer     *time.Timer
	toHost    *queue
	toClient  *queue
	client    *peer
	hostPeer  *peer
}

type peer struct {
	s     *server
	ws    *websocket.Conn
	q     *queue
	data  bool
	dead  chan struct{}
	cause atomic.Pointer[closeMsg]
}

func newServer(cfg config) *server {
	return &server{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			WriteBufferPool: &sync.Pool{},
			CheckOrigin:     func(*http.Request) bool { return true },
		},
		limiter: newIPLimiter(cfg.admissionRate, cfg.admissionBurst),
		hosts:   map[string]*host{},
		conns:   map[*websocket.Conn]struct{}{},
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/control", s.handleControl)
	mux.HandleFunc("GET /v1/connect", s.handleConnect)
	mux.HandleFunc("GET /v1/accept", s.handleAccept)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
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
	writeJSON(w, http.StatusOK, m)
}

func (s *server) reject(w http.ResponseWriter, status int, msg string) {
	s.rejectedConnections.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *server) admit(w http.ResponseWriter, r *http.Request, duringDrain bool) bool {
	if !duringDrain && s.draining.Load() {
		s.reject(w, http.StatusServiceUnavailable, "draining")
		return false
	}
	if !s.limiter.allow(clientIP(r, s.cfg.trustedProxies), time.Now()) {
		s.reject(w, http.StatusTooManyRequests, "rate limited")
		return false
	}
	return true
}

func decodeB64(s string, n int) ([]byte, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || len(b) != n || base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, false
	}
	return b, true
}

func randomB64(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func EndpointID(pub []byte) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func (s *server) upgrade(w http.ResponseWriter, r *http.Request, limit int64) (*websocket.Conn, bool) {
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

func (s *server) closeNow(ws *websocket.Conn, c closeMsg) {
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.code, c.reason), time.Now().Add(closeGrace))
	_ = ws.SetReadDeadline(time.Now().Add(closeGrace))
	for {
		if _, _, err := ws.NextReader(); err != nil {
			break
		}
	}
	s.forget(ws)
}

func (s *server) forget(ws *websocket.Conn) {
	_ = ws.Close()
	s.mu.Lock()
	delete(s.conns, ws)
	s.mu.Unlock()
}

func (s *server) handleControl(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, false) {
		return
	}
	pub, ok := decodeB64(r.URL.Query().Get("publicKey"), ed25519.PublicKeySize)
	if !ok {
		s.reject(w, http.StatusBadRequest, "invalid publicKey")
		return
	}
	s.mu.Lock()
	if s.controls >= s.cfg.maxHosts {
		s.mu.Unlock()
		s.reject(w, http.StatusServiceUnavailable, "host capacity reached")
		return
	}
	s.controls++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.controls--
		s.mu.Unlock()
	}()

	ws, ok := s.upgrade(w, r, controlReadLimit)
	if !ok {
		return
	}
	id := EndpointID(pub)
	nonce := randomB64(32)
	_ = ws.SetWriteDeadline(time.Now().Add(s.cfg.writeTimeout))
	if err := ws.WriteMessage(websocket.TextMessage, mustJSON(map[string]string{"type": "challenge", "nonce": nonce})); err != nil {
		s.forget(ws)
		return
	}
	_ = ws.SetReadDeadline(time.Now().Add(s.cfg.authTimeout))
	if !verifyAuth(ws, pub, []byte(signaturePrefix+id+"\n"+nonce)) {
		s.rejectedConnections.Add(1)
		s.closeNow(ws, closeMsg{websocket.ClosePolicyViolation, "authentication failed"})
		return
	}

	ctrl := s.newPeer(ws, newQueue(s.cfg.maxQueueBytes, s.cfg.maxQueueMessages), false)
	h := &host{id: id, ctrl: ctrl, pairs: map[string]*pair{}}
	ctrl.q.push(frame{websocket.TextMessage, mustJSON(map[string]string{"type": "registered", "endpointId": id})})
	s.mu.Lock()
	if s.hosts[id] != nil {
		s.mu.Unlock()
		s.rejectedConnections.Add(1)
		s.closeNow(ws, closeMsg{websocket.ClosePolicyViolation, "endpoint already registered"})
		return
	}
	s.hosts[id] = h
	s.mu.Unlock()

	go ctrl.writeLoop()
	ctrl.readControl()
	s.removeHost(h)
}

func verifyAuth(ws *websocket.Conn, pub ed25519.PublicKey, msg []byte) bool {
	typ, data, err := ws.ReadMessage()
	if err != nil || typ != websocket.TextMessage {
		return false
	}
	var auth struct {
		Type      string `json:"type"`
		Signature string `json:"signature"`
	}
	if json.Unmarshal(data, &auth) != nil || auth.Type != "authenticate" {
		return false
	}
	sig, ok := decodeB64(auth.Signature, ed25519.SignatureSize)
	return ok && ed25519.Verify(pub, msg, sig)
}

func (s *server) removeHost(h *host) {
	s.mu.Lock()
	if s.hosts[h.id] == h {
		delete(s.hosts, h.id)
	}
	h.gone = true
	pairs := make([]*pair, 0, len(h.pairs))
	for _, p := range h.pairs {
		pairs = append(pairs, p)
	}
	s.mu.Unlock()
	for _, p := range pairs {
		p.close(closeMsg{websocket.CloseGoingAway, "host offline"}, nil, false)
	}
}

func (s *server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, false) {
		return
	}
	id := r.URL.Query().Get("endpointId")
	s.mu.Lock()
	h := s.hosts[id]
	if h == nil {
		s.mu.Unlock()
		s.reject(w, http.StatusNotFound, "endpoint not found")
		return
	}
	if s.pending+s.active >= s.cfg.maxClients || len(h.pairs) >= s.cfg.maxClientsPerHost || h.pending >= s.cfg.maxPendingPerHost {
		s.mu.Unlock()
		s.reject(w, http.StatusServiceUnavailable, "client capacity reached")
		return
	}
	p := &pair{
		s:        s,
		id:       randomB64(16),
		token:    randomB64(32),
		host:     h,
		toHost:   newQueue(s.cfg.maxQueueBytes, s.cfg.maxQueueMessages),
		toClient: newQueue(s.cfg.maxQueueBytes, s.cfg.maxQueueMessages),
	}
	h.pairs[p.id] = p
	h.pending++
	s.pending++
	p.timer = time.AfterFunc(s.cfg.pairTimeout, p.expire)
	s.mu.Unlock()

	ws, ok := s.upgrade(w, r, int64(s.cfg.maxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "upgrade failed"}, nil, false)
		return
	}
	cp := s.newPeer(ws, p.toClient, true)
	s.mu.Lock()
	if p.state == closed {
		cause := p.cause
		s.mu.Unlock()
		s.closeNow(ws, cause)
		return
	}
	p.client = cp
	p.announced = true
	if !h.gone && !h.ctrl.q.push(frame{websocket.TextMessage, mustJSON(map[string]string{"type": "incoming", "connectionId": p.id, "token": p.token})}) {
		h.ctrl.q.finish(closeMsg{websocket.CloseTryAgainLater, "control queue limit exceeded"}, true)
	}
	s.mu.Unlock()

	go cp.writeLoop()
	cp.readData(p, p.toHost)
}

func (s *server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, true) {
		return
	}
	q := r.URL.Query()
	s.mu.Lock()
	var p *pair
	if h := s.hosts[q.Get("endpointId")]; h != nil {
		p = h.pairs[q.Get("connectionId")]
	}
	if p == nil {
		s.mu.Unlock()
		s.reject(w, http.StatusNotFound, "connection not found")
		return
	}
	token := q.Get("token")
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(p.token)) != 1 || p.state != pending || p.client == nil {
		s.mu.Unlock()
		s.reject(w, http.StatusForbidden, "invalid token")
		return
	}
	p.state = accepting
	p.timer.Stop()
	s.mu.Unlock()

	ws, ok := s.upgrade(w, r, int64(s.cfg.maxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "host accept failed"}, nil, false)
		return
	}
	hp := s.newPeer(ws, p.toHost, true)
	s.mu.Lock()
	if p.state != accepting {
		cause := p.cause
		s.mu.Unlock()
		s.closeNow(ws, cause)
		return
	}
	p.state = active
	p.hostPeer = hp
	p.host.pending--
	s.pending--
	s.active++
	s.mu.Unlock()

	go hp.writeLoop()
	hp.readData(p, p.toClient)
}

func (p *pair) expire() {
	p.s.mu.Lock()
	waiting := p.state == pending
	p.s.mu.Unlock()
	if waiting {
		p.close(closeMsg{websocket.CloseTryAgainLater, "pair timeout"}, nil, false)
	}
}

func (p *pair) close(c closeMsg, origin *peer, discard bool) {
	s := p.s
	s.mu.Lock()
	prev := p.state
	if prev == closed {
		s.mu.Unlock()
		return
	}
	p.state = closed
	p.cause = c
	h := p.host
	delete(h.pairs, p.id)
	if prev == active {
		s.active--
	} else {
		h.pending--
		s.pending--
	}
	if p.announced && !h.gone && !h.ctrl.q.push(frame{websocket.TextMessage, mustJSON(map[string]string{"type": "closed", "connectionId": p.id})}) {
		h.ctrl.q.finish(closeMsg{websocket.CloseTryAgainLater, "control queue limit exceeded"}, true)
	}
	peers := []*peer{p.client, p.hostPeer}
	s.mu.Unlock()
	p.timer.Stop()
	for _, x := range peers {
		if x != nil && x != origin {
			x.q.finish(c, discard)
		}
	}
}

func (s *server) newPeer(ws *websocket.Conn, q *queue, data bool) *peer {
	p := &peer{s: s, ws: ws, q: q, data: data, dead: make(chan struct{})}
	_ = ws.SetReadDeadline(time.Now().Add(s.cfg.pongTimeout()))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(s.cfg.pongTimeout()))
	})
	return p
}

func (p *peer) fail(c closeMsg) {
	p.cause.CompareAndSwap(nil, &c)
	_ = p.ws.Close()
}

func (p *peer) writeLoop() {
	cfg := p.s.cfg
	ping := time.NewTicker(cfg.heartbeat)
	defer ping.Stop()
	sendPing := func() bool {
		if err := p.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(cfg.writeTimeout)); err != nil {
			p.fail(closeMsg{websocket.CloseGoingAway, "peer unreachable"})
			return false
		}
		return true
	}
	for {
		f, final, ok := p.q.next()
		if ok {
			_ = p.ws.SetWriteDeadline(time.Now().Add(cfg.writeTimeout))
			if err := p.ws.WriteMessage(f.typ, f.data); err != nil {
				p.fail(closeMsg{websocket.CloseTryAgainLater, "peer write timeout"})
				return
			}
			p.q.release(len(f.data))
			if p.data {
				p.s.forwardedMessages.Add(1)
				p.s.forwardedBytes.Add(int64(len(f.data)))
			}
			select {
			case <-ping.C:
				if !sendPing() {
					return
				}
			default:
			}
			continue
		}
		if final != nil {
			_ = p.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(final.code, final.reason), time.Now().Add(cfg.writeTimeout))
			_ = p.ws.SetReadDeadline(time.Now().Add(closeGrace))
			return
		}
		select {
		case <-p.q.wake:
		case <-ping.C:
			if !sendPing() {
				return
			}
		case <-p.dead:
			return
		}
	}
}

func (p *peer) extend() {
	_ = p.ws.SetReadDeadline(time.Now().Add(p.s.cfg.pongTimeout()))
}

func (p *peer) exit() {
	close(p.dead)
	p.s.forget(p.ws)
}

func (p *peer) readControl() {
	defer p.exit()
	for {
		if _, _, err := p.ws.ReadMessage(); err != nil {
			return
		}
		p.q.finish(closeMsg{websocket.ClosePolicyViolation, "unexpected control message"}, true)
	}
}

func (p *peer) readData(pr *pair, out *queue) {
	defer p.exit()
	for {
		typ, data, err := p.ws.ReadMessage()
		if err != nil {
			pr.close(p.closeCause(err), p, false)
			return
		}
		p.extend()
		if !out.push(frame{typ, data}) {
			out.finish(closeMsg{websocket.CloseTryAgainLater, "queue limit exceeded"}, true)
			pr.close(closeMsg{websocket.CloseTryAgainLater, "queue limit exceeded"}, nil, true)
		}
	}
}

func (p *peer) closeCause(err error) closeMsg {
	if c := p.cause.Load(); c != nil {
		return *c
	}
	var ce *websocket.CloseError
	switch {
	case errors.As(err, &ce) && ce.Code == websocket.CloseNoStatusReceived:
		return closeMsg{websocket.CloseNormalClosure, ""}
	case errors.As(err, &ce) && sendableCode(ce.Code):
		return closeMsg{ce.Code, ce.Text}
	case errors.Is(err, websocket.ErrReadLimit):
		return closeMsg{websocket.CloseMessageTooBig, "message too big"}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return closeMsg{websocket.CloseGoingAway, "peer timeout"}
	}
	return closeMsg{websocket.CloseGoingAway, "peer disconnected"}
}

func sendableCode(c int) bool {
	return (c >= 1000 && c <= 1003) || (c >= 1007 && c <= 1014) || (c >= 3000 && c <= 4999)
}
