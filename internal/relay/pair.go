package relay

import (
	"crypto/subtle"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
)

type pairState int

const (
	pending pairState = iota
	accepting
	active
	closed
)

func (state pairState) String() string {
	return [...]string{"pending", "accepting", "active", "closed"}[state]
}

type pair struct {
	s          *Server
	id         string
	token      string
	host       *host
	state      pairState
	closedFrom pairState
	cause      closeMsg
	announced  bool
	timer      *time.Timer
	ready      chan struct{}
	done       chan struct{}
	client     *peer
	hostPeer   *peer
	refs       int
	trace      *diagnostics.Trace
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, false) {
		return
	}
	id := r.URL.Query().Get("endpointId")
	s.mu.Lock()
	h := s.hosts[id]
	if h == nil {
		s.mu.Unlock()
		s.reject(w, r, http.StatusNotFound, "endpoint not found")
		return
	}
	if s.clientSlots >= s.cfg.MaxClients || h.clientSlots >= s.cfg.MaxClientsPerHost || h.pending >= s.cfg.MaxPendingPerHost {
		total, hostSlots, hostPending := s.clientSlots, h.clientSlots, h.pending
		s.mu.Unlock()
		diagnostics.From(r).Event("pair.capacity.reached",
			"current_clients", total, "max_clients", s.cfg.MaxClients,
			"current_host_clients", hostSlots, "max_host_clients", s.cfg.MaxClientsPerHost,
			"current_host_pending", hostPending, "max_host_pending", s.cfg.MaxPendingPerHost)
		s.reject(w, r, http.StatusServiceUnavailable, "client capacity reached")
		return
	}
	p := &pair{
		s:     s,
		refs:  1,
		id:    randomB64(16),
		token: randomB64(32),
		host:  h,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
	p.trace = diagnostics.From(r).With("pair_tag", diagnostics.Tag(p.id))
	h.pairs[p.id] = p
	s.pairs[p.id] = p
	h.pending++
	h.clientSlots++
	s.clientSlots++
	s.pending++
	p.timer = time.AfterFunc(s.cfg.PairTimeout, p.expire)
	s.mu.Unlock()
	p.trace.Event("pair.created", "pair_timeout_ms", s.cfg.PairTimeout.Milliseconds(), "control_trace_id", h.trace.ID())
	defer p.releaseSlot()

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "upgrade failed"})
		return
	}
	cp := s.newPeer(ws, nil, &h.bytesOut)
	cp.trace = p.trace.With("peer", "client")
	s.mu.Lock()
	if p.state == closed {
		cause := p.cause
		s.mu.Unlock()
		s.closeNow(ws, cause)
		return
	}
	p.client = cp
	p.announced = true
	outcome := h.notify(map[string]string{"type": "incoming", "connectionId": p.id, "token": p.token})
	s.mu.Unlock()
	p.trace.Event("pair.host.notification", "outcome", outcome)

	cp.readData(p)
}

func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
	if !s.admit(w, r, true) {
		return
	}
	q := r.URL.Query()
	s.mu.Lock()
	p := s.pairs[q.Get("connectionId")]
	if p != nil && p.host.id != q.Get("endpointId") {
		p = nil
	}
	if p == nil {
		s.mu.Unlock()
		s.reject(w, r, http.StatusNotFound, "connection not found")
		return
	}
	token := q.Get("token")
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(p.token)) != 1 || p.state != pending || p.client == nil {
		tag, clientTrace, phase := diagnostics.Tag(p.id), p.trace.ID(), p.state
		s.mu.Unlock()
		diagnostics.From(r).Event("pair.accept.rejected", "pair_tag", tag, "client_trace_id", clientTrace,
			"phase", phase.String())
		s.reject(w, r, http.StatusForbidden, "invalid token")
		return
	}
	p.state = accepting
	p.refs++
	s.mu.Unlock()
	trace := diagnostics.From(r).With("pair_tag", diagnostics.Tag(p.id), "client_trace_id", p.trace.ID())
	trace.Event("pair.accept.authorized")
	defer p.releaseSlot()

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "host accept failed"})
		return
	}
	hp := s.newPeer(ws, nil, &p.host.bytesIn)
	hp.trace = trace.With("peer", "host")
	s.mu.Lock()
	if p.state != accepting {
		cause := p.cause
		s.mu.Unlock()
		s.closeNow(ws, cause)
		return
	}
	p.state = active
	p.timer.Stop()
	p.hostPeer = hp
	p.host.pending--
	s.pending--
	s.active++
	close(p.ready)
	s.mu.Unlock()
	p.trace.Event("pair.active", "accept_trace_id", trace.ID())

	hp.readData(p)
}

func (p *pair) expire() {
	p.closeWhen(closeMsg{websocket.CloseTryAgainLater, "pair timeout"}, true)
}

func (p *pair) close(c closeMsg) {
	p.closeWhen(c, false)
}

func (p *pair) closeWhen(c closeMsg, waitingOnly bool) {
	s := p.s
	s.mu.Lock()
	prev := p.state
	if waitingOnly && prev != pending && prev != accepting {
		s.mu.Unlock()
		return
	}
	if prev == closed {
		s.mu.Unlock()
		<-p.done
		return
	}
	p.state = closed
	p.closedFrom = prev
	p.cause = c
	h := p.host
	delete(h.pairs, p.id)
	delete(s.pairs, p.id)
	if prev == active {
		s.active--
	} else {
		h.pending--
		s.pending--
	}
	if p.announced {
		h.notify(map[string]string{"type": "closed", "connectionId": p.id})
	}
	client, hostPeer := p.client, p.hostPeer
	s.mu.Unlock()
	p.trace.Event("pair.closing", "phase", prev.String(), "close_code", c.code, "reason", diagnostics.CloseReason(c.reason))
	p.timer.Stop()
	// Close controls may run alongside a data writer. One shared deadline
	// bounds teardown even when either destination has stopped reading.
	deadline := time.Now().Add(closeGrace)
	var cleanup sync.WaitGroup
	for _, peer := range []*peer{client, hostPeer} {
		if peer == nil {
			continue
		}
		cleanup.Add(1)
		go func() {
			defer cleanup.Done()
			_ = peer.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(c.code, c.reason), deadline)
			_ = peer.ws.Close()
		}()
	}
	cleanup.Wait()
	close(p.done)
}

func (p *pair) logClosed() {
	var clientBytes, hostBytes, clientMessages, hostMessages int64
	if p.client != nil {
		clientBytes, clientMessages = p.client.bytes.Load(), p.client.messages.Load()
	}
	if p.hostPeer != nil {
		hostBytes, hostMessages = p.hostPeer.bytes.Load(), p.hostPeer.messages.Load()
	}
	p.trace.Event("pair.closed", "phase", p.closedFrom.String(),
		"close_code", p.cause.code, "reason", diagnostics.CloseReason(p.cause.reason),
		"bytes_to_host", clientBytes, "bytes_to_client", hostBytes,
		"messages_to_host", clientMessages, "messages_to_client", hostMessages)
}

// Keep admission ownership until every accepted socket and writer has exited.
func (p *pair) releaseSlot() {
	p.s.mu.Lock()
	p.refs--
	last := p.refs == 0
	if last {
		p.s.clientSlots--
		p.host.clientSlots--
	}
	p.s.mu.Unlock()
	if last {
		p.logClosed()
	}
}
