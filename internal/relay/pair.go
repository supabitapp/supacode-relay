package relay

import (
	"crypto/subtle"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type pairState int

const (
	pending pairState = iota
	accepting
	active
	closed
)

type pair struct {
	s         *Server
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

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
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
	if s.pending+s.active >= s.cfg.MaxClients || len(h.pairs) >= s.cfg.MaxClientsPerHost || h.pending >= s.cfg.MaxPendingPerHost {
		s.mu.Unlock()
		s.reject(w, http.StatusServiceUnavailable, "client capacity reached")
		return
	}
	p := &pair{
		s:        s,
		id:       randomB64(16),
		token:    randomB64(32),
		host:     h,
		toHost:   newQueue(s.cfg.MaxQueueBytes, s.cfg.MaxQueueMessages, s.budget),
		toClient: newQueue(s.cfg.MaxQueueBytes, s.cfg.MaxQueueMessages, s.budget),
	}
	h.pairs[p.id] = p
	h.pending++
	s.pending++
	p.timer = time.AfterFunc(s.cfg.PairTimeout, p.expire)
	s.mu.Unlock()

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
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
	h.notify(map[string]string{"type": "incoming", "connectionId": p.id, "token": p.token})
	s.mu.Unlock()

	go cp.writeLoop()
	cp.readData(p, p.toHost)
}

func (s *Server) handleAccept(w http.ResponseWriter, r *http.Request) {
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

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
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
	if p.announced {
		h.notify(map[string]string{"type": "closed", "connectionId": p.id})
	}
	client, hostPeer := p.client, p.hostPeer
	s.mu.Unlock()
	p.timer.Stop()
	p.toClient.finish(c, discard || client == nil || client == origin)
	p.toHost.finish(c, discard || hostPeer == nil || hostPeer == origin)
}
