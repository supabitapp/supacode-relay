package relay

import (
	"crypto/subtle"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/directory"
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
	ready     chan struct{}
	done      chan struct{}
	client    *peer
	hostPeer  *peer
	refs      int
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
	if s.clientSlots >= s.cfg.MaxClients || h.clientSlots >= s.cfg.MaxClientsPerHost || h.pending >= s.cfg.MaxPendingPerHost {
		s.mu.Unlock()
		s.reject(w, http.StatusServiceUnavailable, "client capacity reached")
		return
	}
	p := &pair{
		s:     s,
		refs:  1,
		id:    directory.ConnectionID(s.cfg.NodeID, randomB64(16)),
		token: randomB64(32),
		host:  h,
		ready: make(chan struct{}),
		done:  make(chan struct{}),
	}
	h.pairs[p.id] = p
	s.pairs[p.id] = p
	h.pending++
	h.clientSlots++
	s.clientSlots++
	s.pending++
	p.timer = time.AfterFunc(s.cfg.PairTimeout, p.expire)
	s.mu.Unlock()
	defer p.releaseSlot()

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "upgrade failed"})
		return
	}
	cp := s.newPeer(ws, nil, &h.bytesOut)
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
	p.refs++
	s.mu.Unlock()
	defer p.releaseSlot()

	ws, ok := s.upgrade(w, r, int64(s.cfg.MaxMessageBytes))
	if !ok {
		p.close(closeMsg{websocket.CloseInternalServerErr, "host accept failed"})
		return
	}
	hp := s.newPeer(ws, nil, &p.host.bytesIn)
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

// Keep admission ownership until every accepted socket and writer has exited.
func (p *pair) releaseSlot() {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	p.refs--
	if p.refs == 0 {
		p.s.clientSlots--
		p.host.clientSlots--
	}
}
