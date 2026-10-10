package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

const (
	dirQueueBytes    = 16 << 20
	dirQueueMessages = 1 << 16
	dirReadLimit     = 1 << 20
	dirLogInterval   = 30 * time.Second
)

type dirStream struct {
	ws   *websocket.Conn
	q    *queue
	dead chan struct{}
}

func (s *Server) publishLocked(m directory.Message) {
	if len(s.dirStreams) == 0 {
		return
	}
	f := textFrame(m)
	for st := range s.dirStreams {
		if !st.q.push(f) {
			st.q.finish(closeMsg{websocket.CloseTryAgainLater, "directory queue limit exceeded"}, true)
		}
	}
}

func (s *Server) beginDrain() {
	s.draining.Store(true)
	if !s.cfg.Clustered() {
		return
	}
	s.mu.Lock()
	s.publishLocked(directory.Message{Type: directory.TypeDrain})
	hosts := make([]*host, 0, len(s.hosts))
	for id, h := range s.hosts {
		delete(s.hosts, id)
		h.detached = true
		s.publishLocked(directory.Message{Type: directory.TypeDel, Registration: s.registration(h)})
		hosts = append(hosts, h)
	}
	s.mu.Unlock()
	for _, h := range hosts {
		h.ctrl.q.finish(closeMsg{websocket.CloseGoingAway, "relay draining"}, true)
	}
	log.Printf("relay: released %d hosts to other nodes", len(hosts))
}

func (s *Server) evict(m directory.Message) {
	s.clock.Observe(m.Clock)
	r := m.Registration
	if r == nil {
		return
	}
	s.mu.Lock()
	h := s.hosts[r.EndpointID]
	if h == nil || h.regID != r.RegistrationID {
		s.mu.Unlock()
		return
	}
	delete(s.hosts, h.id)
	s.mu.Unlock()
	s.evicted.Add(1)
	h.trace.Event("host.evicted", "winner_version", m.Clock)
	h.ctrl.q.finish(closeMsg{directory.CloseSuperseded, directory.SupersededText}, true)
}

func routerName(target string) string {
	u, err := url.Parse(target)
	if err != nil {
		return "router"
	}
	return u.Host
}

func (s *Server) RunDirectory(ctx context.Context) {
	for _, target := range s.cfg.Routers {
		go s.runDirectory(ctx, target)
	}
}

func (s *Server) runDirectory(ctx context.Context, target string) {
	name := routerName(target)
	backoff := 100 * time.Millisecond
	failures := 0
	var lastLog time.Time
	for {
		started := time.Now()
		trace := diagnostics.New(s.events, "directory").With("router_tag", diagnostics.Tag(target))
		trace.Event("directory.connecting", "attempt", failures+1)
		established, err := s.directorySession(ctx, target, name, trace)
		trace.Failure("directory.ended", err, "established", established)
		if ctx.Err() != nil {
			return
		}
		if established || time.Since(started) > 5*time.Second {
			backoff = 100 * time.Millisecond
			failures = 0
		}
		failures++
		if failures == 1 || time.Since(lastLog) > dirLogInterval {
			log.Printf("relay: directory stream to %s unavailable (attempt %d): %v", name, failures, err)
			lastLog = time.Now()
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		trace.Event("directory.retry.scheduled", "wait_ms", wait.Milliseconds(), "attempt", failures)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = min(2*backoff, s.cfg.DirectoryRetryMax)
	}
}

func (s *Server) directorySession(ctx context.Context, target, name string, trace *diagnostics.Trace) (bool, error) {
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second, ReadBufferSize: 4096, WriteBufferSize: 4096}
	ws, resp, err := d.DialContext(ctx, target, http.Header{"Authorization": {"Bearer " + s.cfg.DirectoryToken}, diagnostics.Header: {trace.ID()}})
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		trace.Failure("directory.upgrade.failed", err, "status", status)
		if resp != nil {
			return false, fmt.Errorf("handshake status %d", resp.StatusCode)
		}
		return false, err
	}
	trace.Event("directory.socket.upgraded")
	defer ws.Close()
	ws.SetReadLimit(dirReadLimit)
	_ = ws.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	hello := directory.Message{Type: directory.TypeHello, NodeID: s.cfg.NodeID, Addr: s.cfg.AdvertiseURL, Draining: s.draining.Load()}
	if err := ws.WriteJSON(hello); err != nil {
		trace.Failure("directory.hello.failed", err)
		return false, err
	}
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var welcome directory.Message
	if err := ws.ReadJSON(&welcome); err != nil || welcome.Type != directory.TypeWelcome {
		trace.Failure("directory.welcome.failed", err, "reason", "invalid_welcome")
		return false, fmt.Errorf("expected welcome: %v", err)
	}
	s.clock.Observe(welcome.Clock)

	st := &dirStream{ws: ws, q: newQueue(dirQueueBytes, dirQueueMessages), dead: make(chan struct{})}
	s.mu.Lock()
	regs := make([]directory.Registration, 0, len(s.hosts))
	for _, h := range s.hosts {
		regs = append(regs, *s.registration(h))
	}
	st.q.push(textFrame(directory.Message{Type: directory.TypeSnapshot, Registrations: regs}))
	if s.draining.Load() {
		st.q.push(textFrame(directory.Message{Type: directory.TypeDrain}))
	}
	s.dirStreams[st] = struct{}{}
	s.mu.Unlock()
	trace.Event("directory.snapshot.queued", "registrations", len(regs))
	log.Printf("relay: directory stream to %s established with %d registrations", name, len(regs))
	defer func() {
		s.mu.Lock()
		delete(s.dirStreams, st)
		s.mu.Unlock()
	}()

	stop := context.AfterFunc(ctx, func() {
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "relay shutting down"), time.Now().Add(time.Second))
		_ = ws.Close()
	})
	defer stop()
	go st.writeLoop(s.cfg.WriteTimeout, s.cfg.DirectoryHeartbeat)
	defer close(st.dead)

	timeout := 2 * s.cfg.DirectoryHeartbeat
	extend := func() { _ = ws.SetReadDeadline(time.Now().Add(timeout)) }
	ws.SetPongHandler(func(string) error { extend(); return nil })
	extend()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return true, err
		}
		extend()
		var m directory.Message
		if err := json.Unmarshal(data, &m); err != nil {
			return true, fmt.Errorf("invalid directory message: %w", err)
		}
		switch m.Type {
		case directory.TypeSync:
			s.clock.Observe(m.Clock)
			if !st.q.push(textFrame(directory.Message{Type: directory.TypeSynced, Seq: m.Seq})) {
				return true, fmt.Errorf("directory queue limit exceeded")
			}
		case directory.TypeEvict:
			s.evict(m)
		}
	}
}

func (st *dirStream) writeLoop(writeTimeout, heartbeat time.Duration) {
	ping := time.NewTicker(heartbeat)
	defer ping.Stop()
	sendPing := func() bool {
		if err := st.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
			_ = st.ws.Close()
			return false
		}
		return true
	}
	for {
		f, final, ok := st.q.next()
		if ok {
			_ = st.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := st.ws.WriteMessage(f.typ, f.data); err != nil {
				_ = st.ws.Close()
				return
			}
			st.q.release(f)
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
			_ = st.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(final.code, final.reason), time.Now().Add(writeTimeout))
			_ = st.ws.Close()
			return
		}
		select {
		case <-st.q.wake:
		case <-ping.C:
			if !sendPing() {
				return
			}
		case <-st.dead:
			return
		}
	}
}
