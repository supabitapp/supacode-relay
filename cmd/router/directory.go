package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

const streamBuffer = 1024

type stream struct {
	nodeID  string
	session uint64
	ws      *websocket.Conn
	out     chan []byte
	done    chan struct{}

	mu      sync.Mutex
	closed  bool
	seq     uint64
	waiters map[uint64]chan struct{}
	trace   *diagnostics.Trace
}

type round struct {
	done chan struct{}
}

func newStream(nodeID string, session uint64, ws *websocket.Conn) *stream {
	return &stream{nodeID: nodeID, session: session, ws: ws, out: make(chan []byte, streamBuffer), done: make(chan struct{}), waiters: map[uint64]chan struct{}{}}
}

func (st *stream) send(m directory.Message) bool {
	data, _ := json.Marshal(m)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return false
	}
	select {
	case st.out <- data:
		return true
	default:
		st.closeLocked()
		return false
	}
}

func (st *stream) sync(clock uint64) (<-chan struct{}, func()) {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return nil, func() {}
	}
	st.seq++
	seq := st.seq
	ch := make(chan struct{})
	st.waiters[seq] = ch
	st.mu.Unlock()
	cancel := func() {
		st.mu.Lock()
		delete(st.waiters, seq)
		st.mu.Unlock()
	}
	if !st.send(directory.Message{Type: directory.TypeSync, Seq: seq, Clock: clock}) {
		cancel()
		return nil, func() {}
	}
	return ch, cancel
}

func (st *stream) ack(seq uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for s, ch := range st.waiters {
		if s <= seq {
			close(ch)
			delete(st.waiters, s)
		}
	}
}

func (st *stream) close() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.closeLocked()
}

func (st *stream) closeLocked() {
	if st.closed {
		return
	}
	st.closed = true
	close(st.done)
	for s, ch := range st.waiters {
		close(ch)
		delete(st.waiters, s)
	}
	_ = st.ws.Close()
}

func (st *stream) writeLoop(heartbeat time.Duration) {
	ping := time.NewTicker(heartbeat)
	defer ping.Stop()
	for {
		select {
		case data := <-st.out:
			_ = st.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := st.ws.WriteMessage(websocket.TextMessage, data); err != nil {
				st.trace.Failure("directory.write.failed", err)
				st.close()
				return
			}
		case <-ping.C:
			if err := st.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				st.trace.Failure("directory.ping.failed", err)
				st.close()
				return
			}
		case <-st.done:
			return
		}
	}
}

func (rt *router) authorized(r *http.Request) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(got), []byte(rt.cfg.directoryToken)) == 1
}

func validNodeAddr(addr string) bool {
	u, err := url.Parse(addr)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && (u.Path == "" || u.Path == "/") && u.RawQuery == "" && u.User == nil
}

func (rt *router) handleDirectory(w http.ResponseWriter, r *http.Request) {
	authorized := rt.authorized(r)
	trace, request := diagnostics.Request(rt.events, r, diagnostics.RequestOptions{InheritTrace: authorized})
	r = request
	w.Header().Set(diagnostics.Header, trace.ID())
	trace.Event("directory.request.begin")
	defer trace.Event("directory.request.end")
	if !authorized {
		trace.Event("directory.auth.failed", "status", http.StatusUnauthorized)
		rt.directoryRejected.Add(1)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	ws, err := rt.upgrader.Upgrade(w, r, http.Header{diagnostics.Header: {trace.ID()}})
	if err != nil {
		trace.Failure("directory.upgrade.failed", err)
		return
	}
	defer ws.Close()
	ws.SetReadLimit(directoryReadLimit)
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hello directory.Message
	if err := ws.ReadJSON(&hello); err != nil || hello.Type != directory.TypeHello || !directory.ValidNodeID(hello.NodeID) || !validNodeAddr(hello.Addr) {
		trace.Failure("directory.hello.failed", err, "reason", "invalid_hello")
		rt.directoryRejected.Add(1)
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "invalid hello"), time.Now().Add(time.Second))
		return
	}
	session := rt.sessions.Add(1)
	st := newStream(hello.NodeID, session, ws)
	st.trace = trace.With("directory_node", hello.NodeID, "directory_session", session)
	st.send(directory.Message{Type: directory.TypeWelcome, Clock: rt.table.Clock()})
	rt.mu.Lock()
	old := rt.streams[hello.NodeID]
	rt.streams[hello.NodeID] = st
	replaced := rt.table.Join(hello.NodeID, strings.TrimSuffix(hello.Addr, "/"), session, hello.Draining)
	rt.mu.Unlock()
	if old != nil {
		old.close()
	}
	st.trace.Event("directory.node.joined", "replaced_session", replaced, "draining", hello.Draining)
	rt.log.Printf("router: node %s joined (session %d, replaced %d, draining %v)", hello.NodeID, session, replaced, hello.Draining)
	go st.writeLoop(rt.cfg.heartbeat)

	defer func() {
		rt.mu.Lock()
		if rt.streams[hello.NodeID] == st {
			delete(rt.streams, hello.NodeID)
		}
		rt.mu.Unlock()
		left := rt.table.Leave(hello.NodeID, session, time.Now())
		st.close()
		st.trace.Event("directory.node.left", "purged", left)
		rt.log.Printf("router: node %s left (session %d, purged %v)", hello.NodeID, session, left)
	}()

	timeout := 2 * rt.cfg.heartbeat
	extend := func() { _ = ws.SetReadDeadline(time.Now().Add(timeout)) }
	ws.SetPongHandler(func(string) error { extend(); return nil })
	extend()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			st.trace.Failure("directory.read.ended", err, "close_code", diagnostics.CloseCode(err))
			return
		}
		extend()
		var m directory.Message
		if json.Unmarshal(data, &m) != nil {
			st.trace.Event("directory.message.invalid", "bytes", len(data))
			return
		}
		switch m.Type {
		case directory.TypeSnapshot:
			ev, accepted := rt.table.Snapshot(hello.NodeID, session, m.Registrations)
			st.trace.Event("directory.snapshot", "registrations", len(m.Registrations), "accepted", accepted, "evictions", len(ev))
			rt.evict(ev)
		case directory.TypePut:
			if m.Registration != nil {
				ev, accepted := rt.table.Put(hello.NodeID, session, *m.Registration)
				st.trace.Event("directory.endpoint.put", "endpoint_tag", diagnostics.Tag(m.Registration.EndpointID), "accepted", accepted, "evictions", len(ev))
				rt.evict(ev)
			}
		case directory.TypeDel:
			if m.Registration != nil {
				removed := rt.table.Del(hello.NodeID, session, *m.Registration)
				st.trace.Event("directory.endpoint.deleted", "endpoint_tag", diagnostics.Tag(m.Registration.EndpointID), "removed", removed)
			}
		case directory.TypeDrain:
			if rt.table.SetDraining(hello.NodeID, session) {
				st.trace.Event("directory.node.draining")
				rt.log.Printf("router: node %s draining", hello.NodeID)
			}
		case directory.TypeSynced:
			st.ack(m.Seq)
			st.trace.Event("directory.sync.acknowledged", "sequence", m.Seq)
		default:
			st.trace.Event("directory.message.unknown", "bytes", len(data))
		}
	}
}

func (rt *router) evict(evs []directory.Eviction) {
	for _, ev := range evs {
		rt.mu.Lock()
		st := rt.streams[ev.NodeID]
		rt.mu.Unlock()
		if st == nil {
			continue
		}
		reg := ev.Registration
		if st.send(directory.Message{Type: directory.TypeEvict, Registration: &reg, Clock: ev.Winner}) {
			st.trace.Event("directory.eviction.sent", "endpoint_tag", diagnostics.Tag(reg.EndpointID), "winner_version", ev.Winner)
			rt.evictions.Add(1)
		}
	}
}

func (rt *router) refresh(ctx context.Context) {
	started := time.Now()
	state, _ := ctx.Value(stateKey{}).(*proxyState)
	rt.refreshes.Add(1)
	rt.mu.Lock()
	rd := rt.nextRound
	if rd == nil {
		rd = &round{done: make(chan struct{})}
		rt.nextRound = rd
		if !rt.syncing {
			rt.syncing = true
			go rt.runRounds()
		}
	}
	rt.mu.Unlock()
	select {
	case <-rd.done:
		if state != nil {
			state.trace.Event("directory.refresh.finished", "refresh_ms", time.Since(started).Milliseconds(), "registered_endpoints", rt.table.Len())
		}
	case <-ctx.Done():
		if state != nil {
			state.trace.Failure("directory.refresh.cancelled", ctx.Err(), "refresh_ms", time.Since(started).Milliseconds())
		}
	}
}

func (rt *router) runRounds() {
	for {
		rt.mu.Lock()
		rd := rt.nextRound
		if rd == nil {
			rt.syncing = false
			rt.mu.Unlock()
			return
		}
		rt.nextRound = nil
		streams := make([]*stream, 0, len(rt.streams))
		for _, st := range rt.streams {
			streams = append(streams, st)
		}
		rt.mu.Unlock()
		rt.syncRound(streams)
		close(rd.done)
	}
}

func (rt *router) syncRound(streams []*stream) {
	rt.rounds.Add(1)
	trace := diagnostics.New(rt.events, "directory")
	trace.Event("directory.sync.started", "streams", len(streams), "timeout_ms", rt.cfg.refreshTimeout.Milliseconds())
	timer := time.NewTimer(rt.cfg.refreshTimeout)
	defer timer.Stop()
	clock := rt.table.Clock()
	var waits []<-chan struct{}
	for _, st := range streams {
		ch, cancel := st.sync(clock)
		defer cancel()
		if ch != nil {
			waits = append(waits, ch)
		}
	}
	for _, ch := range waits {
		select {
		case <-ch:
		case <-timer.C:
			rt.roundTimeouts.Add(1)
			trace.Event("directory.sync.timeout", "streams", len(streams), "awaited_streams", len(waits))
			return
		}
	}
	trace.Event("directory.sync.finished", "streams", len(streams))
}

func (rt *router) closeStreams() {
	rt.mu.Lock()
	streams := make([]*stream, 0, len(rt.streams))
	for _, st := range rt.streams {
		streams = append(streams, st)
	}
	rt.mu.Unlock()
	msg := websocket.FormatCloseMessage(websocket.CloseGoingAway, "router shutting down")
	for _, st := range streams {
		_ = st.ws.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
		st.close()
	}
}
