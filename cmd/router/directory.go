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
				st.close()
				return
			}
		case <-ping.C:
			if err := st.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
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
	if !rt.authorized(r) {
		rt.directoryRejected.Add(1)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	ws, err := rt.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	ws.SetReadLimit(directoryReadLimit)
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hello directory.Message
	if err := ws.ReadJSON(&hello); err != nil || hello.Type != directory.TypeHello || !directory.ValidNodeID(hello.NodeID) || !validNodeAddr(hello.Addr) {
		rt.directoryRejected.Add(1)
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "invalid hello"), time.Now().Add(time.Second))
		return
	}
	session := rt.sessions.Add(1)
	st := newStream(hello.NodeID, session, ws)
	st.send(directory.Message{Type: directory.TypeWelcome, Clock: rt.table.Clock()})
	rt.mu.Lock()
	old := rt.streams[hello.NodeID]
	rt.streams[hello.NodeID] = st
	replaced := rt.table.Join(hello.NodeID, strings.TrimSuffix(hello.Addr, "/"), session, hello.Draining)
	rt.mu.Unlock()
	if old != nil {
		old.close()
	}
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
		rt.log.Printf("router: node %s left (session %d, purged %v)", hello.NodeID, session, left)
	}()

	timeout := 2 * rt.cfg.heartbeat
	extend := func() { _ = ws.SetReadDeadline(time.Now().Add(timeout)) }
	ws.SetPongHandler(func(string) error { extend(); return nil })
	extend()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		extend()
		var m directory.Message
		if json.Unmarshal(data, &m) != nil {
			return
		}
		switch m.Type {
		case directory.TypeSnapshot:
			ev, _ := rt.table.Snapshot(hello.NodeID, session, m.Registrations)
			rt.evict(ev)
		case directory.TypePut:
			if m.Registration != nil {
				ev, _ := rt.table.Put(hello.NodeID, session, *m.Registration)
				rt.evict(ev)
			}
		case directory.TypeDel:
			if m.Registration != nil {
				rt.table.Del(hello.NodeID, session, *m.Registration)
			}
		case directory.TypeDrain:
			if rt.table.SetDraining(hello.NodeID, session) {
				rt.log.Printf("router: node %s draining", hello.NodeID)
			}
		case directory.TypeSynced:
			st.ack(m.Seq)
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
			rt.evictions.Add(1)
		}
	}
}

func (rt *router) refresh(ctx context.Context) {
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
	case <-ctx.Done():
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
			return
		}
	}
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
