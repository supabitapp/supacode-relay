package directory

import (
	"sort"
	"sync"
	"time"
)

type Node struct {
	ID       string
	Addr     string
	Session  uint64
	Draining bool
	Ready    bool
	Entries  int
}

type Eviction struct {
	NodeID       string
	Registration Registration
	Winner       uint64
}

type Table struct {
	mu       sync.RWMutex
	grace    time.Duration
	nodes    map[string]*Node
	departed map[string]departed
	entries  map[string]Registration
	clock    uint64
}

type departed struct {
	addr  string
	until time.Time
}

func NewTable(acceptGrace time.Duration) *Table {
	return &Table{
		grace:    acceptGrace,
		nodes:    map[string]*Node{},
		departed: map[string]departed{},
		entries:  map[string]Registration{},
	}
}

func (t *Table) Join(id, addr string, session uint64, draining bool) (replaced uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old := t.nodes[id]; old != nil {
		replaced = old.Session
		t.purge(id)
	}
	delete(t.departed, id)
	t.nodes[id] = &Node{ID: id, Addr: addr, Session: session, Draining: draining}
	return replaced
}

func (t *Table) Leave(id string, session uint64, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.current(id, session)
	if n == nil {
		return false
	}
	t.purge(id)
	delete(t.nodes, id)
	t.departed[id] = departed{addr: n.Addr, until: now.Add(t.grace)}
	for k, d := range t.departed {
		if now.After(d.until) {
			delete(t.departed, k)
		}
	}
	return true
}

func (t *Table) Snapshot(id string, session uint64, regs []Registration) ([]Eviction, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.current(id, session)
	if n == nil {
		return nil, false
	}
	var ev []Eviction
	for _, r := range regs {
		ev = append(ev, t.put(n, r)...)
	}
	n.Ready = true
	return ev, true
}

func (t *Table) Put(id string, session uint64, reg Registration) ([]Eviction, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.current(id, session)
	if n == nil {
		return nil, false
	}
	return t.put(n, reg), true
}

func (t *Table) Del(id string, session uint64, reg Registration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.current(id, session)
	if n == nil {
		return false
	}
	cur, ok := t.entries[reg.EndpointID]
	if !ok || cur.NodeID != id || cur.RegistrationID != reg.RegistrationID {
		return false
	}
	delete(t.entries, reg.EndpointID)
	n.Entries--
	return true
}

func (t *Table) SetDraining(id string, session uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.current(id, session)
	if n == nil {
		return false
	}
	n.Draining = true
	return true
}

func (t *Table) Lookup(endpointID string) (Node, Registration, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	r, ok := t.entries[endpointID]
	if !ok {
		return Node{}, Registration{}, false
	}
	n := t.nodes[r.NodeID]
	if n == nil {
		return Node{}, Registration{}, false
	}
	return *n, r, true
}

func (t *Table) NodeAddr(id string, now time.Time) (string, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n := t.nodes[id]; n != nil {
		return n.Addr, true
	}
	if d, ok := t.departed[id]; ok && !now.After(d.until) {
		return d.addr, true
	}
	return "", false
}

func (t *Table) Candidates() []Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []Node
	for _, n := range t.nodes {
		if n.Ready && !n.Draining {
			out = append(out, *n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (t *Table) Nodes() []Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Node, 0, len(t.nodes))
	for _, n := range t.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries)
}

func (t *Table) Clock() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.clock
}

func (t *Table) current(id string, session uint64) *Node {
	n := t.nodes[id]
	if n == nil || n.Session != session {
		return nil
	}
	return n
}

func (t *Table) purge(id string) {
	for k, r := range t.entries {
		if r.NodeID == id {
			delete(t.entries, k)
		}
	}
	if n := t.nodes[id]; n != nil {
		n.Entries = 0
	}
}

func (t *Table) put(n *Node, reg Registration) []Eviction {
	reg.NodeID = n.ID
	t.clock = max(t.clock, reg.Version)
	cur, ok := t.entries[reg.EndpointID]
	if !ok {
		t.entries[reg.EndpointID] = reg
		n.Entries++
		return nil
	}
	if cur.NodeID == reg.NodeID {
		if Newer(reg, cur) {
			t.entries[reg.EndpointID] = reg
		}
		return nil
	}
	if !Newer(reg, cur) {
		return []Eviction{{NodeID: reg.NodeID, Registration: reg, Winner: cur.Version}}
	}
	t.entries[reg.EndpointID] = reg
	if prev := t.nodes[cur.NodeID]; prev != nil {
		prev.Entries--
	}
	n.Entries++
	return []Eviction{{NodeID: cur.NodeID, Registration: cur, Winner: reg.Version}}
}
