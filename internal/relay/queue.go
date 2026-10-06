package relay

import (
	"sync"
	"sync/atomic"
)

type frame struct {
	typ      int
	data     []byte
	reserved int
}

type closeMsg struct {
	code   int
	reason string
}

type pushResult uint8

const (
	pushAccepted pushResult = iota
	pushFinal
	pushQueueLimit
	pushBudgetLimit
)

func (r pushResult) accepted() bool {
	return r == pushAccepted || r == pushFinal
}

const maxIngressEvictions = 3

type queue struct {
	mu       sync.Mutex
	items    []frame
	bytes    int
	maxBytes int
	maxMsgs  int
	budget   *byteBudget
	arbiter  *Server
	held     atomic.Int64
	final    *closeMsg
	wake     chan struct{}
	space    *sync.Cond
}

func newQueue(maxBytes, maxMsgs int, budgets ...*byteBudget) *queue {
	var budget *byteBudget
	if len(budgets) != 0 {
		budget = budgets[0]
	}
	q := &queue{maxBytes: maxBytes, maxMsgs: maxMsgs, budget: budget, wake: make(chan struct{}, 1)}
	q.space = sync.NewCond(&q.mu)
	return q
}

func (q *queue) push(f frame) bool {
	return q.pushResult(f).accepted()
}

func (q *queue) pushResult(f frame) pushResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.final == nil && q.full(len(f.data)) {
		q.releaseReserved(f.reserved)
		return pushQueueLimit
	}
	return q.appendLocked(f)
}

func (q *queue) pushWait(f frame) pushResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.final == nil && q.full(len(f.data)) {
		q.space.Wait()
	}
	return q.appendLocked(f)
}

func (q *queue) full(n int) bool {
	return len(q.items) >= q.maxMsgs || (q.bytes > 0 && n > q.maxBytes-q.bytes)
}

func (q *queue) appendLocked(f frame) pushResult {
	if q.final != nil {
		q.releaseReserved(f.reserved)
		return pushFinal
	}
	if q.budget != nil && f.reserved == 0 && cap(f.data) != 0 {
		if !q.take(cap(f.data)) {
			return pushBudgetLimit
		}
		f.reserved = cap(f.data)
	}
	q.items = append(q.items, f)
	q.bytes += len(f.data)
	q.signal()
	return pushAccepted
}

func (q *queue) finish(c closeMsg, discard bool) {
	q.mu.Lock()
	if q.final == nil {
		q.final = &c
	}
	if discard {
		for _, f := range q.items {
			q.bytes -= len(f.data)
			q.releaseReserved(f.reserved)
		}
		clear(q.items)
		q.items = nil
	}
	q.space.Broadcast()
	q.mu.Unlock()
	q.signal()
}

func (q *queue) next() (frame, *closeMsg, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return frame{}, q.final, false
	}
	f := q.items[0]
	q.items[0] = frame{}
	q.items = q.items[1:]
	q.space.Broadcast()
	return f, nil, true
}

func (q *queue) release(f frame) {
	q.mu.Lock()
	q.bytes -= len(f.data)
	q.releaseReserved(f.reserved)
	q.space.Broadcast()
	q.mu.Unlock()
}

func (q *queue) take(n int) bool {
	if q.budget == nil {
		return true
	}
	if !q.budget.reserve(n) {
		return false
	}
	q.held.Add(int64(n))
	return true
}

func (q *queue) reserve(n int) bool {
	for range maxIngressEvictions {
		if q.take(n) {
			return true
		}
		if q.arbiter == nil || !q.arbiter.evictFor(q, n) {
			return false
		}
	}
	return q.take(n)
}

func (q *queue) releaseReserved(n int) {
	if q.budget != nil {
		q.budget.release(n)
		q.held.Add(-int64(n))
	}
}

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
