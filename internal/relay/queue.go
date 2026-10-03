package relay

import "sync"

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

type queue struct {
	mu       sync.Mutex
	items    []frame
	bytes    int
	maxBytes int
	maxMsgs  int
	budget   *byteBudget
	final    *closeMsg
	wake     chan struct{}
}

func newQueue(maxBytes, maxMsgs int, budgets ...*byteBudget) *queue {
	var budget *byteBudget
	if len(budgets) != 0 {
		budget = budgets[0]
	}
	return &queue{maxBytes: maxBytes, maxMsgs: maxMsgs, budget: budget, wake: make(chan struct{}, 1)}
}

func (q *queue) push(f frame) bool {
	return q.pushResult(f).accepted()
}

func (q *queue) pushResult(f frame) pushResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.final != nil {
		q.releaseReserved(f.reserved)
		return pushFinal
	}
	if len(q.items) >= q.maxMsgs || len(f.data) > q.maxBytes-q.bytes {
		q.releaseReserved(f.reserved)
		return pushQueueLimit
	}
	if q.budget != nil && f.reserved == 0 && cap(f.data) != 0 {
		if !q.budget.reserve(cap(f.data)) {
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
	return f, nil, true
}

func (q *queue) release(f frame) {
	q.mu.Lock()
	q.bytes -= len(f.data)
	q.releaseReserved(f.reserved)
	q.mu.Unlock()
}

func (q *queue) releaseReserved(n int) {
	if q.budget != nil {
		q.budget.release(n)
	}
}

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
