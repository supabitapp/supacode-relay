package main

import "sync"

type frame struct {
	typ  int
	data []byte
}

type closeMsg struct {
	code   int
	reason string
}

type queue struct {
	mu       sync.Mutex
	items    []frame
	bytes    int
	maxBytes int
	maxMsgs  int
	final    *closeMsg
	wake     chan struct{}
}

func newQueue(maxBytes, maxMsgs int) *queue {
	return &queue{maxBytes: maxBytes, maxMsgs: maxMsgs, wake: make(chan struct{}, 1)}
}

func (q *queue) push(f frame) bool {
	q.mu.Lock()
	if q.final != nil {
		q.mu.Unlock()
		return true
	}
	if len(q.items) >= q.maxMsgs || q.bytes+len(f.data) > q.maxBytes {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, f)
	q.bytes += len(f.data)
	q.mu.Unlock()
	q.signal()
	return true
}

func (q *queue) finish(c closeMsg, discard bool) {
	q.mu.Lock()
	if q.final == nil {
		q.final = &c
	}
	if discard {
		clear(q.items)
		q.items = nil
		q.bytes = 0
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

func (q *queue) release(n int) {
	q.mu.Lock()
	q.bytes = max(0, q.bytes-n)
	q.mu.Unlock()
}

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
