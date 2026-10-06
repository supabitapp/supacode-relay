package relay

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/gorilla/websocket"
)

var errIngressCapacity = errors.New("ingress capacity exceeded")

type byteBudget struct {
	limit  int64
	weight int64
	used   atomic.Int64
	bytes  atomic.Int64
}

func newByteBudget(limit, weight int64) *byteBudget {
	return &byteBudget{limit: limit, weight: weight}
}

func (b *byteBudget) reserve(n int) bool {
	for {
		used := b.used.Load()
		if int64(n) > (b.limit-used)/b.weight {
			return false
		}
		if b.used.CompareAndSwap(used, used+int64(n)*b.weight) {
			b.bytes.Add(int64(n))
			return true
		}
	}
}

func (b *byteBudget) release(n int) {
	b.used.Add(-int64(n) * b.weight)
	b.bytes.Add(-int64(n))
}

func (b *byteBudget) capacity() int64 {
	return b.limit / b.weight
}

func (q *queue) read(r io.Reader, limit int) (frame, error) {
	chunkSize := min(32<<10, limit+1)
	data := make([]byte, 0, min(32<<10, limit))
	buf := make([]byte, chunkSize)
	reserved := 0
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if n > limit-len(data) {
				q.releaseReserved(reserved)
				return frame{}, websocket.ErrReadLimit
			}
			if !q.reserve(n) {
				q.releaseReserved(reserved)
				return frame{}, errIngressCapacity
			}
			data = append(data, buf[:n]...)
			reserved += n
		}
		if err == io.EOF {
			return frame{data: data, reserved: reserved}, nil
		}
		if err != nil {
			q.releaseReserved(reserved)
			return frame{}, err
		}
	}
}

func (s *Server) evictFor(q *queue, n int) bool {
	victim := s.ingressVictim(q, int64(n))
	if victim == nil {
		return false
	}
	s.ingressEvictions.Add(1)
	victim.close(closeMsg{websocket.CloseTryAgainLater, "ingress capacity exceeded"}, nil, true)
	return true
}

func (s *Server) ingressVictim(q *queue, n int64) *pair {
	s.mu.Lock()
	defer s.mu.Unlock()
	holders := int64(1)
	var victim *pair
	var most int64
	for _, p := range s.pairs {
		for _, d := range [...]*queue{p.toHost, p.toClient} {
			held := d.held.Load()
			if d == q || held == 0 {
				continue
			}
			holders++
			if held > most {
				victim, most = p, held
			}
		}
	}
	share := s.budget.capacity() / holders
	if q.held.Load()+n > share || most <= share {
		return nil
	}
	return victim
}
