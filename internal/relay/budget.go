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

func (b *byteBudget) read(r io.Reader, limit int) (frame, error) {
	chunkSize := min(32<<10, limit+1)
	data := make([]byte, 0, min(32<<10, limit))
	buf := make([]byte, chunkSize)
	reserved := 0
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if n > limit-len(data) {
				b.release(reserved)
				return frame{}, websocket.ErrReadLimit
			}
			if !b.reserve(n) {
				b.release(reserved)
				return frame{}, errIngressCapacity
			}
			data = append(data, buf[:n]...)
			reserved += n
		}
		if err == io.EOF {
			return frame{data: data, reserved: reserved}, nil
		}
		if err != nil {
			b.release(reserved)
			return frame{}, err
		}
	}
}
