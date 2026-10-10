package relay

import (
	"bytes"
	"slices"
	"testing"
)

func FuzzControlQueue(f *testing.F) {
	f.Add([]byte{8, 2, 0, 4, 0, 4, 1, 0, 4, 0, 2, 0})
	f.Add([]byte{1, 1, 0, 0, 1, 0, 3, 0, 0, 1, 2, 0})
	f.Add([]byte{0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		maxBytes, maxMessages := int(data[0])+1, int(data[1]%8)+1
		q := newQueue(maxBytes, maxMessages)
		var queued []frame
		var writing *frame
		var final *closeMsg
		heldBytes := func() int {
			n := 0
			for _, item := range queued {
				n += len(item.data)
			}
			if writing != nil {
				n += len(writing.data)
			}
			return n
		}
		for i := 2; i+1 < min(len(data), 258); i += 2 {
			switch data[i] % 5 {
			case 0:
				item := frame{typ: int(data[i+1]%2) + 1, data: bytes.Repeat([]byte{byte(i)}, int(data[i+1]))}
				want := "queued"
				if final != nil {
					want = "queue_finished"
				} else if len(queued)+1 > maxMessages || heldBytes()+len(item.data) > maxBytes {
					want = "queue_full"
				}
				if got := q.enqueue(item); got != want {
					t.Fatalf("step %d: enqueue returned %q, want %q", i/2, got, want)
				}
				if want == "queued" {
					queued = append(queued, item)
				}
			case 1:
				if writing != nil {
					continue
				}
				item, closing, ok := q.next()
				if ok != (len(queued) > 0) {
					t.Fatalf("step %d: dequeue availability disagrees with model", i/2)
				}
				if ok {
					want := queued[0]
					if item.typ != want.typ || !bytes.Equal(item.data, want.data) || closing != nil {
						t.Fatalf("step %d: dequeue changed message order, type, or bytes", i/2)
					}
					queued = queued[1:]
					writing = &item
				} else if closing != final {
					if closing == nil || final == nil || *closing != *final {
						t.Fatalf("step %d: incorrect terminal close", i/2)
					}
				}
			case 2:
				if writing != nil {
					q.release(*writing)
					writing = nil
				}
			case 3, 4:
				close := closeMsg{code: 1000 + int(data[i+1]), reason: "finished"}
				discard := data[i]%5 == 4
				q.finish(close, discard)
				if final == nil {
					final = &close
				}
				if discard {
					queued = nil
				}
			}
			if q.bytes != heldBytes() || q.bytes < 0 || q.bytes > maxBytes || len(q.items) != len(queued) {
				t.Fatalf("step %d: retained queue resources disagree with model: bytes=%d want=%d", i/2, q.bytes, heldBytes())
			}
			if !slices.EqualFunc(q.items, queued, func(a, b frame) bool {
				return a.typ == b.typ && bytes.Equal(a.data, b.data)
			}) {
				t.Fatalf("step %d: queued messages disagree with model", i/2)
			}
		}
		q.finish(closeMsg{code: 1001}, true)
		if writing != nil {
			q.release(*writing)
		}
		if q.bytes != 0 || len(q.items) != 0 {
			t.Fatalf("cleanup retained %d bytes and %d messages", q.bytes, len(q.items))
		}
	})
}
