package relay

import "testing"

func TestDecodeB64Canonical(t *testing.T) {
	if _, ok := decodeB64("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 32); !ok {
		t.Fatal("canonical rejected")
	}
	for _, s := range []string{"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "AAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAA"} {
		if _, ok := decodeB64(s, 32); ok {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestControlQueueKeepsInFlightBytesUntilWriteEnds(t *testing.T) {
	q := newQueue(8, 2)
	if !q.push(frame{data: []byte("1234")}) || !q.push(frame{data: []byte("5678")}) {
		t.Fatal("control queue rejected available capacity")
	}
	writing, _, ok := q.next()
	if !ok {
		t.Fatal("missing control message")
	}
	q.finish(closeMsg{code: 1001}, true)
	if q.bytes != 4 {
		t.Fatalf("held %d bytes, want the in-flight write", q.bytes)
	}
	q.release(writing)
	if q.bytes != 0 {
		t.Fatalf("write completed with %d bytes held", q.bytes)
	}
}

func TestHostNotificationsQueueOrCloseControl(t *testing.T) {
	for _, state := range []string{"open", "full", "finished", "gone"} {
		t.Run(state, func(t *testing.T) {
			q := newQueue(1024, 1)
			h := &host{ctrl: &peer{q: q}, gone: state == "gone"}
			switch state {
			case "full":
				q.push(textFrame(map[string]string{"type": "registered"}))
			case "finished":
				q.finish(closeMsg{code: 1001}, true)
			}
			h.notify(map[string]string{"type": "incoming"})
			_, final, queued := q.next()
			if queued != (state == "open") {
				t.Fatalf("%s control queue: notification queued=%v", state, queued)
			}
			if state == "full" && (final == nil || final.reason != "control queue limit exceeded") {
				t.Fatal("overloaded host control did not close")
			}
		})
	}
}
