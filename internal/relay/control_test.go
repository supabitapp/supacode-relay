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
	for _, tc := range []struct {
		name      string
		gone      bool
		prepare   func(*queue)
		queued    bool
		closeWith string
	}{
		{name: "open queue", queued: true},
		{name: "full queue", prepare: func(q *queue) { q.push(textFrame(map[string]string{"type": "registered"})) }, closeWith: "control queue limit exceeded"},
		{name: "finished queue", prepare: func(q *queue) { q.finish(closeMsg{code: 1001, reason: "finished"}, true) }, closeWith: "finished"},
		{name: "host gone", gone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue(1024, 1)
			h := &host{ctrl: &peer{q: q}, gone: tc.gone}
			if tc.prepare != nil {
				tc.prepare(q)
			}
			h.notify(map[string]string{"type": "incoming"})
			_, final, queued := q.next()
			if queued != tc.queued {
				t.Fatalf("notification queued=%v, want %v", queued, tc.queued)
			}
			reason := ""
			if final != nil {
				reason = final.reason
			}
			if reason != tc.closeWith {
				t.Fatalf("control close reason %q, want %q", reason, tc.closeWith)
			}
		})
	}
}
