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

func TestHostNotificationsReportQueueOutcome(t *testing.T) {
	for _, outcome := range []string{"queued", "queue_full", "queue_finished", "host_gone"} {
		t.Run(outcome, func(t *testing.T) {
			q := newQueue(1024, 1)
			h := &host{ctrl: &peer{q: q}, gone: outcome == "host_gone"}
			if outcome == "queue_full" {
				q.push(textFrame(map[string]string{"type": "registered"}))
			}
			if outcome == "queue_finished" {
				q.finish(closeMsg{code: 1001}, true)
			}
			if got := h.notify(map[string]string{"type": "incoming"}); got != outcome {
				t.Fatalf("notification outcome %q, want %q", got, outcome)
			}
			_, final, queued := q.next()
			if queued != (outcome == "queued") {
				t.Fatalf("notification queue contradicts outcome %q", outcome)
			}
			if outcome == "queue_full" && (final == nil || final.reason != "control queue limit exceeded") {
				t.Fatal("overloaded host control did not close")
			}
		})
	}
}
