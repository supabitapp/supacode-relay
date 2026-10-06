package relay

import (
	"errors"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestByteBudgetReserveAndRelease(t *testing.T) {
	b := newByteBudget(16, 4)
	if !b.reserve(4) {
		t.Fatal("first reservation was rejected")
	}
	if b.reserve(1) {
		t.Fatal("reservation exceeded weighted limit")
	}
	b.release(4)
	if got := b.used.Load(); got != 0 {
		t.Fatalf("used bytes = %d, want 0", got)
	}
	if !b.reserve(4) {
		t.Fatal("released capacity was not reusable")
	}
}

func TestQueueReadAccountsPayloadBytes(t *testing.T) {
	b := newByteBudget(16, 2)
	q := newQueue(16, 4, b)
	f, err := q.read(strings.NewReader("12345678"), 8)
	if err != nil {
		t.Fatal(err)
	}
	if string(f.data) != "12345678" || f.reserved != 8 {
		t.Fatalf("frame = len %d reserved %d, want len 8 reserved 8", len(f.data), f.reserved)
	}
	if got := b.used.Load(); got != 16 {
		t.Fatalf("used bytes = %d, want 16", got)
	}
	if got := q.held.Load(); got != 8 {
		t.Fatalf("held bytes = %d, want 8", got)
	}
	q.releaseReserved(f.reserved)
	if got, held := b.used.Load(), q.held.Load(); got != 0 || held != 0 {
		t.Fatalf("used %d held %d after release, want 0 0", got, held)
	}
}

func TestQueueReadRejectsOversizeAndCapacity(t *testing.T) {
	b := newByteBudget(64, 1)
	q := newQueue(64, 4, b)
	if _, err := q.read(strings.NewReader("1234"), 3); !errors.Is(err, websocket.ErrReadLimit) {
		t.Fatalf("oversize read error = %v, want ErrReadLimit", err)
	}
	if got, held := b.used.Load(), q.held.Load(); got != 0 || held != 0 {
		t.Fatalf("used %d held %d after oversize read, want 0 0", got, held)
	}

	b = newByteBudget(3, 1)
	q = newQueue(64, 4, b)
	if _, err := q.read(strings.NewReader("1234"), 4); !errors.Is(err, errIngressCapacity) {
		t.Fatalf("capacity read error = %v, want ingress capacity", err)
	}
	if got, held := b.used.Load(), q.held.Load(); got != 0 || held != 0 {
		t.Fatalf("used %d held %d after capacity rejection, want 0 0", got, held)
	}
}

func TestIngressVictimPicksHeaviestOverShare(t *testing.T) {
	s := &Server{budget: newByteBudget(120, 1), pairs: map[string]*pair{}}
	held := func(n int64) *queue {
		q := newQueue(1<<20, 1024, s.budget)
		q.held.Store(n)
		return q
	}
	heavy := &pair{toHost: held(90), toClient: held(0)}
	light := &pair{toHost: held(10), toClient: held(0)}
	s.pairs["heavy"], s.pairs["light"] = heavy, light

	if got := s.ingressVictim(light.toHost, 5); got != heavy {
		t.Fatalf("victim for light reader = %v, want heavy pair", got)
	}
	if got := s.ingressVictim(heavy.toHost, 5); got != nil {
		t.Fatalf("victim for heavy reader = %v, want nil", got)
	}
	idle := held(0)
	if got := s.ingressVictim(idle, 5); got != heavy {
		t.Fatalf("victim for new reader = %v, want heavy pair", got)
	}

	heavy.toHost.held.Store(40)
	light.toHost.held.Store(40)
	if got := s.ingressVictim(idle, 5); got != nil {
		t.Fatalf("victim with every holder at its share = %v, want nil", got)
	}
}
