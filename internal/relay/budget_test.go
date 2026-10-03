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

func TestByteBudgetReadAccountsPayloadBytes(t *testing.T) {
	b := newByteBudget(16, 2)
	f, err := b.read(strings.NewReader("12345678"), 8)
	if err != nil {
		t.Fatal(err)
	}
	if string(f.data) != "12345678" || f.reserved != 8 {
		t.Fatalf("frame = len %d reserved %d, want len 8 reserved 8", len(f.data), f.reserved)
	}
	if got := b.used.Load(); got != 16 {
		t.Fatalf("used bytes = %d, want 16", got)
	}
	b.release(f.reserved)
	if got := b.used.Load(); got != 0 {
		t.Fatalf("used bytes after release = %d, want 0", got)
	}
}

func TestByteBudgetReadRejectsOversizeAndCapacity(t *testing.T) {
	b := newByteBudget(64, 1)
	if _, err := b.read(strings.NewReader("1234"), 3); !errors.Is(err, websocket.ErrReadLimit) {
		t.Fatalf("oversize read error = %v, want ErrReadLimit", err)
	}
	if got := b.used.Load(); got != 0 {
		t.Fatalf("used bytes after oversize read = %d, want 0", got)
	}

	b = newByteBudget(3, 1)
	if _, err := b.read(strings.NewReader("1234"), 4); !errors.Is(err, errIngressCapacity) {
		t.Fatalf("capacity read error = %v, want ingress capacity", err)
	}
	if got := b.used.Load(); got != 0 {
		t.Fatalf("used bytes after capacity rejection = %d, want 0", got)
	}
}
