package directory

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

const epA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func reg(endpoint, id string, version uint64) Registration {
	return Registration{EndpointID: endpoint, RegistrationID: id, Version: version}
}

func joined(t *testing.T, nodes ...string) *Table {
	t.Helper()
	tb := NewTable(time.Minute)
	for i, n := range nodes {
		tb.Join(n, "http://"+n+":8080", uint64(i+1), false)
		if _, ok := tb.Snapshot(n, uint64(i+1), nil); !ok {
			t.Fatalf("snapshot for %s rejected", n)
		}
	}
	return tb
}

func owner(t *testing.T, tb *Table, endpoint string) (string, string) {
	t.Helper()
	n, r, ok := tb.Lookup(endpoint)
	if !ok {
		return "", ""
	}
	if n.ID != r.NodeID {
		t.Fatalf("lookup node %s does not match registration node %s", n.ID, r.NodeID)
	}
	return n.ID, r.RegistrationID
}

func TestNewerOrdersByVersionThenRegistrationID(t *testing.T) {
	cases := []struct {
		a, b Registration
		want bool
	}{
		{reg(epA, "r1", 2), reg(epA, "r2", 1), true},
		{reg(epA, "r1", 1), reg(epA, "r2", 2), false},
		{reg(epA, "r2", 5), reg(epA, "r1", 5), true},
		{reg(epA, "r1", 5), reg(epA, "r2", 5), false},
		{reg(epA, "r1", 5), reg(epA, "r1", 5), false},
	}
	for _, c := range cases {
		if got := Newer(c.a, c.b); got != c.want {
			t.Errorf("Newer(%+v, %+v) = %v", c.a, c.b, got)
		}
	}
}

func TestPutOrderingIsIndependentOfArrivalOrder(t *testing.T) {
	older := reg(epA, "old", 100)
	newer := reg(epA, "new", 200)

	tb := joined(t, "a", "b")
	if ev, _ := tb.Put("a", 1, older); len(ev) != 0 {
		t.Fatalf("unexpected eviction %+v", ev)
	}
	ev, _ := tb.Put("b", 2, newer)
	if len(ev) != 1 || ev[0].NodeID != "a" || ev[0].Registration.RegistrationID != "old" || ev[0].Winner != 200 {
		t.Fatalf("expected eviction of old on a, got %+v", ev)
	}
	if n, id := owner(t, tb, epA); n != "b" || id != "new" {
		t.Fatalf("owner %s/%s", n, id)
	}

	tb = joined(t, "a", "b")
	tb.Put("b", 2, newer)
	ev, _ = tb.Put("a", 1, older)
	if len(ev) != 1 || ev[0].NodeID != "a" || ev[0].Registration.RegistrationID != "old" {
		t.Fatalf("late stale put should evict itself, got %+v", ev)
	}
	if n, id := owner(t, tb, epA); n != "b" || id != "new" {
		t.Fatalf("late stale put replaced owner: %s/%s", n, id)
	}
	if got := tb.Clock(); got != 200 {
		t.Fatalf("clock %d", got)
	}
}

func TestDuplicatePutIsIdempotent(t *testing.T) {
	tb := joined(t, "a")
	r := reg(epA, "r1", 10)
	tb.Put("a", 1, r)
	if ev, _ := tb.Put("a", 1, r); len(ev) != 0 {
		t.Fatalf("replayed put evicted %+v", ev)
	}
	if n := tb.Nodes()[0]; n.Entries != 1 {
		t.Fatalf("entries %d", n.Entries)
	}
}

func TestStaleRemoveCannotDeleteNewerRegistration(t *testing.T) {
	tb := joined(t, "a", "b")
	tb.Put("a", 1, reg(epA, "r1", 1))
	tb.Put("b", 2, reg(epA, "r2", 2))
	if tb.Del("a", 1, reg(epA, "r1", 1)) {
		t.Fatal("stale remove from previous owner applied")
	}
	if tb.Del("b", 2, reg(epA, "r1", 1)) {
		t.Fatal("remove with stale registration id applied on owner node")
	}
	if n, id := owner(t, tb, epA); n != "b" || id != "r2" {
		t.Fatalf("owner after stale removes %s/%s", n, id)
	}
	if !tb.Del("b", 2, reg(epA, "r2", 2)) {
		t.Fatal("matching remove rejected")
	}
	if _, _, ok := tb.Lookup(epA); ok {
		t.Fatal("entry survived matching remove")
	}
}

func TestSameNodeReRegistrationFencesOldRemove(t *testing.T) {
	tb := joined(t, "a")
	tb.Put("a", 1, reg(epA, "r1", 1))
	if ev, _ := tb.Put("a", 1, reg(epA, "r2", 2)); len(ev) != 0 {
		t.Fatalf("node-local supersede should not round-trip an eviction, got %+v", ev)
	}
	if tb.Del("a", 1, reg(epA, "r1", 1)) {
		t.Fatal("old registration remove deleted new registration")
	}
	if _, id := owner(t, tb, epA); id != "r2" {
		t.Fatalf("owner %s", id)
	}
	if n := tb.Nodes()[0]; n.Entries != 1 {
		t.Fatalf("entries %d", n.Entries)
	}
}

func TestStaleSessionUpdatesAreIgnored(t *testing.T) {
	tb := joined(t, "a")
	tb.Put("a", 1, reg(epA, "r1", 1))
	if replaced := tb.Join("a", "http://a:8080", 7, false); replaced != 1 {
		t.Fatalf("replaced session %d", replaced)
	}
	if _, _, ok := tb.Lookup(epA); ok {
		t.Fatal("old session entries survived rejoin")
	}
	if _, ok := tb.Put("a", 1, reg(epA, "r1", 1)); ok {
		t.Fatal("put from replaced session accepted")
	}
	if tb.Leave("a", 1, time.Now()) {
		t.Fatal("leave from replaced session purged the new session")
	}
	tb.Snapshot("a", 7, []Registration{reg(epA, "r9", 9)})
	if n, id := owner(t, tb, epA); n != "a" || id != "r9" {
		t.Fatalf("owner after resnapshot %s/%s", n, id)
	}
	if tb.Del("a", 1, reg(epA, "r9", 9)) {
		t.Fatal("remove from replaced session applied")
	}
}

func TestLeavePurgesOnlyThatNode(t *testing.T) {
	tb := joined(t, "a", "b")
	epB := strings.Repeat("b", 64)
	tb.Put("a", 1, reg(epA, "r1", 1))
	tb.Put("b", 2, reg(epB, "r2", 2))
	now := time.Unix(1000, 0)
	if !tb.Leave("a", 1, now) {
		t.Fatal("leave rejected")
	}
	if _, _, ok := tb.Lookup(epA); ok {
		t.Fatal("departed node entry still routable")
	}
	if n, _ := owner(t, tb, epB); n != "b" {
		t.Fatal("other node entry purged")
	}
	if addr, ok := tb.NodeAddr("a", now.Add(30*time.Second)); !ok || addr != "http://a:8080" {
		t.Fatal("departed node address not kept for accept grace")
	}
	if _, ok := tb.NodeAddr("a", now.Add(2*time.Minute)); ok {
		t.Fatal("departed node address kept past grace")
	}
	if c := tb.Candidates(); len(c) != 1 || c[0].ID != "b" {
		t.Fatalf("candidates %+v", c)
	}
}

func TestCandidatesExcludeDrainingAndUnready(t *testing.T) {
	tb := joined(t, "a", "b")
	tb.Join("c", "http://c:8080", 3, false)
	tb.SetDraining("b", 2)
	c := tb.Candidates()
	if len(c) != 1 || c[0].ID != "a" {
		t.Fatalf("candidates %+v", c)
	}
	tb.Snapshot("c", 3, nil)
	if c := tb.Candidates(); len(c) != 2 {
		t.Fatalf("candidates after snapshot %+v", c)
	}
	if addr, ok := tb.NodeAddr("b", time.Now()); !ok || addr == "" {
		t.Fatal("draining node not routable for accepts")
	}
}

func TestConnectionIDParsing(t *testing.T) {
	random := "AAAAAAAAAAAAAAAAAAAAAA"
	if got := ConnectionID("", random); got != random {
		t.Fatalf("single-node id changed: %s", got)
	}
	id := ConnectionID("node-b", random)
	if n, ok := ConnectionNode(id); !ok || n != "node-b" {
		t.Fatalf("parse %q: %q %v", id, n, ok)
	}
	for _, bad := range []string{"", random, ".abc", "node-b.", "Node-B.abc", "-a.abc", "a-.abc", strings.Repeat("a", 33) + ".abc", "a_b.abc", "a/b.abc", "a b.abc"} {
		if n, ok := ConnectionNode(bad); ok {
			t.Errorf("accepted %q as node %q", bad, n)
		}
	}
	if n, ok := ConnectionNode("a.b.c"); !ok || n != "a" {
		t.Fatalf("dotted suffix: %q %v", n, ok)
	}
}

func TestNodeAndEndpointValidation(t *testing.T) {
	for _, ok := range []string{"a", "node-a", "n1", strings.Repeat("a", 32)} {
		if !ValidNodeID(ok) {
			t.Errorf("rejected %q", ok)
		}
	}
	for _, bad := range []string{"", "-a", "a-", "A", "a.b", strings.Repeat("a", 33)} {
		if ValidNodeID(bad) {
			t.Errorf("accepted %q", bad)
		}
	}
	if !ValidEndpointID(epA) || ValidEndpointID(strings.ToUpper(epA)) || ValidEndpointID(epA[1:]) {
		t.Fatal("endpoint id validation")
	}
	key := make([]byte, 32)
	key[0] = 7
	sum := sha256.Sum256(key)
	b64 := base64.RawURLEncoding.EncodeToString(key)
	if id, ok := EndpointIDFromPublicKey(b64); !ok || id != hex.EncodeToString(sum[:]) {
		t.Fatalf("endpoint id from key: %q %v", id, ok)
	}
	for _, bad := range []string{b64 + "=", base64.StdEncoding.EncodeToString(key), b64[:42]} {
		if _, ok := EndpointIDFromPublicKey(bad); ok {
			t.Errorf("accepted key %q", bad)
		}
	}
}

func TestClockIsMonotonicAndObservesRemoteVersions(t *testing.T) {
	wall := time.Unix(0, 1000)
	c := &Clock{Now: func() time.Time { return wall }}
	a := c.Next()
	wall = time.Unix(0, 500)
	b := c.Next()
	if b <= a {
		t.Fatalf("clock went backwards: %d then %d", a, b)
	}
	c.Observe(5000)
	if d := c.Next(); d <= 5000 {
		t.Fatalf("clock ignored observed version: %d", d)
	}
}
