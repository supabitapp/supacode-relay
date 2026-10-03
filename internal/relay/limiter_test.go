package relay

import (
	"net/http"
	"net/netip"
	"testing"
	"time"
)

func TestLimiterBurstAndRefill(t *testing.T) {
	l := newIPLimiter(10, 3)
	ip := netip.MustParseAddr("192.0.2.1")
	now := time.Unix(1000, 0)
	for i := range 3 {
		if !l.allow(ip, now) {
			t.Fatalf("attempt %d rejected", i)
		}
	}
	if l.allow(ip, now) {
		t.Fatal("burst exceeded")
	}
	if !l.allow(ip, now.Add(100*time.Millisecond)) {
		t.Fatal("token not refilled")
	}
	if !l.allow(netip.MustParseAddr("192.0.2.2"), now) {
		t.Fatal("other ip limited")
	}
}

func TestLimiterBookkeepingExpiresAndIsBounded(t *testing.T) {
	l := newIPLimiter(1, 1)
	now := time.Unix(1000, 0)
	for i := range limiterMaxEntries {
		l.allow(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}), now)
	}
	if l.size() != limiterMaxEntries {
		t.Fatalf("size %d", l.size())
	}
	if l.allow(netip.MustParseAddr("198.51.100.1"), now) {
		t.Fatal("new ip admitted while table full of live entries")
	}
	if !l.allow(netip.MustParseAddr("198.51.100.1"), now.Add(limiterIdleTTL+time.Second)) {
		t.Fatal("expired entries not swept")
	}
	if l.size() != 1 {
		t.Fatalf("size after sweep %d", l.size())
	}
}

func TestClientIPTrustedProxies(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8")}
	req := func(remote, xff string) *http.Request {
		r := &http.Request{RemoteAddr: remote, Header: http.Header{}}
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		return r
	}
	cases := []struct {
		remote, xff string
		trusted     []netip.Prefix
		want        string
	}{
		{"203.0.113.5:1234", "1.2.3.4", trusted, "203.0.113.5"},
		{"127.0.0.1:1234", "198.51.100.7", trusted, "198.51.100.7"},
		{"127.0.0.1:1234", "6.6.6.6, 198.51.100.7, 10.1.1.1", trusted, "198.51.100.7"},
		{"127.0.0.1:1234", "garbage", trusted, "127.0.0.1"},
		{"127.0.0.1:1234", "198.51.100.7", nil, "127.0.0.1"},
		{"[::ffff:127.0.0.1]:1234", "", trusted, "127.0.0.1"},
	}
	for _, c := range cases {
		if got := clientIP(req(c.remote, c.xff), c.trusted); got.String() != c.want {
			t.Errorf("%s %q: got %s want %s", c.remote, c.xff, got, c.want)
		}
	}
}
