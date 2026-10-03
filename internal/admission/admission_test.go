package admission

import (
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"
)

func TestLimiterBurstAndRefill(t *testing.T) {
	l := NewLimiter(10, 3)
	ip := netip.MustParseAddr("192.0.2.1")
	now := time.Unix(1000, 0)
	for i := range 3 {
		if !l.Allow(ip, now) {
			t.Fatalf("attempt %d rejected", i)
		}
	}
	if l.Allow(ip, now) {
		t.Fatal("burst exceeded")
	}
	if !l.Allow(ip, now.Add(100*time.Millisecond)) {
		t.Fatal("token not refilled")
	}
	if !l.Allow(netip.MustParseAddr("192.0.2.2"), now) {
		t.Fatal("other ip limited")
	}
}

func TestLimiterBookkeepingExpiresAndIsBounded(t *testing.T) {
	l := NewLimiter(1, 1)
	now := time.Unix(1000, 0)
	for i := range MaxEntries {
		l.Allow(netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}), now)
	}
	if l.Size() != MaxEntries {
		t.Fatalf("size %d", l.Size())
	}
	if l.Allow(netip.MustParseAddr("198.51.100.1"), now) {
		t.Fatal("new ip admitted while table full of live entries")
	}
	if !l.Allow(netip.MustParseAddr("198.51.100.1"), now.Add(IdleTTL+time.Second)) {
		t.Fatal("expired entries not swept")
	}
	if l.Size() != 1 {
		t.Fatalf("size after sweep %d", l.Size())
	}
}

func TestClientIPTrustedProxies(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8")}
	req := func(remote string, xff ...string) *http.Request {
		r := &http.Request{RemoteAddr: remote, Header: http.Header{}}
		for _, v := range xff {
			r.Header.Add("X-Forwarded-For", v)
		}
		return r
	}
	cases := []struct {
		r       *http.Request
		trusted []netip.Prefix
		want    string
	}{
		{req("203.0.113.5:1234", "1.2.3.4"), trusted, "203.0.113.5"},
		{req("127.0.0.1:1234", "198.51.100.7"), trusted, "198.51.100.7"},
		{req("127.0.0.1:1234", "6.6.6.6, 198.51.100.7, 10.1.1.1"), trusted, "198.51.100.7"},
		{req("127.0.0.1:1234", "6.6.6.6", "198.51.100.7"), trusted, "198.51.100.7"},
		{req("127.0.0.1:1234", "garbage"), trusted, "127.0.0.1"},
		{req("127.0.0.1:1234", "198.51.100.7"), nil, "127.0.0.1"},
		{req("[::ffff:127.0.0.1]:1234"), trusted, "127.0.0.1"},
	}
	for _, c := range cases {
		if got := ClientIP(c.r, c.trusted, ""); got.String() != c.want {
			t.Errorf("%s %q: got %s want %s", c.r.RemoteAddr, c.r.Header.Values("X-Forwarded-For"), got, c.want)
		}
	}
}

func TestClientIPHeaderOnlyFromTrustedPeers(t *testing.T) {
	trusted := []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	req := func(remote string, values ...string) *http.Request {
		r := &http.Request{RemoteAddr: remote, Header: http.Header{"X-Forwarded-For": {"109.94.97.37, 109.94.97.37"}}}
		for _, v := range values {
			r.Header.Add("X-Relay-Client-Ip", v)
		}
		return r
	}
	cases := []struct {
		r    *http.Request
		want string
	}{
		{req("127.0.0.1:1", "198.51.100.7"), "198.51.100.7"},
		{req("127.0.0.1:1", "::ffff:198.51.100.7"), "198.51.100.7"},
		{req("203.0.113.5:1", "198.51.100.7"), "203.0.113.5"},
		{req("127.0.0.1:1", "198.51.100.7", "6.6.6.6"), "109.94.97.37"},
		{req("127.0.0.1:1", "not-an-ip"), "109.94.97.37"},
		{req("127.0.0.1:1"), "109.94.97.37"},
	}
	for _, c := range cases {
		if got := ClientIP(c.r, trusted, "X-Relay-Client-Ip"); got.String() != c.want {
			t.Errorf("%s %v: got %s want %s", c.r.RemoteAddr, c.r.Header.Values("X-Relay-Client-Ip"), got, c.want)
		}
	}
	if got := ClientIP(req("127.0.0.1:1", "198.51.100.7"), trusted, ""); got.String() != "109.94.97.37" {
		t.Errorf("header honored without opt-in: %s", got)
	}
}

func TestGateLimits(t *testing.T) {
	g := NewGate(3, 2)
	a, b := netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")
	if g.Acquire(a) != Admitted || g.Acquire(a) != Admitted {
		t.Fatal("first two rejected")
	}
	if g.Acquire(a) != PerIPFull {
		t.Fatal("per-ip limit not enforced")
	}
	if g.Acquire(b) != Admitted {
		t.Fatal("other ip rejected")
	}
	if g.Acquire(b) != GlobalFull {
		t.Fatal("global limit not enforced")
	}
	g.Release(a)
	if g.Acquire(b) != Admitted {
		t.Fatal("released slot not reusable")
	}
	g.Release(a)
	g.Release(b)
	g.Release(b)
	if total, ips := g.Stats(); total != 0 || ips != 0 {
		t.Fatalf("leaked state total=%d ips=%d", total, ips)
	}
}

func TestFilterListenerDropsUnlistedPeers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if FilterListener(ln, nil) != ln {
		t.Fatal("empty allowlist should not wrap the listener")
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })}
	go func() { _ = srv.Serve(FilterListener(ln, []netip.Prefix{netip.MustParsePrefix("10.9.9.9/32")})) }()
	defer srv.Close()
	c := http.Client{Timeout: 2 * time.Second}
	if resp, err := c.Get("http://" + ln.Addr().String() + "/healthz"); err == nil {
		resp.Body.Close()
		t.Fatalf("unlisted peer got HTTP %d", resp.StatusCode)
	}

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv2 := &http.Server{Handler: srv.Handler}
	go func() { _ = srv2.Serve(FilterListener(ln2, []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")})) }()
	defer srv2.Close()
	resp, err := c.Get("http://" + ln2.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Fatalf("listed peer got %d", resp.StatusCode)
	}
}
