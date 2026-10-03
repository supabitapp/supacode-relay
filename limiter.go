package main

import (
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	limiterMaxEntries = 65536
	limiterIdleTTL    = time.Minute
)

type ipLimiter struct {
	mu      sync.Mutex
	rate    rate.Limit
	burst   int
	entries map[netip.Addr]*limiterEntry
	swept   time.Time
}

type limiterEntry struct {
	lim  *rate.Limiter
	seen time.Time
}

func newIPLimiter(r float64, burst int) *ipLimiter {
	return &ipLimiter{rate: rate.Limit(r), burst: burst, entries: map[netip.Addr]*limiterEntry{}}
}

func (l *ipLimiter) allow(ip netip.Addr, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > limiterIdleTTL || len(l.entries) >= limiterMaxEntries {
		l.sweep(now)
	}
	e, ok := l.entries[ip]
	if !ok {
		if len(l.entries) >= limiterMaxEntries {
			return false
		}
		e = &limiterEntry{lim: rate.NewLimiter(l.rate, l.burst)}
		l.entries[ip] = e
	}
	e.seen = now
	return e.lim.AllowN(now, 1)
}

func (l *ipLimiter) sweep(now time.Time) {
	l.swept = now
	for ip, e := range l.entries {
		if now.Sub(e.seen) > limiterIdleTTL {
			delete(l.entries, ip)
		}
	}
}

func (l *ipLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

func clientIP(r *http.Request, trusted []netip.Prefix) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	ip = ip.Unmap()
	if !isTrusted(ip, trusted) {
		return ip
	}
	hops := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip
		}
		hop = hop.Unmap()
		if !isTrusted(hop, trusted) {
			return hop
		}
		ip = hop
	}
	return ip
}

func isTrusted(ip netip.Addr, trusted []netip.Prefix) bool {
	for _, p := range trusted {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}
