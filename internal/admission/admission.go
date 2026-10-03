package admission

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
	MaxEntries = 65536
	IdleTTL    = time.Minute
)

type Limiter struct {
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

func NewLimiter(r float64, burst int) *Limiter {
	return &Limiter{rate: rate.Limit(r), burst: burst, entries: map[netip.Addr]*limiterEntry{}}
}

func (l *Limiter) Allow(ip netip.Addr, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.swept) > IdleTTL || len(l.entries) >= MaxEntries {
		l.sweep(now)
	}
	e, ok := l.entries[ip]
	if !ok {
		if len(l.entries) >= MaxEntries {
			return false
		}
		e = &limiterEntry{lim: rate.NewLimiter(l.rate, l.burst)}
		l.entries[ip] = e
	}
	e.seen = now
	return e.lim.AllowN(now, 1)
}

func (l *Limiter) sweep(now time.Time) {
	l.swept = now
	for ip, e := range l.entries {
		if now.Sub(e.seen) > IdleTTL {
			delete(l.entries, ip)
		}
	}
}

func (l *Limiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

type Gate struct {
	mu     sync.Mutex
	max    int
	perIP  int
	total  int
	counts map[netip.Addr]int
}

type GateResult int

const (
	Admitted GateResult = iota
	GlobalFull
	PerIPFull
)

func NewGate(max, perIP int) *Gate {
	return &Gate{max: max, perIP: perIP, counts: map[netip.Addr]int{}}
}

func (g *Gate) Acquire(ip netip.Addr) GateResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.total >= g.max {
		return GlobalFull
	}
	if g.counts[ip] >= g.perIP {
		return PerIPFull
	}
	g.total++
	g.counts[ip]++
	return Admitted
}

func (g *Gate) Release(ip netip.Addr) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.total--
	if g.counts[ip] <= 1 {
		delete(g.counts, ip)
		return
	}
	g.counts[ip]--
}

func (g *Gate) Stats() (total, ips int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.total, len(g.counts)
}

func ClientIP(r *http.Request, trusted []netip.Prefix, header string) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	ip = ip.Unmap()
	if !IsTrusted(ip, trusted) {
		return ip
	}
	if header != "" {
		if vals := r.Header.Values(header); len(vals) == 1 {
			if forwarded, err := netip.ParseAddr(strings.TrimSpace(vals[0])); err == nil {
				return forwarded.Unmap()
			}
		}
	}
	hops := strings.Split(strings.Join(r.Header.Values("X-Forwarded-For"), ","), ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop, err := netip.ParseAddr(strings.TrimSpace(hops[i]))
		if err != nil {
			return ip
		}
		hop = hop.Unmap()
		if !IsTrusted(hop, trusted) {
			return hop
		}
		ip = hop
	}
	return ip
}

func IsTrusted(ip netip.Addr, trusted []netip.Prefix) bool {
	for _, p := range trusted {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

func ParsePrefixes(v string) ([]netip.Prefix, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	var out []netip.Prefix
	for _, s := range strings.Split(v, ",") {
		p, err := netip.ParsePrefix(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

type filteredListener struct {
	net.Listener
	allow []netip.Prefix
}

func FilterListener(ln net.Listener, allow []netip.Prefix) net.Listener {
	if len(allow) == 0 {
		return ln
	}
	return &filteredListener{Listener: ln, allow: allow}
}

func (l *filteredListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if ap, err := netip.ParseAddrPort(c.RemoteAddr().String()); err == nil && IsTrusted(ap.Addr().Unmap(), l.allow) {
			return c, nil
		}
		_ = c.Close()
	}
}
