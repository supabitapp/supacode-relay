package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/supabitapp/supacode-relay/internal/admission"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

type route int

const (
	routeControl route = iota
	routeConnect
	routeAccept
)

var routePaths = map[string]route{
	"/v1/control": routeControl,
	"/v1/connect": routeConnect,
	"/v1/accept":  routeAccept,
}

func (r route) String() string {
	return [...]string{"control", "connect", "accept"}[r]
}

const halfCloseGrace = 2 * time.Second

type stateKey struct{}

type hijackWriter struct {
	http.ResponseWriter
}

func (h hijackWriter) Unwrap() http.ResponseWriter {
	return h.ResponseWriter
}

func (h hijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(h.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &halfCloseConn{Conn: conn}, brw, nil
}

type halfCloseConn struct {
	net.Conn
	once sync.Once
}

const copyBufferBytes = 16 * 1024

var copyBuffers = sync.Pool{New: func() any {
	buffer := make([]byte, copyBufferBytes)
	return &buffer
}}

// Preserve the wrapper's half-close deadline while giving each upgrade
// direction a fixed copy buffer, including TLS and buffered handshake bytes.
func (c *halfCloseConn) ReadFrom(reader io.Reader) (int64, error) {
	return copyFixed(c.Conn, reader)
}

func (c *halfCloseConn) WriteTo(writer io.Writer) (int64, error) {
	return copyFixed(writer, c.Conn)
}

func copyFixed(writer io.Writer, reader io.Reader) (int64, error) {
	buffer := copyBuffers.Get().(*[]byte)
	defer func() {
		clear(*buffer)
		copyBuffers.Put(buffer)
	}()
	// Hide optional copy interfaces to avoid recursion and uncontrolled buffers.
	return io.CopyBuffer(struct{ io.Writer }{writer}, struct{ io.Reader }{reader}, *buffer)
}

func (c *halfCloseConn) CloseWrite() error {
	var err error
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		err = cw.CloseWrite()
	}
	c.once.Do(func() { _ = c.Conn.SetReadDeadline(time.Now().Add(halfCloseGrace)) })
	return err
}

type proxyState struct {
	route      route
	ip         netip.Addr
	endpointID string
	nodeID     string
	upgraded   string
}

func stateOf(r *http.Request) *proxyState {
	st, _ := r.Context().Value(stateKey{}).(*proxyState)
	return st
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func synthetic(req *http.Request, status int, msg string) *http.Response {
	body, _ := json.Marshal(map[string]string{"error": msg})
	body = append(body, '\n')
	h := http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}
	return &http.Response{
		Status:        http.StatusText(status),
		StatusCode:    status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(string(body))),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func (rt *router) newProxy() *httputil.ReverseProxy {
	dialer := &net.Dialer{Timeout: rt.cfg.dialTimeout, KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: keepAliveIdle, Interval: 5 * time.Second, Count: 3}}
	base := &http.Transport{
		DialContext:           dialer.DialContext,
		ResponseHeaderTimeout: rt.cfg.headerTimeout,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       30 * time.Second,
		DisableCompression:    true,
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			st := stateOf(pr.In)
			pr.Out.URL = &url.URL{Scheme: "http", Host: "upstream.invalid", Path: pr.In.URL.Path, RawQuery: pr.In.URL.RawQuery}
			pr.Out.Host = ""
			for _, h := range []string{"X-Real-Ip", "True-Client-Ip", "Cf-Connecting-Ip", "Forwarded", "X-Forwarded-Host", "X-Forwarded-Proto"} {
				pr.Out.Header.Del(h)
			}
			pr.Out.Header.Set("X-Forwarded-For", st.ip.String())
			pr.Out.Header.Set(directory.ClientIPHeader, st.ip.String())
		},
		Transport:    &upstream{rt: rt, base: base},
		ErrorHandler: rt.proxyError,
		ErrorLog:     rt.log,
	}
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rt0, ok := routePaths[r.URL.Path]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if rt0 != routeAccept && rt.draining.Load() {
		rt.rejected(w, http.StatusServiceUnavailable, "draining", &rt.rejectedDraining)
		return
	}
	ip := admission.ClientIP(r, rt.cfg.trustedProxies, "")
	if !rt.limiter.Allow(ip, time.Now()) {
		rt.rejected(w, http.StatusTooManyRequests, "rate limited", &rt.rejectedRate)
		return
	}
	switch rt.gate.Acquire(ip) {
	case admission.GlobalFull:
		rt.rejected(w, http.StatusServiceUnavailable, "router capacity reached", &rt.rejectedCapacity)
		return
	case admission.PerIPFull:
		rt.rejected(w, http.StatusTooManyRequests, "too many connections", &rt.rejectedCapacity)
		return
	}
	defer rt.gate.Release(ip)

	st := &proxyState{route: rt0, ip: ip}
	q := r.URL.Query()
	switch rt0 {
	case routeControl:
		id, ok := directory.EndpointIDFromPublicKey(q.Get("publicKey"))
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid publicKey"})
			return
		}
		st.endpointID = id
	case routeConnect:
		st.endpointID = q.Get("endpointId")
	case routeAccept:
		st.nodeID, _ = directory.ConnectionNode(q.Get("connectionId"))
	}
	rt.requests[rt0].Add(1)
	rt.active[rt0].Add(1)
	defer rt.active[rt0].Add(-1)
	rt.proxy.ServeHTTP(hijackWriter{w}, r.WithContext(context.WithValue(r.Context(), stateKey{}, st)))
	if st.upgraded != "" && rt0 == routeControl {
		rt.mu.Lock()
		rt.controls[st.upgraded]--
		if rt.controls[st.upgraded] <= 0 {
			delete(rt.controls, st.upgraded)
		}
		rt.mu.Unlock()
	}
}

func (rt *router) rejected(w http.ResponseWriter, status int, msg string, counter interface{ Add(int64) int64 }) {
	counter.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func (rt *router) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	rt.upstreamErrors.Add(1)
	route := "unknown"
	if st := stateOf(r); st != nil {
		route = st.route.String()
	}
	rt.log.Printf("router: %s upstream error: %v", route, err)
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
}

type upstream struct {
	rt   *router
	base http.RoundTripper
}

func (u *upstream) RoundTrip(req *http.Request) (*http.Response, error) {
	rt := u.rt
	st := stateOf(req)
	tried := map[string]bool{}
	for attempt := 0; ; attempt++ {
		node, addr, res := rt.pick(req, st, tried, attempt)
		if res != nil {
			return res, nil
		}
		target, err := url.Parse(addr)
		if err != nil {
			return synthetic(req, http.StatusBadGateway, "upstream unavailable"), nil
		}
		out := req.Clone(req.Context())
		out.URL.Scheme = target.Scheme
		out.URL.Host = target.Host
		out.Host = target.Host
		res, err = u.base.RoundTrip(out)
		if attempt > 0 || !rt.retryable(st, res, err) {
			if err == nil && res.StatusCode == http.StatusSwitchingProtocols {
				st.upgraded = node
				if st.route == routeControl {
					rt.mu.Lock()
					rt.controls[node]++
					rt.mu.Unlock()
				}
			}
			return res, err
		}
		if res != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
			res.Body.Close()
		}
		tried[node] = true
		rt.retries[st.route].Add(1)
		if st.route == routeConnect {
			rt.refresh(req.Context())
		}
	}
}

func (rt *router) retryable(st *proxyState, res *http.Response, err error) bool {
	if err != nil {
		var op *net.OpError
		dial := errors.As(err, &op) && op.Op == "dial"
		var ne net.Error
		timeout := errors.As(err, &ne) && ne.Timeout()
		switch st.route {
		case routeControl:
			return dial || timeout
		case routeConnect:
			return dial
		}
		return false
	}
	switch st.route {
	case routeControl:
		return res.StatusCode == http.StatusServiceUnavailable
	case routeConnect:
		return res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusServiceUnavailable
	}
	return false
}

func (rt *router) pick(req *http.Request, st *proxyState, tried map[string]bool, attempt int) (string, string, *http.Response) {
	switch st.route {
	case routeControl:
		var cands []directory.Node
		for _, n := range rt.table.Candidates() {
			if !tried[n.ID] {
				cands = append(cands, n)
			}
		}
		if len(cands) == 0 {
			rt.misses[st.route].Add(1)
			return "", "", synthetic(req, http.StatusServiceUnavailable, "no relay nodes available")
		}
		if owner, _, ok := rt.table.Lookup(st.endpointID); ok {
			for _, n := range cands {
				if n.ID == owner.ID {
					return n.ID, n.Addr, nil
				}
			}
		}
		n := rt.leastLoaded(cands)
		return n.ID, n.Addr, nil
	case routeConnect:
		n, _, ok := rt.table.Lookup(st.endpointID)
		if !ok && attempt == 0 && directory.ValidEndpointID(st.endpointID) {
			rt.refresh(req.Context())
			n, _, ok = rt.table.Lookup(st.endpointID)
		}
		if !ok {
			rt.misses[st.route].Add(1)
			return "", "", synthetic(req, http.StatusNotFound, "endpoint not found")
		}
		return n.ID, n.Addr, nil
	default:
		if st.nodeID == "" {
			rt.misses[st.route].Add(1)
			return "", "", synthetic(req, http.StatusNotFound, "connection not found")
		}
		addr, ok := rt.table.NodeAddr(st.nodeID, time.Now())
		if !ok {
			rt.refresh(req.Context())
			addr, ok = rt.table.NodeAddr(st.nodeID, time.Now())
		}
		if !ok {
			rt.misses[st.route].Add(1)
			return "", "", synthetic(req, http.StatusNotFound, "connection not found")
		}
		return st.nodeID, addr, nil
	}
}

func (rt *router) leastLoaded(cands []directory.Node) directory.Node {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var best []directory.Node
	bestLoad := 0
	for _, n := range cands {
		load := rt.controls[n.ID]
		switch {
		case len(best) == 0 || load < bestLoad:
			best, bestLoad = []directory.Node{n}, load
		case load == bestLoad:
			best = append(best, n)
		}
	}
	return best[rand.N(len(best))]
}
