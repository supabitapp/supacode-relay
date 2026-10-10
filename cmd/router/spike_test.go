package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

type spikeConn struct {
	writes atomic.Int64
	errors atomic.Int64
	net.Conn
	deadline time.Duration
	slots    chan struct{}
	once     sync.Once
}

func (c *spikeConn) Write(p []byte) (int, error) {
	if c.deadline > 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(c.deadline))
	}
	c.writes.Add(1)
	n, err := c.Conn.Write(p)
	if err != nil {
		c.errors.Add(1)
	}
	return n, err
}

func (c *spikeConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		if c.slots != nil {
			<-c.slots
		}
	})
	return err
}

type spikeListener struct {
	last atomic.Pointer[spikeConn]
	net.Listener
	deadline time.Duration
	slots    chan struct{}
	peak     atomic.Int64
}

func (l *spikeListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if l.slots != nil {
			select {
			case l.slots <- struct{}{}:
				n := int64(len(l.slots))
				for old := l.peak.Load(); n > old && !l.peak.CompareAndSwap(old, n); old = l.peak.Load() {
				}
			default:
				c.Close()
				continue
			}
		}
		if tcp, ok := c.(*net.TCPConn); ok {
			tcp.SetWriteBuffer(4096)
		}
		wrapped := &spikeConn{Conn: c, deadline: l.deadline, slots: l.slots}
		l.last.Store(wrapped)
		return wrapped, nil
	}
}

func spikeRouter(t *testing.T, candidate bool, handler func(*router) http.Handler) (*router, *httptest.Server, *spikeListener) {
	t.Helper()
	cfg, err := loadConfig(func(k string) string {
		switch k {
		case "ROUTER_DIRECTORY_TOKEN":
			return testToken
		case "ROUTER_ADMISSION_RATE":
			return "100000"
		case "ROUTER_MAX_CONNS":
			return "8"
		case "ROUTER_MAX_CONNS_PER_IP":
			return "8"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := newRouter(cfg, io.Discard)
	selected := handler(rt)
	if candidate {
		next := selected
		selected = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
				r.Close = true
				w.Header().Set("Connection", "close")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	s := httptest.NewUnstartedServer(selected)
	s.Config.ReadHeaderTimeout = 5 * time.Second
	l := &spikeListener{Listener: s.Listener}
	if candidate {
		l.deadline = 150 * time.Millisecond
		l.slots = make(chan struct{}, 8)
		s.Config.IdleTimeout = 100 * time.Millisecond
		s.Config.ReadTimeout = 200 * time.Millisecond
		s.Config.WriteTimeout = 300 * time.Millisecond
		s.Config.MaxHeaderBytes = 16 * 1024
	}
	s.Listener = l
	s.Start()
	t.Cleanup(s.Close)
	return rt, s, l
}

func TestSpikeHTTPRetention(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		t.Run(fmt.Sprint(candidate), func(t *testing.T) {
			rt, s, l := spikeRouter(t, candidate, func(r *router) http.Handler { return r })
			conns := make([]net.Conn, 0, 64)
			for range 64 {
				c, err := net.Dial("tcp", s.Listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				conns = append(conns, c)
				fmt.Fprint(c, "GET /missing HTTP/1.1\r\nHost: spike\r\n\r\n")
			}
			defer func() {
				for _, c := range conns {
					c.Close()
				}
			}()
			var kept atomic.Int64
			var readers sync.WaitGroup
			for _, c := range conns {
				readers.Add(1)
				go func() {
					defer readers.Done()
					c.SetReadDeadline(time.Now().Add(350 * time.Millisecond))
					br := bufio.NewReader(c)
					res, err := http.ReadResponse(br, nil)
					if err != nil {
						return
					}
					io.Copy(io.Discard, res.Body)
					res.Body.Close()
					_, err = br.ReadByte()
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						kept.Add(1)
					}
				}()
			}
			readers.Wait()
			retained := kept.Load()
			admitted, _ := rt.gate.Stats()
			t.Logf("candidate=%v idle_retained=%d requests=%d gate=%d accepted_peak=%d", candidate, retained, len(conns), admitted, l.peak.Load())
			if candidate && (retained != 0 || l.peak.Load() > 8) {
				t.Fatal("candidate failed containment")
			}
			if !candidate && retained != 64 {
				t.Fatalf("baseline changed: retained=%d", retained)
			}
		})
	}
}

func TestSpikeWithheldHTTPBody(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		t.Run(fmt.Sprint(candidate), func(t *testing.T) {
			_, s, _ := spikeRouter(t, candidate, func(r *router) http.Handler { return r })
			c, err := net.Dial("tcp", s.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			start := time.Now()
			fmt.Fprint(c, "GET /missing HTTP/1.1\r\nHost: spike\r\nContent-Length: 1\r\n\r\n")
			c.SetReadDeadline(time.Now().Add(900 * time.Millisecond))
			_, err = io.ReadAll(c)
			retained := false
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				retained = true
			}
			t.Logf("candidate=%v body_retained=%v observed_ms=%d", candidate, retained, time.Since(start).Milliseconds())
			if retained == candidate {
				t.Fatal("unexpected retention")
			}
		})
	}
}

func TestSpikeStalledDownstream(t *testing.T) {
	for _, candidate := range []bool{false, true} {
		t.Run(fmt.Sprint(candidate), func(t *testing.T) {
			backendDone := make(chan struct{})
			up := websocket.Upgrader{}
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := up.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer ws.Close()
				defer close(backendDone)
				ws.SetWriteDeadline(time.Now().Add(time.Second))
				payload := make([]byte, 65536)
				for range 1024 {
					if ws.WriteMessage(websocket.BinaryMessage, payload) != nil {
						return
					}
				}
			}))
			defer backend.Close()
			ended := make(chan struct{})
			rt, s, l := spikeRouter(t, candidate, func(r *router) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) { defer close(ended); r.ServeHTTP(w, q) })
			})
			addNode(t, rt, "a", backend.URL)
			d := websocket.Dialer{NetDial: func(network, addr string) (net.Conn, error) {
				c, e := net.Dial(network, addr)
				if e == nil {
					c.(*net.TCPConn).SetReadBuffer(4096)
				}
				return c, e
			}}
			key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
			ws, _, err := d.Dial(strings.Replace(s.URL, "http", "ws", 1)+"/v1/control?publicKey="+key, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			select {
			case <-backendDone:
			case <-time.After(2 * time.Second):
				t.Fatal("backend did not finish")
			}
			start := time.Now()
			released := false
			observation := 450 * time.Millisecond
			if !candidate {
				observation = 5 * time.Second
			}
			select {
			case <-ended:
				released = true
			case <-time.After(observation):
			}
			admitted, _ := rt.gate.Stats()
			t.Logf("candidate=%v proxy_released=%v gate=%d after_backend_close_ms=%d", candidate, released, admitted, time.Since(start).Milliseconds())
			t.Logf("client_writes=%d errors=%d configured_deadline=%v", l.last.Load().writes.Load(), l.last.Load().errors.Load(), l.last.Load().deadline)
			if released != candidate {
				t.Fatal("unexpected proxy cleanup")
			}
		})
	}
}

type spikeAttempt struct {
	base    http.RoundTripper
	timeout time.Duration
}
type spikeBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	timer  *time.Timer
}

func (b *spikeBody) Close() error { b.timer.Stop(); b.cancel(); return b.ReadCloser.Close() }
func (a spikeAttempt) RoundTrip(q *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(q.Context())
	timer := time.AfterFunc(a.timeout, cancel)
	res, err := a.base.RoundTrip(q.Clone(ctx))
	if err != nil {
		timer.Stop()
		cancel()
		return nil, err
	}
	if res.StatusCode == 101 {
		timer.Stop()
		return res, nil
	}
	res.Body = &spikeBody{ReadCloser: res.Body, cancel: cancel, timer: timer}
	return res, nil
}

func TestSpikeUpstreamWaits(t *testing.T) {
	for _, kind := range []string{"tls", "retry-body"} {
		for _, candidate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", kind, candidate), func(t *testing.T) {
				good := newFakeNode(t, 101)
				badURL := ""
				stop := make(chan struct{})
				defer close(stop)
				if kind == "tls" {
					ln, err := net.Listen("tcp", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					defer ln.Close()
					badURL = "https://" + ln.Addr().String()
					go func() {
						for {
							c, e := ln.Accept()
							if e != nil {
								return
							}
							go func() { defer c.Close(); <-stop }()
						}
					}()
				} else {
					bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Length", "4096")
						w.WriteHeader(503)
						w.(http.Flusher).Flush()
						select {
						case <-stop:
						case <-r.Context().Done():
						}
					}))
					defer bad.Close()
					badURL = bad.URL
				}
				rt, s, _ := spikeRouter(t, false, func(r *router) http.Handler { return r })
				u := rt.proxy.Transport.(*upstream)
				if candidate {
					u.base.(*http.Transport).TLSHandshakeTimeout = 100 * time.Millisecond
					u.base = spikeAttempt{base: u.base, timeout: 150 * time.Millisecond}
				}
				key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
				id, _ := directory.EndpointIDFromPublicKey(key)
				addNode(t, rt, "a", badURL, directory.Registration{EndpointID: id, RegistrationID: "r1", Version: 1})
				addNode(t, rt, "b", good.URL)
				ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, "GET", s.URL+"/v1/control?publicKey="+key, nil)
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "websocket")
				req.Header.Set("Sec-WebSocket-Version", "13")
				req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				start := time.Now()
				res, err := http.DefaultClient.Do(req)
				status := 0
				if res != nil {
					status = res.StatusCode
					res.Body.Close()
				}
				t.Logf("candidate=%v kind=%s status=%d duration_ms=%d healthy_hits=%d error=%v", candidate, kind, status, time.Since(start).Milliseconds(), good.hits.Load(), err)
				if candidate && status != 101 {
					t.Fatal("candidate did not retry healthy node")
				}
				if !candidate && status != 0 {
					t.Fatal("baseline unexpectedly bounded")
				}
			})
		}
	}
}

func TestSpikeIdleUpgradedSession(t *testing.T) {
	backend := newFakeNode(t, http.StatusSwitchingProtocols)
	rt, s, _ := spikeRouter(t, true, func(r *router) http.Handler { return r })
	addNode(t, rt, "a", backend.URL)
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	ws, _, err := websocket.DefaultDialer.Dial(strings.Replace(s.URL, "http", "ws", 1)+"/v1/control?publicKey="+key, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	expectChallenge(t, ws)
	timer := time.NewTimer(350 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if err := ws.WriteMessage(websocket.TextMessage, []byte("still-connected")); err != nil {
		t.Fatal(err)
	}
	ws.SetReadDeadline(time.Now().Add(time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil || string(data) != "still-connected" {
		t.Fatal("HTTP deadlines leaked into upgrade", err)
	}
	t.Log("candidate_idle_timeout_ms=100 upgraded_idle_ms=350 echo_preserved=true")
}
