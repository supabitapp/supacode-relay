package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
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

const (
	testToken = "unit-test-directory-token"
	endpointA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testRouter(t *testing.T, env map[string]string) (*router, string, *lockedBuffer) {
	t.Helper()
	vals := map[string]string{"ROUTER_DIRECTORY_TOKEN": testToken, "ROUTER_ADMISSION_RATE": "100000", "ROUTER_REFRESH_TIMEOUT_MS": "2000"}
	for k, v := range env {
		vals[k] = v
	}
	cfg, err := loadConfig(func(k string) string { return vals[k] })
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	rt := newRouter(cfg, logs)
	ts := httptest.NewServer(rt)
	t.Cleanup(ts.Close)
	return rt, "ws://" + ts.Listener.Addr().String(), logs
}

var sessionSeq atomic.Uint64

func addNode(t *testing.T, rt *router, id, addr string, regs ...directory.Registration) uint64 {
	t.Helper()
	s := sessionSeq.Add(1)
	rt.table.Join(id, addr, s, false)
	if _, ok := rt.table.Snapshot(id, s, regs); !ok {
		t.Fatal("snapshot rejected")
	}
	return s
}

type fakeNode struct {
	*httptest.Server
	hits     atomic.Int64
	xff      atomic.Value
	realIP   atomic.Value
	relayIP  atomic.Value
	path     atomic.Value
	status   int
	onReject func()
	mu       sync.Mutex
	conns    []net.Conn
}

func (u *fakeNode) dropAll() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, c := range u.conns {
		c.Close()
	}
}

func newFakeNode(t *testing.T, status int) *fakeNode {
	t.Helper()
	u := &fakeNode{status: status}
	up := websocket.Upgrader{}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		u.xff.Store(r.Header.Get("X-Forwarded-For"))
		u.realIP.Store(r.Header.Get("X-Real-Ip"))
		u.relayIP.Store(r.Header.Get(directory.ClientIPHeader))
		u.path.Store(r.URL.RequestURI())
		if u.status != http.StatusSwitchingProtocols {
			if u.onReject != nil {
				u.onReject()
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(u.status)
			_, _ = io.WriteString(w, `{"error":"upstream says no"}`)
			return
		}
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		u.mu.Lock()
		u.conns = append(u.conns, ws.UnderlyingConn())
		u.mu.Unlock()
		_ = ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","nonce":"n"}`))
		for {
			typ, data, err := ws.ReadMessage()
			if err != nil || ws.WriteMessage(typ, data) != nil {
				return
			}
		}
	}))
	t.Cleanup(u.Close)
	return u
}

func dial(t *testing.T, base, path string, hdr http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	ws, resp, err := d.Dial(base+path, hdr)
	if ws != nil {
		t.Cleanup(func() { ws.Close() })
	}
	return ws, resp, err
}

func expectChallenge(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil || !strings.Contains(string(data), "challenge") {
		t.Fatalf("first frame: %q %v", data, err)
	}
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func TestConnectRetriesOnceAfterRefreshWhenOwnerMoved(t *testing.T) {
	rt, base, _ := testRouter(t, nil)
	good := newFakeNode(t, http.StatusSwitchingProtocols)
	stale := newFakeNode(t, http.StatusNotFound)
	sa := addNode(t, rt, "a", stale.URL, directory.Registration{EndpointID: endpointA, RegistrationID: "r1", Version: 1})
	sb := addNode(t, rt, "b", good.URL)
	stale.onReject = func() {
		rt.table.Del("a", sa, directory.Registration{EndpointID: endpointA, RegistrationID: "r1"})
		rt.table.Put("b", sb, directory.Registration{EndpointID: endpointA, RegistrationID: "r2", Version: 2})
	}
	ws, resp, err := dial(t, base, "/v1/connect?endpointId="+endpointA, nil)
	if err != nil {
		t.Fatalf("connect: %v (%d)", err, statusOf(resp))
	}
	expectChallenge(t, ws)
	if stale.hits.Load() != 1 || good.hits.Load() != 1 || rt.retries[routeConnect].Load() != 1 {
		t.Fatalf("stale=%d good=%d retries=%d", stale.hits.Load(), good.hits.Load(), rt.retries[routeConnect].Load())
	}
}

func TestConnectRetriesAtMostOnce(t *testing.T) {
	rt, base, _ := testRouter(t, nil)
	a := newFakeNode(t, http.StatusNotFound)
	b := newFakeNode(t, http.StatusNotFound)
	sa := addNode(t, rt, "a", a.URL, directory.Registration{EndpointID: endpointA, RegistrationID: "r1", Version: 1})
	sb := addNode(t, rt, "b", b.URL)
	a.onReject = func() {
		rt.table.Put("b", sb, directory.Registration{EndpointID: endpointA, RegistrationID: "r2", Version: 2})
	}
	b.onReject = func() {
		rt.table.Put("a", sa, directory.Registration{EndpointID: endpointA, RegistrationID: "r3", Version: 3})
	}
	_, resp, err := dial(t, base, "/v1/connect?endpointId="+endpointA, nil)
	if err == nil || statusOf(resp) != http.StatusNotFound {
		t.Fatalf("expected 404, got %v %d", err, statusOf(resp))
	}
	if a.hits.Load()+b.hits.Load() != 2 {
		t.Fatalf("upstream attempts %d+%d, want exactly 2", a.hits.Load(), b.hits.Load())
	}
}

func TestConnectMissRefreshesThroughDirectoryBarrier(t *testing.T) {
	rt, base, _ := testRouter(t, nil)
	good := newFakeNode(t, http.StatusSwitchingProtocols)
	priv := httptest.NewServer(rt.private())
	t.Cleanup(priv.Close)

	node, resp, err := websocket.DefaultDialer.Dial("ws://"+priv.Listener.Addr().String()+directory.Path, http.Header{"Authorization": {"Bearer " + testToken}})
	if err != nil {
		t.Fatalf("directory dial: %v (%d)", err, statusOf(resp))
	}
	defer node.Close()
	if err := node.WriteJSON(directory.Message{Type: directory.TypeHello, NodeID: "late", Addr: good.URL}); err != nil {
		t.Fatal(err)
	}
	var welcome directory.Message
	if err := node.ReadJSON(&welcome); err != nil || welcome.Type != directory.TypeWelcome {
		t.Fatalf("welcome: %+v %v", welcome, err)
	}
	if err := node.WriteJSON(directory.Message{Type: directory.TypeSnapshot}); err != nil {
		t.Fatal(err)
	}
	synced := make(chan error, 4)
	go func() {
		for {
			var m directory.Message
			if node.ReadJSON(&m) != nil {
				return
			}
			if m.Type != directory.TypeSync {
				continue
			}
			put := directory.Registration{EndpointID: endpointA, RegistrationID: "late", Version: 10}
			if err := node.WriteJSON(directory.Message{Type: directory.TypePut, Registration: &put}); err != nil {
				synced <- err
				return
			}
			err := node.WriteJSON(directory.Message{Type: directory.TypeSynced, Seq: m.Seq})
			synced <- err
			if err != nil {
				return
			}
		}
	}()
	ws, resp, err := dial(t, base, "/v1/connect?endpointId="+endpointA, nil)
	if err != nil {
		t.Fatalf("connect after barrier: %v (%d)", err, statusOf(resp))
	}
	expectChallenge(t, ws)
	select {
	case err := <-synced:
		if err != nil {
			t.Fatalf("sync barrier: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("router did not run a sync barrier on miss")
	}
	if rt.refreshes.Load() != 1 || rt.retries[routeConnect].Load() != 0 || rt.roundTimeouts.Load() != 0 {
		t.Fatalf("refreshes=%d retries=%d timeouts=%d", rt.refreshes.Load(), rt.retries[routeConnect].Load(), rt.roundTimeouts.Load())
	}
}

func TestAcceptRoutesByConnectionPrefixWithoutRetry(t *testing.T) {
	rt, base, _ := testRouter(t, nil)
	owner := newFakeNode(t, http.StatusSwitchingProtocols)
	other := newFakeNode(t, http.StatusSwitchingProtocols)
	addNode(t, rt, "node-b", owner.URL)
	addNode(t, rt, "node-c", other.URL)
	ws, resp, err := dial(t, base, "/v1/accept?endpointId="+endpointA+"&connectionId=node-b.Zm9v&token=t", nil)
	if err != nil {
		t.Fatalf("accept: %v (%d)", err, statusOf(resp))
	}
	expectChallenge(t, ws)
	if owner.hits.Load() != 1 || other.hits.Load() != 0 {
		t.Fatalf("owner=%d other=%d", owner.hits.Load(), other.hits.Load())
	}

	rejecting := newFakeNode(t, http.StatusNotFound)
	addNode(t, rt, "node-d", rejecting.URL)
	_, resp, err = dial(t, base, "/v1/accept?endpointId="+endpointA+"&connectionId=node-d.Zm9v&token=t", nil)
	if err == nil || statusOf(resp) != http.StatusNotFound || rejecting.hits.Load() != 1 {
		t.Fatalf("accept 404 retried or masked: %v %d hits=%d", err, statusOf(resp), rejecting.hits.Load())
	}
	for _, id := range []string{"Zm9v", "unknown.Zm9v", "Node-B.Zm9v", ".Zm9v"} {
		_, resp, err = dial(t, base, "/v1/accept?endpointId="+endpointA+"&connectionId="+id+"&token=t", nil)
		if err == nil || statusOf(resp) != http.StatusNotFound {
			t.Fatalf("connection id %q: %v %d", id, err, statusOf(resp))
		}
	}
	if owner.hits.Load()+other.hits.Load()+rejecting.hits.Load() != 2 {
		t.Fatal("unroutable accepts reached an upstream")
	}
}

func TestControlRoutingAffinityLoadAndDraining(t *testing.T) {
	rt, _, _ := testRouter(t, nil)
	addNode(t, rt, "a", "http://a:8080")
	sb := addNode(t, rt, "b", "http://b:8080")
	sc := addNode(t, rt, "c", "http://c:8080")
	rt.controls["a"], rt.controls["b"], rt.controls["c"] = 5, 1, 3
	req := httptest.NewRequest("GET", "/v1/control", nil)
	st := &proxyState{route: routeControl, endpointID: endpointA}

	for range 20 {
		if n, _, res := rt.pick(req, st, map[string]bool{}, 0); res != nil || n != "b" {
			t.Fatalf("least loaded pick %q", n)
		}
	}
	rt.table.Put("c", sc, directory.Registration{EndpointID: endpointA, RegistrationID: "r", Version: 1})
	if n, _, _ := rt.pick(req, st, map[string]bool{}, 0); n != "c" {
		t.Fatalf("affinity pick %q, want current owner c", n)
	}
	if n, _, _ := rt.pick(req, st, map[string]bool{"c": true}, 1); n != "b" {
		t.Fatalf("retry pick %q, want next least loaded b", n)
	}
	rt.table.SetDraining("c", sc)
	rt.table.SetDraining("b", sb)
	if n, _, _ := rt.pick(req, st, map[string]bool{}, 0); n != "a" {
		t.Fatalf("pick with draining nodes %q", n)
	}
	if _, _, res := rt.pick(req, st, map[string]bool{"a": true}, 1); res == nil || res.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("expected 503 when no candidates remain")
	}
}

func TestControlRetriesAnotherNodeWhenDrainingOrDown(t *testing.T) {
	rt, base, _ := testRouter(t, nil)
	draining := newFakeNode(t, http.StatusServiceUnavailable)
	good := newFakeNode(t, http.StatusSwitchingProtocols)
	addNode(t, rt, "a", draining.URL)
	addNode(t, rt, "b", good.URL)
	rt.controls["b"] = 10
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	ws, resp, err := dial(t, base, "/v1/control?publicKey="+key, nil)
	if err != nil {
		t.Fatalf("control: %v (%d)", err, statusOf(resp))
	}
	expectChallenge(t, ws)
	if draining.hits.Load() != 1 || good.hits.Load() != 1 {
		t.Fatalf("draining=%d good=%d", draining.hits.Load(), good.hits.Load())
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + ln.Addr().String()
	ln.Close()
	rt2, base2, _ := testRouter(t, nil)
	addNode(t, rt2, "dead", dead)
	addNode(t, rt2, "live", good.URL)
	rt2.controls["live"] = 10
	ws, resp, err = dial(t, base2, "/v1/control?publicKey="+key, nil)
	if err != nil {
		t.Fatalf("control with a dead node: %v (%d)", err, statusOf(resp))
	}
	expectChallenge(t, ws)

	if _, resp, err := dial(t, base, "/v1/control?publicKey="+key+"=", nil); err == nil || statusOf(resp) != http.StatusBadRequest {
		t.Fatalf("noncanonical key: %v %d", err, statusOf(resp))
	}
}

func TestForwardedForIsRewrittenFromTransportPeer(t *testing.T) {
	up := newFakeNode(t, http.StatusSwitchingProtocols)
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	spoof := http.Header{}
	spoof.Set("X-Forwarded-For", "6.6.6.6")
	spoof.Add("X-Forwarded-For", "7.7.7.7")
	spoof.Set("X-Real-IP", "8.8.8.8")
	spoof.Set("Forwarded", "for=9.9.9.9")
	spoof.Set(directory.ClientIPHeader, "5.5.5.5")

	rt, base, _ := testRouter(t, nil)
	addNode(t, rt, "a", up.URL)
	if _, resp, err := dial(t, base, "/v1/control?publicKey="+key, spoof); err != nil {
		t.Fatalf("%v %d", err, statusOf(resp))
	}
	if got := up.xff.Load(); got != "127.0.0.1" {
		t.Fatalf("untrusted peer forwarded X-Forwarded-For %q", got)
	}
	if got := up.realIP.Load(); got != "" {
		t.Fatalf("X-Real-IP passed through: %q", got)
	}
	if got := up.relayIP.Load(); got != "127.0.0.1" {
		t.Fatalf("client IP header %q, want the transport peer", got)
	}

	rt, base, _ = testRouter(t, map[string]string{"ROUTER_TRUSTED_PROXIES": "127.0.0.0/8"})
	addNode(t, rt, "a", up.URL)
	if _, resp, err := dial(t, base, "/v1/control?publicKey="+key, spoof); err != nil {
		t.Fatalf("%v %d", err, statusOf(resp))
	}
	if got := up.xff.Load(); got != "7.7.7.7" {
		t.Fatalf("trusted peer: forwarded %q, want rightmost untrusted hop", got)
	}
	if got := up.path.Load(); got != "/v1/control?publicKey="+key {
		t.Fatalf("upstream request uri %q", got)
	}
}

func TestRouterAdmissionLimits(t *testing.T) {
	up := newFakeNode(t, http.StatusSwitchingProtocols)
	rt, base, _ := testRouter(t, map[string]string{"ROUTER_MAX_CONNS_PER_IP": "2"})
	addNode(t, rt, "a", up.URL)
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	for range 2 {
		if _, resp, err := dial(t, base, "/v1/control?publicKey="+key, nil); err != nil {
			t.Fatalf("%v %d", err, statusOf(resp))
		}
	}
	if _, resp, err := dial(t, base, "/v1/control?publicKey="+key, nil); err == nil || statusOf(resp) != http.StatusTooManyRequests {
		t.Fatalf("per-client concurrency not enforced: %v %d", err, statusOf(resp))
	}

	rt, base, _ = testRouter(t, map[string]string{"ROUTER_ADMISSION_RATE": "2"})
	addNode(t, rt, "a", up.URL)
	var codes []int
	for range 6 {
		ws, resp, err := dial(t, base, "/v1/control?publicKey="+key, http.Header{"X-Forwarded-For": {fmt.Sprint(time.Now().UnixNano())}})
		if err == nil {
			ws.Close()
			codes = append(codes, 101)
			continue
		}
		codes = append(codes, statusOf(resp))
	}
	if codes[0] != 101 || codes[5] != http.StatusTooManyRequests || rt.rejectedRate.Load() == 0 {
		t.Fatalf("rate limit codes %v", codes)
	}
}

func TestPublicListenerServesOnlyProtocolPaths(t *testing.T) {
	rt, _, _ := testRouter(t, nil)
	for _, c := range []struct {
		method, path string
		want         int
	}{
		{"GET", "/", 404},
		{"GET", "/healthz", 404},
		{"GET", "/metrics", 404},
		{"GET", directory.Path, 404},
		{"GET", "/v1/control/../healthz", 404},
		{"GET", "/v1/control/", 404},
		{"POST", "/v1/connect", 405},
		{"GET", "/v1/connect?endpointId=nothex", 404},
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(c.method, "http://router"+c.path, nil)
		r.URL.Path = strings.SplitN(c.path, "?", 2)[0]
		rt.ServeHTTP(w, r)
		if w.Code != c.want {
			t.Errorf("%s %s: %d want %d", c.method, c.path, w.Code, c.want)
		}
	}
}

func TestLogsRedactQueryStringsAndIdentifiers(t *testing.T) {
	for _, in := range []string{
		`dial "http://node-a:8080/v1/accept?endpointId=` + endpointA + `&connectionId=node-a.c2VjcmV0&token=dG9rZW4"`,
		`GET /v1/control?publicKey=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA failed`,
		`lookup ` + endpointA + ` failed`,
	} {
		out := redact(in)
		for _, leak := range []string{endpointA, "c2VjcmV0", "dG9rZW4", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} {
			if strings.Contains(out, leak) {
				t.Fatalf("redact(%q) = %q leaks %q", in, out, leak)
			}
		}
	}

	rt, base, logs := testRouter(t, nil)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := "http://" + ln.Addr().String()
	ln.Close()
	addNode(t, rt, "node-a", dead)
	_, resp, err := dial(t, base, "/v1/accept?endpointId="+endpointA+"&connectionId=node-a.c2VjcmV0&token=dG9rZW4", nil)
	if err == nil || statusOf(resp) != http.StatusBadGateway {
		t.Fatalf("expected 502, got %v %d", err, statusOf(resp))
	}
	rt.log.Printf("router: upstream error: Get %q", dead+"/v1/accept?endpointId="+endpointA+"&token=dG9rZW4")
	out := logs.String()
	if !strings.Contains(out, "accept upstream error") {
		t.Fatalf("missing upstream error log:\n%s", out)
	}
	for _, leak := range []string{endpointA, "c2VjcmV0", "dG9rZW4"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaked %q:\n%s", leak, out)
		}
	}
}

func TestFirstFrameInUpgradeSegmentIsDelivered(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	payload := []byte(`{"type":"challenge","nonce":"same-segment"}`)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				req, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				h := sha1.Sum([]byte(req.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				var buf bytes.Buffer
				fmt.Fprintf(&buf, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(h[:]))
				buf.Write([]byte{0x81, byte(len(payload))})
				buf.Write(payload)
				if _, err := c.Write(buf.Bytes()); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, c)
			}()
		}
	}()
	rt, base, _ := testRouter(t, nil)
	addNode(t, rt, "a", "http://"+ln.Addr().String())
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	ws, resp, err := dial(t, base, "/v1/control?publicKey="+key, nil)
	if err != nil {
		t.Fatalf("%v %d", err, statusOf(resp))
	}
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, got, err := ws.ReadMessage()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("first frame lost or altered: %q %v", got, err)
	}
}

func TestHalfClosedClientIsReleased(t *testing.T) {
	up := newFakeNode(t, http.StatusSwitchingProtocols)
	rt, base, _ := testRouter(t, nil)
	addNode(t, rt, "a", up.URL)
	key := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	ws, resp, err := dial(t, base, "/v1/control?publicKey="+key, nil)
	if err != nil {
		t.Fatalf("%v %d", err, statusOf(resp))
	}
	expectChallenge(t, ws)
	up.dropAll()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = ws.ReadMessage()
	var ce *websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != websocket.CloseAbnormalClosure {
		t.Fatalf("expected abnormal closure from half-close, got %v", err)
	}
	start := time.Now()
	for rt.activeTotal() > 0 && time.Since(start) < 2*halfCloseGrace {
		time.Sleep(10 * time.Millisecond)
	}
	if rt.activeTotal() != 0 {
		t.Fatal("router kept a half-closed client socket open")
	}
}

func TestMetricsExposeNoIdentifiers(t *testing.T) {
	rt, _, _ := testRouter(t, nil)
	s := addNode(t, rt, "node-a", "http://node-a:8080", directory.Registration{EndpointID: endpointA, RegistrationID: "regsecret", Version: 1})
	_ = s
	w := httptest.NewRecorder()
	rt.private().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, leak := range []string{endpointA, "regsecret", "node-a:8080"} {
		if strings.Contains(body, leak) {
			t.Fatalf("metrics leaked %q: %s", leak, body)
		}
	}
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil || m["endpoints"] != float64(1) {
		t.Fatalf("metrics %s %v", body, err)
	}
}
