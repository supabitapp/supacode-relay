package e2e

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

const directoryToken = "e2e-directory-token-0123456789"

type cluster struct {
	t      *testing.T
	router *relay
	nodes  map[string]*relay
}

func startCluster(t *testing.T, ids []string, nodeEnv ...string) *cluster {
	t.Helper()
	c := &cluster{t: t, nodes: map[string]*relay{}}
	c.router = startBinary(t, routerBin, []string{
		"ROUTER_ADDR=127.0.0.1:0",
		"ROUTER_PRIVATE_ADDR=127.0.0.1:0",
		"ROUTER_ADMISSION_RATE=100000",
		"ROUTER_DIRECTORY_TOKEN=" + directoryToken,
		"ROUTER_DIRECTORY_HEARTBEAT_MS=200",
	})
	for _, id := range ids {
		c.addNode(id, nodeEnv...)
	}
	c.waitReady(len(ids))
	return c
}

func (c *cluster) addNode(id string, env ...string) *relay {
	c.t.Helper()
	n := startRelay(c.t, append([]string{
		"RELAY_NODE_ID=" + id,
		"RELAY_PRIVATE_ADDR=127.0.0.1:0",
		"RELAY_ROUTERS=http://" + c.router.privAddr,
		"RELAY_DIRECTORY_TOKEN=" + directoryToken,
		"RELAY_DIRECTORY_HEARTBEAT_MS=200",
		"RELAY_DIRECTORY_RETRY_MAX_MS=300",
		"RELAY_TRUSTED_PROXIES=127.0.0.1/32",
	}, env...)...)
	c.nodes[id] = n
	return n
}

type routerNode struct {
	ID        string
	Ready     bool
	Draining  bool
	Endpoints int
}

type routerMetrics struct {
	Nodes          []routerNode
	Endpoints      int
	Retries        map[string]int
	Misses         map[string]int
	Refreshes      int
	EvictionsSent  int
	UpstreamErrors int
}

func (c *cluster) metrics() routerMetrics {
	c.t.Helper()
	_, body := c.router.get("/metrics")
	var m routerMetrics
	if err := json.Unmarshal(body, &m); err != nil {
		c.t.Fatal(err)
	}
	return m
}

func (c *cluster) waitRouter(desc string, pred func(routerMetrics) bool) routerMetrics {
	c.t.Helper()
	end := time.Now().Add(deadline)
	for {
		m := c.metrics()
		if pred(m) {
			return m
		}
		if time.Now().After(end) {
			c.t.Fatalf("timed out waiting for %s: %+v", desc, m)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (c *cluster) waitReady(n int) {
	c.t.Helper()
	c.waitRouter(fmt.Sprintf("%d ready nodes", n), func(m routerMetrics) bool {
		ready := 0
		for _, node := range m.Nodes {
			if node.Ready && !node.Draining {
				ready++
			}
		}
		return ready == n
	})
}

func nodeOf(connectionID string) string {
	node, _, _ := strings.Cut(connectionID, ".")
	return node
}

type echoHost struct {
	*endpoint.Host
	tag   string
	nodes chan string
}

func serveEcho(t *testing.T, base string, priv ed25519.PrivateKey, tag string) *echoHost {
	t.Helper()
	h, err := endpoint.Register(base, priv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Control.Close() })
	e := &echoHost{Host: h, tag: tag, nodes: make(chan string, 1024)}
	go func() {
		for ev := range h.Events {
			if ev.Type != "incoming" {
				continue
			}
			select {
			case e.nodes <- nodeOf(ev.ConnectionID):
			default:
			}
			go func() {
				ws, _, err := h.Accept(ev)
				if err != nil {
					return
				}
				defer ws.Close()
				for {
					typ, data, err := ws.ReadMessage()
					if err != nil {
						return
					}
					if ws.WriteMessage(typ, append([]byte(tag+":"), data...)) != nil {
						return
					}
				}
			}()
		}
	}()
	return e
}

func (e *echoHost) node(t *testing.T, base string) string {
	t.Helper()
	ws, resp, err := endpoint.Connect(base, e.ID)
	if err != nil {
		t.Fatalf("connect: %v (%s)", err, status(resp))
	}
	defer ws.Close()
	select {
	case n := <-e.nodes:
		return n
	case <-time.After(deadline):
		t.Fatal("no incoming event")
		return ""
	}
}

func exchange(ws *websocket.Conn, tag, label string, rounds int) error {
	for k := range rounds {
		typ := websocket.TextMessage
		payload := []byte(fmt.Sprintf("%s-%d", label, k))
		if k%2 == 1 {
			typ = websocket.BinaryMessage
			payload = append([]byte{0, 0xff, byte(k)}, payload...)
		}
		_ = ws.SetWriteDeadline(time.Now().Add(deadline))
		if err := ws.WriteMessage(typ, payload); err != nil {
			return err
		}
		_ = ws.SetReadDeadline(time.Now().Add(deadline))
		gotTyp, got, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		if want := append([]byte(tag+":"), payload...); gotTyp != typ || string(got) != string(want) {
			return fmt.Errorf("%s round %d: got type=%d %q want type=%d %q", label, k, gotTyp, got, typ, want)
		}
	}
	return nil
}

func TestClusterPairsAcrossNodes(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b", "node-c"})
	const hosts, clients, rounds = 9, 3, 40
	hs := make([]*echoHost, hosts)
	for i := range hs {
		hs[i] = serveEcho(t, c.router.base, newKey(t), fmt.Sprintf("h%d", i))
	}
	placement := map[string]int{}
	for _, h := range hs {
		placement[h.node(t, c.router.base)]++
	}
	if len(placement) != 3 || placement["node-a"] != 3 || placement["node-b"] != 3 || placement["node-c"] != 3 {
		t.Fatalf("hosts not spread across nodes: %v", placement)
	}

	var wg sync.WaitGroup
	errs := make(chan error, hosts*clients)
	for i, h := range hs {
		for k := range clients {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ws, resp, err := endpoint.Connect(c.router.base, h.ID)
				if err != nil {
					errs <- fmt.Errorf("connect %d/%d: %v (%s)", i, k, err, status(resp))
					return
				}
				defer ws.Close()
				if err := exchange(ws, h.tag, fmt.Sprintf("c%d-%d", i, k), rounds); err != nil {
					errs <- err
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for id, n := range c.nodes {
		if m := n.metrics(); m["activeHosts"] != 3 {
			t.Fatalf("%s hosts %v", id, m["activeHosts"])
		}
	}
}

func TestClusterConnectSeesFreshRegistration(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b"})
	for i := range 30 {
		n := c.nodes[[]string{"node-a", "node-b"}[i%2]]
		h := serveEcho(t, n.base, newKey(t), "fresh")
		ws, resp, err := endpoint.Connect(c.router.base, h.ID)
		if err != nil {
			t.Fatalf("iteration %d: connect right after direct registration failed: %v (%s)", i, err, status(resp))
		}
		if err := exchange(ws, "fresh", "probe", 1); err != nil {
			t.Fatal(err)
		}
		ws.Close()
		h.Control.Close()
	}
	t.Logf("router metrics after fresh registrations: %+v", c.metrics())
}

func TestClusterDuplicateRegistrationAcrossNodes(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b"})
	priv := newKey(t)
	old := serveEcho(t, c.nodes["node-a"].base, priv, "old")
	if n := old.node(t, c.router.base); n != "node-a" {
		t.Fatalf("old host routed to %s", n)
	}
	fresh := serveEcho(t, c.nodes["node-b"].base, priv, "fresh")

	expectControlClose(t, old.Host, 4001, "registration superseded")
	c.nodes["node-a"].waitMetrics("node-a released stale host", func(m map[string]float64) bool {
		return m["activeHosts"] == 0 && m["evictedRegistrations"] == 1
	})
	for range 5 {
		if n := fresh.node(t, c.router.base); n != "node-b" {
			t.Fatalf("connect routed to stale owner %s", n)
		}
	}
	if m := c.metrics(); m.Endpoints != 1 || m.EvictionsSent < 1 {
		t.Fatalf("unexpected router state %+v", m)
	}

	again := serveEcho(t, c.router.base, priv, "again")
	expectControlClose(t, fresh.Host, 4001, "registration superseded")
	if n := again.node(t, c.router.base); n != "node-b" {
		t.Fatalf("re-registration through router left owner node: %s", n)
	}
	c.nodes["node-b"].waitMetrics("node-b superseded locally", func(m map[string]float64) bool {
		return m["activeHosts"] == 1 && m["supersededRegistrations"] == 1
	})
}

func TestClusterNodeLossPurgesDirectory(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b"})
	priv := newKey(t)
	h := serveEcho(t, c.router.base, priv, "h")
	owner := h.node(t, c.router.base)
	other := map[string]string{"node-a": "node-b", "node-b": "node-a"}[owner]

	start := time.Now()
	_ = c.nodes[owner].cmd.Process.Kill()
	select {
	case <-h.Done:
	case <-time.After(deadline):
		t.Fatal("host control survived owner SIGKILL")
	}
	c.waitRouter("owner purged", func(m routerMetrics) bool { return m.Endpoints == 0 && len(m.Nodes) == 1 })
	t.Logf("owner purged %s after SIGKILL", time.Since(start).Round(time.Millisecond))
	_, resp, err := endpoint.Connect(c.router.base, h.ID)
	expectStatus(t, resp, err, 404)

	moved := serveEcho(t, c.router.base, priv, "moved")
	if n := moved.node(t, c.router.base); n != other {
		t.Fatalf("re-registered host landed on %s", n)
	}
}

func TestClusterDrainReleasesHostsAndKeepsPairs(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b"}, "RELAY_PAIR_TIMEOUT_MS=10000")
	priv := newKey(t)
	h, err := endpoint.Register(c.nodes["node-a"].base, priv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Control.Close() })

	activeClient, ev := connect(t, c.router, h)
	if nodeOf(ev.ConnectionID) != "node-a" {
		t.Fatalf("connection id %q not prefixed with owner", ev.ConnectionID)
	}
	activeHost, resp, err := endpoint.Accept(c.router.base, h.ID, ev.ConnectionID, ev.Token)
	if err != nil {
		t.Fatalf("accept through router: %v (%s)", err, status(resp))
	}
	defer activeHost.Close()
	pendingClient, pendingEv := connect(t, c.router, h)

	c.nodes["node-a"].signal(syscall.SIGTERM)
	expectControlClose(t, h, websocket.CloseGoingAway, "relay draining")
	moved := serveEcho(t, c.router.base, priv, "moved")
	if n := moved.node(t, c.router.base); n != "node-b" {
		t.Fatalf("host re-registered on %s during drain", n)
	}

	for i := range 20 {
		m := message{websocket.BinaryMessage, []byte{byte(i), 0xfe}}
		send(t, activeClient, m)
		expectMessage(t, activeHost, m)
		send(t, activeHost, m)
		expectMessage(t, activeClient, m)
	}
	pendingHost, resp, err := endpoint.Accept(c.router.base, h.ID, pendingEv.ConnectionID, pendingEv.Token)
	if err != nil {
		t.Fatalf("accept on draining node: %v (%s)", err, status(resp))
	}
	defer pendingHost.Close()
	m := message{websocket.TextMessage, []byte("accepted while draining")}
	send(t, pendingClient, m)
	expectMessage(t, pendingHost, m)

	activeClient.Close()
	pendingClient.Close()
	if err := c.nodes["node-a"].waitExit(deadline); err != nil {
		t.Fatalf("drained node exit: %v\n%s", err, c.nodes["node-a"].stderr)
	}
	c.waitRouter("drained node left", func(m routerMetrics) bool { return len(m.Nodes) == 1 && m.Endpoints == 1 })
}

func TestClusterPublicSurface(t *testing.T) {
	c := startCluster(t, []string{"node-a"})
	for _, path := range []string{"/", "/healthz", "/metrics/", "/v1/directory", "/v1/control/../healthz", "/v1/controlx", "/V1/control"} {
		resp, err := http.Get("http://" + c.router.addr + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatalf("public router %s: %d", path, resp.StatusCode)
		}
	}
	resp, err := http.Get("http://" + c.router.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var metrics routerMetrics
	err = json.NewDecoder(resp.Body).Decode(&metrics)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || err != nil || len(metrics.Nodes) != 1 || !metrics.Nodes[0].Ready {
		t.Fatalf("public metrics: status %d, metrics %+v, error %v", resp.StatusCode, metrics, err)
	}
	resp, err = http.Post("http://"+c.router.addr+"/v1/control", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Fatalf("POST control: %d", resp.StatusCode)
	}
	for _, tok := range []string{"", "Bearer wrong-token-0123456789", "Basic " + directoryToken, directoryToken} {
		req, _ := http.NewRequest("GET", "http://"+c.router.privAddr+"/v1/directory", nil)
		if tok != "" {
			req.Header.Set("Authorization", tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Fatalf("directory with %q: %d", tok, resp.StatusCode)
		}
	}
	node := c.nodes["node-a"]
	for _, path := range []string{"/healthz", "/metrics"} {
		resp, err := http.Get("http://" + node.addr + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatalf("node public %s: %d", path, resp.StatusCode)
		}
	}
	if code, _ := node.get("/healthz"); code != 200 {
		t.Fatalf("node private healthz %d", code)
	}
	if code, _ := c.router.get("/healthz"); code != 200 {
		t.Fatalf("router private healthz %d", code)
	}
}

func TestClusterForwardedForCannotBypassLimits(t *testing.T) {
	c := startCluster(t, []string{"node-a"}, "RELAY_ADMISSION_RATE=5", "RELAY_CLIENT_IP_HEADER=X-Relay-Client-Ip")
	var codes []int
	for i := range 20 {
		hdr := http.Header{}
		hdr.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i+1))
		hdr.Set("X-Real-IP", fmt.Sprintf("203.0.113.%d", i+1))
		hdr.Set("Forwarded", fmt.Sprintf("for=192.0.2.%d", i+1))
		hdr.Set("X-Relay-Client-Ip", fmt.Sprintf("192.0.2.%d", i+1))
		ws, resp, err := endpoint.Dialer.Dial(c.router.base+"/v1/control?publicKey="+endpoint.B64(newKey(t).Public().(ed25519.PublicKey)), hdr)
		if err == nil {
			ws.Close()
			codes = append(codes, 101)
			continue
		}
		if resp == nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		codes = append(codes, resp.StatusCode)
	}
	if codes[0] != 101 || codes[len(codes)-1] != 429 {
		t.Fatalf("spoofed forwarding headers bypassed the node limiter: %v", codes)
	}

	strict := startBinary(t, routerBin, []string{
		"ROUTER_ADDR=127.0.0.1:0", "ROUTER_PRIVATE_ADDR=127.0.0.1:0", "ROUTER_ADMISSION_RATE=3",
		"ROUTER_DIRECTORY_TOKEN=" + directoryToken,
	})
	codes = nil
	for i := range 10 {
		hdr := http.Header{}
		hdr.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i+1))
		_, resp, err := endpoint.Dialer.Dial(strict.base+"/v1/connect?endpointId="+strings.Repeat("ab", 32), hdr)
		if err == nil || resp == nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		codes = append(codes, resp.StatusCode)
	}
	if codes[0] != 404 || codes[len(codes)-1] != 429 {
		t.Fatalf("spoofed forwarding headers bypassed the router limiter: %v", codes)
	}
}

func TestClusterLogsContainNoIdentifiers(t *testing.T) {
	c := startCluster(t, []string{"node-a", "node-b"})
	var secrets []string
	var nonce string
	for i := range 3 {
		priv := newKey(t)
		ws, ch, _, err := endpoint.DialControl(c.router.base, endpoint.B64(priv.Public().(ed25519.PublicKey)))
		if err != nil {
			t.Fatal(err)
		}
		nonce = ch.Nonce
		ws.Close()
		h, err := endpoint.Register(c.router.base, priv)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Control.Close() })
		secrets = append(secrets, h.ID, endpoint.B64(priv.Public().(ed25519.PublicKey)), nonce)
		client, ev := connect(t, c.router, h)
		secrets = append(secrets, ev.ConnectionID, ev.Token)
		_, resp, err := endpoint.Accept(c.router.base, h.ID, ev.ConnectionID, "wrong-"+ev.Token[6:])
		expectStatus(t, resp, err, 403)
		host, resp, err := endpoint.Accept(c.router.base, h.ID, ev.ConnectionID, ev.Token)
		if err != nil {
			t.Fatalf("accept: %v (%s)", err, status(resp))
		}
		send(t, client, message{websocket.TextMessage, []byte("x")})
		recv(t, host)
		client.Close()
		host.Close()
		if i == 2 {
			h.Control.Close()
		}
	}
	unknown := endpoint.EndpointID(randomBytes(32))
	secrets = append(secrets, unknown)
	_, resp, err := endpoint.Connect(c.router.base, unknown)
	expectStatus(t, resp, err, 404)

	victim := register(t, c.router)
	client, ev := connect(t, c.router, victim)
	client.Close()
	secrets = append(secrets, victim.ID, ev.ConnectionID, ev.Token)
	_ = c.nodes[nodeOf(ev.ConnectionID)].cmd.Process.Kill()
	_, resp, err = endpoint.Accept(c.router.base, victim.ID, ev.ConnectionID, ev.Token)
	expectStatus(t, resp, err, 502)
	c.waitRouter("killed node purged", func(m routerMetrics) bool { return len(m.Nodes) == 1 })

	c.router.signal(syscall.SIGTERM)
	_ = c.router.waitExit(10 * time.Second)
	for id, n := range c.nodes {
		n.stop()
		for _, s := range secrets {
			if strings.Contains(n.stderr.String(), s) {
				t.Fatalf("%s log leaked %q:\n%s", id, s, n.stderr)
			}
		}
	}
	logs := c.router.stderr.String()
	if !strings.Contains(logs, "accept upstream error") {
		t.Fatalf("expected an upstream error to be logged:\n%s", logs)
	}
	for _, s := range secrets {
		if strings.Contains(logs, s) {
			t.Fatalf("router log leaked %q:\n%s", s, logs)
		}
	}
}

func expectControlClose(t *testing.T, h *endpoint.Host, code int, reason string) {
	t.Helper()
	select {
	case <-h.Done:
	case <-time.After(deadline):
		t.Fatalf("control not closed, want %d %q", code, reason)
	}
	var ce *websocket.CloseError
	if !errors.As(h.Err, &ce) || ce.Code != code || ce.Text != reason {
		t.Fatalf("control closed with %v, want %d %q", h.Err, code, reason)
	}
}
