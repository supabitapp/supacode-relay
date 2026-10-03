//go:build dockere2e

package docker

import (
	"fmt"
	mrand "math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func startHosts(t *testing.T, prefix string, n int, opts ...func(*host)) []*host {
	t.Helper()
	hosts := make([]*host, n)
	for i := range hosts {
		hosts[i] = newHost(t, fmt.Sprintf("%s%d", prefix, i), opts...)
		hosts[i].waitRegistered(t, 10*time.Second)
	}
	return hosts
}

func balanced(t *testing.T, hosts []*host, nodes ...string) map[string][]*host {
	t.Helper()
	pl := placements(t, hosts)
	for _, n := range nodes {
		if len(pl[n]) == 0 {
			t.Fatalf("no host placed on %s: %v", n, counts(pl))
		}
	}
	return pl
}

func counts(pl map[string][]*host) map[string]int {
	out := map[string]int{}
	for n, hs := range pl {
		out[n] = len(hs)
	}
	return out
}

func waitReregistered(t *testing.T, hosts []*host, minRegs int, since time.Time, within time.Duration) durations {
	t.Helper()
	var took durations
	for _, h := range hosts {
		for h.registrations() < minRegs || !h.isOnline() {
			if time.Since(since) > within {
				t.Fatalf("host %s not re-registered within %s: regs=%d attempts=%d reasons=%v", h.tag, within, h.registrations(), h.attemptCount(), h.reasons())
			}
			time.Sleep(20 * time.Millisecond)
		}
		at, _ := h.registeredAt(minRegs)
		took = append(took, at.Sub(since))
	}
	return took
}

func allReachable(t *testing.T, hosts []*host, label string, within time.Duration) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, len(hosts))
	for _, h := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := exchangeWithRetry(st.base, h, label+"-"+h.tag, 4, within); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func gapsOf(hosts []*host) durations {
	var d durations
	for _, h := range hosts {
		d = append(d, h.reconnectGaps()...)
	}
	return d
}

func assertUndisturbed(t *testing.T, hosts []*host) {
	t.Helper()
	for _, h := range hosts {
		if r := h.registrations(); r != 1 {
			t.Fatalf("host %s on an unaffected node re-registered %d times", h.tag, r)
		}
	}
}

func acceptFailures(hosts []*host) (int64, []string) {
	var n int64
	var reasons []string
	for _, h := range hosts {
		if f := h.acceptFails.Load(); f > 0 {
			n += f
			reasons = append(reasons, h.reasons()...)
		}
	}
	return n, reasons
}

func TestPublicSurface(t *testing.T) {
	st.baseline(t)
	for _, path := range []string{"/", "/healthz", "/metrics", "/v1/directory", "/v1/control/../healthz", "/debug/pprof/", "/v1/accept/"} {
		if code := httpStatus(t, "GET", st.http+path, nil); code != 404 {
			t.Fatalf("public %s: %d", path, code)
		}
	}
	hdr := http.Header{"Authorization": {"Bearer " + getenv("E2E_DIRECTORY_TOKEN", "compose-e2e-directory-token-change-me")}, "Connection": {"Upgrade"}, "Upgrade": {"websocket"}, "Sec-Websocket-Version": {"13"}, "Sec-Websocket-Key": {"dGhlIHNhbXBsZSBub25jZQ=="}}
	if code := httpStatus(t, "GET", st.http+"/v1/directory", hdr); code != 404 {
		t.Fatalf("public directory upgrade with token: %d", code)
	}
	if code := httpStatus(t, "POST", st.http+"/v1/control", nil); code != 405 {
		t.Fatalf("POST control: %d", code)
	}
	results := map[string]string{}
	for _, target := range []string{
		"http://10.231.2.10:9090/healthz",
		"http://router:9090/metrics",
		"http://10.231.1.10:9090/healthz",
		"http://node-a:8080/v1/connect",
		"http://node-a:9090/metrics",
		"http://10.231.1.11:9090/healthz",
		"http://10.231.1.11:8080/v1/connect",
	} {
		r := st.probe(t, "-mode", "http", "-url", target)
		if r.Error == "" {
			t.Fatalf("internal endpoint %s reachable from the public network: %d", target, r.Status)
		}
		results[target] = r.Error
	}
	if r := st.probe(t, "-mode", "http", "-url", "http://router:8080/healthz"); r.Status != 404 {
		t.Fatalf("router public healthz from public network: %+v", r)
	}
	st.record("publicSurface", results)
}

func TestPairingAcrossThreeNodes(t *testing.T) {
	st.baseline(t)
	hosts := startHosts(t, "pair", 12)
	pl := balanced(t, hosts, "node-a", "node-b", "node-c")
	for n, hs := range pl {
		if len(hs) != 4 {
			t.Fatalf("unbalanced placement %v (node %s)", counts(pl), n)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	var messages atomic.Int64
	start := time.Now()
	for _, h := range hosts {
		for c := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ws, code, err := connect(st.base, h.id)
				if err != nil {
					errs <- fmt.Errorf("connect %s: %d %v", h.tag, code, err)
					return
				}
				defer ws.Close()
				if err := exchange(ws, h, fmt.Sprintf("%s-c%d", h.tag, c), 60); err != nil {
					errs <- err
					return
				}
				messages.Add(120)
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ws, code, err := connect(st.base, h.id)
			if err != nil {
				errs <- fmt.Errorf("connect %s: %d %v", h.tag, code, err)
				return
			}
			defer ws.Close()
			if err := stream(ws, h, h.tag+"-stream", 1000); err != nil {
				errs <- err
				return
			}
			messages.Add(2000)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for _, n := range []string{"node-a", "node-b", "node-c"} {
		if m := st.nodeMetrics(t, n); m["activeHosts"] != 4 {
			t.Fatalf("%s activeHosts %v", n, m["activeHosts"])
		}
	}
	if f, reasons := acceptFailures(hosts); f > 0 {
		t.Fatalf("%d accept failures: %v", f, reasons)
	}
	st.record("pairing", map[string]any{"placement": counts(pl), "messagesVerified": messages.Load(), "seconds": time.Since(start).Seconds()})
}

func TestDuplicateIdentityNewestWins(t *testing.T) {
	st.baseline(t)
	first := newHost(t, "dup-old")
	first.waitRegistered(t, 10*time.Second)
	if _, err := exchangeWithRetry(st.base, first, "before", 2, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	node := first.currentNode()
	second := startHostKey(t, "dup-new", first.priv)
	second.waitRegistered(t, 10*time.Second)
	select {
	case <-first.done:
	case <-time.After(10 * time.Second):
		t.Fatal("older registration was not superseded")
	}
	if codes := first.codes(); len(codes) != 1 || codes[0] != supersededCode {
		t.Fatalf("older registration closed with %v, want [%d]", codes, supersededCode)
	}
	for i := range 5 {
		if _, err := exchangeWithRetry(st.base, second, fmt.Sprintf("after-%d", i), 3, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if second.currentNode() != node {
		t.Fatalf("duplicate registered on %s, owner was %s", second.currentNode(), node)
	}
	m := st.waitNode(t, node, "supersede counted", 5*time.Second, func(m map[string]float64) bool { return m["supersededRegistrations"] >= 1 })
	st.record("duplicateIdentity", map[string]any{"node": node, "supersededRegistrations": m["supersededRegistrations"]})
}

func TestNodeGracefulDrain(t *testing.T) {
	st.baseline(t)
	hosts := startHosts(t, "drain", 9)
	pl := balanced(t, hosts, "node-a", "node-b", "node-c")
	victims := pl["node-b"]
	others := append(slices.Clone(pl["node-a"]), pl["node-c"]...)

	type openPair struct {
		ws *websocket.Conn
		h  *host
	}
	var active []openPair
	for _, h := range victims {
		ws, code, err := connect(st.base, h.id)
		if err != nil {
			t.Fatalf("connect: %d %v", code, err)
		}
		defer ws.Close()
		if err := exchange(ws, h, "pre-drain-"+h.tag, 5); err != nil {
			t.Fatal(err)
		}
		if h.currentNode() != "node-b" {
			t.Fatalf("pair for %s landed on %s", h.tag, h.currentNode())
		}
		active = append(active, openPair{ws, h})
	}
	slow := victims[0]
	slow.acceptDelay.Store(int64(1500 * time.Millisecond))
	pendingWS, code, err := connect(st.base, slow.id)
	if err != nil {
		t.Fatalf("pending connect: %d %v", code, err)
	}
	defer pendingWS.Close()
	acceptsBefore := slow.accepts.Load()

	start := time.Now()
	st.must(t, "kill", "-s", "SIGTERM", "node-b")
	migrated := waitReregistered(t, victims, 2, start, 10*time.Second)
	for _, h := range victims {
		if codes := h.codes(); len(codes) == 0 || codes[0] != websocket.CloseGoingAway {
			t.Fatalf("victim %s control closed with %v, want 1001", h.tag, codes)
		}
	}
	for _, p := range active {
		if err := exchange(p.ws, p.h, "during-drain-"+p.h.tag, 20); err != nil {
			t.Fatalf("active pair broke during drain: %v", err)
		}
	}
	if err := exchange(pendingWS, slow, "accepted-during-drain", 10); err != nil {
		t.Fatalf("pair accepted during drain: %v", err)
	}
	if slow.accepts.Load() != acceptsBefore+1 {
		t.Fatal("delayed accept did not complete")
	}
	slow.acceptDelay.Store(0)
	for _, p := range active {
		p.ws.Close()
	}
	pendingWS.Close()
	st.waitRouter(t, "node-b removed after drain", 15*time.Second, func(m routerMetrics) bool {
		_, ok := m.node("node-b")
		return !ok
	})
	exited := time.Since(start)
	allReachable(t, hosts, "post-drain", 10*time.Second)
	for _, h := range victims {
		if h.currentNode() == "node-b" {
			t.Fatalf("victim %s still served by drained node", h.tag)
		}
	}
	assertUndisturbed(t, others)
	if f, reasons := acceptFailures(hosts); f > 0 {
		t.Fatalf("%d accept failures during drain: %v", f, reasons)
	}
	st.record("gracefulDrain", map[string]any{"victims": len(victims), "reregisteredSinceAction": migrated.summary(), "reconnectGaps": gapsOf(victims).summary(), "nodeGoneAfterMs": exited.Milliseconds()})
}

func TestNodeSIGKILL(t *testing.T) {
	st.baseline(t)
	hosts := startHosts(t, "kill", 9)
	pl := balanced(t, hosts, "node-a", "node-b", "node-c")
	victims := pl["node-c"]
	others := append(slices.Clone(pl["node-a"]), pl["node-b"]...)
	start := time.Now()
	st.must(t, "kill", "-s", "SIGKILL", "node-c")
	migrated := waitReregistered(t, victims, 2, start, 10*time.Second)
	m := st.waitRouter(t, "node-c purged", 10*time.Second, func(m routerMetrics) bool {
		_, ok := m.node("node-c")
		return !ok
	})
	purged := time.Since(start)
	allReachable(t, hosts, "post-kill", 10*time.Second)
	assertUndisturbed(t, others)
	st.record("sigkill", map[string]any{"victims": len(victims), "reregisteredSinceAction": migrated.summary(), "reconnectGaps": gapsOf(victims).summary(), "routerPurgedWithinMs": purged.Milliseconds(), "routerEndpoints": m.Endpoints})
}

func TestNodeSilentPause(t *testing.T) {
	st.baseline(t)
	hosts := startHosts(t, "pause", 9)
	pl := balanced(t, hosts, "node-a", "node-b", "node-c")
	victims := pl["node-a"]
	others := append(slices.Clone(pl["node-b"]), pl["node-c"]...)
	t.Cleanup(func() { _, _ = st.compose("unpause", "node-a") })

	start := time.Now()
	st.must(t, "pause", "node-a")
	migrated := waitReregistered(t, victims, 2, start, 15*time.Second)
	st.waitRouter(t, "paused node-a purged", 10*time.Second, func(m routerMetrics) bool {
		_, ok := m.node("node-a")
		return !ok
	})
	purged := time.Since(start)
	allReachable(t, hosts, "while-paused", 10*time.Second)
	assertUndisturbed(t, others)

	st.must(t, "unpause", "node-a")
	st.waitRouter(t, "node-a rejoined", 15*time.Second, func(m routerMetrics) bool { return m.readyNodes() == 3 })
	stale := st.waitNode(t, "node-a", "stale registrations released", 15*time.Second, func(m map[string]float64) bool { return m["activeHosts"] == 0 })
	rm := st.waitRouter(t, "directory consistent", 10*time.Second, func(m routerMetrics) bool { return m.Endpoints == len(hosts) })
	allReachable(t, hosts, "after-unpause", 10*time.Second)
	for _, h := range victims {
		if h.currentNode() == "node-a" {
			t.Fatalf("victim %s routed back to a stale registration on node-a", h.tag)
		}
	}
	st.record("silentPause", map[string]any{"victims": len(victims), "reregisteredSinceAction": migrated.summary(), "reconnectGaps": gapsOf(victims).summary(), "routerPurgedWithinMs": purged.Milliseconds(), "staleEvictedOnNodeA": stale["evictedRegistrations"], "routerEvictionsSent": rm.EvictionsSent})
}

func TestNodeNetworkLoss(t *testing.T) {
	st.setEnv(map[string]string{"E2E_RELAY_HEARTBEAT_MS": "15000"})
	t.Cleanup(func() { st.setEnv(map[string]string{"E2E_RELAY_HEARTBEAT_MS": ""}) })
	st.baseline(t)
	hosts := startHosts(t, "netloss", 9, abandonStale)
	pl := balanced(t, hosts, "node-a", "node-b", "node-c")
	victims := pl["node-b"]
	others := append(slices.Clone(pl["node-a"]), pl["node-c"]...)
	network := st.project + "_private"
	id := st.container(t, "node-b")
	connected := false
	reconnect := func() {
		if !connected {
			st.docker(t, "network", "connect", "--ip", "10.231.1.12", "--alias", "node-b", network, id)
			connected = true
		}
	}
	t.Cleanup(func() {
		if !connected {
			_, _ = runDocker("network", "connect", "--ip", "10.231.1.12", "--alias", "node-b", network, id)
		}
	})

	start := time.Now()
	st.docker(t, "network", "disconnect", network, id)
	migrated := waitReregistered(t, victims, 2, start, 15*time.Second)
	st.waitRouter(t, "partitioned node-b purged", 10*time.Second, func(m routerMetrics) bool {
		_, ok := m.node("node-b")
		return !ok
	})
	purged := time.Since(start)
	allReachable(t, hosts, "partitioned", 10*time.Second)
	assertUndisturbed(t, others)

	if m := st.nodeMetrics(t, "node-b"); m["activeHosts"] != float64(len(victims)) {
		t.Fatalf("partitioned node-b should still hold %d stale registrations, has %v", len(victims), m["activeHosts"])
	}
	healStart := time.Now()
	reconnect()
	st.waitRouter(t, "node-b rejoined", 20*time.Second, func(m routerMetrics) bool { return m.readyNodes() == 3 })
	stale := st.waitNode(t, "node-b", "stale registrations evicted", 20*time.Second, func(m map[string]float64) bool {
		return m["activeHosts"] == 0 && m["evictedRegistrations"] == float64(len(victims))
	})
	healed := time.Since(healStart)
	rm := st.waitRouter(t, "directory consistent", 10*time.Second, func(m routerMetrics) bool {
		return m.Endpoints == len(hosts) && m.EvictionsSent >= int64(len(victims))
	})
	allReachable(t, hosts, "healed", 10*time.Second)
	newHost(t, "netloss-new").waitRegistered(t, 10*time.Second)
	for _, h := range victims {
		if h.currentNode() == "node-b" {
			t.Fatalf("victim %s routed to its stale registration on node-b", h.tag)
		}
	}
	st.record("networkLoss", map[string]any{"victims": len(victims), "reregisteredSinceAction": migrated.summary(), "reconnectGaps": gapsOf(victims).summary(), "routerPurgedWithinMs": purged.Milliseconds(), "staleEvictedOnNodeB": stale["evictedRegistrations"], "routerEvictionsSent": rm.EvictionsSent, "healedAndEvictedMs": healed.Milliseconds()})
}

func TestMembershipChange(t *testing.T) {
	st.baseline(t)
	original := startHosts(t, "member", 9)
	balanced(t, original, "node-a", "node-b", "node-c")

	var poolMu sync.Mutex
	pool := slices.Clone(original)
	stop := make(chan struct{})
	var ops, retries atomic.Int64
	var failMu sync.Mutex
	var failures []string
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; ; k++ {
				select {
				case <-stop:
					return
				default:
				}
				poolMu.Lock()
				h := pool[mrand.N(len(pool))]
				poolMu.Unlock()
				attempts, err := exchangeWithRetry(st.base, h, fmt.Sprintf("w%d-%d", w, k), 3, 10*time.Second)
				ops.Add(1)
				retries.Add(int64(attempts - 1))
				if err != nil {
					failMu.Lock()
					failures = append(failures, err.Error())
					failMu.Unlock()
				}
			}
		}()
	}
	defer func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
		wg.Wait()
	}()

	time.Sleep(time.Second)
	joinStart := time.Now()
	st.must(t, "--profile", "extra", "up", "-d", "--wait", "node-d")
	st.waitRouter(t, "node-d joined", 15*time.Second, func(m routerMetrics) bool { return m.readyNodes() == 4 })
	joined := time.Since(joinStart)
	assertUndisturbed(t, original)

	added := startHosts(t, "member-new", 6)
	poolMu.Lock()
	pool = append(pool, added...)
	poolMu.Unlock()
	pl := placements(t, added)
	if len(pl["node-d"]) < 3 {
		t.Fatalf("new registrations not steered to the new node: %v", counts(pl))
	}
	onD := pl["node-d"]
	time.Sleep(time.Second)

	leaveStart := time.Now()
	st.must(t, "--profile", "extra", "kill", "-s", "SIGTERM", "node-d")
	migratedD := waitReregistered(t, onD, 2, leaveStart, 15*time.Second)
	st.waitRouter(t, "node-d left", 20*time.Second, func(m routerMetrics) bool {
		_, ok := m.node("node-d")
		return !ok && m.readyNodes() == 3
	})
	left := time.Since(leaveStart)
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()

	all := append(slices.Clone(original), added...)
	allReachable(t, all, "post-membership", 10*time.Second)
	assertUndisturbed(t, original)
	if len(failures) > 0 {
		t.Fatalf("%d of %d workload operations failed: %v", len(failures), ops.Load(), failures[:min(5, len(failures))])
	}
	if f, reasons := acceptFailures(all); f > 0 {
		t.Fatalf("%d accept failures during membership change: %v", f, reasons)
	}
	st.record("membershipChange", map[string]any{"workloadOps": ops.Load(), "connectRetries": retries.Load(), "newHostPlacement": counts(pl), "nodeDJoinMs": joined.Milliseconds(), "nodeDLeaveMs": left.Milliseconds(), "migratedFromD": len(onD), "migratedSinceSIGTERM": migratedD.summary()})
}

func TestRouterRestart(t *testing.T) {
	st.baseline(t)
	hosts := startHosts(t, "rr", 9)
	balanced(t, hosts, "node-a", "node-b", "node-c")

	start := time.Now()
	st.must(t, "restart", "router")
	graceful := waitReregistered(t, hosts, 2, start, 30*time.Second)
	st.waitRouter(t, "router rebuilt directory", 15*time.Second, func(m routerMetrics) bool {
		return m.readyNodes() == 3 && m.Endpoints == len(hosts)
	})
	allReachable(t, hosts, "after-restart", 10*time.Second)

	start = time.Now()
	st.must(t, "kill", "-s", "SIGKILL", "router")
	st.must(t, "up", "-d", "router")
	abrupt := waitReregistered(t, hosts, 3, start, 30*time.Second)
	st.waitRouter(t, "router rebuilt directory after SIGKILL", 15*time.Second, func(m routerMetrics) bool {
		return m.readyNodes() == 3 && m.Endpoints == len(hosts)
	})
	allReachable(t, hosts, "after-kill", 10*time.Second)
	st.record("routerRestart", map[string]any{
		"gracefulRestartReregisteredSinceCommand": graceful.summary(),
		"sigkillReregisteredSinceCommand":         abrupt.summary(),
		"reconnectGaps":                           gapsOf(hosts).summary(),
	})
}

func TestRateLimitingThroughProxy(t *testing.T) {
	t.Cleanup(func() {
		if err := st.collectLogs(); err != nil {
			t.Error(err)
		}
		st.setEnv(map[string]string{"E2E_NODE_RATE": "", "E2E_ROUTER_RATE": ""})
		_, _ = st.compose("up", "-d", "--wait", "router", "node-a", "node-b", "node-c")
	})
	spoof := http.Header{"X-Forwarded-For": {probeIP}, "X-Real-Ip": {probeIP}, "Forwarded": {"for=" + probeIP}}

	st.setEnv(map[string]string{"E2E_NODE_RATE": "5"})
	st.baseline(t)
	start := time.Now()
	var codes []int
	for range 40 {
		codes = append(codes, dialCode(st.base+"/v1/control?publicKey="+randomKey(), spoof))
	}
	elapsed := time.Since(start).Seconds()
	admitted := countOf(codes, 101)
	if codes[len(codes)-1] != 429 || countOf(codes, 429) == 0 || float64(admitted) > 3*(5+5*elapsed)+1 {
		t.Fatalf("node limiter not enforced per client through the router: %v", codes)
	}
	r := st.probe(t, "-mode", "control", "-url", "ws://router:8080", "-n", "3")
	if countOf(r.Codes, 101) != 3 {
		t.Fatalf("spoofed headers consumed another client's node budget or the node keyed on the router address: %v", r.Codes)
	}
	nodeCodes, probeNode := codes, r.Codes

	st.setEnv(map[string]string{"E2E_NODE_RATE": "", "E2E_ROUTER_RATE": "5"})
	st.baseline(t)
	var routerCodes []int
	for range 20 {
		routerCodes = append(routerCodes, dialCode(st.base+"/v1/connect?endpointId="+strings.Repeat("cd", 32), spoof))
	}
	if routerCodes[0] != 404 || routerCodes[len(routerCodes)-1] != 429 {
		t.Fatalf("router limiter not enforced: %v", routerCodes)
	}
	r = st.probe(t, "-mode", "connect", "-url", "ws://router:8080", "-n", "3", "-xff", "203.0.113.9")
	if countOf(r.Codes, 404) != 3 {
		t.Fatalf("router limited a different client or trusted its forwarding headers: %v", r.Codes)
	}
	m, err := st.routerMetrics(t)
	if err != nil || m.RejectedRateLimited == 0 {
		t.Fatalf("router rate limit rejections not counted: %+v %v", m, err)
	}
	st.record("rateLimiting", map[string]any{
		"nodeRate5HostCodes": nodeCodes, "nodeRate5ProbeCodes": probeNode,
		"routerRate5HostCodes": routerCodes, "routerRate5ProbeCodes": r.Codes,
		"routerRejected": m.RejectedRateLimited, "probeIP": probeIP, "spoofedHeaders": "X-Forwarded-For, X-Real-IP, Forwarded = probeIP",
	})
}
