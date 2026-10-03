package main

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

const token = "pathbench-directory-token-0123456789"

var (
	directURL = flag.String("direct", "", "ws base URL of a relay node reached directly")
	routedURL = flag.String("routed", "", "ws base URL of a router in front of relay nodes")
	spawnBins = flag.Bool("spawn", false, "start a router and one node on loopback and measure both paths")
	relayBin  = flag.String("relay-bin", "bin/relay", "relay binary for -spawn")
	routerBin = flag.String("router-bin", "bin/relay-router", "router binary for -spawn")
	pathSel   = flag.String("path", "both", "direct, routed, or both")
	payloads  = flag.String("payloads", "64,1024,65536", "payload sizes in bytes")
	clients   = flag.String("clients", "1,32", "concurrent client counts")
	inflight  = flag.Int("inflight", 4, "messages in flight per client")
	warmup    = flag.Duration("warmup", 2*time.Second, "warmup per case")
	duration  = flag.Duration("duration", 5*time.Second, "measurement window per case")
	hostCount = flag.Int("hosts", 4, "echo hosts per path")
)

type dist struct {
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type usage struct {
	CPUPercent float64 `json:"cpuPercentOfOneCore"`
	RSSPeakMiB float64 `json:"rssPeakMiB"`
}

type result struct {
	Path      string           `json:"path"`
	Payload   int              `json:"payloadBytes"`
	Clients   int              `json:"clients"`
	Messages  int64            `json:"messagesInWindow"`
	MsgPerSec float64          `json:"messagesPerSec"`
	MiBPerSec float64          `json:"payloadMiBPerSec"`
	RTT       dist             `json:"rttMicros"`
	Failures  int64            `json:"failures"`
	Corrupt   int64            `json:"corrupt"`
	Processes map[string]usage `json:"processes,omitempty"`
}

type target struct {
	name  string
	base  string
	hosts []*endpoint.Host
	pids  map[string]int
}

func main() {
	flag.Parse()
	var procs []*exec.Cmd
	defer func() {
		for _, p := range procs {
			_ = p.Process.Signal(syscall.SIGTERM)
			_, _ = p.Process.Wait()
		}
	}()
	pids := map[string]int{}
	if *spawnBins {
		var err error
		procs, err = spawnLocal(pids)
		if err != nil {
			fail(err)
		}
	}
	var targets []*target
	for _, p := range []struct{ name, base string }{{"direct", *directURL}, {"routed", *routedURL}} {
		if *pathSel != "both" && *pathSel != p.name {
			continue
		}
		if p.base == "" {
			fail(fmt.Errorf("-%s is required", p.name))
		}
		hosts, err := serveEcho(p.base, *hostCount)
		if err != nil {
			fail(fmt.Errorf("%s hosts: %w", p.name, err))
		}
		tg := &target{name: p.name, base: p.base, hosts: hosts, pids: map[string]int{}}
		if pid, ok := pids["node"]; ok {
			tg.pids["node"] = pid
		}
		if pid, ok := pids["router"]; ok && p.name == "routed" {
			tg.pids["router"] = pid
		}
		targets = append(targets, tg)
	}
	sizes, err := ints(*payloads)
	if err != nil {
		fail(err)
	}
	counts, err := ints(*clients)
	if err != nil {
		fail(err)
	}
	var results []result
	for _, size := range sizes {
		for _, n := range counts {
			for _, tg := range targets {
				r, err := run(tg, size, n)
				if err != nil {
					fail(fmt.Errorf("%s %dB x%d: %w", tg.name, size, n, err))
				}
				fmt.Fprintf(os.Stderr, "%-6s %6dB x%-3d p50=%.0fus p99=%.0fus %8.0f msg/s %7.1f MiB/s %v fail=%d corrupt=%d\n", r.Path, r.Payload, r.Clients, r.RTT.P50, r.RTT.P99, r.MsgPerSec, r.MiBPerSec, r.Processes, r.Failures, r.Corrupt)
				results = append(results, r)
				time.Sleep(300 * time.Millisecond)
			}
		}
	}
	out, _ := json.Marshal(map[string]any{
		"direct": *directURL, "routed": *routedURL, "spawned": *spawnBins,
		"warmupSec": warmup.Seconds(), "durationSec": duration.Seconds(), "inflightPerClient": *inflight, "hosts": *hostCount,
		"driver":  map[string]any{"goos": runtime.GOOS, "goarch": runtime.GOARCH, "cpus": runtime.NumCPU(), "go": runtime.Version()},
		"results": results,
	})
	fmt.Println(string(out))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "pathbench:", err)
	os.Exit(1)
}

func ints(v string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid list %q", v)
		}
		out = append(out, n)
	}
	return out, nil
}

func spawnLocal(pids map[string]int) ([]*exec.Cmd, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	private := ln.Addr().String()
	ln.Close()
	router, routerAddr, err := spawn(*routerBin, "ROUTER_ADDR=127.0.0.1:0", "ROUTER_PRIVATE_ADDR="+private, "ROUTER_DIRECTORY_TOKEN="+token, "ROUTER_ADMISSION_RATE=1000000", "ROUTER_MAX_CONNS_PER_IP=100000")
	if err != nil {
		return nil, err
	}
	node, nodeAddr, err := spawn(*relayBin, "RELAY_ADDR=127.0.0.1:0", "RELAY_NODE_ID=bench", "RELAY_ROUTERS=http://"+private, "RELAY_DIRECTORY_TOKEN="+token, "RELAY_TRUSTED_PROXIES=127.0.0.1/32", "RELAY_ADMISSION_RATE=1000000")
	if err != nil {
		return []*exec.Cmd{router}, err
	}
	procs := []*exec.Cmd{node, router}
	pids["router"], pids["node"] = router.Process.Pid, node.Process.Pid
	*directURL, *routedURL = "ws://"+nodeAddr, "ws://"+routerAddr
	end := time.Now().Add(10 * time.Second)
	for time.Now().Before(end) {
		if resp, err := http.Get("http://" + private + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return procs, nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return procs, errors.New("router never reported a ready node")
}

func spawn(bin string, env ...string) (*exec.Cmd, string, error) {
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	if err := cmd.Start(); err != nil {
		return nil, "", err
	}
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		var ev struct{ Event, Address, Listener string }
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Event == "listening" && ev.Listener != "private" {
			go func() {
				for sc.Scan() {
				}
			}()
			return cmd, ev.Address, nil
		}
	}
	return cmd, "", fmt.Errorf("%s did not report listening", bin)
}

func serveEcho(base string, n int) ([]*endpoint.Host, error) {
	var hosts []*endpoint.Host
	for range n {
		_, priv, _ := ed25519.GenerateKey(rand.Reader)
		h, err := endpoint.Register(base, priv)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
		go func() {
			for ev := range h.Events {
				if ev.Type != "incoming" {
					continue
				}
				go func() {
					ws, _, err := h.Accept(ev)
					if err != nil {
						return
					}
					defer ws.Close()
					ws.SetReadLimit(1 << 20)
					for {
						typ, data, err := ws.ReadMessage()
						if err != nil || ws.WriteMessage(typ, data) != nil {
							return
						}
					}
				}()
			}
		}()
	}
	return hosts, nil
}

type client struct {
	ws       *websocket.Conn
	rtts     []int64
	recv     int64
	bytes    int64
	sent     atomic.Int64
	got      atomic.Int64
	failures int64
	corrupt  int64
}

func run(tg *target, size, n int) (result, error) {
	res := result{Path: tg.name, Payload: size, Clients: n}
	block := make([]byte, max(size, 16))
	_, _ = rand.Read(block)
	cs := make([]*client, n)
	var wg sync.WaitGroup
	var dialErr atomic.Value
	for i := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ws, resp, err := endpoint.Connect(tg.base, tg.hosts[i%len(tg.hosts)].ID)
			if err != nil {
				if resp != nil {
					err = fmt.Errorf("%w (HTTP %d)", err, resp.StatusCode)
				}
				dialErr.Store(err)
				return
			}
			ws.SetReadLimit(1 << 20)
			cs[i] = &client{ws: ws}
		}()
	}
	wg.Wait()
	defer func() {
		for _, c := range cs {
			if c != nil {
				c.ws.Close()
			}
		}
	}()
	if err, _ := dialErr.Load().(error); err != nil {
		return res, err
	}
	base := time.Now()
	mStart := (*warmup).Nanoseconds()
	mEnd := (*warmup + *duration).Nanoseconds()
	stopAt := base.Add(*warmup + *duration)
	samplers := make(chan map[string]*sampler, 1)
	go func() {
		time.Sleep(*warmup)
		m := map[string]*sampler{}
		for name, pid := range tg.pids {
			m[name] = startSampler(pid)
		}
		samplers <- m
	}()
	for _, c := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.run(block, base, mStart, mEnd, stopAt)
		}()
	}
	wg.Wait()
	for name, s := range <-samplers {
		if res.Processes == nil {
			res.Processes = map[string]usage{}
		}
		res.Processes[name] = s.stop()
	}
	var rtts []int64
	var bytesIn int64
	for _, c := range cs {
		rtts = append(rtts, c.rtts...)
		res.Messages += c.recv
		res.Failures += c.failures + c.sent.Load() - c.got.Load()
		res.Corrupt += c.corrupt
		bytesIn += c.bytes
	}
	res.MsgPerSec = round(float64(res.Messages) / duration.Seconds())
	res.MiBPerSec = round(float64(bytesIn) / duration.Seconds() / (1 << 20))
	res.RTT = summarize(rtts)
	return res, nil
}

func (c *client) run(block []byte, base time.Time, mStart, mEnd int64, stopAt time.Time) {
	sem := make(chan struct{}, *inflight)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var expect uint64
		for {
			_, data, err := c.ws.ReadMessage()
			if err != nil {
				return
			}
			now := time.Since(base).Nanoseconds()
			if len(data) != len(block) || binary.BigEndian.Uint64(data) != expect || !bytes.Equal(data[16:], block[16:]) {
				c.corrupt++
			}
			expect++
			if sentAt := int64(binary.BigEndian.Uint64(data[8:])); sentAt >= mStart && sentAt < mEnd {
				c.rtts = append(c.rtts, now-sentAt)
			}
			if now >= mStart && now < mEnd {
				c.recv++
				c.bytes += int64(len(data))
			}
			c.got.Add(1)
			<-sem
		}
	}()
	for seq := uint64(0); time.Now().Before(stopAt); seq++ {
		select {
		case sem <- struct{}{}:
		case <-done:
			c.failures++
			return
		case <-time.After(5 * time.Second):
			c.failures++
			c.ws.Close()
			<-done
			return
		}
		msg := slices.Clone(block)
		binary.BigEndian.PutUint64(msg, seq)
		binary.BigEndian.PutUint64(msg[8:], uint64(time.Since(base).Nanoseconds()))
		_ = c.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if c.ws.WriteMessage(websocket.BinaryMessage, msg) != nil {
			c.failures++
			break
		}
		c.sent.Add(1)
	}
	end := time.Now().Add(5 * time.Second)
	for c.got.Load() < c.sent.Load() && time.Now().Before(end) {
		time.Sleep(time.Millisecond)
	}
	c.ws.Close()
	<-done
}

func summarize(ns []int64) dist {
	if len(ns) == 0 {
		return dist{}
	}
	slices.Sort(ns)
	q := func(p float64) float64 {
		return round(float64(ns[max(0, int(math.Ceil(p*float64(len(ns))))-1)]) / 1e3)
	}
	return dist{P50: q(0.5), P99: q(0.99), Max: q(1)}
}

func round(v float64) float64 {
	return math.Round(v*10) / 10
}

type sampler struct {
	pid   int
	start time.Time
	cpu0  float64
	peak  int64
	stopc chan struct{}
	done  chan struct{}
}

func psStats(pid int) (int64, float64, error) {
	out, err := exec.Command("ps", "-o", "rss=,time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, 0, err
	}
	f := strings.Fields(string(out))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("unexpected ps output %q", out)
	}
	rss, err := strconv.ParseInt(f[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	var cpu float64
	for _, part := range strings.Split(strings.ReplaceAll(f[1], "-", ":"), ":") {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, 0, err
		}
		cpu = cpu*60 + v
	}
	return rss, cpu, nil
}

func startSampler(pid int) *sampler {
	s := &sampler{pid: pid, start: time.Now(), stopc: make(chan struct{}), done: make(chan struct{})}
	_, s.cpu0, _ = psStats(pid)
	go func() {
		defer close(s.done)
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			if rss, _, err := psStats(pid); err == nil {
				s.peak = max(s.peak, rss)
			}
			select {
			case <-t.C:
			case <-s.stopc:
				return
			}
		}
	}()
	return s
}

func (s *sampler) stop() usage {
	close(s.stopc)
	<-s.done
	rss, cpu, _ := psStats(s.pid)
	return usage{CPUPercent: round(100 * (cpu - s.cpu0) / time.Since(s.start).Seconds()), RSSPeakMiB: round(float64(max(s.peak, rss)) / 1024)}
}
