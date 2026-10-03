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
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
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

var (
	relayBin    = flag.String("relay-bin", "bin/relay", "path to the built relay binary")
	outDir      = flag.String("out", "", "directory for raw JSON artifacts (outside the worktree)")
	mode        = flag.String("mode", "suite", "suite, merge, or echo")
	addr        = flag.String("addr", "127.0.0.1:0", "echo server listen address")
	warmup      = flag.Duration("warmup", 2*time.Second, "warmup per case")
	duration    = flag.Duration("duration", 5*time.Second, "measurement window per case")
	inflight    = flag.Int("inflight", 4, "messages in flight per client")
	reps        = flag.Int("reps", 3, "repetitions of the 1 KiB / 32 client case")
	quick       = flag.Bool("quick", false, "small matrix for smoke runs")
	section     = flag.String("section", "", "benchmark section to run")
	maxDials    = flag.Int("max-dials", 2000, "connection budget per section")
	churnCycles = flag.Int("churn-cycles", 600, "connect/echo/close cycles in the churn section")
	maxStalled  = flag.Int("max-stalled", 150, "maximum stalled pairs opened in the slow reader section")
	limit       = flag.Duration("max-runtime", 20*time.Minute, "abort the whole suite with a goroutine dump after this long")
)

var (
	procsMu sync.Mutex
	procs   []*proc
)

func stopAll() {
	procsMu.Lock()
	ps := append([]*proc(nil), procs...)
	procsMu.Unlock()
	for _, p := range ps {
		p.stop()
	}
}

func phase(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s bench: %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func main() {
	flag.Parse()
	if *mode == "echo" {
		runEcho()
		return
	}
	time.AfterFunc(*limit, func() {
		phase("max runtime %s exceeded; goroutine dump follows", *limit)
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		stopAll()
		os.Exit(3)
	})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		phase("received %s; stopping spawned processes", sig)
		stopAll()
		os.Exit(130)
	}()
	err := runSuite()
	stopAll()
	if err != nil {
		phase("failed: %v", err)
		os.Exit(1)
	}
}

func echoLoop(ws *websocket.Conn) {
	defer ws.Close()
	for {
		typ, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		_ = ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if ws.WriteMessage(typ, data) != nil {
			return
		}
	}
}

func runEcho() {
	up := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096, WriteBufferPool: &sync.Pool{}, CheckOrigin: func(*http.Request) bool { return true }}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	line, _ := json.Marshal(map[string]string{"event": "listening", "address": ln.Addr().String()})
	fmt.Println(string(line))
	_ = http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ws.SetReadLimit(1 << 20)
		echoLoop(ws)
	}))
}

type proc struct {
	cmd    *exec.Cmd
	addr   string
	exited chan struct{}
	err    error
}

func spawn(name string, args []string, env []string, logPath string) (*proc, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = append(os.Environ(), env...)
	logf, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd.Stderr = logf
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &proc{cmd: cmd, exited: make(chan struct{})}
	procsMu.Lock()
	procs = append(procs, p)
	procsMu.Unlock()
	phase("spawned %s pid=%d", filepath.Base(name), cmd.Process.Pid)
	go func() {
		p.err = cmd.Wait()
		logf.Close()
		close(p.exited)
	}()
	lines := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			default:
			}
		}
	}()
	select {
	case l := <-lines:
		var ev struct{ Event, Address string }
		if json.Unmarshal([]byte(l), &ev) != nil || ev.Event != "listening" {
			p.stop()
			return nil, fmt.Errorf("unexpected startup line %q", l)
		}
		p.addr = ev.Address
		return p, nil
	case <-p.exited:
		return nil, fmt.Errorf("%s exited: %v", name, p.err)
	case <-time.After(10 * time.Second):
		p.stop()
		return nil, errors.New("startup timeout")
	}
}

func (p *proc) stop() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.exited
	}
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func psStats(pid int) (rssKiB int64, cpu float64, err error) {
	out, err := exec.Command("ps", "-o", "rss=,time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, 0, err
	}
	f := strings.Fields(string(out))
	if len(f) != 2 {
		return 0, 0, fmt.Errorf("unexpected ps output %q", out)
	}
	rssKiB, err = strconv.ParseInt(f[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	for _, part := range strings.Split(strings.ReplaceAll(f[1], "-", ":"), ":") {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, 0, err
		}
		cpu = cpu*60 + v
	}
	return rssKiB, cpu, nil
}

type resources struct {
	CPUPercent float64 `json:"cpuPercentOfOneCore"`
	RSSPeakMiB float64 `json:"rssPeakMiB"`
	RSSEndMiB  float64 `json:"rssEndMiB"`
	Samples    int     `json:"rssSamples"`
}

type sampler struct {
	pid    int
	start  time.Time
	cpu0   float64
	peak   int64
	last   int64
	n      int
	stopCh chan struct{}
	done   chan struct{}
}

func startSampler(pid int) *sampler {
	s := &sampler{pid: pid, stopCh: make(chan struct{}), done: make(chan struct{}), start: time.Now()}
	_, s.cpu0, _ = psStats(pid)
	go func() {
		defer close(s.done)
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			if rss, _, err := psStats(pid); err == nil {
				s.peak = max(s.peak, rss)
				s.last = rss
				s.n++
			}
			select {
			case <-t.C:
			case <-s.stopCh:
				return
			}
		}
	}()
	return s
}

func (s *sampler) stop() resources {
	close(s.stopCh)
	<-s.done
	rss, cpu, _ := psStats(s.pid)
	s.peak = max(s.peak, rss)
	wall := time.Since(s.start).Seconds()
	return resources{
		CPUPercent: round(100*(cpu-s.cpu0)/wall, 1),
		RSSPeakMiB: round(float64(s.peak)/1024, 2),
		RSSEndMiB:  round(float64(rss)/1024, 2),
		Samples:    s.n + 1,
	}
}

func round(v float64, d int) float64 {
	p := math.Pow10(d)
	return math.Round(v*p) / p
}

type dist struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

func summarize(ns []int64, unit time.Duration) dist {
	if len(ns) == 0 {
		return dist{}
	}
	slices.Sort(ns)
	q := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(ns)))) - 1
		return round(float64(ns[max(0, i)])/float64(unit), 3)
	}
	var sum float64
	for _, v := range ns {
		sum += float64(v)
	}
	return dist{Count: len(ns), P50: q(0.50), P95: q(0.95), P99: q(0.99), Max: q(1), Mean: round(sum/float64(len(ns))/float64(unit), 3)}
}

type target struct {
	name  string
	proc  *proc
	hosts []*endpoint.Host
	relay bool
}

var (
	dialBudget    atomic.Int64
	dialsUsed     atomic.Int64
	portExhausted atomic.Bool
	errExhausted  = errors.New("local ephemeral ports exhausted (EADDRNOTAVAIL); aborting section without retry")
)

func takeDial() error {
	if portExhausted.Load() {
		return errExhausted
	}
	if dialsUsed.Add(1) > dialBudget.Load() {
		return fmt.Errorf("dial budget of %d connections exceeded", dialBudget.Load())
	}
	return nil
}

func checkDial(err error) error {
	if err != nil && errors.Is(err, syscall.EADDRNOTAVAIL) {
		if !portExhausted.Swap(true) {
			phase("local port exhaustion detected after %d dials; stopping new connections", dialsUsed.Load())
		}
		return errExhausted
	}
	return err
}

func (tg *target) dial(i int) (*websocket.Conn, error) {
	if err := takeDial(); err != nil {
		return nil, err
	}
	ws, err := tg.dialOnce(i)
	return ws, checkDial(err)
}

func (tg *target) dialOnce(i int) (*websocket.Conn, error) {
	if tg.relay {
		ws, resp, err := endpoint.Connect("ws://"+tg.proc.addr, tg.hosts[i%len(tg.hosts)].ID)
		if err != nil && resp != nil {
			err = fmt.Errorf("%w (HTTP %d)", err, resp.StatusCode)
		}
		return ws, err
	}
	ws, _, err := endpoint.Dialer.Dial("ws://"+tg.proc.addr+"/", nil)
	return ws, err
}

func serveHosts(base string, n int, handle func(*websocket.Conn)) ([]*endpoint.Host, error) {
	var hosts []*endpoint.Host
	for range n {
		_, priv, _ := ed25519.GenerateKey(rand.Reader)
		if err := takeDial(); err != nil {
			return nil, err
		}
		h, err := endpoint.Register(base, priv)
		if err = checkDial(err); err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
		go func() {
			for ev := range h.Events {
				if ev.Type != "incoming" {
					continue
				}
				go func() {
					if takeDial() != nil {
						return
					}
					ws, _, err := h.Accept(ev)
					if checkDial(err) == nil {
						handle(ws)
					}
				}()
			}
		}()
	}
	return hosts, nil
}

func metrics(p *proc) map[string]float64 {
	resp, err := http.Get("http://" + p.addr + "/metrics")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&raw)
	m := map[string]float64{}
	for k, v := range raw {
		if f, ok := v.(float64); ok {
			m[k] = f
		}
	}
	return m
}

func waitFor(p *proc, timeout time.Duration, pred func(map[string]float64) bool) (map[string]float64, time.Duration, error) {
	start := time.Now()
	for {
		m := metrics(p)
		if m != nil && pred(m) {
			return m, time.Since(start), nil
		}
		if time.Since(start) > timeout {
			return m, time.Since(start), errors.New("timeout waiting for relay metrics")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type caseResult struct {
	Target          string    `json:"target"`
	PayloadBytes    int       `json:"payloadBytes"`
	Clients         int       `json:"clients"`
	InflightPerConn int       `json:"inflightPerClient"`
	Rep             int       `json:"rep"`
	WarmupSec       float64   `json:"warmupSec"`
	DurationSec     float64   `json:"durationSec"`
	Messages        int64     `json:"messagesInWindow"`
	MsgPerSec       float64   `json:"messagesPerSec"`
	PayloadMiBps    float64   `json:"payloadMiBPerSec"`
	RTTMicros       dist      `json:"rttMicros"`
	ConnectMillis   dist      `json:"pairReadyMillis"`
	Failures        int64     `json:"failures"`
	Timeouts        int64     `json:"timeouts"`
	Corrupt         int64     `json:"corrupt"`
	Server          resources `json:"serverProcess"`
}

type client struct {
	ws      *websocket.Conn
	connect time.Duration
	rtts    []int64
	recv    int64
	bytes   int64
	sent    int64
	got     int64
	fail    int64
	corrupt int64
}

func runCase(tg *target, payload, clients, k, rep int, warm, dur time.Duration) (caseResult, error) {
	res := caseResult{Target: tg.name, PayloadBytes: payload, Clients: clients, InflightPerConn: k, Rep: rep, WarmupSec: warm.Seconds(), DurationSec: dur.Seconds()}
	base := time.Now()
	block := make([]byte, payload)
	_, _ = rand.Read(block)
	cs := make([]*client, clients)
	var wg sync.WaitGroup
	var dialErr atomic.Value
	for i := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := &client{}
			cs[i] = c
			start := time.Now()
			ws, err := tg.dial(i)
			if err != nil {
				dialErr.Store(err)
				return
			}
			ws.SetReadLimit(1 << 20)
			c.ws = ws
			_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
			if ws.WriteMessage(websocket.BinaryMessage, []byte("probe")) != nil {
				dialErr.Store(errors.New("probe write failed"))
				return
			}
			if _, data, err := ws.ReadMessage(); err != nil || string(data) != "probe" {
				dialErr.Store(fmt.Errorf("probe failed: %v", err))
				return
			}
			_ = ws.SetReadDeadline(time.Time{})
			c.connect = time.Since(start)
		}()
	}
	wg.Wait()
	defer func() {
		for _, c := range cs {
			if c != nil && c.ws != nil {
				c.ws.Close()
			}
		}
	}()
	if err, _ := dialErr.Load().(error); err != nil {
		return res, err
	}

	smCh := make(chan *sampler, 1)
	t0 := time.Now()
	mStart := t0.Add(warm).Sub(base).Nanoseconds()
	mEnd := t0.Add(warm + dur).Sub(base).Nanoseconds()
	stopAt := t0.Add(warm + dur)
	go func() {
		time.Sleep(warm)
		smCh <- startSampler(tg.proc.pid())
	}()
	for _, c := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClient(c, block, k, base, mStart, mEnd, stopAt)
		}()
	}
	wg.Wait()
	res.Server = (<-smCh).stop()

	var rtts, conns []int64
	for _, c := range cs {
		rtts = append(rtts, c.rtts...)
		conns = append(conns, c.connect.Nanoseconds())
		res.Messages += c.recv
		res.Failures += c.fail
		res.Corrupt += c.corrupt
		res.Timeouts += c.sent - c.got
	}
	var bytesIn int64
	for _, c := range cs {
		bytesIn += c.bytes
	}
	res.MsgPerSec = round(float64(res.Messages)/dur.Seconds(), 1)
	res.PayloadMiBps = round(float64(bytesIn)/dur.Seconds()/(1<<20), 2)
	res.RTTMicros = summarize(rtts, time.Microsecond)
	res.ConnectMillis = summarize(conns, time.Millisecond)
	return res, nil
}

func runClient(c *client, block []byte, k int, base time.Time, mStart, mEnd int64, stopAt time.Time) {
	sem := make(chan struct{}, k)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		var expect uint64
		for {
			_, data, err := c.ws.ReadMessage()
			if err != nil {
				return
			}
			now := time.Since(base).Nanoseconds()
			if len(data) < 16 {
				c.corrupt++
				atomic.AddInt64(&c.got, 1)
				<-sem
				continue
			}
			if len(data) != len(block) || binary.BigEndian.Uint64(data) != expect || !bytes.Equal(data[16:], block[16:]) {
				c.corrupt++
			}
			expect++
			sentAt := int64(binary.BigEndian.Uint64(data[8:]))
			if sentAt >= mStart && sentAt < mEnd {
				c.rtts = append(c.rtts, now-sentAt)
			}
			if now >= mStart && now < mEnd {
				c.recv++
				c.bytes += int64(len(data))
			}
			atomic.AddInt64(&c.got, 1)
			<-sem
		}
	}()
	var seq uint64
	for time.Now().Before(stopAt) {
		select {
		case sem <- struct{}{}:
		case <-readerDone:
			c.fail++
			c.ws.Close()
			return
		case <-time.After(5 * time.Second):
			c.fail++
			c.ws.Close()
			<-readerDone
			return
		}
		msg := make([]byte, len(block))
		copy(msg, block)
		binary.BigEndian.PutUint64(msg, seq)
		binary.BigEndian.PutUint64(msg[8:], uint64(time.Since(base).Nanoseconds()))
		_ = c.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.ws.WriteMessage(websocket.BinaryMessage, msg); err != nil {
			c.fail++
			<-sem
			break
		}
		seq++
		atomic.AddInt64(&c.sent, 1)
	}
	drain := time.After(5 * time.Second)
	for atomic.LoadInt64(&c.got) < atomic.LoadInt64(&c.sent) {
		select {
		case <-readerDone:
			c.fail++
			return
		case <-drain:
			c.ws.Close()
			<-readerDone
			return
		case <-time.After(time.Millisecond):
		}
	}
	c.ws.Close()
	<-readerDone
}

type env struct {
	OS, Arch, CPU, GoVersion, BuildFlags string
	CPUs                                 int
	MemGiB                               float64
	Libraries                            map[string]string
	Note                                 string
}

func environment() env {
	e := env{OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), GoVersion: runtime.Version(), Libraries: map[string]string{}}
	if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		e.CPU = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
		v, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		e.MemGiB = round(v/(1<<30), 1)
	}
	if out, err := exec.Command("sw_vers", "-productVersion").Output(); err == nil {
		e.OS += " " + strings.TrimSpace(string(out))
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			e.Libraries[d.Path] = d.Version
		}
		for _, s := range bi.Settings {
			if s.Key == "-trimpath" || s.Key == "-ldflags" || s.Key == "CGO_ENABLED" {
				e.BuildFlags += s.Key + "=" + s.Value + " "
			}
		}
	}
	e.BuildFlags = strings.TrimSpace(e.BuildFlags)
	e.Note = "shared machine; results are provisional; driver, relay, and echo server share one host over loopback"
	return e
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

var matrixEnv = []string{
	"RELAY_ADDR=127.0.0.1:0",
	"RELAY_ADMISSION_RATE=1000000",
	"RELAY_MAX_CLIENTS=4096",
	"RELAY_MAX_CLIENTS_PER_HOST=1024",
	"RELAY_MAX_PENDING_PER_HOST=1024",
}

var sections = []string{"matrix-64", "matrix-1024", "matrix-65536", "idle", "churn", "slow"}

func workload() map[string]any {
	return map[string]any{
		"warmupSec": warmup.Seconds(), "durationSec": duration.Seconds(), "inflightPerClient": *inflight,
		"relayEnv": matrixEnv, "relayHosts": 4, "maxDialsPerSection": *maxDials,
		"rtt":        "client sends binary payload with seq and send timestamp; host endpoint or direct server echoes; RTT measured at client for messages sent inside the window",
		"throughput": "echoed messages and payload bytes received by clients inside the measurement window (one direction)",
		"pairReady":  "dial start until first probe echo, includes relay pairing and host accept",
	}
}

func runSuite() error {
	if *outDir == "" {
		return errors.New("-out is required")
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		return err
	}
	if *mode == "merge" {
		return merge()
	}
	dialBudget.Store(int64(*maxDials))
	started := time.Now()
	phase("section %s (dial budget %d)", *section, *maxDials)
	var data any
	var err error
	switch {
	case strings.HasPrefix(*section, "matrix-"):
		p, perr := strconv.Atoi(strings.TrimPrefix(*section, "matrix-"))
		if perr != nil {
			return perr
		}
		data, err = runMatrix(p)
	case *section == "idle":
		data, err = runIdle()
	case *section == "churn":
		data, err = runChurnSection()
	case *section == "slow":
		data, err = runSlowReader()
	default:
		return fmt.Errorf("unknown section %q (want one of %v)", *section, sections)
	}
	out := map[string]any{"section": *section, "startedAt": started.UTC().Format(time.RFC3339), "elapsedSec": round(time.Since(started).Seconds(), 1), "dialsUsed": dialsUsed.Load(), "localPortExhausted": portExhausted.Load()}
	if err != nil {
		out["error"] = err.Error()
	} else {
		out["data"] = data
	}
	if werr := writeJSON(filepath.Join(*outDir, "section-"+*section+".json"), out); werr != nil {
		return werr
	}
	phase("section %s done in %.1fs using %d dials", *section, time.Since(started).Seconds(), dialsUsed.Load())
	return err
}

func merge() error {
	report := map[string]any{"environment": environment(), "workload": workload(), "rawDir": *outDir}
	var missing []string
	for _, name := range sections {
		raw, err := os.ReadFile(filepath.Join(*outDir, "section-"+name+".json"))
		var sec map[string]any
		if err != nil || json.Unmarshal(raw, &sec) != nil || sec["error"] != nil || sec["data"] == nil {
			missing = append(missing, name)
			if sec != nil {
				report[name] = sec
			}
			continue
		}
		report[name] = sec
	}
	report["failedOrMissingSections"] = missing
	if err := writeJSON(filepath.Join(*outDir, "results.json"), report); err != nil {
		return err
	}
	b, _ := json.Marshal(report)
	fmt.Println(string(b))
	if len(missing) > 0 {
		return fmt.Errorf("sections failed or missing: %v; rerun them with RELAY_BENCH_SECTIONS", missing)
	}
	return nil
}

func runMatrix(p int) (any, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	relay, err := spawn(*relayBin, nil, matrixEnv, filepath.Join(*outDir, fmt.Sprintf("relay-matrix-%d.log", p)))
	if err != nil {
		return nil, err
	}
	defer relay.stop()
	echo, err := spawn(self, []string{"-mode", "echo"}, nil, filepath.Join(*outDir, fmt.Sprintf("echo-%d.log", p)))
	if err != nil {
		return nil, err
	}
	defer echo.stop()
	hosts, err := serveHosts("ws://"+relay.addr, 4, echoLoop)
	if err != nil {
		return nil, err
	}
	targets := []*target{{name: "relay", proc: relay, hosts: hosts, relay: true}, {name: "direct", proc: echo}}
	counts := []int{1, 32, 128}
	if *quick {
		counts = []int{1, 32}
	}
	var cases []caseResult
	for _, n := range counts {
		r := 1
		if p == 1024 && n == 32 {
			r = *reps
		}
		for rep := 1; rep <= r; rep++ {
			for _, tg := range targets {
				res, err := runCase(tg, p, n, *inflight, rep, *warmup, *duration)
				if err != nil {
					return cases, fmt.Errorf("%s %dB x%d rep%d: %w", tg.name, p, n, rep, err)
				}
				fmt.Fprintf(os.Stderr, "%-6s %6dB x%-3d rep%d  p50=%.0fus p99=%.0fus  %.0f msg/s  %.1f MiB/s  cpu=%.0f%% rss=%.1fMiB fail=%d timeout=%d corrupt=%d\n",
					tg.name, p, n, rep, res.RTTMicros.P50, res.RTTMicros.P99, res.MsgPerSec, res.PayloadMiBps, res.Server.CPUPercent, res.Server.RSSPeakMiB, res.Failures, res.Timeouts, res.Corrupt)
				cases = append(cases, res)
				_ = writeJSON(filepath.Join(*outDir, fmt.Sprintf("case-%s-%d-%d-rep%d.json", tg.name, p, n, rep)), res)
				if tg.relay {
					if _, _, err := waitFor(relay, 10*time.Second, func(m map[string]float64) bool { return m["activePairs"] == 0 && m["pendingPairs"] == 0 }); err != nil {
						return cases, err
					}
				}
			}
		}
	}
	return map[string]any{"cases": cases, "relayMetricsAfter": metrics(relay)}, nil
}

func runChurnSection() (any, error) {
	relay, err := spawn(*relayBin, nil, matrixEnv, filepath.Join(*outDir, "relay-churn.log"))
	if err != nil {
		return nil, err
	}
	defer relay.stop()
	hosts, err := serveHosts("ws://"+relay.addr, 2, echoLoop)
	if err != nil {
		return nil, err
	}
	return runChurn(&target{name: "relay", proc: relay, hosts: hosts, relay: true}, 16, *churnCycles)
}

func rssMiB(p *proc) float64 {
	rss, _, _ := psStats(p.pid())
	return round(float64(rss)/1024, 2)
}

func holdIdle(ws *websocket.Conn) {
	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			ws.Close()
			return
		}
	}
}

func runIdle() (map[string]any, error) {
	env := append(append([]string{}, matrixEnv...), "RELAY_MAX_CLIENTS_PER_HOST=128", "RELAY_MAX_PENDING_PER_HOST=64")
	relay, err := spawn(*relayBin, nil, env, filepath.Join(*outDir, "relay-idle.log"))
	if err != nil {
		return nil, err
	}
	defer relay.stop()
	out := map[string]any{"relayEnv": env, "hosts": 10, "settleSec": 2}
	time.Sleep(time.Second)
	out["rssMiBNoHosts"] = rssMiB(relay)
	tg := &target{name: "relay", proc: relay, relay: true}
	tg.hosts, err = serveHosts("ws://"+relay.addr, 10, holdIdle)
	if err != nil {
		return nil, err
	}
	var levels []map[string]any
	var conns []*websocket.Conn
	for _, n := range []int{0, 100, 500} {
		for i := len(conns); i < n; i++ {
			ws, err := tg.dial(i)
			if err != nil {
				return nil, err
			}
			conns = append(conns, ws)
			go holdIdle(ws)
		}
		if _, _, err := waitFor(relay, 10*time.Second, func(m map[string]float64) bool { return int(m["activePairs"]) == n && m["pendingPairs"] == 0 }); err != nil {
			return nil, err
		}
		time.Sleep(2 * time.Second)
		m := metrics(relay)
		levels = append(levels, map[string]any{"pairedClients": n, "rssMiB": rssMiB(relay), "goroutines": m["goroutines"], "openSockets": m["openSockets"]})
		fmt.Fprintf(os.Stderr, "idle %3d pairs rss=%.1fMiB goroutines=%v\n", n, rssMiB(relay), m["goroutines"])
	}
	out["levels"] = levels

	for _, ws := range conns {
		ws.Close()
	}
	m, took, err := waitFor(relay, 15*time.Second, func(m map[string]float64) bool {
		return m["activePairs"] == 0 && m["pendingPairs"] == 0 && m["openSockets"] == 10
	})
	if err != nil {
		return nil, err
	}
	time.Sleep(2 * time.Second)
	out["cleanup"] = map[string]any{"secondsToRelease500Pairs": round(took.Seconds(), 3), "rssMiBAfter": rssMiB(relay), "goroutinesAfter": metrics(relay)["goroutines"], "metrics": m}

	return out, nil
}

func runChurn(tg *target, workers, cycles int) (map[string]any, error) {
	before := metrics(tg.proc)
	sm := startSampler(tg.proc.pid())
	var next, fails atomic.Int64
	lat := make([][]int64, workers)
	start := time.Now()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > int64(cycles) || fails.Load() > int64(cycles/100) {
					return
				}
				t := time.Now()
				ws, err := tg.dial(int(i))
				if err != nil {
					fails.Add(1)
					if errors.Is(err, errExhausted) {
						return
					}
					continue
				}
				_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
				ok := ws.WriteMessage(websocket.TextMessage, []byte("churn")) == nil
				if ok {
					_, _, err = ws.ReadMessage()
					ok = err == nil
				}
				_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1000, ""), time.Now().Add(time.Second))
				ws.Close()
				if !ok {
					fails.Add(1)
					continue
				}
				lat[w] = append(lat[w], time.Since(t).Nanoseconds())
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if fails.Load() > int64(cycles/100) {
		sm.stop()
		return nil, fmt.Errorf("churn aborted after %d failures", fails.Load())
	}
	m, took, err := waitFor(tg.proc, 15*time.Second, func(m map[string]float64) bool {
		return m["activePairs"] == 0 && m["pendingPairs"] == 0 && m["openSockets"] == before["openSockets"]
	})
	res := sm.stop()
	if err != nil {
		return nil, err
	}
	var all []int64
	for _, l := range lat {
		all = append(all, l...)
	}
	fmt.Fprintf(os.Stderr, "churn %d cycles in %.2fs fails=%d\n", cycles, elapsed.Seconds(), fails.Load())
	return map[string]any{
		"workers": workers, "cycles": cycles, "failures": fails.Load(), "elapsedSec": round(elapsed.Seconds(), 3),
		"cyclesPerSec":           round(float64(cycles)/elapsed.Seconds(), 1),
		"cycleMillis":            summarize(all, time.Millisecond),
		"cycle":                  "connect, host accept, one echo, client close",
		"secondsToCleanAfterEnd": round(took.Seconds(), 3),
		"goroutinesBefore":       before["goroutines"], "goroutinesAfter": m["goroutines"],
		"server": res,
	}, nil
}

func runSlowReader() (map[string]any, error) {
	env := append([]string{}, matrixEnv...)
	relay, err := spawn(*relayBin, nil, env, filepath.Join(*outDir, "relay-slow.log"))
	if err != nil {
		return nil, err
	}
	defer relay.stop()
	healthyHosts, err := serveHosts("ws://"+relay.addr, 1, echoLoop)
	if err != nil {
		return nil, err
	}
	var stalledMu sync.Mutex
	var stalledConns []*websocket.Conn
	stalledHosts, err := serveHosts("ws://"+relay.addr, 1, func(ws *websocket.Conn) {
		stalledMu.Lock()
		stalledConns = append(stalledConns, ws)
		stalledMu.Unlock()
	})
	if err != nil {
		return nil, err
	}
	healthy := &target{name: "relay", proc: relay, hosts: healthyHosts, relay: true}
	stalled := &target{name: "relay", proc: relay, hosts: stalledHosts, relay: true}

	baseline, err := runCase(healthy, 1024, 1, 1, 1, *warmup, *duration)
	if err != nil {
		return nil, err
	}

	stop := make(chan struct{})
	var floods, floodBytes atomic.Int64
	codes := map[string]int{}
	var codesMu sync.Mutex
	var closeLat []int64
	var lastStalled atomic.Int64
	floodStart := time.Now()
	var fwg sync.WaitGroup
	for w := range 4 {
		fwg.Add(1)
		go func() {
			defer fwg.Done()
			chunk := make([]byte, 65536)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if floods.Add(1) > int64(*maxStalled) {
					floods.Add(-1)
					return
				}
				ws, err := stalled.dial(w)
				if err != nil {
					codesMu.Lock()
					codes["dialError"]++
					codesMu.Unlock()
					return
				}
				start := time.Now()
				closed := make(chan string, 1)
				go func() {
					_, _, err := ws.ReadMessage()
					var ce *websocket.CloseError
					if errors.As(err, &ce) {
						closed <- strconv.Itoa(ce.Code)
					} else {
						closed <- "error"
					}
				}()
				var code string
			flood:
				for {
					select {
					case code = <-closed:
						break flood
					case <-stop:
						code = "stopped"
						break flood
					default:
					}
					_ = ws.SetWriteDeadline(time.Now().Add(time.Second))
					if ws.WriteMessage(websocket.BinaryMessage, chunk) != nil {
						select {
						case code = <-closed:
						case <-time.After(2 * time.Second):
							code = "writeError"
						}
						break
					}
					floodBytes.Add(int64(len(chunk)))
				}
				ws.Close()
				codesMu.Lock()
				codes[code]++
				if code != "stopped" {
					closeLat = append(closeLat, time.Since(start).Nanoseconds())
				}
				codesMu.Unlock()
				lastStalled.Store(time.Now().UnixNano())
				select {
				case <-stop:
					return
				case <-time.After(150 * time.Millisecond):
				}
			}
		}()
	}
	sm := startSampler(relay.pid())
	during, err := runCase(healthy, 1024, 1, 1, 1, *warmup, *duration)
	res := sm.stop()
	close(stop)
	fwg.Wait()
	stalledMu.Lock()
	for _, ws := range stalledConns {
		ws.Close()
	}
	stalledMu.Unlock()
	if err != nil {
		return nil, err
	}
	m, _, _ := waitFor(relay, 15*time.Second, func(m map[string]float64) bool { return m["activePairs"] == 0 && m["pendingPairs"] == 0 })
	fmt.Fprintf(os.Stderr, "slow reader: healthy p50 %.0fus -> %.0fus, p99 %.0fus -> %.0fus, stalled pairs %d codes %v rssPeak=%.1fMiB\n",
		baseline.RTTMicros.P50, during.RTTMicros.P50, baseline.RTTMicros.P99, during.RTTMicros.P99, floods.Load(), codes, res.RSSPeakMiB)
	return map[string]any{
		"workload":           "4 workers repeatedly open a pair whose host never reads and flood 64 KiB binary messages; one healthy 1 KiB pair (1 in flight) is measured with and without the flood",
		"relayEnv":           env,
		"healthyBaseline":    baseline,
		"healthyDuringFlood": during,
		"stalledPairs":       floods.Load(),
		"floodActiveSec":     round(float64(lastStalled.Load()-floodStart.UnixNano())/1e9, 2), "healthyWindowSec": round((*warmup + *duration).Seconds(), 2),
		"floodMiBSent":          round(float64(floodBytes.Load())/(1<<20), 1),
		"stalledCloseCodes":     codes,
		"stalledSecondsToClose": summarize(closeLat, time.Second),
		"relayDuringFlood":      res,
		"metricsAfter":          m,
	}, nil
}
