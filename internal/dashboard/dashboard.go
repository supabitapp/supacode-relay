package dashboard

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"html/template"
	"io/fs"
	"log"
	"math"
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const (
	pollInterval          = 2 * time.Second
	refreshFloor          = pollInterval / 2
	maxRateWindow         = 5 * pollInterval
	unknown               = "—"
	contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

var (
	//go:embed templates
	templateFS embed.FS
	//go:embed static
	staticFS   embed.FS
	templates  = template.Must(template.New("").Funcs(template.FuncMap{"pollInterval": pollInterval.String}).ParseFS(templateFS, "templates/*.html"))
	staticTags = hashStatic()
)

type Stats struct {
	Draining            bool
	ActiveHosts         int
	ActivePairs         int
	PendingPairs        int
	ControlConnections  int
	ClientSlots         int
	OpenSockets         int
	OpenConnections     int
	MaxHosts            int
	MaxClients          int
	MaxConnections      int
	RejectedConnections int64
	ForwardedMessages   int64
	ForwardedBytes      int64
}

type Dashboard struct {
	stats   func() Stats
	started time.Time
	mux     *http.ServeMux

	mu   sync.Mutex
	last sample
	view view
}

type sample struct {
	at          time.Time
	relay       Stats
	cpu         time.Duration
	cpuOK       bool
	cpus        int
	goroutines  int
	retained    uint64
	heapInuse   uint64
	gcCycles    uint32
	memoryLimit int64
	load        [3]float64
	loadOK      bool
	systemUsed  uint64
	systemTotal uint64
	systemOK    bool
}

type view struct {
	at       time.Time
	Draining bool
	Time     string
	Uptime   string
	Started  string
	Sections []section
}

type section struct {
	Title string
	Rows  []row
}

type row struct {
	Label  string
	Value  string
	Detail string
	Level  float64
	Meter  bool
}

func New(stats func() Stats, limit func(http.Handler) http.Handler) *Dashboard {
	d := &Dashboard{stats: stats, started: time.Now(), mux: http.NewServeMux()}
	d.last = takeSample(d.started, Stats{})
	d.mux.Handle("GET /{$}", limit(d.render("page")))
	d.mux.Handle("GET /stats", limit(d.render("stats")))
	d.mux.HandleFunc("GET /static/{file}", serveStatic)
	return d
}

func (d *Dashboard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	d.mux.ServeHTTP(w, r)
}

func (d *Dashboard) render(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		var body bytes.Buffer
		if err := templates.ExecuteTemplate(&body, name, d.snapshot(time.Now())); err != nil {
			log.Println("dashboard: render:", err)
			http.Error(w, "dashboard unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = body.WriteTo(w)
	})
}

func serveStatic(w http.ResponseWriter, r *http.Request) {
	file := r.PathValue("file")
	tag, ok := staticTags[file]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("ETag", tag)
	http.ServeFileFS(w, r, staticFS, "static/"+file)
}

func hashStatic() map[string]string {
	entries, err := fs.ReadDir(staticFS, "static")
	if err != nil {
		panic(err)
	}
	tags := make(map[string]string, len(entries))
	for _, entry := range entries {
		data, err := staticFS.ReadFile("static/" + entry.Name())
		if err != nil {
			panic(err)
		}
		sum := sha256.Sum256(data)
		tags[entry.Name()] = `"` + hex.EncodeToString(sum[:12]) + `"`
	}
	return tags
}

func (d *Dashboard) snapshot(now time.Time) view {
	d.mu.Lock()
	defer d.mu.Unlock()
	if now.Sub(d.view.at) < refreshFloor {
		return d.view
	}
	current := takeSample(now, d.stats())
	d.view = d.build(d.last, current)
	d.last = current
	return d.view
}

func takeSample(at time.Time, relay Stats) sample {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	s := sample{
		at:          at,
		relay:       relay,
		cpus:        runtime.NumCPU(),
		goroutines:  runtime.NumGoroutine(),
		retained:    memory.Sys - memory.HeapReleased,
		heapInuse:   memory.HeapInuse,
		gcCycles:    memory.NumGC,
		memoryLimit: debug.SetMemoryLimit(-1),
	}
	s.cpu, s.cpuOK = processCPU()
	s.load, s.loadOK = loadAverage()
	s.systemUsed, s.systemTotal, s.systemOK = systemMemory()
	return s
}

func (d *Dashboard) build(previous, current sample) view {
	relay := current.relay
	return view{
		at:       current.at,
		Draining: relay.Draining,
		Time:     current.at.UTC().Format("2006-01-02 15:04:05 UTC"),
		Uptime:   formatUptime(current.at.Sub(d.started)),
		Started:  d.started.UTC().Format("2006-01-02 15:04 UTC"),
		Sections: []section{
			{"Relay", []row{
				{Label: "Connected hosts", Value: formatCount(int64(relay.ActiveHosts))},
				{Label: "Active pairs", Value: formatCount(int64(relay.ActivePairs))},
				{Label: "Pending pairs", Value: formatCount(int64(relay.PendingPairs))},
				{Label: "Open sockets", Value: formatCount(int64(relay.OpenSockets))},
				{Label: "Rejected connections", Value: formatCount(relay.RejectedConnections), Detail: "since start"},
			}},
			{"Capacity", []row{
				capacity("Hosts", relay.ControlConnections, relay.MaxHosts),
				capacity("Pairs", relay.ClientSlots, relay.MaxClients),
				capacity("Connections", relay.OpenConnections, relay.MaxConnections),
			}},
			{"Traffic", trafficRows(previous, current)},
			{"CPU", cpuRows(previous, current)},
			{"Memory", memoryRows(current)},
		},
	}
}

func rateWindow(previous, current sample) (seconds float64, ok bool) {
	window := current.at.Sub(previous.at)
	return window.Seconds(), window >= refreshFloor && window <= maxRateWindow
}

func trafficRows(previous, current sample) []row {
	messages := row{Label: "Messages", Value: unknown, Detail: formatCount(current.relay.ForwardedMessages) + " total"}
	throughput := row{Label: "Throughput", Value: unknown, Detail: formatBytes(float64(current.relay.ForwardedBytes)) + " total"}
	if seconds, ok := rateWindow(previous, current); ok {
		messages.Value = formatQuantity(float64(current.relay.ForwardedMessages-previous.relay.ForwardedMessages)/seconds) + "/s"
		throughput.Value = formatBytes(float64(current.relay.ForwardedBytes-previous.relay.ForwardedBytes)/seconds) + "/s"
	}
	return []row{messages, throughput}
}

func cpuRows(previous, current sample) []row {
	cores := formatCount(int64(current.cpus)) + " cores"
	if current.cpus == 1 {
		cores = "1 core"
	}
	usage := row{Label: "Relay process", Value: unknown, Detail: "of " + cores}
	if seconds, ok := rateWindow(previous, current); ok && previous.cpuOK && current.cpuOK {
		fraction := (current.cpu - previous.cpu).Seconds() / seconds / float64(current.cpus)
		usage = gauge(usage.Label, formatPercent(fraction), usage.Detail, fraction)
	}
	rows := []row{usage}
	if current.loadOK {
		load := current.load
		rows = append(rows, row{Label: "Load average", Value: formatLoad(load[0]) + "  " + formatLoad(load[1]) + "  " + formatLoad(load[2]), Detail: "1, 5, 15 min"})
	}
	return append(rows, row{Label: "Goroutines", Value: formatCount(int64(current.goroutines))})
}

func memoryRows(current sample) []row {
	retained := float64(current.retained)
	goRuntime := row{Label: "Go runtime", Value: formatBytes(retained)}
	if current.memoryLimit != math.MaxInt64 {
		limit := float64(current.memoryLimit)
		goRuntime = gauge(goRuntime.Label, goRuntime.Value, "of "+formatBytes(limit)+" limit", retained/limit)
	}
	rows := []row{goRuntime, {Label: "Heap in use", Value: formatBytes(float64(current.heapInuse))}}
	if current.systemOK {
		used, total := float64(current.systemUsed), float64(current.systemTotal)
		rows = append(rows, gauge("System", formatBytes(used), "of "+formatBytes(total), used/total))
	}
	return append(rows, row{Label: "GC cycles", Value: formatCount(int64(current.gcCycles))})
}

func capacity(label string, n, limit int) row {
	return gauge(label, formatCount(int64(n)), "of "+formatCount(int64(limit)), float64(n)/float64(limit))
}

func gauge(label, value, detail string, level float64) row {
	return row{Label: label, Value: value, Detail: detail, Level: min(max(level, 0), 1), Meter: true}
}
