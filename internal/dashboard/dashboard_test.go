package dashboard

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var capacityLimits = Stats{MaxHosts: 10, MaxClients: 10, MaxConnections: 10}

func unlimited(next http.Handler) http.Handler {
	return next
}

func serve(t *testing.T, d *Dashboard, path string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	for name, values := range header {
		request.Header[name] = values
	}
	response := httptest.NewRecorder()
	d.ServeHTTP(response, request)
	return response
}

func findRow(t *testing.T, v view, label string) row {
	t.Helper()
	for _, s := range v.Sections {
		for _, r := range s.Rows {
			if r.Label == label {
				return r
			}
		}
	}
	t.Fatalf("no %q row in %+v", label, v.Sections)
	return row{}
}

func TestPageEmbedsCurrentStats(t *testing.T) {
	stats := capacityLimits
	stats.ActiveHosts, stats.ControlConnections = 3, 4
	d := New(func() Stats { return stats }, unlimited)
	response := serve(t, d, "/", nil)
	body := response.Body.String()
	if response.Code != http.StatusOK || response.Header().Get("Content-Security-Policy") != contentSecurityPolicy || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("page: %d %v", response.Code, response.Header())
	}
	for _, want := range []string{`hx-get="stats"`, `hx-trigger="every 2s"`, ">Online<", "Connected hosts", "4 <small>of 10</small>"} {
		if !strings.Contains(body, want) {
			t.Fatalf("page is missing %q:\n%s", want, body)
		}
	}
}

func TestStatsFragmentReportsDraining(t *testing.T) {
	stats := capacityLimits
	stats.Draining = true
	body := serve(t, New(func() Stats { return stats }, unlimited), "/stats", nil).Body.String()
	if strings.Contains(body, "<html") || !strings.Contains(body, `class="status draining">Draining<`) {
		t.Fatalf("incorrect stats fragment:\n%s", body)
	}
}

func TestLimitAppliesToPagesNotAssets(t *testing.T) {
	reject := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		})
	}
	d := New(func() Stats { return capacityLimits }, reject)
	for path, status := range map[string]int{
		"/":                             http.StatusTooManyRequests,
		"/stats":                        http.StatusTooManyRequests,
		"/static/htmx-2.0.11.min.js":    http.StatusOK,
		"/static/dashboard.js":          http.StatusOK,
		"/static/dashboard.css":         http.StatusOK,
		"/static/missing.js":            http.StatusNotFound,
		"/static/":                      http.StatusNotFound,
		"/templates/page.html":          http.StatusNotFound,
		"/static/..%2ftemplates%2fpage": http.StatusNotFound,
	} {
		response := serve(t, d, path, nil)
		if response.Code != status {
			t.Errorf("%s: status %d, want %d", path, response.Code, status)
		}
		if response.Header().Get("Content-Security-Policy") != contentSecurityPolicy || response.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("%s: missing security headers: %v", path, response.Header())
		}
	}
}

func TestAssetsRevalidateWithETags(t *testing.T) {
	d := New(func() Stats { return capacityLimits }, unlimited)
	first := serve(t, d, "/static/htmx-2.0.11.min.js", nil)
	tag := first.Header().Get("ETag")
	if first.Code != http.StatusOK || tag == "" || first.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("asset: %d %v", first.Code, first.Header())
	}
	if again := serve(t, d, "/static/htmx-2.0.11.min.js", http.Header{"If-None-Match": {tag}}); again.Code != http.StatusNotModified || again.Body.Len() != 0 {
		t.Fatalf("revalidation: %d with %d bytes", again.Code, again.Body.Len())
	}
	if other := serve(t, d, "/static/dashboard.js", nil).Header().Get("ETag"); other == tag {
		t.Fatal("distinct assets share an ETag")
	}
}

func TestSnapshotSamplesAtMostOncePerRefresh(t *testing.T) {
	calls := 0
	d := New(func() Stats {
		calls++
		return capacityLimits
	}, unlimited)
	for _, offset := range []time.Duration{time.Second, 1500 * time.Millisecond, 1999 * time.Millisecond, 2 * time.Second} {
		d.snapshot(d.started.Add(offset))
	}
	if calls != 2 {
		t.Fatalf("relay sampled %d times, want 2", calls)
	}
}

func TestBuildReportsRatesOverTheSampleWindow(t *testing.T) {
	d := &Dashboard{started: time.Now()}
	previous := sample{at: d.started, relay: capacityLimits, cpuOK: true, cpus: 4}
	current := previous
	current.at = previous.at.Add(2 * time.Second)
	current.relay.ForwardedMessages, current.relay.ForwardedBytes = 40, 4096
	current.cpu = time.Second
	v := d.build(previous, current)
	for label, want := range map[string]row{
		"Messages":      {Label: "Messages", Value: "20/s", Detail: "40 total"},
		"Throughput":    {Label: "Throughput", Value: "2.0 KiB/s", Detail: "4.0 KiB total"},
		"Relay process": {Label: "Relay process", Value: "12.5%", Detail: "of 4 cores", Level: 0.125, Meter: true},
	} {
		if got := findRow(t, v, label); got != want {
			t.Errorf("%s = %+v, want %+v", label, got, want)
		}
	}
}

func TestBuildOmitsRatesAfterAQuietPeriod(t *testing.T) {
	d := &Dashboard{started: time.Now()}
	previous := sample{at: d.started, relay: capacityLimits, cpuOK: true, cpus: 1}
	current := previous
	current.at = previous.at.Add(time.Hour)
	current.relay.ForwardedMessages = 3600
	v := d.build(previous, current)
	for _, label := range []string{"Messages", "Throughput", "Relay process"} {
		if got := findRow(t, v, label); got.Value != unknown || got.Meter {
			t.Errorf("%s = %+v, want an unknown rate", label, got)
		}
	}
	if got := findRow(t, v, "Relay process").Detail; got != "of 1 core" {
		t.Errorf("core detail = %q", got)
	}
}

func TestBuildGaugesCapacityAndMemoryLimits(t *testing.T) {
	d := &Dashboard{started: time.Now()}
	current := sample{
		at:          d.started,
		relay:       Stats{ActiveHosts: 1, ControlConnections: 2, ClientSlots: 5, MaxHosts: 4, MaxClients: 10, MaxConnections: 10},
		retained:    1 << 30,
		memoryLimit: 2 << 30,
		systemOK:    true,
		systemUsed:  3 << 30,
		systemTotal: 4 << 30,
	}
	v := d.build(current, current)
	for label, want := range map[string]row{
		"Hosts":      {Label: "Hosts", Value: "2", Detail: "of 4", Level: 0.5, Meter: true},
		"Pairs":      {Label: "Pairs", Value: "5", Detail: "of 10", Level: 0.5, Meter: true},
		"Go runtime": {Label: "Go runtime", Value: "1.0 GiB", Detail: "of 2.0 GiB limit", Level: 0.5, Meter: true},
		"System":     {Label: "System", Value: "3.0 GiB", Detail: "of 4.0 GiB", Level: 0.75, Meter: true},
	} {
		if got := findRow(t, v, label); got != want {
			t.Errorf("%s = %+v, want %+v", label, got, want)
		}
	}
	current.memoryLimit = math.MaxInt64
	if got := findRow(t, d.build(current, current), "Go runtime"); got.Meter || got.Detail != "" {
		t.Errorf("unlimited runtime memory = %+v", got)
	}
}

func TestCapacityClampsMeterLevel(t *testing.T) {
	for _, tc := range []struct {
		n, limit int
		level    float64
	}{{0, 10, 0}, {5, 10, 0.5}, {12, 10, 1}} {
		if got := capacity("hosts", tc.n, tc.limit); !got.Meter || got.Level != tc.level {
			t.Errorf("capacity(%d, %d) = %+v, want level %v", tc.n, tc.limit, got, tc.level)
		}
	}
}

func TestParseLoadAverage(t *testing.T) {
	if load, ok := parseLoadAverage("0.52 1.04 2.00 2/345 6789\n"); !ok || load != [3]float64{0.52, 1.04, 2} {
		t.Fatalf("parseLoadAverage = %v, %v", load, ok)
	}
	for _, invalid := range []string{"", "0.1 0.2", "0.1 x 0.3"} {
		if _, ok := parseLoadAverage(invalid); ok {
			t.Errorf("parseLoadAverage(%q) succeeded", invalid)
		}
	}
}

func TestParseMeminfo(t *testing.T) {
	const meminfo = "MemTotal:        4026412 kB\nMemFree:          123456 kB\nMemAvailable:    3026412 kB\n"
	used, total, ok := parseMeminfo(meminfo)
	if !ok || total != 4026412<<10 || used != 1000000<<10 {
		t.Fatalf("parseMeminfo = %d, %d, %v", used, total, ok)
	}
	for _, invalid := range []string{"", "MemTotal: 10 kB\n", "MemTotal: 10 kB\nMemAvailable: 20 kB\n", "MemTotal: x kB\nMemAvailable: 1 kB\n"} {
		if _, _, ok := parseMeminfo(invalid); ok {
			t.Errorf("parseMeminfo(%q) succeeded", invalid)
		}
	}
}
