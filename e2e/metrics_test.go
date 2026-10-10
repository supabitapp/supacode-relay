package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type hostTraffic struct {
	Endpoint string `json:"endpoint"`
	BytesIn  int64  `json:"bytesIn"`
	BytesOut int64  `json:"bytesOut"`
	Pairs    int    `json:"pairs"`
}

func topHosts(r *relay) []hostTraffic {
	r.t.Helper()
	_, body := r.get("/metrics")
	var m struct {
		TopHosts []hostTraffic `json:"topHosts"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		r.t.Fatal(err)
	}
	return m.TopHosts
}

func TestMetricsReportHostTraffic(t *testing.T) {
	r := startRelay(t)
	busy, quiet := register(t, r), register(t, r)
	client, host, _ := pairUp(t, r, busy)
	pairUp(t, r, quiet)

	send(t, client, message{websocket.BinaryMessage, randomBytes(1000)})
	recv(t, host)
	send(t, host, message{websocket.BinaryMessage, randomBytes(300)})
	recv(t, client)

	want := hostTraffic{Endpoint: busy.ID[:16], BytesIn: 1000, BytesOut: 300, Pairs: 1}
	end := time.Now().Add(deadline)
	for {
		got := topHosts(r)
		if len(got) == 1 && got[0] == want {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("topHosts = %+v, want only %+v", got, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublicMetricsExcludeHostDetails(t *testing.T) {
	r := startRelay(t, "RELAY_PRIVATE_ADDR=127.0.0.1:0")
	h := register(t, r)
	client, host, _ := pairUp(t, r, h)
	send(t, client, message{websocket.TextMessage, []byte("private-host-traffic")})
	recv(t, host)
	r.waitMetrics("forwarded traffic", func(m map[string]float64) bool { return m["forwardedBytes"] > 0 })
	response, err := http.Get("http://" + r.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var metrics map[string]any
	if err := json.Unmarshal(body, &metrics); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || metrics["activeHosts"] != float64(1) || metrics["activePairs"] != float64(1) {
		t.Fatalf("incorrect public metrics: %d %v", response.StatusCode, metrics)
	}
	if _, exists := metrics["topHosts"]; exists || strings.Contains(string(body), h.ID[:16]) {
		t.Fatal("public metrics expose host details")
	}
	if private := topHosts(r); len(private) != 1 || private[0].Endpoint != h.ID[:16] {
		t.Fatalf("missing private host details: %+v", private)
	}
}

func TestPublicMetricsHaveAnIndependentAdmissionBudget(t *testing.T) {
	r := startRelay(t, "RELAY_PRIVATE_ADDR=127.0.0.1:0", "RELAY_ADMISSION_RATE=1")
	response, err := http.Get("http://" + r.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("public metrics: %d", response.StatusCode)
	}
	register(t, r)
	response, err = http.Get("http://" + r.addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("public metrics limiter: %d", response.StatusCode)
	}
	if code, _ := r.get("/metrics"); code != http.StatusOK {
		t.Fatalf("private metrics throttled: %d", code)
	}
}
