package e2e

import (
	"encoding/json"
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
