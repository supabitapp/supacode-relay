package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supaterm-relay/internal/admission"
)

func TestRedirectsCannotInjectHeadersOrCrash(t *testing.T) {
	r := startRelay(t)
	client := http.Client{Timeout: deadline, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, upgrade := range []bool{false, true} {
		request, err := http.NewRequest("GET", "http://"+r.addr+"/%0D%0A//dashboard/", nil)
		if err != nil {
			t.Fatal(err)
		}
		if upgrade {
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Upgrade", "websocket")
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		location := response.Header.Get("Location")
		if response.StatusCode != 307 || location == "" || strings.ContainsAny(location, "\r\n") {
			t.Fatalf("unsafe redirect: %d %q", response.StatusCode, location)
		}
		if status, _ := r.get("/healthz"); status != 200 {
			t.Fatalf("health after redirect %d", status)
		}
	}
}

func TestAddressFixtures(t *testing.T) {
	data, err := os.ReadFile("../testdata/addresses.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Address string
		Key     *string
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		ip, err := netip.ParseAddr(fixture.Address)
		if fixture.Key == nil {
			if err == nil {
				t.Errorf("accepted %s", fixture.Address)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		key := admission.ClientKey(ip)
		var encoded string
		if key.Is4() {
			encoded = "4:" + strings.TrimLeft(fmt.Sprintf("%x", key.As4()), "0")
		} else {
			value := key.As16()
			encoded = "6:" + strings.TrimLeft(fmt.Sprintf("%x", value[:8]), "0")
		}
		if strings.HasSuffix(encoded, ":") {
			encoded += "0"
		}
		if encoded != *fixture.Key {
			t.Errorf("%s: %s, want %s", fixture.Address, encoded, *fixture.Key)
		}
		if fixture.Address == "::ffff:192.0.2.4" && key != netip.MustParseAddr("192.0.2.4") {
			t.Fatal("mapped address did not share quota")
		}
		if key.Is6() && key != netip.PrefixFrom(ip, 64).Masked().Addr() {
			t.Fatal("IPv6 prefix did not share quota")
		}
	}
}

func TestRawFrameProtocolErrors(t *testing.T) {
	for _, frame := range [][]byte{{0x82, 0}, {0x80, 0x80}, {0x09, 0x80}, {0xc2, 0x80}} {
		r := startRelay(t)
		client, host, _ := pairUp(t, r, register(t, r))
		_ = client.SetWriteDeadline(time.Now().Add(deadline))
		if _, err := client.UnderlyingConn().Write(frame); err != nil {
			t.Fatal(err)
		}
		expectClose(t, client, websocket.CloseProtocolError)
		expectClose(t, host, websocket.CloseGoingAway)
	}
}

func TestNetworkQuotaAndFailedUpgradeRelease(t *testing.T) {
	r := startRelay(t, "RELAY_TRUSTED_PROXIES=127.0.0.1/32", "RELAY_MAX_CLIENTS_PER_HOST_PER_IP=1")
	h := register(t, r)
	dial := func(ip string) (*websocket.Conn, *http.Response, error) {
		return websocket.DefaultDialer.Dial(r.base+"/v1/connect?endpointId="+h.ID, http.Header{"X-Forwarded-For": []string{ip}})
	}
	first, _, err := dial("2001:db8:1:2::1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	_, response, err := dial("2001:db8:1:2::ffff")
	expectStatus(t, response, err, 429)
	second, _, err := dial("2001:db8:1:3::1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	client := http.Client{Timeout: deadline}
	response, err = client.Get("http://" + r.addr + "/v1/connect?endpointId=" + h.ID)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatalf("failed upgrade status %d", response.StatusCode)
	}
	r.waitMetrics("failed upgrade reservation released", func(m map[string]float64) bool { return m["clientSlots"] == 2 })
}
