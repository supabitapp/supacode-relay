package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/diagnostics"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

func TestMissingEndpointHasCorrelatedRoutingDiagnostics(t *testing.T) {
	rt, base, logs := testRouter(t, nil)
	request, err := http.NewRequest("GET", "http"+strings.TrimPrefix(base, "ws")+"/v1/connect?endpointId="+endpointA+"&token=private-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(diagnostics.Header, "ABCDabcd1234-_56")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", response.StatusCode)
	}
	trace := response.Header.Get(diagnostics.Header)
	if len(trace) != 16 || trace == request.Header.Get(diagnostics.Header) {
		t.Fatalf("router trusted a client trace or failed to return one: %q", trace)
	}
	wanted := map[string]bool{
		"request.begin": false, "router.directory.miss": false,
		"router.endpoint.missing": false, "router.route.unavailable": false, "request.end": false,
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || record["trace_id"] != trace {
			continue
		}
		if record["endpoint_tag"] != diagnostics.Tag(endpointA) {
			t.Fatalf("missing endpoint correlation: %v", record)
		}
		if event, ok := record["msg"].(string); ok {
			if _, exists := wanted[event]; exists {
				wanted[event] = true
			}
		}
		if record["msg"] == "request.end" && record["status"] != float64(404) {
			t.Fatalf("incorrect final status: %v", record)
		}
	}
	for event, found := range wanted {
		if !found {
			t.Errorf("missing %s", event)
		}
	}
	if rt.misses[routeConnect].Load() != 1 {
		t.Fatal("diagnostics changed routing admission")
	}
	for _, secret := range []string{endpointA, "private-token"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log exposed %q", secret)
		}
	}
}

func TestProxiedResponsesKeepOneTraceAndTheForwardedClientTag(t *testing.T) {
	for _, status := range []int{http.StatusSwitchingProtocols, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rt, base, logs := testRouter(t, map[string]string{"ROUTER_TRUSTED_PROXIES": "127.0.0.1/32"})
			nodeLogs := &lockedBuffer{}
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				trace, request := diagnostics.Request(diagnostics.Logger(nodeLogs, "node", "node-a"), r, diagnostics.RequestOptions{
					TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
					ClientIPHeader: directory.ClientIPHeader,
					InheritTrace:   true,
				})
				trace.Event("request.begin")
				if status != http.StatusSwitchingProtocols {
					w.Header().Set(diagnostics.Header, trace.ID())
					w.WriteHeader(status)
					return
				}
				upgrader := websocket.Upgrader{}
				ws, err := upgrader.Upgrade(w, request, http.Header{diagnostics.Header: {trace.ID()}})
				if err != nil {
					return
				}
				defer ws.Close()
				_, _, _ = ws.ReadMessage()
			}))
			t.Cleanup(node.Close)
			addNode(t, rt, "node-a", node.URL, directory.Registration{EndpointID: endpointA, RegistrationID: "r1", Version: 1})
			const suppliedTrace = "ABCDabcd1234-_56"
			ws, response, err := dial(t, base, "/v1/connect?endpointId="+endpointA, http.Header{
				diagnostics.Header: {suppliedTrace},
				"X-Forwarded-For":  {"203.0.113.2"},
			})
			if status == http.StatusSwitchingProtocols && err != nil || status != http.StatusSwitchingProtocols && err == nil {
				t.Fatalf("unexpected upgrade result: %v", err)
			}
			if ws != nil {
				defer ws.Close()
			}
			if response == nil || response.StatusCode != status {
				t.Fatalf("wrong upstream response: %v", response)
			}
			values := response.Header.Values(diagnostics.Header)
			if len(values) != 1 || len(values[0]) != 16 || values[0] == suppliedTrace {
				t.Fatalf("public trace was duplicated or inherited: %v", values)
			}
			for _, output := range []*lockedBuffer{logs, nodeLogs} {
				found := false
				for _, line := range strings.Split(output.String(), "\n") {
					var record map[string]any
					if json.Unmarshal([]byte(line), &record) != nil || record["msg"] != "request.begin" {
						continue
					}
					found = true
					if record["trace_id"] != values[0] || record["client_tag"] != diagnostics.Tag("203.0.113.2") {
						t.Fatalf("trace or client attribution changed across router: %v", record)
					}
				}
				if !found {
					t.Fatal("missing request diagnostic")
				}
			}
		})
	}
}
