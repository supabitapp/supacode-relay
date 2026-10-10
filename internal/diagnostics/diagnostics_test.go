package diagnostics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestTraceCorrelationAndSensitiveValues(t *testing.T) {
	const incoming = "ABCDabcd1234-_56"
	endpoint := strings.Repeat("a", 64)
	for _, trusted := range []bool{false, true} {
		var output bytes.Buffer
		log := Logger(&output)
		request := httptest.NewRequest("GET", "http://relay/v1/connect?endpointId="+endpoint+"&token=pair-secret", nil)
		request.RemoteAddr = "127.0.0.1:1234"
		request.Header.Set(Header, incoming)
		request.Header.Set("Authorization", "Bearer bearer-secret")
		request.Header.Set("Cookie", "session=cookie-secret")
		var peers []netip.Prefix
		if trusted {
			peers = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
		}
		trace, enriched := Request(log, request, RequestOptions{TrustedProxies: peers})
		if trace.ID() == incoming || From(enriched).ID() != trace.ID() {
			t.Fatalf("incorrect trace trust or context propagation: trusted=%v", trusted)
		}
		trace.Failure("request.failed", errors.New("https://upstream?token=error-secret"), "status", 502)
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record["endpoint_tag"] != Tag(endpoint) || record["trace_id"] != trace.ID() || record["error_kind"] != "io_error" || record["status"] != float64(502) {
			t.Fatalf("missing useful failure fields: %v", record)
		}
		for _, secret := range []string{endpoint, "pair-secret", "bearer-secret", "cookie-secret", "error-secret", "http://relay", "https://upstream", "127.0.0.1"} {
			if strings.Contains(output.String(), secret) {
				t.Fatalf("log exposed %q", secret)
			}
		}
	}
}

func TestClientTagsUseTheSameForwardedAddressAsAdmission(t *testing.T) {
	var output bytes.Buffer
	request := httptest.NewRequest("GET", "http://relay/v1/connect", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	request.Header.Set("X-Forwarded-For", "198.51.100.1")
	request.Header.Set("X-Relay-Client-Ip", "203.0.113.2")
	trace, _ := Request(Logger(&output), request, RequestOptions{
		TrustedProxies: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")},
		ClientIPHeader: "X-Relay-Client-Ip",
	})
	trace.Event("request.begin")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["client_tag"] != Tag("203.0.113.2") {
		t.Fatalf("wrong client attribution: %v", record)
	}
}

func TestErrorsAndRemoteCloseReasonsAreClassifiedWithoutTheirContents(t *testing.T) {
	for _, test := range []struct {
		err  error
		kind string
	}{
		{context.Canceled, "cancelled"},
		{context.DeadlineExceeded, "timeout"},
		{io.EOF, "eof"},
		{websocket.ErrReadLimit, "message_too_big"},
		{&websocket.CloseError{Code: 1008, Text: "private-close-text"}, "websocket_close"},
		{&net.OpError{Op: "dial", Err: errors.New("private-address")}, "dial_failed"},
	} {
		if got := ErrorKind(test.err); got != test.kind {
			t.Errorf("error class %q, want %q", got, test.kind)
		}
	}
	if CloseReason("private-close-text") != "peer_closed" || CloseReason("pair timeout") != "pair timeout" {
		t.Fatal("close reasons were not normalized")
	}
}

func TestOperatorCommandFindsTheSameEndpointTagWithoutServerConfiguration(t *testing.T) {
	var output, failure bytes.Buffer
	id := strings.Repeat("a", 64)
	code, handled := Command([]string{"relay", "trace-tag", id}, &output, &failure)
	if !handled || code != 0 || strings.TrimSpace(output.String()) != Tag(id) || failure.Len() != 0 {
		t.Fatalf("invalid tag command result: %d %v %q %q", code, handled, output.String(), failure.String())
	}
	output.Reset()
	code, handled = Command([]string{"relay", "trace-tag", "private-invalid-input"}, &output, &failure)
	if !handled || code != 2 || output.Len() != 0 || strings.Contains(failure.String(), "private-invalid-input") {
		t.Fatal("invalid operator input was not safely rejected")
	}
}
