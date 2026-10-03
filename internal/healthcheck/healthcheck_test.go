package healthcheck

import "testing"

func TestURLUsesLoopbackForUnspecifiedBinds(t *testing.T) {
	cases := map[string]string{
		"0.0.0.0:9090":    "http://127.0.0.1:9090/healthz",
		"[::]:9090":       "http://127.0.0.1:9090/healthz",
		":9090":           "http://127.0.0.1:9090/healthz",
		"10.1.2.3:9090":   "http://10.1.2.3:9090/healthz",
		"127.0.0.1:18080": "http://127.0.0.1:18080/healthz",
	}
	for addr, want := range cases {
		if got, err := URL(addr, "/healthz"); err != nil || got != want {
			t.Errorf("%s: got %q %v want %q", addr, got, err, want)
		}
	}
}
