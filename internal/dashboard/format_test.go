package dashboard

import (
	"testing"
	"time"
)

func TestFormatCount(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 123456: "123,456", 1234567: "1,234,567"} {
		if got := formatCount(n); got != want {
			t.Errorf("formatCount(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	for n, want := range map[float64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 3 << 30: "3.0 GiB"} {
		if got := formatBytes(n); got != want {
			t.Errorf("formatBytes(%v) = %q, want %q", n, got, want)
		}
	}
}

func TestFormatQuantity(t *testing.T) {
	for n, want := range map[float64]string{0: "0.0", 2.25: "2.2", 10: "10", 12345.6: "12,346"} {
		if got := formatQuantity(n); got != want {
			t.Errorf("formatQuantity(%v) = %q, want %q", n, got, want)
		}
	}
}

func TestFormatUptime(t *testing.T) {
	for d, want := range map[time.Duration]string{
		42 * time.Second:                            "42s",
		3*time.Minute + 5*time.Second:               "3m 5s",
		2*time.Hour + 7*time.Minute + 9*time.Second: "2h 7m",
		50*time.Hour + 30*time.Minute:               "2d 2h 30m",
	} {
		if got := formatUptime(d); got != want {
			t.Errorf("formatUptime(%s) = %q, want %q", d, got, want)
		}
	}
}
