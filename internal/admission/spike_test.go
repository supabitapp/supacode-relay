package admission

import (
	"net/netip"
	"testing"
	"time"
)

func BenchmarkSpikeLimiter(b *testing.B) {
	for _, full := range []bool{false, true} {
		name := "small"
		if full {
			name = "full"
		}
		for _, existing := range []bool{false, true} {
			kind := "new"
			if existing {
				kind = "existing"
			}
			b.Run(name+"/"+kind, func(b *testing.B) {
				now := time.Now()
				l := NewLimiter(1e9, 1000000000)
				count := 1
				if full {
					count = MaxEntries
				}
				for i := range count {
					ip := netip.AddrFrom16([16]byte{0x20, 1, 0xd, 0xb8, byte(i >> 16), byte(i >> 8), byte(i), 1})
					l.Allow(ip, now)
				}
				ip := netip.AddrFrom16([16]byte{0x20, 1, 0xd, 0xb8, 0, 0, 0, 1})
				if !existing {
					ip = netip.MustParseAddr("2001:db8:ffff::1")
				}
				b.ResetTimer()
				for range b.N {
					l.Allow(ip, now)
				}
			})
		}
	}
}
