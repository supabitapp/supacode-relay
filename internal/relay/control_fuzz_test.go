package relay

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func FuzzDecodeB64(f *testing.F) {
	f.Add("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", false)
	f.Add("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB", false)
	f.Add("AAAAAAAAAAAAAAAAAAAA\nAAAAAAAAAAAAAAAAAAAAAAA", false)
	f.Add("", true)
	f.Fuzz(func(t *testing.T, encoded string, signature bool) {
		if len(encoded) > 4096 {
			return
		}
		size := 32
		if signature {
			size = 64
		}
		decoded, ok := decodeB64(encoded, size)
		if ok && (len(decoded) != size || base64.RawURLEncoding.EncodeToString(decoded) != encoded) {
			t.Fatal("accepted a noncanonical value or incorrect decoded size")
		}
		raw := make([]byte, size)
		copy(raw, encoded)
		canonical := base64.RawURLEncoding.EncodeToString(raw)
		decoded, ok = decodeB64(canonical, size)
		if !ok || !bytes.Equal(decoded, raw) {
			t.Fatal("canonical encoding did not round trip")
		}
		for _, invalid := range []string{canonical + "=", canonical[:len(canonical)-1], canonical[:10] + "\n" + canonical[10:]} {
			if _, ok := decodeB64(invalid, size); ok {
				t.Fatal("accepted padding, truncation, or an embedded newline")
			}
		}
	})
}
