package diagnostics

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

func validEndpointID(id string) bool {
	if len(id) != 2*sha256.Size {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func endpointIDFromPublicKey(publicKey string) (string, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(publicKey)
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != publicKey {
		return "", false
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), true
}
