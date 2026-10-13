package push

import "encoding/base64"

type PreviewEnvelope struct {
	Version    int    `json:"version"`
	KeyID      string `json:"keyId"`
	Enc        string `json:"enc"`
	Ciphertext string `json:"ciphertext"`
	Signature  string `json:"signature"`
}

func (preview *PreviewEnvelope) valid() bool {
	if preview == nil {
		return true
	}
	if preview.Version != 1 || !uuidPattern.MatchString(preview.KeyID) || len(preview.Enc) != 43 || len(preview.Signature) != 86 || len(preview.Ciphertext) < 22 || len(preview.Ciphertext) > 1046 {
		return false
	}
	encoding := base64.RawURLEncoding.Strict()
	enc, encErr := encoding.DecodeString(preview.Enc)
	signature, signatureErr := encoding.DecodeString(preview.Signature)
	ciphertext, ciphertextErr := encoding.DecodeString(preview.Ciphertext)
	return encErr == nil && signatureErr == nil && ciphertextErr == nil && len(enc) == 32 && len(signature) == 64 && len(ciphertext) >= 16 && len(ciphertext) <= 784
}
