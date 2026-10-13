package push

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func previewFixture() *PreviewEnvelope {
	return &PreviewEnvelope{
		Version: 1, KeyID: notificationID,
		Enc:        base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		Ciphertext: base64.RawURLEncoding.EncodeToString(make([]byte, 784)),
		Signature:  base64.RawURLEncoding.EncodeToString(make([]byte, 64)),
	}
}

func TestPreviewPayloadRetainsGenericFallbackWithinAPNsBudget(t *testing.T) {
	preview := previewFixture()
	if !preview.valid() {
		t.Fatal("maximum bounded envelope rejected")
	}
	data := map[string]any{"version": 1, "recipientId": recipientID, "generation": 1, "notificationId": notificationID, "preview": preview}
	encoded, err := marshalAPNsPayload(recipientID, data)
	if err != nil || len(encoded) > 4096 {
		t.Fatal("bounded envelope did not fit APNs payload")
	}
	var payload struct {
		APS struct {
			Alert struct {
				Body string `json:"body"`
			} `json:"alert"`
			Mutable int `json:"mutable-content"`
		} `json:"aps"`
	}
	if err := json.Unmarshal(encoded, &payload); err != nil || payload.APS.Mutable != 1 || payload.APS.Alert.Body != "Supacode has an update" {
		t.Fatal("preview lost generic fallback or extension activation")
	}
	delete(data, "preview")
	encoded, err = marshalAPNsPayload(recipientID, data)
	if err != nil || strings.Contains(string(encoded), "mutable-content") {
		t.Fatal("generic alert activated preview extension")
	}
}

func TestGatewayForwardsOnlyValidatedOpaquePreview(t *testing.T) {
	f := setup(t)
	f.activate(t)
	request := delivery(f.now)
	request["preview"] = previewFixture()
	status(t, f.request(t, "/v1/push/send", request, f.key), 202)
	if len(f.sender.calls) != 2 || f.sender.calls[1]["preview"] == nil || f.sender.calls[1]["createdAt"] != f.now.UnixMilli() {
		t.Fatal("validated envelope metadata did not reach APNs adapter")
	}
	status(t, f.request(t, "/v1/push/send", request, f.key), 202)
	if len(f.sender.calls) != 2 {
		t.Fatal("duplicate encrypted payload reached APNs")
	}
	preview := previewFixture()
	preview.Ciphertext = base64.RawURLEncoding.EncodeToString(make([]byte, 785))
	request["preview"] = preview
	status(t, f.request(t, "/v1/push/send", request, f.key), 400)
	preview = previewFixture()
	preview.Enc = preview.Enc[:42] + "B"
	request["preview"] = preview
	status(t, f.request(t, "/v1/push/send", request, f.key), 400)
	request["preview"] = map[string]any{"version": 1, "keyId": notificationID, "title": "Private title"}
	status(t, f.request(t, "/v1/push/send", request, f.key), 400)
}
