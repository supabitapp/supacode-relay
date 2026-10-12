package push

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const recipientID = "e27a8dac-172b-4b24-86c0-991ae3d7f178"
const notificationID = "b7912a03-566d-4638-87f4-e0c21bfa82f7"
const secret = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"

type fakeSender struct {
	calls  []map[string]any
	result DeliveryResult
}

func (sender *fakeSender) Send(_ context.Context, _ Recipient, data map[string]any, _ string, _ time.Time) DeliveryResult {
	sender.calls = append(sender.calls, data)
	return sender.result
}

type fixture struct {
	store   *Store
	sender  *fakeSender
	gateway *Gateway
	key     ed25519.PrivateKey
	now     time.Time
}

func setup(t *testing.T) *fixture {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "push.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{result: DeliveryResult{Accepted: true}}
	f := &fixture{store: store, sender: sender, key: key, now: time.Now().Truncate(time.Second)}
	f.gateway = NewGateway(store, sender, []string{"com.supaterm.supacode"}, []string{"sandbox"})
	f.gateway.now = func() time.Time { return f.now }
	return f
}
func (f *fixture) request(t *testing.T, path string, value any, key ed25519.PrivateKey) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	if key != nil {
		timestamp := f.now.Unix()
		digest := sha256.Sum256(body)
		message := fmt.Sprintf("supacode-push-request-v1\nPOST\n%s\n%d\n%s", path, timestamp, hex.EncodeToString(digest[:]))
		r.Header.Set("X-Supacode-Host", base64.RawURLEncoding.EncodeToString(key.Public().(ed25519.PublicKey)))
		r.Header.Set("X-Supacode-Timestamp", fmt.Sprint(timestamp))
		r.Header.Set("X-Supacode-Signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(message))))
	}
	response := httptest.NewRecorder()
	f.gateway.Handler().ServeHTTP(response, r)
	return response
}
func enrollment(generation int) map[string]any {
	return map[string]any{"recipientId": recipientID, "generation": generation, "token": strings.Repeat("ab", 32), "topic": "com.supaterm.supacode", "environment": "sandbox", "revocationToken": secret}
}
func delivery(now time.Time) map[string]any {
	return map[string]any{"version": 1, "recipientId": recipientID, "generation": 1, "notificationId": notificationID, "createdAt": now.UnixMilli(), "expiresAt": now.Add(time.Hour).UnixMilli()}
}
func status(t *testing.T, response *httptest.ResponseRecorder, expected int) {
	t.Helper()
	if response.Code != expected {
		t.Fatalf("status %d, want %d: %s", response.Code, expected, response.Body)
	}
}
func (f *fixture) activate(t *testing.T) {
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), f.key), 202)
	challenge := f.sender.calls[len(f.sender.calls)-1]["challenge"]
	status(t, f.request(t, "/v1/push/confirm", map[string]any{"recipientId": recipientID, "generation": 1, "challenge": challenge}, f.key), 200)
}
func TestEnrollmentRequiresTokenPossessionAndHostOwnership(t *testing.T) {
	f := setup(t)
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), nil), 401)
	response := f.request(t, "/v1/push/enroll", enrollment(1), f.key)
	status(t, response, 202)
	if strings.Contains(response.Body.String(), "challenge") {
		t.Fatal("challenge disclosed to host before APNs receipt")
	}
	status(t, f.request(t, "/v1/push/send", delivery(f.now), f.key), 404)
	status(t, f.request(t, "/v1/push/confirm", map[string]any{"recipientId": recipientID, "generation": 1, "challenge": secret}, f.key), 404)
	challenge := f.sender.calls[0]["challenge"]
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	status(t, f.request(t, "/v1/push/confirm", map[string]any{"recipientId": recipientID, "generation": 1, "challenge": challenge}, other), 404)
	status(t, f.request(t, "/v1/push/confirm", map[string]any{"recipientId": recipientID, "generation": 1, "challenge": challenge}, f.key), 200)
}
func TestDeliveryDeduplicationAndRevocation(t *testing.T) {
	f := setup(t)
	f.activate(t)
	request := delivery(f.now)
	status(t, f.request(t, "/v1/push/send", request, f.key), 202)
	status(t, f.request(t, "/v1/push/send", request, f.key), 202)
	if len(f.sender.calls) != 2 {
		t.Fatal("duplicate reached APNs")
	}
	request["expiresAt"] = f.now.Add(2 * time.Hour).UnixMilli()
	status(t, f.request(t, "/v1/push/send", request, f.key), 409)
	status(t, f.request(t, "/v1/push/device-revoke", map[string]any{"recipientId": recipientID, "generation": 1, "revocationToken": strings.Repeat("a", 43)}, nil), 404)
	status(t, f.request(t, "/v1/push/device-revoke", map[string]any{"recipientId": recipientID, "generation": 1, "revocationToken": secret}, nil), 200)
	status(t, f.request(t, "/v1/push/send", delivery(f.now), f.key), 404)
}
func TestPendingEnrollmentCanRecoverUnsentChallenge(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	host := base64.RawURLEncoding.EncodeToString(f.key.Public().(ed25519.PublicKey))
	_, _, err := f.store.Enroll(ctx, Recipient{ID: recipientID, Host: host, Generation: 1, Token: strings.Repeat("ab", 32), Topic: "com.supaterm.supacode", Environment: "sandbox", ChallengeHash: hash(secret), ChallengeExpiry: f.now.Add(5 * time.Minute).UnixMilli(), RevocationHash: hash(secret)}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), f.key), 429)
	f.now = f.now.Add(time.Minute)
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), f.key), 202)
	if len(f.sender.calls) != 1 {
		t.Fatal("unsent enrollment did not recover")
	}
}
func TestStaleInvalidationDoesNotRevokeCurrentToken(t *testing.T) {
	f := setup(t)
	f.activate(t)
	f.sender.result = DeliveryResult{InvalidToken: true, InvalidAt: f.now.Add(-time.Minute).UnixMilli()}
	response := f.request(t, "/v1/push/send", delivery(f.now), f.key)
	status(t, response, 503)
	var result DeliveryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.InvalidToken || !result.Retryable {
		t.Fatal("stale invalidation reached host")
	}
	recipient, err := f.store.Recipient(context.Background(), recipientID)
	if err != nil {
		t.Fatal(err)
	}
	if recipient.Status != "active" {
		t.Fatal("current recipient revoked")
	}
	f.sender.result = DeliveryResult{InvalidToken: true}
	f.now = f.now.Add(time.Second)
	status(t, f.request(t, "/v1/push/send", delivery(f.now), f.key), 409)
}
func TestClaimRecoveryFencesLateWorker(t *testing.T) {
	f := setup(t)
	f.activate(t)
	ctx := context.Background()
	recipient, _ := f.store.Recipient(ctx, recipientID)
	state, _, first, err := f.store.Claim(ctx, recipientID, 1, notificationID, "digest", f.now.Add(time.Hour), f.now)
	if err != nil || state != "claimed" {
		t.Fatalf("first claim: %s %v", state, err)
	}
	secondStore, err := OpenStore(f.storePath(t))
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	state, _, second, err := secondStore.Claim(ctx, recipientID, 1, notificationID, "digest", f.now.Add(time.Hour), f.now.Add(16*time.Second))
	if err != nil || state != "claimed" || second == first {
		t.Fatalf("recovery: %s %v", state, err)
	}
	if err := secondStore.Finish(ctx, recipient, notificationID, second, DeliveryResult{Accepted: true}); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Finish(ctx, recipient, notificationID, first, DeliveryResult{Retryable: true}); err == nil {
		t.Fatal("stale worker overwrote new receipt")
	}
	state, _, _, err = f.store.Claim(ctx, recipientID, 1, notificationID, "digest", f.now.Add(time.Hour), f.now.Add(20*time.Second))
	if err != nil || state != "accepted" {
		t.Fatalf("acceptance lost: %s %v", state, err)
	}
}
func (f *fixture) storePath(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := f.store.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestStableSecretRevokesNewerGeneration(t *testing.T) {
	f := setup(t)
	f.activate(t)
	f.now = f.now.Add(time.Minute)
	status(t, f.request(t, "/v1/push/enroll", enrollment(2), f.key), 202)
	status(t, f.request(t, "/v1/push/device-revoke", map[string]any{"recipientId": recipientID, "generation": 1, "revocationToken": secret}, nil), 200)
	recipient, _ := f.store.Recipient(context.Background(), recipientID)
	if recipient.Status != "revoked" || recipient.Generation != 2 {
		t.Fatal("old device state did not revoke current recipient")
	}
	status(t, f.request(t, "/v1/push/enroll", enrollment(2), f.key), 409)
}
func TestPendingRegistryCapacityAndCleanup(t *testing.T) {
	f := setup(t)
	_, err := f.store.db.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000)
 INSERT INTO recipients SELECT printf('old-%d',x),printf('host-%d',x),1,'token','topic','sandbox','pending','',0,'',? FROM n`, f.now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), f.key), 503)
	f.now = f.now.Add(25 * time.Hour)
	status(t, f.request(t, "/v1/push/enroll", enrollment(1), f.key), 202)
}

func TestAbsentRecipientIsIdempotentlyDeviceRevoked(t *testing.T) {
	f := setup(t)
	status(t, f.request(t, "/v1/push/device-revoke", map[string]any{"recipientId": recipientID, "generation": 1, "revocationToken": secret}, nil), 200)
}

func TestTypeScriptSignatureFixture(t *testing.T) {
	body := []byte(`{"version":1}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/push/send", strings.NewReader(string(body)))
	request.Header.Set("X-Supacode-Host", "iojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1w")
	request.Header.Set("X-Supacode-Timestamp", "1791840000")
	request.Header.Set("X-Supacode-Signature", "BekRytzP30lJiPF4UOe3LmjrPeRd5DIeJ-9Dbzl3Gb_te_kUlV947XyRhleCf07oeYTuk96JupLiGggNKKElDw")
	now := time.Unix(1791840000, 0)
	if _, err := authenticate(request, body, now); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticate(request, []byte(`{"version":2}`), now); err == nil {
		t.Fatal("modified body authenticated")
	}
	request.URL.Path = "/v1/push/enroll"
	if _, err := authenticate(request, body, now); err == nil {
		t.Fatal("signature reused for another operation")
	}
}
