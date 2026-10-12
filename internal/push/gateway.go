package push

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
var tokenPattern = regexp.MustCompile(`^[0-9a-f]{32,256}$`)
var secretPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

type rateEntry struct {
	limiter *rate.Limiter
	seen    time.Time
}

type Gateway struct {
	store        *Store
	sender       Sender
	topics       map[string]bool
	environments map[string]bool
	now          func() time.Time
	global       *rate.Limiter
	mu           sync.Mutex
	hosts        map[string]rateEntry
	accepted     atomic.Uint64
	retried      atomic.Uint64
	failed       atomic.Uint64
}

func NewGateway(store *Store, sender Sender, topics, environments []string) *Gateway {
	allowedTopics, allowedEnvironments := make(map[string]bool), make(map[string]bool)
	for _, topic := range topics {
		allowedTopics[topic] = true
	}
	for _, environment := range environments {
		allowedEnvironments[environment] = true
	}
	return &Gateway{store: store, sender: sender, topics: allowedTopics, environments: allowedEnvironments,
		now: time.Now, global: rate.NewLimiter(50, 100), hosts: make(map[string]rateEntry)}
}

func (gateway *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := gateway.store.db.PingContext(r.Context()); err != nil {
			reply(w, 503, DeliveryResult{Retryable: true})
			return
		}
		reply(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		reply(w, 200, map[string]uint64{"accepted": gateway.accepted.Load(), "retryable": gateway.retried.Load(), "failed": gateway.failed.Load()})
	})
	for _, path := range []string{"enroll", "confirm", "revoke", "send", "device-revoke"} {
		mux.HandleFunc("POST /v1/push/"+path, gateway.handle)
	}
	return mux
}

func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

func decode(body []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing input")
	}
	return nil
}

func authenticate(r *http.Request, body []byte, now time.Time) (string, error) {
	encodedKey := r.Header.Get("X-Supacode-Host")
	publicKey, err := base64.RawURLEncoding.DecodeString(encodedKey)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return "", errors.New("invalid host")
	}
	signature, err := base64.RawURLEncoding.DecodeString(r.Header.Get("X-Supacode-Signature"))
	if err != nil || len(signature) != ed25519.SignatureSize {
		return "", errors.New("invalid signature")
	}
	timestamp, err := strconv.ParseInt(r.Header.Get("X-Supacode-Timestamp"), 10, 64)
	if err != nil || timestamp < now.Add(-5*time.Minute).Unix() || timestamp > now.Add(5*time.Minute).Unix() || r.URL.RawQuery != "" {
		return "", errors.New("expired request")
	}
	digest := sha256.Sum256(body)
	message := fmt.Sprintf("supacode-push-request-v1\nPOST\n%s\n%d\n%s", r.URL.Path, timestamp, hex.EncodeToString(digest[:]))
	if !ed25519.Verify(publicKey, []byte(message), signature) {
		return "", errors.New("invalid signature")
	}
	return encodedKey, nil
}

func (gateway *Gateway) allow(host string, now time.Time) bool {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	entry, found := gateway.hosts[host]
	if !found {
		if len(gateway.hosts) >= 4096 {
			for key, prior := range gateway.hosts {
				if now.Sub(prior.seen) > 15*time.Minute {
					delete(gateway.hosts, key)
				}
			}
			if len(gateway.hosts) >= 4096 {
				return false
			}
		}
		entry.limiter = rate.NewLimiter(2, 10)
	}
	entry.seen = now
	gateway.hosts[host] = entry
	return entry.limiter.Allow()
}

func (gateway *Gateway) handle(w http.ResponseWriter, r *http.Request) {
	if !gateway.global.Allow() {
		reply(w, 429, DeliveryResult{Retryable: true})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16*1024))
	if err != nil {
		reply(w, 400, DeliveryResult{})
		return
	}
	now := gateway.now()
	if r.URL.Path == "/v1/push/device-revoke" {
		gateway.revokeDevice(w, r, body)
		return
	}
	host, err := authenticate(r, body, now)
	if err != nil {
		reply(w, 401, DeliveryResult{})
		return
	}
	if !gateway.allow(host, now) {
		reply(w, 429, DeliveryResult{Retryable: true})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	switch r.URL.Path {
	case "/v1/push/enroll":
		gateway.enroll(ctx, w, host, body, now)
	case "/v1/push/confirm":
		gateway.confirm(ctx, w, host, body, now)
	case "/v1/push/revoke":
		gateway.revoke(ctx, w, host, body)
	case "/v1/push/send":
		gateway.send(ctx, w, host, body, now)
	}
}

type recipientRequest struct {
	RecipientID string `json:"recipientId"`
	Generation  int64  `json:"generation"`
}

func validRecipient(request recipientRequest) bool {
	return uuidPattern.MatchString(request.RecipientID) && request.Generation > 0
}

func (gateway *Gateway) enroll(ctx context.Context, w http.ResponseWriter, host string, body []byte, now time.Time) {
	var request struct {
		recipientRequest
		Token           string `json:"token"`
		Topic           string `json:"topic"`
		Environment     string `json:"environment"`
		RevocationToken string `json:"revocationToken"`
	}
	if decode(body, &request) != nil || !validRecipient(request.recipientRequest) || !tokenPattern.MatchString(request.Token) || !secretPattern.MatchString(request.RevocationToken) || !gateway.topics[request.Topic] || !gateway.environments[request.Environment] {
		reply(w, 400, DeliveryResult{})
		return
	}
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		reply(w, 503, DeliveryResult{Retryable: true})
		return
	}
	challenge := base64.RawURLEncoding.EncodeToString(bytes[:])
	recipient, send, err := gateway.store.Enroll(ctx, Recipient{
		ID: request.RecipientID, Host: host, Generation: request.Generation, Token: request.Token,
		Topic: request.Topic, Environment: request.Environment, ChallengeHash: hash(challenge),
		ChallengeExpiry: now.Add(5 * time.Minute).UnixMilli(), RevocationHash: hash(request.RevocationToken),
	}, now)
	if err != nil {
		gateway.storeError(w, err)
		return
	}
	if send {
		result := gateway.sender.Send(ctx, recipient, map[string]any{"version": 1, "recipientId": recipient.ID, "generation": recipient.Generation, "challenge": challenge}, "", now.Add(5*time.Minute))
		if !result.Accepted {
			if result.InvalidToken && (result.InvalidAt == 0 || result.InvalidAt >= recipient.UpdatedAt) {
				gateway.store.Revoke(ctx, host, recipient.ID, recipient.Generation)
				reply(w, 422, result)
				return
			}
			gateway.store.ExpireChallenge(ctx, recipient.ID, recipient.Generation)
			reply(w, 503, DeliveryResult{Retryable: true})
			return
		}
	}
	reply(w, 202, map[string]string{"status": recipient.Status})
}

func (gateway *Gateway) confirm(ctx context.Context, w http.ResponseWriter, host string, body []byte, now time.Time) {
	var request struct {
		recipientRequest
		Challenge string `json:"challenge"`
	}
	if decode(body, &request) != nil || !validRecipient(request.recipientRequest) || !secretPattern.MatchString(request.Challenge) {
		reply(w, 400, DeliveryResult{})
		return
	}
	recipient, err := gateway.store.Recipient(ctx, request.RecipientID)
	if err != nil || recipient.Host != host || recipient.Generation != request.Generation || recipient.Status == "revoked" {
		reply(w, 404, DeliveryResult{})
		return
	}
	if recipient.Status != "active" {
		if err := gateway.store.Confirm(ctx, host, request.RecipientID, request.Generation, request.Challenge, now); err != nil {
			gateway.storeError(w, err)
			return
		}
	}
	reply(w, 200, map[string]string{"status": "active"})
}

func (gateway *Gateway) revoke(ctx context.Context, w http.ResponseWriter, host string, body []byte) {
	var request recipientRequest
	if decode(body, &request) != nil || !validRecipient(request) {
		reply(w, 400, DeliveryResult{})
		return
	}
	if err := gateway.store.Revoke(ctx, host, request.RecipientID, request.Generation); err != nil {
		gateway.storeError(w, err)
		return
	}
	reply(w, 200, map[string]string{"status": "revoked"})
}

func (gateway *Gateway) revokeDevice(w http.ResponseWriter, r *http.Request, body []byte) {
	var request struct {
		recipientRequest
		RevocationToken string `json:"revocationToken"`
	}
	if decode(body, &request) != nil || !validRecipient(request.recipientRequest) || !secretPattern.MatchString(request.RevocationToken) {
		reply(w, 400, DeliveryResult{})
		return
	}
	if err := gateway.store.RevokeDevice(r.Context(), request.RecipientID, request.Generation, request.RevocationToken); err != nil {
		gateway.storeError(w, err)
		return
	}
	reply(w, 200, map[string]string{"status": "revoked"})
}

func (gateway *Gateway) send(ctx context.Context, w http.ResponseWriter, host string, body []byte, now time.Time) {
	var request struct {
		recipientRequest
		Version        int    `json:"version"`
		NotificationID string `json:"notificationId"`
		CreatedAt      int64  `json:"createdAt"`
		ExpiresAt      int64  `json:"expiresAt"`
	}
	if decode(body, &request) != nil || !validRecipient(request.recipientRequest) || request.Version != 1 || !uuidPattern.MatchString(request.NotificationID) || request.CreatedAt > now.Add(5*time.Minute).UnixMilli() || request.ExpiresAt <= now.UnixMilli() || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt > request.CreatedAt+int64((24*time.Hour)/time.Millisecond) {
		reply(w, 400, DeliveryResult{})
		return
	}
	recipient, err := gateway.store.Recipient(ctx, request.RecipientID)
	if err != nil || recipient.Host != host || recipient.Generation != request.Generation || recipient.Status != "active" {
		reply(w, 404, DeliveryResult{})
		return
	}
	status, invalid, claimToken, err := gateway.store.Claim(ctx, recipient.ID, recipient.Generation, request.NotificationID, hash(string(body)), time.UnixMilli(request.ExpiresAt), now)
	if err != nil {
		gateway.storeError(w, err)
		return
	}
	switch status {
	case "accepted":
		reply(w, 202, DeliveryResult{Accepted: true})
		return
	case "failed":
		reply(w, 422, DeliveryResult{InvalidToken: invalid})
		return
	case "running", "retry":
		reply(w, 429, DeliveryResult{Retryable: true})
		return
	}
	current, err := gateway.store.Recipient(ctx, recipient.ID)
	if err != nil || current.Generation != recipient.Generation || current.Status != "active" {
		reply(w, 404, DeliveryResult{})
		return
	}
	result := gateway.sender.Send(ctx, current, map[string]any{
		"version": 1, "recipientId": recipient.ID, "generation": recipient.Generation, "notificationId": request.NotificationID,
	}, request.NotificationID, time.UnixMilli(request.ExpiresAt))
	if result.InvalidToken && result.InvalidAt != 0 && result.InvalidAt < current.UpdatedAt {
		result.InvalidToken, result.Retryable = false, true
	}
	if err := gateway.store.Finish(ctx, current, request.NotificationID, claimToken, result); err != nil {
		gateway.storeError(w, err)
		return
	}
	if result.Accepted {
		gateway.accepted.Add(1)
		reply(w, 202, result)
	} else if result.Retryable {
		gateway.retried.Add(1)
		reply(w, 503, result)
	} else {
		gateway.failed.Add(1)
		reply(w, 422, result)
	}
}

func (gateway *Gateway) storeError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrRateLimited) {
		reply(w, 429, DeliveryResult{Retryable: true})
	} else if errors.Is(err, ErrCapacity) {
		reply(w, 503, DeliveryResult{Retryable: true})
	} else if errors.Is(err, ErrConflict) {
		reply(w, 409, DeliveryResult{})
	} else if errors.Is(err, ErrUnavailable) {
		reply(w, 404, DeliveryResult{})
	} else {
		reply(w, 503, DeliveryResult{Retryable: true})
	}
}
