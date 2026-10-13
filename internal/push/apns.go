package push

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sideshow/apns2"
	"github.com/sideshow/apns2/token"
)

type DeliveryResult struct {
	Accepted     bool  `json:"accepted"`
	Retryable    bool  `json:"retryable"`
	InvalidToken bool  `json:"invalidToken"`
	InvalidAt    int64 `json:"-"`
}

type Sender interface {
	Send(context.Context, Recipient, map[string]any, string, time.Time) DeliveryResult
}

type APNsSender struct {
	clients map[string]*apns2.Client
}

func NewAPNsSender(teamID string, keys map[string]Credentials) (*APNsSender, error) {
	clients := make(map[string]*apns2.Client)
	for environment, credentials := range keys {
		key, err := token.AuthKeyFromFile(credentials.Path)
		if err != nil {
			return nil, fmt.Errorf("load %s APNs credential: %w", environment, err)
		}
		client := apns2.NewTokenClient(&token.Token{AuthKey: key, KeyID: credentials.KeyID, TeamID: teamID})
		client.HTTPClient.Timeout = 8 * time.Second
		if environment == "sandbox" {
			client = client.Development()
		} else {
			client = client.Production()
		}
		clients[environment] = client
	}
	return &APNsSender{clients: clients}, nil
}

func (sender *APNsSender) Send(ctx context.Context, recipient Recipient, data map[string]any, notificationID string, expiry time.Time) DeliveryResult {
	client := sender.clients[recipient.Environment]
	if client == nil {
		return DeliveryResult{Retryable: true}
	}
	encoded, err := marshalAPNsPayload(recipient.ID, data)
	if err != nil {
		return DeliveryResult{}
	}
	response, err := client.PushWithContext(ctx, &apns2.Notification{
		DeviceToken: recipient.Token, Topic: recipient.Topic, Payload: encoded,
		PushType: apns2.PushTypeAlert, Priority: apns2.PriorityHigh,
		ApnsID: notificationID, CollapseID: notificationID, Expiration: expiry,
	})
	if err != nil {
		return DeliveryResult{Retryable: true}
	}
	if response.Sent() {
		return DeliveryResult{Accepted: true}
	}
	invalid := response.Reason == apns2.ReasonUnregistered || response.Reason == apns2.ReasonBadDeviceToken
	invalidAt := int64(0)
	if !response.Timestamp.IsZero() {
		invalidAt = response.Timestamp.UnixMilli()
	}
	return DeliveryResult{
		Retryable:    response.StatusCode == 429 || response.StatusCode >= 500 || response.Reason == apns2.ReasonExpiredProviderToken,
		InvalidToken: invalid,
		InvalidAt:    invalidAt,
	}
}

func marshalAPNsPayload(recipientID string, data map[string]any) ([]byte, error) {
	aps := map[string]any{
		"alert": map[string]string{"title": "Supacode", "body": "Supacode has an update"},
		"sound": "default", "thread-id": recipientID,
	}
	if data["preview"] != nil {
		aps["mutable-content"] = 1
	}
	payload := map[string]any{
		"aps":      aps,
		"supacode": data,
	}
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > 4096 {
		return nil, fmt.Errorf("APNs payload exceeds its budget or cannot be encoded")
	}
	return encoded, nil
}
