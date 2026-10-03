package directory

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	Path = "/v1/directory"

	TypeHello    = "hello"
	TypeWelcome  = "welcome"
	TypeSnapshot = "snapshot"
	TypePut      = "put"
	TypeDel      = "del"
	TypeDrain    = "drain"
	TypeSync     = "sync"
	TypeSynced   = "synced"
	TypeEvict    = "evict"

	CloseSuperseded = 4001
	SupersededText  = "registration superseded"

	ClientIPHeader = "X-Relay-Client-Ip"
)

type Registration struct {
	EndpointID     string `json:"endpointId"`
	NodeID         string `json:"nodeId"`
	RegistrationID string `json:"registrationId"`
	Version        uint64 `json:"version"`
}

type Message struct {
	Type          string         `json:"type"`
	NodeID        string         `json:"nodeId,omitempty"`
	Addr          string         `json:"addr,omitempty"`
	Draining      bool           `json:"draining,omitempty"`
	Seq           uint64         `json:"seq,omitempty"`
	Clock         uint64         `json:"clock,omitempty"`
	Registration  *Registration  `json:"registration,omitempty"`
	Registrations []Registration `json:"registrations,omitempty"`
}

func Newer(a, b Registration) bool {
	if a.Version != b.Version {
		return a.Version > b.Version
	}
	return a.RegistrationID > b.RegistrationID
}

var nodeIDPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`)

func ValidNodeID(id string) bool {
	return nodeIDPattern.MatchString(id)
}

func ConnectionID(nodeID, random string) string {
	if nodeID == "" {
		return random
	}
	return nodeID + "." + random
}

func ConnectionNode(connectionID string) (string, bool) {
	node, rest, ok := strings.Cut(connectionID, ".")
	if !ok || rest == "" || !ValidNodeID(node) {
		return "", false
	}
	return node, true
}

func ValidEndpointID(id string) bool {
	if len(id) != 2*sha256.Size {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func EndpointIDFromPublicKey(publicKey string) (string, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(publicKey)
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != publicKey {
		return "", false
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), true
}

type Clock struct {
	mu   sync.Mutex
	last uint64
	Now  func() time.Time
}

func (c *Clock) Next() uint64 {
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	t := uint64(now().UnixNano())
	c.mu.Lock()
	defer c.mu.Unlock()
	if t <= c.last {
		t = c.last + 1
	}
	c.last = t
	return t
}

func (c *Clock) Observe(v uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.last = max(c.last, v)
}
