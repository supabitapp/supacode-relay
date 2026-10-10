package diagnostics

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/admission"
	"github.com/supabitapp/supacode-relay/internal/directory"
)

const Header = "X-Supacode-Relay-Trace"

var validTraceID = regexp.MustCompile(`^[A-Za-z0-9_-]{16}$`)

type traceKey struct{}

type Trace struct {
	id      string
	started time.Time
	log     *slog.Logger
}

func Logger(writer io.Writer, component, node string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, nil)).With("component", component, "node", node)
}

func Tag(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("supacode-relay-diagnostics/v1/" + value))
	return base64.RawURLEncoding.EncodeToString(sum[:9])
}

func New(log *slog.Logger, route string) *Trace {
	id := rand.Text()[:16]
	return &Trace{id: id, started: time.Now(), log: log.With("trace_id", id, "route", route)}
}

type RequestOptions struct {
	TrustedProxies []netip.Prefix
	ClientIPHeader string
	InheritTrace   bool
}

func Request(log *slog.Logger, r *http.Request, options RequestOptions) (*Trace, *http.Request) {
	trace := New(log, Route(r.URL.Path))
	if id := r.Header.Get(Header); options.InheritTrace && validTraceID.MatchString(id) {
		trace.id = id
		trace.log = log.With("trace_id", id, "route", Route(r.URL.Path))
	}
	trace = trace.With("client_tag", Tag(admission.ClientIP(r, options.TrustedProxies, options.ClientIPHeader).String()))
	query := r.URL.Query()
	id := query.Get("endpointId")
	if r.URL.Path == "/v1/control" {
		id, _ = directory.EndpointIDFromPublicKey(query.Get("publicKey"))
	}
	if directory.ValidEndpointID(id) {
		trace = trace.With("endpoint_tag", Tag(id))
	}
	return trace, r.WithContext(context.WithValue(r.Context(), traceKey{}, trace))
}

func From(r *http.Request) *Trace {
	trace, _ := r.Context().Value(traceKey{}).(*Trace)
	return trace
}

func (t *Trace) With(fields ...any) *Trace {
	if t == nil {
		return nil
	}
	return &Trace{id: t.id, started: t.started, log: t.log.With(fields...)}
}

func (t *Trace) ID() string {
	if t == nil {
		return ""
	}
	return t.id
}

func (t *Trace) Event(event string, fields ...any) {
	if t == nil {
		return
	}
	attrs := append([]any{"elapsed_ms", time.Since(t.started).Milliseconds()}, fields...)
	t.log.Info(event, attrs...)
}

func (t *Trace) Failure(event string, err error, fields ...any) {
	t.Event(event, append(fields, "error_kind", ErrorKind(err))...)
}

func Route(path string) string {
	switch path {
	case "/v1/control":
		return "control"
	case "/v1/connect":
		return "connect"
	case "/v1/accept":
		return "accept"
	case directory.Path:
		return "directory"
	default:
		return "other"
	}
}

func ErrorKind(err error) string {
	var closed *websocket.CloseError
	var network net.Error
	var operation *net.OpError
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &network) && network.Timeout():
		return "timeout"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, websocket.ErrReadLimit):
		return "message_too_big"
	case errors.As(err, &closed):
		return "websocket_close"
	case errors.As(err, &operation) && operation.Op == "dial":
		return "dial_failed"
	default:
		return "io_error"
	}
}

func CloseReason(reason string) string {
	switch reason {
	case "", "peer disconnected", "peer timeout", "peer unreachable", "peer write timeout",
		"pair timeout", "host offline", "upgrade failed", "host accept failed", "message too big",
		"control queue limit exceeded", "directory queue limit exceeded", "authentication failed",
		"relay draining", "relay shutting down", "unexpected control message", "writer stopped":
		return reason
	default:
		return "peer_closed"
	}
}

func CloseCode(err error) int {
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		return closed.Code
	}
	return 0
}

type Response struct {
	http.ResponseWriter
	Status   int
	Hijacked bool
}

func (w *Response) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *Response) WriteHeader(status int) {
	if w.Status == 0 {
		w.Status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *Response) Write(data []byte) (int, error) {
	if w.Status == 0 {
		w.Status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *Response) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, reader, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err == nil {
		w.Hijacked = true
		w.Status = http.StatusSwitchingProtocols
	}
	return conn, reader, err
}
