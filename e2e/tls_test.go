package e2e

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

type wsConn struct {
	ws      *websocket.Conn
	r       io.Reader
	wmu     sync.Mutex
	mangle  func([]byte) []byte
	lastOut []byte
}

func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.r != nil {
			n, err := c.r.Read(p)
			if err != io.EOF {
				return n, err
			}
			c.r = nil
			if n > 0 {
				return n, nil
			}
		}
		typ, r, err := c.ws.NextReader()
		if err != nil {
			return 0, err
		}
		if typ != websocket.BinaryMessage {
			return 0, errors.New("unexpected text frame")
		}
		c.r = r
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	out := append([]byte(nil), p...)
	if c.mangle != nil {
		out = c.mangle(out)
	}
	c.lastOut = append([]byte(nil), out...)
	if err := c.ws.WriteMessage(websocket.BinaryMessage, out); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) resendLast() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.ws.WriteMessage(websocket.BinaryMessage, c.lastOut)
}

func (c *wsConn) Close() error                       { return c.ws.Close() }
func (c *wsConn) LocalAddr() net.Addr                { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr               { return c.ws.RemoteAddr() }
func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }
func (c *wsConn) SetDeadline(t time.Time) error {
	return errors.Join(c.ws.SetReadDeadline(t), c.ws.SetWriteDeadline(t))
}

func selfSigned(t *testing.T, name string) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, leaf
}

type tlsHost struct {
	results chan error
}

func serveTLS(t *testing.T, h *endpoint.Host, cert tls.Certificate) *tlsHost {
	th := &tlsHost{results: make(chan error, 16)}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}
	go func() {
		for ev := range h.Events {
			if ev.Type != "incoming" {
				continue
			}
			ws, _, err := h.Accept(ev)
			if err != nil {
				th.results <- err
				continue
			}
			go func() {
				conn := tls.Server(&wsConn{ws: ws}, cfg)
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(deadline))
				rd := bufio.NewReader(conn)
				for {
					line, err := rd.ReadString('\n')
					if err != nil {
						if errors.Is(err, io.EOF) {
							err = nil
						}
						th.results <- err
						return
					}
					if _, err := io.WriteString(conn, "secret-response:"+line); err != nil {
						th.results <- err
						return
					}
				}
			}()
		}
	}()
	return th
}

func (th *tlsHost) result(t *testing.T) error {
	t.Helper()
	select {
	case err := <-th.results:
		return err
	case <-time.After(deadline):
		t.Fatal("host TLS session did not finish")
		return nil
	}
}

func dialTLS(t *testing.T, r *relay, endpointID string, pinned *x509.Certificate) (*tls.Conn, *wsConn, error) {
	t.Helper()
	ws, resp, err := endpoint.Connect(r.base, endpointID)
	if err != nil {
		t.Fatalf("connect: %v %s", err, status(resp))
	}
	pool := x509.NewCertPool()
	pool.AddCert(pinned)
	wc := &wsConn{ws: ws}
	conn := tls.Client(wc, &tls.Config{RootCAs: pool, ServerName: "supacode-test-host", MinVersion: tls.VersionTLS13})
	_ = conn.SetDeadline(time.Now().Add(deadline))
	return conn, wc, conn.Handshake()
}

func roundTrip(conn *tls.Conn, rd *bufio.Reader, req string) error {
	if _, err := io.WriteString(conn, req+"\n"); err != nil {
		return err
	}
	line, err := rd.ReadString('\n')
	if err != nil {
		return err
	}
	if line != "secret-response:"+req+"\n" {
		return errors.New("unexpected response " + line)
	}
	return nil
}

func TestEndToEndEncryptedExchange(t *testing.T) {
	r := startRelay(t)
	hostCert, hostLeaf := selfSigned(t, "supacode-test-host")
	impostorCert, _ := selfSigned(t, "supacode-test-host")
	h := register(t, r)
	impostor := register(t, r)
	th := serveTLS(t, h, hostCert)
	ti := serveTLS(t, impostor, impostorCert)

	conn, _, err := dialTLS(t, r, h.ID, hostLeaf)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if v := conn.ConnectionState().Version; v != tls.VersionTLS13 {
		t.Fatalf("negotiated %x", v)
	}
	rd := bufio.NewReader(conn)
	for _, req := range []string{"GET /secret", `{"type":"e2ee_hello"}`, strings.Repeat("x", 30000)} {
		if err := roundTrip(conn, rd, req); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()
	if err := th.result(t); err != nil && !isClosed(err) {
		t.Fatalf("host session: %v", err)
	}

	conn, _, err = dialTLS(t, r, impostor.ID, hostLeaf)
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("impostor accepted: %v", err)
	}
	conn.Close()
	if err := ti.result(t); err == nil {
		t.Fatal("impostor host saw no handshake failure")
	}

	conn, wc, err := dialTLS(t, r, h.ID, hostLeaf)
	if err != nil {
		t.Fatal(err)
	}
	rd = bufio.NewReader(conn)
	if err := roundTrip(conn, rd, "before tamper"); err != nil {
		t.Fatal(err)
	}
	wc.wmu.Lock()
	wc.mangle = func(b []byte) []byte {
		b[len(b)-1] ^= 0x01
		return b
	}
	wc.wmu.Unlock()
	_, _ = io.WriteString(conn, "tampered\n")
	if err := th.result(t); err == nil || !strings.Contains(err.Error(), "bad record MAC") {
		t.Fatalf("modified ciphertext accepted: %v", err)
	}
	conn.Close()

	conn, wc, err = dialTLS(t, r, h.ID, hostLeaf)
	if err != nil {
		t.Fatal(err)
	}
	rd = bufio.NewReader(conn)
	if err := roundTrip(conn, rd, "pay 10"); err != nil {
		t.Fatal(err)
	}
	if err := wc.resendLast(); err != nil {
		t.Fatal(err)
	}
	if err := th.result(t); err == nil || !strings.Contains(err.Error(), "bad record MAC") {
		t.Fatalf("replayed record accepted: %v", err)
	}
	conn.Close()

	conn, _, err = dialTLS(t, r, h.ID, hostLeaf)
	if err != nil {
		t.Fatalf("fresh reconnect: %v", err)
	}
	rd = bufio.NewReader(conn)
	if err := roundTrip(conn, rd, "fresh session"); err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func isClosed(err error) bool {
	var ce *websocket.CloseError
	return errors.As(err, &ce) || errors.Is(err, net.ErrClosed)
}
