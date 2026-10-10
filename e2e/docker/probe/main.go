package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

func main() {
	mode := flag.String("mode", "http", "http, control, connect, or smoke")
	target := flag.String("url", "", "URL for http mode, ws base for control and connect")
	n := flag.Int("n", 1, "attempts")
	xff := flag.String("xff", "", "spoofed X-Forwarded-For, X-Real-IP, and Forwarded value")
	endpointID := flag.String("endpoint", strings.Repeat("ab", 32), "endpoint id for connect mode")
	flag.Parse()

	out := map[string]any{"mode": *mode}
	switch *mode {
	case "smoke":
		pairs, err := smoke(*target, *n)
		out["pairsVerified"] = pairs
		if err != nil {
			out["error"] = err.Error()
			_ = json.NewEncoder(os.Stdout).Encode(out)
			os.Exit(1)
		}
	case "http":
		c := http.Client{Timeout: 3 * time.Second}
		resp, err := c.Get(*target)
		if err != nil {
			out["error"] = err.Error()
			break
		}
		resp.Body.Close()
		out["status"] = resp.StatusCode
	default:
		hdr := http.Header{}
		if *xff != "" {
			hdr.Set("X-Forwarded-For", *xff)
			hdr.Set("X-Real-IP", *xff)
			hdr.Set("Forwarded", "for="+*xff)
		}
		var codes []int
		for range *n {
			url := *target + "/v1/connect?endpointId=" + *endpointID
			if *mode == "control" {
				pub, _, _ := ed25519.GenerateKey(rand.Reader)
				url = *target + "/v1/control?publicKey=" + endpoint.B64(pub)
			}
			ws, resp, err := endpoint.Dialer.Dial(url, hdr)
			switch {
			case err == nil:
				ws.Close()
				codes = append(codes, 101)
			case resp != nil:
				codes = append(codes, resp.StatusCode)
			default:
				codes = append(codes, 0)
			}
		}
		out["codes"] = codes
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}

func smoke(base string, hosts int) (int, error) {
	verified := 0
	if hosts <= 0 {
		return 0, errors.New("host count must be positive")
	}
	for i := range hosts {
		_, priv, _ := ed25519.GenerateKey(rand.Reader)
		h, err := endpoint.Register(base, priv)
		if err != nil {
			return verified, fmt.Errorf("host %d register: %w", i, err)
		}
		err = smokePair(base, h)
		h.Close()
		if err != nil {
			return verified, fmt.Errorf("host %d: %w", i, err)
		}
		verified++
	}
	return verified, nil
}

func smokePair(base string, h *endpoint.Host) error {
	client, resp, err := endpoint.Connect(base, h.ID)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("connect: HTTP %d", resp.StatusCode)
		}
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Close()
	ev, err := h.Next(10 * time.Second)
	if err != nil || ev.Type != "incoming" {
		return fmt.Errorf("no incoming event: %v", err)
	}
	accepted, resp, err := h.Accept(ev)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("accept: HTTP %d", resp.StatusCode)
		}
		return fmt.Errorf("accept: %w", err)
	}
	defer accepted.Close()
	for k := range 20 {
		typ, payload := websocket.TextMessage, []byte(fmt.Sprintf("smoke-%d", k))
		if k%2 == 1 {
			typ, payload = websocket.BinaryMessage, make([]byte, 1024+k)
			_, _ = rand.Read(payload)
		}
		from, to := client, accepted
		if k%3 == 2 {
			from, to = accepted, client
		}
		_ = from.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := from.WriteMessage(typ, payload); err != nil {
			return fmt.Errorf("write %d: %w", k, err)
		}
		_ = to.SetReadDeadline(time.Now().Add(10 * time.Second))
		gotTyp, got, err := to.ReadMessage()
		if err != nil {
			return fmt.Errorf("read %d: %w", k, err)
		}
		if gotTyp != typ || !bytes.Equal(got, payload) {
			return errors.New("payload mismatch")
		}
	}
	return nil
}
