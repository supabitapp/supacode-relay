package e2e

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

func payloadSet(max int) []message {
	text := func(s string) message { return message{websocket.TextMessage, []byte(s)} }
	bin := func(b []byte) message { return message{websocket.BinaryMessage, b} }
	msgs := []message{
		text("hello"),
		text(""),
		bin(nil),
		bin([]byte{0}),
		bin([]byte{0xff, 0xfe, 0x00, 0x80, 0xc3}),
		text("ünïcødé ✓ 🚀"),
		text(`{"type":"hello","version":1}`),
		text(`{"type":"e2ee_hello","publicKey":"AAAA","nonce":"BBBB"}`),
		text(`{"type":"challenge","nonce":"spoofed"}`),
		text(`{"type":"authenticate","signature":"spoofed"}`),
		text(`{"type":"registered","endpointId":"00"}`),
		text(`{"type":"incoming","connectionId":"x","token":"y"}`),
		text(`{"type":"closed","connectionId":"x"}`),
		bin([]byte(`{"type":"e2ee_hello"}`)),
	}
	for _, n := range []int{1, 2, 125, 126, 127, 65535, 65536, max - 1, max} {
		msgs = append(msgs, bin(randomBytes(n)))
	}
	for i := range 400 {
		if i%2 == 0 {
			msgs = append(msgs, text(fmt.Sprintf("seq-%06d", i)))
		} else {
			b := make([]byte, 8+i)
			binary.BigEndian.PutUint64(b, uint64(i))
			msgs = append(msgs, bin(b))
		}
	}
	return msgs
}

func TestForwardingPreservesMessages(t *testing.T) {
	const max = 256 << 10
	r := startRelay(t, fmt.Sprintf("RELAY_MAX_MESSAGE_BYTES=%d", max), "RELAY_MAX_QUEUE_BYTES=8388608", "RELAY_MAX_QUEUE_MESSAGES=1024")
	h := register(t, r)
	client, host, _ := pairUp(t, r, h)
	msgs := payloadSet(max)

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	pump := func(from, to *websocket.Conn) {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, m := range msgs {
				if err := from.WriteMessage(m.typ, m.data); err != nil {
					errs <- err
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i, want := range msgs {
				typ, data, err := to.ReadMessage()
				if err != nil {
					errs <- err
					return
				}
				if typ != want.typ || !bytes.Equal(data, want.data) {
					errs <- fmt.Errorf("message %d mismatch: got type=%d len=%d want %v", i, typ, len(data), want)
					return
				}
			}
		}()
	}
	pump(client, host)
	pump(host, client)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	m := r.metrics()
	if m["forwardedMessages"] != float64(2*len(msgs)) {
		t.Fatalf("forwardedMessages = %v, want %d", m["forwardedMessages"], 2*len(msgs))
	}
}

func TestClientReceivesNoRelayRecords(t *testing.T) {
	r := startRelay(t)
	h := register(t, r)
	client, host, _ := pairUp(t, r, h)
	first := message{websocket.BinaryMessage, []byte("first-from-host")}
	send(t, host, first)
	expectMessage(t, client, first)
}
