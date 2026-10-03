package e2e

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

func jsonRPCMessages() []message {
	var out []message
	add := func(v any) {
		b, _ := json.Marshal(v)
		out = append(out, message{websocket.TextMessage, b})
	}
	add(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"capabilities": map[string]any{}}})
	add([]any{map[string]any{"jsonrpc": "2.0", "id": 2, "method": "sum", "params": []int{1, 2, 3}}, map[string]any{"jsonrpc": "2.0", "method": "notify"}})
	add(map[string]any{"jsonrpc": "2.0", "id": 3, "error": map[string]any{"code": -32601, "message": "Method not found ✗"}})
	add(map[string]any{"jsonrpc": "2.0", "method": "hello", "params": map[string]any{"type": "e2ee_hello"}})
	out = append(out, message{websocket.TextMessage, []byte("{\n  \"jsonrpc\" : \"2.0\",\t\"id\":null }  ")})
	return out
}

func varint(b []byte, v uint64) []byte {
	return binary.AppendUvarint(b, v)
}

func recordMessages() []message {
	var out []message
	for i := range 20 {
		body := varint(nil, 1<<3|0)
		body = varint(body, uint64(i)*1_000_003)
		blob := randomBytes(i * 97)
		body = varint(body, 2<<3|2)
		body = varint(body, uint64(len(blob)))
		body = append(body, blob...)
		frame := binary.BigEndian.AppendUint32([]byte{0xCA, 0xFE}, uint32(len(body)))
		out = append(out, message{websocket.BinaryMessage, append(frame, body...)})
	}
	return out
}

func TestUnrelatedPayloadFormats(t *testing.T) {
	r := startRelay(t)
	h := register(t, r)
	formats := map[string][]message{"json-rpc": jsonRPCMessages(), "binary-records": recordMessages()}
	type pairConns struct{ client, host *websocket.Conn }
	pairs := map[string]pairConns{}
	for name := range formats {
		c, s, _ := pairUp(t, r, h)
		pairs[name] = pairConns{c, s}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for name, msgs := range formats {
		p := pairs[name]
		for _, dir := range [][2]*websocket.Conn{{p.client, p.host}, {p.host, p.client}} {
			wg.Add(2)
			go func() {
				defer wg.Done()
				for _, m := range msgs {
					if err := dir[0].WriteMessage(m.typ, m.data); err != nil {
						errs <- err
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				for i, want := range msgs {
					typ, data, err := dir[1].ReadMessage()
					if err != nil {
						errs <- err
						return
					}
					if typ != want.typ || !bytes.Equal(data, want.data) {
						errs <- fmt.Errorf("%s message %d altered", name, i)
						return
					}
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
