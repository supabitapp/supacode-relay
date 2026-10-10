package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/directory"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
	node "github.com/supabitapp/supacode-relay/internal/relay"
)

func TestSpikeAcceptHeadroom(t *testing.T) {
	for _, reserve := range []bool{false, true} {
		t.Run(fmt.Sprint(reserve), func(t *testing.T) {
			cfg, err := node.LoadConfig(func(k string) string {
				switch k {
				case "RELAY_NODE_ID":
					return "a"
				case "RELAY_PAIR_TIMEOUT_MS":
					return "2000"
				case "RELAY_ADMISSION_RATE":
					return "100000"
				}
				return ""
			})
			if err != nil {
				t.Fatal(err)
			}
			server := node.New(cfg)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			go server.Serve(ln)
			t.Cleanup(server.Shutdown)
			rt, public, _ := spikeRouter(t, false, func(r *router) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
					if reserve && q.URL.Path == "/v1/connect" {
						used, _ := r.gate.Stats()
						if used >= 6 {
							w.WriteHeader(503)
							return
						}
					}
					r.ServeHTTP(w, q)
				})
			})
			addNode(t, rt, "a", "http://"+ln.Addr().String())
			_, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			h, err := endpoint.Register("ws"+public.URL[len("http"):], key)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			rt.table.Put("a", rt.table.Nodes()[0].Session, directory.Registration{EndpointID: h.ID, RegistrationID: "host", Version: 1})
			clients := []*websocket.Conn{}
			defer func() {
				for _, ws := range clients {
					ws.Close()
				}
			}()
			rejected := 0
			for range 7 {
				ws, resp, err := endpoint.Connect(h.Base, h.ID)
				if err != nil {
					if statusOf(resp) != 503 {
						t.Fatal(err)
					}
					rejected++
					continue
				}
				clients = append(clients, ws)
			}
			ev, err := h.Next(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			accepted, resp, err := h.Accept(ev)
			code := statusOf(resp)
			if accepted != nil {
				defer accepted.Close()
			}
			t.Logf("reserve=%v gate_limit=8 pending_clients=%d rejected_connects=%d accept_status=%d", reserve, len(clients), rejected, code)
			if reserve && (err != nil || code != 101) {
				t.Fatal("reserved accept failed")
			}
			if !reserve && (err == nil || code != 503) {
				t.Fatal("baseline headroom exhaustion did not reproduce")
			}
			if reserve {
				clients[0].WriteMessage(websocket.TextMessage, []byte("progress"))
				accepted.SetReadDeadline(time.Now().Add(time.Second))
				_, payload, err := accepted.ReadMessage()
				if err != nil || string(payload) != "progress" {
					t.Fatal("pair made no progress", err)
				}
			}
		})
	}
}
