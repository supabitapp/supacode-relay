package main

import (
	stdbytes "bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/supabitapp/supacode-relay/internal/endpoint"
)

var (
	base       = flag.String("url", "", "relay WebSocket URL")
	usersFlag  = flag.String("users", "10,50,100,250", "ascending active-user counts")
	ratesFlag  = flag.String("rates", "2,10", "events per second per agent")
	agents     = flag.Int("agents", 10, "agents per user")
	eventBytes = flag.Int("event-bytes", 1024, "application bytes per event")
	warmup     = flag.Duration("warmup", 3*time.Second, "steady-state warmup")
	duration   = flag.Duration("duration", 12*time.Second, "measurement duration")
	setupRate  = flag.Int("setup-rate", 15, "new users per second, below public admission limits")
	maxLatency = flag.Duration("stop-p99", 2*time.Second, "stop a ramp above this p99 delivery delay")
	idle       = flag.Bool("idle", false, "hold connected users without agent traffic for memory measurements")
)

type stats struct {
	id                                    int
	users, rate                           int
	start, end                            time.Time
	sent, received, bytes, late, corrupt  atomic.Int64
	creditWaitNanos, schedulingDelayNanos atomic.Int64
	bins                                  [20001]atomic.Int64
}

type state struct {
	current   atomic.Pointer[stats]
	phases    [64]atomic.Pointer[stats]
	errors    atomic.Int64
	lastError atomic.Value
	quiet     atomic.Bool
	stop      chan struct{}
	wg        sync.WaitGroup
}

type user struct {
	host         *endpoint.Host
	client, peer *websocket.Conn
}

func (s *state) failed(err error) {
	if !s.quiet.Load() {
		s.errors.Add(1)
		s.lastError.Store(err.Error())
	}
}

func openUser(s *state, index int) (*user, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	h, err := endpoint.Register(*base, key)
	if err != nil {
		return nil, err
	}
	u := &user{host: h}
	accepted := make(chan *websocket.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-h.Done:
				return
			case e, ok := <-h.Events:
				if !ok {
					return
				}
				if e.Type == "incoming" {
					ws, _, err := h.Accept(e)
					if err != nil {
						acceptErr <- err
						return
					}
					accepted <- ws
				}
			}
		}
	}()
	u.client, _, err = endpoint.Connect(*base, h.ID)
	if err != nil {
		h.Close()
		return nil, err
	}
	select {
	case u.peer = <-accepted:
	case err = <-acceptErr:
		u.client.Close()
		h.Close()
		return nil, err
	case <-time.After(10 * time.Second):
		u.client.Close()
		h.Close()
		return nil, fmt.Errorf("accept timeout")
	}
	u.client.SetReadLimit(1 << 20)
	u.peer.SetReadLimit(1 << 20)
	credits := make(chan struct{}, 16)
	permits := make(chan struct{}, 170)
	for range 170 {
		permits <- struct{}{}
	}
	s.wg.Add(4)
	go func() {
		defer s.wg.Done()
		var expected uint64
		tail := make([]byte, *eventBytes+1)
		for i := range tail {
			tail[i] = byte(((i+32)*31 + index*17) % 251)
		}
		tail[len(tail)-1] = 0xa5
		for {
			typ, bytes, err := u.client.ReadMessage()
			if err != nil {
				s.failed(err)
				return
			}
			if len(bytes) != *eventBytes+33 {
				s.failed(fmt.Errorf("incorrect payload length"))
				return
			}
			seq := binary.BigEndian.Uint64(bytes)
			id := binary.BigEndian.Uint64(bytes[8:])
			stamp := int64(binary.BigEndian.Uint64(bytes[16:]))
			owner := binary.BigEndian.Uint64(bytes[24:])
			valid := typ == websocket.BinaryMessage && seq == expected && owner == uint64(index) && stdbytes.Equal(bytes[32:], tail)
			expected++
			if id < uint64(len(s.phases)) {
				if p := s.phases[id].Load(); p != nil && stamp >= p.start.UnixNano() && stamp < p.end.UnixNano() {
					p.received.Add(1)
					p.bytes.Add(int64(len(bytes)))
					if !valid {
						p.corrupt.Add(1)
					}
					lag := time.Since(time.Unix(0, stamp))
					bin := min(len(p.bins)-1, max(0, int(lag/time.Millisecond)))
					p.bins[bin].Add(1)
					if lag > 250*time.Millisecond {
						p.late.Add(1)
					}
				}
			}
			// v2 grants session credit after roughly 43 small DATA records.
			if expected%43 == 0 {
				select {
				case credits <- struct{}{}:
				case <-s.stop:
					return
				}
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		command := time.NewTicker(time.Second)
		defer command.Stop()
		for {
			credit := false
			select {
			case <-s.stop:
				return
			case <-credits:
				credit = true
			case <-command.C:
				p := s.current.Load()
				if p == nil || index >= p.users {
					continue
				}
			}
			_ = u.client.SetWriteDeadline(time.Now().Add(3 * time.Second))
			message := make([]byte, 64)
			if credit {
				message[0] = 1
			}
			if err := u.client.WriteMessage(websocket.BinaryMessage, message); err != nil {
				s.failed(err)
				return
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		for {
			_, message, err := u.peer.ReadMessage()
			if err != nil {
				s.failed(err)
				return
			}
			if len(message) == 64 && message[0] == 1 {
				for range 43 {
					select {
					case permits <- struct{}{}:
					case <-s.stop:
						return
					}
				}
			}
		}
	}()
	go func() {
		defer s.wg.Done()
		data := make([]byte, *eventBytes+33)
		for i := 32; i < len(data); i++ {
			data[i] = byte((i*31 + index*17) % 251)
		}
		data[len(data)-1] = 0xa5
		binary.BigEndian.PutUint64(data[24:], uint64(index))
		var seq uint64
		last := 0
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			var scheduled time.Time
			select {
			case <-s.stop:
				return
			case scheduled = <-ticker.C:
			}
			p := s.current.Load()
			if p == nil || index >= p.users {
				continue
			}
			if last != p.id {
				last = p.id
				interval := time.Second / time.Duration(p.rate**agents)
				// Different schedules avoid making all users emit in one burst.
				select {
				case <-s.stop:
					return
				case <-time.After(time.Duration(index%97) * interval / 97):
				}
				ticker.Reset(interval)
			}
			creditStart := time.Now()
			select {
			case <-permits:
			case <-s.stop:
				return
			}
			stamp := time.Now().UnixNano()
			binary.BigEndian.PutUint64(data, seq)
			binary.BigEndian.PutUint64(data[8:], uint64(p.id))
			binary.BigEndian.PutUint64(data[16:], uint64(stamp))
			_ = u.peer.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if err := u.peer.WriteMessage(websocket.BinaryMessage, data); err != nil {
				s.failed(err)
				return
			}
			seq++
			if stamp >= p.start.UnixNano() && stamp < p.end.UnixNano() {
				p.sent.Add(1)
				p.creditWaitNanos.Add(time.Since(creditStart).Nanoseconds())
				p.schedulingDelayNanos.Add(max(0, creditStart.Sub(scheduled).Nanoseconds()))
			}
		}
	}()
	return u, nil
}

func ints(s string) []int {
	var result []int
	for _, v := range strings.Split(s, ",") {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			panic("invalid integer list")
		}
		result = append(result, n)
	}
	return result
}

func quantile(s *stats, q float64) int {
	target := int64(float64(s.received.Load()) * q)
	var count int64
	for i := range s.bins {
		count += s.bins[i].Load()
		if count >= target {
			return i
		}
	}
	return 20000
}

func cpuSeconds() float64 {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func main() {
	flag.Parse()
	if *base == "" || *eventBytes < 32 {
		panic("url and event-bytes required")
	}
	s := &state{stop: make(chan struct{})}
	var us []*user
	var stopOnce sync.Once
	cleanup := func() {
		stopOnce.Do(func() {
			s.quiet.Store(true)
			s.current.Store(nil)
			close(s.stop)
			for _, u := range us {
				u.client.Close()
				u.peer.Close()
				u.host.Close()
			}
			s.wg.Wait()
		})
	}
	defer cleanup()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-interrupt; cleanup(); os.Exit(130) }()
	fmt.Fprintf(os.Stderr, "driver pid=%d cpus=%d go=%s agents=%d eventBytes=%d\n", os.Getpid(), runtime.NumCPU(), runtime.Version(), *agents, *eventBytes)
	encoder := json.NewEncoder(os.Stdout)
	id := 1
	for _, count := range ints(*usersFlag) {
		if count < len(us) {
			panic("counts must ascend")
		}
		pace := time.NewTicker(time.Second / time.Duration(*setupRate))
		var setup sync.WaitGroup
		newUsers := make([]*user, count-len(us))
		var setupMu sync.Mutex
		var setupErr error
		previous := len(us)
		for i := range newUsers {
			<-pace.C
			setup.Add(1)
			go func(index int) {
				defer setup.Done()
				u, err := openUser(s, previous+index)
				setupMu.Lock()
				defer setupMu.Unlock()
				if err != nil {
					setupErr = err
				} else {
					newUsers[index] = u
				}
			}(i)
		}
		pace.Stop()
		setup.Wait()
		for _, u := range newUsers {
			if u != nil {
				us = append(us, u)
			}
		}
		if setupErr != nil {
			fmt.Fprintln(os.Stderr, "setup failed:", setupErr)
			cleanup()
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "ready users=%d\n", len(us))
		if *idle {
			start := time.Now()
			_ = encoder.Encode(map[string]any{"kind": "idle-start", "users": count, "start": start.UTC().Format(time.RFC3339Nano), "durationSec": duration.Seconds()})
			time.Sleep(*duration)
			_ = encoder.Encode(map[string]any{"kind": "idle-end", "users": count, "end": time.Now().UTC().Format(time.RFC3339Nano), "errors": s.errors.Load()})
			if s.errors.Load() != 0 {
				cleanup()
				os.Exit(1)
			}
			continue
		}
		for _, rate := range ints(*ratesFlag) {
			p := &stats{id: id, users: count, rate: rate, start: time.Now().Add(*warmup)}
			p.end = p.start.Add(*duration)
			s.phases[id].Store(p)
			id++
			errors := s.errors.Load()
			s.current.Store(p)
			fmt.Fprintf(os.Stderr, "start users=%d agents=%d eventRate=%d expected=%.2fMiB/s measureAt=%s\n", count, count**agents, rate, float64(count**agents*rate*(*eventBytes+33))/(1<<20), p.start.UTC().Format(time.RFC3339Nano))
			time.Sleep(time.Until(p.start))
			cpuStart := cpuSeconds()
			time.Sleep(time.Until(p.end))
			driverCPU := 100 * (cpuSeconds() - cpuStart) / duration.Seconds()
			s.current.Store(nil)
			time.Sleep(2 * time.Second)
			errCount := s.errors.Load() - errors
			p99 := quantile(p, .99)
			result := map[string]any{"users": count, "agents": count * *agents, "eventsPerAgentSec": rate, "applicationEventBytes": *eventBytes, "opaqueRecordBytes": *eventBytes + 33, "start": p.start.UTC().Format(time.RFC3339Nano), "end": p.end.UTC().Format(time.RFC3339Nano), "durationSec": duration.Seconds(), "expectedMessages": count * *agents * rate * int(duration.Seconds()), "sent": p.sent.Load(), "offeredRateAttainment": float64(p.sent.Load()) / float64(count**agents*rate*int(duration.Seconds())), "received": p.received.Load(), "messagesPerSec": float64(p.received.Load()) / duration.Seconds(), "MiBPerSec": float64(p.bytes.Load()) / duration.Seconds() / (1 << 20), "deliveryP50Ms": quantile(p, .5), "deliveryP95Ms": quantile(p, .95), "deliveryP99Ms": p99, "over250ms": p.late.Load(), "errors": errCount, "corrupt": p.corrupt.Load()}
			if last := s.lastError.Load(); last != nil {

				result["lastError"] = last
			}
			result["driverCPUPercentOneCore"] = driverCPU
			result["driverCPUs"] = runtime.NumCPU()
			result["meanCreditAndWriteWaitMs"] = float64(p.creditWaitNanos.Load()) / float64(max(1, p.sent.Load())) / 1e6
			result["meanProducerSchedulingDelayMs"] = float64(p.schedulingDelayNanos.Load()) / float64(max(1, p.sent.Load())) / 1e6
			result["latencyStartsAfterCredit"] = true
			result["flowControlled"] = true
			result["fullPayloadValidated"] = true
			_ = encoder.Encode(result)
			fmt.Fprintf(os.Stderr, "done users=%d rate=%d messages=%d p50=%dms p99=%dms errors=%d corrupt=%d\n", count, rate, p.received.Load(), quantile(p, .5), p99, errCount, p.corrupt.Load())
			if errCount > 0 || p.corrupt.Load() > 0 || time.Duration(p99)*time.Millisecond > *maxLatency {
				fmt.Fprintln(os.Stderr, "stopping ramp at latency/error threshold")
				if errCount > 0 || p.corrupt.Load() > 0 {
					cleanup()
					os.Exit(1)
				}
				return
			}
		}
	}
}
