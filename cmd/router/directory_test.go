package main

import (
	"io"
	"testing"
	"time"
)

func TestSyncWaitersAreReleasedWhenRoundEnds(t *testing.T) {
	st := &stream{out: make(chan []byte, 1), waiters: map[uint64]chan struct{}{}}
	for range 1000 {
		ch, cancel := st.sync(1)
		if ch == nil || len(st.waiters) != 1 {
			t.Fatal("missing sync waiter")
		}
		<-st.out
		cancel()
		if len(st.waiters) != 0 {
			t.Fatal("cancelled round retained a waiter")
		}
	}
	ch, cancel := st.sync(1)
	defer cancel()
	<-st.out
	st.ack(st.seq - 1)
	select {
	case <-ch:
		t.Fatal("stale ack completed a newer round")
	default:
	}
	st.ack(st.seq)
	<-ch
	if len(st.waiters) != 0 {
		t.Fatal("ack retained a waiter")
	}
}

func TestSyncCancellationAndAckCanRace(t *testing.T) {
	st := &stream{out: make(chan []byte, 1), waiters: map[uint64]chan struct{}{}}
	for range 1000 {
		_, cancel := st.sync(1)
		<-st.out
		seq := st.seq
		done := make(chan struct{})
		go func() { st.ack(seq); close(done) }()
		cancel()
		<-done
		if len(st.waiters) != 0 {
			t.Fatal("finished round retained a waiter")
		}
	}
}

func TestTimedOutSyncRoundsReleaseWaiters(t *testing.T) {
	rt := newRouter(config{refreshTimeout: time.Millisecond}, io.Discard)
	st := &stream{out: make(chan []byte, 16), waiters: map[uint64]chan struct{}{}}
	for range 10 {
		rt.syncRound([]*stream{st})
		<-st.out
		if len(st.waiters) != 0 {
			t.Fatal("timed-out round retained a waiter")
		}
	}
	if rt.roundTimeouts.Load() != 10 {
		t.Fatal("round timeout was not counted")
	}
}
