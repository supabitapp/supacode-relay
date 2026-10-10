# Fuzz and memory experiments

Keep the queue, admission, and proxy-identity fuzz targets for generated invariant checks. Run memory churn as a separate opt-in test, with heap profiles available for investigation. Limit the socket-based lifecycle fuzzer by execution count on developer machines. The Base64 target costs little to maintain, but tests a small wrapper around the standard library.

The measurements below describe local experiments against the relay source at [b41df22](https://github.com/supabitapp/supacode-relay/tree/b41df22e5a2b68c23c3c1c39c754bde715db56d5), with the adjacent test harnesses. No production relay was contacted. [Retained results](fuzz-testing-results.json) contain commands, execution summaries, memory samples, and synthetic-fault check results.

## Fuzz measurements

Measurements on 2026-10-10 used Go 1.27.1 on a Mac17,16 running darwin/arm64 with 18 logical CPUs. Each initial campaign requested 30 seconds with two workers. The memory experiments shared this machine during part of the campaigns, so execution counts are workload observations, not portable throughput estimates.

| Target | Reported executions | New interesting inputs | Result | Assessment |
| --- | ---: | ---: | --- | --- |
| [Control queue](../internal/relay/queue_fuzz_test.go) | 1,407,992 | 97 | Passed | Useful for FIFO order, terminal behavior, and bytes held by writes in flight |
| [Admission gate](../internal/admission/admission_fuzz_test.go) | 1,421,011 | 131 | Passed | Useful for generated acquisition and release sequences against a slot model |
| [Client IP](../internal/admission/admission_fuzz_test.go) | 1,111,622 | 124 | Passed | Useful for proxy trust and repeated-header invariants |
| [Base64](../internal/relay/control_fuzz_test.go) | 289,887 | 34 | Passed | Lower incremental value because the relay wrapper has little custom logic |
| [Pair lifecycle](../internal/relay/lifecycle_fuzz_test.go) | 5,360 at the last progress report | 22 at that report | Host address error | Useful scenarios, but sustained real-socket churn adds operating-system limits |

The socket campaign ended with `connect: can't assign requested address`. Its minimized input contained `[]byte("0")` for both arguments. Ten direct replays passed. A subsequent single-worker campaign limited to 1,000 executions passed in 13.6 seconds. Sustained connection churn exhausting host address resources is the supported explanation; the failure did not identify an input-specific relay defect.

The lifecycle target runs at most four self-contained scenarios per input. It varies pending cleanup, forwarding, invalid and reused tokens, host replacement, interrupted messages, and rejected authentication. It does not generate arbitrary overlaps among many active pairs, shutdown, or timer expiration. Those require a more controlled transport and scheduler harness.

The other targets bound input lengths and operation counts. Queue and gate generators respect internal API ownership, including one release per successful acquisition or dequeued frame. Their models check retained resources after each operation. Empty or oversized inputs may perform little work, so execution counts do not equal distinct relay behaviors.

Go retains inputs that expand instrumented coverage. Those counts include harness and dependency paths and are not production coverage percentages. Ordinary tests plus fixed fuzz seeds raised internal relay statement coverage from 75.0% to 79.9%; admission coverage remained 86.0%. Active campaign corpus coverage was not measured. See [Go fuzzing](https://go.dev/doc/security/fuzz/) for corpus behavior.

## Memory measurements

The [memory harness](../internal/relay/memory_spike_test.go) starts a separate test process serving the relay handler over loopback. Client allocations stay in the parent process. Each cycle releases its connections before the next cycle begins. Samples follow two explicit garbage collections to reduce noise from reusable `sync.Pool` buffers. The measured heap includes the relay, Go runtime, and test server.

Each run warms up with 24 cycles, then performs six batches of 1,024 cycles. Three ordinary runs and one race-enabled run passed. All sampled host, pair, socket, control, and admission counts were zero after cleanup. Every run retained its baseline count of five goroutines.

| Run | Measured cycles | Final post-GC heap growth | Final heap-object growth | Test time |
| --- | ---: | ---: | ---: | ---: |
| Ordinary 1 | 6,144 | 288,416 bytes | 1,006 | 8.13 s |
| Ordinary 2 | 6,144 | 258,928 bytes | 907 | 8.14 s |
| Ordinary 3 | 6,144 | 307,896 bytes | 1,101 | 8.19 s |
| Race detector | 6,144 | 324,712 bytes | 1,223 | 19.48 s |

The test permits up to 2 MiB of post-GC growth and four additional goroutines. These are coarse regression budgets, not a proof that all leaks are absent. Small retained allocations, other workloads, and longer runs can exceed what this experiment establishes. Serial churn also leaves concurrent peak-memory behavior to the existing streaming and stalled-reader tests.

An additional profiling run recorded every allocation and saved baseline and final heap profiles. It passed with 287,120 bytes of final heap growth and no goroutine growth. The profile difference attributed its largest positive allocation change to `runtime.mallocgc`; smaller changes appeared at relay timer and synchronization sites. Allocation sites alone do not establish why objects remain reachable, so this experiment does not classify the observed growth as harmless or as a leak.

Comparing retained heap after cleanup gives a more useful memory signal than measuring the fuzzer process or cumulative allocated bytes. `TotalAlloc` grows whenever objects are allocated, including objects later freed. Go can also reserve memory for reuse. [Runtime memory statistics](https://pkg.go.dev/runtime#MemStats) and [heap profiles](https://pkg.go.dev/runtime/pprof) describe these distinctions.

## Harness sensitivity

Seven separate temporary source copies contained deliberate regressions. The corresponding fixed fuzz seeds or memory test failed in every copy. These checks establish that the assertions recognize those specific failures; they are not seven discovered production defects.

The five logical regressions were checked by replaying fixed fuzz seeds. The heap and goroutine checks used six batches of 96 cycles in their isolated processes, following the same warmup and cleanup rules as ordinary memory runs.

| Temporary regression | Changed production function | Detecting check |
| --- | --- | --- |
| Omit the byte decrement when a write finishes | `queue.release` | Queue model disagreed with retained bytes |
| Omit the global slot decrement | `Gate.Release` | Gate statistics disagreed with its ownership model |
| Honor forwarded headers from an untrusted peer | `ClientIP` | Proxy trust assertion failed |
| Remove canonical re-encoding validation | `decodeB64` | Embedded-newline and canonical-encoding assertions failed |
| Keep the client slot after final pair cleanup | `pair.releaseSlot` | Lifecycle idle check timed out |
| Retain a 64 KiB buffer per completed pair | `pair.releaseSlot` | Memory churn exceeded its heap budget |
| Start a goroutine that never exits per completed pair | `pair.releaseSlot` | Memory churn exceeded its goroutine budget |

The temporary copies stayed outside the repository. Production code and CI configuration are unchanged by these experiments.

## Run the targets

`make test` replays the fixed fuzz seeds with the race detector. Active fuzzing requires a separate command. Each command below selects exactly one package and target, bounds execution, and limits worker count:

```sh
go test ./internal/relay -run '^$' -fuzz '^FuzzControlQueue$' -fuzztime=30s -parallel=2 -fuzzminimizetime=5s -timeout=2m
go test ./internal/admission -run '^$' -fuzz '^FuzzAdmissionGate$' -fuzztime=30s -parallel=2 -fuzzminimizetime=5s -timeout=2m
go test ./internal/admission -run '^$' -fuzz '^FuzzClientIP$' -fuzztime=30s -parallel=2 -fuzzminimizetime=5s -timeout=2m
go test ./internal/relay -run '^$' -fuzz '^FuzzDecodeB64$' -fuzztime=30s -parallel=2 -fuzzminimizetime=5s -timeout=2m
go test ./internal/relay -run '^$' -fuzz '^FuzzPairLifecycle$' -fuzztime=1000x -parallel=1 -fuzzminimizetime=5s -timeout=2m
```

Keep socket runs separate from other connection-heavy experiments. The execution limit reduces address pressure but does not guarantee available host resources. A real relay failure needs a reproducible input; preserve its `testdata/fuzz` entry with the fix. Go stores coverage-expanding inputs separately in its fuzz cache, so retaining that cache helps future campaigns continue exploring.

Memory churn is skipped unless `RELAY_MEMORY_SPIKE_CYCLES` is set. The value specifies cycles per batch and accepts integers from 1 through 10,000. These commands reproduce the ordinary and race-enabled measurements:

```sh
RELAY_MEMORY_SPIKE_CYCLES=1024 go test ./internal/relay -run '^TestConnectionChurnMemory$' -count=3 -v
RELAY_MEMORY_SPIKE_CYCLES=1024 go test -race ./internal/relay -run '^TestConnectionChurnMemory$' -count=1 -v
```

For diagnostic profiles, set `RELAY_MEMORY_SPIKE_PROFILE_DIR` to an output directory outside the repository. This enables allocation-by-allocation profiling in the child process and writes `baseline.pprof` and `final.pprof`. Test cleanup saves the final profile even when a memory assertion fails. A subsequent profiling run overwrites those two files. The extra instrumentation changes runtime cost and should be evaluated separately from ordinary runs.

```sh
RELAY_MEMORY_SPIKE_CYCLES=1024 RELAY_MEMORY_SPIKE_PROFILE_DIR=/tmp/supacode-relay-fuzz-spikes.ZD9R0J/profiles go test ./internal/relay -run '^TestConnectionChurnMemory$' -count=1 -v
go tool pprof -top -diff_base=/tmp/supacode-relay-fuzz-spikes.ZD9R0J/profiles/baseline.pprof /tmp/supacode-relay-fuzz-spikes.ZD9R0J/profiles/final.pprof
```

The diagnostic HTTP routes exist only in the isolated test process. Profiling and memory sampling add no routes to the relay executable.
