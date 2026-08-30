# 29 — Which residual ngtcp2 HTTP/3 candidates are eligible?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8370C
**Date:** 2026-08-30
**Source:** `6a7af570` (accepted packet-bounded staging origin plus the Phase 3 disposition)
**Kernel / toolchain:** Linux `7.0.0-1012-azure`; rustc 1.98.0
**CPU:** CPU 3, selected with `taskset -c 3`
**Workload:** one explicit empty warm-up, then 125 or 250 persistent 1 KiB echo exchanges
**Purpose:** residual-candidate eligibility only; no candidate was implemented
**Otherwise idle:** no build, test, or other benchmark was run concurrently by this workflow
**Exclusions:** no post-ready system-under-test pass was discarded

## Decision rule and controls

A candidate is eligible for a separately planned experiment only if all four conditions hold:

1. its attributed event count is positive in each of three post-warm-up passes;
2. each paired 250/125 count ratio is between 1.8 and 2.2;
3. its relevant end-to-end gap exceeds unchanged-control drift; and
4. scoped observations or controlled arms associate both the event and gap with the layer the
   candidate would change.

The target was `ngnet-quic-h3`. `h3-quinn` was the designated unchanged drift control.
`ngnet-h3-quinn` was the impact control: it shares the generic `ngnet-h3` driver with the
target, but uses the Quinn adapter rather than the detached ngtcp2 endpoint/adapter. The
`ngnet-h3-quinn` to `h3-quinn` difference contains both generic-driver and Quinn-adapter work;
it does not separate them by itself.

All timing and diagnostic observations came from the same release binary:

```sh
cargo build -p ngnet-bench --example probe --release --features diagnostics
taskset -c 3 ./target/release/examples/probe ARM body 1024 COUNT timing
taskset -c 3 ./target/release/examples/probe ngnet-quic-h3 body 1024 COUNT diagnostic
```

Diagnostic mode was armed only after `PROBE-READY`. Timing mode called the same feature-gated
hooks while unarmed. The invariant test
`feature_enabled_unarmed_diagnostic_checks_allocate_nothing` separately verifies that unarmed
hooks report the default snapshot and allocate nothing
([`tests/ngnet-quic-h3-tests/tests/zero_alloc.rs:487-507`](../../../../tests/ngnet-quic-h3-tests/tests/zero_alloc.rs#L487-L507)).

## End-to-end timing and drift

The three arms were rotated through the first, second, and third position across passes. Raw
elapsed values cover only the fixed post-warm-up exchange loop.

| Count | Pass | `ngnet-quic-h3` ns | `ngnet-h3-quinn` ns | `h3-quinn` ns |
| ---: | ---: | ---: | ---: | ---: |
| 125 | 1 | 22,582,316 | 19,889,498 | 12,769,504 |
| 125 | 2 | 22,423,128 | 15,523,264 | 5,648,567 |
| 125 | 3 | 14,207,528 | 11,171,658 | 5,458,622 |
| 250 | 1 | 54,403,255 | 26,687,726 | 16,318,867 |
| 250 | 2 | 30,661,676 | 32,423,734 | 16,215,922 |
| 250 | 3 | 32,644,391 | 23,551,503 | 10,923,904 |

Per-exchange values and the unchanged-control bar are:

| Count | Arm | Passes, µs/exchange | Median | Absolute span |
| ---: | --- | --- | ---: | ---: |
| 125 | target | 180.659; 179.385; 113.660 | 179.385 | 66.998 |
| 125 | impact control | 159.116; 124.186; 89.373 | 124.186 | 69.743 |
| 125 | drift control | 102.156; 45.189; 43.669 | 45.189 | **58.487** |
| 250 | target | 217.613; 122.647; 130.578 | 130.578 | 94.966 |
| 250 | impact control | 106.751; 129.695; 94.206 | 106.751 | 35.489 |
| 250 | drift control | 65.275; 64.864; 43.696 | 64.864 | **21.580** |

The paired target-minus-impact gaps were 21.543, 55.199, and 24.287 µs at 125 exchanges
(median 24.287 µs), which is smaller than the 58.487 µs unchanged-control span. At 250 they
were 110.862, -7.048, and 36.372 µs (median 36.372 µs): the sign changed, the arm ranges
overlapped, and the target span was 94.966 µs despite the smaller 21.580 µs control span.
There is therefore no stable adapter-specific gap that clears matched drift.

The paired impact-minus-drift gaps were 56.960, 78.998, and 45.704 µs at 125 exchanges
(median 56.960 µs), also smaller than the 58.487 µs control span. The 250-exchange differences
were 41.475, 64.831, and 50.510 µs (median 50.510 µs), but no generic-driver or Quinn-handoff
counter partitions that combined gap. The host noise and missing partition prevent an
eligibility claim.

## Scoped ngtcp2 observations

The final cumulative diagnostic snapshots reconciled accepted bytes to release bytes,
produced packets to transport-only plus stream-carrying packets, and reported zero inbound
drops, zero zero-accept retries without an enabling event, and no counter overflow in every
pass.

`produce` and stream `drain` hand each successful packet buffer to the detached endpoint
queue. The send path therefore forces one owned allocation and one outbound queue handoff per
produced packet
([`crates/ngnet-quic-h3/src/pump.rs:125-153`](../../../../crates/ngnet-quic-h3/src/pump.rs#L125-L153),
[`crates/ngnet-quic-h3/src/transmit.rs:51-55`](../../../../crates/ngnet-quic-h3/src/transmit.rs#L51-L55),
[`crates/ngnet-quic-h3/src/transmit.rs:102-107`](../../../../crates/ngnet-quic-h3/src/transmit.rs#L102-L107)).
The allocation-counting invariant independently asserts this one-allocation-per-datagram
relationship
([`tests/ngnet-quic-h3-tests/tests/zero_alloc.rs:261-386`](../../../../tests/ngnet-quic-h3-tests/tests/zero_alloc.rs#L261-L386)).

| Pass | Count | Client packets | Server packets | Owned-buffer / handoff proxy | Client timer rearms | Server timer rearms | Timer-rearm proxy |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 125 | 378 | 500 | **878** | 552 | 753 | **1,305** |
| 1 | 250 | 760 | 1,002 | **1,762** | 1,100 | 1,509 | **2,609** |
| 2 | 125 | 380 | 502 | **882** | 561 | 756 | **1,317** |
| 2 | 250 | 759 | 1,003 | **1,762** | 1,096 | 1,510 | **2,606** |
| 3 | 125 | 379 | 500 | **879** | 560 | 753 | **1,313** |
| 3 | 250 | 754 | 1,003 | **1,757** | 1,239 | 1,505 | **2,744** |

| Observation | 125 counts | 250 counts | Paired 250/125 ratios | Positive and 1.8–2.2? |
| --- | --- | --- | --- | --- |
| Owned buffers / outbound handoffs | 878; 882; 879 | 1,762; 1,762; 1,757 | 2.0068; 1.9977; 1.9989 | yes |
| Timer rearms | 1,305; 1,317; 1,313 | 2,609; 2,606; 2,744 | 1.9992; 1.9787; 2.0899 | yes |

Every changed deadline replaces the boxed Tokio sleep, so `timer_rearms` is the selected
timer-allocation proxy
([`crates/ngnet-quic-h3/src/pump.rs:176-193`](../../../../crates/ngnet-quic-h3/src/pump.rs#L176-L193),
[`crates/ngnet-quic/src/endpoint/tokio.rs:145-162`](../../../../crates/ngnet-quic/src/endpoint/tokio.rs#L145-L162)).
It is not a general allocator count.

The detached inbound path copies each received datagram with `to_vec()` before queueing it
because the endpoint reuses its receive buffer
([`crates/ngnet-quic/src/endpoint/driver.rs:258-278`](../../../../crates/ngnet-quic/src/endpoint/driver.rs#L258-L278)).
The current snapshot reports queue depth, high water, drops, and the number of wakers consumed,
but not inbound deliveries or copies. `inbound_wakes` is not silently substituted for a copy
count. The required inbound-copy event count is **unavailable**.

## Socket-call observations

`strace` was used only for counts, not timing conclusions. Its output was counted after
`PROBE-READY` and stopped at `PROBE-DONE`:

```sh
strace -f -qq \
  -e trace=sendto,sendmsg,sendmmsg,recvfrom,recvmsg,recvmmsg \
  taskset -c 3 ./target/release/examples/probe ARM body 1024 COUNT timing
```

| Arm | Pass | 125 send calls | 250 send calls | Ratio | Interface |
| --- | ---: | ---: | ---: | ---: | --- |
| `ngnet-quic-h3` | 1 | 880 | 1,757 | 1.9966 | `sendto` |
| `ngnet-quic-h3` | 2 | 894 | 1,755 | 1.9631 | `sendto` |
| `ngnet-quic-h3` | 3 | 881 | 1,781 | 2.0216 | `sendto` |
| `ngnet-h3-quinn` | 1 | 575 | 1,144 | 1.9896 | `sendmsg` |
| `ngnet-h3-quinn` | 2 | 569 | 1,136 | 1.9965 | `sendmsg` |
| `ngnet-h3-quinn` | 3 | 572 | 1,136 | 1.9860 | `sendmsg` |
| `h3-quinn` | 1 | 392 | 782 | 1.9949 | `sendmsg` |
| `h3-quinn` | 2 | 393 | 783 | 1.9924 | `sendmsg` |
| `h3-quinn` | 3 | 391 | 782 | 2.0000 | `sendmsg` |

`sendmmsg` was observed exactly zero times in every listed process. That is an observed
absence of batching calls, not proof that packets were batchable. The current diagnostics do
not count consecutive queued datagrams at each socket-ready boundary, UDP GSO eligibility, or
safe ngtcp2 `MORE` coalescing opportunities. Those required opportunity observations are
**unavailable**.

Four instrumented attempts did not produce a usable post-ready observation and are not
silently represented as zero. Three initial traces retained only zero counters and
`done=0`; their first filter did not retain failure text or metadata state, so whether they
reached readiness and why they ended are unavailable. A later target pass-1/250 attempt
panicked before `PROBE-METADATA` while awaiting the warm-up response head at
`tests/ngnet-bench/src/lib.rs:1436`. Fresh replacement processes produced the table entries
above. The unusable attempts reinforce the decision not to promote a candidate from this
instrumented run.

## Availability and attribution by layer

| Layer | Available observation | Unavailable observation / limit |
| --- | --- | --- |
| Generic `ngnet-h3` | Target and impact arms share the same driver; impact-to-drift timing is recorded. | No per-pass `take_events`/`apply_events` scratch allocation count; impact-to-drift combines generic driver and Quinn adapter. |
| `ngnet-quic-h3` plus detached endpoint | Produced-packet/owned-buffer proxy, timer-rearm proxy, queue invariants, and target socket calls. | No inbound-copy count, no allocation bytes for owned datagrams or timers, and no control that isolates ngtcp2, OpenSSL, endpoint, and adapter costs from one another. |
| `ngnet-h3-quinn` | Impact-arm timing and socket calls. | No task spawn, reader/writer task, channel send/receive, or per-stream handoff count. |
| Packet protection | Packet production is counted around the whole ngtcp2/OpenSSL path. | No protect/unprotect call count or function/CPU profile. `perf stat -e cycles,instructions` was denied with `perf_event_paranoid=4`. The target-to-impact comparison also changes transport, TLS backend, endpoint, and adapter. |

## Candidate dispositions

| Candidate | Recurring count and scaling | Gap beyond drift | Layer attribution | Disposition |
| --- | --- | --- | --- | --- |
| Detached datagram recycling / ownership transfer | Outbound owned-buffer proxy: 878/882/879 → 1,762/1,762/1,757; 2.0068/1.9977/1.9989. Inbound-copy count unavailable. | No: 125 target-impact median gap 24.287 µs < 58.487 µs control drift; 250 gaps change sign and ranges overlap. | Packet ownership is endpoint/adapter-scoped, but its share of the combined ngtcp2/OpenSSL/endpoint gap is not isolated. | **Deferred / not evidenced** |
| Quinn per-stream task / channel restructuring | Required task and channel counts unavailable. | No at 125; the 56.960 µs impact-drift median is below 58.487 µs control drift. | Impact-to-drift combines generic HTTP/3 and Quinn-adapter work. | **Deferred / not evidenced** |
| Generic HTTP/3 driver scratch reuse | Required scoped allocation count unavailable. | No at 125; same combined gap and drift limit. | The two ngnet arms identify shared generic work but do not isolate its scratch allocation from their different transports/adapters. | **Deferred / not evidenced** |
| Timer reuse | Rearm proxy: 1,305/1,317/1,313 → 2,609/2,606/2,744; 1.9992/1.9787/2.0899. | No stable target-impact gap beyond drift. | Rearms are adapter-scoped, but no arm attributes the end-to-end gap specifically to boxed sleep replacement. | **Deferred / not evidenced** |
| Syscall batching / additional packet coalescing | Target sends: 880/894/881 → 1,757/1,755/1,781; all ratios 1.9631–2.0216. `sendmmsg` = 0 in every arm/pass. | No stable target-impact gap; target spans and one paired sign reversal dominate the 250 result. | Socket calls are scoped, but batchable/coalescible opportunity counts and ngtcp2 `MORE` eligibility are unavailable. | **Deferred / not evidenced** |
| Crypto backend / packet-protection path | Protect/unprotect and crypto CPU counts unavailable. | The target gap is unstable. | No crypto-matched arm; target-to-impact changes multiple layers and `perf` is unavailable. | **Deferred / not evidenced** |

## Decision

No candidate satisfies every eligibility condition. Positive linear packet, timer-rearm, and
socket-call counts are real observations, but they do not override the matched timing drift or
the missing layer attribution. Generic `ngnet-h3`, detached endpoint/`ngnet-quic-h3`, and
`ngnet-h3-quinn` task/channel work remain separate in the record; no source responsibility
was moved to make a measurement.

All six candidates are terminally deferred for this workflow. There is no promotion-pending
candidate and no residual source behavior change.
