# 01 — Hyperium H3 and ngnet H3 over the same ngtcp2 transport

> **Superseded where it says the defect is unlocated, and where it says the large-body probes
> cannot be run.** Both have since changed: the defect was root-caused and fixed, and the
> 16 KiB and 1 MiB probes have been run on both arms. See
> [`02-h3-ngnet-quic-fin-fix`](02-h3-ngnet-quic-fin-fix.md).
>
> **Nothing about the measurement is superseded.** No performance conclusion was drawn here
> and none has been drawn since. The timings below remain what this host produced on a day it
> could not hold still, and they are not restated or revised anywhere.

> **New machine.** This is the first run on [`epyc-7763-azure`](README.md), which is a different
> machine from `xeon-8370c-azure` — AMD rather than Intel, 4 vCPU rather than that host's count,
> different kernel and generation. **Every absolute timing and every drift threshold recorded on
> that host is historical only and is not comparable with anything below.** Nothing here is
> tabulated beside it.

**Machine:** [`epyc-7763-azure`](README.md)
**Date:** 2026-09-02
**Commit:** `6119972`
**Cases:** `quic_stack_h3_serial_latency`, `quic_stack_h3_body_throughput`, plus pinned
interleaved `probe` passes
**Command:** `taskset -c 3 ./target/release/examples/probe <arm> body <size> 200`
**Repetitions:** 5 interleaved passes per arm at each of two payloads
**Controls:** `h3-qmux-duplex` (unchanged arm) carried at the start and end of the session
**Exclusions:** none. The rule, fixed before any number was seen, was that no pass would be
excluded and that anomalously slow passes would be reported with the rest

## What was being asked

With the QUIC transport held fixed — the same `ngnet-quic` endpoints, credentials, ALPN, server
name and configuration on both sides — does replacing the HTTP/3 implementation and its adapter
change round-trip cost? The two arms are `ngnet-h3` + `ngnet-quic-h3` and hyperium `h3` +
`h3-ngnet-quic`. This is the first time that question could be asked at all, because until the
new adapter existed hyperium's HTTP/3 could not reach this transport.

## The pre-registered rule, and its verdict

Recorded before the first measured pass. A difference would be claimed **only if** all four
held: five interleaved passes per arm; non-overlapping per-pass ranges; a gap larger than the
session's control drift; and a consistent direction in every pass pair. A further condition
required the host's load average to be below 1.0.

**Three of the five conditions failed. No difference is claimed.**

## Results

Per-exchange means, in microseconds, from 200 exchanges per pass. Lower is better. Reported as
measured; nothing excluded.

### Empty body

| Pass | `ngnet-quic-h3` | `h3-ngnet-quic` | Faster |
| --- | ---: | ---: | --- |
| 1 | 187.4 | 137.8 | hyperium |
| 2 | 144.0 | 156.3 | native |
| 3 | 138.5 | 202.8 | native |
| 4 | 145.6 | 166.5 | native |
| 5 | 151.0 | 228.1 | native |
| **Range** | **138.5 – 187.4** | **137.8 – 228.1** | overlapping |
| **Median** | **145.6** | **166.5** | — |

### 1 KiB body

| Pass | `ngnet-quic-h3` | `h3-ngnet-quic` | Faster |
| --- | ---: | ---: | --- |
| 1 | 153.6 | 796.5 | native |
| 2 | 224.4 | 329.8 | native |
| 3 | 187.5 | 259.7 | native |
| 4 | 229.3 | 252.8 | native |
| 5 | 356.0 | **did not complete** | — |
| **Range** | **153.6 – 356.0** | **252.8 – 796.5** | overlapping |

Pass 5 of the hyperium arm did not produce a number. It is reported as a failure rather than
retried; see "The adapter defect" below.

## Drift controls in the same session

| Control arm | Start | End | Movement |
| --- | ---: | ---: | ---: |
| `h3-qmux-duplex`, 200 x 1 KiB | 18.52 ms | 4.39 ms | **4.2x** |

An unchanged arm moved by a factor of 4.2 within the session. The gap between the two arms'
medians at the empty-body size is about 1.14x. **The control moved roughly thirty times further
than the effect being looked for.** This alone disqualifies any comparative reading of the
tables above.

The host explains it: the machine was never idle. A Kubernetes control plane and two unrelated
processes at ~45% CPU each ran throughout, with load average between 1.9 and 5.4 against a
pre-registered requirement of < 1.0.

## The adapter defect this run found

The more useful outcome. Chasing the missing pass 5 produced a reproducible, and previously
unknown, liveness defect in `h3-ngnet-quic`.

| Workload | Arm | Runs | Failures |
| --- | --- | ---: | ---: |
| 200 x 1 KiB exchanges | `h3-ngnet-quic` | 10 | 6 |
| 200 x 1 KiB exchanges | `ngnet-quic-h3` (native) | 10 | **0** |
| 200 x 1 KiB exchanges, after two fixes | `h3-ngnet-quic` | 25 | 11 |

Every failure took 30.06–30.11 s — the connection's idle timeout exactly — against 109–215 ms
for a success. The failing exchange index is random (3, 8, 11, 13, 99, 113, 142, 186).

Captured at the stall: the request was delivered **and acknowledged** (zero retained bytes on the
client), no datagrams were dropped on either side, the server had observed the stream open and
returned to accepting with an empty queue, the client was parked reading the response, and both
sides had their expiry timer armed.

**This is not the known S9 stall.** The attribution rule was fixed before measuring: a failure
could be blamed on `ngnet-quic-h3`'s unresolved large-body stall only if reproduced on both
stacks. It was not — the native arm passed 10 out of 10 on the identical workload — and S9 is a
defect in `ngnet-h3`'s driver, which the new adapter does not use.

Two genuine defects were found and fixed while investigating, both of which lowered the failure
rate without removing it: single-slot waker registries where two tasks legitimately wait, and an
expiry timer armed before the caller's write rather than after it. Details in
[`../../../h3-ngnet-quic/pending-work.md`](../../../h3-ngnet-quic/pending-work.md).

## Held constant between the arms

Verified by construction — both fixtures call the same helpers:

| Variable | Held equal |
| --- | --- |
| Transport | `ngnet-quic`, same `ngnet-quic-h3-tests` endpoint helpers |
| Credentials, ALPN, server name | `Credentials::generate()`, `b"h3"`, `"localhost"` |
| Transport config | `Config::new().handshake_timeout(5s)`, `build_detachable()`, same entropy seed |
| QPACK dynamic table | 0 on both — the native arm uses a purpose-built matched fixture, because `ngnet-h3` defaults to 4 KiB and hyperium 0.0.8 has none |
| Max field section size | set explicitly on both, on both client and server |
| GREASE | disabled on both hyperium sides |
| Spawned tasks per endpoint | 1 endpoint driver + 1 HTTP/3 driver |
| Runtime | one `current_thread` runtime per arm |
| Request head | byte-identical: POST, `https`, same authority, path, `content-type`, `x-bench` |
| Response | status 200, `application/octet-stream`, echoed body |
| Body | identical payload; one copy into a contiguous buffer on each server |
| Drain | every byte read on both |
| Warm-up | one empty exchange inside `establish()`, outside every measured region |
| Pinning | `taskset -c 3` for both |
| Timed region | exactly one exchange plus its full drain |
| Criterion sampling | defaults, unmodified, both arms |

## Disclosed asymmetries

These could not be removed and are part of what "the adapter difference" means here:

1. **Where HTTP/3 driving happens relative to the timed region.** `ngnet-h3` advances its state
   machine in its spawned driver task; hyperium advances a request stream from the task polling
   it, which is the one inside the measured closure. UDP I/O is symmetric — both use the shared
   endpoint driver — but this is not.
2. **Two independently written QUIC pumps**, one per adapter.
3. **Hyperium clones its request handle per exchange**; the native handle does not need to.
4. **Hyperium has more await points inside the timed region**: `send_request`, `send_data`,
   `finish`, `recv_response`, `recv_data`, against the native arm's single `send_request` plus
   drain.
5. **Body chunking granularity** may differ between the two HTTP/3 layers for an identical
   payload; neither exposes a control that would equalise it.

## Larger payloads

Not measured. The committed body-throughput target stays at 1 KiB, matching the restriction the
existing `quic_stack_body_throughput` already applies to this transport because of the S9 stall.
Supervised 16 KiB and 1 MiB probes on both arms were planned, and were **not run**: the adapter's
own defect makes a repeated large-body probe against it meaningless until it is fixed. So the
question of how S9 affects payload coverage for the *new* arm remains open, and is recorded as
open rather than answered by assumption. *(Since run: see
[`02-h3-ngnet-quic-fin-fix`](02-h3-ngnet-quic-fin-fix.md).)*

## What this establishes

- Hyperium `h3` runs over `ngnet-quic` end to end: handshake, control streams, request, response,
  and byte-exact bodies up to 96 KiB in a single exchange. The correctness suite passes.
- Both benchmark arms build, run and complete a Criterion verification pass.
- The two arms are variable-matched in every respect listed above, so the apparatus is ready to
  answer the comparison question on a machine that can hold still.
- `h3-ngnet-quic` has a reproducible liveness defect under repeated exchanges, which is its own
  and not the inherited one, with the evidence above.
- This host, as configured, cannot measure a performance difference of the size in question: an
  unchanged control arm moved 4.2x within a single session.

## What it does not

- **It does not establish that either stack is faster than the other**, at any payload. The
  ranges overlap, the direction is inconsistent at the empty-body size, one pass did not
  complete, and the control drift exceeds the candidate effect by roughly thirty times.
- It does not establish anything about payloads above 1 KiB for either arm.
- It does not establish that `h3-ngnet-quic` is correct under sustained load. It is not.
- It does not establish a rate or a root cause for the adapter defect beyond the figures above;
  the failing component between the server accepting a stream and the client seeing a response
  has not been identified. *(Since identified: see
  [`02-h3-ngnet-quic-fin-fix`](02-h3-ngnet-quic-fin-fix.md). The rest of this list is unchanged
  by that, because none of it depended on the defect's cause.)*
- It does not establish anything about a real network. Loopback only.
- It says nothing about the compio or QMux families; they were not run here beyond the one
  control arm.
- It does not carry over to `xeon-8370c-azure`, and nothing here should be compared with it.
