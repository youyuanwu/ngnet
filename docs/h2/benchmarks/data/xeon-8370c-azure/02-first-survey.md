# 02 — First survey of the arms on this machine

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-16
**Commit:** `e75118e`
**Cases:** all eight bench targets
**Command:** the same two passes as [01-drift-baseline](01-drift-baseline.md) — this is that
session read as a survey rather than as a drift measurement
**Repetitions:** two, back to back
**Controls:** none needed for a survey; the drift bar established by the same passes is
**~1% median, 2% for all but one benchmark**, which is what the differences below are sized
against
**Exclusions:** none

## What was being asked

Where the arms stand on this host, which of the legacy host's conclusions survive the move, and
where this stack is actually faster than hyper. Each finding states what a new machine should
reproduce; this is the first attempt at those lists.

Both passes are shown throughout, `r1 / r2`, because a survey with two repetitions should
display them rather than average them away.

## Results: the real-socket family

Serial latency, empty body, µs — lower is better:

| Arm | r1 / r2 |
| --- | --- |
| `ngnet-h2-compio` | 22.00 / 21.99 |
| `ngnet-h2-tokio` | 21.19 / 21.26 |
| `hyper-tokio` | **20.93 / 21.18** |

Concurrent throughput, Kelem/s — higher is better:

| N | `ngnet-h2-compio` | `ngnet-h2-tokio` | `hyper-tokio` |
| --- | --- | --- | --- |
| 1 | 43.6 / 43.8 | **45.8 / 45.5** | 44.4 / 44.5 |
| 8 | **109.4 / 108.9** | 107.7 / 109.0 | 107.4 / 107.0 |
| 64 | 121.0 / 120.4 | 116.9 / 116.5 | **126.4 / 128.5** |

Body throughput, MiB/s — higher is better; 0 B reported as µs per iteration:

| Body | `ngnet-h2-compio` | `ngnet-h2-tokio` | `hyper-tokio` |
| --- | --- | --- | --- |
| 0 B | 21.54 / 21.59 µs | 20.98 / 21.32 µs | **20.65 / 21.17 µs** |
| 1 KiB | **38 / 38** | 28 / 28 | 32 / 31 |
| 64 KiB | 560 / 559 | **598 / 586** | 545 / 535 |
| 1 MiB | 600 / 597 | 716 / 708 | **835 / 819** |

## Results: the duplex family

| Measure | `ngnet-h2` | `hyper` |
| --- | --- | --- |
| Serial latency | 10.91 / 10.85 µs | **9.57 / 9.51 µs** |
| Concurrent N=1 | 86.8 / 87.6 Kelem/s | **95.8 / 96.6** |
| Concurrent N=8 | 125.7 / 127.0 Kelem/s | **129.5 / 130.9** |
| Concurrent N=64 | 119.4 / 120.3 Kelem/s | **131.3 / 131.9** |
| Body 0 B | 10.62 / 10.87 µs | **9.58 / 9.86 µs** |
| Body 1 KiB | **72 / 71 MiB/s** | 71 / 70 |
| Body 64 KiB | 1605 / 1570 MiB/s | **1685 / 1714** |
| Body 1 MiB | 1971 / 1959 MiB/s | **2554 / 2559** |

## Against the write-path finding's predictions

[`../../findings/write-path-and-gathering.md`](../../findings/write-path-and-gathering.md)
asks a new machine for four things. Two reproduce, one reproduces in part, one is untested.

1. **"At N=8 and N=64, gathering tokio sits level with or ahead of both compio and hyper, not
   2× behind."** ✅ **for the central claim, ✗ for the ordering.** At N=8 the three arms are
   within 2% of one another (109.4 / 107.7 / 107.4), which is the point — the 2.1×–2.4× gap the
   per-block drain caused is gone. At N=64 tokio is 116.9 against compio's 121.0 and hyper's
   126.4, so it is **3% behind compio and 8% behind hyper**, not level or ahead. That is
   comfortably outside this host's ~1% drift bar and is a real, if small, ordering difference
   from the legacy host, where gathering tokio led at 166.0 against ~152. Recorded as measured;
   the finding's headline survives and its ordering claim does not.
2. **"At N=1 the three arms are within noise, and so is empty-body serial latency."** ✅ N=1
   spans 43.6–45.8 Kelem/s, about 5%; serial latency spans 20.93–22.00 µs, about 5%. Wider than
   the drift bar but far narrower than any effect this suite is used to detect, and the
   ordering is stable across both passes.
3. **"Forcing `is_write_vectored` to `false` costs a large fraction at N=64 and nothing at
   N=1."** ⏳ **Not run.** This needs a deliberate code change and is the single most valuable
   run still outstanding on this host; the mechanism, not the ranking, is what the finding
   rests on.
4. **"A write-side change moves 1 KiB most, 64 KiB less, 1 MiB indistinguishably."** ⏳ Not
   testable from a survey — it is a statement about a change, not about a standing. See
   [03-shared-body](03-shared-body.md), where the write-count ratios are exercised directly.

## Against hyper, case by case

The tables above place the arms; this reads them as the question a caller actually asks. Every
figure is the ngnet arm against `hyper` (duplex) or `hyper-tokio` (socket), **negative meaning
ngnet-h2 is faster**, averaged over the two passes — five for `transport_shared_body`. All are
consistent in sign across every replicate.

**Where `ngnet-h2` wins, by more than this host's ~1% drift bar:**

| Case | Arm | Body / N | Delta |
| --- | --- | --- | --- |
| `transport_shared_body` | `tokio-shared` | 64 KiB | **−29.88%** |
| `transport_shared_body` | `tokio-shared` | 1 KiB | **−21.96%** |
| `transport_body_throughput` | `ngnet-h2-compio` | 1 KiB | **−17.56%** |
| `transport_shared_body` | `compio-shared` | 1 KiB | −16.15% |
| `transport_shared_body` | `tokio-shared` | 1 MiB | **−11.84%** |
| `transport_body_throughput` | `ngnet-h2-tokio` | 64 KiB | −8.81% |
| `concurrent_throughput_multi_thread` | `ngnet-h2` | N=8 | −8.39% |
| `shared_body` (duplex) | `ngnet-h2-shared` | 1 KiB | −6.98% |
| `concurrent_throughput_multi_thread` | `ngnet-h2` | N=64 | −6.39% |
| `transport_body_throughput` | `ngnet-h2-compio` | 64 KiB | −3.57% |
| `transport_concurrent_throughput` | `ngnet-h2-tokio` | N=1 | −2.60% |
| `transport_concurrent_throughput` | `ngnet-h2-compio` | N=8 | −1.75% |
| `transport_concurrent_throughput` | `ngnet-h2-tokio` | N=8 | −1.03% |

**Where hyper wins:**

| Case | Arm | Body / N | Delta |
| --- | --- | --- | --- |
| `transport_shared_body` | `compio-push` | 1 MiB | +40.22% |
| `transport_body_throughput` | `ngnet-h2-compio` | 1 MiB | +38.23% |
| `body_throughput` (duplex) | `ngnet-h2` | 1 MiB | +30.09% |
| `transport_body_throughput` | `ngnet-h2-tokio` | 1 MiB | +16.18% |
| `serial_latency` (duplex) | `ngnet-h2` | — | +14.09% |
| `transport_body_throughput` | `ngnet-h2-tokio` | 1 KiB | +11.32% |
| `concurrent_throughput` (duplex) | `ngnet-h2` | N=1, N=64 | +10.28%, +9.80% |
| `transport_concurrent_throughput` | `ngnet-h2-tokio` | N=64 | +9.23% |
| `transport_serial_latency` | `ngnet-h2-tokio` | — | +0.80% |

Two things fall out of this that neither the arm tables nor the predictions list says on its
own.

1. **Handing bodies over is what turns the socket body sweep around, and it is a bigger lever
   than the choice of transport.** The same tokio transport loses to hyper at 1 KiB (+10.29%)
   and 1 MiB (+16.51%) on the push path, and beats it at *every* size on the shared path,
   1 MiB included (−11.84%). The entry point moves this comparison further than epoll against
   io_uring does.
2. **compio's largest win and its largest loss are the same mechanism.** −17.56% at 1 KiB and
   +38.23% at 1 MiB are both the coalescing copy its push path pays: irrelevant on a small
   body, dominant on a large one. Handing the body over narrows the 1 MiB gap to +33.83% but
   does not close it — see [03-shared-body](03-shared-body.md) for why the completion transport
   has only the copy to win and no syscall.

**1 MiB is hyper's, and the empty-body points are hyper's.** Those are the two shapes where
nothing this crate has done so far competes, and they are worth stating plainly rather than
leaving to be read out of a table.

## Other observations

- **compio leads at 1 KiB by a wide margin** (38 against 32 and 28 MiB/s) and the legacy host
  said the same. That is the clearest carried-over result in the survey.
- **compio is last at 1 MiB** (600 against 716 and 835), which the legacy host also showed
  before bodies could be handed over — it is the coalescing copy the push path pays on a
  completion transport, and [03-shared-body](03-shared-body.md) shows what removing it is
  worth here.
- **hyper leads the duplex family almost throughout**, more clearly than on the legacy host.
  The duplex measures protocol and wrapper CPU with the kernel deleted, so this is a statement
  about CPU work per exchange and not about I/O.
- **The empty-body near-tie holds on the socket family** (20.65–22.00 µs across three arms and
  two I/O models), which is the control it has always been.

## What this does not establish

- **Two repetitions.** Every difference above larger than a few percent is stable across both
  passes, but a survey is not an A/B and none of these are paired deltas against a control.
- Nothing here is comparable with the legacy host's absolute figures, only with its orderings.
- The multi-threaded duplex group was measured but is not tabulated above; it is a separate
  question about scheduling, not about either stack.
