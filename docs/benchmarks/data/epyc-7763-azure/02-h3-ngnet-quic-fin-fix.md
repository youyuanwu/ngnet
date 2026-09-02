# 02 — The lost FIN: root cause and reliability after the fix

> **This is a reliability record, not a performance one.** No timing comparison is made here and
> none is implied. The host's disqualifying control drift, recorded in [`README`](README.md) and
> in [run 01](01-h3-ngnet-quic-comparison.md), has not changed: a carried unchanged control arm
> moved by a factor of 4.2 within one session, which is far larger than any effect of interest.
> Run 01's verdict — *no difference is claimed* — stands unaltered and is not revisited below.

**Machine:** [`epyc-7763-azure`](README.md)
**Date:** 2026-09-02
**Commit:** the FIN fix, on `feature/h3-ngnet-quic` (parent `2851bf8`)
**What was measured:** completion or failure of a fixed workload, counted over repeated runs
**Command:** `taskset -c 0 ./target/release/examples/probe <arm> body <size> <count> timing`
**Failure definition:** the process exits non-zero or exceeds its timeout. Nothing else counts
as a failure and nothing was excluded.

## What was being asked

Run 01 found a reproducible liveness defect in `h3-ngnet-quic` and could not locate it. Two
questions followed: what actually caused it, and — once fixed — does the adapter complete the
workload that used to fail it, on the same host, against the same matched native arm?

## The root cause

A lost FIN, and the conflation that lost it was in the transport wrapper rather than in the new
adapter's wake plumbing.

`ngtcp2_conn_writev_stream` reports what it serialised through `*pdatalen`
(`crates/ngnet-quic-sys/vendor/ngtcp2/lib/includes/ngtcp2/ngtcp2.h:5233-5243`). `-1` means the
packet contains no STREAM frame at all — other frames occupied it. `0` means a *zero-length*
STREAM frame was serialised, which ngtcp2 does exactly when the offer carries nothing but `fin`.
On a `fin`-only write the two are opposites. `crates/ngnet-quic/src/stream_io.rs` clamped the
sign with `accepted.max(0)`, so both reached callers as `StreamWrite::Datagram { accepted: 0 }`,
and `h3-ngnet-quic`'s `poll_finish` read that as proof the stream had ended. Nothing was in
flight, so loss recovery had nothing to retransmit, and the peer waited until its idle timeout.

The evidence that fixed the attribution came from instrumented counters dumped at the stall
rather than from tracing, which perturbs the timing away. On one captured failure the server
produced five datagrams for the response — 31, 33, 33, 1055 and 31 bytes, the last being the
`fin`-only write reported as accepted — and the client read all five with `dropped_inbound = 0`,
observing four stream-data events and, from the fifth, an acknowledgement rather than a FIN.

Full derivation, the ngtcp2 gate that skips the caller's stream, and the fix across all four
call sites are in
[`../../../h3-ngnet-quic/pending-work.md`](../../../h3-ngnet-quic/pending-work.md).

## Reliability after the fix

Release build, pinned with `taskset -c 0`, both arms over the same `ngnet-quic` transport with
the same fixtures and the same workload. The host was **not** quiesced for these runs and did
not need to be: a completion count is not a timing.

| Workload | Arm | Runs | Failures |
| --- | --- | ---: | ---: |
| 200 x 1 KiB | `h3-ngnet-quic` | 25 | 0 |
| 200 x 1 KiB | `ngnet-quic-h3` (matched) | 25 | 0 |
| 200 x 16 KiB | `h3-ngnet-quic` | 20 | 0 |
| 200 x 16 KiB | `ngnet-quic-h3` (matched) | 20 | **2** |
| 30 x 1 MiB | `h3-ngnet-quic` | 10 | 0 |
| 30 x 1 MiB | `ngnet-quic-h3` (matched) | 10 | 0 |

For comparison, from run 01 on the same host before the fix, same 1 KiB workload:
`h3-ngnet-quic` 6 failures in 10, `ngnet-quic-h3` 0 in 10.

The crate's own suite, with every previously `#[ignore]`d live-loopback test enabled, passed 35
tests in each of 3 consecutive pinned release runs.

## The 16 KiB row, and who owns it

The two failures are the *native* arm's, and they are the known S9 large-body defect rather than
anything new. Both failed with `ErrorKind::Closed` — "the connection has ended" — reported by
the ngtcp2 HTTP/3 server driver, not with a timeout.

The attribution rule is the one run 01 fixed before measuring, applied unchanged and this time
pointing the other way: the transport, the fixtures, the payload and the exchange count were
identical across the two arms, so a failure on one and not the other belongs to the layer that
differs. In run 01 that reasoning assigned the 1 KiB failures to `h3-ngnet-quic`. Here it
assigns the 16 KiB failures to `ngnet-quic-h3`.

The FIN fix also removed a latent FIN-loss path in `ngnet-quic-h3` — its `transmit::drain` now
reports a stream-less packet as `Blocked`, which reaches the arm in `ngnet-h3`'s driver written
for exactly that case (`crates/ngnet-h3/src/http/driver.rs:293-298`). It did not resolve S9,
which was checked rather than assumed.

## What this establishes

- The liveness defect run 01 found is root-caused, fixed, and covered by a deterministic
  regression test that fails against the previous behaviour
  (`crates/ngnet-quic/tests/fin_delivery.rs`).
- `h3-ngnet-quic` completed 55 supervised runs across three payload sizes without a failure,
  including the exact workload that failed 6 times in 10 before.
- Its whole test suite is enabled and passes; nothing in it is `#[ignore]`d.
- 16 KiB and 1 MiB probes, recorded as *not run* in run 01 because the adapter's defect made
  them meaningless, have now been run on both arms.

## What it does not

- **It establishes nothing about performance.** No timing from these runs is reported,
  compared, or used. This host still cannot measure a difference of the size in question, and
  run 01's "no difference is claimed" is unchanged.
- It does not put a rate on any residual timing failure in `h3-ngnet-quic`. Zero failures in 55
  runs bounds it loosely; it does not prove it is zero.
- It does not resolve S9, and does not locate it. The native arm's 16 KiB failure is recorded,
  not explained.
- It does not establish that 1 MiB is safe on either arm: 10 runs of 30 exchanges each is a
  small sample, and the 16 KiB failures show this class of fault is intermittent enough to hide
  in one.
- It does not establish anything about a real network. Loopback only.
- It does not carry over to `xeon-8370c-azure`, and nothing here should be compared with it.
