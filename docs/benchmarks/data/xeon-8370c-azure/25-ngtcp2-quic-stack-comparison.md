# 25 — Is ngnet-quic-h3 faster than ngnet-h3-quinn?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8370C
**Date:** 2026-08-30
**Source:** `feature/ngtcp2-h3-benchmark` working tree based on `7422956`
**Cases:** `quic_stack_serial_latency` and `quic_stack_body_throughput`
**Command:** `taskset -c 3 <bench-binary> --bench --warm-up-time 1
--measurement-time 3 --sample-size 30 --save-baseline <pass> --noplot`
**Repetitions:** three serial and three 1 KiB body passes; arms adjacent within each pass
**Controls:** same current-thread Tokio runtime shape, loopback UDP, `h3` ALPN, generated
certificate trust, persistent warmed connection, request, echo response, and full drain
**Exclusions:** no timed serial or 1 KiB repetition excluded; 16 KiB was excluded from the
canonical matrix after five completed but highly variable passes and one failed warm-up;
1 MiB was excluded after repeatable native crashes

## What was being asked

Does the ngtcp2/OpenSSL integration make `ngnet-h3` faster than its Quinn/rustls integration
on persistent local request/response work? Upstream `h3-quinn` remains beside them as a Quinn
control, but the ngtcp2 comparison varies the complete QUIC/TLS/endpoint/adapter stack and
cannot attribute a result to one layer.

## Results

Criterion median point estimate per complete exchange. The table's median is the median of
the three pass estimates. Lower is better.

| Case | Arm | Pass 1 | Pass 2 | Pass 3 | Median |
| --- | --- | ---: | ---: | ---: | ---: |
| empty serial | `ngnet-h3-quinn` | 78.095 µs | 77.459 µs | 77.538 µs | **77.538 µs** |
| empty serial | `ngnet-quic-h3` | 117.023 µs | 117.286 µs | 119.830 µs | **117.286 µs** |
| empty serial | `h3-quinn` | 39.840 µs | 39.524 µs | 38.761 µs | **39.524 µs** |
| 1 KiB echo | `ngnet-h3-quinn` | 86.066 µs | 87.964 µs | 86.708 µs | **86.708 µs** |
| 1 KiB echo | `ngnet-quic-h3` | 120.701 µs | 121.513 µs | 123.573 µs | **121.513 µs** |
| 1 KiB echo | `h3-quinn` | 43.014 µs | 40.807 µs | 41.430 µs | **41.430 µs** |

Against `ngnet-h3-quinn`, `ngnet-quic-h3` is 1.513× slower on the empty exchange and
1.401× slower for the 1 KiB echo. Its median 1 KiB throughput is 16.07 MiB/s, against
22.53 MiB/s for `ngnet-h3-quinn` and 47.14 MiB/s for upstream `h3-quinn`.

## Drift controls in the same session

The two Quinn arms were stable across the three canonical passes: `ngnet-h3-quinn` spans
0.82% serial and 2.2% at 1 KiB; upstream `h3-quinn` spans 2.7% serial and 5.3% at 1 KiB.
The ngtcp2 spans are 2.4% and 2.4%, respectively. These are much smaller than the measured
40–51% gap between the two `ngnet-h3` transports.

## Larger-body stability

The initially attempted 16 KiB case did not meet the bar for a recorded timing. Five passes
completed, but ngtcp2 median estimates ranged from 358.198 to 684.784 µs while the
`ngnet-h3-quinn` control stayed between 154.938 and 158.657 µs; a sixth pass closed or stalled
the ngtcp2 connection during warm-up. At 1 MiB, isolated optimized runs could terminate with
`SIGSEGV` during Criterion collection. No favorable completed timing is substituted for
either failed workload.

Building the persistent fixture also exposed that `ngnet-quic-h3` did not restore the peer's
stream allowance after a peer-opened stream closed. The adapter now grants one slot back on
close, and a 125-exchange regression crosses the default limit of 100. That correction was
present for every result above but did not settle the larger-body instability.

## What this establishes

- `ngnet-quic-h3` is not the faster `ngnet-h3` transport on the two workloads it can run
  repeatably today.
- The difference is much larger than same-session drift: 51.3% for empty exchanges and
  40.1% at 1 KiB.
- Larger persistent body workloads are stability bugs to fix before they are throughput
  comparisons.

## What it does not

- It does not isolate ngtcp2 from OpenSSL, endpoint driving, or `ngnet-quic-h3`; those differ
  together.
- It does not compare congestion control, loss, internet latency, tail latency,
  multi-connection scaling, or multi-core execution.
- It does not establish a 16 KiB or 1 MiB throughput ratio because the ngtcp2 arm was not
  reliable at those sizes.
