# Native HTTP/3 S9 timer investigation

**Date:** 2026-09-03
**Pre-fix base:** `c78cd78719d89ac0e0ed57bdd5772201ec159123`
**Harness implementation:** final branch
**Production result:** no timer fallback retained; S9 remains open
**Result type:** reproduction and correctness evidence only; no performance claim

## Workload and supervision

The target workload is one native loopback connection carrying 125 sequential POST/echo
exchanges with an exact 1 MiB body in each direction. `reliability` mode checks length,
contents, and terminal completion, uses a 60-second per-exchange bound and a 135-second
process bound, and remains transport-diagnostics-unarmed. The committed supervisor adds a
180-second outer bound and verifies process-group cleanup:

```sh
cargo build -p ngnet-bench --examples --release --features diagnostics
./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 reliability \
  1048576 125 100 180 1
```

This host was not suitable for comparative timing during the run. Elapsed times are omitted.

## Pre-change reproduction

At least five of the first 73 auditable processes failed. Four stopped during the first five
exchanges; one stopped at exchange 45. Each returned a classified unexpected `Closed` result
instead of completing the exact exchange. No process escaped the outer supervisor and no
child remained.

The original invocation continued to 100, but its final 27 per-run results and summary were
lost to terminal output truncation. It is therefore evidence of repeated reproduction, not a
complete 100-process pre-change rate estimate. The supervisor now supports an append-only
manifest and rejects duplicate run numbers so future schedules retain the actual denominator.

Armed runs reproduced both response-head and body-drain failures at 1 MiB and 16 KiB. In the
clearest same-occurrence trace, a write was blocked with positive stream, connection, and
congestion credit. Its expiry was armed 15 ns in the future, the adapter parked, and no
timer-ready or driver-wake event followed before idle timeout. Other captures showed the same
shape with imminent deadlines up to 11.8 µs. Queues retained capacity, inbound drops were zero,
and receive credit was returned.

This is not PR #57's lost FIN: substantial body data remained, the native result was generic
transport blocking, and the failure was not a complete body waiting only for its terminal
frame.

## Candidate changes

Three timer-fallback revisions were evaluated: an immediate one-wake edge, a bounded self-wake
budget, and a coarse backup sleep gated on no-progress transport blocking. Their focused tests
and exact exchanges passed, but none deterministically reproduced the missing runtime wake or
proved progress from the captured state. The fallback changes were removed from production.

## Post-change result

Three candidate schedules each exited successfully:

| Revision | Processes | Exchanges | Classified failures | Outer kills | Cleanup failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| `ad14c82` initial one-wake fallback | 100 | 12,500 | 0 | 0 | 0 |
| `fb8257d` bounded-64 final fallback | 100 | 12,500 | 0 | 0 | 0 |
| `4dec3b1` deadline-backed candidate | 100 | 12,500 | 0 | 0 | 0 |

The schedules are not pooled because they exercised successive candidates. Each 0/100 result
independently gives an approximate one-sided 95% upper per-process failure-rate bound of 3%.
They did not prove that intermittent failure is impossible or that the candidate was causal.

A separate armed 16 KiB schedule on the self-wake candidate completed 10/10. The backup-sleep
candidate completed 20/20 armed 125 × 1 MiB processes. Nevertheless, its ignored 1 MiB fixture
recorded one response-head failure in 30 processes, and the earlier self-wake candidate had one
close-before-response failure in eight all-feature workspace runs.

The timer-fire counter now means "expiry observed due at pump entry"; historical captures used
the narrower "sleep observed ready" hook. Current runs separately report `timer-ready`, so old
and new `timer_fires` counts must not be compared directly.

The backup-sleep candidate also completed ten pre-declared sequential
`cargo test --workspace --all-features` invocations. An earlier self-wake candidate produced
one close-before-response failure in eight such invocations.

With all fallback candidates removed, a final all-feature workspace invocation run concurrently
with the other cargo gates failed the basic request/response test and timed out the large
flow-control response. An isolated rerun passed. The final evidence-only branch therefore
retains the contention-sensitive workspace caveat.

At the final evidence-only HEAD, the first isolated all-feature workspace rerun timed out an
`h3-ngnet-quic` lifecycle test (the separate adapter); its immediate isolated rerun passed.
This is recorded as validation instability, not attributed to native S9.

### Residual fixture observation

The backup-sleep candidate's ignored 125 × 16 KiB fixture completed 5/5. Its ignored
125 × 1 MiB fixture recorded one classified response-head failure in its first five processes;
25 subsequent processes completed, for 1/30 overall. The failing process had no armed
same-occurrence trace, so it cannot be attributed to the timer state or another mechanism.

This residual prevents a claim that every S9-shaped failure is eliminated. The committed
fixture supervisor and typed checkpoints make it reproducible and classify the exact phase,
integrity, terminal state, completion marker, and cleanup result. A future occurrence needs an
armed same-occurrence capture before another production change is justified.

## Decision and limits

- The timer state is a high-confidence correlation, not a deterministically proven root cause.
- No production timer fallback is retained because residual failures remained and the
  candidates added steady-state scheduling work.
- The committed supervisor and bounded diagnostics provide a high-confidence reproducer.
- A fix remains blocked on an armed same-occurrence residual capture.
- After removing the candidates, 20 additional primary processes completed. That clean sample
  does not override the earlier repeated failures or prove the residual absent.
- ngtcp2 frame inventory and retransmission attribution remain unavailable through the safe
  wrapper.
- The host remains unsuitable for cross-run or cross-machine performance conclusions.

Follow-up root-cause work is recorded in
[`04-native-h3-s9-root-cause.md`](04-native-h3-s9-root-cause.md). It selected the
pump/no-progress scheduling seam and retained a surgical correction, but a separate
pre-readiness 1 MiB failure blocked a full resolution claim.
