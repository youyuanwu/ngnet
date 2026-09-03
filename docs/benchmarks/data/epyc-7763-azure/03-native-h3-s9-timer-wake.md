# Native HTTP/3 S9 timer wake

**Date:** 2026-09-03  
**Pre-fix base:** `c78cd78719d89ac0e0ed57bdd5772201ec159123`  
**Qualified implementation:** `fb8257d` (the later `91dad55` removes dead state and changes
wording only)  
**Result type:** reliability and correctness only; no performance claim

## Workload and supervision

The target workload is one native loopback connection carrying 125 sequential POST/echo
exchanges with an exact 1 MiB body in each direction. `reliability` mode checks length,
contents, and terminal completion, uses a 60-second per-exchange bound and a 150-second
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

Five of the first 73 qualifying processes failed. Four stopped during the first five
exchanges; one stopped at exchange 45. Each returned a classified unexpected `Closed` result
instead of completing the exact exchange. No process escaped the outer supervisor and no
child remained.

Armed runs reproduced both response-head and body-drain failures at 1 MiB and 16 KiB. In the
clearest same-occurrence trace, a write was blocked with positive stream, connection, and
congestion credit. Its expiry was armed 15 ns in the future, the adapter parked, and no
timer-ready or driver-wake event followed before idle timeout. Other captures showed the same
shape with imminent deadlines up to 11.8 µs. Queues retained capacity, inbound drops were zero,
and receive credit was returned.

This is not PR #57's lost FIN: substantial body data remained, the native result was generic
transport blocking, and the failure was not a complete body waiting only for its terminal
frame.

## Change and regression

The detached adapter keeps its ordinary expiry sleep. For an expiry no more than 100 µs away,
it additionally schedules fallback polls, capped at 64 for one unchanged deadline and reset
when the deadline moves or the timer completes. The focused regression checks the exact
threshold, threshold plus one nanosecond, the cap, and a fresh budget for a new deadline.

This is a bounded scheduling fallback, not an unconditional poll loop. Armed observations
count it separately as `TimerKick`; record retention remains bounded and reports drops.

## Post-change result

Two separate post-change schedules each exited successfully:

| Revision | Processes | Exchanges | Classified failures | Outer kills | Cleanup failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| initial one-wake fallback | 100 | 12,500 | 0 | 0 | 0 |
| bounded-64 final fallback | 100 | 12,500 | 0 | 0 | 0 |

The schedules are not pooled because they exercised successive revisions. Each 0/100 result
independently gives an approximate one-sided 95% upper per-process failure-rate bound of 3%.
It does not prove that intermittent failure is impossible.

A separate armed 16 KiB schedule completed 10/10 and recorded 35,054 bounded timer kicks,
confirming that the corrected path was exercised. Both ignored exact fixtures also completed
under the committed supervisor. These are supporting correctness observations, not timing
measurements.

## Limits

- The current reliability result applies to the reproduced missing imminent-timer wake.
- Response-head and body-drain occurrences shared that transport state; a future failure with
  different evidence must be classified independently.
- ngtcp2 frame inventory and retransmission attribution remain unavailable through the safe
  wrapper.
- The host remains unsuitable for cross-run or cross-machine performance conclusions.
