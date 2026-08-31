# 30 — ngtcp2 final-review calibration, repetition, and RSS resolution

**Machine:** historical [`xeon-8370c-azure`](README.md) label; CPU 3
**Date:** 2026-08-30
**Source:** `d8d9d90` (final-review source/test fixes; measurements were collected from the
identical release source immediately before that local commit)
**Base:** `24874f3`
**Purpose:** resolve final-review timing, liveness, failure-evidence, and memory findings
**Exclusions:** no system-under-test failure was excluded or replaced

## Instrument repairs

Timing now performs equivalent drain/count work in all three arms. Byte-exact comparison is
confined to diagnostic/correctness mode. Feature-enabled but unarmed stream writes skip
diagnostic range/retention traversal and the diagnostic-only staging control; a representative
allocation test exercises an actual drain with the control deliberately left set.

Diagnostic output now takes one exclusive interval drain. Cumulative counters, attempts,
liveness, and overflow state share one boundary; live retained/queue gauges survive the drain
and re-seed their next high-water marks. RSS is sampled immediately after response drain,
before formatting, and failure paths emit best-effort RSS plus the exclusive snapshot.

Local transport-packet production and generic driver wakes no longer count as enabling events.
Only inbound datagrams, timer fires, and outbound-capacity transitions can satisfy the
diagnostic retry relation. The resulting non-zero
`zero_accept_retries_without_enable` values are retained rather than converted back to zero.
This changes observation only; the outer HTTP/3 sendability-generation scheduling candidate
was not implemented.

Application body bytes and transport stream bytes are reported separately. Transport stream
bytes include HTTP/3 framing/control data, must cover each exact body, and still reconcile
accepted bytes against immediate release events.

## Predetermined exactness repetitions

The protocol was fixed before execution: five default-profile and five release-profile
invocations of the active 125 × 1 MiB exact test, with per-exchange body/build-scaled timeouts
and outer limits of 1,800 seconds (debug) and 900 seconds (release).

The first release harness invocation mistakenly used Cargo's unstable `-C` option. All five
attempts failed before compilation with exit 101. This was a procedure failure, not a
system-under-test result, and is retained here. The corrected five-pass release set was then
run in full.

| Profile | Repetition elapsed seconds | Exit / result |
| --- | --- | --- |
| release | 6.96; 9.94; 9.95; 9.84; 9.86 | `0,0,0,0,0`; all 125 responses exact |
| debug | 15.91; 18.98; 16.21; 15.78; 15.72 | `0,0,0,0,0`; all 125 responses exact |

No exact-test repetition stalled, timed out, corrupted content, or terminated abnormally.
This supports the active fixture regression; it does not override the diagnostic failures
below.

## Predetermined 1 KiB calibration

The release Criterion target and feature-enabled-but-unarmed probe were built together, then
run as three fixed Criterion/probe pairs without source edits.

| Pass | Criterion estimate | Probe elapsed | Probe per exchange | Exit |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 117.00 µs | 1,211,623,972 ns | 121.1623972 µs | 0 / 0 |
| 2 | 116.96 µs | 1,224,527,378 ns | 122.4527378 µs | 0 / 0 |
| 3 | 118.49 µs | 1,279,327,001 ns | 127.9327001 µs | 0 / 0 |

Criterion's point-estimate median is 117.00 µs and its three-pass span is 1.308%. The probe
median is 122.4527378 µs, 4.660% above Criterion and inside the 5% agreement limit, but the
probe's own span is 5.529%. Calibration therefore **fails** the predeclared maximum 5% span.
All fixed-count probe timing — including runs 27 and 29 — remains report-only and ungated.

## Predetermined fresh RSS processes

The fixed schedule was three fresh processes at each of 125, 250, and 500 × 1 MiB. Each
exchange had the 15-second release diagnostic timeout; each process used the documented
`60 + 5 × count` outer timeout. Values are sampled `VmRSS`, not kernel `VmHWM`.

| Run | Exchanges | Ready | Maximum sampled | Final | Increase | Exit / completion |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 125 | 14,024 KiB | 19,800 KiB | 18,772 KiB | 5,776 KiB | 0 / 125 |
| 2 | 125 | 14,024 KiB | 22,188 KiB | 21,160 KiB | 8,164 KiB | 0 / 125 |
| 3 | 125 | 14,072 KiB | 22,204 KiB | 18,204 KiB | 8,132 KiB | 0 / 125 |
| 4 | 250 | 13,884 KiB | 22,280 KiB | unavailable | 8,396 KiB | **101 / timeout at exchange 3; last completed 2** |
| 5 | 250 | 13,848 KiB | 21,912 KiB | 17,348 KiB | 8,064 KiB | 0 / 250 |
| 6 | 250 | 13,960 KiB | 21,636 KiB | 18,476 KiB | 7,676 KiB | 0 / 250 |
| 7 | 500 | 13,880 KiB | 22,000 KiB | 18,268 KiB | 8,120 KiB | 0 / 500 |
| 8 | 500 | 14,000 KiB | 19,016 KiB | 17,628 KiB | 5,016 KiB | 0 / 500 |
| 9 | 500 | 13,896 KiB | 20,892 KiB | 18,160 KiB | 6,996 KiB | 0 / 500 |

The three 125-run increases produce an 8,164 KiB envelope and a 2,048 KiB tolerance, hence
a 10,212 KiB limit. Every observed longer-run increase is below it, but run 4 did not
complete. The required stability/RSS criterion is therefore **unmet**; the failed run is not
replaced by a passing process. All nine processes reported zero inbound drops, zero terminal
outbound discards, and no counter overflow through their last complete snapshot.

Run 4's failure snapshot had no inbound drop or overflow and reported 4,119
zero-accept retries without a true enabling event in its observed intervals. The timeout path
successfully emitted RSS and coherent snapshots before panic.

## Focused stall investigation

After run 4 failed, five additional 250 × 1 MiB diagnostic processes were predetermined with
the same 15-second per-exchange and 1,310-second outer limits. Results:

| Repetition | Result | Maximum sampled `VmRSS` | Zero-accept retries without enable |
| ---: | --- | ---: | ---: |
| 1 | **exit 101; timeout at exchange 2, last completed 1** | 17,760 KiB | 925 |
| 2 | exit 0; completed 250 | 22,728 KiB | 17,377 |
| 3 | exit 0; completed 250 | 22,036 KiB | 24,794 |
| 4 | exit 0; completed 250 | 20,960 KiB | 23,474 |
| 5 | exit 0; completed 250 | 18,184 KiB | 2,380 |

The failed exchange's final client interval offered 123,570,681 transport stream bytes,
prepared 174,724, accepted/released 55,016, and recorded 82 zero acceptances, 81 retries, 60
retries without an enabling event, zero drops, zero queue-capacity parks, and no timer fire.
The final trace includes repeated `DriverWake → Retry(enabling=unavailable) →
Park(transport-blocked)` sequences, followed eventually by an inbound event. This localizes
the observation to unproductive outer repolling but does not establish a safe minimal
production fix; enforcing a sendability generation would be the separately deferred S9
scheduling redesign.

The two reproduced diagnostic timeouts are durable correctness/stability evidence. The
checkout may be described as passing ten predetermined exact fixture repetitions, but not as
unconditionally stable under the armed persistent diagnostic workload.

## Deterministic resource and bound coverage

- Normal detached output uses 63 slots and reserves the 64th for synchronous
  CONNECTION_CLOSE. The close drains after all existing output; a deterministic full-queue
  test proves total depth remains 64 and every datagram is returned.
- Terminal transition inventories/discards unread inbound datagrams but preserves all
  outbound data. A deterministic induced-drop test fills the 64-datagram inbound queue,
  records two expected drops, then records all 64 queued datagrams at terminal.
- Dropping a connection reports any retained backing as released and resets live retained
  gauges.
- Fixed-limit staging scaling is now a deterministic controlled retention test with an exact
  2.0× relation. The historical live-loopback 2.021× observation remains evidence only, not
  an automated 2.1× gate.

## Validation

After the fixes:

- targeted diagnostics: 318 `ngnet-quic` tests, 3 `zero_alloc` tests, 2 probe tests, and 5
  diagnostic fixture tests passed;
- `cargo test --workspace --all-features`: 1,627 passed, 1 ignored;
- `cargo test --workspace`: 1,611 passed, 1 ignored;
- all-target warning-denying clippy passed after one local needless-borrow correction;
- workspace benchmark smoke passed after one test-only import warning correction;
- warning-denying `ngnet-quic`/`ngnet-quic-h3` documentation passed;
- changed Rust files passed direct rustfmt checking and the branch diff passed
  `git diff --check`.

The first all-feature workspace attempt failed to compile because the new terminal-retention
test lacked a `StreamId` import; the corrected exact command passed. The first clippy attempt
found one needless borrow, and the first post-change benchmark smoke emitted one unused
test-import warning; both corrections and successful reruns are included rather than omitted.

## Disposition

S9 remains deferred: no production packet-order or sendability-generation scheduling change
is included. The new failures make that candidate suitable for focused follow-up research,
but this resolution preserves evidence rather than guessing at a scheduler redesign. The six
run-29 optimization candidates and all other report-only consider findings remain deferred.
