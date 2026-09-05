# Native HTTP/3 S9 final qualification

**Date:** 2026-09-05
**Base:** `60b12d3a49f2` (merged PR #59)
**Qualified revision:** `fda2fcd1353c`
**Host:** `ubuntu26-dev-vm` (`epyc-7763-azure`)
**Result:** S9 resolved; planned 100-process schedules passed
**Result type:** reliability and root-cause evidence only; no performance claim

## Outcome

The remaining pre-readiness blocker is now typed before the existing terminal boundary.
`NgnetNgtcpH3` exposes only its internal warm-up response-head error through the existing
checked-failure type
([`tests/ngnet-bench/src/lib.rs:2252-2322`](../../../../tests/ngnet-bench/src/lib.rs#L2252-L2322)).
The probe emits one single-line record with phase `pre-readiness-warmup`, classifier
`pre-readiness-response-head`, percent-encoded detail and the actual diagnostic armed state
before panicking
([`tests/ngnet-bench/examples/probe.rs:225-265`](../../../../tests/ngnet-bench/examples/probe.rs#L225-L265),
[`tests/ngnet-bench/examples/probe.rs:641-656`](../../../../tests/ngnet-bench/examples/probe.rs#L641-L656)).

The supervisor now stops after the first process other than a clean completion, including an
otherwise completed process whose evidence is invalid, dropped or truncated. It retains
bounded pre-readiness records that precede a failure marker, clears them at readiness, and
records host identity beside revision and binary identity
([`tests/ngnet-bench/examples/s9_supervisor.rs:40-161`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L40-L161),
[`tests/ngnet-bench/examples/s9_supervisor.rs:1431-1494`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L1431-L1494),
[`tests/ngnet-bench/examples/s9_supervisor.rs:634-659`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L634-L659),
[`tests/ngnet-bench/examples/s9_supervisor.rs:1169-1178`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L1169-L1178)).

The fresh host-qualified armed 1 MiB canary completed 10/10. The pre-readiness class did not
reproduce, so the declared activation condition for transport capture was false. No warm-up
reset/arm/drain instrumentation and no setup/lifecycle production correction were added.

Qualification then restarted from zero. All 100 supervised 16 KiB processes and all 100
supervised 1 MiB processes completed 125 exact POST/echo exchanges. There were no classified
or unclassified failures, outer kills, cleanup failures, interrupted attempts, resource
guards, invalid records, dropped records or truncation. This satisfies the planned
qualification and resolves S9. It does not change the historical 9/10 denominator in run 04.

## Changes under qualification

| Revision | Change |
| --- | --- |
| `ab0cbea18420` | Typed native warm-up response-head record, first-failure schedule stopping, bounded pre-marker evidence retention |
| `38ac13aadd45` | Supervisor host field added to campaign metadata |
| `fda2fcd1353c` | System-hostname fallback when `HOSTNAME` is not exported |

PR #59's production timer behavior is unchanged. An earlier sleep is still polled before a
later ngtcp2 deadline replaces it, and actual one-shot readiness remains propagated through
both timer polling call sites. This work adds no periodic wake, wake budget or backup sleep.

## Bounded canary sequence

All canaries were distinct append-only invocations. The first two clean observations exposed
reviewed provenance omissions and remain recorded rather than being rewritten:

| Revision | Host metadata | Processes | Completed | Elapsed | Disposition |
| --- | --- | ---: | ---: | ---: | --- |
| `ab0cbea18420` | field absent | 10 | 10 | 873,131 ms | retained observation; review required durable host identity |
| `38ac13aadd45` | `unavailable` | 10 | 10 | 936,208 ms | retained observation; shell hostname was not exported |
| `fda2fcd1353c` | `ubuntu26-dev-vm` | 10 | 10 | 835,217 ms | qualifying canary |

Every process performed 125 exact 1 MiB exchanges in diagnostic mode. Across all three
manifests there were zero failures, outer kills, cleanup failures, interrupted attempts,
guards, invalid/dropped records or truncation. Only the final row carried the complete planned
provenance and selected the next gate.

The qualifying command was:

```sh
S9_REVISION=fda2fcd1353c ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 diagnostic \
  1048576 125 10 685 1 \
  <fda2fcd1353c/initial-armed-1m-canary.manifest>
```

Because that row completed 10/10 without `pre-readiness-response-head`, bounded warm-up
transport capture was not justified. The absence of a recurrence is not reattributed to the
S9 pump fix; it is the predeclared gate for entering final qualification.

## Final reliability qualification

Both schedules used the same reviewed revision and binaries as the qualifying canary.
Reliability mode compiled diagnostics but left them unarmed.

| Body | Processes | Exact exchanges/process | Completed | Elapsed | Approximate one-sided 95% per-process upper bound |
| --- | ---: | ---: | ---: | ---: | ---: |
| 16 KiB | 100 | 125 | 100 | 53,170 ms | 3% |
| 1 MiB | 100 | 125 | 100 | 2,170,810 ms | 3% |

The commands were:

```sh
S9_REVISION=fda2fcd1353c ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 reliability \
  16384 125 100 180 1 \
  <fda2fcd1353c/qualification-reliability-16k.manifest>
S9_REVISION=fda2fcd1353c ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 reliability \
  1048576 125 100 180 1 \
  <fda2fcd1353c/qualification-reliability-1m.manifest>
```

Each result contained `outcome=Completed`, `success_marker=true`, an exchange-125 exact
completion checkpoint, `remaining_pids=[]`, no inspection error, no invalid or dropped records,
no evidence truncation, and `diagnostic_continue=true`. Both summaries recorded 100 completed,
zero failures, `guarded=false` and `unexecuted=0`.

## Auditable files

Raw manifests remain local-only. Their committed hashes and sizes are:

| Manifest | Bytes | SHA-256 |
| --- | ---: | --- |
| `ab0cbea18420/initial-armed-1m-canary.manifest` | 10,391 | `d43e737aa536b7fa552c16b26437e69a80b80bc6a5a5413f76f80fe50d72d31c` |
| `38ac13aadd45/initial-armed-1m-canary.manifest` | 10,406 | `241e15e638f1a82dda83463c9bcfcea81e5b0f530b2e5633ee3610749ec9c19b` |
| `fda2fcd1353c/initial-armed-1m-canary.manifest` | 10,409 | `c58a360aa7151bc68a08106a37efc02b026c7ef190269de2af1d546afb0e0651` |
| `fda2fcd1353c/qualification-reliability-16k.manifest` | 97,874 | `86d341f1950bd112ce556f9207f68357a0c7342be8323b15be04fcee24ea1b12` |
| `fda2fcd1353c/qualification-reliability-1m.manifest` | 98,470 | `aa359c69a467169780815936fe2c138f9e9bc5ba69e57ed9dbe3f727eccb2f46` |

The qualified release binaries were:

| Binary | Bytes | SHA-256 |
| --- | ---: | --- |
| `probe` | 11,310,920 | `44ba5ceaa5f006471a46a565a9bccb370ffc689dd08590479c4259e1252766d8` |
| `s9_supervisor` | 803,760 | `db8caff349f9950aea360073a44dc956479755bed9067891fc90d9794d3ca3fa` |

The two earlier supervisor binaries were
`c9b5a73d093e9e23a9300de545346371db5a29192d0cb06b04d66fc02a6ec4e4`
at 807,096 bytes and
`fcae2d67a9ed82564be688987fa30e8b838bc66d02f59e6190a6e29f96f2f197`
at 807,824 bytes. All canaries used the same probe hash shown above.

## Regression and repository gates

The typed record is pinned exactly, including phase, classifier, false armed state and
percent-encoded newline detail. A forced typed failure proves the warm-up observer runs before
the result. Supervisor tests cover first-failure stopping, unsafe evidence, bounded pre-marker
promotion, readiness clearing and host normalization
([`tests/ngnet-bench/src/lib.rs:292-340`](../../../../tests/ngnet-bench/src/lib.rs#L292-L340),
[`tests/ngnet-bench/examples/probe.rs:1104-1134`](../../../../tests/ngnet-bench/examples/probe.rs#L1104-L1134),
[`tests/ngnet-bench/examples/s9_supervisor.rs:1557-1569`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L1557-L1569),
[`tests/ngnet-bench/examples/s9_supervisor.rs:1795-1863`](../../../../tests/ngnet-bench/examples/s9_supervisor.rs#L1795-L1863)).

The following passed sequentially with `CARGO_BUILD_JOBS=2`:

- probe and supervisor example unit tests with diagnostics;
- release native fixture tests with and without diagnostics;
- `ngnet-quic` all-feature tests;
- release `ngnet-quic-h3` and `ngnet-quic-h3-tests`, including all four zero-allocation/timer
  tests;
- default and all-feature workspace tests;
- all-feature, all-target workspace clippy with warnings denied;
- warning-denying Rust documentation for `ngnet-quic` and `ngnet-quic-h3`;
- release diagnostic example compilation and workspace benchmark smoke;
- touched-file formatting and diff checks.

No concurrent cargo builds or reliability campaigns ran during qualification. No new
production transport correction was made after PR #59.

## Resolution boundary

S9 is resolved because the pre-readiness failure is now classifiable, the fixed fresh armed
canary passed, and both planned 100-process schedules passed every exactness, cleanup,
identity, evidence and resource guard. This is the complete declared denominator, not an
extrapolation from the earlier 10-process observations.

The result does not prove a zero failure rate and does not make larger-body performance
benchmarks eligible on this host. If the typed pre-readiness class appears later, it is a
separate setup/lifecycle occurrence. Its next step remains same-occurrence bounded warm-up
capture, not a periodic wake or an unsupported attribution to the resolved S9 pump/retry seam.
