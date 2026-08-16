# 04 — Handing bodies over, and the SC-005 verdict

**Machine:** [`legacy-dev-host`](README.md)
**Date:** 2026-08-05
**Commit:** #9 — *Hand bodies over to the transport instead of copying them into libnghttp2*
**Cases:** [`transport_shared_body`](../../cases/transport-shared-body.md),
[`shared_body`](../../cases/shared-body.md)
**Command:** `taskset -c 3`
**Repetitions:** ten independent runs of the socket family, three of the duplex family; the
recorded result aggregates paired deltas across them
**Controls:** `hyper-tokio` (untouched), and the untouched `*-push` twin of each transport. The
0-byte point is a second, mechanistic control.
**Exclusions:** any replicate whose 0-byte paired delta exceeded ±5% was discarded whole — a
rule fixed **before** the results were seen. Three of ten socket replicates were excluded,
leaving seven.

## What was being asked

Whether handing bodies to libnghttp2 as the caller's own `Bytes`, serialised with
`NGHTTP2_DATA_FLAG_NO_COPY`, beats copying them into the frame buffer. Each arm is identical to
its twin but for the connection entry point — `handshake_shared_with` versus `handshake_with` —
so a difference within a pair is the body strategy or it is drift.

## Results

Negative is faster. Real socket, seven clean replicates:

| Body | `tokio` shared vs push | `compio` shared vs push |
| --- | --- | --- |
| 0 B | +1.0% (control) | −0.2% (control) |
| 1 KiB | **−35.3%** | −0.9% |
| 64 KiB | **−25.4%** | −3.3% |
| 1 MiB | **−30.6%** | **−4.07%** |

Duplex family, three replicates: 1 KiB −9.2%, 64 KiB −9.7%, 1 MiB **−14.4%**; controls ≤4.9%.
The duplex 1 MiB figure lands almost exactly on the gate's pre-registered ceiling of 14.98% of
protocol CPU for that workload — a prediction made before the code existed, reproduced within
half a point.

Recomputed over **all ten** socket replicates with nothing discarded, `tokio` at 1 MiB is
**−31.10%**, every individual replicate falls between −28.0% and −35.5%, and the conclusion is
unchanged. The exclusion rule only ever mattered to `compio`.

## Drift controls in the same session

Over the same seven replicates:

| Control arm | 0 B | 1 MiB |
| --- | --- | --- |
| `hyper-tokio` (untouched) | 5.05% | 4.56% |
| `tokio-push` (untouched) | 4.33% | 7.22% |
| `compio-push` (untouched) | 15.14% | **34.94%** |

The controls disagree, so the choice of which to judge against was made on evidence and
recorded: in the three replicates where `compio-push` wandered 24–42%, `tokio`'s own 0-byte
control moved at most 4.6% and its 1 MiB result was indistinguishable from the clean runs. The
disturbance was a property of the compio arms, not a session-wide noise floor, so each
transport is judged against the controls on its own transport.

## Supporting counts

Write counts for one upload, push → shared, pinned by
`http_shared_body.rs::handing_a_body_over_collapses_the_write_count_on_the_gathering_path`:
0 B 1→1, 1 KiB 2→1, 64 KiB 5→2, 1 MiB 65→17. `tests/fixtures_move_their_bytes.rs` asserts every
arm echoes every size back at its exact length, so an arm cannot look faster by moving fewer
bytes than its twin.

## What this establishes

- **SC-005 MET on the readiness transport.** −30.6% at 1 MiB, consistent in sign and magnitude
  across all seven clean replicates (−28.0% to −35.5%), against a largest same-transport
  control movement of 7.22% — more than four times the bar. The duplex family corroborates it
  independently, from a separate binary with no compio arm at all.
- **SC-005 NOT MET on the completion transport.** −4.07% at 1 MiB against a 34.94% movement in
  its own untouched control arm. Reported as measured rather than reworded into a win.
- **The dominant mechanism on the readiness path is write-count collapse, not copy removal.**
  The gains are five times larger than the copy alone could explain, and they track the write
  counts above, vanishing exactly at 0 B where the ratio is 1.
- What bounds the batch at 1 MiB is the 64 KiB initial flow-control window, which admits about
  four 16 KiB frames per pass — **not** `MAX_REGIONS`, which is a guard rail here rather than
  the binding constraint.

## What it does not

- It does not settle the compio question. The paired delta is far steadier than the control
  spread — all seven replicates agree on sign and fall in a 2.8–5.4% band, as one expects if
  the wander is a common-mode session effect hitting both arms together — but that is a weaker
  statistical argument than SC-005 specifies. **This is the run most worth repeating on a quiet
  machine.**
- It does not measure concurrency or latency with a handed-over body; only the body sweep was
  run.

Conclusion drawn in
[`../../findings/handing-bodies-over.md`](../../findings/handing-bodies-over.md).

**Superseded on the completion transport.** The NOT MET verdict here failed on `compio-push`'s
34.94% wander, not on its own delta. Re-measured on a host that drifts ~1%
([`../xeon-8370c-azure/03-shared-body.md`](../xeon-8370c-azure/03-shared-body.md)), the compio
delta came out at −4.55% — within half a point of this run's −4.07% — against a 1.92% control
spread, and the verdict is now MET. This run's numbers stand; only what could be claimed from
them has changed.
