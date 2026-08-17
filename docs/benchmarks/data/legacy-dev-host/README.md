# `legacy-dev-host`

**Status: retired and unavailable.** Every benchmark figure recorded in this repository before
2026-08-16 was taken here, and none of it can be re-run on this host.

**First run:** 2026-08-03 · **Last run:** 2026-08-05

## Hardware and system

| | |
| --- | --- |
| CPU | not recorded |
| Cores / threads | not recorded; benchmarks were pinned to core 3, so at least four |
| Memory | not recorded |
| Virtualised | not recorded |
| Kernel | not recorded — Linux, and new enough for io_uring |
| Distribution | not recorded |
| io_uring | available; the compio arms assert `DriverType::IoUring` and abort otherwise, and they ran |
| Frequency scaling | not recorded, and **not** disabled — see drift below |
| Rust toolchain | `rust-toolchain.toml` as of the run's commit |

The gap in this table is the reason it exists. The numbers below were recorded without the
host that produced them ever being written down, which is precisely why they cannot be
compared with anything measured since, and why this file is the shape every future machine
directory follows.

## Measurement conditions

- **Pinning:** `taskset -c 3` on the socket family. The duplex family was not always pinned.
- **Otherwise idle?** Shared machine. Not exclusively idle, and known to have neighbours.
- **Observed drift — the important number.** Unchanged control arms moved **5–15% within a
  single session**, and in one shared-body session an untouched `compio-push` arm wandered
  **34.94%**. Both stacks once moved together by ~15% between two runs minutes apart.

Every method rule in [`../../controls.md`](../../controls.md) was learned here, from that
drift: interleave the sides of an A/B, carry unchanged arms as controls, prefer a mechanistic
control where the mechanism allows one, replicate, and fix the exclusion rule in advance.

## Arms this machine could run

All eight bench targets, including both compio arms on io_uring.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| [01-three-arm-baseline](01-three-arm-baseline.md) | 2026-08-03 | `c8dd79c` (#6) | The three socket arms, before gathering existed |
| [02-gathering-path](02-gathering-path.md) | 2026-08-04 | `c8dd79c` vs the gathering branch (#7) | Gathering against the per-block drain |
| [03-coalescing-buffer-reuse](03-coalescing-buffer-reuse.md) | 2026-08-04 | #8 | Reusing the coalescing buffer instead of rebuilding it |
| [04-shared-body](04-shared-body.md) | 2026-08-05 | #9 | Handing bodies over, and the SC-005 verdict |

## Reading anything in this directory

1. **Do not tabulate these figures with numbers from another machine.** Not in a table, not in
   a sentence, not as a "roughly".
2. **Paired deltas travel; absolutes do not.** Where a run reports a delta against a control
   measured in the same session, that comparison is still evidence. A standalone microsecond
   figure from here is a historical curiosity.
3. **Everything here is awaiting reproduction.** Each finding states what a new machine should
   reproduce, in a form that can fail.
