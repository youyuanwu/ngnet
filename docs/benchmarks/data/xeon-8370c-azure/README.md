# `xeon-8370c-azure`

**Status: current.** The machine benchmarks are being collected on from 2026-08-16.

**First run:** 2026-08-16 · **Last run:** 2026-08-17

## Hardware and system

| | |
| --- | --- |
| CPU | Intel Xeon Platinum 8370C @ 2.80 GHz (Ice Lake-SP) |
| Cores / threads | 4 cores, 8 threads, one socket, one NUMA node |
| Memory | 31 GiB |
| Virtualised | yes — Azure, Microsoft hypervisor |
| Kernel | `6.17.0-1015-azure` |
| Distribution | Ubuntu 24.04.4 LTS |
| io_uring | available — `/proc/sys/kernel/io_uring_disabled` is `0`, and both compio arms obtained `DriverType::IoUring` |
| Frequency scaling | `performance` governor, `intel_pstate` with `no_turbo=0`. Turbo is **not** disabled, and a guest cannot pin the frequency, so some drift is expected. |
| Rust toolchain | 1.97.1, from `rust-toolchain.toml` |

**A shared virtual machine is not a benchmark rig.** Turbo cannot be disabled from inside the
guest, the host's other tenants are invisible from here, and hyper-threading means a "pinned"
core is half a physical one. Nothing on this host will produce trustworthy absolute figures;
what it can produce is paired deltas against drift controls measured in the same session, which
is what [`../../controls.md`](../../controls.md) requires anyway. Record the controls' movement
in every run.

## Measurement conditions

- **Pinning:** `taskset -c 3`, matching the legacy host's convention. Sibling thread 7 shares
  the physical core, so keep the machine otherwise idle.
- **Otherwise idle?** To be recorded per run. Runs 01–03 had the machine to themselves, and
  `04` records the same for its two passes. `05` and `06` do **not** state it, so it should not
  be assumed of them: their control movements — 0.73% mean for `05`, 1.06% mean and 4.47% worst
  for `06` — are the only evidence available about how quiet the host was, and they are the
  right thing to size those runs' deltas against rather than the 1% figure below.
- **Observed drift — the important number. About 1%.** Across 78 benchmarks run twice with no
  code change between, median |drift| was **0.90%**, mean 1.23%, and only one benchmark
  exceeded 5%. See [01-drift-baseline](01-drift-baseline.md) — that is the bar every result
  from this host is sized against, and it is roughly an order of magnitude tighter than the
  legacy host's.

  The one exception worth remembering: `shared_body/hyper-tokio/1048576` moved **+11.48%**
  between identical passes. A duplex 1 MiB hyper figure needs replication here.

## Arms this machine can run

All eight bench targets. `cargo bench -p ngnet-bench -- --test` completes with `Success` for
every arm, including `compio-push` and `compio-shared`, which abort unless they obtain
`DriverType::IoUring` — so io_uring is genuinely available here and not silently falling back
to a polling driver.

`ngnet-bench` needs the `deps/nghttp2`, `deps/nghttp3` (with its nested `lib/sfparse`) and
`deps/dwnx` submodules, and it still needs **neither `deps/ngtcp2` nor OpenSSL ≥ 3.5**, which
belong to `ngnet-quic-sys` and are reached by no arm in the suite: `cargo bench -p ngnet-bench`
builds with the pinned toolchain, a C compiler, CMake and libclang, plus those three
submodules. [`../../running.md`](../../running.md) states the requirement in full.

> **Editorial note, 2026-08-17.** The paragraph above previously read that `ngnet-bench`
> "depends on `ngnet-h2` alone, so it needs neither the ngtcp2/nghttp3 submodules nor
> OpenSSL ≥ 3.5". That was true of runs 01–03 and of every measurement recorded on this page,
> and it stopped being true when the HTTP/3-over-QMux arms were added: the crate now compiles
> nghttp3 and dwnx as well. The OpenSSL half of the claim is unchanged and still holds. The
> paragraph is corrected rather than annotated in place because it describes what a reader has
> to install *today* to reproduce anything here, and a stale prerequisite list is a build
> failure rather than a historical curiosity. Nothing else on this page has been touched, and
> in particular no run, number or verdict below has been altered — runs 01–03 were taken
> before the QMux arms existed and their submodule requirement really was the smaller one.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| [01-drift-baseline](01-drift-baseline.md) | 2026-08-16 | `e75118e` | Two identical passes: what an unchanged arm does here — **~1%** |
| [02-first-survey](02-first-survey.md) | 2026-08-16 | `e75118e` | Where the arms stand, which legacy conclusions carried over, and where this stack beats hyper |
| [03-shared-body](03-shared-body.md) | 2026-08-16 | `e75118e` | Handing bodies over, five replicates — **settled the compio verdict** |
| [04-qmux-drift-baseline](04-qmux-drift-baseline.md) | 2026-08-17 | `524fa54` | Drift for the QMux arms, which `01` predates — socket **0.67%**, duplex **1.55%** |
| [05-qmux-delivery-aliasing](05-qmux-delivery-aliasing.md) | 2026-08-17 | `223960d` against `9f97334` | A two-orders-of-magnitude allocation cut that was **2.5–4.8% slower** — reverted |
| [06-qmux-write-path](06-qmux-write-path.md) | 2026-08-17 | `524fa54` against `a54ea43` | The write-path set end to end — **−30% at 1 MiB**, −8.5% at concurrency 64 on a socket |

Still outstanding, in the order they are worth doing:

1. **Force `TokioWriter::is_write_vectored` to `false` and re-run
   `transport_concurrent_throughput`.** This is the direct test of the mechanism the whole
   write-path finding rests on, and the one thing
   [02-first-survey](02-first-survey.md) could not settle from a standing survey.
2. **A cross-protocol run with the QMux arms in it.** Runs `04` to `06` all contain those arms
   and none of them compares one to an HTTP/2 arm, because each is a paired build comparison in
   which the HTTP/2 arms are controls. The open question that needs it is whether the QMux-to-
   HTTP/2 ratio still grows with concurrency over a socket now that the write count no longer
   does; `docs/qmux-h3/pending-work.md` carries the lead and what would settle it. This is
   worth doing before item 3, because it is the only outstanding run that can close an entry
   rather than confirm one.
3. **Replicate the duplex 1 MiB arms**, the only place this host is noisy — and note that `04`
   sharpens this: `body_throughput/ngnet-qmux-h3/1048576` drifts **10.42%**, the worst
   identifier in the suite, so the QMux arm needs this more than the others do.
4. Everything else listed under *What a new machine should reproduce* in
   [`../../findings/`](../../findings/).
