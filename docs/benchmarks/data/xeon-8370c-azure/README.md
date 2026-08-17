# `xeon-8370c-azure`

**Status: current.** The machine benchmarks are being collected on from 2026-08-16.

**First run:** 2026-08-16 · **Last run:** 2026-08-16

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
- **Otherwise idle?** To be recorded per run. Runs 01–03 had the machine to themselves.
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

`ngnet-bench` depends on `ngnet-h2` alone, so it needs neither the ngtcp2/nghttp3
submodules nor OpenSSL ≥ 3.5 that the QUIC crates require: `cargo bench -p ngnet-bench`
builds with the pinned toolchain, a C compiler, CMake and libclang, plus the `deps/nghttp2`
submodule that `ngnet-h2-sys` compiles.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| [01-drift-baseline](01-drift-baseline.md) | 2026-08-16 | `e75118e` | Two identical passes: what an unchanged arm does here — **~1%** |
| [02-first-survey](02-first-survey.md) | 2026-08-16 | `e75118e` | Where the arms stand, which legacy conclusions carried over, and where this stack beats hyper |
| [03-shared-body](03-shared-body.md) | 2026-08-16 | `e75118e` | Handing bodies over, five replicates — **settled the compio verdict** |

Still outstanding, in the order they are worth doing:

1. **Force `TokioWriter::is_write_vectored` to `false` and re-run
   `transport_concurrent_throughput`.** This is the direct test of the mechanism the whole
   write-path finding rests on, and the one thing
   [02-first-survey](02-first-survey.md) could not settle from a standing survey.
2. **Replicate the duplex 1 MiB arms**, the only place this host is noisy.
3. Everything else listed under *What a new machine should reproduce* in
   [`../../findings/`](../../findings/).
