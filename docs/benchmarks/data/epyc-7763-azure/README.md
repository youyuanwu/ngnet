# `epyc-7763-azure`

**Status:** current
**First run:** 2026-09-02
**Last run:** 2026-09-03

A different machine from [`xeon-8370c-azure`](../xeon-8370c-azure/), not a re-labelling of it.
Different vendor, different core count, different generation. **No absolute figure recorded on
that host is comparable with anything here**, and neither are its drift thresholds; they were
calibrated against a machine that no longer exists in this project's hands.

## Hardware and system

| | |
| --- | --- |
| CPU | AMD EPYC 7763 64-Core Processor (base clock not exposed under the hypervisor; BogoMIPS 4890.86) |
| Azure VM size | `Standard_D4as_v5`, region `westus2` |
| Cores / threads | 4 vCPU = 1 socket x 2 cores x 2 threads. `cpu0`/`cpu1` are SMT siblings on core 0; `cpu2`/`cpu3` on core 1 |
| Cache | L1d/L1i 64 KiB x2, L2 1 MiB x2, L3 32 MiB (1 instance) |
| Memory | 15.9 GB |
| NUMA | 1 node; node0 = cpus 0–3, 15933 MB |
| Virtualised | yes, Microsoft hypervisor (AMD-V) |
| Kernel | `7.0.0-1012-azure` |
| Distribution | Ubuntu 26.04.1 LTS |
| io_uring | available — `/proc/sys/kernel/io_uring_disabled` is `0` |
| Frequency scaling | **governor not exposed.** `/sys/devices/system/cpu/cpu0/cpufreq/` does not exist on this VM, so neither the governor nor turbo state can be read or pinned. Frequency behaviour here is the hypervisor's business and is not observable from inside |
| Rust toolchain | 1.98.0 (`rust-toolchain.toml`); `rustc 1.98.0 (88d9e12ae 2026-08-18)`, `cargo 1.98.0 (797e8a9bc 2026-08-05)` |
| OpenSSL | 3.5.5 (27 Jan 2026) — above the 3.5.0 the ngtcp2 arms require |

## Measurement conditions

- **Pinning:** `taskset -c 3`, following the convention in [`../../running.md`](../../running.md).
  Note that `cpu3`'s SMT sibling is `cpu2` and was not isolated, so a pinned process here shares
  a physical core with whatever else the scheduler puts on `cpu2`.
- **Otherwise idle? No, and this is the defining property of this host.** It runs a Kubernetes
  control plane (`kube-apiserver`, `kubelet`, `etcd`, `containerd`) plus two unrelated
  processes consuming roughly 45% CPU each, all predating the measurement session. Load average
  stayed between 1.9 and 5.4 throughout and never approached the < 1.0 that run 01's
  pre-registered conditions required.
- **Observed drift:** the single most useful number here, and on this host it is disqualifying.
  A carried unchanged control arm (`h3-qmux-duplex`, 200 x 1 KiB) moved from **18.52 ms to
  4.39 ms** across one session — a factor of **4.2**. Any effect smaller than that is not
  measurable on this machine under these conditions, and effects of interest in this workspace
  are far smaller than that.

**Consequence: this host cannot currently produce a comparative timing result.** It can compile
and run every arm, and it is useful for correctness and liveness work — run 01 found a real
adapter defect precisely because it ran a repeated workload here, and run 02 confirmed the fix
for it and surfaced the native stack's own remaining one. It is not useful for deciding which of
two stacks is faster until it can be quiesced. Nothing in run 02 changes that: it counts
completions, not microseconds.

## Arms this machine can run

`cargo bench -p ngnet-bench -- --test` was not run to completion as a whole-suite check. The
arms exercised individually, all successfully:

- `quic_stack_h3_serial_latency` — both arms, `--test`: Success
- `quic_stack_h3_body_throughput` — both arms, `--test`: Success
- `quic_stack_serial_latency` — all three pre-existing arms, `--test`: Success
- `probe` arms `h3-ngnet-quic`, `ngnet-quic-h3-matched`, `h3-qmux-duplex`

The compio arms' `DriverType` was **not recorded**; io_uring is enabled on the host but no compio
arm was run in this session.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| [01-h3-ngnet-quic-comparison](01-h3-ngnet-quic-comparison.md) | 2026-09-02 | `6119972` | Hyperium H3 and ngnet H3 over the same ngtcp2 transport — inconclusive, and an adapter defect found |
| [02-h3-ngnet-quic-fin-fix](02-h3-ngnet-quic-fin-fix.md) | 2026-09-02 | `feature/h3-ngnet-quic` | The lost FIN — root cause of run 01's defect, and reliability after the fix. **Reliability only; no timing claimed** |
| [03-native-h3-s9-timer-wake](03-native-h3-s9-timer-wake.md) | 2026-09-03 | `088e6c0` | Native large-body S9 — reproduced timer-wake stall and final 100-process qualification. **Reliability only; no timing claimed** |
