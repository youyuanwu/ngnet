# `<machine-id>`

<!-- Copy to data/<machine-id>/README.md and fill in. Write `not recorded` for anything that
     was not captured at the time, rather than guessing it later. -->

**Status:** current | retired
**First run:** YYYY-MM-DD
**Last run:** YYYY-MM-DD

## Hardware and system

| | |
| --- | --- |
| CPU | model, base clock |
| Cores / threads | |
| Memory | |
| Virtualised | yes/no, hypervisor |
| Kernel | `uname -r` |
| Distribution | |
| io_uring | available? `/proc/sys/kernel/io_uring_disabled` |
| Frequency scaling | governor, turbo on/off |
| Rust toolchain | from `rust-toolchain.toml` at the time |

## Measurement conditions

- **Pinning:** which core, via `taskset -c N`.
- **Otherwise idle?** what else was running.
- **Observed drift:** how far an unchanged control arm moves between runs on this host. This
  is the single most useful number in this file, because it is the bar every result on this
  machine has to clear. Fill it in from the first run and keep it updated.

## Arms this machine can run

Record the output of `cargo bench -p ngnet-bench -- --test`, and in particular whether the
compio arms obtained `DriverType::IoUring`. An arm that cannot run here is not a missing
result, it is a property of the host.

## Runs

| Run | Date | Commit | Subject |
| --- | --- | --- | --- |
| | | | |
