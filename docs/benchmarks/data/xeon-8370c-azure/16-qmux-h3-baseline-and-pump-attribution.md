# 16 — PR #45 baseline and pump attribution

**Machine:** historical [`xeon-8370c-azure`](README.md) label; this run reports an **Intel Xeon Platinum 8573C**
**Date:** 2026-08-28
**Commit(s):** `364dbb2c67269b562aa931d0e5c5cf9c332efb86` (merged PR #45)
**Cases:** HTTP/2 and QMux/H3 empty serial, concurrency 1/8/64, and 1 MiB echo over duplex and loopback socket; exact empty-duplex calls/allocations; socket write syscalls for empty, concurrency 64, and 1 MiB; repeated attribution profiles for empty, concurrency 64, and 1 MiB
**Command:** `cargo build --release -p ngnet-bench --benches --example probe`; Criterion as `taskset -c 3 <binary> --bench <filter> --save-baseline <pass> --noplot`; two-point probes as `(c(3N)-c(N))/2N`; DWARF profiles as `sudo perf record -e task-clock -F 4000 -g --call-graph dwarf,4096 --no-buildid -- taskset -c 3 <probe> ...`; socket writes as `strace -c -f -e trace=write,writev,send,sendto,sendmsg taskset -c 3 <probe> <arm> <workload> <param> <N>`
**Repetitions:** three Criterion passes for duplex and two for socket; two task-clock and DWARF profiles per arm/workload; exact counts at 100/300 iterations
**Controls:** unchanged HTTP/2 in every timing pass; across median and slope estimates, H2 spread was 0.05–0.87% on socket and 1.54–10.02% on duplex
**Exclusions:** none. No pass, profile, count, or sample was discarded. Read-only code inspection ran on other cores during DWARF profiles. Criterion pass 2 overlapped small file writes; a third duplex pass was added for that reason, while the two socket passes and all results remain reported. Exact-count runs were otherwise idle

## What was being asked

This run establishes the mandatory fresh `364dbb2` baseline on the current machine before any
candidate code exists. It asks how much QMux/H3 still costs relative to H2 after PR #45, where
that differential belongs, and what exact call/allocation/write counts later candidates must move.
It does not use absolute values from the pre-migration Xeon 8370C as controlled evidence.

## Environment and exact binaries

| Property | Value |
| --- | --- |
| CPU | Intel Xeon Platinum 8573C; 4 cores / 8 threads |
| Memory | 31 GiB |
| Kernel / distribution | `6.17.0-1022-azure` / Ubuntu 24.04.4 LTS; machine index records the older `-1015` kernel |
| Governor / pinned CPU | `performance` / CPU 3 |
| io_uring | available; `/proc/sys/kernel/io_uring_disabled = 0` |
| Toolchain | `rustc 1.98.0` (`rust-toolchain.toml`); machine index records the older 1.97.1 pin |

| Binary | SHA-256 |
| --- | --- |
| `probe` | `0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` |
| `serial_latency` | `294ec76d110f77eeb86b2c222fa27cc22e6462df6cd6e4077bf55231f368776a` |
| `transport_serial_latency` | `1a704657a524a6677a960c36f7d1ba98df66fdced24376ee1481009de9e66c82` |
| `concurrent_throughput` | `f3bed26412b5874711e55a141d2aa31ad255934fba2b713fa9e384e2941d299d` |
| `transport_concurrent_throughput` | `01704c5524a71e2ef0148a7f884cdff412f9fed04f44e5cdbbfb8c1e4cc3f55d` |
| `body_throughput` | `4910af4e89d7b9283ea90fe6611a52d57f4b948f535e3646c5f9f3abc7efbc18` |
| `transport_body_throughput` | `e2325e731a92f39aca2658d063bdd5e12dcb5e60ecc987bb83ae7f9755972af2` |

The first three hashes exactly match run 15's final-side binaries, proving revision continuity.
Before the build, `git rev-parse HEAD` returned `364dbb2c67269b562aa931d0e5c5cf9c332efb86`
and `git status --porcelain` was empty.

## Criterion results

Criterion median point estimates are microseconds; lower is better. Passes are listed in
execution order. The ratio is the median QMux/H3 point estimate divided by the median H2 point
estimate across passes.

| Case | H2 passes | H2 spread | QMux/H3 passes | QMux/H3 spread | QMux/H3 ÷ H2 |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 10.372 / 10.266 / 10.215 | 1.54% | 21.238 / 21.409 / 21.138 | 1.28% | **2.07×** |
| socket serial | 17.855 / 17.980 | 0.70% | 32.606 / 32.946 | 1.04% | **1.83×** |
| duplex concurrency 1 | 11.021 / 11.947 / 10.869 | 9.92% | 21.922 / 22.872 / 21.823 | 4.81% | 1.99× |
| duplex concurrency 8 | 65.606 / 70.216 / 64.926 | 8.15% | 124.323 / 128.794 / 123.626 | 4.18% | 1.89× |
| duplex concurrency 64 | 556.292 / 585.088 / 552.566 | 5.89% | 1004.320 / 1040.638 / 1000.302 | 4.03% | 1.81× |
| socket concurrency 1 | 18.674 / 18.645 | 0.16% | 33.334 / 33.384 | 0.15% | **1.79×** |
| socket concurrency 8 | 73.475 / 73.210 | 0.36% | 134.774 / 134.955 | 0.13% | **1.84×** |
| socket concurrency 64 | 568.576 / 564.404 | 0.74% | 1008.463 / 1003.724 | 0.47% | **1.78×** |
| duplex 1 MiB | 500.253 / 501.401 / 511.716 | 2.29% | 612.769 / 619.262 / 622.719 | 1.62% | 1.24× |
| socket 1 MiB | 1353.819 / 1355.144 | 0.10% | 1160.525 / 1162.567 | 0.18% | **0.86×** |

Criterion's throughput-model slope point estimates were also retained. Their H2 full ranges—the
values used when the gates were pre-registered before median extraction—were duplex
serial/concurrency-1/8/64/1-MiB **2.64% / 10.02% / 7.26% / 5.96% / 2.15%** and socket
**0.16% / 0.05% / 0.33% / 0.87% / 0.23%**. Gates conservatively use the larger applicable
median or slope spread; duplex concurrency remains guard/reporting only.

## Two-point whole-stack cost

Microseconds per completed workload, mean of two `(t(3N)-t(N))/2N` observations.

| Workload | Substrate | H2 | QMux/H3 | Ratio | QMux/H3 − H2 |
| --- | --- | ---: | ---: | ---: | ---: |
| empty serial | duplex | 10.73 | 20.71 | 1.93× | +9.98 |
| empty serial | socket | 17.54 | 31.79 | 1.81× | +14.25 |
| concurrency 64 | duplex | 517.39 | 948.67 | 1.83× | +431.28 |
| concurrency 64 | socket | 523.51 | 958.73 | 1.83× | +435.22 |
| 1 MiB echo | duplex | 508.32 | 607.07 | 1.19× | +98.75 |
| 1 MiB echo | socket | 1243.05 | 1058.51 | **0.85×** | −184.54 |

Repetition spread was 0.02–2.57%.

## Exact empty-duplex counts

Counts are per exchange from `(c(300)-c(100))/200`; raw QMux/H3 totals are shown where captured.
For each release symbol, its file offset was obtained from `nm`/`objdump`, installed with
`echo 'p:ngprobe/<event> <absolute-binary>:0x<offset>' | sudo tee -a
/sys/kernel/tracing/uprobe_events`, and counted with `sudo perf stat -e ngprobe:<events> --
taskset -c 3 target/release/examples/probe qmux-duplex body 0 <N>`. The same two-point command
counted `malloc`/`free`; all uprobe definitions were removed after reduction.

| Operation | H2 | QMux/H3 |
| --- | ---: | ---: |
| transport `poll_read` | 7 | **73** (7,501 / 22,101) |
| `Connection::pump` | — | **70** (7,194 / 21,194) |
| `Connection::write_side` | — | **73** |
| waker clone / drop | 11 / 3 | **71 / 67** |
| H3 `poll_event` | — | **30** |
| H3 `poll_transmit` | — | **14** |
| `Shared::drain_work` | — | **14** |
| forced / join-buffered pump helpers | — | **5 / 9** |
| `EventQueue::pop` / `push` | — | **23 / 7** |
| `poll_next_event_with` | — | **23** |
| reads delivering bytes to dwnx | — | **3** |
| libc `malloc` / `free` | 86 / 86 | **128 / 128** |

Only three of 73 reads deliver bytes. Each of the 70 pumps terminates with one pending read. The
older attribution of 56 pumps to `fill` conflicts with 23 measured queue pops and is not used for
design; phase 2 of this run will append a monomorphisation/inlining-safe source split.

At 1 MiB, QMux/H3 makes **710** allocator calls against H2's **194.5**. Two repeated 1-in-20
`malloc` stack profiles attribute 37.9% to `RawVec::finish_grow`, 22.9% to
`bytes::shallow_clone_vec`, and 22.1% to the QMux stream-data handler: **82.9%** under the
registered delivery-path classifier.

## Fresh exact socket write counts

Counts use `strace -c -f` at 100 and 300 iterations. The probe's two diagnostic `write` calls
cancel; H2 writes with `writev`, QMux/H3 with `sendto`.

| Arm | Workload | raw 100 / 300 | writes per workload |
| --- | --- | ---: | ---: |
| H2 | empty body | 205 / 605 | **2** |
| QMux/H3 | empty body | 308 / 908 | **3** |
| H2 | concurrency 64 | 205 / 605 | **2** |
| QMux/H3 | concurrency 64 | 312 / 922 | **3.05** |
| H2 | 1 MiB body | 18,890 / 56,690 | **189** |
| QMux/H3 | 1 MiB body | 6,708 / 20,108 | **67** |

These fresh counts reproduce the post-run-11 constant concurrency shape and the bulk write
advantage without borrowing absolute timings from the older CPU.
The release transport `poll_write` call is inlined and has no standalone symbol in `probe`;
therefore the table reports the release-visible kernel write primitive rather than claiming a
separate function-entry uprobe count.

## QMux/H3-minus-H2 attribution

Self-cost differences are microseconds per workload iteration from the initial 24 pinned DWARF
profiles plus eight review-time reacquisitions for the two omitted socket columns. Positive means
QMux/H3 is more expensive.

| Layer | empty duplex | empty socket | c64 duplex | c64 socket | 1 MiB duplex | 1 MiB socket |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust HTTP driver | +2.54 | +2.81 | +130.70 | +57.55 | −72.71 | −111.56 |
| runtime/readiness/wakers | +2.07 | +1.60 | +68.36 | +50.79 | −14.69 | −10.98 |
| QMux join/transport | +1.92 | +1.97 | +54.83 | +101.60 | +66.11 | +98.95 |
| C protocol library | +1.25 | +1.38 | +51.96 | +65.69 | +2.50 | +3.52 |
| dwnx transport | +0.77 | +0.73 | +30.26 | +36.33 | +34.72 | +41.00 |
| allocation/memory | +0.29 | +0.37 | +11.79 | +15.37 | +4.74 | +6.07 |
| kernel/vDSO | +0.33 | +3.83 | +10.30 | +16.88 | +13.55 | −220.43 |
| unresolved libc/other | +0.19 | −0.13 | +21.59 | +40.84 | +50.32 | +33.64 |
| fixture/other residual | +0.62 | +1.69 | +51.49 | +50.16 | +14.21 | −24.74 |
| **total differential** | **+9.98** | **+14.25** | **+431.28** | **+435.22** | **+98.75** | **−184.54** |

The two socket columns were freshly re-profiled after review with the same byte-identical probe
(eight additional profiles, H2/QMux × two workloads × two repeats) because their prior reductions
had been deleted before publication. Their explicit residual classifier makes the layer rows sum
to the two-point differential; the original four columns receive the corresponding difference as
`fixture/other residual`. Classifier composition is approximately comparable across acquisitions,
while totals are exact by construction. Profile percentages attribute ownership but never replace
exact call counts.

## Pre-registered candidate gates

These gates were fixed before implementation and do not change with later results. A positive
claim must exceed matching H2-control movement, both sides' full median range, and 2%.

| Candidate | Claim targets | Guards | Required count evidence |
| --- | --- | --- | --- |
| A | socket serial, socket concurrency 1, duplex serial | duplex/socket 1 MiB; socket concurrency 8/64 | reads < 73; pumps < 70; event/transmit/drain = 30/14/14 |
| B | duplex and socket 1 MiB | duplex/socket serial; socket concurrency 1 | allocations < 710; delivery share below 82.9% |
| C | socket and duplex serial | duplex/socket 1 MiB; socket concurrency 8/64 | empty allocations < 128 with reduced-site attribution |
| D | socket serial and socket concurrency 1 | duplex/socket 1 MiB | queue pops < 23; pushes = 7 |

## Drift controls in the same session

| Control | Full median range |
| --- | ---: |
| H2 duplex serial | median 1.54%; slope 2.64% |
| H2 duplex concurrency 1 / 8 / 64 | median 9.92% / 8.15% / 5.89%; slope 10.02% / 7.26% / 5.96% |
| H2 duplex 1 MiB | median 2.29%; slope 2.15% |
| H2 socket serial | median 0.70%; slope 0.16% |
| H2 socket concurrency 1 / 8 / 64 | median 0.16% / 0.36% / 0.74%; slope 0.05% / 0.33% / 0.87% |
| H2 socket 1 MiB | median 0.10%; slope 0.23% |

## Validation

- `cargo test -p ngnet-qmux-h3 -p ngnet-qmux-h3-tests --release`: passed.
- `cargo bench -p ngnet-bench -- --test`: passed every duplex/socket arm; compio obtained io_uring.

## What this establishes

- On the current Xeon 8573C, QMux/H3 is 1.78–2.07× H2 for empty/concurrent cases, 1.24× for
  duplex 1 MiB, and 0.86× for socket 1 MiB.
- Empty-exchange amplification remains exact: 14 productive passes produce 30 event polls,
  70 pumps, 73 reads, and 138 waker clone/drop operations; only three reads deliver bytes.
- Bulk delivery remains allocation-heavy: 710 versus 194.5 calls, with 82.9% of sampled QMux/H3
  stacks in the registered delivery-path classifier.
- Socket writes remain about three per empty/concurrent QMux/H3 workload and 67 per 1 MiB,
  against H2's two and 189 respectively.
- The candidate gates and same-machine baseline are fixed before implementation.

## What it does not

- It does not claim that any of the remaining 70 pumps or pending reads is redundant.
- It does not use the instrument-limited 56-pump `fill` attribution for design; the corrected
  source split is phase 2.
- It does not measure real-network or OpenSSL-dependent QUIC performance.
- It does not make a controlled absolute comparison with a Xeon 8370C run.
