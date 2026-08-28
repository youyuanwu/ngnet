# 12 — Reprofiling `apply_events` after the closed-stream and flush fixes

**Machine:** historical [`xeon-8370c-azure`](README.md) VM label; this run's `perf` header
reported an **Intel Xeon Platinum 8573C**, so absolute comparisons with runs 09–11 are not
controlled hardware A/B comparisons

**Date:** 2026-08-28

**Commit(s):** `700bfa6fdb96cc5fe25991ad42da4956941e7957` exactly, the merge result after
the changes measured in runs 10 and 11

**Cases:** the single-arm `tests/ngnet-bench/examples/probe.rs` driver, QMux/H3 empty
serial exchanges over a duplex and loopback socket, plus duplex concurrency 64; allocation
shape was also counted at concurrency 8

**Command:** `cargo build --example probe -p ngnet-bench --release`, followed by the exact
sampling and uprobe commands below

**Repetitions:** two 150,000-exchange serial profiles per substrate and two 3,000-batch
concurrency-64 profiles. The serial profiles contained about 14,000 duplex samples and 21,000
socket samples per pass; concurrency 64 contained about 12,000 samples per pass
An additional representative pass per workload used the same commands after review solely to
count address-only symbols; it did not replace the two attribution passes.

**Allocation counts:** exact uprobes on the release binary's `RawVec::grow_one` call sites,
reduced as `(count(3N) - count(N)) / 2N`; serial used N=1,000 and concurrency used N=100

**Controls:** none. This is attribution of one unchanged revision, not a timing comparison.
The two serial sampling passes are the stability check

**Exclusions:** none. `kernel.perf_event_paranoid` was temporarily changed from 4 to 1 for
sampling and restored to 4; all probe events were removed. No source instrumentation was added

### Exact commands

```sh
cargo build --example probe -p ngnet-bench --release
sudo sysctl kernel.perf_event_paranoid=1

perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-duplex-dwarf.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex body 0 150000
perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-duplex-dwarf-2.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex body 0 150000
perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-socket-dwarf.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-socket body 0 150000
perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-socket-dwarf-2.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-socket body 0 150000
perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-conc64-dwarf.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 64 3000
perf record -F 4000 -g --call-graph dwarf,4096 --no-buildid \
  -o /tmp/paw-ae-conc64-dwarf-2.data -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 64 3000
```

`objdump -d -C` and `readelf -lW` identified the release binary's grow call instructions and
the executable segment's `0x1000` virtual-address/file-offset difference. The exact file-offset
probes were:

```sh
sudo perf probe -x "$PWD/target/release/examples/probe" \
  --add 'ae_duplex_client_data=0x13bebe' \
  --add 'ae_duplex_server_data=0x13c512' \
  --add 'ae_duplex_server_unheard=0x13c5c1' \
  --add 'ae_duplex_events_poll=0x142054' \
  --add 'ae_duplex_events_pushback=0x141f5d' \
  --add 'ae_socket_client_data=0x13cf2e' \
  --add 'ae_socket_server_data=0x13d582' \
  --add 'ae_socket_server_unheard=0x13d631' \
  --add 'ae_socket_events_poll=0x142b14' \
  --add 'ae_socket_events_pushback=0x142a1d'

sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex body 0 1000
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex body 0 3000
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-socket body 0 1000
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-socket body 0 3000
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 8 100
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 8 300
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 64 100
sudo perf stat -x, -e 'probe_probe:ae_*' -- \
  taskset -c 3 ./target/release/examples/probe qmux-duplex concurrent 64 300

sudo perf probe --del 'probe_probe:ae_*'
sudo sysctl kernel.perf_event_paranoid=4
```

The report commands were:

```sh
# Flat layer ownership and self cost.
perf report -i <data-file> --stdio -g none --no-children \
  --percent-limit 0 --sort dso,symbol --demangle

# Inclusive target/callee cost.
perf report -i <data-file> --stdio -g none --children \
  --percent-limit 0 --sort dso,symbol --demangle

# Sample count and task-clock event total.
perf report -i <data-file> --stdio -g none --no-children \
  --percent-limit 99

# Discover the target symbols, call instructions, and load-segment offset used above.
nm -C target/release/examples/probe
objdump -d -C target/release/examples/probe
readelf -lW target/release/examples/probe
```

For layer attribution, each flat `perf report` row was assigned by its outer symbol:
`ngnet_h3`/`nghttp3`, `ngnet_qmux`, `dwnx`, `tokio`, libc, kernel/vDSO, or other. The printed
two-decimal percentages were summed per class and the two serial/concurrency passes averaged.
Address-only rows—symbols beginning `0x` in any DSO—were also summed separately for the
unresolved-symbol audit. Absolute microseconds are
`task-clock event total / completed workload units × inclusive percentage`, rounded to three
decimals from the percentages printed by `perf report`.

## What was being asked

Run 09 attributed 2.39 microseconds, 8.1% of an empty exchange, to
`Driver::apply_events` across both roles. That number predates both the constant-time
closed-stream lookup and QMux flush decoupling, so it is not an acceptance baseline for a new
change.

The decision was made before looking at this run: reuse scratch storage only if the fresh
profile still showed a material timed hotspot. Allocation reduction alone was not sufficient,
because [run 05](05-qmux-delivery-aliasing.md) already found a much larger allocation reduction
that made elapsed time 2.5–4.8% worse.

## Layer attribution

Flat `perf` samples were classified by the outer function's owning layer. The serial columns
are the mean of two profiles. “Other” includes standard-library, benchmark-fixture, address-only,
and unresolved samples.

| Layer | duplex serial | socket serial | duplex concurrency 64 |
| --- | ---: | ---: | ---: |
| `ngnet-h3` Rust driver | 26.31% | 19.07% | 19.87% |
| `nghttp3` C library | 10.18% | 7.57% | 11.88% |
| QMux Rust transport and join | 15.15% | 10.32% | 10.43% |
| `dwnx` C framing | 3.90% | 2.62% | 3.42% |
| tokio | 17.26% | 10.45% | 12.11% |
| libc allocation and memory functions | 10.82% | 8.23% | 24.15% |
| kernel and vDSO | 2.35% | 26.78% | 1.53% |
| other and unresolved | 14.22% | 14.15% | 17.23% |

The columns total 100.19%, 99.19%, and 100.62%, rather than exactly 100%, because `perf report`
prints every one of the long-tail per-symbol percentages to two decimal places before they are
classified and summed. The table preserves those displayed values rather than normalizing away
the aggregate rounding error.

The sampled task-clock cost was 24.857 and 24.632 microseconds per duplex exchange, and
36.283 and 35.840 microseconds per socket exchange. Concurrency 64 cost 1,016.583 and
1,042.083 microseconds per batch, or 15.884 and 16.283 microseconds per exchange. These are
profiler task-clock figures, not Criterion latency measurements.

## `apply_events` attribution

The inclusive percentage includes callees such as `read`, stream failure/close handling, and
allocator work reached while the function is active. It is therefore an upper bound on what
reusing the function's local `data` and `unheard` vectors could remove. `take_events` is shown
separately because its owned event vector is the third proposed scratch buffer.

| Workload | pass | `apply_events` inclusive | self / callees | absolute inclusive | `take_events` inclusive |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 1 | 1.04% | 0.39% / 0.65% | 0.259 µs/exchange | 0.49% |
| duplex serial | 2 | 1.05% | 0.49% / 0.56% | 0.259 µs/exchange | 0.43% |
| socket serial | 1 | 0.74% | 0.35% / 0.39% | 0.268 µs/exchange | 0.70% |
| socket serial | 2 | 0.78% | 0.36% / 0.42% | 0.280 µs/exchange | 0.75% |
| duplex concurrency 64 | 1 | 1.89% | 0.34% / 1.55% | 0.300 µs/exchange | 0.39% |
| duplex concurrency 64 | 2 | 1.86% | 0.32% / 1.54% | 0.303 µs/exchange | 0.39% |

Serial `apply_events` attribution reproduced within 0.01 percentage point on the duplex and
0.04 point on the socket. Its absolute inclusive cost is also nearly constant across the three
workloads: 0.259–0.303 microseconds per exchange. Even adding all of `take_events` produces an
upper bound of 1.48–1.53% on the serial duplex, 1.44–1.53% on the serial socket, and 2.28% at
duplex concurrency 64. Scratch reuse could remove only part of those bounds because event
dispatch, reads, and transport polling still have to occur.

Those bounds do include both sides of the measured scratch lifecycle. `take_events` allocates
the owned event vector, which is consumed and dropped within `apply_events`; the local `data`
vector is allocated and dropped there as well. `unheard` is returned to the run loop, but it
never allocated in these workloads. Allocation and deallocation time therefore do not escape
the combined bound.

## Exact scratch allocation shape

The release compiler leaves separate `RawVec::grow_one` call sites for the event vector's
pushback and transport-poll paths, the client/server `data` vectors, and the server's `unheard`
vector. Uprobes at those call instructions count actual capacity growth, not calls to
`apply_events`. Each non-zero-size growth invokes allocation or reallocation.

Allocation cells below are per batch, with the per-exchange value in parentheses where a batch
contains more than one exchange.

| Workload | event pushback | event poll | client `data` | server `data` | server `unheard` | total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| duplex serial | 4 | 5 | 1 | 2 | 0 | **12** |
| socket serial | 4 | 5 | 1 | 2 | 0 | **12** |
| duplex concurrency 8 | 3 (0.375) | 38 (4.750) | 8 (1.000) | 4 (0.500) | 0 | **53 (6.625)** |
| duplex concurrency 64 | 3 (0.047) | 279 (4.359) | 64 (1.000) | 14 (0.219) | 0 | **360 (5.625)** |

The serial raw counts were 4,007/12,007 for event pushback, 5,013/15,013 for duplex
event-poll growth (5,014/15,014 on the socket), 1,003/3,003 for client data,
2,004/6,004 for server data, and zero for unheard resets. Concurrency counts used the same
two-point subtraction: at 8 they were 307/907, 3,813/11,413, 803/2,403, and 404/1,204;
at 64 they were 307/907, 27,913/83,713, 6,403/19,203, and 1,404/4,204.
The attached `unheard` probe reported 0/0 at N/3N for serial, concurrency 8, and concurrency 64.
Its liveness is established by successful attachment in the same `perf stat` group as the
non-zero neighboring grow probes; zero means that measured workload never took the
reset-before-head growth path.

The allocation mechanism is real and frequent. It is not, however, a material timed hotspot
on this revision: three serial growths occur in `apply_events`, whose inclusive cost is about
a quarter microsecond per exchange, while all twelve proposed scratch growths sit in the
combined `take_events` plus `apply_events` path, whose inclusive serial bound is
0.365–0.548 microseconds per exchange.

## Comparison with run 09

| Profile | flat/self, both roles | inclusive, both roles |
| --- | ---: | ---: |
| run 09, `dc922be` | 2.39 µs, 8.1% | not recorded |
| run 12, `700bfa6`, duplex serial | 0.097–0.121 µs, 0.39–0.49% | 0.259 µs, 1.04–1.05% |
| run 12, `700bfa6`, socket serial | 0.127–0.129 µs, 0.35–0.36% | 0.268–0.280 µs, 0.74–0.78% |
| run 12, `700bfa6`, duplex concurrency 64 | 0.052–0.054 µs/exchange, 0.32–0.34% | 0.300–0.303 µs/exchange, 1.86–1.89% |

Run 09 used flat symbol attribution and did not record call graphs, while run 12 deliberately
records both flat/self and inclusive cost. Comparing run 09's 2.39 microseconds with run 12's
inclusive quarter microsecond is conservative: the like-for-like current self cost is only
0.097–0.121 microseconds on the duplex. The old number is stale by more than an order of
magnitude on that basis.

The underlying Azure VM reported a different CPU model for this run, so the table is not
evidence that runs 10 and 11 alone caused the full absolute reduction. The current-revision
values stand independently as the baseline that matters for deciding whether to optimize now.

## What this establishes

Do not implement scratch reuse from this evidence. The fresh `apply_events` hotspot is at most
1.05% of a serial exchange and 1.89% at concurrency 64 inclusive of work that reuse cannot
remove. Including all of `take_events` makes the whole proposed scratch path at most 1.53%
serial and 2.28% at concurrency 64, but still does not make that a direct removable-time
measurement: polling and event collection remain required. The profile can bound the
opportunity but cannot isolate a precise scratch-only duration. Because reuse could remove only
a strict subset of this already-small inclusive path, the required ownership, early-error
cleanup, same-batch reset replay, two-sweep order, and bounded-retention policy are not
justified without stronger direct timing evidence.

No Criterion before/after run was performed because there is no “after” implementation: the
profile-first gate rejected the optimization before code was changed. This is the intended
outcome of the gate, not missing acceptance data.

This run does not revise the durable cross-protocol finding: it profiles only the QMux/H3 arm,
on migrated hardware, and cannot recompute the HTTP/3-versus-HTTP/2 layer gap from run 09. It
updates the implementation backlog whose priority depended directly on the stale
`apply_events` sub-attribution.

## What it does not

- The host reports Xeon 8573C although this historical result directory is named for the
  original 8370C host. Percentages and within-run repeats are usable; strict absolute
  comparison to old runs is not.
- The broad `other and unresolved` layer bucket combines Rust support, fixture work, and
  address-only symbols. A separate representative-pass audit counts address-only samples alone
  at **8.40% duplex serial, 6.48% socket serial, and 21.18% duplex concurrency 64**. Most are
  unresolved libc/vDSO offsets. DWARF call chains resolve `apply_events`, `take_events`, and
  their named callees directly, so those shares do not hide samples charged to the target.
- Sampling cannot identify which instruction inside an allocator benefited a particular
  vector. Exact call-site uprobes supply the allocation counts; inclusive symbol attribution
  supplies the time bound.
- At approximately 14,000 samples, a one-percent symbol has roughly 140 samples, so the
  two-decimal percentages have about 0.08 percentage point of simple counting uncertainty.
  The repeat agreement is a stability check, not precision below that resolution. The verdict
  relies on the whole path remaining near one percent serial and below 2.3% including
  `take_events` at concurrency, not on the hundredth-place digits.
- Loopback, tokio, current-thread runtime only. The QUIC join and other executors were not
  profiled.
