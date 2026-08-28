# 09 — Why is QMux slower than HTTP/2, and why is it faster at a megabyte over a socket?

**Machine:** [`xeon-8370c-azure`](README.md)
**Date:** 2026-08-27
**Commit(s):** `dc922be` — a single sha. Both stacks are measured as they stand; nothing is
compared against an earlier build
**Cases:** not Criterion. A purpose-built single-arm driver, `tests/ngnet-bench/examples/probe.rs`,
which establishes one fixture and runs one workload in a loop so that a profiler and a syscall
tracer can be pointed at exactly one arm. Criterion's process carries every arm at once and
cannot be attributed
**Command:**
`cargo build --example probe -p ngnet-bench --release`, then per measurement
`taskset -c 3 ./target/release/examples/probe <arm> <workload> <param> <iters>` under
`strace -c -f`, `perf record -F 4000`, or `perf stat -e <uprobe>`
**Repetitions:** counts are two-point — every count is taken at N and 3N iterations and reported
as `(c(3N) − c(N)) / 2N`, so connection setup and warm-up cancel exactly rather than being
amortised away. Timings are five runs of 150,000 exchanges (3,000 for concurrency), pinned to
core 3, reported as the median with the full range
**Controls:** none, and the usual kind does not apply — this run reports **counts**, which are
integers reproducible run to run, not timings competing with session drift. Where a timing is
quoted it is an A/B against the same binary with one constant changed, both sides measured in the
same session, and both ranges are given so the reader can see they do not overlap. The one place
a control matters is the check that the driver reproduces the suite: it does, to within 8% on
absolute cost and to within 3% on the ratio ([below](#does-the-driver-reproduce-the-suite))
**Exclusions:** none. No measurement was discarded

## What was being asked

[`08`](08-qmux-against-h2.md) established *that* HTTP/3 over QMux costs 2.6–2.9× an HTTP/2
exchange with no body, 1.31× per byte on a duplex and 0.86× per byte over a socket, and it closed
by naming two things it could not explain: why QMux moves bulk bytes more cheaply through the
kernel, and why concurrency is the one workload whose ratio gets *worse* when a kernel is added.
Both were left as arithmetic on timings. This run asks for mechanisms instead, and answers with
counts: how many writes, how many reads, how many polls, and where the processor time goes by
layer.

## Does the driver reproduce the suite?

A new instrument has to be checked against the one it replaces before its numbers mean anything.

| Arm | `08` (Criterion, µs) | This driver (µs) | Difference |
| --- | --- | --- | --- |
| `ngnet-h2`, duplex, empty body | 10.2 | 10.74 | +5% |
| `ngnet-qmux-h3`, duplex, empty body | 29.5 | 28.93 | −2% |
| **ratio** | **2.92×** | **2.69×** | −8% |

The profiler agrees independently: sampling at 4 kHz over 150,000 exchanges accounts for 11.03 µs
and 29.66 µs per exchange, within 8% of Criterion on both arms. The driver is faithful enough to
attribute with.

## Result 1 — writes, and this is the whole of the socket story

Syscalls per exchange over a loopback socket, two-point. An "exchange" is one request and one
response; at 1 MiB that is 2 MiB across the socket, since the server echoes.

| Workload | | HTTP/2 | QMux/H3 |
| --- | --- | --- | --- |
| empty body | writes | 2 | 4 |
| | reads | 2 | 3 |
| | `epoll_wait` | 3 | 3 |
| 1 MiB body | **writes** | **189** | **68** |
| | reads | 192.5 | 193 |
| | `epoll_wait` | 93 | 83 |
| 64 streams | **writes** | **2** | **132** |
| | reads | 2 | 3.05 |
| | `epoll_wait` | 5 | 5 |

HTTP/2 writes with `writev`; QMux writes with `sendto`, because a coalesced pass is already
contiguous and has nothing to gather.

Write sizes at 1 MiB, from the same traces, setup discarded:

| | writes | mean | median | **max** |
| --- | --- | --- | --- | --- |
| HTTP/2 | 3,774 | 10.9 KiB | 16.0 KiB | **16.0 KiB** |
| QMux/H3 | 1,368 | 30.1 KiB | 0.2 KiB | **64.1 KiB** |

**HTTP/2 never issues a write larger than one frame.** QMux fills a 64 KiB buffer and empties it.
That is the mechanism `08` asked for: same bytes, 2.8× fewer calls to move them. QMux's median is
small because the per-stream-ending flushes of Result 3 sit in the same distribution as the bulk
writes.

## Result 2 — where the processor time goes, by layer

`perf record`, sampled at 4 kHz, attributed by symbol to the layer that owns it. Both stacks are
one process running both endpoints, so these are the cost of a whole exchange, both sides.

### Empty body, duplex — the fixed cost, with no kernel in it

| Layer | HTTP/2 (µs) | QMux/H3 (µs) | Δ |
| --- | --- | --- | --- |
| **Rust HTTP driver** | 4.89 `ngnet-h2` | **13.18** `ngnet-h3` | **+8.29** |
| tokio | 0.97 | 3.87 | +2.90 |
| C protocol library | 1.19 `nghttp2` | 3.53 `nghttp3` | +2.34 |
| QMux transport | — | 2.06 `ngnet-qmux` | +2.06 |
| record framing | — | 1.16 `dwnx` | +1.16 |
| libc — malloc, free, memcpy | 2.37 | 3.24 | +0.87 |
| kernel / vdso | 0.19 | 0.58 | +0.39 |
| join, fixture, unresolved | 1.56 | 2.19 | +0.63 |
| **total** | **11.03** | **29.66** | **+18.63** |

**The extra layer is not where the fixed cost is.** QMux's own transport and its record framing
together add 3.22 µs — 17% of the gap. The single largest term, at 45%, is `ngnet-h3`, which costs
2.7× what `ngnet-h2` costs to move an exchange carrying no bytes. `ngnet-h3` is shared with the
QUIC stack and contains no QMux code.

Inside that 13.18 µs:

| Function | µs | share of exchange |
| --- | --- | --- |
| `Driver::apply_events` (both roles) | 2.39 | 8.1% |
| `Driver::close_stream` (both roles) | **2.04** | **6.9%** |
| `QmuxConnection::poll_event` | 0.63 | 2.1% |
| six `Shared::*_pending` / `take_*` predicates | ~1.35 | 4.6% |
| remainder — a flat tail, nothing above 1% | ~6.8 | 23% |

### 1 MiB body, duplex — the marginal cost

| Layer | HTTP/2 (µs) | QMux/H3 (µs) | Δ |
| --- | --- | --- | --- |
| libc — malloc, free, memcpy | 215.7 | 265.9 | +50.2 |
| Rust HTTP driver | 184.6 `ngnet-h2` | 157.1 `ngnet-h3` | **−27.5** |
| QMux transport | — | 63.9 | +63.9 |
| record framing `dwnx` | — | 41.0 | +41.0 |
| tokio | 52.9 | 63.2 | +10.3 |
| C protocol library | 32.2 | 39.7 | +7.5 |
| kernel / vdso | 3.9 | 16.2 | +12.3 |
| join, fixture, unresolved | 26.9 | 31.3 | +4.4 |
| **total** | **518.1** | **674.6** | **+156.5** |

The composition has inverted. At a megabyte the record layer — 63.9 µs of QMux plus 41.0 µs of
dwnx, 105 µs together — is 67% of the gap, and it is genuine per-byte work: 2 MiB chopped into
16382-byte records is 256 records to build, write headers for and scan. And `ngnet-h3` is now
**27.5 µs cheaper** than `ngnet-h2`, which is the same fact as the empty-body table read the other
way round: the HTTP/3 driver's penalty is per exchange, not per byte.

## Result 3 — QMux's write count grows with concurrency; HTTP/2's does not

Writes per exchange over a socket, sweeping the stream count:

| streams | HTTP/2 | QMux/H3 |
| --- | --- | --- |
| 1 | 2 | 4 |
| 8 | 2 | 18 |
| 64 | 2 | 132 |

QMux fits `writes = 2n + 2` at every point measured. HTTP/2 is **constant at 2** whether it is
carrying one stream or sixty-four. Reads tell the opposite story: QMux takes 3 reads at every
concurrency, so the sixty-four responses do arrive coalesced — the bytes are collectable, and it
is only the writer that will not collect them.

The cause is a batching rule at the join, and it is deliberate.
`ngnet-h3`'s `apply_events` applies control-plane events before data events *within* a batch, so a
stream ending placed in the same batch as that stream's last bytes would be applied first and the
bytes would then be read against a stream the state machine had already released. QMux's
`poll_event` therefore returns `Poll::Pending` at every stream ending to start a fresh batch. That
`Pending` ends the HTTP/3 driver's turn, and the end of a driver turn is exactly what forces the
QMux outbound buffer to flush. One stream ending, one flush, one write — on each side, hence 2n.

This was confirmed by removing the rule, which is unsound and fails as its own doc comment
predicts: the warm-up exchange dies with `the exchange ended before a response arrived`. The rule
is load-bearing, so the scaling law and the read/write asymmetry stand as the evidence in its
place.

**This is the concurrency inversion.** Over a duplex each of those 132 writes is a memcpy into a
buffer; over a socket each is a syscall. It is the only workload in the suite where QMux's write
count depends on a parameter that HTTP/2's does not, and correspondingly the only one whose ratio
worsens when a kernel is added.

## Result 4 — transport polls, and a linear scan

Per empty exchange, duplex, counted with uprobes on `tokio`'s own `DuplexStream` methods, which
both stacks call:

| | HTTP/2 | QMux/H3 |
| --- | --- | --- |
| `poll_read` | 7 | **91** |
| `poll_write_vectored` | 2 | 0 |
| `poll_write` | 0 | 4 |

The 91 decomposes: the HTTP/3 driver calls `poll_event` **30 times per empty exchange**, and
`poll_event` runs a full `pump` of the transport every time — 88 pumps, 91 read attempts, nearly
all of them returning `Pending`. It costs less than it looks: skipping the pump when an event is
already queued removes most of it and buys **1.3%**, because a `Pending` read on a duplex is about
17 ns. It is reported as the shape of the cascade, not as a cost.

The cost that is real sits in `close_stream`. `ngnet-h3` keeps a `Vec<StreamId>` of the last
`CLOSED_TOMBSTONES = 1024` closed streams and opens every close with `self.closed.contains(&stream)`
— a linear scan of 1024 entries, on a connection that has closed more than 1024 streams. Changing
only that constant:

| Arm | 1024 | 16 | change |
| --- | --- | --- | --- |
| QMux duplex, empty body | 28.93 µs (28.82–29.27) | 25.56 µs (25.43–25.58) | **−11.7%** |
| QMux socket, empty body | 54.98 µs (53.93–55.08) | 51.16 µs (50.62–51.76) | **−6.9%** |
| QMux duplex, 64 streams | 1218.26 µs (1210.82–1220.65) | 975.73 µs (974.99–983.12) | **−19.9%** |

No range overlaps its pair. The scan is the cost and not the `drain` beside it: leaving the
constant at 1024 but trimming only at 2048 — which amortises the drain while *lengthening* the
average scan by half — made both arms **worse** in proportion, 30.73 µs and 1318.26 µs. A cost
that grows when the scanned list grows and shrinks when it shrinks is the scan.

## What this establishes

- **QMux beats HTTP/2 at a megabyte over a socket because it issues 68 writes where HTTP/2
  issues 189.** HTTP/2 caps a write at one frame — 16.0 KiB, measured as its maximum over 3,774
  writes — while QMux empties a 64 KiB buffer. The saving is worth about 2.7 µs per avoided write
  on this loopback, enough to pay for QMux's 174 µs of extra per-exchange processor cost and leave
  the 153 µs that `08` measured as its margin.
- **The fixed per-exchange cost is mostly not QMux.** Of +18.6 µs on a duplex, the QMux transport
  and record framing account for 3.2 µs; `ngnet-h3` accounts for 8.3 µs, which is 45%. Any
  statement that the gap is "the extra layer" is wrong by a factor of two and a half.
- **A linear tombstone scan in `ngnet-h3::close_stream` costs 11.7% of an empty exchange and
  19.9% of a 64-stream one**, on any connection that has closed more than 1024 streams. It is a
  scan, not a drain, and it is not QMux code.
- **The concurrency inversion is a write count that grows with the stream count**, `2n + 2`
  against HTTP/2's constant 2, caused by a correctness-required batch boundary at every stream
  ending forcing a flush. This closes the standing lead in
  [`../../qmux-h3/pending-work.md`](../../../qmux-h3/pending-work.md), which asked for exactly this
  count.
- **At a megabyte the record layer is the marginal cost** — 105 of 156 µs — and the HTTP/3 driver
  is *cheaper* than the HTTP/2 one, so the driver penalty does not scale with the body.

## What it does not

- **It does not measure `ngnet-quic-h3`**, which shares `ngnet-h3` and should therefore pay the
  same tombstone scan and the same driver cost. That the code is shared is a fact about the
  source; that the cost transfers is an inference, and this host cannot build the QUIC stack
  (OpenSSL 3.0.13 against a required 3.5).
- **It does not propose or test a fix for anything.** The tombstone A/B changes a constant to
  demonstrate where the time goes; 16 tombstones is not a correct setting, and the fix is a
  different data structure, not a smaller number.
- **It does not establish that HTTP/2 would win back the megabyte by coalescing.** That HTTP/2's
  writes are capped at 16 KiB is measured; that raising the cap would recover 327 µs of kernel
  path is not, and the two stacks' write paths are not interchangeable.
- **`perf` attribution has a 6% unresolved bucket on the QMux arm and 12% on the HTTP/2 one**,
  from frame pointers missing in library code. Layer totals carry that uncertainty; the syscall
  and uprobe counts do not, being exact.
- **Loopback only, tokio only, single-threaded, no memory measurement.** The concurrency figures
  are `current_thread`; the multi-worker hang recorded in
  [`../../qmux-h3/pending-work.md`](../../../qmux-h3/pending-work.md) is untouched here.
- **The 2.7 µs per avoided write is a quotient**, 327 µs over 121 writes, not a measured
  per-syscall cost. It is offered as an order of magnitude.
- **The two instruments disagree by 10% on the 1 MiB duplex gap** — 156.5 µs from this run's
  profiler against 174.4 µs from [`08`](08-qmux-against-h2.md)'s Criterion medians. Both are used
  above, each within its own arithmetic. The disagreement is the size [`04`](04-qmux-drift-baseline.md)
  predicts for that identifier, which it records as the noisiest in the suite at 10.42%, and no
  claim here turns on the difference.
