# 18 — Candidate B: non-pooled delivery ownership

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Baseline:** `a4a26b7` (production code identical to `364dbb2`)
**Disposition:** **closed before implementation**; every safe non-pooled owner misses the
allocation gate, and the only count-reducing fast path cannot clear the elapsed-time gate
**Cases:** QMux/H3 1 MiB echo over duplex and loopback socket
**Command:** `cargo build --release -p ngnet-bench --example probe`; source counters as
`taskset -c 3 target/release/examples/probe qmux-{duplex,socket} body 1048576 10`; exact malloc
stacks as `sudo perf record -e probe_libc:candidate_b_malloc -g --call-graph dwarf,8192
--no-buildid -- taskset -c 3 <probe> qmux-duplex body 1048576 <N>`
**Repetitions:** ten exchanges on each substrate for source counts; malloc stacks at 1/3 and
10/30 exchanges, reduced by two-point subtraction
**Controls:** no timed candidate was created; the immutable gate and run 05's directly measured
prototype bound selection before implementation
**Exclusions:** none; temporary counters, the malloc uprobe, profiles, and instrumented binary
were removed after the pristine probe hash was reproduced

## Question and immutable gate

Candidate B asked whether delivery data could leave dwnx's callback without the
`event.data.to_vec()` copy and without repeating run 05's recycling pool. Retention required all
of the following:

- fewer than **710** allocator calls per 1 MiB exchange and a delivery-path share below 82.9%;
- duplex and socket 1 MiB each faster beyond matching controls, both sides' spread, and 2%;
- no material regression in duplex/socket serial or socket concurrency 1.

The selection rule required investigation to stop before production code when no safe option
could meet the count gate, or when a count-reducing option's measured upper bound could not meet
both elapsed gates. That is the outcome here.

## Fresh ownership and allocation counts

The release-visible counter build changed no branch, buffer, owner, or event. It counted each
productive QMux read, stream-data callback, callback containment result, framer capacity growth,
H3 read, and H3 `view_of` result. The socket run was exactly divisible by ten:

| Operation per 1 MiB echo | Count |
| --- | ---: |
| Productive QMux read batches | **193** |
| QMux stream-data callbacks / `to_vec()` copies | **162** |
| Bytes delivered through those callbacks | **2,097,194** |
| Callback slices inside the current QMux read | **162** |
| Callback slices equal to the entire QMux read / foreign to it | **0 / 0** |
| H3 transport-data reads / first `Bytes` promotions | **162 / 162** |
| H3 body callback views equal to the whole parent | **158** |
| H3 body callback proper slices / foreign replays | **2 / 0** |
| Framer partial-record buffer growth / copied bytes | **0 / 0** |

Duplex produced 1,931 reads and 1,621 callbacks in ten exchanges rather than 1,930/1,620; the
one-event difference is an initial connection-level boundary amortised by the documented
two-point method. Its H3 body split was still exactly 1,580 whole-parent views and 20 proper
slices. Socket therefore supplies the integer per-exchange table while duplex independently
reproduces its ownership shape.

The `to_vec()` count is an allocation count, not merely a call count: all 162 deliveries were
non-empty. `Bytes::from(Vec<u8>)` adopts each exact-capacity vector in
`ngnet-qmux-h3/src/event.rs`; the first clone in the H3 driver then promotes that unique
vector-backed `Bytes`, accounting for the 162 `bytes::shallow_clone_vec` allocations. The
whole-parent population is real—158 of 160 body views—but it appears *after* that promotion.

The exact malloc trace reproduced **711** calls in the short 1/3 subtraction and 709.65 in the
long 10/30 event-record subtraction; the fractional long result reflects lost tracing events.
Run 16's 100/300 `perf stat` count of **710** remains the gate baseline. The short exact stack
split was:

| Allocation site | Calls per exchange | Share of 710 |
| --- | ---: | ---: |
| QMux callback `event.data.to_vec()` | **162** | 22.8% |
| `bytes::shallow_clone_vec` promotion | **163** | 23.0% |
| `RawVec::finish_grow` | **279** | 39.3% |
| all other sites | **107** | 15.1% |

The one-call promotion difference from the source count is startup amortisation in the short
subtraction. The longer subtraction converged to 162.85 sampled calls, while the branch-neutral
source counter was exactly 162 on socket.

### `RawVec` is not framer ownership

A capacity-before/after counter at `RecordFramer::consume` observed **zero** growth and zero
copied bytes. ELF monomorph attribution put the steady `RawVec` events in H3, not the framer:
approximately 273 H3, three QMux queue, two tokio, and one other call per exchange in the short
subtraction.

Additional exact capacity counters identified **242** of the H3 calls per exchange:

| H3 vector | Growth allocations |
| --- | ---: |
| `Driver::take_events` transport batch | **105** |
| `Driver::apply_events` data sweep | **66** |
| `Events::observed` callback batch | **68** |
| field accumulator | **3** |
| unheard resets / partial-section registry | **0 / 0** |

The remaining roughly 31 H3 `RawVec` calls are header/map and adjacent driver storage. They
belong to Candidate C's fixed-work investigation, not to the complete-record owner. B4 is
therefore deferred with a concrete site boundary rather than mislabelled as framer growth.

## The four ownership options

### B1 — one bounded owner per read batch or record

dwnx exposes enough information to form ranges: every one of 1,620 callback slices in the
ten-exchange socket run lay inside the active read. An `Arc`-backed standard-library owner would
be `Send`, could be cloned inside the callback, and would be reclaimed when its Rust owners
dropped. A 1 KiB copy-out threshold against the 16,382-byte read buffer would reproduce run 05's
explicit at-most-16× pinning bound without pinning 64 KiB.

The non-pooled construction cannot pass the count gate:

1. Polling directly into shared storage needs a fresh mutable owner whenever the prior read has
   a live view. `Arc<Vec<u8>>` needs both the vector allocation and its control allocation.
   Allocating it before `poll_read` also charges repeated `Pending` reads; retaining a free
   owner to avoid that is the prohibited recycling pool.
2. Polling into the existing reusable staging vector and making an immutable `Arc<[u8]>`
   afterwards avoids allocation on `Pending`, but safely creates at least one owner allocation
   and copy for each of **193** productive reads.
3. Every one of the **162** delivered ranges then needs a `Bytes::from_owner` control block.
   The minimum is therefore **355** new allocations replacing the current 324
   copy-plus-promotion allocations. It raises the whole-exchange lower bound from 710 to at least
   **741**, before queue growth or an `Arc<Vec<u8>>`'s second allocation.

A complete-record owner has the same arithmetic because this workload has no framer retention
and at least one owner per delivered record. A bounded batched arena also has the same 193-owner
floor unless it spans reads; spanning reads becomes the excluded retained-buffer pool and needs
compaction bookkeeping. B1 is safe in principle, but gate-incompatible without the forbidden
recycling mechanism.

### B2 — construct `Bytes` in the QMux callback

This would remove both allocations in one representation, but `ngnet-qmux` deliberately has
one non-optional dependency. Naming `bytes` there fails
`ngnet-workspace-tests/tests/dependency_graph.rs`. A caller-supplied owner abstraction would
change the publicly re-exported `Event::StreamData` field shape. Both are outside this work, so
B2 remains structurally excluded.

### B3 — move a whole parent instead of promoting it

The population exists: **158 of 160** H3 body views cover their entire parent. Merely moving
`Bytes::from(data)` earlier changes nothing. To avoid promotion, H3 would have to record callback
ranges and move the original parent only after `read_stream` returns; cloning the parent before
the call has already paid `shallow_clone_vec`.

That internal range-deferral design can be memory-safe, but the removable count is 162 of 710,
**22.8%**, below the selection rule's 30% preference threshold. More importantly, run 16
attributes only +4.74 µs/+6.07 µs of the duplex/socket QMux-minus-H2 result to the entire
allocation/memory category. Even granting B3 that whole differential—far more than one promotion
site owns—bounds it to 0.77% of duplex and 0.52% of socket time, below the immutable 2% floor.
Run 05's stronger copy-removing prototype improved the 1 MiB duplex arm by only 0.61%, inside
controls. B3 is therefore gate-incompatible and was not prototyped.

### B4 — partial-record growth

The proposed adjacent mechanism has no population here: zero framer growth and zero retained
bytes in both substrates. The observed `RawVec` cost is H3 batch storage and is carried into
Candidate C; changing the framer cannot move it.

## Contracts checked

- The current owner is created by `to_vec()` inside the callback; no dwnx borrow escapes.
- `Event` remains `Send`, and read-ahead delivered/credited accounting is unchanged.
- `RETAINS_BUFFERS = false` and outbound release on write acceptance are unrelated to every
  investigated inbound option and were not touched.
- Event/data/close order, fragmented records, multiple deliveries, EOF/error, abandonment, and
  early cleanup are unchanged because no production path changed.
- No pool, free list, unsafe lifetime extension, `deps/dwnx` change, dependency addition, or
  large-buffer retention was introduced.

## Disposition

Candidate B is **closed without production code**. B1 and the bounded arena cannot reduce the
required allocation count without the recycling design run 05 already rejected. B2 violates the
dependency/public-shape boundary. B3 has a large whole-parent population but removes only 22.8%
of allocations, and its generous measured upper bound cannot reach either 2% elapsed target. B4
has zero framer population.

No timing claim is made, no allocation-only code is retained, and no candidate binary exists to
compare. The pristine release probe was rebuilt after removing all counters and reproduced
SHA-256 `0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8`.

## Validation

All locally supported phase checks passed on the pristine source:

- release tests for `ngnet-qmux`, `ngnet-qmux-h3`, and `ngnet-qmux-h3-tests`;
- default and no-default-feature tests for `ngnet-h3`, and no-default-feature tests for
  `ngnet-qmux`;
- the workspace dependency-graph structural test;
- all-target/all-feature clippy for the three touched-path crates plus no-default-feature clippy
  for `ngnet-qmux` and `ngnet-h3`, with warnings denied;
- rustdoc for `ngnet-qmux`, `ngnet-qmux-h3`, and `ngnet-h3`, with warnings denied;
- `git diff --check`.

The OpenSSL-dependent linkage test remains assigned to CI under the repository's documented
machine limitation. `deps/dwnx` was not changed.
