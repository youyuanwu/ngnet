# 20 — Candidate D: QMux event-queue traffic

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Commit(s):** `6bff8ee` (production code identical to `364dbb2`)
**Disposition:** **documentation-only: gate-incompatible**; queue-local changes cannot reduce
registered pop calls
**Cases:** warmed empty QMux/H3 exchange over duplex
**Command:** release-visible source counters around 100 and 300 exchanges; pinned release
microprobe of 10,000,000 empty `Mutex<VecDeque<Event>>` pops, repeated five times
**Repetitions:** exact counters at 100 and 300 exchanges; five lock-cost repeats
**Controls:** immutable D gate: pops below 23, pushes exactly 7; 2% elapsed floors on socket serial
and socket concurrency 1
**Exclusions:** none; all counter runs and all five lock-cost repeats are reported

## What was being asked

Candidate D asks whether cost *inside* `EventQueue` can be removed independently of Candidate A's
driver/read amplification. Retention requires all of:

- `EventQueue::pop` below **23** with pushes unchanged at **7**;
- socket serial and socket concurrency 1 improvements beyond controls, spreads and 2%;
- no duplex/socket 1 MiB regression beyond the same threshold.

An optimization that makes an existing pop cheaper but leaves 23 calls is not admissible.

## Results

### Refreshed exact counts

Temporary release-visible counters were sampled immediately after the fixture warm exchange:

| Exchanges | Pops | Empty pops | Pushes | `Inner::fill` iterations |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 2,300 | 1,600 | 700 | 2,300 |
| 300 | 6,900 | 4,800 | 2,100 | 6,900 |
| per exchange | **23** | **16** | **7** | **23** |

Every pop acquires the queue mutex, so mutex acquisitions are also **23**. The 16 empty calls are
69.6% of pops. Separate fill instrumentation reproduced 2,300 and 6,900 iterations; inspection of
that loop confirms each iteration makes exactly one `poll_next_event_buffered` call and its
`poll_next_event_with` path makes exactly one queue pop. The equality is therefore measured, not
inferred from the old run-16 value.

All instrumentation was removed before this run was committed.

### Uncontended lock cost

The temporary release microprobe timed the exact recovered-lock plus `VecDeque::pop_front`
operation on an empty queue and subtracted an otherwise identical black-box loop:

| Repeat | Empty loop | Locked empty pop | Net per pop |
| ---: | ---: | ---: | ---: |
| 1 | 3.253 ms | 165.817 ms | 16.256 ns |
| 2 | 3.297 ms | 165.753 ms | 16.246 ns |
| 3 | 3.318 ms | 164.243 ms | 16.093 ns |
| 4 | 3.261 ms | 165.723 ms | 16.246 ns |
| 5 | 3.400 ms | 164.413 ms | 16.101 ns |

The microprobe source was removed with the temporary instrumentation, so these values are an
illustrative calibration rather than a standalone reproducible benchmark artifact. The
count-gate decision does not rely on them. The full 23-lock population is about 0.37 µs. Even granting D1 all 16 empty locks gives only
0.257–0.260 µs, below the approximately 0.63 µs socket-serial 2% floor before control movement or
spread. More importantly, it leaves the registered pop count at 23 and fails the count gate.

### Options

- **D1 — atomic empty hint:** queue-confined and capable of avoiding up to 16 uncontended mutex
  acquisitions, but it still invokes `EventQueue::pop` 23 times. It fails the immutable count
  gate and its complete lock-cost population cannot clear the elapsed floor. It was not
  implemented.
- **D2 — pre-sized or alternate storage:** may change growth behavior, but the warmed serial queue
  has no per-exchange growth to remove and still receives 23 pops. It is gate-incompatible.
- **D3 — batch drain:** excluded for correctness. The QMux/H3 join deliberately retains one-event
  lookahead because the lower queue is the read-ahead-accounting boundary. Draining it into a
  second buffer would report that the reader caught up when bytes had only moved
  (`crates/ngnet-qmux-h3/src/connection.rs`, `Inner::next`).

No newly discovered mechanism exists inside `EventQueue` that changes how often its caller invokes
`pop`. Reducing calls requires changing the `Inner::fill`/driver schedule, which is Candidate A's
already measured and reverted mechanism, not an independent D optimization.

### Disposition and validation

Candidate D is closed without production code. Ordering, poisoning recovery, `Send` behavior and
the events-before-ending boundary remain byte-for-byte unchanged. Focused QMux/QMux-H3 tests,
feature variants, clippy and rustdoc passed on the uninstrumented tree at the phase gate.

Do not retry an atomic hint, queue pre-sizing or wholesale draining from the 16-empty-pop count.
A future queue candidate must independently reduce registered pop calls while preserving the
read-ahead boundary.

## Drift controls in the same session

This count/microprobe run has no cross-build elapsed claim. All five lock repeats are reported;
net locked-empty-pop cost ranged from 16.09 to 16.26 ns.

## What this establishes

- The queue still receives 23 pops and seven pushes, and 16 pops are empty.
- Pop count equals fill-loop iterations, so reducing calls is Candidate A-shaped rather than
  queue-internal.
- Every known queue-local option fails the immutable count gate.

## What it does not

- It does not time an atomic hint, change the read-ahead boundary, or claim that a future
  independently count-reducing queue mechanism is impossible.
