# 17 — Candidate A: read and pump amplification

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Baseline:** `43b7da0` (production code identical to `364dbb2`)
**Candidate:** `4e91115`
**Disposition:** **reverted** by `c23df30`; socket serial failed the pre-registered elapsed gate
**Cases:** duplex/socket empty serial; socket concurrency 1/8/64; duplex/socket 1 MiB
**Command:** preserved binaries run as `taskset -c 3 <binary> --bench <paired-filter>
--save-baseline <label> --noplot`; exact counts used 100/300-iteration `perf stat` uprobes
**Repetitions:** three interleaved passes per claim arm; two per guard arm
**Controls:** unchanged H2 arm in every Criterion pass
**Exclusions:** the first, weaker prototype's screening pass was not used for the final candidate
decision because that implementation was replaced before `4e91115`; no final-candidate pass,
sample, or count was discarded

## Question and immutable gate

Phase 2 attributed 35 of 70 empty-exchange pumps to `poll_open` and `transmit::drain`, selecting
the low-risk A2 source-collapse design. Candidate A could be retained only if:

- duplex serial, socket serial, and socket concurrency 1 all improved beyond matching control
  movement, both sides' spread, and 2%;
- duplex/socket 1 MiB and socket concurrency 8/64 did not regress beyond the same threshold;
- reads fell below 73 and pumps below 70 while `poll_event`/`poll_transmit`/`drain_work` remained
  exactly 30/14/14.

## Stage-1 transition and wake analysis

The selected A2 design caches no pending-read state and suppresses no read *inside* a lower pump.
Every retained pump still executes `read_side` with the current `Context`. The eleven state
transitions catalogued in `CodeResearch.md` §A.4 divide as follows:

| Transition | Can itself produce inbound bytes? | A2 consequence |
| --- | --- | --- |
| outbound production/write; `produce_pending` set/cleared | no | bounded output may trigger the conditional capacity pump |
| dwnx callback pushes | no; consequence of an already productive read | event order and one-event lookahead remain unchanged |
| read-ahead delivered/credited; deferred H3 credit | no | may produce outbound credit, flushed at the existing interaction/suspension points |
| local stream open | no | lower open still pumps first; post-open production pump remains |
| lower terminal/closing or join ending latch | no | checked by every retained pump and by the mandatory suspension flush |

The only inbound-byte-producing transition is a peer write outside the connection lock. On the
driver path, `take_events` immediately before transmit has already issued a lower read and left
the byte stream holding the task's current waker. A concurrent peer write therefore wakes that
registration. Before any real park, `poll_flush` performs a forced lower pump, consumes bytes
that raced the productive turn, and registers the current waker again if the read remains
pending. Local write readiness similarly wakes either the conditional capacity pump or forced
flush. Flow-control credit and open-stream capacity arrive in peer bytes; processing that read
fires the existing `wake_credit`/stream-limit signals. EOF, close, and read/write errors are
latched by the next retained event, capacity, or suspension pump.

No waker is stored by A2, so there is no stale-registration invalidation set and no
`Waker::will_wake` decision to make. A1 would require a new guaranteed wake for every skipped
read; A3 would require a driver-turn reset boundary that the transport interface does not expose.
Those facts are why the selection rule chose A2 rather than either caching design.

## Mechanism tested

The candidate removed the duplicate join pre-pump before the lower `poll_open`, whose lower call
already begins with the same pump. In `transmit::drain`, it removed unconditional per-offer and
tail pumps. The preceding event pass owns productive transport progress and read registration;
the mandatory `poll_flush` owns the final read registration and flush before suspension. A
buffer-room predicate retained a buffered pump only when another maximum-size record would exceed
`OUTBOUND_CEILING`, preventing local buffer pressure from being misreported as peer flow-control
backpressure.

The first screen retained one unconditional transmit-entry pump. It improved duplex/socket serial
by about 4.5%/2.2% but socket concurrency 1 by only 1.3%, below the 2% floor. The final candidate
removed that pump too, then restarted the controlled passes with newly built and hashed binaries.

Focused deterministic tests pinned:

- a ready open uses exactly the lower open and post-open production pumps;
- four small offers add no pump or read between the preceding event pass and suspension flush;
- all four payloads still arrive in order;
- a byte-stream terminal injected between two accepted offers is found by the suspension flush
  and reported only after both accepted-buffer releases;
- two loopback peer responses released together on a two-worker runtime both wake and complete
  within the explicit timeout;
- existing short-write, output-ceiling, first-request, final-DATA/close boundary, release,
  fragmented-offer, reset, abandonment, and loopback tests remain green.

The exact candidate commit was rebuilt after these tests were integrated; all six benchmark
hashes below reproduced the measured binaries byte for byte.

## Binary identity

| Binary | baseline SHA-256 | candidate SHA-256 |
| --- | --- | --- |
| `probe` | `0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` | `5ec895ff21171d9617b9c20e084b6a8af339541a9c48f5ceb393847d1a9fadee` |
| `serial_latency` | `294ec76d110f77eeb86b2c222fa27cc22e6462df6cd6e4077bf55231f368776a` | `33f9dbd8721e475c0cfc04d55fc3fb072baf2ce0acc4b874ffeebea7a54e0143` |
| `transport_serial_latency` | `1a704657a524a6677a960c36f7d1ba98df66fdced24376ee1481009de9e66c82` | `fc4d46a2693a8bd5a4dbc91f9a823d454664f1acf972bb97d1ad39147c271284` |
| `transport_concurrent_throughput` | `01704c5524a71e2ef0148a7f884cdff412f9fed04f44e5cdbbfb8c1e4cc3f55d` | `1ec8495b1be8f8963ead7c31ab5b6f8b55b040bf165c241ba0ada314ac83f13b` |
| `body_throughput` | `4910af4e89d7b9283ea90fe6611a52d57f4b948f535e3646c5f9f3abc7efbc18` | `7bd44cea30c80ba072cbe08201b51cabcd4722bf771402d3b42dd327ab278694` |
| `transport_body_throughput` | `e2325e731a92f39aca2658d063bdd5e12dcb5e60ecc987bb83ae7f9755972af2` | `0d34d10d8dd5c00489515146d994b787c1a1555d88e5aa04f09d9fb61f545f8f` |

## Exact counts

Counts are `(c(300)-c(100))/200` against the preserved candidate probe. The release symbols were
mapped to ELF file offsets and all uprobes were removed immediately after the two runs.

| Empty-duplex operation | baseline | candidate |
| --- | ---: | ---: |
| transport `poll_read` | 73 | **40** |
| `Connection::pump` | 70 | **37** |
| `Connection::write_side` | 73 | **40** |
| productive `Conn::read` | 3 | **3** |
| `EventQueue::pop` | 23 | **23** |
| H3 `poll_event` | 30 | **30** |
| H3 `poll_transmit` | 14 | **14** |
| `Shared::drain_work` | 14 | **14** |

The count gate passed exactly: 33 repeated pending reads/pumps disappeared without changing the
driver pass structure.

## Criterion medians

Times are Criterion median point estimates in microseconds. `raw` is candidate versus baseline;
`control` is candidate-binary H2 versus baseline-binary H2 in the same pass pair; `ratio` is the
change in the within-pass `(QMux/H3 ÷ H2)` ratio.

| Case/pass | baseline H2 / QMux | candidate H2 / QMux | raw | control | ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| duplex serial 1 | 10.231 / 20.974 | 10.275 / 19.963 | −4.82% | +0.43% | −5.23% |
| duplex serial 2 | 10.243 / 20.854 | 10.041 / 19.513 | −6.43% | −1.98% | −4.54% |
| duplex serial 3 | 10.127 / 20.809 | 10.092 / 19.681 | −5.42% | −0.35% | −5.09% |
| socket serial 1 | 16.996 / 32.461 | 17.131 / 31.493 | −2.98% | +0.79% | −3.75% |
| socket serial 2 | 16.828 / 31.793 | 16.798 / 30.976 | −2.57% | −0.18% | −2.40% |
| socket serial 3 | 16.735 / 31.854 | 16.794 / 31.865 | **+0.04%** | +0.35% | −0.31% |
| socket concurrency 1, pass 1 | 17.564 / 32.682 | 17.824 / 32.074 | −1.86% | +1.48% | −3.29% |
| socket concurrency 1, pass 2 | 17.596 / 33.130 | 17.410 / 31.655 | −4.45% | −1.06% | −3.43% |
| socket concurrency 1, pass 3 | 17.589 / 32.919 | 17.983 / 31.893 | −3.11% | +2.24% | −5.24% |

The claim-arm full ranges make the failure explicit:

| Claim arm | baseline QMux range/spread | candidate QMux range/spread | median-of-passes change |
| --- | ---: | ---: | ---: |
| duplex serial | 20.809–20.974 / 0.79% | 19.513–19.963 / 2.31% | **−5.62%** |
| socket serial | 31.793–32.461 / 2.10% | 30.976–31.865 / 2.87% | **−1.13%** |
| socket concurrency 1 | 32.682–33.130 / 1.37% | 31.655–32.074 / 1.33% | **−3.11%** |

Socket serial's 1.13% median change is below the immutable 2% floor and below the candidate's
2.87% spread. Its third paired pass showed no raw improvement. Count reduction alone cannot
retain the mechanism.

## Guard arms

| Guard/pass | baseline H2 / QMux | candidate H2 / QMux | raw | control |
| --- | ---: | ---: | ---: | ---: |
| socket concurrency 8, pass 1 | 72.387 / 133.218 | 73.483 / 131.705 | −1.14% | +1.51% |
| socket concurrency 8, pass 2 | 71.953 / 133.339 | 71.842 / 128.847 | −3.37% | −0.15% |
| socket concurrency 64, pass 1 | 558.198 / 997.938 | 557.487 / 971.539 | −2.65% | −0.13% |
| socket concurrency 64, pass 2 | 556.600 / 1000.593 | 554.789 / 966.482 | −3.41% | −0.33% |
| duplex 1 MiB, pass 1 | 496.622 / 600.178 | 509.490 / 598.564 | −0.27% | +2.59% |
| duplex 1 MiB, pass 2 | 498.245 / 600.972 | 494.387 / 581.785 | −3.19% | −0.77% |
| socket 1 MiB, pass 1 | 1220.097 / 1141.810 | 1256.212 / 1130.621 | −0.98% | +2.96% |
| socket 1 MiB, pass 2 | 1222.998 / 1139.602 | 1223.963 / 1126.382 | −1.16% | +0.08% |

No guard regressed. That does not override a failed claim target.

## Validation and removal proof

At exact candidate commit `4e91115`, all of the following passed:

```text
cargo test -p ngnet-qmux-h3 -p ngnet-qmux-h3-tests --release
cargo test -p ngnet-qmux
cargo test -p ngnet-qmux --no-default-features
cargo test -p ngnet-h3
cargo test -p ngnet-h3 --no-default-features
cargo clippy -p ngnet-qmux -p ngnet-qmux-h3 --all-targets --all-features -- -D warnings
cargo clippy -p ngnet-qmux -p ngnet-qmux-h3 --all-targets --no-default-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-qmux -p ngnet-qmux-h3
cargo bench -p ngnet-bench -- --test
```

The detached verification checkout used the main checkout's initialized dwnx sources because Git
worktrees do not populate submodules. The two initially affected source-reading QMux tests were
rerun after that fixture correction and passed.

Removal was also tested rather than inferred: restoring the two production files to `43b7da0`
while leaving the candidate tests in place made
`several_small_offers_share_one_entry_pump` fail with **6 pumps observed versus 0 expected**.
The candidate files were restored before the complete revert was created.

## Decision

**Rejected and reverted.** A2 is safe enough to pass the focused correctness suite and removes
47% of pumps/reads, but the removed operations do not own a stable 2% of socket-serial elapsed
time on this machine. Neither this exact conditional-room design nor the weaker
one-entry-pump-per-transmit design should be retried without new profile evidence that changes
the elapsed upper bound.

A1 pending-read caching remains structurally unsafe without a new guaranteed wake source. A3
requires an outer driver-turn boundary unavailable through the current transport interface.
Neither was prototyped after the lower-risk A2 mechanism failed the elapsed gate.
