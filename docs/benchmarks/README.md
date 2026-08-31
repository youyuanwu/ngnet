# Benchmarks

`tests/ngnet-bench` holds four [Criterion](https://bheisler.github.io/criterion.rs/)
benchmark families, which answer different questions and must not be read as one:

- **The duplex family** — this stack against [hyper](https://hyper.rs) and against
  HTTP/3-over-QMux, all on tokio over a `tokio::io::duplex`, an in-memory pipe with no
  sockets. Varies the *HTTP implementation* and the *protocol*, holding I/O constant, and
  deletes the kernel entirely.
- **The real-socket family** — four arms over real loopback TCP, varying the HTTP
  implementation, the protocol *and* the I/O model. The `transport_*` benches.
- **The Quinn HTTP/3 family** — two arms over real loopback UDP with the same Quinn, rustls,
  Tokio, ALPN, certificate, request, echo, and body drain. It varies the HTTP/3 implementation
  and adapter: `ngnet-h3` + `ngnet-h3-quinn` against upstream `h3` + `h3-quinn`.
- **The QUIC-stack HTTP/3 family** — three arms over loopback UDP: the two Quinn stacks above
  plus `ngnet-h3` + `ngnet-quic-h3` + ngtcp2/OpenSSL. This is an end-to-end transport-stack
  comparison, not an adapter-only attribution.

Between them they fill in the whole matrix of stack against I/O model:

| | duplex (no kernel) | tokio (epoll) | compio (io_uring) |
| --- | --- | --- | --- |
| **`ngnet-h2`** (HTTP/2) | `ngnet-h2` | `ngnet-h2-tokio` | `ngnet-h2-compio` |
| **`ngnet-qmux-h3`** (HTTP/3 over QMux) | `ngnet-qmux-h3` | `ngnet-qmux-h3-tokio` | n/a — no completion byte stream for QMux |
| **hyper** (HTTP/2) | `hyper` | `hyper-tokio` | n/a — hyper has no completion transport |

Neither empty cell is an omission. hyper's connection types are built on tokio's
readiness-based `AsyncRead`/`AsyncWrite`, so there is no hyper-on-io_uring arm to run.
`ngnet-qmux` ships a byte-stream adapter for tokio and none for a completion runtime, and
supplying one would be a piece of transport engineering rather than benchmark infrastructure —
so the cross-protocol comparison deliberately holds the I/O model still, which is what makes it
a comparison of protocols. The duplex column has no compio entry for a third reason: a
`tokio::io::duplex` has no file descriptor, so no completion runtime can attach to one at all.
That is precisely why the second family uses real sockets.

**Only compare within a column, or within a row.** The two families measure different units
of work, so `ngnet-h2` and `ngnet-h2-tokio` are not two measurements of one thing and the
duplex numbers cannot be used to chain a comparison across to the socket ones.

## What a cross-protocol comparison licenses, and what it does not

The matrix now carries three axes, not two, and each pair of arms varies exactly one of them —
or does not, in which case it attributes to none:

| Pair | Varies | Reads as |
| --- | --- | --- |
| `ngnet-h2` against `ngnet-qmux-h3` | protocol | HTTP/2 against HTTP/3-over-QMux, same substrate, runtime, request, body and drain |
| `ngnet-h2` against `hyper` | HTTP/2 implementation | this crate against the reference implementation — the comparison that predates the HTTP/3 arm, carried unchanged |
| `ngnet-h2-tokio` against `ngnet-h2-compio` | I/O model | completion against readiness, same stack |
| `ngnet-qmux-h3` against `hyper` | protocol **and** implementation | attributable to neither; not a comparison this suite makes a claim from |

**It licenses a statement about these two stacks, on this workload, on this machine.** It does
not license a statement about HTTP/2 against HTTP/3, or about QMux against TCP. Three reasons,
all structural rather than fixable:

1. **The layering differs, and that is the subject rather than a flaw.** The QMux arms carry a
   stream-multiplexing transport underneath their HTTP framing; the HTTP/2 arms carry framing
   over a byte stream and nothing else. Some part of every gap is that extra layer, which is
   what a reader wanting to know the cost of running HTTP/3 over a byte stream is asking about.
   It is also why no amount of configuration matching can make the two arms alike: matching
   settings removes the differences that are *settings*, and this is not one.
2. **One implementation of each.** `ngnet-qmux-h3` has never been optimised, has no recorded
   measurement before this suite, and is a join of two layers each younger than the HTTP/2
   stack beside it. A gap here is a fact about this code today.
3. **The substrate is a duplex or loopback.** Neither is a network, and QMux exists to carry
   QUIC's stream operations over a reliable ordered byte stream — a setting where its costs and
   its benefits both look different from what a wire would show.
   [`interpreting.md`](interpreting.md) sets out what these families delete.

The settings the two protocols hold in common, what each is set to, which side moved to meet
the other, and the one setting neither stack can reach are on
[`configuration.md`](configuration.md). The confounds the matching cannot remove — the extra
layer, the record-size difference, the warm-up asymmetry, and QMux's unidirectional streams
spending connection credit where HTTP/2's control frames do not — are on
[`controls.md`](controls.md), each with the direction it pushes.

## Three original groups carry no QMux arm, for two different reasons

Six of the original nine Criterion groups gained a QMux arm. The three that did not are listed here
together, and the reasons are kept apart on purpose: one is an absence of anything to compare,
the other is a defect this stack cannot currently get past. Filing them under one explanation
would make a known bug look like a design boundary.

| Group | Absent arm | Why |
| --- | --- | --- |
| [`shared_body`](cases/shared-body.md) | HTTP/3-over-QMux | **No counterpart mechanism.** The case measures an HTTP/2 body-handover entry point against its copying twin. Nothing on the QMux path hands a body over in that sense, so a QMux arm would be a third quantity beside a two-sided comparison rather than half of one. |
| [`transport_shared_body`](cases/transport-shared-body.md) | HTTP/3-over-QMux | The same, on a real socket. |
| `concurrent_throughput_multi_thread` (in [`concurrent_throughput`](cases/concurrent-throughput.md)) | HTTP/3-over-QMux | **A defect, not an absence.** A QMux arm at concurrency 64 on a multi-worker runtime hangs on most attempts. Nothing in `cargo bench -- --test` imposes a timeout, so such an arm would occasionally wedge CI rather than fail it. The cause is recorded on [`../qmux-h3/pending-work.md`](../qmux-h3/pending-work.md), which is what makes the omission traceable to a known defect rather than indistinguishable from an oversight. |

Note that the two `shared_body` groups are single-**protocol** rather than single-stack: both
carry a `hyper-tokio` arm as a drift control, so "one stack is missing" is not what is being
said about them.

Across all families, latency comes from Criterion's per-iteration timing, and throughput is
derived by putting a known number of requests or bytes in each iteration and declaring it
with `Throughput::Elements` / `Throughput::Bytes`.

The crate is `publish = false` and lives outside `ngnet-h2` for the same reason
`ngnet-h2-tests` does: the wrapper takes exactly one dependency and no dev-dependencies, so
anything needing a third-party stack — hyper — or a second protocol family — `ngnet-qmux-h3`
and `ngnet-h3` — belongs in a crate of its own. That is also why the suite's build
prerequisites changed when the QMux arms arrived; [`running.md`](running.md) states what a
machine now needs.

## How this directory is arranged

The one long page this used to be is split three ways, because the three kinds of content
have different lifetimes. **Case descriptions** change when a bench changes. **Method and
interpretation** change rarely, and are what a reader needs before believing any number.
**Measurements** are perishable: they belong to the machine and the commit that produced
them, and a number without those is not evidence.

| Directory | What lives there | Lifetime |
| --- | --- | --- |
| [`cases/`](cases/) | One page per bench target: what it measures, its arms and parameters, and how to read it. | Follows the code. |
| [`findings/`](findings/) | Conclusions drawn from measurements, and the mechanisms behind them. Links to the runs, quotes at most the headline figure. | Survives re-measurement. |
| [`data/`](data/) | The measurements themselves, one file per run, filed under the machine that produced them. | Per machine, per commit. |

Plus four pages of shared context that every case and every run depends on:

| Page | What it settles |
| --- | --- |
| [`running.md`](running.md) | The commands, pinning, baselines, and how to record a run into `data/`. |
| [`interpreting.md`](interpreting.md) | What these numbers do and do not mean, the noise caveat, and what is deliberately not measured. |
| [`controls.md`](controls.md) | The confounds and which way each pushes; the drift controls and measurement-design rules a run must follow to count. |
| [`configuration.md`](configuration.md) | Which protocol settings are matched between the stacks being compared — for the HTTP/2 pair and for the cross-protocol pair separately — which could not be, and which layer's limit binds first. |

One page sits outside that scheme because it is not a timing at all:
[`allocation-counts.md`](allocation-counts.md) records counts pinned by tests, which are a
property of the code rather than of a machine, and so are neither a case nor a run.

## The cases

"Varies" names the axis a case's arms move along; **protocol** means the case carries both an
HTTP/2 arm and an HTTP/3-over-QMux arm.

| Case | Family | Varies | Reports |
| --- | --- | --- | --- |
| [`serial_latency`](cases/serial-latency.md) | duplex | stack × protocol | latency of one empty-body exchange |
| [`concurrent_throughput`](cases/concurrent-throughput.md) | duplex | stack × protocol × N ∈ {1, 8, 64} | requests/sec |
| [`body_throughput`](cases/body-throughput.md) | duplex | stack × protocol × body ∈ {0, 1 KiB, 64 KiB, 1 MiB, 8 MiB} | MB/s |
| [`shared_body`](cases/shared-body.md) | duplex | body strategy × body size — HTTP/2 only | MB/s |
| [`transport_serial_latency`](cases/transport-serial-latency.md) | socket | stack × protocol × I/O model | latency of one empty-body exchange |
| [`transport_concurrent_throughput`](cases/transport-concurrent-throughput.md) | socket | stack × protocol × I/O model × N | requests/sec |
| [`transport_body_throughput`](cases/transport-body-throughput.md) | socket | stack × protocol × I/O model × body ∈ {0, 1 KiB, 64 KiB, 1 MiB, 8 MiB} | MB/s |
| [`transport_shared_body`](cases/transport-shared-body.md) | socket | body strategy × I/O model × body — HTTP/2 only | MB/s |
| [`quinn_serial_latency`](cases/quinn-serial-latency.md) | Quinn loopback | HTTP/3 implementation | latency of one empty-body exchange |
| [`quinn_body_throughput`](cases/quinn-body-throughput.md) | Quinn loopback | HTTP/3 implementation × body ∈ {16 KiB, 1 MiB} | MB/s |
| [`quic_stack_serial_latency`](cases/quic-stack-serial-latency.md) | QUIC loopback | HTTP/3 × QUIC × TLS stack | latency of one empty-body exchange |
| [`quic_stack_body_throughput`](cases/quic-stack-body-throughput.md) | QUIC loopback | HTTP/3 × QUIC × TLS stack at 1 KiB | MB/s |

## The findings so far

| Finding | In short |
| --- | --- |
| [Write path and gathering](findings/write-path-and-gathering.md) | The arms separated on **write syscalls per pass**, not on the I/O model. Gathering closed the gap: −52% at N=8, −59% at N=64. |
| [Reusing the coalescing buffer](findings/coalescing-buffer-reuse.md) | About 4–7% for the completion transport, from not rebuilding the buffer each pass. |
| [Handing bodies over](findings/handing-bodies-over.md) | `NGHTTP2_DATA_FLAG_NO_COPY` is worth −24% to −31% at 1 MiB on the readiness transport, and a small but real gain on the completion one. |
| [QMux against HTTP/2](findings/qmux-against-h2.md) | Post-PR-45 QMux/H3 is 1.8–2.1× slower for fixed/concurrent work, reaches 1.005× at socket 64 KiB and 0.845× at socket 1 MiB. Candidates A–C failed elapsed gates and were reverted; D was closed documentation-only because no queue-local mechanism could satisfy its count gate. |

## Where the numbers are

**Every measurement recorded before 2026-08-16 was taken on a machine that is no longer
available**, and is archived under [`data/legacy-dev-host/`](data/legacy-dev-host/) with what
little is known about that host. It was noisy — unchanged control arms drifted 5–15% within a
session — which is why the findings above rest on paired deltas and drift controls rather than
on absolute figures.

Measurements since are taken on [`data/xeon-8370c-azure/`](data/xeon-8370c-azure/), which
drifts about **1%** between identical passes. That machine's first three runs establish the
drift bar, survey the arms, and re-settle one verdict the legacy host could not.

**Absolute numbers from the two hosts are not comparable and must never be tabulated
together**; a claim carried over from the legacy host is a claim awaiting re-measurement, and
[`data/README.md`](data/README.md) says how to file the run that settles it.

QMux/H3 is now measured against HTTP/2 in runs
[`08`](data/xeon-8370c-azure/08-qmux-against-h2.md),
[`09`](data/xeon-8370c-azure/09-qmux-h2-mechanisms.md),
[`16`](data/xeon-8370c-azure/16-qmux-h3-baseline-and-pump-attribution.md), and
[`21`](data/xeon-8370c-azure/21-qmux-h3-combined-final-matrix.md). Run 21 is the current
post-PR-45 comparison; the earlier runs remain historical mechanism evidence.

The ngtcp2/OpenSSL stack is recorded in runs
[`25`–`30`](data/xeon-8370c-azure/README.md#runs). Run 27 is historical packet-bounded
correctness/resource evidence: 125 exact 16 KiB and 1 MiB exchanges remain active regressions.
Final-review run 30 records that ten predetermined exact 1 MiB repetitions passed, while two
fresh diagnostic processes timed out; the current checkout therefore does not carry an
unqualified persistent-stability or RSS-envelope claim. Runs 28 and 29 defer packet-order and
residual optimization candidates without source changes; they are limitations and attribution
records, not implemented performance gains.

Run 25's 1.513× empty and 1.401× 1 KiB gaps are historical pre-repair context, not the final
remaining gap. Getting within 10% of the alternative `ngnet-h3` transport was a stretch
objective, not evidence that could waive correctness, drift, attribution, or throughput gates.
