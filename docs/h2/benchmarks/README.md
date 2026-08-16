# Benchmarks

`tests/ngnet-h2-bench` holds two [Criterion](https://bheisler.github.io/criterion.rs/)
benchmark families, which answer different questions and must not be read as one:

- **The duplex family** — this stack against [hyper](https://hyper.rs), both on tokio over a
  `tokio::io::duplex`, an in-memory pipe with no sockets. Varies the *HTTP/2
  implementation*, holding I/O constant, and deletes the kernel entirely.
- **The real-socket family** — three arms over real loopback TCP, varying the HTTP/2
  implementation *and* the I/O model. The `transport_*` benches.

Between them they fill in the whole matrix of stack against I/O model:

| | duplex (no kernel) | tokio (epoll) | compio (io_uring) |
| --- | --- | --- | --- |
| **`ngnet-h2`** | `ngnet-h2` | `ngnet-h2-tokio` | `ngnet-h2-compio` |
| **hyper** | `hyper` | `hyper-tokio` | n/a — hyper has no completion transport |

The empty cell is not an omission: hyper's connection types are built on tokio's
readiness-based `AsyncRead`/`AsyncWrite`, so there is no hyper-on-io_uring arm to run. The
duplex column has no compio entry for a different reason — a `tokio::io::duplex` has no file
descriptor, so no completion runtime can attach to one at all. That is precisely why the
second family uses real sockets.

**Only compare within a column, or within a row.** The two families measure different units
of work, so `ngnet-h2` and `ngnet-h2-tokio` are not two measurements of one thing and the
duplex numbers cannot be used to chain a comparison across to the socket ones.

In both families, latency comes from Criterion's per-iteration timing, and throughput is
derived by putting a known number of requests or bytes in each iteration and declaring it
with `Throughput::Elements` / `Throughput::Bytes`.

The crate is `publish = false` and lives outside `ngnet-h2` for the same reason
`ngnet-h2-tests` does: the wrapper takes exactly one dependency and no dev-dependencies, so
anything needing a third-party stack — here, hyper — belongs in a crate of its own.

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
| [`configuration.md`](configuration.md) | Which protocol settings are matched between the two stacks, and which could not be. |

One page sits outside that scheme because it is not a timing at all:
[`allocation-counts.md`](allocation-counts.md) records counts pinned by tests, which are a
property of the code rather than of a machine, and so are neither a case nor a run.

## The cases

| Case | Family | Varies | Reports |
| --- | --- | --- | --- |
| [`serial_latency`](cases/serial-latency.md) | duplex | stack | latency of one empty-body exchange |
| [`concurrent_throughput`](cases/concurrent-throughput.md) | duplex | stack × N ∈ {1, 8, 64} | requests/sec |
| [`body_throughput`](cases/body-throughput.md) | duplex | stack × body ∈ {0, 1 KiB, 64 KiB, 1 MiB} | MB/s |
| [`shared_body`](cases/shared-body.md) | duplex | body strategy × body size | MB/s |
| [`transport_serial_latency`](cases/transport-serial-latency.md) | socket | stack × I/O model | latency of one empty-body exchange |
| [`transport_concurrent_throughput`](cases/transport-concurrent-throughput.md) | socket | stack × I/O model × N | requests/sec |
| [`transport_body_throughput`](cases/transport-body-throughput.md) | socket | stack × I/O model × body | MB/s |
| [`transport_shared_body`](cases/transport-shared-body.md) | socket | body strategy × I/O model × body | MB/s |

## The findings so far

| Finding | In short |
| --- | --- |
| [Write path and gathering](findings/write-path-and-gathering.md) | The arms separated on **write syscalls per pass**, not on the I/O model. Gathering closed the gap: −52% at N=8, −59% at N=64. |
| [Reusing the coalescing buffer](findings/coalescing-buffer-reuse.md) | About 4–7% for the completion transport, from not rebuilding the buffer each pass. |
| [Handing bodies over](findings/handing-bodies-over.md) | `NGHTTP2_DATA_FLAG_NO_COPY` is worth −24% to −31% at 1 MiB on the readiness transport, and a small but real gain on the completion one. |

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
