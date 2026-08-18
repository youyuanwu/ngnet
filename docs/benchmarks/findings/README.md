# Findings

Conclusions drawn from the benchmarks, and the mechanisms behind them. A finding belongs here
once it has survived its drift controls; the measurements it rests on stay in
[`../data/`](../data/), and are linked rather than repeated.

| Finding | In short |
| --- | --- |
| [Write path and gathering](write-path-and-gathering.md) | The arms separated on **write syscalls per pass**, not on the I/O model. Gathering closed the gap: −52.2% at N=8, −58.9% at N=64. |
| [Reusing the coalescing buffer](coalescing-buffer-reuse.md) | About 4–7% for the completion transport, from not rebuilding the coalescing buffer every pass. |
| [Handing bodies over](handing-bodies-over.md) | `NGHTTP2_DATA_FLAG_NO_COPY` is worth −24% to −31% at 1 MiB on the readiness transport, and a small but real gain on the completion one. |
| [The QMux write path](qmux-write-path.md) | Coalescing a pass's records into one write is worth **−30% on bodies** and **−8.5% on socket concurrency**, and costs a few percent where there is no payload to amortise it. The same parameter moves the *other* way over a duplex, which is the evidence. A separate change cut allocation a hundredfold and was slower; it was reverted. |

## Where each finding stands

The first two were established on the legacy development host
([`../data/legacy-dev-host/`](../data/legacy-dev-host/)) and have **not** been reproduced on
the current one. The third has: it was re-measured on `xeon-8370c-azure` in
[`03-shared-body`](../data/xeon-8370c-azure/03-shared-body.md), which confirmed the readiness
verdict and **overturned the completion one** — it had failed on a misbehaving control arm
rather than on its own delta.

That the legacy figures have not been reproduced does not make them wrong — each rests on
paired deltas against drift controls measured in the same session, which is the part that
travels between machines — but it does mean **the percentages are that host's, not this
project's**, and a new host may put different numbers on the same mechanism. The shared-body
re-measurement is the worked example: same sign, same ordering, magnitudes about a fifth
smaller.

What a re-measurement should be asked to reproduce is the *shape*: the sign, the ordering, the
points that must not move, and the mechanism's own prediction. Each finding states those
explicitly under **What a new machine should reproduce**, so a run on new hardware either
confirms it or falsifies it rather than merely producing a different number.
[`02-first-survey`](../data/xeon-8370c-azure/02-first-survey.md) works through the write-path
finding's list and marks one of its four claims as not carried over.

## Two claims that are structural rather than measured

Recorded here so they are not mistaken for measurements waiting to be redone:

- **The write-primitive split, the transport-supplied policy default, and the capability
  change that replaced `Config::write_policy` with `TransportWrite::is_write_vectored`** were
  argued structurally and pinned by counts, not by timings. For those questions counts are the
  stronger evidence. The capability change does move a count — see
  [`../allocation-counts.md`](../allocation-counts.md) — but it moves no timing on record,
  because the two shipped adapters both declare `true` and therefore occupy exactly the arms
  already measured.
- **Dropping the `split().freeze()` from the readiness coalesced drain** removes a pair of
  atomic refcount operations per pass. No allocation counter can see it and no benchmark here
  isolates it; the claim is structural and is recorded as such in
  `the_coalesced_write_path_reuses_its_coalescing_buffer`.
