# `shared_body`

**Family:** duplex — `tests/ngnet-bench/benches/shared_body.rs`

The opt-in no-copy body path against the push path, over a duplex: the same question as
[`transport_shared_body`](transport-shared-body.md), asked without a socket in the way.

```sh
cargo bench -p ngnet-bench --bench shared_body
```

## What it measures

A connection built through the opt-in `handshake_shared`/`serve_shared` entry points hands its
bodies to libnghttp2 as the caller's own `Bytes` rather than copying them into the frame
buffer, and those frames serialise with `NGHTTP2_DATA_FLAG_NO_COPY`. Each arm here is
identical to its twin but for the connection entry point.

There is no `writev`, no io_uring and no loopback here, so the only thing handing a body over
can remove is CPU: the memset of libnghttp2's frame buffer and the source-side copy into it.
**That makes this family the mechanism check for the socket family's result** — a gain that
appears there and not here needs a socket-level explanation, and a gain that appears here and
not there needs an explanation for why the socket swallowed it. Without one, it is drift.

## Arms and parameters

| Arm | Body strategy |
| --- | --- |
| `ngnet-h2-push` | copies into libnghttp2's frame buffer |
| `ngnet-h2-shared` | hands the caller's `Bytes` over, `NGHTTP2_DATA_FLAG_NO_COPY` |
| `hyper-tokio` | untouched by this work — carried as a drift control |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**, the same points as every other sweep.

## Why there is no QMux arm here

Every other duplex case gained an HTTP/3-over-QMux arm. This one did not, and the reason is
specific to what this case measures rather than to anything wrong with the QMux stack.

**There is no counterpart mechanism to put in the arm.** The two arms above differ in exactly
one thing: whether the body is copied into libnghttp2's frame buffer or handed over as the
caller's own `Bytes` and serialised with `NGHTTP2_DATA_FLAG_NO_COPY`. That flag, that frame
buffer and the `handshake_shared`/`serve_shared` entry points that opt into avoiding it are
libnghttp2 constructs. `ngnet-qmux-h3` has no equivalent pair of entry points, no equivalent
flag, and therefore no second arm to be the first arm's twin.

A QMux arm here could only be a *third* strategy standing beside the pair, not a member of it —
and this case's whole design is the paired-twin comparison described below, in which the 0-byte
point is a mechanistic control precisely because the two arms are identical but for the entry
point. Adding an arm that shares no mechanism with either twin would put a number on the page
that the case's controls do not cover and that its finding could not be read against.

**This reason is not the reason
[`concurrent_throughput_multi_thread`](concurrent-throughput.md) has no QMux arm, and the two
must not be filed together.** There, the mechanism exists and the arm was written; it was left
out because it hangs, which is a recorded defect on
[`../../qmux-h3/pending-work.md`](../../qmux-h3/pending-work.md). Here nothing is broken and
nothing is pending: there is simply no such thing to measure. If `ngnet-qmux-h3` ever grows an
opt-in no-copy body path of its own, this case gets a twin pair for it — a fourth and fifth
arm, not one arm — and the omission ends for a reason unrelated to the other.

## Design rules this case obeys

The full reasoning is in [`../controls.md`](../controls.md); in brief:

- **Arms are paired and adjacent, sizes are the outer loop**, so a twin pair sits as close
  together in time as Criterion allows.
- **`hyper-tokio` is a drift control.** Its movement between runs is the session's noise floor
  and the bar a claimed gain has to clear.
- **The 0-byte point is a mechanistic control.** With no body there is nothing to copy, so the
  two arms *cannot* legitimately differ there.

`tests/fixtures_move_their_bytes.rs` asserts every arm echoes every size back at its exact
length, so an arm cannot look faster by moving fewer bytes than its twin.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md); the conclusion drawn
from them is [`../findings/handing-bodies-over.md`](../findings/handing-bodies-over.md).
