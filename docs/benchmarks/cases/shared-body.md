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
