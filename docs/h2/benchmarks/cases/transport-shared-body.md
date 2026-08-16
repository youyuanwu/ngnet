# `transport_shared_body`

**Family:** real socket — `tests/ngnet-h2-bench/benches/transport_shared_body.rs`

The shared-body comparison on a real socket: does handing bodies over actually beat copying
them? **This is the benchmark the no-copy work is judged on.**

```sh
taskset -c 3 cargo bench -p ngnet-h2-bench --bench transport_shared_body
```

## What it measures

Each arm is paired with its own twin, identical in every respect but the connection entry
point — `handshake_shared_with` versus `handshake_with` — so a difference between a pair is
the body strategy or it is drift.

## Arms and parameters

| Arm | Transport | Body strategy |
| --- | --- | --- |
| `compio-push` | compio, io_uring | copies into libnghttp2's frame buffer |
| `compio-shared` | compio, io_uring | hands the caller's `Bytes` over |
| `tokio-push` | tokio, epoll | copies into libnghttp2's frame buffer |
| `tokio-shared` | tokio, epoll | hands the caller's `Bytes` over |
| `hyper-tokio` | tokio, epoll | untouched — drift control |

Body sizes sweep **0 B, 1 KiB, 64 KiB, 1 MiB**. One runtime per arm, as elsewhere in this
family: sharing a runtime between arms would let one arm's leftover wakeups land inside
another's measurement.

**The completion pair runs first**, and the ordering is now known to be the wrong guess. It
was chosen because that pair was predicted to show the largest effect — its push path pays a
coalescing copy the readiness paths do not. It measures the *smallest* effect, because the
coalescing path had already collapsed a pass into one write and so had no syscall prize left
to win. The order is kept anyway: changing it would make new results incomparable with the
ones already recorded.

## Design rules this case obeys

Set out in full in [`../controls.md`](../controls.md), because this case is where they were
worked out:

1. **The pairs are adjacent**, sizes are the outer loop — never all of one arm and then all of
   the other. This is adjacency, not sample-level interleaving; Criterion samples one benchmark
   to completion before starting the next, and replication covers the rest.
2. **`hyper-tokio` is a drift control.** Nothing in this work touches hyper, so its movement
   between runs is the session's noise floor. The untouched `*-push` twins serve the same role
   for their own transport.
3. **The 0-byte point is a second, mechanistic control.** No body means no memset, no source
   copy and no coalescing copy to remove, so the shared path can only be *level* there. If 0 B
   moves, the harness is measuring something other than what it claims.
4. **Replication with a pre-registered exclusion rule.** The recorded verdict aggregates paired
   deltas over ten independent runs, discarding any replicate whose 0-byte paired delta exceeded
   ±5% — a rule fixed before the results were seen, and reported both with and without.

A gain that shows up here but not in the duplex family ([`shared_body`](shared-body.md)), with
no mechanism to explain the difference, is drift. Two apparent regressions in PR #7 dissolved
exactly that way.

`tests/fixtures_move_their_bytes.rs` asserts every arm echoes every size back at its exact
length, so an arm cannot look faster by moving fewer bytes than its twin.

## Where its numbers are

Recorded runs are indexed in [`../data/README.md`](../data/README.md); the verdict drawn from
them is [`../findings/handing-bodies-over.md`](../findings/handing-bodies-over.md).
