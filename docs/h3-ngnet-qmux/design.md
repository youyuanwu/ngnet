# Hyperium H3 over QMux: design

`h3-ngnet-qmux` adapts an established `ngnet_qmux::io::Connection<S, C>` to
hyperium H3's connection, opener, send, receive, bidi, split, and unframed
traits. Establishing the ordered byte stream, including any endpoint, socket,
TLS, authentication, and runtime policy, remains the caller's responsibility.

## Shared core and driver

The root connection, cloneable openers, stream halves, and one caller-polled
driver share `Arc<Mutex<Core<S, C>>>`. Each active send may retain one
`WriteBuf<Bytes>` directly in its stream state, so resets and terminal fan-out
discard it under the same authoritative lock without a second registry.

The driver is the stable lower-I/O wake target and shutdown-completion owner.
Trait polls perform only immediate Core operations, register their direction's
current waiter when blocked, and notify the driver when they create work. The
crate never spawns and creates no per-stream task. QMux receives one proxy
waker. It records lower readiness while the core is locked and delivers user
wakes only after unlock, preventing inline-waker reentrancy from deadlocking
the mutex.

## Bounded lower seam and routing

The adapter first drains decoded QMux events. It then admits at most one lower
read batch and routes at most 64 events. A partial record schedules exactly one
positive-progress continuation because an already-ready byte stream owes no
second wake. Exhausting the routing budget likewise schedules one continuation;
an idle or credit-blocked turn does not self-wake.

Every current QMux event has an explicit route. `StreamData` can discover a
peer uni or bidi stream before `StreamOpened`; stream-ID initiator and direction
bits classify it. Unknown future event variants fail the connection rather than
disappearing. Completed entries remain retained until the ordered lower
`StreamClosed`, so data already decoded beyond one routing budget is discarded
and credited rather than rediscovering a stream.

## State, credit, and lifecycle

Pending accepts are capped independently of QMux's advertised cumulative stream
allowance. Each stream records directional handles, receive items, stable
terminals, side effects, and independent waiters. Cleanup requires no pending
accept, no handles, no queued receive payload, and an ordered lower close.

Normal delivery extends both the named stream and connection windows at the
moment H3 takes owned bytes. Stop or final receive drop queues one read
shutdown, discards retained data, and extends only connection credit. Future
data on the retired receive side follows the same discard rule.

Framed readiness advances exactly what QMux accepted across every `Bytes`
buffer chunk. Finish drains retained data before one empty FIN. Reset discards
unaccepted framed data before one write shutdown. Directional drop performs
abandonment only for a still-live direction.

Synchronous H3 close records the first reason. The driver subsequently drains
stream/control output, writes QMux close, flushes it, and shuts down the lower
write side. Without another driver poll, delivery is not promised.

## Measurement

The production adapter contains no observation feature or counters. The
benchmark crate wraps both compared adapters with the same per-instance lower-I/O
counter and polls each adapter/H3 pair through one combined endpoint future.
Timings remain whole-stack comparisons. Run 31 is historical evidence from the
superseded asymmetric topology and instrumentation; current conclusions require
the post-revision matched runs. Run 32 records the controlled progress-ownership
A/B that retained driver-only lower I/O.
