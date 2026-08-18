# What HTTP/3 over QMux costs against HTTP/2

**Measurements:** [`08-qmux-against-h2`](../data/xeon-8370c-azure/08-qmux-against-h2.md) —
`xeon-8370c-azure`, five passes, ratios formed within each pass.

The cross-protocol arms were added so this question could be asked, and then it was not asked for
several increments: every run before this one compared a build against another build. This is the
first that compares the two stacks.

## The answer is two numbers, not one

A single "QMux is N× slower" is the wrong shape, because the cost divides cleanly into a fixed
part and a marginal part that behave differently:

| | fixed, per exchange | marginal, per MiB of body |
| --- | --- | --- |
| **over a duplex** | +19.4 µs | 1.31× |
| **over a socket** | +34.0 µs | 0.86× |

**The fixed part is the extra layer**, and it is what every small-request figure is made of. QMux
carries records, transport-level flow control and a pump between the transport and the HTTP
framing; an HTTP/2 connection carries framing over a byte stream and nothing else. An exchange
with no body costs 2.6× to 2.9× more, on either substrate.

**The marginal part changes sign with the substrate**, and that is the interesting half. In pure
processor terms QMux costs 31% more per byte. Put a kernel in the path and it costs **14% less**.

## The crossover

Over a real socket the two effects meet between 64 KiB and 1 MiB:

| body | ratio |
| --- | --- |
| empty | 2.61× |
| 1 KiB | 1.70× |
| 64 KiB | 1.21× |
| 1 MiB | **0.89×** |

`transport_body_throughput/1048576` is the only identifier in the suite where a QMux arm beats its
HTTP/2 counterpart, and it is not a marginal win: all five passes fell between 0.88× and 0.90×.

Before the write-path work in the same branch, that identifier was 1.29×. Coalescing a pass's
records into one write is what moved it, and the mechanism is measured separately in
[the QMux write path](qmux-write-path.md).

## What is not explained

**Why QMux moves bulk bytes more cheaply over a socket.** Combining the two families implies the
kernel-path cost is 878 µs per megabyte for HTTP/2 and 536 µs for QMux — 61%. That is arithmetic
on measured numbers and not a mechanism. The candidate is writes per megabyte: QMux fills a 64 KiB
buffer and writes it once, and what HTTP/2's gathering path does per megabyte has not been counted
here. Both stacks already have the instrumentation, so this is cheap to settle and has not been.

**The concurrency inversion.** Concurrency over a socket is QMux's worst case at 3.12–3.14×, and
it is *worse* than the same parameter over a duplex at 2.33×. Every other workload has a smaller
ratio with a kernel in the way than without one. This is the standing lead in
`docs/qmux-h3/pending-work.md`; this run measures it rather than suspecting it, shows it at both 8
and 64 streams in all five passes, and eliminates the mechanism previously suspected — one write
per offered slice, which was removed in the same branch and did not take the inversion with it.

## How to read this at all

It compares **one implementation of each**, not two protocols. The HTTP/2 stack has had its write
path, its body handover and its buffer reuse measured and tuned across several increments; QMux
has had one round. Nothing here licenses a statement about what the QMux draft costs.

And every unremoved confound biases against QMux: a 16382-byte record against a 16384-byte frame
payload, and HTTP/3's control and QPACK streams spending connection-level credit that HTTP/2's
control frames do not. Both are small and both are enumerated on
[`configuration.md`](../configuration.md).
