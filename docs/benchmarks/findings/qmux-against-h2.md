# What HTTP/3 over QMux costs against HTTP/2

**Measurements:** [`08-qmux-against-h2`](../data/xeon-8370c-azure/08-qmux-against-h2.md) —
`xeon-8370c-azure`, five passes, ratios formed within each pass. The mechanisms behind those
ratios are [`09-qmux-h2-mechanisms`](../data/xeon-8370c-azure/09-qmux-h2-mechanisms.md), which
answered both questions `08` left open by counting rather than timing.

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

**The fixed part is mostly not QMux**, which is the opposite of what this finding said before
[`09`](../data/xeon-8370c-azure/09-qmux-h2-mechanisms.md) looked. An exchange with no body costs
2.6× to 2.9× more on either substrate, and it is natural to attribute that to the layer QMux adds
— records, transport-level flow control, a pump between the transport and the HTTP framing. Time
attributed by layer says otherwise. Of the +18.6 µs on a duplex, the QMux transport and its record
framing account for **3.2 µs, 17%**. The largest single term, at **8.3 µs and 45%**, is
`ngnet-h3`, which costs 2.7× what `ngnet-h2` costs to carry an exchange containing no bytes, and
which is shared with the QUIC stack and contains no QMux code at all.

About a quarter of *that* is one line: `close_stream` opens with a linear scan of a 1024-entry
tombstone `Vec`, which every connection that has closed more than 1024 streams pays on every
close. Shortening the list is worth 11.7% of an empty exchange and 19.9% of a 64-stream one. It
is a defect rather than a design cost, and it is recorded as one in
[`../../h3/pending-work.md`](../../h3/pending-work.md).

**The marginal part changes sign with the substrate**, and that is the interesting half. In pure
processor terms QMux costs 31% more per byte — and that part *is* the record layer: building,
writing headers for and scanning 256 records per megabyte-exchange is 105 µs of the 156 µs gap,
two thirds of it. Notably the HTTP/3 driver is 27.5 µs *cheaper* than the HTTP/2 one here, so its
penalty is per exchange and does not scale with the body.

Put a kernel in the path and QMux costs **14% less** per byte, and the reason is a write count:
**68 writes per megabyte-exchange against HTTP/2's 189.** HTTP/2 never issues a write larger than
one frame — 16.0 KiB, measured as its maximum over 3,774 writes — while QMux fills a 64 KiB buffer
and empties it, averaging 30.1 KiB. Same bytes, 2.8× fewer calls to move them, worth roughly
2.7 µs per avoided write on this loopback: enough to pay off QMux's extra processor cost and leave
the margin `08` measured.

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

## The concurrency inversion, which was the last thing unexplained

Concurrency over a socket is QMux's worst case at 3.12–3.14×, and it is *worse* than the same
parameter over a duplex at 2.33× — the only workload in the suite where adding a kernel makes
QMux's position worse rather than better. [`09`](../data/xeon-8370c-azure/09-qmux-h2-mechanisms.md)
counted why:

| streams | HTTP/2 writes | QMux writes |
| --- | --- | --- |
| 1 | 2 | 4 |
| 8 | 2 | 18 |
| 64 | 2 | 132 |

**QMux's write count grows with the stream count and HTTP/2's does not.** Reads do not: QMux takes
3 reads at every concurrency, so the sixty-four responses arrive perfectly coalesced. The bytes are
collectable; only the writer will not collect them. Over a duplex each extra write is a memcpy, so
the penalty is mild; over a socket each is a syscall, so it grows — which is the inversion.

The cause is correctness, not oversight. `ngnet-h3` applies control-plane events before data events
within a batch, so a stream ending sharing a batch with that stream's last bytes would release the
stream before the bytes were read. QMux's `poll_event` therefore returns `Pending` at every stream
ending to start a fresh batch — and a `Pending` ends the driver's turn, which is exactly what forces
the outbound buffer to flush. One ending, one flush, one write, on each side: `2n + 2`. Deleting the
rule to confirm this breaks the connection in precisely the way its own doc comment predicts.

This also **corrects a claim** the earlier write-path work left behind. That work established the
write count per *driver turn* no longer grows with the streams in flight, which is true. The number
of driver turns does.

## What is still not explained

Nothing from this pair of runs. What remains is scope rather than mystery: `ngnet-quic-h3` shares
`ngnet-h3` and so should pay the same driver cost and the same tombstone scan, but this host cannot
build the QUIC stack, so that is an inference from shared source rather than a measurement. And
whether HTTP/2 would take the megabyte back by coalescing past 16 KiB is untested — that its writes
are capped is measured; that raising the cap would pay is not.

## How to read this at all

It compares **one implementation of each**, not two protocols. The HTTP/2 stack has had its write
path, its body handover and its buffer reuse measured and tuned across several increments; QMux
has had one round. Nothing here licenses a statement about what the QMux draft costs.

And every unremoved confound biases against QMux: a 16382-byte record against a 16384-byte frame
payload, and HTTP/3's control and QPACK streams spending connection-level credit that HTTP/2's
control frames do not. Both are small and both are enumerated on
[`configuration.md`](../configuration.md).
