# HTTP/3 over ngtcp2: pending work

What is missing, and what would settle each.

## Interoperability is proven against one implementation

`tests/ngnet-quic-h3-tests/tests/interop.rs` runs this stack against **quinn**: a bare QUIC
handshake with no HTTP/3 involved, HTTP/3 requests in both directions, and a 512 KiB payload
crossing each way byte for byte. A negative test confirms an untrusted certificate is
refused, so the positive results are not an artefact of verification being off.

That is one implementation. quiche, msquic, picoquic and browsers are untried, and so are the
conditions a loopback socket does not produce: real loss, reordering, path changes, and peers
whose transport parameters differ meaningfully from quinn's.

**What would settle it:** entering the QUIC Interop Runner, which exercises exactly these
against a matrix of implementations.

## A packet that carries no STREAM frame is a block, not an acceptance

`ngtcp2_conn_writev_stream` may produce a packet containing no STREAM frame for the offered
stream, because something else was queued ahead of it — an acknowledgement, a retransmission, a
window update. It says so by leaving `*pdatalen` at `-1`, and it is a different thing from the
*zero-length* STREAM frame it writes for an offer carrying nothing but `fin`, which is
`*pdatalen == 0`.

`ngnet-quic` used to clamp the sign, so both arrived here as `Datagram { accepted: 0 }` and
`transmit::drain` reported `WriteOutcome::Accepted(0)`. For a `fin`-only offer that is a
commitment: `Offers::write_next` commits an acceptance and marks the stream ended
([`../../crates/ngnet-h3/src/http/driver.rs`](../../crates/ngnet-h3/src/http/driver.rs), the
`Accepted(taken)` arm), so the FIN was recorded as sent on a packet that never carried it. The
arm immediately above it — the one whose comment reads "a transport that declined it leaves the
peer waiting for an end that never comes" — was written for exactly this and was unreachable,
because the transport never reported a block for it.

The transport now reports `StreamWrite::DatagramWithoutStream`, `transmit::drain` maps it to
`WriteOutcome::Blocked`, and the offer is abandoned and made again rather than committed. The
drain pass is deliberately *not* ended on it: `Offers::write_next` only unblocks and re-offers a
stream when it is asked again, once per pass, and the packet just produced may be a bare
acknowledgement — not ack-eliciting, so nothing is coming back to prompt another pass.

The deterministic reproduction of the transport's half is
[`../../crates/ngnet-quic/tests/fin_delivery.rs`](../../crates/ngnet-quic/tests/fin_delivery.rs);
the end-to-end story, including which stack this actually stalled, is in
[`../h3-ngnet-quic/pending-work.md`](../h3-ngnet-quic/pending-work.md).

**This is not the large-body stall and does not fix it.** That was re-measured afterwards and
survives; see the next section.

## The intermittent large-body failure survives, and it reaches the test suite

Run 30's outer-driver liveness failure — S9 — is still open. It was re-measured on
`epyc-7763-azure` after the FIN change above, release build, pinned to one core, against the
new `h3-ngnet-quic` arm over the identical transport, fixtures, payload and exchange count
(200 x 16 KiB): this stack failed 2 of 20, the hyperium-over-ngtcp2 stack 0 of 20. The failures
were `ErrorKind::Closed`, "the connection has ended", not timeouts. Transport held fixed and
HTTP/3 layer varied, so it belongs here.

A later revision — not ending the drain pass on a stream-less packet, so `Offers::write_next` is
asked again and unblocks the displaced stream within the pass — was then measured over 50 runs
of the same workload with no failure. That is a plausible mechanism for part of this defect and
is **not** a claim that it is fixed: fifty clean runs make a 10% rate unlikely and leave a 2%
rate entirely plausible, and S9's original evidence is a different workload on a different host,
which nothing here re-examined.

`tests/ngnet-bench/tests/ngtcp2_fixture.rs::unarmed_and_armed_diagnostics_preserve_and_reconcile_echoes`
drives 16 KiB bodies through this stack and can therefore fail the same way. It was seen to do
so once, with that exact signature, during a full `cargo test --workspace --all-features` run —
the contended case, with every test binary competing for four cores.

It was **not** reproduced afterwards, and the attempt to attribute it is recorded rather than
skipped: 12 isolated runs and 20 runs under deliberate CPU contention, on this branch and on
unmodified `main`, gave 64 runs and no failure on either. So there is no measured difference
between the two, the signature is S9's, and nothing here claims otherwise in either direction.
It is not attributed to the FIN change, and it is not claimed fixed.

## Body bytes are copied twice

The HTTP/3 layer lends its buffers through `StreamSource::write_next` and the slices are
invalid once the closure returns, so this crate cannot take ownership of them. `ngnet-quic`
then stages its own copy, because ngtcp2 keeps the pointer it is handed until the peer
acknowledges. So each body byte is written into a packet buffer and also held in a retained
copy.

The retained copy is unavoidable given ngtcp2's contract. It is now bounded to one sampled
maximum-UDP-payload prefix per attempt rather than the caller's complete outstanding body.
The other copy — serialising nghttp3's bytes into the packet — is not obviously avoidable,
since that is what constructing a datagram means.

Run [`27`](../benchmarks/data/xeon-8370c-azure/27-ngtcp2-packet-bounded-staging.md) records
historical staged backing, accepted progress, exactness, and sampled RSS. Final-review
[`run 30`](../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md) records
fresh diagnostic timeouts, so current persistent stability and the RSS envelope remain unmet.
Neither record isolates copy CPU cost from packet protection, endpoint, adapter, or generic
HTTP/3 work.

**What would settle the remaining copy:** route ownership-taking HTTP/3 buffers through
`Conn::write_stream_owned`, change release to acknowledgement, and change
`RETAINS_BUFFERS` to `true`, then measure against the accepted packet-bounded origin. That is
a buffer-accounting change, not a drop-in optimization.

## A detached close is sent once, without retransmission

`NgtcpConnection::close` writes one CONNECTION_CLOSE and queues it. The endpoint's managed
path keeps its close datagram for the closing period, because `write_connection_close`
returns nothing once a connection is closing and a close that must be answered again cannot
be regenerated. The detached path does not do this, so a lost close leaves the peer waiting
out its idle timeout instead of closing promptly.

Legal, and invisible in ordinary use. Worth fixing when the detached path grows a closing
period of its own.

**What would settle it:** keeping the close datagram in the connection's outbound queue
until the endpoint evicts it, rather than releasing immediately.

## No datagram or WebTransport support

Neither `ngnet-quic` nor `ngnet-h3` exposes unreliable datagrams, so this crate cannot. See
both families' pending-work documents.

## Stream priority is not exposed

nghttp3 supports the HTTP/3 priority scheme and ngtcp2 has no opinion about it, but the
transport trait has no priority concept, so nothing here can carry one.

## The connection is not observable

There is no way to ask a live connection about its round-trip time, congestion window, loss
rate or which path it is on. `ngnet-quic`'s `Conn` exposes some of this and the trait does
not carry it, so a caller holding an `NgtcpConnection` cannot reach it either.

**What would settle it:** deciding whether that belongs on this crate's own type, which is
reachable before the connection is handed to the HTTP/3 layer but not afterwards.

## The detached hand-over queue is unbounded

A connection that completes its handshake is placed in a queue for whoever asked for it. A
caller who stops accepting leaves connections there, each holding its protocol and TLS state,
and the endpoint keeps routing to them. A caller who *drops* a pending `connect_detached` is
handled — the wait releases what it never collected — but a server that simply stops calling
`accept` is not.

This is the same shape as the bounded-accept-backlog item in `docs/quic/pending-work.md` and
should be fixed with it: a server under load is exactly where both bite.

**What would settle it:** an accept permit with a bound, releasing connections that overflow
it rather than holding them.

## Inbound datagrams are dropped rather than queued without bound

When a connection's inbound queue is full the endpoint drops the datagram, because it reads
one socket on behalf of every connection and waiting for a slow consumer would starve the
rest. `DetachedConnection::dropped_inbound` counts these, and `NgtcpConnection` exposes it.
Nothing acts on the count.

The persistent diagnostic workload now exercises 125/250/500 sequential 1 MiB exchanges.
Before the endpoint yielded after each receive batch, one 1 MiB run recorded 73 unexpected
drops. Later quiet-path processes reported zero drops, including the final-review attempts
that timed out for a different reason. A deterministic induced-drop test now fills the
64-datagram inbound queue, accounts for the expected discarded packets, and inventories the
queued datagrams when the owner marks the connection terminal.

**What would settle the broader network behavior:** a deliberately slow detached consumer and
multiple connections sharing one socket, with loss recovery treated as the expected result
rather than a benchmark invalidation.

## Packet ordering and residual optimizations are deferred

Transport-first pumping remains in place. Existing counters cannot establish that a
standalone packet could have carried pending stream data, and the origin timing spread did
not leave a predeclared target beyond drift.
[Run 28](../benchmarks/data/xeon-8370c-azure/28-ngtcp2-stream-first-gate.md) records the
unchanged decision.

[Run 29](../benchmarks/data/xeon-8370c-azure/29-ngtcp2-residual-eligibility.md) observes linear
owned-datagram, timer-rearm, and socket-call proxies, but no stable attributed timing gap.
Detached recycling, timer reuse, syscall batching/additional coalescing, and crypto-path
changes are therefore deferred/not evidenced, not implemented.
