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

## Body bytes are copied twice

The HTTP/3 layer lends its buffers through `StreamSource::write_next` and the slices are
invalid once the closure returns, so this crate cannot take ownership of them. `ngnet-quic`
then stages its own copy, because ngtcp2 keeps the pointer it is handed until the peer
acknowledges. So each body byte is written into a packet buffer and also held in a retained
copy.

The second copy is unavoidable given ngtcp2's contract. The first — from nghttp3's buffer
into the packet — is not obviously avoidable either, since that is what serialising into a
datagram means. This is noted because "one copy of every byte sent" is the figure recorded in
`docs/quic/pending-work.md`, and a reader tracing an HTTP/3 body through the stack should
know where each copy is rather than assuming there is only one.

**What would settle it:** measurement. There are no benchmarks, so the cost is currently a
description rather than a number.

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

## Inbound datagrams are dropped rather than queued without bound

When a connection's inbound queue is full the endpoint drops the datagram, because it reads
one socket on behalf of every connection and waiting for a slow consumer would starve the
rest. `DetachedConnection::dropped_inbound` counts these, and `NgtcpConnection` exposes it.
Nothing acts on the count.

**What would settle it:** knowing whether it ever becomes non-zero under load, which needs a
load test that does not exist.
