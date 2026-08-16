# HTTP/3 over QMux: invariants

The properties this crate pins rather than merely exercises, and where each is enforced.

Unlike `ngnet-quic-h3`, this crate ships **no structural test of its own**: there is no
`tests/invariants.rs` reading its source back. What holds the claims below is a mixture of the
compiler, the translation suite in `crates/ngnet-qmux-h3/tests/translation.rs`, the end-to-end
suite in `tests/ngnet-qmux-h3-tests/`, and the workspace-level checks. A structural suite
mirroring the QUIC one is recorded in `pending-work.md`; the claims it would carry are marked
below as resting on the compiler or on review rather than on a test.

## Enforced by the compiler

| Property | How |
| --- | --- |
| **Nothing here is `unsafe`** | `#![deny(unsafe_code)]` in `lib.rs`, with **no allowance list at all** — unlike both crates it depends on. This crate joins two safe APIs and has no foreign boundary of its own, so an `unsafe` block appearing here would mean the thing needing it belongs in `ngnet-qmux` or `ngnet-h3`, behind their allowance lists, where the reasoning about the C library's contract already lives. The connection future sees through its own `Pin` without a projection, because every field is `Unpin`. |
| **Everything public is documented** | `#![deny(missing_docs)]`, and CI builds the documentation with `RUSTDOCFLAGS='-D warnings'`. |
| **The error type satisfies the transport abstraction's bound** | A `const` block in `connection.rs` asserts `Error: Into<Box<dyn Error + Send + Sync>>`. `ngnet-h3` requires it of any transport, and the crate compiling at all is the demonstration that a byte-stream failure can reach the HTTP/3 layer as one. |
| **Neither surface imposes `Send`** | `tests/portability.rs` in `ngnet-qmux-h3-tests`. `both_surfaces_build_over_a_non_send_byte_stream` compiles `connect` and `serve` over an `Rc`-based byte stream, and `the_transport_follows_its_byte_stream_rather_than_imposing_send` asserts the property holds in both directions rather than only the permissive one. `a_non_send_connection_completes_an_exchange` then runs one, because a bound that compiles and a connection that works are different claims. |

## Enforced about the translation

In `crates/ngnet-qmux-h3/tests/translation.rs`, which drives a client and a server over the
in-memory byte-stream pair and reads what each surfaced.

| Property | Where |
| --- | --- |
| **Only peer-opened bidirectional streams are announced** | `only_peer_opened_bidirectional_streams_are_announced`. Announcing a peer's unidirectional stream would tell the HTTP/3 layer to answer on a stream that exists to be read, and nghttp3 identifies those from the stream-type prefix itself. |
| **A zero-length end-of-stream survives translation** | `an_empty_final_delivery_is_not_swallowed`. It is how a peer that has already sent everything ends a stream, and suppressing it as "an empty event" leaves a request body that never ends. |
| **Every accepted byte is released exactly once** | `accepted_bytes_are_released_exactly_once`, over a payload large enough that each stream's total is made of several accepts. The assertion is an equality against what the writes accepted, so it fails on under- and over-reporting alike; a one-sided check would pass while the second bug was live. It also asserts the release is attributed to the stream that carried the bytes, and that the peer received each byte once. |
| **A peer's application error code arrives intact, per direction** | `a_peer_reset_arrives_with_its_code`, `a_peer_stop_sending_arrives_with_its_code`, and `a_stream_close_carries_a_code_for_each_direction`. The last is where `None` and `Some(0)` are different answers — a stream that ended, and a stream reset with the code zero — and normalising them would lose the distinction at the layer that acts on it. |
| **A close reaches the peer with its code** | `a_close_reaches_the_peer_with_its_code`. `QuicConnection::close` writes nothing, so a close that was encoded and never flushed would satisfy every check that only asked what this side did. This one reads it off the other end. |
| **A peer that closes is an event, not a failure** | `a_peer_that_closes_is_an_event_and_not_a_failure`. The orderly/failed split decides whether the HTTP/3 driver winds down or reports a protocol error, and getting it backwards turns every polite disconnection into a server-side failure. |

## Enforced end to end

In `tests/ngnet-qmux-h3-tests/`, over the in-memory pair and over a loopback TCP socket.

| Property | Where |
| --- | --- |
| **A first request on a fresh connection completes** | `exchange.rs`, `the_first_request_on_a_fresh_connection_completes`. This is the pump's whole justification: without it the driver waits for stream capacity that arrives in a record nobody reads. It fails by hanging, which is why it is asserted rather than assumed. |
| **Concurrent requests each get their own response** | `multiplexing.rs`, `eight_concurrent_requests_each_receive_their_own_response`, each response matched to its request rather than merely counted. |
| **A body past both windows transfers in both directions** | `large_body.rs`, `a_body_larger_than_the_windows_completes_in_both_directions`. Exercises the credit-exhausted write path, the record split, and the release accounting together over a payload none of them can shortcut. |
| **A reset mid-body fails only its own request** | `reset.rs`, `a_peer_reset_mid_body_fails_only_its_own_request`, with a later request on the same connection completing. That is the assertion that catches an absorbed refusal having been propagated instead. |
| **An abandoned body informs the peer and leaves the connection usable** | `abandoned.rs`, `an_abandoned_request_body_informs_the_peer_and_leaves_the_connection_usable`. |
| **A close through the HTTP/3 layer reaches the peer** | `closing.rs`, `a_close_through_the_http3_layer_reaches_the_peer` and `a_served_connection_also_closes_when_it_is_done`. `a_connection_dropped_before_its_tail_tells_the_peer_nothing` pins the other half — that the tail is what does it, so a future that stops being polled cannot be mistaken for one that ran. |
| **A client that disappears is not a protocol failure** | `loopback.rs`, `a_client_that_disappears_is_not_a_protocol_failure`, over a real socket, because a peer that vanishes is a property of the transport rather than of the harness. |
| **A request completes over loopback TCP** | `loopback.rs`, `a_request_completes_over_loopback_tcp`. The in-memory pair is a legitimate deployment of QMux rather than a stand-in, but it preserves write boundaries a socket does not. |

## Enforced about the workspace

In `tests/ngnet-workspace-tests/`.

| Property | Why it is asked this way |
| --- | --- |
| **This crate reaches both families, and no third thing** | `dependency_graph.rs`, `the_qmux_adapter_depends_on_both_families`. The positive half is stated separately on purpose: leaving the crate off the forbidden lists would be satisfied equally well by a crate that had lost one of its halves, or by no crate at all. The same test forbids `ngnet-quic`, `ngnet-quic-sys`, `quinn` and `openssl-sys` — joining HTTP/3 to QMux needs no QUIC implementation and no TLS. |
| **Nothing established reaches it** | `dependency_graph.rs`, `no_existing_crate_reaches_qmux`, which names `ngnet-qmux-h3` alongside the other two QMux crates. All three are unpublished and track an unratified draft, so anything depending on them inherits that churn. |
| **No QMux binary links OpenSSL, this one included** | `linkage.rs`, `qmux_links_no_tls_in_any_configuration`, via `readelf`. A native library arrives through link flags a build script emits, which no manifest inspection can see. This crate is inspected alongside the two it sits on because it is the one place the QMux family meets a family that *does* have a TLS backend. |

## What is not enforced

Stated so a reader does not infer it from the pages next door.

- **Module files being flat, and nothing being `include!`d.** Both are true of this crate and
  both are checked in `ngnet-quic-h3` by a structural suite this crate does not have.
- **The manifest's dependency list.** `ngnet-quic-h3`'s suite pins its three; nothing pins this
  crate's five — `ngnet-h3`, `ngnet-qmux`, `bytes`, and `http` and `http-body`, which appear
  only in the bounds `ngnet_h3::http::serve` and `handshake` impose and which `ngnet-h3`
  already compiles. The workspace graph test would catch a new *family* arriving, not a new
  utility crate.
- **No sockets, threads or processes named here.** True by inspection — a byte stream and a
  clock arrive as arguments — but nothing fails if that stops being true.
