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
| **A list of fragments is one offer and one run of records** | `a_list_of_fragments_is_offered_once_and_taken_whole`. `StreamSource::write_next` may lend several slices, and this crate submits the list in one vectored call rather than one call per slice — a call begins a record, so a call per slice cost a record per slice. The test asserts the call count, the whole take, the peer's bytes in order, and the release against the same total, because the release is what a change here could break quietly. What the packing is worth on the wire is a count, and it is pinned in `tests/ngnet-qmux-h3-tests/tests/fragmented_offers.rs`. |
| **A source may reclaim its fragments the moment a write returns** | `a_source_may_invalidate_its_fragments_once_the_write_returns`, which overwrites them before anything is flushed. `RETAINS_BUFFERS = false` is a promise to the HTTP/3 layer that the vectored form has to keep as well as the single-slice one; dwnx copies the vectors into the record during the call, and this is the check that reading the C was not enough on its own. |
| **An empty offer costs a record only when it carries the end-of-stream** | `an_empty_offer_costs_a_record_only_when_it_carries_the_end_of_stream`, measured against the byte stream's write log because nothing else can tell an offer that produced nothing from a record nobody looked at. The short-circuit in `transmit.rs` is conditioned on the marker's *absence* for this reason: an otherwise-empty offer carrying the marker is the only way a stream that has finished writing is ever ended. `a_trailing_empty_fragment_does_not_take_the_end_of_stream_marker` is the converse — an empty fragment at the end of a real payload must not take the marker away from it. |
| **Every accepted byte is released exactly once** | `accepted_bytes_are_released_exactly_once`, over a payload large enough that each stream's total is made of several accepts. The assertion is an equality against what the writes accepted, so it fails on under- and over-reporting alike; a one-sided check would pass while the second bug was live. It also asserts the release is attributed to the stream that carried the bytes, and that the peer received each byte once. |
| **A peer's application error code arrives intact, per direction** | `a_peer_reset_arrives_with_its_code`, `a_peer_stop_sending_arrives_with_its_code`, and `a_stream_close_carries_a_code_for_each_direction`. The last is where `None` and `Some(0)` are different answers — a stream that ended, and a stream reset with the code zero — and normalising them would lose the distinction at the layer that acts on it. |
| **A close reaches the peer with its code** | `a_close_reaches_the_peer_with_its_code`. `QuicConnection::close` writes nothing, so a close that was encoded and never flushed would satisfy every check that only asked what this side did. This one reads it off the other end. |
| **A peer that closes is an event, not a failure** | `a_peer_that_closes_is_an_event_and_not_a_failure`. The orderly/failed split decides whether the HTTP/3 driver winds down or reports a protocol error, and getting it backwards turns every polite disconnection into a server-side failure. |
| **A stream ending keeps its own event batch** | `final_data_and_stream_close_are_separated_by_a_woken_boundary`. The transport trace covers both nonempty and zero-length final `Data`, a self-woken `Pending`, then `StreamClosed`, so the close cannot overtake the last bytes. `exchange.rs`'s `a_response_head_and_its_final_data_settle_before_stream_close` is the end-to-end companion through the HTTP/3 driver. |
| **An active event branch selects one initial pump** | `an_empty_event_poll_uses_one_pump_and_one_transport_read` fails on the old explicit-plus-lower empty branch and proves its pending read owns the wake. `a_queued_release_uses_one_pump_and_precedes_a_new_terminal_error` pins the queued-release and terminal-error order. `final_data_and_stream_close_are_separated_by_a_woken_boundary` also counts the held translated-event branch. Later lower polls after untranslated events remain progress iterations rather than duplicates. |
| **Suspension flushes once without inventing a wake loop** | `a_suspension_flush_parks_on_backpressure_and_finishes_after_its_wake` and `a_flush_wakes_once_for_a_new_ending_and_never_spins_on_it`. A forced flush drains retained output or registers the byte stream's wake; an ending discovered there wakes the interrupted operation exactly once. |

## Enforced at the shared HTTP/3 transport seam

| Property | Where |
| --- | --- |
| **Every real driver suspension first polls the transport flush** | `crates/ngnet-h3/tests/http_flush.rs`. A recording transport proves the pending operation precedes the flush at all four sites, and each site covers ready, pending-with-wake, and explicit-error outcomes. The QMux-specific drain, backpressure, and ending behavior is enforced by the translation tests above. |

## Enforced end to end

In `tests/ngnet-qmux-h3-tests/`, over the in-memory pair and over a loopback TCP socket.

| Property | Where |
| --- | --- |
| **A first request on a fresh connection completes** | `exchange.rs`, `the_first_request_on_a_fresh_connection_completes`. This is the pump's whole justification: without it the driver waits for stream capacity that arrives in a record nobody reads. It fails by hanging, which is why it is asserted rather than assumed. |
| **Concurrent requests each get their own response** | `multiplexing.rs`, `eight_concurrent_requests_each_receive_their_own_response`, each response matched to its request rather than merely counted. |
| **A body past both windows transfers in both directions** | `large_body.rs`, `a_body_larger_than_the_windows_completes_in_both_directions`. Exercises the credit-exhausted write path, the record split, and the release accounting together over a payload none of them can shortcut. |
| **A reset mid-body fails only its own request** | `reset.rs`, `a_peer_reset_mid_body_fails_only_its_own_request`, with a later request on the same connection completing. That is the assertion that catches an absorbed refusal having been propagated instead. |
| **A response body that fails is never read as a whole one** | `reset.rs`, `a_response_body_that_fails_with_nothing_queued_behind_it_still_fails_the_callers_read`. The one above it arranges a backlog, so the reset has queued bytes to discard and the truncation is visible either way; this one leaves nothing queued, which is the shape that used to hand the caller a complete-looking message. The decision is `ngnet-h3`'s and is pinned there too, but a join is where a real transport gets to disagree with it. |
| **An abandoned body informs the peer and leaves the connection usable** | `abandoned.rs`, `an_abandoned_request_body_informs_the_peer_and_leaves_the_connection_usable`. |
| **A close through the HTTP/3 layer reaches the peer** | `closing.rs`, `a_close_through_the_http3_layer_reaches_the_peer` and `a_served_connection_also_closes_when_it_is_done`. `a_connection_dropped_before_its_tail_tells_the_peer_nothing` pins the other half — that the tail is what does it, so a future that stops being polled cannot be mistaken for one that ran. |
| **A client that disappears is not a protocol failure** | `loopback.rs`, `a_client_that_disappears_is_not_a_protocol_failure`, over a real socket, because a peer that vanishes is a property of the transport rather than of the harness. |
| **The HTTP/3 driver does not coalesce its credit reports** | `credit_batching.rs`, `the_http3_driver_does_not_coalesce_its_credit_reports`. The answer Spec FR-037 asked for, held as a count rather than as a reading of the driver's loop: a driver that started batching would fail this test, and that failure is the finding being overturned rather than a defect. |
| **A run of credit reports becomes one connection-window extension** | `credit_batching.rs`, `a_run_of_credit_reports_reaches_the_connection_as_one_extension_per_window`. Eight concurrent bodies, so a pass reports the shared window many times over; the assertion is a ratio rather than a fixed figure because how many deliveries a body arrives in is not this test's business. |
| **A request completes over loopback TCP** | `loopback.rs`, `a_request_completes_over_loopback_tcp`. The in-memory pair is a legitimate deployment of QMux rather than a stand-in, but it preserves write boundaries a socket does not. |
| **Concurrent empty exchanges do not restore a per-stream write** | `concurrent_driver_writes.rs`, at 1, 8, and 64 streams. The test requires a nonempty server log, applies absolute limits of 7/12/12, and also requires n=8 and n=64 to remain within 2 and 4 writes of n=1; the current result is 5/5/5. A return to the old linear client or server term fails. |

## Enforced about the configuration passthrough

In `tests/ngnet-qmux-h3-tests/tests/config.rs`. Every assertion here is made against what the
**peer** received rather than against what this end was told, because a configuration that is
accepted, stored and then not sent leaves every exchange working exactly as it did — a test
that only checked an exchange succeeded would pass over a passthrough that dropped its argument
on the floor. The observer is a bare `ngnet_qmux::io::Connection` opposite the subject, which
sees the transport parameters as `Event::PeerTransportParams` and the HTTP/3 SETTINGS frame as
ordinary control-stream bytes it decodes. That is a stronger claim than any accessor on
`QmuxConnection` would give, and it is available today, whereas the accessor is not — see the
observability entry in `pending-work.md`.

| Property | Where |
| --- | --- |
| **A supplied transport configuration reaches the peer, in both roles** | `a_server_advertises_the_transport_configuration_it_was_given` and `a_client_advertises_the_transport_configuration_it_was_given`. Both roles are asserted because they are separate code paths and a passthrough wired into one of them is the likely half-fix. |
| **A supplied HTTP/3 configuration reaches the peer's SETTINGS** | `the_http3_configuration_reaches_the_connections_settings`. Transport parameters and SETTINGS travel by entirely different means, so the transport assertion says nothing about the HTTP half. |
| **A connection built through the new entry points still exchanges** | `a_connection_built_with_a_supplied_configuration_exchanges_bodies_both_ways`. Advertising the right numbers and working are different claims. |
| **The defaulting entry points advertise what they always did** | `the_defaulting_entry_points_advertise_what_they_always_did` and `the_defaulting_entry_points_still_exchange`. `connect`/`serve` forward the defaults, so their behaviour is unchanged by construction; these pin it so that a later change to the defaults is a decision rather than a silent difference. |

Two of that file's tests pin **defects rather than features**, deliberately, so that fixing
either is a test failure and a decision:

| Behaviour pinned | Where |
| --- | --- |
| **A stream allowance is spent once and never returned** | `a_small_stream_allowance_is_spent_once_and_never_returned`. `max_streams_bidi` is a lifetime budget, not a concurrency limit, and nothing calls `extend_stream_limit` when a stream closes — so a connection that outlives its allowance presents that as a request which never resolves rather than as an error. The test uses a bounded wait, because the behaviour it asserts is a hang. |
| **An allowance above the transport maximum fails the connection rather than being rejected** | `a_stream_allowance_at_the_transport_maximum_is_accepted` and `a_stream_allowance_above_the_transport_maximum_fails_the_connection`. `TransportParams::validate` only checks that the value fits a varint, so a value above dwnx's `1 << 60` ceiling passes validation and then fails at setup. |

Both are recorded in `pending-work.md`, which is where the argument for changing them belongs.

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
