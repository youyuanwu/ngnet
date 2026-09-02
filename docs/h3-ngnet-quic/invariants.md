# Hyperium H3 over ngtcp2: invariants

> **Read this first.** None of the suites below are `#[ignore]`d. The intermittent liveness
> failure that once gated them was found and fixed — a lost FIN, one layer down in the
> transport's report of what ngtcp2 serialised; see [`pending-work.md`](pending-work.md).
> Each row states a property the crate has and the test that pins it.

Each row is a claim the crate makes and the test that would fail if it stopped being true.
Where a test is marked *(regression)*, it was checked against the implementation before the fix
and observed to fail, so it pins a defect rather than merely agreeing with the current code.

| Invariant | Evidence |
| --- | --- |
| Every offered byte reaches the peer exactly once, in order, across partial and zero acceptance | `tests/send_buffers.rs::an_unframed_send_advances_only_the_exact_accepted_prefix`; `tests/e2e.rs::a_body_spanning_many_packets_round_trips_exactly` |
| `poll_send` advances the caller's buffer by exactly the count it reports, on every call | `tests/send_buffers.rs::an_unframed_send_advances_only_the_exact_accepted_prefix` |
| A framed send walks its frame header and its payload exactly once across the `chunk()` boundary | `tests/send_buffers.rs::a_framed_send_walks_the_header_and_every_payload_chunk_exactly_once` |
| An empty framed send is legal and carries only its header | `tests/send_buffers.rs::an_empty_framed_send_is_accepted_and_carries_nothing` |
| A second logical send while one is outstanding is rejected, not interleaved | `tests/lifecycle.rs::an_overlapping_logical_send_is_rejected` |
| An unframed send while framed data is retained is rejected and consumes nothing | `tests/lifecycle.rs::an_unframed_send_while_framed_data_is_retained_is_rejected` |
| Unread data is not absorbed on the receiver's behalf: the sender blocks on flow control | `tests/send_buffers.rs::a_sender_that_outruns_its_reader_blocks_instead_of_buffering_without_bound` |
| FIN accompanies only the true final data, and finishing is idempotent | `tests/lifecycle.rs::a_finished_stream_ends_cleanly_for_the_peer`; `::finish_is_idempotent_and_emits_one_end_of_stream` |
| A peer reset is observed with its code, after already-delivered data, and is stable on re-observation | `tests/lifecycle.rs::a_peer_reset_is_observed_with_its_code_after_delivered_data` |
| A peer reset ends our receiving side only; our sending side survives it | `tests/lifecycle.rs::a_peer_reset_does_not_terminate_our_sending_side` *(regression)* |
| A peer stop-sending ends our sending side only; a completed response stays readable | `tests/lifecycle.rs::stop_sending_terminates_the_peers_send_side_with_its_code`; `::stop_sending_does_not_terminate_our_receiving_side` *(regression)* |
| Split halves are independent: dropping one does not invalidate the other's retained send | `tests/lifecycle.rs::dropping_one_split_half_does_not_invalidate_the_other` *(regression)* |
| Dropping an unfinished send is observed as exactly one reset, not an indefinite wait | `tests/lifecycle.rs::dropping_an_unfinished_send_is_observed_as_one_reset` |
| A connection close is observed by the peer with its application code | `tests/lifecycle.rs::a_connection_close_is_observed_with_its_application_code` |
| A terminated connection resolves every outstanding stream and opener rather than leaving them pending | `tests/lifecycle.rs::a_terminated_connection_resolves_every_outstanding_operation` |
| A quiet connection fires its own timers: the expiry sleep is polled under a waker the core owns, not a caller's | `tests/liveness.rs::a_quiet_connection_still_fires_its_own_idle_timeout`; `::the_expiry_timer_outlives_the_task_that_armed_it` |
| An idle timeout reaches HTTP/3 as a timeout, not as an opaque transport failure | `src/error.rs::tests::an_idle_timeout_is_a_timeout_and_not_an_opaque_failure`; `tests/liveness.rs` |
| Every hyperium transport trait and associated type is implemented, and the handles are spawnable | `tests/traits.rs` |
| The crate is a three-way join — `bytes`, `h3`, `ngnet-quic` — and reaches no second protocol stack | `ngnet-workspace-tests/tests/dependency_graph.rs::the_hyperium_quic_adapter_has_the_exact_direct_dependencies` |
| It does link the OpenSSL its transport brings, unlike the QMux adapter | `ngnet-workspace-tests/tests/linkage.rs::the_hyperium_quic_adapter_links_the_tls_its_transport_brings` |
| No `unsafe`, and no missing documentation | `#![deny(missing_docs, unsafe_code)]` in `src/lib.rs` |

## Not established here

**The resolved liveness defect is covered by tests, not by argument.** The fix is pinned from
two sides: `crates/ngnet-quic/tests/fin_delivery.rs` reproduces ngtcp2's "the packet carried no
STREAM frame" case deterministically and asserts it is never reported as a written FIN, and
`tests/repeated.rs` runs the 200-exchange workload that used to fail roughly two runs in five.
What is *not* established is a bound on how rare any remaining timing failure is: 45 matched
release-mode runs of that workload on this host produced no failure on this adapter, which
bounds it loosely and does not prove it is zero.

**A distinct defect in the *other* stack remains, and it is not this crate's.** `ngnet-quic-h3`
— the native HTTP/3 implementation over the same transport — still ends connections
intermittently under repeated 16 KiB exchanges: review finding S9, recorded in
[`../quic-h3/invariants.md`](../quic-h3/invariants.md) and
[`../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md`](../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md).
Measured on this host after the FIN fix, at 200 x 16 KiB with both arms on the same transport
and the same workload: this adapter completed 20 of 20, the native arm failed 2 of 20 with
`ErrorKind::Closed`. Transport held fixed, HTTP/3 layer varied — so that fault is the native
stack's, and no test here is `#[ignore]`d for it. A later revision to the native stack's drain
loop then produced 50 clean runs of the same workload, which bounds the fault without showing
it is gone; [`pending-work.md`](pending-work.md) records both rounds and why the distinction is
kept.
