# Hyperium H3 over ngtcp2: invariants

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

**Repeated exchanges do not work reliably.** Every invariant above is about a single exchange
or a single lifecycle event, and each holds. Repetition is a different matter: under a repeated
small-body workload this adapter intermittently stalls, roughly two runs in five at 200 x 1 KiB.
That is this crate's own defect, distinguished from the inherited one below by a pre-registered
attribution rule and by the native stack passing the identical workload 10 out of 10. It is
reproduced by `tests/repeated.rs`, which is `#[ignore]`d, and documented in
[`pending-work.md`](pending-work.md). No invariant in the table above should be read as implying
the crate is fit for use.

The transport underneath this crate has a known, unresolved intermittent connection-ending stall
under repeated 16 KiB and 1 MiB workloads — review finding S9, recorded in
[`../quic-h3/invariants.md`](../quic-h3/invariants.md) and
[`../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md`](../benchmarks/data/xeon-8370c-azure/30-ngtcp2-final-review-resolution.md).
No test here is `#[ignore]`d for it, because none of these tests provoked it. That is not
evidence that this adapter is immune; the workload that provokes it is repeated large-body
exchanges, which is a benchmark shape rather than a correctness shape. See
[`pending-work.md`](pending-work.md) and the run record for what was actually observed.
