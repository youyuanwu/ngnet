# HTTP/2 invariants

Properties the `ngnet-h2` suite *pins* rather than merely exercises. Each is a claim the
crate makes about itself that would otherwise decay silently, so each has a test whose
failure is the only warning you get.

If you change something here, the test is not in your way — it is the reason the claim can be
made at all.

"The crate" below means `ngnet-h2`. The HTTP/3 family pins its own, separately, in
[`../h3/invariants.md`](../h3/invariants.md). What CI runs for both is in
[`../ci.md`](../ci.md).

## Structural — `crates/ngnet-h2/tests/invariants.rs`

These assert properties of the *source*, not of runtime behaviour.

| Test | Claim |
| --- | --- |
| `the_sans_io_core_reaches_for_no_io_threading_or_time_facility` | The sans-I/O core never names `std::net`, `std::io`, `std::fs`, `std::thread`, `std::time` or `std::process`. It cannot perform I/O, block, sleep or spawn if it never names the means to. |
| `the_facility_scan_would_still_catch_the_core` | The scan above would still fail for core files — so its exemption for `src/http/` has not quietly turned it into a no-op. |
| `no_async_facility_escapes_the_subtree` | `async`/`await` appear only under `src/http/`. Containment is structural, not a matter of habit. |
| `an_included_doc_cannot_smuggle_async_past_the_scan` | Every `include_str!` target resolves under `src/http/`. Doc bodies carry compiled doctests, and the scanner reads only `.rs` — without this, an included `.md` would be a hole in the rule above. |
| `unsafe_is_confined_to_the_modules_that_wrap_the_bindings` | `unsafe` appears only where the FFI is wrapped. |
| `the_async_subtree_contains_no_unsafe_at_all` | The async layer has none, anywhere. |
| `no_test_needs_unsafe_to_use_the_api` | No test needs `unsafe` to *use* the crate. Exemptions are named individually, and `every_unsafe_exemption_is_still_earned` fails if one stops needing its exemption. |
| `the_crate_declares_exactly_one_non_optional_dependency` | The sans-I/O claim is not quietly funded by a dependency. |
| `the_frame_buffer_is_zeroed_only_on_the_copying_read_path` | The frame-buffer memset appears exactly once, and only inside `read_push_body`'s own body — proven by brace-bounded containment, not textual position. The no-copy shared read path hands nothing to a source and writes no payload into that buffer, so it must have no `write_bytes`; a second one, or one that migrated into the shared path, fails here. |

The helpers have their own tests — `the_comment_stripper_actually_strips`,
`the_unsafe_keyword_detector_distinguishes_identifiers`,
`included_docs_are_found_wherever_they_are_spelled` — because a scanner that silently stops
matching is worse than no scanner.

## Public surface — `crates/ngnet-h2/tests/compat_surface.rs`

`the_sans_io_surface_is_unchanged` and `the_asynchronous_surface_is_unchanged` pin the public
API by referencing it, so a signature change breaks a build rather than a downstream user.

## Performance — `tests/http_zero_alloc.rs`

Measured across whole driver passes, on the thread that armed the counter, after warm-up.
Per-stream setup is excluded deliberately: the claim is about the recurring cost of moving
frames, not the one-off cost of standing a stream up.

| Test | Claim |
| --- | --- |
| `steady_state_receive_allocates_nothing` | A steady-state receive pass — and the body drainer's poll — allocate **zero**. |
| `steady_state_send_allocates_nothing_on_the_borrowed_path` | Likewise for sending, on a transport that reaches gathering through the emulating default. |
| `steady_state_send_allocates_nothing_on_the_vectored_path` | Likewise on the gathering path — so gathering costs nothing the borrowed path did not. |
| `steady_state_multiplexed_send_allocates_nothing_on_the_vectored_path` | And still zero when eight streams are multiplexed, which is where the driver's own buffer would be tempted to grow. |
| `steady_state_send_allocates_nothing_on_the_owned_region_path` | Likewise on the owned-region path — the completion transport's gathering allocates nothing in steady state either. |
| `the_read_buffer_pool_settles_to_a_fixed_size` | The pool reaches a high-water mark during warm-up and does not grow afterwards. |
| `the_owned_write_path_coalesces_a_pass_into_one_write` | Under `WritePolicy::Coalesced`, one write per pass, with more than one frame's worth of bytes — so the single write is not trivially single. |
| `emulated_gathering_costs_no_more_writes_than_native_on_an_upload` | Emulated and native gathering cost the **same** write count on the copying upload path, because the driver offers one region per large block either way. Replaces `the_borrowed_write_path_writes_each_block_separately`, whose premise — a per-region drain — no longer exists. |
| `a_multiplexed_pass_costs_one_write_natively_and_under_emulation_alike` | One write for a whole multiplexed pass, and **the same count** for a transport that only emulates gathering. Accumulation happens in the driver before any write, so 513 small blocks collapse into one region and the emulating loop runs once. This is the whole affordability argument for mandatory gathering. |
| `the_vectored_write_path_writes_once_per_large_block_and_no_more` | A large block still costs exactly one write, so gathering never degenerates into a write per region. |
| `the_owned_region_write_path_coalesces_a_pass_into_one_write` | The owned-region (completion) path coalesces a push-model pass into one write — indistinguishable from the owned path here, since a payload only rides its own region once a body is handed over. |
| `the_owned_write_path_reuses_its_coalescing_buffer` | The owned path reuses its coalescing buffer rather than rebuilding it per pass, so it too allocates nothing in steady state — it pays a copy, not an allocation. |
| `waking_parked_handlers_allocates_nothing` | Waking parked server handlers repeatedly allocates nothing. |
| `the_counter_notices_a_deliberate_allocation` | The measuring instrument works, guarding every test above against a false pass. |

## Behavioural properties pinned elsewhere

- `the_send_path_has_nowhere_to_put_a_second_chunk` (`tests/invariants.rs`) — a source scan
  forbidding any additional chunk container in the send path, so the one-chunk rule cannot be
  broken by adding a buffer somewhere.
- `a_buffering_transport_still_completes_an_exchange` (`tests/http_flush.rs`) — a transport
  that releases writes only on `commit` still completes, under a bounded poll budget so a
  regression fails rather than hanging forever.
- **Five compile-fail doctests** on `TransportWrite` and `BorrowedWrite` — the ways an adapter
  could get a write declaration wrong do not compile. Each pins an error code
  (`compile_fail,E0277` or `E0053`) rather than bare `compile_fail`, so a typo in the doctest
  cannot pass for the guarded failure, and each was mutation-verified by making the guarded
  construct legal and confirming the doctest then fails. The count is unchanged at five, but
  two were retargeted when the strategy markers went away:
  - declaring `Readiness` without implementing `BorrowedWrite`;
  - declaring `Completion` without implementing `RegionWrite` — which bites even though
    `RegionWrite` has no required methods, because the empty impl block is still required;
  - implementing operations from both I/O models on one type;
  - an `Option`-returning `write_borrowed`, i.e. trying to decline a path mid-pass;
  - implementing `WriteModel` downstream, i.e. inventing a third I/O model.
- **`the_write_policy_is_the_h2_layers_and_holds_for_the_connections_life`**
  (`tests/http_vectored.rs`) — the *same* natively-gathering transport, driven under both
  `WritePolicy` values, produces gathered writes under one and none under the other, across
  several passes, with identical octets on the wire. This replaces
  `the_gathering_capability_is_consulted_exactly_once_per_connection`, which pinned that the
  driver read `VectoredWrite::gathers` exactly once. There is no capability to read any more —
  nothing is consulted on any path — which is strictly stronger than reading it once.
- **`an_emulating_transport_delivers_identical_octets_one_region_at_a_time`** and
  **`an_emulating_transport_delivers_every_octet_of_a_multi_region_offer_in_order`**
  (`tests/http_vectored.rs`) — the emulation contract. Both run on the handed-over body, the
  only measured workload where the driver offers more than one region and therefore the only
  one where native and emulated gathering are distinguishable at all; both assert that
  multi-region offers actually occurred, so neither can pass vacuously.
- **`an_emulating_partial_acceptor_leaves_no_gap_between_the_regions_it_wrote`**
  (`tests/http_vectored.rs`) — the emulating default's *short-write* rule: stop at the short
  region and return the running total, rather than carrying on to the next one. Carrying on
  would report a total the driver then retries from while later octets had already gone out,
  putting a hole in the stream and a duplicate after it.

  This test exists because the rule was initially **unpinned**, and the gap was found by
  mutation rather than by review: deleting the `break` from `emulate_gathering` left all 834
  tests green. The cause was in the harness, not the suite — `DuplexWriter::do_write_borrowed`
  accepted every write whole, so `Duplex::accept_at_most` could not reach the one primitive
  the emulation loop is built from. Making that method honour the cap is what gives this test
  teeth, and it is a standing requirement: if `do_write_borrowed` ever stops capping, this
  test silently stops testing anything. Same shape as the PR #9 vacuity below, caught the same
  way.
- **`an_emulating_completion_transport_delivers_every_region_in_order`** and
  **`an_emulating_completion_partial_acceptor_leaves_no_gap_between_its_regions`**
  (`tests/http_vectored.rs`) — the same two properties on the completion side, where
  `RegionWrite::write_regions` is also provided and loops one owned `write` per region.

  These exist for the same reason and were found the same way. Every other completion writer
  in the workspace overrides `write_regions` — the shipped `CompioWriter` has a real vectored
  write, and `Duplex<Regions>` records through its own — so the default was **dead code under
  test**: a mutation making it drop all but the first region left all 835 tests green.
  `Duplex<RegionEmulating>`, whose `impl RegionWrite for … {}` is empty, is the only shape in
  the workspace that runs it. Its `write` records and caps for the same reason
  `do_write_borrowed` does.
- **`elects_owned_regions::<CompioWriter<TcpStream>>()`** (`ngnet-h2-tests/tests/http_compio.rs`)
  — a compile-time assertion that the shipped completion transport declares `Completion`. This
  replaces a runtime predicate check that was found vacuous: in PR #9, flipping
  `gathers_owned_regions()` to `false` left the entire workspace suite green, so the whole
  owned-region fast path could have silently regressed to copying every octet. Demoting
  `CompioWriter` to `Readiness` now fails to compile, and the failure is workspace-visible.
- **A compile-fail doctest** on `Connection` — discarding the driver is an error, because a
  connection that compiles and never sends a byte is the trap the type exists to prevent.
- **A negative `Send` assertion** in `ngnet-h2-tests` — a connection over a non-`Send`
  transport is genuinely not `Send`. Running it on a `LocalSet` proves nothing on its own,
  since `spawn_local` accepts `Send` futures too.

