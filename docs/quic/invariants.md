# QUIC invariants

Properties the `ngnet-quic` suite *pins* rather than merely exercises. Each is a claim the
crate makes about itself that would otherwise decay silently, so each has a test whose
failure is the only warning you get.

This is a separate suite from the HTTP/2 and HTTP/3 ones rather than a widened version of
them, because the crate makes different promises: it has no async subtree at all, and it
carries obligations — struct-version constants, a TLS object cycle — that neither framing
crate has.

"The crate" below means `ngnet-quic`. What CI runs for every family is in
[`../ci.md`](../ci.md).

## Structural — `crates/ngnet-quic/tests/invariants.rs`

These assert properties of the *source*, not of runtime behaviour.

| Test | Claim |
| --- | --- |
| `unsafe_lives_only_in_the_modules_that_declare_they_need_it` | `unsafe` appears only in the modules `lib.rs` grants `#[allow(unsafe_code)]`, **and** every module granted it actually uses it. The list is read from `lib.rs`, so the two cannot disagree. The second half is what caught `cid` carrying an allowance it did not need: a boundary wider than the code requires is a boundary that has stopped meaning anything. |
| `the_allowance_list_is_the_ffi_boundary_and_nothing_else` | That list is exactly the fourteen modules that touch the raw bindings. The compiler cannot express this — adding an allowance is precisely how the crate-level deny would be silenced — so changing it must be a deliberate edit in two places. |
| `all_module_files_are_flat` | Every module file sits directly in `src/`. Not style: the scan above derives a module's name from its file stem, so a nested `foo/bar.rs` would be read as `bar`, a name `lib.rs` never declares, and would either be reported as an ungranted `unsafe` user or slip through as `mod`. |
| `the_crate_reaches_for_no_io_threading_or_time_facility` | The crate never names `std::net`, `std::fs`, `std::thread`, `std::time`, `std::process` or `std::env`. The clock is the interesting one: ngtcp2 wants a timestamp on almost every call, and the only way to supply one without reading a clock is to make the caller pass it — which is why `Timestamp` exists. `std::net` is the subtle one: socket addresses are unavoidable in a transport library and the obvious spelling would fail this scan, so the crate uses `core::net`, which is the same data with no I/O attached. |
| `no_asynchrony_escapes_into_the_crate` | No `async fn` and no `await`, anywhere. Unlike `ngnet-h2` and `ngnet-h3` there is no subtree where asynchrony is permitted; this crate is a state machine and nothing else. Its absence is what lets it be driven from blocking code, from any runtime, and from a test with no runtime at all. |
| `the_crate_declares_exactly_one_non_optional_dependency` | `ngnet-quic-sys`, and nothing else. Read textually from the manifest, because the claim is about what the crate *asks for*, not about what happens to be in the workspace lock file. |
| `the_crate_has_no_dev_dependencies` | None. A test-only dependency is still something a contributor has to build, and it would be the easy way to acquire a certificate generator or an RNG — which is exactly why `ngnet-quic-tests` exists. |
| `a_caller_never_needs_unsafe` | No test needs `unsafe` to *use* the crate. Exemptions are named individually, so one becoming unnecessary is noticed rather than silently kept. Note the distinction the exemptions record: implementing the TLS seam is not *using* the API, it is extending it, and that trait is `unsafe` precisely because doing so carries obligations the compiler cannot check. |
| `every_version_constant_lives_in_one_module` | The five struct-version constants appear only in `ffi.rs`. A wrong one is neither a compile error nor a runtime error — it is ngtcp2 misreading the memory behind a pointer — so keeping them together is what makes them reviewable. |
| `the_scan_actually_sees_files` / `the_scanner_would_catch_a_real_violation` / `the_scanner_sees_through_comments_and_literals` / `the_unsafe_word_scanner_does_not_match_substrings` | The scans fail on real code, pass on prose, and do not match `unsafely_named_thing`. A scanner that silently stopped matching would turn every claim above it into a claim about the empty set, which is worse than having no scanner at all. |

## Public surface — `crates/ngnet-quic/tests/compat_surface.rs`

Three tests name every public item and use each in a way that pins its shape. Nothing there
asserts behaviour: **compiling is the assertion**. Adding public API means extending that
file.

Whether an enumeration is matched exhaustively or with a wildcard is itself a promise, and
the file records which is which:

- **Closed** — `WriteOutcome`, `ReadOutcome`, `ExpiryOutcome`, `StreamWrite`, `Inspection`,
  `Role`, `Initiator`, `Directionality`. Each variant leads somewhere different, so a new one
  is a change every caller must be forced to notice. `WriteOutcome` is the sharpest case:
  conflating `Idle` with `Blocked` is the classic QUIC stall bug.
- **Open** — `ErrorKind`, `StreamCloseReason`, `Verify`. ngtcp2 may grow conditions this
  crate has to classify, and adding a variant must not break a caller.

## Allocation — `crates/ngnet-quic/tests/zero_alloc.rs`

`writing_packets_allocates_nothing_in_the_wrapper` installs a counting global allocator and
arms it around a send loop.

The design reason the property *can* hold is that the caller supplies the datagram buffer, so
nothing need be allocated per packet. But that is an argument, not a guarantee, and it is
exactly the kind of property that decays silently: one `to_vec()` added inside the wrapper for
convenience would never fail a functional test.

`the_counter_would_notice_a_real_allocation` and `the_counter_is_disarmed_outside_a_measured_region`
guard the guard.

## FFI — `crates/ngnet-quic/tests/versioned_ffi.rs`

Asserts three separate things about the eighteen hand-written macro replacements: that the
version constants match the bindings, that every `*_versioned` symbol links, and that the
unversioned names genuinely do **not** exist — because if a future bindgen started emitting
them, this is where that should be noticed rather than in a duplicate-definition error
somewhere odd.

It also reads `NGTCP2_DCIDTR_MAX_UNUSED_DCID_SIZE` back out of the vendored C header.
`validate.rs` restates that value because it lives in a *private* header and is therefore
absent from the bindings; if ngtcp2 ever changes it, this test fails rather than the range
check silently becoming wrong.

## Behavioural — `crates/ngnet-quic-tests/`

Not invariants in the same sense, but two are worth naming here because they guard against
tests that pass for the wrong reason:

| Test | Claim |
| --- | --- |
| `the_verification_test_would_notice_if_verification_were_disabled` | Runs the mismatched-certificate setup with verification *off* and asserts it completes. Without it, the two negative verification tests could both be passing because the handshake was broken for some unrelated reason. |
| `the_handshake_reports_progress_rather_than_hanging` | The relay is bounded and returns a datagram count. A handshake that stopped making progress would otherwise hang the suite rather than fail it. |
