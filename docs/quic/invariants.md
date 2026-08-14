# QUIC invariants

Properties the `ngnet-quic` suite *pins* rather than merely exercises. Each is a claim the
crate makes about itself that would otherwise decay silently, so each has a test whose
failure is the only warning you get.

This is a separate suite from the HTTP/2 and HTTP/3 ones rather than a widened version of
them, because the crate makes different promises: it carries obligations — struct-version
constants, a TLS object cycle — that neither framing crate has, and where they permit
asynchrony throughout a subtree, this crate's core forbids it entirely and confines it to
`src/endpoint/`.

"The crate" below means `ngnet-quic`. What CI runs for every family is in
[`../ci.md`](../ci.md).

## Structural — `crates/ngnet-quic/tests/invariants.rs`

These assert properties of the *source*, not of runtime behaviour.

The suite is **partitioned**. `src/endpoint/` is the asynchronous layer, and the core is
everything outside it. Several claims below are about the core alone, because the subtree
exists precisely to do what the core must not — name a socket, name a clock, be asynchronous
— and scanning it for those would flag the feature rather than a defect. The subtree makes
its own, different claims, and one test exists solely to fail if the partition ever stops
matching anything.

| Test | Claim |
| --- | --- |
| `unsafe_lives_only_in_the_modules_that_declare_they_need_it` | `unsafe` appears only in the core modules `lib.rs` grants `#[allow(unsafe_code)]`, **and** every module granted it actually uses it. The list is read from `lib.rs`, so the two cannot disagree. The second half is what caught `cid` carrying an allowance it did not need: a boundary wider than the code requires is a boundary that has stopped meaning anything. Scoped to the core, because a subtree file's stem could collide with a granted core module's name — `endpoint/error.rs` against `error` — and inherit a grant it was never given. |
| `the_allowance_list_is_the_ffi_boundary_and_nothing_else` | That list is exactly the modules that touch the raw bindings — fifteen, after the safe TLS seam removed `tls` and added `tls_bridge`. The compiler cannot express this — adding an allowance is precisely how the crate-level deny would be silenced — so changing it must be a deliberate edit in two places. The asynchronous subtree adds no entry, and the test below says why it needs none. |
| `all_core_module_files_are_flat` | Every *core* module file sits directly in `src/`. Not style: the scan above derives a module's name from its file stem, so a nested `foo/bar.rs` would be read as `bar`, a name `lib.rs` never declares, and would either be reported as an ungranted `unsafe` user or slip through as `mod`. `src/endpoint/` is exempt because it makes the stronger claim — no `unsafe` at all — which is what the flat rule was protecting. |
| `the_core_reaches_for_no_io_threading_or_time_facility` | The core never names `std::net`, `std::fs`, `std::thread`, `std::time`, `std::process` or `std::env`. The clock is the interesting one: ngtcp2 wants a timestamp on almost every call, and the only way to supply one without reading a clock is to make the caller pass it — which is why `Timestamp` exists. `std::net` is the subtle one: socket addresses are unavoidable in a transport library and the obvious spelling would fail this scan, so the crate uses `core::net`, which is the same data with no I/O attached. |
| `no_asynchrony_escapes_the_subtree` | No `async fn`, `async move` or `await` outside `src/endpoint/`. The core is driven, not driving, and that is what lets it be used from blocking code, from any runtime, and from a test with no runtime at all. |
| `the_async_subtree_spawns_nothing_and_runs_nothing` | The subtree may name a socket and a clock — that is what it is for — but never `std::thread`, `std::process` or `std::env`. It takes no executor: every future it produces is polled by the caller, and a subtree that could spawn would be able to hide work from the caller's runtime. |
| `the_async_subtree_contains_no_unsafe_at_all` | A claim the core cannot make, and the reason the subtree needs no allowance-list entry. Every foreign call lives below this layer; `unsafe` appearing here would mean the safe API it is built on has a hole. The crate-level `deny(unsafe_code)` already rejects a plain `unsafe` block, so what this test actually catches is a local `#[allow(unsafe_code)]` silencing it — which the compiler accepts and nothing else would notice. |
| `the_async_subtree_exists_and_is_scanned` / `the_subtree_filter_discriminates` | The subtree contains files, the core contains files, and the filter tells them apart. Without these, deleting or renaming the subtree would turn every claim about it into a claim about the empty set, silently and with every test still green — and a filter matching *everything* would do the same to the core's claims. |
| `the_crate_declares_exactly_one_non_optional_dependency` | `ngnet-quic-sys`, and nothing else. Optional dependencies are skipped, which is how the asynchronous layer's runtime integration is admitted without widening this. Read textually from the manifest, because the claim is about what the crate *asks for*, not about what happens to be in the workspace lock file. The companion claim about what a caller actually *builds* is asked of the resolved dependency graph instead, in `tests/ngnet-workspace-tests` — see [`../ci.md`](../ci.md). |
| `the_crate_has_no_dev_dependencies` | None. A test-only dependency is still something a contributor has to build, and it would be the easy way to acquire a certificate generator or an RNG — which is exactly why `ngnet-quic-tests` exists. |
| `an_included_file_cannot_smuggle_code_past_the_scans` | `include_str!` splices a file into what the compiler sees but not into what these scans read. The core may include only inert data — `pem`, `md`, `txt`, `json` — and it must exist. An include *inside* the subtree must additionally resolve inside it, because one reaching out would pull in text the core's scans never examine. |
| `a_caller_never_needs_unsafe` | No test needs `unsafe` to *use* the crate. Exemptions are named individually, so one becoming unnecessary is noticed rather than silently kept. Implementing the TLS seam now counts as *using* it: `compat_surface` lost its exemption when the seam became safe, and implements a backend, a session and both key kinds with no `unsafe` at all. |
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

## Behavioural — `tests/ngnet-quic-tests/`

Not invariants in the same sense, but two are worth naming here because they guard against
tests that pass for the wrong reason:

| Test | Claim |
| --- | --- |
| `the_verification_test_would_notice_if_verification_were_disabled` | Runs the mismatched-certificate setup with verification *off* and asserts it completes. Without it, the two negative verification tests could both be passing because the handshake was broken for some unrelated reason. |
| `the_handshake_reports_progress_rather_than_hanging` | The relay is bounded and returns a datagram count. A handshake that stopped making progress would otherwise hang the suite rather than fail it. |

## The TLS seam — `crates/ngnet-quic/tests/invariants.rs`, `tests/safe_backend.rs`

| Test | Claim |
| --- | --- |
| `the_tls_seam_names_nothing_a_backend_cannot_have` | `src/tls.rs` mentions no raw pointer, no `sys::` path, no `ngtcp2_` name and no `c_void`. The compiler cannot express this: a signature can name a foreign type perfectly safely and still leak the library into an interface whose purpose is to hide it. Read textually, because "does not mention" is a different claim from "does not depend on". The seam's own tests are excluded — they deliberately compare its constants against the bindings, which is why the constants may be restated at all. |
| `the_safe_backend_proves_the_seam_needs_no_unsafe` | `tests/safe_backend.rs` still says `forbid(unsafe_code)`, not `deny`. A `deny` can be silenced from inside by an allowance; a `forbid` cannot. That difference is the entire evidential value of the file. |
| `a_backend_that_forbids_unsafe_completes_a_connection_in_both_roles` | A whole TLS backend, in a module that forbids unsafe code and depends on nothing but this crate and `std`, carries two real connections through a handshake and moves application data — as a client **and** as a server. The server half is not decoration: a server is the side whose transport parameters cannot be produced up front, and it is where an earlier design of this seam failed. |
| the `compile_fail` doctest on `Handshaking` | A backend that stores the borrowed connection instead of using it does not compile. Checked for non-vacuity: the same backend that uses it and lets it go compiles, so the test fails for keeping it and nothing else. |
