# HTTP/3 invariants

Properties the `ngnet-h3` suite *pins* rather than merely exercises. Each is a claim the
crate makes about itself that would otherwise decay silently, so each has a test whose
failure is the only warning you get.

This is a separate suite from the HTTP/2 one rather than a widened version of it, because
the two crates make different promises: `ngnet-h3` has no async subtree to exempt, and one
item from `std::io` it is allowed to name.

"The crate" below means `ngnet-h3`. The HTTP/2 family pins its own in
[`../h2/invariants.md`](../h2/invariants.md); what CI runs for both is in
[`../ci.md`](../ci.md).

## Structural — `crates/ngnet-h3/tests/invariants.rs`

These assert properties of the *source*, not of runtime behaviour.

| Test | Claim |
| --- | --- |
| `unsafe_lives_only_in_the_modules_that_declare_they_need_it` | `unsafe` appears only in the modules `lib.rs` grants `#[allow(unsafe_code)]`, and every module granted it actually uses it. The list is read from `lib.rs`, so the two cannot disagree. |
| `the_allowance_list_is_the_ffi_boundary_and_nothing_else` | That list is exactly `alloc`, `callbacks`, `conn`, `error`, `send`, `settings`. The compiler cannot express this: adding an allow is precisely how it is silenced. |
| `a_caller_never_needs_unsafe` | No test needs `unsafe` to *use* the crate. Exemptions are named individually, and the test fails if one stops being needed. |
| `the_crate_reaches_for_no_io_threading_or_time_facility` | The crate never names `std::net`, `std::fs`, `std::thread`, `std::time`, `std::process` or `std::env`, and the only `std::io` item it names is `IoSlice` — a description of borrowed bytes, not a way to move them. The clock is the interesting one: nghttp3 wants a timestamp on every read, and the caller supplies it. |
| `the_crate_has_no_asynchrony_of_its_own` | No `async fn` anywhere. Its absence is what lets the crate be driven from blocking code, from any runtime, and from a test with no runtime at all. |
| `the_crate_declares_exactly_one_non_optional_dependency` | `ngnet-h3-sys`, and nothing else — nor any dev-dependency. quinn, rustls and tokio live in `ngnet-h3-tests` and reach the wrapper only through its public API. |
| `the_scanner_sees_through_comments_and_literals` / `the_scanner_would_catch_a_real_violation` | The scans fail on real code and pass on prose. A scanner that silently stops matching is worse than no scanner. |

## Public surface — `crates/ngnet-h3/tests/compat_surface.rs`

`the_public_surface_still_has_the_shape_it_promised` names every public item and uses each in
a way that pins its shape. It includes `ngnet_h3::raw`, the documented escape hatch, which is
deliberately excluded from the no-unsafe claim: capabilities the safe API does not yet cover
stay reachable, at the cost of upholding nghttp3's invariants yourself.

**Adding public API means extending this file**, or the test fails.

