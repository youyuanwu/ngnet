# HTTP/3 invariants

Properties the `ngnet-h3` suite *pins* rather than merely exercises. Each is a claim the
crate makes about itself that would otherwise decay silently, so each has a test whose
failure is the only warning you get.

This is a separate suite from the HTTP/2 one rather than a widened version of it, because
the two crates make different promises: `ngnet-h3` has one item from `std::io` its core is
allowed to name, and its async subtree is scoped differently.

Several of the claims below are split in two by `src/http/`. The core may not be
asynchronous and may not reach for I/O; the async layer may be asynchronous — that is what it
is for — but may bring no runtime and no `unsafe`. Scoping them that way is what keeps each
claim true and worth making.

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
| `the_core_reaches_for_no_io_threading_or_time_facility` | Outside `src/http/`, the crate never names `std::net`, `std::fs`, `std::thread`, `std::time`, `std::process` or `std::env`, and the only `std::io` item it names is `IoSlice` — a description of borrowed bytes, not a way to move them. The clock is the interesting one: nghttp3 wants a timestamp on every read, and the caller supplies it. |
| `no_asynchrony_escapes_the_subtree` | No `async fn` outside `src/http/`. Its absence is what lets the core be driven from blocking code, from any runtime, and from a test with no runtime at all. |
| `the_async_subtree_contains_no_unsafe_at_all` | Not "confined to a declared list", as the core's rule is: none. Every FFI call and every raw pointer lives below the layer, which is what lets it be reviewed as ordinary Rust. |
| `the_async_layer_grants_itself_no_unsafe_allowance` | No file under `src/http/` carries `#[allow(unsafe_code)]`. A different claim from the one above, and the one that guards it: the crate-level deny is what makes an `unsafe` block there a compile error, and the allowance is exactly how that would be silenced. |
| `the_async_layer_brings_no_runtime` | The layer names no spawner, thread or third-party executor. It may be asynchronous; it may not choose a runtime on the caller's behalf. `testing.rs` is exempt by name — its condition-variable parker is what lets the whole suite run with no runtime at all. |
| `the_async_subtree_exists_and_is_scanned` | The path filter still matches files on both sides. A scan that silently stops matching turns every claim above it into a claim about an empty set. |
| `an_included_doc_cannot_smuggle_async_past_the_scan` | Every `include_str!` resolves inside `src/http/`. A doc page spliced into the core would carry code these scans read the source for and never see. |
| `the_crate_declares_exactly_one_non_optional_dependency` | `ngnet-h3-sys`, and nothing else — nor any dev-dependency. `bytes`, `http` and `http-body` are optional and reachable only through a `dep:` feature entry, so a downstream crate cannot enable one without asking for the layer. quinn, rustls and tokio live in `ngnet-h3-tests` and reach the wrapper only through its public API. |
| `the_scanner_sees_through_comments_and_literals` / `the_scanner_would_catch_a_real_violation` | The scans fail on real code and pass on prose. A scanner that silently stops matching is worse than no scanner. |
| `closed_stream_membership_never_scans_eviction_order` | Both driver membership paths use the closed-stream hash index. The FIFO queue is used only to append, evict from the front and enforce the bound; its contents are never scanned for membership. Required identifiers and call sites make the source check fail rather than pass vacuously if the component changes shape. |
| `the_closed_stream_scanner_catches_an_order_lookup` | The closed-stream source check rejects a real queue membership lookup. |

## Public surface — `crates/ngnet-h3/tests/compat_surface.rs`

`the_public_surface_still_has_the_shape_it_promised` names every public item of the sans-I/O
core and uses each in a way that pins its shape. It includes `ngnet_h3::raw`, the documented
escape hatch, which is deliberately excluded from the no-unsafe claim: capabilities the safe
API does not yet cover stay reachable, at the cost of upholding nghttp3's invariants
yourself.

`the_async_surface_still_has_the_shape_it_promised` does the same for everything behind the
`http` feature. So the file pins two shapes rather than one — with the feature off the crate
is what the first test describes, and with it on the second is added. Closed enumerations are
matched exhaustively and open ones with a wildcard, and which is which is itself a promise:
`WriteOutcome` is closed because a fourth way to answer an offer is a change every transport
must notice, while `QuicEvent` and `ErrorKind` are open because adding a variant to either
must not break a caller.

**Adding public API means extending this file**, or the test fails.



## Behavioural — the async layer

Not source scans but properties of what the layer does, kept apart from the functional suites
so a reviewer can find them.

| Suite | Claim |
| --- | --- |
| `tests/http_body.rs` | The retain contract. A buffer is not released while acknowledgement is withheld, *and* is released once it arrives — either alone is vacuous. Dropping the connection mid-body releases what it held, since `delete_outq` leaks alien buffers deliberately. Release is measured through the buffer's owner, so the observation cannot drift from the thing observed. |
| `tests/http_backpressure.rs` | A transport does not read past the credit it has been given, so the memory bound stays in QUIC's flow control rather than moving into the process. A reset is applied ahead of held data. A stalled stream does not starve the others. |
| `tests/http_failed_body.rs` | A caller's body that failed produces no end-of-stream marker for its stream, and exactly one reset, carrying `H3_REQUEST_CANCELLED`. Asserted at the transport seam through a recording backend rather than inferred from what a peer made of it — which is the inference that let a truncated message look complete for as long as it did, and it is what makes the claim hold for every transport rather than for the in-memory one. The bytes of the pull that failed appear in no write; the reset is issued without the peer saying anything and without the driver parking in between; and a body that ends, ends with trailers, or defers and is later resumed is unchanged in every call it produces. |
| Unit tests beside `http/driver.rs` and `tests/http_closed_streams.rs` | Closed-stream membership and FIFO order stay synchronized across insertion, duplicate notification and eviction, with exactly 1,024 retained entries. A release for a retained tombstone is discarded and the connection keeps working; the same nonzero delivered release after eviction follows the unchanged non-tombstoned error path. |
| `tests/http_head.rs` and the unit tests beside `head.rs` | Every construction RFC 9114 forbids is refused, in both directions, one test per rule. Connection-specific fields are refused on the way *in* as well as out — the peer is not running this code. |
| `tests/ngnet-h3-tests/tests/http_quinn.rs` | The same exchanges over a real QUIC connection, driven by a second, independent implementation of the backend trait. It declares `RETAINS_BUFFERS = false` where the in-memory one declares `true`, so both arms of that constant are exercised. |
