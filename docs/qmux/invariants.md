# QMux invariants

The properties the QMux suite pins rather than merely exercises, and where each is enforced.
A test that would still pass if the property were broken is not on this list.

## Enforced by the compiler

These are `compile_fail` doctests in `crates/ngnet-qmux/src/compile_fail.rs`. A runtime test
cannot express "this cannot be written", and each of these claims is about something the API
makes impossible rather than something it rejects.

| Property | Why it matters |
| --- | --- |
| **A handler cannot name the connection it belongs to** | This is the entire mechanism behind dwnx's rule that `dwnx_conn_writev_stream` must not be called from inside a callback, and behind the callback bridge's inability to survive a nested entry point. Two cases are pinned: capturing a connection into a handler, and using one from inside a handler installed on it. |
| **`Conn` is not `Sync`** | The bridge slot is written on every entry point without synchronisation. That is sound only because those entry points take `&mut self`; sharing a connection between threads would break it. `Conn` is `Send`, which a positive assertion in `tests/lifecycle.rs` pins. |
| **The record buffer cannot be touched mid-record** | dwnx retains the `dest` pointer for the whole `WRITE_MORE` sequence. `RecordWriter` borrows the buffer for its own lifetime so that swapping or mutating it partway through does not compile. Without this the API would admit a use-after-free from safe code. |
| **A handler must be `Send`** | `Conn` carries a hand-written `unsafe impl Send` and owns its handlers, so a non-`Send` capture would let safe code move an `Rc` across threads and race its refcount. |

## Enforced against the C library

These read the vendored sources or call into the built archive, so they fail when upstream
moves rather than when this crate does.

| Property | Where |
| --- | --- |
| **Every `DWNX_ERR_*` condition is classified, and classified correctly** | `crates/ngnet-qmux/tests/error_mapping.rs` scans `deps/dwnx/lib/includes/dwnx/dwnx.h` for the constants rather than listing them. A hand-written list would pass forever: adding a twenty-fifth `#define` upstream would not make it fail, which is exactly the regression worth catching when the submodule moves. The check runs both ways, so a constant removed upstream also fails. `DWNX_ERR_FATAL` is excluded as the threshold rather than a condition. The expected `ErrorKind` for each is asserted too, not merely listed: checking only that a constant appears would prove the mapping total while saying nothing about whether it is right. |
| **The restated macros match the header** | `crates/ngnet-qmux-sys/wrapper.h` restates the five time units and the varint bound, each pinned by a `_Static_assert` against dwnx's own definition. bindgen silently drops these macros because they are written with a cast, so the restatement is necessary; the assertion is what stops it drifting. Their Rust widths are pinned separately in `tests/smoke.rs`, because bindgen sizes each constant to its literal rather than to `dwnx_duration`. |
| **Every public dwnx function is reachable** | `crates/ngnet-qmux-sys/tests/smoke.rs` names all 33. Without this, an allowlist regression surfaces as a confusing error inside the safe crate rather than in the crate that owns the bindings. |
| **Struct layout agrees with the C compiler** | The same file constructs and frees a real connection in each role. dwnx reads `callbacks`, `settings` and `params` through pointers the C compiler laid out; a field offset bindgen disagreed about would produce garbage or a crash here. |
| **The stream-id encoding agrees with dwnx** | `crates/ngnet-qmux/src/stream.rs` decodes the initiator and directionality from the low bits without an FFI call, and a unit test compares its answer against `dwnx_is_bidi_stream` across a range of ids. Deriving locally is only worth doing if it is right. |
| **The C defaults are reproduced, not improved upon** | `crates/ngnet-qmux/src/params.rs` and `settings.rs` seed from `dwnx_transport_params_default` and `dwnx_settings_default`, and tests assert the results field by field — including that every flow-control and stream limit is zero. `ngnet-quic` overlays its own working values on ngtcp2's zeros; this crate does not, so "the defaults" means the library's. |

## Enforced about the wrapper's own behaviour

| Property | Where |
| --- | --- |
| **Parameters that would abort are rejected before reaching C** | `crates/ngnet-qmux/tests/lifecycle.rs`. dwnx guards its constructor preconditions with `assert`, so an out-of-range limit aborts rather than failing. Each rejection asserts `error.native().is_none()`, which is what proves the check happened on the Rust side rather than being a C error rendered nicely. |
| **The C defaults still construct** | The same file. The validation above must not overreach: dwnx accepts its own zero-limit defaults, so the wrapper must too, however useless the resulting connection is until limits are raised. |
| **A handler's error message survives the round trip** | `crates/ngnet-qmux/tests/events.rs`. dwnx collapses every nonzero callback return to `DWNX_ERR_CALLBACK_FAILURE`, so the test asserts on the caller's own message, not merely on the error kind — the kind alone would pass with the message lost. |
| **Handler state drops exactly once** | `crates/ngnet-qmux/tests/lifecycle.rs` counts drops of a sentinel captured by a handler. The connection frees the C object and several boxes; this is what would catch a leak or a double drop of the Rust side. Construction and drop are also repeated 512 times. |
| **An abandoned record does not poison the next one** | `crates/ngnet-qmux/tests/events.rs`. `RecordWriter::drop` finalises an unfinished record, without which dwnx would keep appending through a pointer into a freed buffer. A companion test pins the other half — that abandoning a record which took stream data loses those bytes and desynchronises the stream — so the hazard is documented rather than discovered. |
| **A moved connection still works** | The same file. dwnx holds the address of the boxed bridge slot for the life of the connection, which is the reason it is boxed at all. Moving the `Conn` and then using it is what makes that reasoning testable. |
| **All twelve protocol events reach a Rust closure** | `crates/ngnet-qmux/tests/events.rs` drives one connection pair through a sequence that fires each. Grouped into a single test because they need that shared sequence; split apart, each would rebuild it. |
| **A peer close is an outcome, not a failure** | The same file. dwnx reports it as `DWNX_ERR_DRAINING`, which is easy to mistake for an error. The test builds a CONNECTION_CLOSE record by hand, because dwnx parses them but exposes no way to serialise one. |
| **The six write outcomes are distinguishable** | `tests/events.rs` and `tests/transfer.rs` between them produce a completed record, an idle connection, `WRITE_MORE`, a flow-control-blocked stream, a closed write side, and a too-small buffer. The last is the one that would otherwise be indistinguishable from idle, since dwnx answers `0` for both. |
| **Records may be split across reads arbitrarily** | `crates/ngnet-qmux/tests/transfer.rs` runs the same transfer twice, once in bulk and once feeding a single byte at a time, and requires identical observed events. A real transport does not preserve write boundaries, so this is not a synthetic case. |

## Enforced about the workspace

In `tests/ngnet-workspace-tests/`.

| Property | Why it is asked this way |
| --- | --- |
| **`ngnet-qmux` has exactly one dependency, and it is its bindings** | `dependency_graph.rs`. Counting alone would pass for any single dependency, so the name is checked too. Stricter than the equivalent HTTP/3 check, which needs `--no-default-features`: `ngnet-qmux` has no features at all. |
| **QMux reaches no other protocol stack, TLS, or runtime** | `dependency_graph.rs`. The names invite confusion, since QMux comes from the ngtcp2 authors and reuses QUIC's frame encoding, but the two share no code here. |
| **Nothing established reaches QMux** | `dependency_graph.rs`. The reverse direction, and it needs asking separately. Both crates are unpublished and track an unratified draft, so anything depending on them inherits that churn. |
| **No QMux binary links OpenSSL** | `linkage.rs`, via `readelf`. A native library arrives through link flags a build script emits, which no manifest inspection or `cargo tree` check can see. Unconditional here, unlike the QUIC equivalent that needs a feature matrix, because QMux has no TLS backend to turn on. |

## Enforced about the build

| Property | Where |
| --- | --- |
| **A missing submodule fails with a message naming it** | `crates/ngnet-qmux-sys/build.rs` panics with the `git submodule update` command and a pointer to `just submodules`, rather than letting the C compiler report a missing header. `just submodules` checks out `deps/dwnx`, so the advice is true. |
| **The vendored version is pinned** | `crates/ngnet-qmux-sys/tests/smoke.rs` compares `DWNX_VERSION` against the version the build script substitutes. dwnx is pre-release and has never been tagged, so this pins the placeholder; the point is that a submodule bump which changes it is noticed. |
