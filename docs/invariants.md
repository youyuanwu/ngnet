# Invariants

Properties the suite *pins* rather than merely exercises. Each is a claim the crate makes
about itself that would otherwise decay silently, so each has a test whose failure is the
only warning you get.

If you change something here, the test is not in your way — it is the reason the claim can be
made at all.

## Structural — `tests/invariants.rs`

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

## Public surface — `tests/compat_surface.rs`

`the_sans_io_surface_is_unchanged` and `the_asynchronous_surface_is_unchanged` pin the public
API by referencing it, so a signature change breaks a build rather than a downstream user.

**Adding public API means extending this file**, or the test fails.

## Performance — `tests/http_zero_alloc.rs`

Measured across whole driver passes, on the thread that armed the counter, after warm-up.
Per-stream setup is excluded deliberately: the claim is about the recurring cost of moving
frames, not the one-off cost of standing a stream up.

| Test | Claim |
| --- | --- |
| `steady_state_receive_allocates_nothing` | A steady-state receive pass — and the body drainer's poll — allocate **zero**. |
| `steady_state_send_allocates_nothing_on_the_borrowed_path` | Likewise for sending, on the borrowed write path. |
| `steady_state_send_allocates_nothing_on_the_vectored_path` | Likewise on the gathering path — so gathering costs nothing the borrowed path did not. |
| `steady_state_multiplexed_send_allocates_nothing_on_the_vectored_path` | And still zero when eight streams are multiplexed, which is where the driver's own buffer would be tempted to grow. |
| `steady_state_send_allocates_nothing_on_the_owned_region_path` | Likewise on the owned-region path — the completion transport's gathering strategy allocates nothing in steady state either. |
| `the_read_buffer_pool_settles_to_a_fixed_size` | The pool reaches a high-water mark during warm-up and does not grow afterwards. |
| `the_owned_write_path_coalesces_a_pass_into_one_write` | One write per pass, with more than one frame's worth of bytes — so the single write is not trivially single. |
| `the_borrowed_write_path_writes_each_block_separately` | More than one write per pass on identical traffic, constant across passes. |
| `the_vectored_write_path_coalesces_a_multiplexed_pass_into_one_write` | One write for a whole multiplexed pass — the coalesced path's write count at the borrowed path's allocation count, which is the entire claim of the strategy. |
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
- **Compile-fail doctests** on `TransportWrite::write_borrowed` and
  `TransportWrite::write_vectored` — the ways an adapter could implement a write strategy
  inconsistently do not compile.
- **A compile-fail doctest** on `Connection` — discarding the driver is an error, because a
  connection that compiles and never sends a byte is the trap the type exists to prevent.
- **A negative `Send` assertion** in `ngnet-h2-tests` — a connection over a non-`Send`
  transport is genuinely not `Send`. Running it on a `LocalSet` proves nothing on its own,
  since `spawn_local` accepts `Send` futures too.

## Verifying

The feature matrix matters: a doc link to a `tokio`-gated item once passed `--all-features`
and broke every other configuration.

`.github/workflows/ci.yml` runs everything below on every pull request. It reads the
compiler from `rust-toolchain.toml`, so a local run uses the same one. **If you add a check
to CI, add it here too** — this list and that workflow are meant to say the same thing.

```sh
cargo test --workspace --all-features
cargo test --workspace
cargo test -p ngnet-h2 --no-default-features
cargo test -p ngnet-h2-tests --features completion

cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy -p ngnet-h2 --all-targets -- -D warnings
cargo clippy -p ngnet-h2 --no-default-features --all-targets -- -D warnings

for f in "" "--no-default-features" "--all-features" "--features tokio" "--features completion"; do
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p ngnet-h2 $f
done

# Runs each benchmark once without timing it. Benchmarks are not part of `cargo test`, so
# without this they rot silently as the API moves.
cargo bench --workspace -- --test
```

Run `touch crates/ngnet-h2/src/lib.rs` before a final run so no stale incremental artefact
flatters the result.

CI additionally checks a property no source file carries: that the completion transport's
build contains **no readiness backend**. That is a fact about the resolved dependency graph,
and cargo unifies features across the workspace, so a crate added later could enable compio's
`polling` and restore the silent epoll fallback without a line of code changing. The runtime
assertion in the compio test only fires where io_uring is genuinely absent, which is not true
of CI or of most developer machines — this is the check that catches it where io_uring exists.

```sh
cargo tree -p ngnet-h2 --features completion -e features | grep 'compio-driver feature "polling"'
```

Two things CI deliberately does not do, both explained in the workflow: no repository-wide
`cargo fmt --check` (this repo is not globally rustfmt-clean, and the convention is to
format only touched files), and no MSRV job. On the second, note that the `completion`
feature has a **higher minimum toolchain** than the crate's declared `rust-version`: compio's
buffer crate needs a newer compiler than 1.85. The default and `tokio` builds do honour 1.85,
which was not true before this was checked — a let-chain had been present since the first
commit, making the declared minimum wrong from the beginning.
