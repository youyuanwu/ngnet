# 19 — Candidate C: fixed HTTP/3 header work

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Baseline:** `7c36518` (production code identical to `364dbb2`)
**Disposition:** **closed before implementation**; the complete safe C1–C3 set cannot clear the
socket serial timing floor, and C4 has no serial population
**Cases:** warmed empty QMux/H3 exchange over duplex and loopback socket
**Commands:** `taskset -c 3 target/release/examples/probe qmux-{duplex,socket} body 0 10000`
under repeated 4 kHz DWARF `task-clock:u` profiles; exact malloc stacks from 10/30-exchange
`candidate_c_malloc=malloc` uprobes; branch-neutral source counters over ten exchanges
**Controls:** the immutable 2% floors and run 16's unchanged H2 control/spread bounds
**Exclusions:** an accidentally concurrent first profile pair was discarded before analysis and
reacquired sequentially on CPU 3; no reported profile or exact-count observation was discarded

## Question and gate

Candidate C asked whether fixed Rust header storage, head construction, native allocation, QPACK,
or per-stream registry work could produce a safe serial optimization. Retention required:

- exact empty-exchange allocator calls below **128**;
- both duplex and socket serial faster beyond matching control movement, both spreads, and 2%;
- no regression in duplex/socket 1 MiB or socket concurrency 8/64.

The selection rule required a directly attributed removable upper bound above the complete
threshold on both substrates before code was written.

## Exact allocation sites

The 10/30 subtraction reproduced the warmed baseline at approximately 128 calls per exchange
(128.35 recorded stack events; run 16's lossless `perf stat` count is **128**). Stack membership
and capacity-before-push counters produced:

| Site | Allocations per exchange | Notes |
| --- | ---: | --- |
| `Events::push_field` name/value copies | **16** | eight fields, two independent `Vec<u8>` allocations each |
| `OwnedFields::push` and its inputs | **14** | outgoing request/response names and values |
| `OwnedFields::views` `Vec<Header<'_>>` | **4** | validation and submission paths |
| submit-site `Vec<nghttp3_nv>` | **2** | request and response |
| `HeaderMap::try_reserve_one` / head assembly | **8** | request/response map growth |
| native nghttp3 allocator shim | **17** | one QPACK-associated stack; the rest nghttp3 stream/field storage |
| all other Rust/runtime/fixture sites | **67** | total residual to 128 |

These rows are mutually classified malloc events, not percentages. Temporary exact capacity
counters independently found **three** `OwnedFields.fields` growth allocations and **three**
received `Partial.fields` growth allocations per exchange. The partial-stream registry itself
did not grow after warm-up.

Trailers have zero population in the empty fixture. `request_head`/`response_head` continue to
construct `HeaderName`, `HeaderValue`, method/status, scheme, authority, path/query and URI values;
their allocation events are represented by the map/head row or residual rather than counted
again.

## H3-only work outside field storage

The warmed exchange has two field sections and eight field callbacks. `Events::slot` therefore
does ten one-entry slot scans (two begins plus eight fields), while the two end-section lookups
perform the corresponding removal scans. None allocates. Native QPACK encode/decode appears
inside nghttp3's submit/read calls, but there is no per-exchange Rust-level QPACK entry point:
control, encoder and decoder streams are bound once before the measured warm exchange.
`deferred_consume_cb` and foreign-buffer `view_of` replay are the only Rust QPACK-adjacent paths;
the empty fixture produced no foreign replay.

Repeated task-clock profiles found:

- `BodyRegistry` accounting self samples but **zero** per-exchange registry allocation for an
  empty body;
- `Deliveries` credit/silence bookkeeping self samples but **zero** allocation;
- warmed `ClosedStreams` contains/insert self cost of 0.10–0.23%, with **zero** growth allocation;
- `Shared::drain_work` at 1.69–2.22% as already optimized shared-state work, with reused storage
  and **zero** allocation.

Those sites are absent from H2 but are not header-storage mechanisms. The driver, registry, and
native QPACK costs remain attribution, not permission to charge them to C1–C3.

## Direct removable self-cost

Each profile ran 10,000 exchanges. Duplex consumed 22.20–24.78 µs of sampled task-clock per
exchange; socket consumed 22.13–22.50 µs of CPU inside a 33.33–33.38 µs wall-time exchange.
The socket claim floor is therefore at least **0.667 µs** before considering any larger fresh
spread.

The following socket bounds include the named function's self samples and only malloc descendants
from allocations the option can remove:

| Option | Exact maximum allocation reduction | Socket removable bound, repeats 1 / 2 |
| --- | ---: | ---: |
| C1, inline received small fields | 16 | 0.10 / 0.17 µs |
| C2, reserve known-capacity containers | 3 (plus map growth only with a larger head rewrite) | 0.08 / 0.09 µs |
| C3, remove intermediate `Header` vectors | 4 | 0.35 / 0.35 µs |
| **complete C1–C3 set** | **23**, yielding at best 105 total calls | **0.53 / 0.61 µs** |

The complete set can satisfy the allocation count but cannot reach the 0.667 µs socket floor in
either repeat. This is an upper bound: it grants C3 every `OwnedFields::views` iterator/vector
self sample even though validation must remain, and grants C2 all three outer-vector growths.
The duplex bound can cross 2%, but both claim targets are mandatory.

C4's one-entry serial scan had no standalone self symbol at the profile's 0.10% resolution
(less than 0.025 µs per exchange), removes no allocation, and cannot bridge the socket gap. It is
not admitted as a companion.

## Option decisions

- **C1 — inline small-field accumulator:** safe with a bounded inline/heap fallback and
  early-error drop, but its 16 allocations plus directly attributed copy/allocator work are too
  small. It was not implemented.
- **C2 — capacity reservation:** `views()` and `nghttp3_nv` collection already use exact-size
  iterators. The only directly removable growth is three `OwnedFields` expansions; nghttp3 does
  not expose a received field count at `begin_section`, and `http::Request::builder()` provides
  no reserve hook. It cannot close the gap.
- **C3 — collapse submit buffering:** a private direct `nghttp3_nv` path could preserve
  `Header::new` validation order and public APIs while removing four `Header` vectors. It is the
  largest safe option, but still only about 0.35 µs on socket.
- **C4 — replace the slot scan:** no allocation and no meaningful serial population. It remains
  a concurrency-only hypothesis rather than a companion to a non-qualifying set.

## Protocol and cleanup checklist

No code changed, so every checked rule remains byte-for-byte present:

- outgoing request rejects CONNECT, unsupported schemes, missing authority, forbidden
  connection-specific fields, and `te` other than `trailers`;
- outgoing response rejects `te` and forbidden fields; outgoing trailers reject forbidden names;
- incoming request requires pseudo-headers before regular fields, rejects duplicates, unknown
  pseudo-headers and `:protocol`, and requires `:method`, `:path`, and authority or matching host;
- incoming request rejects bad name/value syntax, forbidden fields, invalid `te`, unsupported
  scheme, invalid method, CONNECT, invalid authority, userinfo, invalid path/query, asterisk form,
  and URI/head assembly failure;
- incoming response permits only one leading `:status`, requires it, validates status/name/value,
  and rejects response `te` and forbidden fields;
- incoming trailers reject pseudo-headers, malformed names/values and forbidden fields;
- regular duplicates still use `HeaderMap::append`, preserving order rather than replacing;
- informational responses still bypass final-response settlement; malformed heads still run the
  server cleanup; stream close still forgets bodies before delivery bookkeeping.

Blocked-compression replay, trailers, reset/stop mid-section, failed-submit rollback, and
oversized-section cleanup remain covered by the unchanged suites. Because no storage shape was
selected, no new retention or cleanup state exists to test.

## Disposition

Candidate C is **documentation-only: gate-incompatible**. Exact counts show that C1–C3 could
reduce 128 calls to about 105, but direct dual-substrate profiling shows the complete safe set
cannot clear socket's 2% floor. C4 has no serial population. Allocation reduction alone is not a
retention reason, so no prototype or count-only test was created.

## Validation

The source remained production-identical to `7c36518`. Focused H3/H3-tests and release
QMux-H3/QMux-H3-tests suites passed, as did the H3 no-default-feature suite, all-target/all-feature
and no-default-feature clippy with warnings denied, H3 rustdoc with warnings denied, and
`git diff --check`. The pristine release probe reproduced Phase 4's SHA-256
`0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` after all temporary
instrumentation was removed.
