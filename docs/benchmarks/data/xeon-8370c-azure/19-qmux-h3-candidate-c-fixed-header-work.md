# 19 — Candidate C: fixed HTTP/3 header work

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-28
**Commit(s):** baseline `c7c95d9` against candidate `c188758`, reverted by `3b5c576`
**Baseline:** production code identical to `364dbb2`
**Disposition:** **implemented, measured, and reverted**; 20 mallocs and three reallocs were
removed, but no 100-sample elapsed pass cleared the 2% floor
**Cases:** warmed empty QMux/H3 exchange over duplex and loopback socket
**Command:** `taskset -c 3 target/release/examples/probe qmux-{duplex,socket} body 0 10000`
under repeated 4 kHz DWARF `task-clock:u` profiles; exact malloc/realloc/free counts from
100/300-exchange uprobes; Criterion as three baseline/candidate passes, 100 samples, 3 s
measurement, 1 s warm-up, and matching H2 controls:
`taskset -c 3 <duplex-serial-binary> --bench
'serial_latency/(ngnet-h2|ngnet-qmux-h3)$' <criterion-options>` and
`taskset -c 3 <socket-serial-binary> --bench
'transport_serial_latency/(ngnet-h2-tokio|ngnet-qmux-h3-tokio)$' <criterion-options>`, where
`<criterion-options>` was `--sample-size 100 --measurement-time 3 --warm-up-time 1
--save-baseline <pass> --noplot`
**Repetitions:** two sequential profile repeats per substrate and three interleaved Criterion
baseline/candidate passes per substrate
**Controls:** the immutable 2% floors, each pass's H2 movement, and the observed repeat spread
**Exclusions:** an accidentally concurrent first profile pair was discarded before analysis and
reacquired sequentially on CPU 3; no reported profile or exact-count observation was discarded

## What was being asked

Candidate C asked whether fixed Rust header storage, head construction, native allocation, QPACK,
or per-stream registry work could produce a safe serial optimization. Retention required:

- exact empty-exchange allocator calls below **128**;
- both duplex and socket serial faster beyond matching control movement, both spreads, and 2%;
- no regression in duplex/socket 1 MiB or socket concurrency 8/64.

The initial profile-only bound was withdrawn during review because it omitted deallocation,
reallocation-copy and memcpy time. The complete safe storage prototype was therefore implemented
and timed directly; no retention decision below relies on that bound.

## Results

### Exact allocation sites

The 10/30 subtraction reproduced the warmed baseline at approximately 128 calls per exchange
(128.35 recorded stack events; run 16's lossless `perf stat` count is **128**). Stack membership
and capacity-before-push counters produced:

| Site | Allocations per exchange | Notes |
| --- | ---: | --- |
| `Events::push_field` name/value copies | **16** | eight fields, two independent `Vec<u8>` allocations each |
| outgoing field payloads | **16** | eight names, seven borrowed values copied by `OwnedFields::push`, and the authority input allocation |
| `OwnedFields::views` `Vec<Header<'_>>` | **4** | validation and submission paths |
| submit-site `Vec<nghttp3_nv>` | **2** | request and response |
| `HeaderMap::try_reserve_one` / head assembly | **8** | request/response map growth |
| native nghttp3 allocator shim | **17** | one QPACK-associated stack; the rest nghttp3 stream/field storage |
| all other Rust/runtime/fixture sites | **65** | total residual to 128 |

These rows are mutually classified malloc events, not percentages. The former 14-event outgoing
bucket was an undercount caused by assigning two inlined conversions to the residual bucket:
the fixture emits six request fields and two response fields, hence eight name buffers and eight
value buffers. Seven borrowed values allocate in `OwnedFields::push`; the already-owned authority
allocates immediately before the push. Temporary capacity counters independently found three
`OwnedFields.fields` growth calls and three received `Partial.fields` growth calls per exchange.
Because only the first growth uses `malloc`, subsequent growth is visible in the separate
`realloc` count rather than as another row above.

Trailers have zero population in the empty fixture. `request_head`/`response_head` continue to
construct `HeaderName`, `HeaderValue`, method/status, scheme, authority, path/query and URI values;
their allocation events are represented by the map/head row or residual rather than counted
again.

### H3-only work outside field storage

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
- `ClosedStreams` grows until its 1,024-entry bound, even after the first warm exchange; after
  Criterion's warm-up fills it, each insertion evicts one entry with no further capacity growth;
- `Shared::drain_work` at 1.69–2.22% as already optimized shared-state work, with reused storage
  and **zero** allocation.

`Registry` and server `Tasks` use per-exchange `BTreeMap` insert/lookup/remove paths. Their named
registry self samples totaled at most 0.77% (under 0.18 µs) on duplex and 0.39% (under 0.09 µs)
on socket; `Tasks` was below the approximately 0.025 µs symbol resolution. Replacing those maps
would also introduce peak-concurrency retention policy. Even granting all named work as
removable cannot lift the prototype's 0.74–0.88% socket improvement to 2%, so it is not an
independently plausible candidate.

Those sites are absent from H2 but are not header-storage mechanisms. The driver, registry, and
native QPACK costs remain attribution, not permission to charge them to C1–C3.

### Measured C1–C3 prototype

Commit `c188758` implemented the complete safe set rather than relying on the incomplete sampled
bound:

- **C1:** received names up to 32 bytes and values up to 128 bytes used bounded inline storage;
  larger fields used an owned boxed slice, so callback lifetime was never extended and one large
  field could not pin a large shared arena;
- **C2:** received sections reserved eight field slots and outgoing sections reserved their known
  field count;
- **C3:** validation iterated without constructing a `Vec<Header>`, while private request,
  response and trailer submission accepted a checked `nghttp3_nv` vector directly. Public
  low-level submission APIs and their validation remained unchanged.

The exact two-point allocator results include allocator teardown and distinguish growth:

| Revision | `malloc` | `realloc` | `free` |
| --- | ---: | ---: | ---: |
| `c7c95d9` baseline | **128.02** | **6.02** | **128.02** |
| `c188758` prototype | **108.02** | **3.02** | **108.02** |
| change | **−20.00** | **−3.00** | **−20.00** |

C1 removes 16 received name/value mallocs. C3 removes four intermediate-vector mallocs: two
validation vectors and two redundant submission vectors. C2 does not remove the first malloc for
each outer vector, but it removes the measured growth reallocations. Baseline realloc stacks split
between Rust `RawVec::finish_grow` and native nghttp3; the prototype removed exactly three Rust
calls while native calls remained. Reallocation copying, field memcpy, destruction, and all
other effects are included in the direct elapsed measurements below.

Criterion median point estimates are microseconds. Raw delta compares the candidate with its
matching baseline pass; normalized delta compares `(candidate QMux/H3 ÷ candidate H2)` with
`(baseline QMux/H3 ÷ baseline H2)`.

| Substrate/pass | Baseline H2 | Baseline QMux/H3 | Candidate H2 | Candidate QMux/H3 | Raw delta | Normalized delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| duplex 1 | 10.139 | 20.793 | 10.106 | 20.522 | **−1.30%** | **−0.98%** |
| duplex 2 | 10.078 | 20.798 | 10.017 | 20.398 | **−1.92%** | **−1.33%** |
| duplex 3 | 10.075 | 20.691 | 10.050 | 20.375 | **−1.53%** | **−1.28%** |
| socket 1 | 16.794 | 31.750 | 16.986 | 31.496 | **−0.80%** | **−1.92%** |
| socket 2 | 16.824 | 31.879 | 17.029 | 31.598 | **−0.88%** | **−2.08%** |
| socket 3 | 16.986 | 31.803 | 16.889 | 31.569 | **−0.74%** | **−0.17%** |

The candidate's QMux/H3 range was 20.375–20.522 µs on duplex and 31.496–31.598 µs on socket.
No raw pass cleared the immutable 2% floor. The result therefore fails the pre-registered
dual-substrate gate without needing larger-workload regression guards.

### Semantic and cleanup validation

The prototype added a deterministic inline/heap-fallback ownership test. The complete
`ngnet-h3` and `ngnet-h3-tests` suites passed; existing tests exercised:

- pseudo-header order/uniqueness, unknown pseudo-headers and `:protocol`;
- CONNECT, scheme, authority/host, userinfo, path, asterisk, forbidden-field and `te` rules;
- informational responses, trailers, duplicates through `HeaderMap::append`, and malformed heads;
- blocked-QPACK replay, failed-submit rollback, reset/stop cleanup, stream close ordering, and
  oversized field-section rejection.

No unsafe lifetime extension was introduced. Inline storage moved with the received event; heap
fallback owned its bytes after callback return; both were dropped on every early error. The
prototype was then reverted completely by `3b5c576`.

The preserved Criterion binary hashes were baseline/candidate
`294ec76d110f77eeb86b2c222fa27cc22e6462df6cd6e4077bf55231f368776a` /
`52c555765639541d7c296bfc7f1070ec05729a63da9538356a7017b3d7904ed9` for duplex and
`1a704657a524a6677a960c36f7d1ba98df66fdced24376ee1481009de9e66c82` /
`68d4f04a6a6af0d169d74f0acd1abaab3a9160434099dd400ea42ec429d6cad9` for socket.

### Disposition

Candidate C is **rejected after implementation and direct measurement**. Its exact allocator
improvement is real, but allocation count alone did not produce a qualifying elapsed-time win.
C4's one-entry section scan remained below profile resolution. Registry/Tasks map replacement is
also rejected as a companion: its complete measured socket population is too small to rescue the
failed prototype, and it would add a new retained-capacity policy. Do not retry these mechanisms
from allocation counts alone.

### Validation

The candidate's focused H3/H3-tests suites passed before timing. After `3b5c576`, production
source is again identical to `7c36518`; the restored-source release QMux-H3/QMux-H3-tests,
feature variants, clippy, rustdoc, and diff hygiene passed at the phase gate. The pristine
release probe reproduced Phase 4's SHA-256
`0e8b5b1f1a71759db9e53b35e306e9c81ab2cf8292594b4625839279de9370c8` after temporary profiles,
uprobes, copied binaries and Criterion baselines are removed.

## Drift controls in the same session

H2 movement was −0.61% to −0.25% on duplex and −0.57% to +1.22% on socket. Candidate QMux/H3
ranged 20.375–20.522 µs duplex and 31.496–31.598 µs socket.

## What this establishes

- C1–C3 remove exactly 20 mallocs and three reallocs without unsafe lifetime extension.
- The allocation reduction does not produce a qualifying dual-substrate elapsed-time win.
- C4 and registry/task-map work are too small to rescue the failed socket gate.

## What it does not

- It does not change HTTP semantics, retain the prototype, or measure an upstream native QPACK
  algorithm change.
