# Native HTTP/3 S9 root-cause follow-up

**Date:** 2026-09-04
**Base:** `ecedcc295c043385b74af404055a56830a20ce78` (PR #58)
**Last production revision tested:** `3dccd672b139` (final-review hardening)
**Result:** evidence-selected pump/retry correction retained; resolution claim blocked
**Result type:** reliability and root-cause evidence only; no performance claim

## Outcome

Armed same-occurrence failures selected the native adapter's pump/retry seam rather than a
lost runtime wake or an HTTP/3 terminal-handling defect. The connection's imminent pacing
expiry was observed due. In the original ordering, ngtcp2 then moved directly to its
30-second idle deadline while the HTTP/3 driver still held a transport-refused stream, but
the ready edge was consumed inside the driver's first idle poll and no transmit retry
followed.

The retained correction does not cancel an earlier armed sleep merely because ngtcp2 reports
a later deadline. `poll_timer` keeps and polls the earlier sleep to readiness before installing
and polling the replacement, so the replacement's waker is registered too. Both `poll_event`
and `poll_transmit` propagate that actual one-shot readiness to the driver. This is not a
periodic wake, a wake budget, or a backup sleep.

The correction does **not** receive a full resolution claim. The one allowed qualification
re-entry removed the classified residual from the next 10-process armed 1 MiB canary, but one
process still failed before `PROBE-READY` with an unclassified response-head setup/warm-up
panic. Because that occurrence was unarmed and has no same-occurrence transport records, this
run cannot show whether the production correction is sufficient. Per the predeclared stop
rule, no second production change was attempted.

## Evidence progression

All manifests are append-only and record revision and binary hashes, unique attempt identity,
process identity, timestamps, elapsed time, completion marker, classifier, checkpoint, cleanup,
record loss, and truncation.

### Base plus durable supervisor (`3264891698ce`)

The 1 MiB diagnostic shakedown completed 1/1 with all 125 exchanges exact:

- maximum per-exchange attempt records: 4,448;
- maximum per-exchange liveness records: 46,711;
- dropped attempts/liveness: 0/0;
- cleanup failures: 0.

The first 16 KiB batch completed 8/10. Runs 1 and 6 failed in body drain at exchanges 119
and 18. Both final writes were transport-blocked with positive stream, connection and
congestion credit. Their expiries were respectively 1,021 ns and 817 ns away. The timer was
then observed due, no packet was produced, a replacement timer was armed, and the adapter
parked until idle close. Existing records did not carry the replacement deadline, so this
capture selected one missing bounded fact rather than a production change.

### Deadline instrumentation (`3b245595979c`)

Timer-arm, timer-due and timer-ready records were extended with `now` and `deadline` in the
same ngtcp2 nanosecond clock domain as write attempts. The next 16 KiB batch again completed
8/10, with armed body-drain failures at exchanges 25 and 31.

In run 7 the final server write was blocked at `829762296` with expiry `829763847`.
The expiry was handled at `829764911`; without producing a packet, ngtcp2 rearmed for
`30829757747`, exactly 30 seconds beyond the idle reference. Run 9 repeated the same
ordering: the final block was 1,150 ns before expiry, the timer was handled 653 ns late,
and the replacement was the 30-second idle deadline. The runtime wake and timer owner had
worked; the missing operation was a transmit retry after the due-timer pump made no progress.

### First correction and qualification re-entry (`3dfa65fca0be`)

The first correction carried an explicit due-timer flag from the pump into one generic-driver
retry. Its armed 16 KiB canary completed 10/10. The armed 1 MiB canary completed 8/10:

- run 3 failed at response head with a complete armed same-occurrence trace;
- run 9 failed during setup/warm-up before `PROBE-READY`, so it had no `PROBE-FAIL` or armed
  transport evidence.

Run 3 showed the second ordering of the same seam. The final client write was blocked at
`140741232861`, 4,054 ns before `140741236915`. The sleep remained armed for that exact
expiry, but `poll_timer` next ran at `140741238171` and replaced it with idle deadline
`170741224355` without preserving a ready event. The final correction moved the invariant
into `poll_timer`: a later replacement cannot cancel the earlier sleep; the earlier sleep is
polled to readiness before the replacement is installed and registered.

## Final observed processes

The blocker-producing qualification revision was `72b173c6eb7d`:

| Schedule | Processes | Completed | Classified | Unclassified | Outer kills | Cleanup failures |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Armed diagnostic, 125 × 16 KiB | 10 | 10 | 0 | 0 | 0 | 0 |
| Armed diagnostic, 125 × 1 MiB | 10 | 9 | 0 | 1 | 0 | 0 |
| Unarmed reliability, 125 × 16 KiB | 10 | 10 | 0 | 0 | 0 | 0 |
| Unarmed reliability, 125 × 1 MiB | 10 | 10 | 0 | 0 | 0 | 0 |
| Exact fixture, 125 × 16 KiB | 10 | 10 | 0 | 0 | 0 | 0 |
| Exact fixture, 125 × 1 MiB | 10 | 10 | 0 | 0 | 0 | 0 |

The unclassified armed 1 MiB failure occurred before readiness. The retained tail is:

```text
PROBE-CHECKPOINT exchange=0 phase=setup received_bytes=0 integrity=exact-so-far terminal=false
thread 'main' (...) panicked at tests/ngnet-bench/src/lib.rs:2278:14:
an ngtcp2 response head: Error { kind: Stream,
  detail: "the exchange ended before a response arrived", ... }
```

This is not counted as a classified S9 occurrence, is not omitted from the denominator, and
prevents the planned 100-process-per-size final reliability claim. The clean 10-process
samples are reported as observations only; they are too short for the planned approximate
3% one-sided 95% per-process bound.

The final schedules used these exact command forms, with `start=1` and a distinct manifest
path for each row:

```sh
S9_REVISION=72b173c6eb7d ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 diagnostic \
  16384 125 10 685 1 <final-armed-16k.manifest>
S9_REVISION=72b173c6eb7d ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 diagnostic \
  1048576 125 10 685 1 <final-armed-1m.manifest>
S9_REVISION=72b173c6eb7d ./target/release/examples/s9_supervisor \
  ./target/release/examples/probe ngnet-quic-h3 reliability \
  <body-bytes> 125 10 180 1 <final-reliability-size.manifest>
S9_REVISION=72b173c6eb7d ./target/release/examples/s9_supervisor fixture \
  <ngtcp2_fixture_repeats_size_exactly> 10 900 1 <final-fixture-size.manifest>
```

No controlled-contention batch was run at the final revision. All final attempts were isolated;
there were zero interrupted attempts, outer kills or cleanup failures. “Eligible/confounded”
is therefore not applicable to these final schedules.

### Post-review production revision

Final review found that sampling `now` and cancelling the earlier sleep still left a
nanosecond-scale transition race. Revision `85062e839e4b` instead retains and polls an earlier
sleep whenever ngtcp2 reports a later deadline; only after that sleep resolves does it install
and poll the replacement. It also makes interrupted cleanup fail closed and records resource
guards durably.

The production change did not restart the blocked 100-process claim. It did receive a new
isolated armed sample from zero:

| Schedule | Processes | Completed | Failures | Guarded/unexecuted |
| --- | ---: | ---: | ---: | ---: |
| Armed diagnostic, 125 × 16 KiB | 10 | 10 | 0 | 0/0 |
| Armed diagnostic, 125 × 1 MiB | 10 | 10 | 0 | 0/0 |

The 1 MiB sample took 700,908 ms and is retained as a final-code confirmation, not as a
replacement for the earlier 9/10 blocker denominator or as a 3% bound.

Final review then required the second `poll_timer` caller to propagate the same actual
one-shot readiness and added direct regression coverage for both callers. Revision
`3dccd672b139` completed another fresh armed 10/10 at 16 KiB and 10/10 at 1 MiB. The latter
took 777,727 ms. These samples confirm the delivered code and do not erase or replace the
earlier blocker denominator.

## Regression and repository gates

`captured_s9_progress_seam_regression` constructs a packet-bounded large write on the
hand-moved connection clock, reaches a transport-refused write, advances across the pacing
deadline and asserts exactly one retry wake and no self-wake loop. It fails with wake count
zero when earlier-sleep preservation is removed and passed 100/100 exact release repetitions
on both retained correction revisions. The final form exercises both call sites: a pending
earlier sleep produces no wake, while readiness consumed first by either `poll_event` or
`poll_transmit` produces exactly one.

All targeted native QUIC/H3 tests, exact fixtures, default workspace tests, clippy matrices,
warning-denying rustdoc builds, benchmark smoke compilation, release example compilation and
touched-file formatting passed. Two unrelated qualification caveats are retained:

- the first combined release suite observed one timeout in
  `a_multi_packet_payload_crosses_to_quinn_byte_for_byte`; its immediate rerun, ten isolated
  repetitions and subsequent full-suite runs passed;
- one all-feature workspace run failed four compio tests because io_uring creation returned
  `ENOMEM`; its isolated rerun passed.

During final multi-model review, one of 13 repeated
`cargo test -p ngnet-quic --all-features` runs failed
`diagnostic_record_collections_are_bounded_and_report_drops`: the global armed store observed
three dropped attempts where the isolated test expected one. The other 12 runs passed. The
failure is consistent with process-global diagnostic arming overlapping another recorder, is
not an S9 transport result, and remains recorded rather than being converted into a pass.

`cargo test --doc --workspace --all-features` also fails in generated `ngnet-h2-sys`
bindings because bindgen preserved C examples as Rust doctests. The warning-denying
documentation build matrix passed; the generated-binding doctest failure is preserved rather
than attributed to native HTTP/3.

## Auditable files

Raw manifests remain local-only under the work item's `never-commit` lifecycle. The committed
record includes their hashes so local evidence can be verified:

| Manifest | Bytes | SHA-256 |
| --- | ---: | --- |
| `3264891698ce/isolated-1m.manifest` | 1,546 | `ff9674c8605f26a941b6f66b02af1b48c13ba6f311dedb39a90fff157c6501dd` |
| `3264891698ce/isolated-16k.manifest` | 344,100 | `cf7935be9e1360f98c06d18de7bc3de9de13cb20b855446109d819bed12a7006` |
| `3b245595979c/isolated-16k.manifest` | 427,849 | `fb5e59ec6f83c406434e906b0f8838a56c4b7560c9fbf00c6dafb1ebfb6927bb` |
| `3dfa65fca0be/final-armed-16k.manifest` | 10,389 | `d5e706f5a7ac3079bc603986c5e2830cc3c83618e5708085d46b9c41487a3622` |
| `3dfa65fca0be/final-armed-1m.manifest` | 1,292,338 | `f7a024f16b59d67040da1182a8cccb344feea3ebf1d351f8ab4eec6126bb09cf` |
| `72b173c6eb7d/final-armed-16k.manifest` | 10,388 | `18db0df5eabfcfc414676599143807d508e8a712d53a0a6e780e5bbe06acced6` |
| `72b173c6eb7d/final-armed-1m.manifest` | 11,629 | `81b3568cf77025e2a0ba022d9dda2729f738b104d4fc15c92fdbd3393a431de9` |
| `72b173c6eb7d/final-reliability-16k.manifest` | 10,367 | `9ff2c613876f07e21a2043427aa903d8cc2677f127eefad25cba03d6bcf48284` |
| `72b173c6eb7d/final-reliability-1m.manifest` | 10,418 | `1821fcef5eba5c9ea8b2138b045af743690e88d723275d38c65815554056f208` |
| `72b173c6eb7d/final-fixture-16k.manifest` | 9,562 | `aeab6eb0edc39c8884e53bb9aea91186123b470ff74c9f18a55e983591eb64c8` |
| `72b173c6eb7d/final-fixture-1m.manifest` | 9,602 | `4cd11ec44c95be5d68eced0bbda1b559fcef8502a87cfb7dfb13157bef47ed47` |
| `85062e839e4b/postreview-armed-16k.manifest` | 10,415 | `4497f0b3314af37e271e72b83eb935926c4b0e627be01e3ff271b453f49a3304` |
| `85062e839e4b/postreview-armed-1m.manifest` | 10,507 | `e6199e18255247742e8294b0e4a28ab2747c0fc9ccb9aa8be47d579f19a5e9e2` |
| `3dccd672b139/final-r2-armed-16k.manifest` | 10,415 | `95fc2233df9164032e92b0f53ca9f3bf56a4a44434e28de1deb8d8d93f4a36e7` |
| `3dccd672b139/final-r2-armed-1m.manifest` | 10,507 | `d60f76c138f3de458d6d2db1da7ad287f71fdeabfae9882c30603e9bf5bd0acf` |

At the final qualification build, the supervisor SHA-256 was
`1b2d06e2edeb5a9966d40594e49ec33a68fd8f656701dc130dc69b7b8053fc89`
at 797,112 bytes
and the probe SHA-256 was
`b441a1e0a3d854aa46e81a489119fcf82e003a183fd94e22f3ac3573326ba047`
at 11,316,816 bytes.

For post-review revision `85062e839e4b`, the supervisor SHA-256 was
`067b5217b2474f7d5a94d2b717008a8eb353c1a9140732f1391e428ee63b7a8e`
at 801,984 bytes
and the probe SHA-256 was
`7f815f18895004e2c7e1d44826eda691d000c5be2f0f27c4a014238b5dfa7058`
at 11,316,992 bytes.

For delivered code revision `3dccd672b139`, the supervisor SHA-256 was
`8d633f504016b30dcbb7f99db8d5f3810d9794945ce3904f7327eac9ec518ec4`
at 802,648 bytes and the probe SHA-256 was
`b3c1d4f5881a1ac0b8fc85cfcf47e38f3a9b97b0f3035590073ce250483b58d0`
at 11,316,728 bytes.

## Blocker and next capture

The production change is retained because it is selected by four armed same-occurrence
failures, covers both observed ordering variants, and has a deterministic falsified
regression. What remains unsafe is claiming end-to-end resolution.

The correction preserves one wake per earlier deadline. If the resulting retry is refused
again while ngtcp2 keeps only the idle deadline, no second synthetic edge is created; a future
capture must retain that recurrence rather than assuming one retry always succeeds.

The exact next capture has two bounded steps. First, emit a typed pre-readiness failure record
around the native warm-up response-head call before panic, with phase, classifier and
`diagnostics_armed=false`. If it reproduces, add reviewed pre-readiness transport capture:
reset and arm immediately before warm-up, drain the warm-up interval on success or failure,
then reset and re-arm at `PROBE-READY` so normal workload accounting stays separate. Only then
run a fresh supervised armed 1 MiB canary. If that class disappears, the final 100-process
16 KiB and 100-process 1 MiB reliability schedules can restart from zero. If it recurs, it
must be diagnosed as its own setup/lifecycle occurrence and cannot be attributed to the S9
pump correction without that same-occurrence pre-readiness transport evidence.
