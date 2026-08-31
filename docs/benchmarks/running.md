# Running the benchmarks

## What has to be in place first

The benchmark crate builds four native libraries from source, and the set grew when the
HTTP/3-over-QMux and ngtcp2 arms landed. A checkout that used to be sufficient no longer is,
so this section is the first thing to check when `cargo bench -p ngnet-bench` fails before it
runs anything.

| Submodule | Reached through | Needed by |
| --- | --- | --- |
| `crates/ngnet-h2-sys/vendor/nghttp2` | `ngnet-h2-sys` | every HTTP/2 arm |
| `crates/ngnet-h3-sys/vendor/nghttp3` **and its nested `lib/sfparse`** | `ngnet-h3-sys` | every QMux arm |
| `crates/ngnet-qmux-sys/vendor/dwnx` | `ngnet-qmux-sys` | every QMux arm |
| `crates/ngnet-quic-sys/vendor/ngtcp2` | `ngnet-quic-sys` | the complete ngtcp2 HTTP/3 arm |

`nghttp3/lib/sfparse` is not optional and is not a test dependency: the structured-field
parser is part of the library, and nghttp3 does not compile without it. "Clone
non-recursively" is therefore correct for `nghttp2` and `dwnx` and quietly wrong for
`nghttp3`. The [`justfile`](../../justfile) encodes the exact set so it does not have to be
remembered:

```sh
just submodules

# Or, to see what is checked out, missing (-) or at an unexpected commit (+):
just submodules-status
```

By hand, if `just` is not installed — or is older than 1.27, which cannot parse the
[`justfile`](../../justfile)'s `[doc(...)]` attributes and fails with
`error: Unknown attribute 'doc'` before running anything:

```sh
git submodule update --init \
  crates/ngnet-h2-sys/vendor/nghttp2 \
  crates/ngnet-h3-sys/vendor/nghttp3 \
  crates/ngnet-qmux-sys/vendor/dwnx \
  crates/ngnet-quic-sys/vendor/ngtcp2
git -C crates/ngnet-h3-sys/vendor/nghttp3 submodule update --init lib/sfparse

# The equivalent of `just submodules-status`:
git submodule status --recursive
```

Building those four needs a C compiler, CMake 3.14 or newer, and libclang for `bindgen`.

The complete ngtcp2 HTTP/3 arm **does** require OpenSSL 3.5 or newer. `ngnet-quic-sys`
builds ngtcp2 and drives its handshake through OpenSSL's QUIC TLS API. A host without that
toolchain may still run targets that do not reach `ngnet-quic-sys`, but it cannot build the
workspace-wide smoke check, the QUIC-stack Criterion targets, or the ngtcp2 probe. Report
that as a host limitation rather than substituting another TLS or QUIC implementation.

`cargo tree -p ngnet-bench --prefix none | grep '^ngnet-.*-sys' | sort -u` settles the
dependency set for the current checkout rather than relying on this table.

## The commands

```sh
# Everything. The real-socket benches need the completion feature and a host with io_uring.
cargo bench -p ngnet-bench

# The four-arm real-socket comparison, on one pinned core so the numbers are comparable.
taskset -c 3 cargo bench -p ngnet-bench --bench transport_concurrent_throughput

# One at a time.
cargo bench -p ngnet-bench --bench serial_latency
cargo bench -p ngnet-bench --bench concurrent_throughput
cargo bench -p ngnet-bench --bench body_throughput

# Matched ngnet-H3/hyperium-H3 comparison over QMux: duplex then loopback TCP.
cargo bench -p ngnet-bench --bench qmux_h3_serial_latency
cargo bench -p ngnet-bench --bench qmux_h3_body_throughput
cargo bench -p ngnet-bench --bench qmux_h3_socket_serial_latency
cargo bench -p ngnet-bench --bench qmux_h3_socket_body_throughput

# The default timing probe contains no h3-ngnet-qmux diagnostic path.
cargo build -p ngnet-bench --example probe --release
taskset -c 3 target/release/examples/probe h3-qmux-duplex body 1048576 100 timing

# Focused adapter evidence is a separate feature-enabled, explicitly armed process.
cargo build -p ngnet-bench --example probe --release --features diagnostics
taskset -c 3 target/release/examples/probe h3-qmux-duplex body 1048576 1 diagnostic

# The opt-in no-copy path against the push path: duplex, then real sockets.
cargo bench -p ngnet-bench --bench shared_body
taskset -c 3 cargo bench -p ngnet-bench --bench transport_shared_body

# Compile and run each benchmark once without timing anything. This is what CI does, and
# it is the cheapest way to check a new machine can run every arm at all.
cargo bench -p ngnet-bench -- --test
```

The compio arms assert they obtained `DriverType::IoUring` and abort rather than publishing
numbers from anything else, so `--test` completing is also a statement that io_uring is
available to the user running it.

## Baselines

Comparing against a saved baseline is the point of running these at all — a single run's
absolute numbers say little (see [the noise caveat](interpreting.md#the-noise-caveat)), but a
before/after delta on the same machine says a great deal:

```sh
# Record a baseline before a change.
cargo bench -p ngnet-bench -- --save-baseline before

# After the change, compare against it. Criterion reports the delta and whether it clears
# the noise threshold.
cargo bench -p ngnet-bench -- --baseline before
```

Pin to one core and keep the machine otherwise idle, or the delta will be buried:

```sh
taskset -c 2 cargo bench -p ngnet-bench -- --baseline before
```

Build the benchmarks before timing them, so compilation never contends with measurement:

```sh
cargo build --benches -p ngnet-bench --release
```

## What a run has to do to be worth recording

These are not style preferences. Each one is a mistake this repository actually made and
corrected; the reasoning is in [`controls.md`](controls.md).

1. **Pin to one core** (`taskset -c N`) and leave the machine otherwise idle.
2. **Interleave the sides of an A/B**, baseline → branch → baseline → branch. Running both
   baseline repetitions and then both branch repetitions cannot separate an effect from drift,
   and on the legacy host it repeatedly did not.
3. **Carry unchanged arms as drift controls** and report their movement alongside the result.
   A difference smaller than the controls' own movement is not a result.
4. **Repeat.** Two repetitions per side is the minimum; the shared-body verdicts used ten.
5. **Record the exclusion rule before looking at the numbers**, if there is one, and report
   how many replicates it excluded and what the result is without it.

## Counting instead of timing

Some questions are not about how long something takes but about how many times it happens, and
those five rules are written for timings. A count of syscalls or of calls to a function is an
integer, reproducible run to run, and it does not compete with session drift — so rule 2
(interleave) and rule 3 (carry controls) have nothing to bite on. Rules 1, 4 and 5 still apply.

Two rules replace them:

- **Take every count at two iteration counts and subtract**, `(c(3N) - c(N)) / 2N`. Connection
  setup, warm-up and process teardown then cancel exactly rather than being amortised until they
  look small.
- **Check the instrument against the suite before trusting it.** A driver that does not reproduce
  Criterion's numbers for the same arm is measuring something else.

The instrument is `tests/ngnet-bench/examples/probe.rs`, which exists because Criterion's process
carries every arm at once and a profiler cannot be pointed at one of them. It establishes a single
fixture and runs a single workload in a loop:

```sh
cargo build --example probe -p ngnet-bench --release

# arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket |
#            ngnet-h3-quinn | ngnet-quic-h3 | h3-quinn
# workload = body <bytes> | concurrent <streams>
taskset -c 3 ./target/release/examples/probe qmux-socket body 1048576 300

# what it is for
strace -c -f  ./target/release/examples/probe qmux-socket body 1048576 300
perf record -F 4000 -g -- ./target/release/examples/probe qmux-duplex body 0 150000
```

It prints `PROBE-READY` on stderr once the connection is established and one explicit empty
exchange has warmed the persistent connection. Fixture-internal setup stays outside this
boundary as well. The explicit warm-up keeps a large-body failure after readiness, where the
supervisor can classify it. It is not a benchmark until calibrated as described below;
[`data/xeon-8370c-azure/09-qmux-h2-mechanisms.md`](data/xeon-8370c-azure/09-qmux-h2-mechanisms.md)
is what it produced and how far it was checked.

### Fixed-count ngtcp2 probe modes

The `ngnet-quic-h3` arm accepts body sizes `0`, `1024`, `16384`, and `1048576`. Its default
mode is an **unarmed timing run**:

```sh
cargo build -p ngnet-bench --example probe --release
taskset -c 3 ./target/release/examples/probe ngnet-quic-h3 body 16384 125 timing
```

Only `PROBE-METADATA`, `PROBE-READY`, `PROBE-TIMING`, and `PROBE-DONE` are emitted. Setup and
warm-up precede readiness; the measured interval contains no progress, diagnostic,
allocation-sampling, or RSS work. `PROBE-TIMING` reports two-direction application bytes and
elapsed nanoseconds for body workloads (concurrent workloads report rounds and streams
instead). Every timing arm performs equivalent response draining and byte counting; byte-exact
comparison is deliberately confined to diagnostic/correctness runs. A feature-enabled timing
binary still takes only the arming checks: it does not traverse offered ranges, retained
storage, attempts, or liveness state while unarmed.

Diagnostic mode is a separate feature-enabled process built from the same checkout:

```sh
cargo build -p ngnet-bench --example probe --release --features diagnostics
taskset -c 3 ./target/release/examples/probe ngnet-quic-h3 body 16384 125 diagnostic
```

The non-default `diagnostics` feature is additive and remains unarmed until after
`PROBE-READY`. Armed output is line-flushed only after a fully drained exact response. It
contains one record per transport offer plus exclusive per-exchange client/server intervals:
offered, prepared backing, accepted, retained, released, packet, timer, wake, retry, park,
queue, drop, terminal-discard, and overflow observations. Cumulative counters reset at each
exclusive drain. Live retained/queue gauges keep their current values and their next
high-water intervals start at those values. A field the safe transport cannot currently
distinguish, such as retransmission attribution, is printed as `unavailable`, never as a
guessed zero.

`application_body_bytes` is the body drained by one endpoint for that exchange.
`transport_stream_accepted` and `transport_stream_release_bytes` are QUIC stream bytes and
include HTTP/3 HEADERS/DATA framing and control-stream bytes. Diagnostics assert that accepted
transport stream bytes cover the application body and report the difference as
`framing_overhead_bytes`; accepted/release equality remains a separate transport-copy
reconciliation.
The probe rejects an attempt unless prepared backing is no greater than
`min(offered, sampled_payload_limit)`, rejects FIN on a truncated staged prefix, and checks
aggregate staged bytes against accepted progress plus one sampled limit for every
partial/zero-accept attempt.
RSS is read from `/proc/self/status` after readiness, immediately after each response drain
and before diagnostic formatting, and after the final diagnostic drain; non-Linux hosts print
`rss_kib=unavailable`. Failure paths make a best-effort RSS sample and exclusive diagnostic
drain before panicking.

Diagnostic mode gives each exchange a body- and build-scaled timeout: release builds allow
5 seconds plus 55 seconds per started MiB; debug builds allow 15 seconds plus 75 seconds per
started MiB. It reports the last completed exchange before timing out and checks every
response byte. Timing mode intentionally has no in-process timeout because polling a timeout
changes the measured scheduler path. **An outer supervisor is therefore required for timing
runs** and remains required for diagnostics because a native signal cannot be caught reliably
by Rust:

```sh
# Timing has no in-process timeout.
timeout --signal=TERM --kill-after=5s 900s \
  taskset -c 3 ./target/release/examples/probe \
  ngnet-quic-h3 body 1048576 125 timing
printf 'timing_probe_exit=%s\n' "$?"

# Diagnostic mode also keeps the outer boundary for native signals.
# 125 x 1 MiB: 60 seconds setup allowance plus 125 x 5 seconds.
timeout --signal=TERM --kill-after=5s 685s \
  taskset -c 3 ./target/release/examples/probe \
  ngnet-quic-h3 body 1048576 125 diagnostic
status=$?
printf 'probe_exit=%s\n' "$status"
```

Record exit `0`, `124` (outer timeout), or `128 + signal`, the final
`PROBE-PROGRESS`, and the final complete snapshot. Never exclude a stalled, signalled,
wrong-length, or unexpectedly dropped run from correctness results.

Before stability is claimed, predetermine and run five default-profile and five release-profile
125 × 1 MiB exact repetitions. Each process is externally bounded and every status is kept:

```sh
for profile in debug release; do
  for repetition in 1 2 3 4 5; do
    if [ "$profile" = release ]; then release=--release; outer=900; else release=; outer=1800; fi
    printf 'STABILITY profile=%s repetition=%s outer_timeout_s=%s\n' \
      "$profile" "$repetition" "$outer"
    timeout --signal=TERM --kill-after=5s "${outer}s" \
      cargo test -p ngnet-bench --test ngtcp2_fixture $release \
      ngtcp2_fixture_repeats_1_mib_exactly -- --ignored --exact --nocapture
    printf 'STABILITY profile=%s repetition=%s exit=%s\n' \
      "$profile" "$repetition" "$?"
  done
done
```

### Calibrating 1 KiB timing

Before treating fixed-count probe throughput as a phase guard, compare its 1 KiB
per-exchange time with Criterion in one pinned, otherwise idle session. Build once, then run
three interleaved pairs:

```sh
cargo build -p ngnet-bench \
  --bench quic_stack_body_throughput --example probe \
  --release --features diagnostics

for pass in 1 2 3; do
  printf 'CALIBRATION pass=%s instrument=criterion\n' "$pass"
  taskset -c 3 cargo bench -p ngnet-bench \
    --bench quic_stack_body_throughput --features diagnostics -- \
    'quic_stack_body_throughput/ngnet-quic-h3/1024'

  printf 'CALIBRATION pass=%s instrument=probe\n' "$pass"
  timeout --signal=TERM --kill-after=5s 300s \
    taskset -c 3 ./target/release/examples/probe \
    ngnet-quic-h3 body 1024 10000 timing
  printf 'CALIBRATION pass=%s probe_exit=%s\n' "$pass" "$?"
done
```

Divide each probe's `elapsed_ns` by 10,000. The calibration passes only when the median
probe value is within 5% of the median Criterion `time` estimate and neither three-run set
spans more than 5% of its own median. Otherwise the probe remains a correctness and
diagnostic instrument; its throughput must not gate a phase.

### Persistent memory envelope

Use fresh diagnostic processes for three 125-exchange, three 250-exchange, and three
500-exchange 1 MiB runs. For each process, take the post-warm-up `boundary=ready` sample and
the largest completed-exchange/final sample. The 125-run envelope is the largest post-warm-up
increase across the three fresh runs. Every longer run must remain within that envelope plus
the larger of 5% or 2 MiB. Preserve every complete RSS line and report
`rss_kib=unavailable` when the host cannot provide it. These are sampled `VmRSS` values, not
kernel `VmHWM` process peaks. The exact nine-process schedule is:

```sh
run=0
for count in 125 125 125 250 250 250 500 500 500; do
  run=$((run + 1))
  limit=$((60 + 5 * count))
  printf 'RSS run=%s count=%s outer_timeout_s=%s\n' "$run" "$count" "$limit"
  timeout --signal=TERM --kill-after=5s "${limit}s" \
    taskset -c 3 ./target/release/examples/probe \
    ngnet-quic-h3 body 1048576 "$count" diagnostic
  printf 'RSS run=%s count=%s probe_exit=%s\n' "$run" "$count" "$?"
done
```

### Validating the ngtcp2 HTTP/3 path

The live-loopback large-body repetitions are ignored while the intermittent outer-driver
liveness failure remains open; run them explicitly under the repetition/supervisor protocol
above. Then run the active diagnostic invariant/allocation coverage:

```sh
cargo test -p ngnet-bench --test ngtcp2_fixture --release
timeout --signal=TERM --kill-after=5s 900s \
  cargo test -p ngnet-bench --test ngtcp2_fixture --release \
  ngtcp2_fixture_repeats_16_kib_exactly -- --ignored --exact --nocapture
timeout --signal=TERM --kill-after=5s 900s \
  cargo test -p ngnet-bench --test ngtcp2_fixture --release \
  ngtcp2_fixture_repeats_1_mib_exactly -- --ignored --exact --nocapture
cargo test -p ngnet-bench --test ngtcp2_fixture --release --features diagnostics
cargo test -p ngnet-quic --all-features
cargo test -p ngnet-quic-h3-tests --test zero_alloc --release --all-features
```

Before merging a change to this path, also run both workspace feature modes, every clippy
target, the workspace benchmark smoke, and warning-denying documentation for the changed
public crates:

```sh
cargo test --workspace --all-features
cargo test --workspace
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo bench --workspace -- --test
RUSTDOCFLAGS="-D warnings" cargo doc \
  -p ngnet-quic -p ngnet-quic-h3 --all-features --no-deps
```

The crate-scoped documentation command is deliberate. The broader workspace command also
checks generated and private documentation outside this path; if it fails, record those
warnings separately rather than weakening or omitting the warning-denying check for the
changed QUIC crates.

## Recording a run

Criterion's own output under `target/criterion/` is not committed: it is per-machine, it is
large, and it is regenerated by the next run. What gets committed is a short markdown file
per run under [`data/`](data/), filed by machine.

```sh
cp docs/benchmarks/data/template/run.md \
   docs/benchmarks/data/<machine-id>/NN-<slug>.md
```

Fill in the header — machine, commit, command, repetitions, controls — then the tables, then
what the run does and does not establish. Add the row to the index in the machine's
`README.md`. [`data/README.md`](data/README.md) has the conventions in full, including how to
add a machine that has never been measured before.
