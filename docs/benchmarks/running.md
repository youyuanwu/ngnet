# Running the benchmarks

## What has to be in place first

The benchmark crate builds three C libraries from source, and the set grew when the
HTTP/3-over-QMux arms landed. A checkout that used to be sufficient no longer is, so this
section is the first thing to check when `cargo bench -p ngnet-bench` fails before it runs
anything.

| Submodule | Reached through | Needed by |
| --- | --- | --- |
| `crates/ngnet-h2-sys/vendor/nghttp2` | `ngnet-h2-sys` | every HTTP/2 arm |
| `crates/ngnet-h3-sys/vendor/nghttp3` **and its nested `lib/sfparse`** | `ngnet-h3-sys` | every QMux arm |
| `crates/ngnet-qmux-sys/vendor/dwnx` | `ngnet-qmux-sys` | every QMux arm |

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
git submodule update --init crates/ngnet-h2-sys/vendor/nghttp2 crates/ngnet-h3-sys/vendor/nghttp3 crates/ngnet-qmux-sys/vendor/dwnx
git -C crates/ngnet-h3-sys/vendor/nghttp3 submodule update --init lib/sfparse

# The equivalent of `just submodules-status`:
git submodule status --recursive
```

Building those three needs a C compiler, CMake 3.14 or newer, and libclang for `bindgen`.

**What is still not needed is OpenSSL 3.5 or newer.** That requirement belongs to
`ngnet-quic-sys`, which builds ngtcp2 and drives its handshake through OpenSSL's QUIC TLS API,
and no arm in this suite reaches it — `crates/ngnet-quic-sys/vendor/ngtcp2` need not be
checked out to run the
benchmarks at all. This is worth stating rather than leaving implicit, because the naive
reading of "the benchmarks now cover HTTP/3" is that they must have acquired a QUIC transport
and therefore a TLS stack with an unusually new OpenSSL floor. They have not: QMux runs over
the same loopback TCP connection the HTTP/2 arms use, and the QMux arms are unencrypted. On a
host whose OpenSSL predates 3.5, `cargo bench -p ngnet-bench` works and
`cargo bench --workspace` does not; prefer the `-p` form for that reason alone.

`cargo tree -p ngnet-bench --prefix none | grep '^ngnet-.*-sys' | sort -u` is the check that
settles the question on any given day, rather than trusting this table. It lists exactly the
three `-sys` crates above, and `ngnet-quic-sys` is not among them.

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

# arm      = h2-duplex | h2-socket | qmux-duplex | qmux-socket
# workload = body <bytes> | concurrent <streams>
taskset -c 3 ./target/release/examples/probe qmux-socket body 1048576 300

# what it is for
strace -c -f  ./target/release/examples/probe qmux-socket body 1048576 300
perf record -F 4000 -g -- ./target/release/examples/probe qmux-duplex body 0 150000
```

It prints `PROBE-READY` on stderr once the connection is established and warmed, so a trace can be
started against a steady state. It is not a benchmark and its wall-clock output is not a result;
[`data/xeon-8370c-azure/09-qmux-h2-mechanisms.md`](data/xeon-8370c-azure/09-qmux-h2-mechanisms.md)
is what it produced and how far it was checked.

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
