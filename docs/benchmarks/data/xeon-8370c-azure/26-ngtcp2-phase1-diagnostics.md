# 26 — What does the ngtcp2 diagnostic probe observe before the body fix?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; CPU 3
**Date:** 2026-08-30
**Source:** historical `feature/ngtcp2-h3-performance_phase1` working tree after dangling
commit `fcab0aa`; identical tree is durably represented by reachable commit `8515702`
**Purpose:** correctness and mechanism diagnosis, not a timing comparison
**Exclusions:** no failing system-under-test result was excluded

## Capacity-wake correctness

The detached producer previously had no wake tied specifically to a full outbound queue
regaining capacity. The Phase 1 seam fills the 64-datagram queue with inbound and timers
quiesced, registers the same producer twice, removes two datagrams, and observes exactly one
wake on the full-to-available transition and none on the second removal. The repair's reachable equivalent is
`8515702`; the deterministic test is
`removing_from_a_full_outbound_queue_wakes_one_capacity_retry` in
`crates/ngnet-quic/src/endpoint/shared.rs`.

## Persistent-body results

The new exact-response fixture produced these outcomes:

| Build | Workload | Result |
| --- | --- | --- |
| debug + diagnostics | 125 × 16 KiB | completed exactly |
| debug + diagnostics | 125 × 1 MiB | first exchange completed with incorrect content |
| release | 125 × 1 MiB | process terminated with `SIGSEGV` |
| release + diagnostics probe | 1 × 1 MiB | process terminated with status 139 after `PROBE-READY`, before one exchange completed |

The release diagnostic command was:

```sh
cargo build -p ngnet-bench --example probe --release --features diagnostics
taskset -c 3 ./target/release/examples/probe \
  ngnet-quic-h3 body 1048576 1 diagnostic
```

Its last complete records were:

```text
PROBE-METADATA arm=ngnet-quic-h3 workload=body param=1048576 count=1 \
warmup=1-explicit mode=diagnostic os=linux arch=x86_64 build=release
PROBE-READY
PROBE-RSS boundary=ready exchange=0 rss_kib=13972
```

The process then received `SIGSEGV`; there was no completed-exchange diagnostic snapshot to
interpret.

## Whole-remainder preparation

A release diagnostic run of three exact 16 KiB exchanges completed. Its final cumulative
snapshots reported:

| Role | Offered / prepared | Accepted / released | Prepared ÷ accepted | Zero accepts | Zero-accept retries without a later enabling event |
| --- | ---: | ---: | ---: | ---: | ---: |
| client | 843,675 B | 49,242 B | 17.13× | 66 | 43 |
| server | 372,997 B | 49,188 B | 7.58× | 10 | 9 |

For both roles, prepared backing capacity equaled offered bytes. Accepted bytes equaled the
adapter's release-event bytes, packet totals reconciled into transport-only plus
stream-carrying packets, and no inbound queue drop was observed. The client retained-backing
high-water mark was 57,175 bytes; the server's was 103,576 bytes.

This is direct evidence that the borrowing path repeatedly prepares substantially more
backing storage than the body progress it accepts. It also records repeated zero-progress
offers in one run without a later enabling event. The run does not establish that either
quantity alone is the native crash site.

## What this establishes

- The missing detached outbound-capacity wake was a deterministic liveness defect and has a
  scoped repair.
- The repeated 1 MiB workload remains a correctness failure in both diagnostic and ordinary
  execution; it is not a throughput point.
- The current borrowing path exhibits whole-remainder preparation amplification at 16 KiB,
  while accepted and release-byte accounting still reconciles.
- Packet-bounded borrowing staging is eligible for the next independently reviewed
  correctness phase.

## What it does not

- It does not provide a before/after latency or throughput result.
- It does not localize the native instruction that faults.
- It does not make detached buffer recycling, packet-order changes, Quinn task changes,
  scratch reuse, timer reuse, syscall batching, or crypto changes eligible.
