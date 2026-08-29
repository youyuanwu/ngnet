# 22 — How do QMux/H3 and HTTP/2 compare at 8 MiB?

**Machine:** historical [`xeon-8370c-azure`](README.md) label; Intel Xeon Platinum 8573C
**Date:** 2026-08-29
**Commit(s):** `cae8330`
**Cases:** the new 8 MiB arms in `body_throughput` and
`transport_body_throughput`, comparing `ngnet-h2` with `ngnet-qmux-h3` over a duplex and
`ngnet-h2-tokio` with `ngnet-qmux-h3-tokio` over a loopback socket
**Command:** after `cargo build --benches -p ngnet-bench --release`,
five passes of `taskset -c 3 cargo bench --quiet -p ngnet-bench
--bench body_throughput --bench transport_body_throughput --
8388608 --sample-size 100 --measurement-time 3 --warm-up-time 1
--save-baseline 8mib-<pass> --noplot`
**Repetitions:** five passes. Each H2/QMux pair is registered adjacently and the ratio is
formed within each pass
**Controls:** neither compared arm is unchanged. Within-pass ratios cancel session drift;
all five per-pass ratios and their full range are reported
**Exclusions:** none. Every arm completed and no pass or sample was discarded

## What was being asked

Run 21 stopped at 1 MiB, where QMux/H3 was 1.222× HTTP/2 over a duplex but 0.845× over a
socket. This run extends the same persistent-connection echo workload to 8 MiB to ask whether
the fixed-cost crossover continues, reverses, or approaches a stable marginal ratio.

## Results

Criterion median per exchange. The server echoes, so an iteration transfers 8 MiB in each
direction; Criterion throughput remains normalized to one 8 MiB body. Lower latency and ratio
are better.

| substrate | pass | HTTP/2 | QMux/H3 | QMux/H3 ÷ H2 |
| --- | ---: | ---: | ---: | ---: |
| duplex | 1 | 3.968 ms | 4.665 ms | 1.176× |
| duplex | 2 | 4.121 ms | 4.752 ms | 1.153× |
| duplex | 3 | 3.992 ms | 4.728 ms | 1.184× |
| duplex | 4 | 4.017 ms | 4.814 ms | 1.199× |
| duplex | 5 | 4.071 ms | 4.888 ms | 1.201× |
| **duplex aggregate** | | **4.034 ms** | **4.770 ms** | **1.182×** |
| socket | 1 | 10.125 ms | 9.486 ms | 0.937× |
| socket | 2 | 10.278 ms | 9.311 ms | 0.906× |
| socket | 3 | 10.104 ms | 9.129 ms | 0.904× |
| socket | 4 | 10.165 ms | 9.330 ms | 0.918× |
| socket | 5 | 10.404 ms | 9.659 ms | 0.929× |
| **socket aggregate** | | **10.215 ms** | **9.383 ms** | **0.919×** |

The aggregate throughputs are 1,983 MiB/s for H2 and 1,677 MiB/s for QMux/H3 over the
duplex, and 783 MiB/s against 853 MiB/s over the socket.

## Exact socket syscall counts

Two-point `strace -c -f` counts at 10 and 30 exchanges remove process setup:

| arm | writes per exchange | reads per exchange |
| --- | ---: | ---: |
| HTTP/2 | 1,410 `writev` | 1,536 `recvfrom` |
| QMux/H3 | **515 `sendto`** | 1,537 `recvfrom` |

The socket result is still a write-count result: both stacks read the same number of times,
while QMux/H3 writes 2.74× less often.

## What this establishes

- Over a duplex, QMux/H3 remains slower at 8 MiB: **1.182×**, with every pass between
  1.153× and 1.201×.
- Over a socket, QMux/H3 remains faster: **0.919×**, an 8.1% latency advantage, with every
  pass between 0.904× and 0.937×.
- The socket advantage does not monotonically widen with body size. Run 21 measured 0.845×
  at 1 MiB, while this run measures 0.919× at 8 MiB. That comparison is descriptive across
  sessions, not a code-effect estimate.
- QMux/H3's lower socket write count remains the mechanism: 515 writes against 1,410 with
  effectively identical read counts.

## What it does not

- It does not identify why the socket ratio is closer to one at 8 MiB than run 21's 1 MiB
  ratio. A same-session 1/8 MiB profile would be needed to attribute that curvature.
- It is loopback, tokio, a current-thread runtime, and one persistent connection. It says
  nothing about a real network, packet loss, TLS record behavior, or QUIC.
- The historical 1 MiB comparison and this 8 MiB comparison use the same reported CPU model
  but different sessions. Their absolute times are not a controlled A/B.
