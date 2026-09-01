# Hyperium H3 over QMux: pending work

- **No external QMux interoperability target exists.** The adapter is tested
  against this workspace's two QMux endpoints, not another implementation.
- **Endpoint and security policy remain outside the crate.** QMux can run over
  TCP, a Unix socket, a pipe, or a TLS session; this adapter intentionally
  chooses none.
- **Stream allowances are cumulative.** QMux does not recycle completed stream
  IDs. Long-lived callers must advertise a lifetime budget or extend it.
- **Header and payload chunks are submitted sequentially.** Coalescing them
  would require copying or additional lower support. Run 31 found no controlled
  causal timing evidence that justifies that change.
- **Production diagnostics are intentionally absent.** Benchmark-only lower-I/O
  and endpoint-poll counters are per fixture and wrap both compared adapters
  symmetrically; they do not attribute work inside either H3 implementation.
- **Timeouts remain caller policy.** QMux provides a clock reading but no timer,
  so the adapter does not manufacture timeout errors.
- **The benchmark verdict is noisy.** Duplex and socket signs differ and pinned
  repetitions do not clear within-session spread. Re-measure on controlled
  hardware before making performance claims.
