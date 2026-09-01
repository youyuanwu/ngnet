# Hyperium H3 over QMux: invariants

| Invariant | Evidence |
| --- | --- |
| One lower read batch and at most 64 routed events per adapter turn | `ngnet-qmux/tests/io_bounded.rs`; private `state` unit tests |
| Only the stable proxy waker reaches QMux; user wakes occur after unlock | `h3-ngnet-qmux/tests/scheduling.rs`; `state` unit tests |
| Independent opener, writer, receive, and finish waiters are not displaced | `tests/scheduling.rs`; private `state` unit tests |
| Idle and credit-blocked operations do not form a self-wake loop | `tests/scheduling.rs` |
| Explicit and data-first peer streams are accepted exactly once | private `state` unit tests; `tests/e2e.rs` |
| Every current event variant has a stable route; unknown variants fail closed | `state` unit tests |
| `WriteBuf<Bytes>` header/payload and borrowed generic unframed buffers advance exactly | `tests/send_buffers.rs` |
| Delivery credits stream and connection exactly once; discard credits connection only | private `state` unit tests |
| Finish/reset/stop/drop and repeated terminal polls are idempotent | `tests/lifecycle.rs` |
| Late data cannot resurrect a stream before ordered lower retirement | private `state` unit tests |
| Application and stream codes survive end to end; unrelated siblings complete | `tests/e2e.rs`; `tests/lifecycle.rs` |
| Non-`Send` memory construction and Tokio socket exchanges both work | `tests/traits.rs`; `tests/e2e.rs` |
| Bodies larger than both flow windows survive fragmentation and bounded capacity | `tests/e2e.rs` |
| Both benchmark arms use symmetric per-instance lower-I/O and endpoint-poll counters | `ngnet-bench/tests/fixtures_move_their_bytes.rs` |

The integration suite uses timeouts only to convert a hang into a named failure;
no sleep establishes ordering or readiness.
