# Matched, and unmatched, configuration

Both stacks are pinned to libnghttp2's defaults, since `ngnet-h2`'s async layer advertises
only two settings of its own and leaves the rest at those defaults (`config.rs`,
`driver.rs`). hyper's builders default to much larger windows and header limits, so its
builders are dialled back to match. The flow-control windows matter most: a mismatched
initial window alone can move body throughput by 2x and say nothing about either
implementation.

This applies to `hyper-tokio` exactly as it does to the duplex `hyper` arm — both go through
the same builder helpers in `tests/ngnet-h2-bench/src/lib.rs`.

| Setting | Value both stacks use | How |
| --- | --- | --- |
| `INITIAL_WINDOW_SIZE` (stream) | 65535 | libnghttp2 default; hyper `initial_stream_window_size` |
| Connection window | 65535 | libnghttp2 default; hyper `initial_connection_window_size` + `adaptive_window(false)` |
| `MAX_FRAME_SIZE` | 16384 | libnghttp2 default; hyper `max_frame_size` |
| HPACK table size | 4096 | libnghttp2 default; hyper `header_table_size` |
| `MAX_CONCURRENT_STREAMS` | 128 | `Config` default; hyper `max_concurrent_streams` |
| `MAX_HEADER_LIST_SIZE` | 64 KiB | `Config` default; hyper `max_header_list_size` |
| Response `Date` header | none | hyper's `auto_date_header(false)`; this crate adds none |

Held still across all three socket arms besides the settings above: same request, same
headers, same echo handler, same draining, same number of spawned tasks, `TCP_NODELAY` on all
six endpoints, one worker thread per arm.

## What could not be matched

- **Outbound write batching.** hyper buffers outbound bytes and flushes in large writes, sized
  by `max_send_buf_size`; this crate has no such knob, and reaches the same end differently.
  Until the gathering path existed, the tokio adapter wrote each session block separately —
  zero-copy and zero-alloc but several syscalls per pass — and this was **the** unmatched
  setting that mattered more than everything matched, accounting for the whole
  `ngnet-h2-tokio`/`hyper-tokio` concurrency gap
  ([the finding](findings/write-path-and-gathering.md)). It is now largely matched in effect
  if not in mechanism: this crate emits one `writev` per pass where hyper emits one buffered
  `write`, and hyper still chains large payloads uncopied much as gathering does. The residual
  difference is a threshold (`VECTORED_THRESHOLD` = 256 here, `CHAIN_THRESHOLD` = 256 in `h2`
  when vectored). Note that `tokio::io::duplex` also reports `is_write_vectored() == true`, so
  the duplex family exercises the gathering path too — its `ngnet-h2` arm is not measuring the
  old per-block behaviour.
- **Optimistic stream opening.** hyper's `initial_max_send_streams` lets it open streams
  before the peer's `SETTINGS` arrives; this crate waits. This only affects the first round
  trip, so on a persistent connection it is noise.
