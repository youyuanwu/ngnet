# Matched, and unmatched, configuration

Two comparisons run in this suite, and they are matched separately because they are matched
against different things. The **HTTP/2 comparison** — `ngnet-h2` against hyper — holds one
protocol still and varies the implementation, so both ends can be pinned to the same
libnghttp2 numbers. The **cross-protocol comparison** — `ngnet-h2` against `ngnet-qmux-h3` —
varies the protocol itself, so "the same setting" has to be established before it can be
matched, and one of the settings can be reached from neither side at all. Both accountings are
below; a reader looking at a number needs the one belonging to the pair that number came from.

Everything on this page is set in one place, `tests/ngnet-bench/src/lib.rs`, from named
constants shared between the arms. Where a value is matched, the constant is written into both
sides rather than each side being given a literal of its own, so a change to a matched value
cannot reach one stack and miss the other.

## The Quinn HTTP/3 comparison

`quinn_serial_latency` and `quinn_body_throughput` compare `ngnet-h3-quinn` with upstream
`h3-quinn`. Both arms resolve to Quinn 0.11 and run on separate single-thread Tokio runtimes.
The shared fixture builder gives both the same loopback endpoint shape, self-signed certificate,
rustls configuration, `h3` ALPN, request headers, echo response, and full response drain. Each
connection is established and warmed before Criterion times it.

HTTP-layer defaults are not forced equal because the two implementations do not expose the same
settings: upstream `h3` uses stateless QPACK while `ngnet-h3` delegates QPACK to nghttp3. Those
differences are part of the implementations under comparison, not transport knobs the harness
can match without changing one stack's normal behavior.

## The matched QMux HTTP/3 comparison

The four `qmux_h3_*` targets hold QMux itself fixed while comparing `ngnet-h3` with hyperium
H3. Both ends use 65,535-byte stream and connection windows, 65,535-byte read-ahead, `2^40`
bidi and 16 uni lifetime allowances, a 128 pending/concurrent policy, the same request and
echo, separate current-thread Tokio runtimes, and one untimed warm-up.

Both field-section bounds are 64 KiB. Hyperium GREASE is disabled; ngnet exposes no matching
toggle. Hyperium 0.0.8 exposes no QPACK dynamic-table-capacity setting, so only the comparison
fixtures set ngnet's capacity to zero. The established `NgnetQmuxH3` fixtures retain their
existing 4 KiB configuration and behavior.

## The three-stack QUIC comparison

`quic_stack_serial_latency` and `quic_stack_body_throughput` retain both Quinn arms above and
add `ngnet-quic-h3`, backed by ngtcp2 and OpenSSL. All three use loopback UDP, current-thread
Tokio runtimes, the `h3` ALPN, generated certificate trust, persistent warmed connections, the
same request and echo response, and a full body drain.

The comparison intentionally does not claim to isolate one layer:

- `ngnet-h3-quinn` against `ngnet-quic-h3` holds `ngnet-h3` fixed but changes the QUIC
  implementation, TLS implementation, endpoint driver, and adapter.
- `ngnet-h3-quinn` against upstream `h3-quinn` holds Quinn and rustls fixed while changing the
  HTTP/3 implementation and adapter.
- `ngnet-quic-h3` against upstream `h3-quinn` changes the complete HTTP/3, QUIC, TLS, and
  adapter stack.

Each stack uses its production transport defaults. Matching them by changing flow-control,
stream-count, acknowledgement, pacing, or congestion settings would answer a different
configuration study rather than which default stack performs better today. The body case is
limited to 1 KiB because only that size has a calibrated, low-drift multi-arm performance
protocol. The native crash describes the historical pre-repair path, while later final review
still reproduced outer-driver stalls at 16 KiB/1 MiB. That timer-wake mechanism is now
corrected and covered by supervised exactness and diagnostic protocols. The long-running live
repetition tests remain ignored outside their supervisor, and larger Criterion points remain
excluded because this host has not passed the calibrated measurement gates; the Quinn-only
target retains both larger sizes.

## The HTTP/2 comparison: `ngnet-h2` against hyper

Both stacks are pinned to libnghttp2's defaults, since `ngnet-h2`'s async layer advertises
only two settings of its own and leaves the rest at those defaults (`config.rs`,
`driver.rs`). hyper's builders default to much larger windows and header limits, so its
builders are dialled back to match. The flow-control windows matter most: a mismatched
initial window alone can move body throughput by 2x and say nothing about either
implementation.

This applies to `hyper-tokio` exactly as it does to the duplex `hyper` arm — both go through
the same builder helpers in `tests/ngnet-bench/src/lib.rs`.

| Setting | Value both stacks use | How |
| --- | --- | --- |
| `INITIAL_WINDOW_SIZE` (stream) | 65535 | libnghttp2 default; hyper `initial_stream_window_size` |
| Connection window | 65535 | libnghttp2 default; hyper `initial_connection_window_size` + `adaptive_window(false)` |
| `MAX_FRAME_SIZE` | 16384 | libnghttp2 default; hyper `max_frame_size` |
| HPACK table size | 4096 | libnghttp2 default; hyper `header_table_size` |
| `MAX_CONCURRENT_STREAMS` | 128 | `Config` default; hyper `max_concurrent_streams` |
| `MAX_HEADER_LIST_SIZE` | 64 KiB | `Config` default; hyper `max_header_list_size` |
| Response `Date` header | none | hyper's `auto_date_header(false)`; this crate adds none |

Held still across all four socket arms besides the settings above: same request, same
headers, same echo handler, same draining, same number of spawned tasks, `TCP_NODELAY` on
both endpoints of every arm, one worker thread per arm.

### What could not be matched

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

## The cross-protocol comparison: `ngnet-h2` against `ngnet-qmux-h3`

The arms compared here are `ngnet-h2` against `ngnet-qmux-h3` on the duplex, and
`ngnet-h2-tokio` against `ngnet-qmux-h3-tokio` on a real socket. hyper is not part of this
comparison and is matched by the section above; an `ngnet-qmux-h3` figure read against a hyper
figure varies protocol *and* implementation and is attributable to neither.

**The comparison set is enumerated, and the enumeration is the undertaking.** Five settings are
accounted for below, each under exactly one of three headings: matched at a stated value; fixed
on one side and met by the other; or reachable from neither, in which case both effective
values are given together with the direction their difference biases the result. A setting
outside the five is outside what this page undertakes — a real limit, stated so that it is not
mistaken for a claim that nothing else differs. The two protocols differ in header
representation, in framing, and in whether there is a stream-multiplexing transport under the
HTTP layer at all, and none of that is a *setting*: it is what the comparison is of.
[`controls.md`](controls.md) carries those as confounds, each with its direction.

### Matched at a stated value

Both stacks expose the setting, and both are set to the same number from the same constant.

| Setting | Value | On the HTTP/2 side | On the QMux side |
| --- | --- | --- | --- |
| The concurrent-stream allowance | **128** | `ngnet_h2::http::Config::max_concurrent_streams`, advertised as `SETTINGS_MAX_CONCURRENT_STREAMS` | `ngnet_h3::http::Config::max_concurrent_streams` |
| The bound on a header block | **64 KiB** | `ngnet_h2::http::Config::max_header_list_size`, advertised as `SETTINGS_MAX_HEADER_LIST_SIZE` | `ngnet_h3::http::Config::max_field_section_size` |

The second row is one quantity under two names: HTTP/2 bounds the header *list*, HTTP/3 the
*field section*, and both mean "the largest set of headers this endpoint will accept for one
message". Matching them by identifier would have been impossible; matching them by the quantity
they bound is why the comparison set is enumerated in terms of quantities rather than names.

Both are **set rather than inherited**, and that distinction is load-bearing. `ngnet-h3`'s
defaults already equal `ngnet-h2`'s — deliberately, so that a reader moving between the two
crates does not have to relearn the numbers — so leaving both at their defaults would have
produced the same figures today. It would also have produced a benchmark whose fairness rested
on two crates' defaults staying equal, which is one upstream edit away from silently comparing
unlike things, with nothing failing when it happened. The harness therefore writes each value
from the single constant the HTTP/2 arm reads, through the configuration-taking entry points
`ngnet_qmux_h3::connect_with` and `serve_with`. Adding those entry points is the only change
this work made to a shipped crate, and reaching these settings is part of why it was needed.

The first row is also where the concurrency sweeps' upper point comes from. `N` = 64 sits below
128 on both stacks by construction, and the fixtures refuse a concurrency above it rather than
offering it — see the last section on this page.

### Fixed on one side, met by the other

Neither of these can be set from `ngnet-h2`'s configuration surface: libnghttp2 fixes them and
the async layer exposes no knob, whatever identifiers exist further down in the raw bindings.
The QMux side does expose them, so the QMux side is the one that moves.

| Setting | Value both ends use | Fixed at it by | Set to it on |
| --- | --- | --- | --- |
| Flow-control credit, per stream | **65535** | libnghttp2's `SETTINGS_INITIAL_WINDOW_SIZE` default | QMux `initial_max_stream_data` (whose own default is 256 KiB) |
| Flow-control credit, across the connection | **65535** | libnghttp2's connection window, which it starts from and does not grow | QMux `initial_max_data` (whose own default is 1 MiB) |
| The header-compression state each end keeps | **4096 bytes** | libnghttp2's HPACK dynamic table default | `ngnet_h3::http::Config::qpack_max_dtable_capacity` |

**The credit is the member the whole comparison most depends on.** An arm given sixteen times
the credit of the arm beside it is measuring a window rather than an implementation, and the
difference would be of the same order as the difference being looked for — the section above
puts a mismatched initial window alone at 2× on body throughput, which is why the HTTP/2
comparison dials hyper's windows back rather than leaving them at hyper's own defaults. Left at
their defaults, the QMux arm would have carried 256 KiB per stream against 65535 and 1 MiB
across the connection against 65535 — so at the 1 MiB point of the body sweeps the HTTP/2 arm
would have paid repeated `WINDOW_UPDATE` round trips and the QMux arm would have paid none at
all. That is not a subtle bias; it is the sweep's headline number.

The matching is one-directional, and could not have been the other way round. libnghttp2's
window is unreachable, so the question of which side to move did not arise — but even had it
been reachable, moving the HTTP/2 side would have changed arms whose measurements are already
recorded under [`data/`](data/), and comparability with those runs is the reason the existing
arms were left untouched at all. Had *neither* side exposed the setting, nothing could have been
done but state the difference, which is exactly what happens to the record size below.

**Two caveats on the connection-level row**, both real, neither large:

- **Equal in number, not quite equal in meaning.** HTTP/3's three unidirectional streams —
  control, QPACK encoder, QPACK decoder — are ordinary QMux streams and spend connection
  credit, where HTTP/2's control frames sit outside flow control entirely. The QMux arm
  therefore has marginally less of its 65535 available to bodies than the HTTP/2 arm has of
  its own. It is a few hundred bytes over a connection's whole life, against a window that is
  extended per consumed byte, so it biases nothing measurable — but it is an asymmetry rather
  than an exact match, and [`controls.md`](controls.md) carries it with its direction.
- **The layer's read-ahead is set to the same number and is not this setting.** QMux's
  `read_ahead` bounds what the transport will hold for the HTTP/3 layer before that layer
  reports consuming any. It is local, is never advertised, and is therefore a harness parameter
  rather than a protocol setting; it appears in the harness table below for that reason.

The QPACK row matches a capacity, not a compression outcome. HPACK and QPACK compress
differently at the same table size, and the QMux arm additionally pays for QPACK's encoder and
decoder streams. Setting the capacities equal removes the one difference that is a *setting*;
it does not make the two stacks compress alike, and no configuration could.

### Reachable from neither, stated with its bias

| Setting | HTTP/2 arms | QMux arms | Why neither is reachable |
| --- | --- | --- | --- |
| The largest unit either protocol puts on the wire in one piece | `MAX_FRAME_SIZE` = **16384** payload bytes, plus a 9-byte frame header — 16393 on the wire | `max_record_size` = **16382** bytes for the *whole record*, framing included | libnghttp2 fixes the first and `ngnet-h2`'s `Config` does not carry it. dwnx overwrites any configured `max_record_size` with `DWNX_DEFAULT_MAX_RECORD_SIZE` immediately after copying the parameters in, with the upstream comment "We do not let application increase max record size" — see [`../qmux/design.md`](../qmux/design.md) and `crates/ngnet-qmux-sys/vendor/dwnx/lib/includes/dwnx/dwnx.h:94` |

The two numbers are two bytes apart and the difference between them is larger than that,
because they bound different things: 16384 is an HTTP/2 DATA frame's *payload*, while 16382
bounds a QMux record *including* its record and frame headers. Payload per unit is therefore
strictly smaller on the QMux side, by more than the headline arithmetic suggests.

**Direction of bias: against the QMux arms, and small.** More units per body means more framing
work per byte and more producer passes through the pump; at 1 MiB it also means one more unit
than the 64 an HTTP/2 arm needs, since 1 MiB divides exactly by 16384 and does not divide by
16382. That is a fraction of a percent of the work in a body sweep — at or below the ~1% drift
bar of the machine these arms are measured on
([`data/xeon-8370c-azure/`](data/xeon-8370c-azure/)), and the wrong order of magnitude to
explain any gap observed so far. It is recorded because a difference that is small is not a
difference that is absent, and because a later reader hunting the mechanism behind a 1–2%
body-throughput gap should find this before inventing one.

**Its standing has risen since, without its size changing.** More units used to mean more
*writes* as well as more framing work, because the QMux write path issued one write per record;
that is gone. The whole set of six changes it belongs to was worth −30% at 1 MiB and −25.9% at
64 KiB over a socket; how much of that is this one has not been measured on any socket arm
([`findings/qmux-write-path.md`](findings/qmux-write-path.md)). So a per-byte mechanism that
was previously one of two, and much the smaller, is now the only one of its kind left on the
QMux side. It is still a fraction of a percent and still the wrong size to explain a visible
gap; what has changed is that a reader who does find a per-byte effect in a body sweep has
fewer places to look, and this is the first of them.

Removing it is not available at a price a benchmark may pay. Raising the QMux record size needs
an upstream change to dwnx. Lowering HTTP/2's `MAX_FRAME_SIZE` to 16382 would alter the HTTP/2
arms, invalidating every measurement already recorded under [`data/`](data/) in order to remove
an effect smaller than the noise. Stating it is the whole of the available response, which is
what this heading exists for.

### Every layer that could bind before a compared value does

The concurrent-stream allowance above is matched at 128, but 128 is only the value being
compared if nothing else runs out first. Four limits sit at or under it across the two stacks,
and one of them is not even the same *kind* of quantity — which is exactly why it is the one
that can bite.

| Layer | Setting | Configured value | Why it cannot bind first |
| --- | --- | --- | --- |
| HTTP/2 — `ngnet-h2` | `SETTINGS_MAX_CONCURRENT_STREAMS` | **128** | It *is* the compared value on this stack, and nothing sits beneath it: an HTTP/2 stream needs no transport-level permission, because the connection is the transport. |
| HTTP/3 — `ngnet-h3` | `max_concurrent_streams` | **128** | Equal to the compared value, so it binds exactly when that does and never before. It is local rather than advertised — on a server it bounds how many handler futures the driver holds at once — so it constrains this endpoint rather than the peer. |
| QMux transport — `ngnet-qmux` | `max_streams_bidi` | **2^40** (≈ 1.1 × 10¹²) | About ten orders of magnitude above 128. It is not a concurrency limit at all, so the question is not whether it exceeds 128 but whether a whole run can exhaust it. A connection is established once per benchmark id and reused for every iteration of every sample, so the streams it will ever carry are (warm-up + measurement time) ÷ per-iteration time × `N` — a few hundred thousand at the empty-body points, which are the fastest per iteration and therefore the heaviest consumers. That is roughly 10⁵ against a budget of roughly 10¹²: seven orders of magnitude of margin, which is the point of choosing a number this far from anything a run approaches. |
| QMux transport — `ngnet-qmux` | `max_streams_uni` | **16** | HTTP/3 opens exactly three unidirectional streams per end, once, for the connection's life. Sixteen is those three with room for a peer that opens more; nothing consumes this repeatedly. |

**`max_streams_bidi` is a cumulative budget rather than a concurrency limit, and that is the
whole reason it is on this table.** QMux stream capacity is spent permanently: nothing recycles
it when a stream closes — not dwnx, not `ngnet-qmux` — and `ngnet-qmux-h3` never calls
`extend_stream_limit`. The number is therefore how many requests a connection will *ever*
carry, not how many it will carry at once. A reader who saw "128 concurrent, 100 permitted by
the transport" would conclude the transport was merely close; in fact the transport's default
of 100 is exhausted inside the first Criterion sample of the first parameter value, because
these benches establish one connection and reuse it for every iteration of every sample of
every arm.

What earns it a table row rather than a footnote is *how* it fails. An exhausted allowance does
not error: the next open waits for capacity that will never arrive, neither end reports
anything, and no timeout surrounds a Criterion measurement. The suite would stop partway
through and never return — which is worse than failing, because a failure names itself. The
defect is recorded rather than fixed, on
[`../qmux-h3/pending-work.md`](../qmux-h3/pending-work.md); 2^40 is the harness moving it out
of the comparison's way.

**2^40 is bounded above as well as below**, and a later reader "tidying" it in either direction
breaks the suite differently. dwnx's own ceiling is `DWNX_MAX_STREAMS` = `1 << 60`
(`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_transport_params.h:63`), enforced where the
*peer* decodes the transport
parameters — so a value at 2^61 is accepted where it is configured and then fails the
connection during setup, with an error that names nothing about streams.
`TransportParams::validate`'s varint check does not catch that: it bounds the encoding, not
dwnx's limit. 2^40 sits about six orders of magnitude above anything a run consumes and about
six below the ceiling, which is the widest margin available on both sides at once.

### Harness parameters, which are not protocol settings

Equal across the arms of a comparison, or unequal and stated. None of these is advertised to a
peer, so none is part of the comparison set — but a difference in one of them would confound a
result just as effectively, which is why they are enumerated too.

| Parameter | Value | Equal across the arms? |
| --- | --- | --- |
| Runtime | one worker thread per arm — `current_thread` on tokio, single-threaded compio | Yes, but by different means in each family, and the difference is worth knowing. The socket family gives **each arm its own runtime**, so no arm's idle connection driver sits registered in another's scheduler. The duplex family runs **all of an arm-set's connections on one shared `current_thread` runtime**, which is equally fair for a different reason: there is one worker thread either way, and an idle driver there is parked on a duplex read with no timer to fire — `ngnet-qmux`'s clock exposes only `now()` and arms nothing, and `max_idle_timeout` is left at zero — so a driver that is not being measured is never polled. |
| Task arrangement | drivers on plain `tokio::spawn`; concurrent requests through a `JoinSet` | Yes, deliberately: the QMux fixtures mirror the HTTP/2 fixtures down to the spawn, so both arms pay the same harness overhead. |
| `TCP_NODELAY` (socket family) | set on both endpoints of every arm | Yes — the QMux socket fixture takes the same socket-pair helper the HTTP/2 socket arms take, so it is set by the same code rather than by a second copy of it. |
| Duplex capacity (duplex family) | 1 MiB | Yes. Large enough that the pipe is not the bottleneck; the flow-control window is. |
| Request, headers, body bytes, echo handler, drain | one shared definition | Yes, structurally rather than by assertion: the QMux fixtures call the same request builder, body builder, collector and drain as the HTTP/2 fixtures. The one thing restated is the echo handler's *signature*, because the two stacks hand a handler unrelated `IncomingBody` types of the same name; its body is one call to each shared helper. |
| QMux `read_ahead` | 65535 | QMux-only; HTTP/2 has no counterpart. Stated rather than left at its 1 MiB default because it is a harness parameter, and pinned to the connection window because it must **not** sit below it: below it the layer declines to read bytes the peer has already been told it may send, and a body needing more than one instalment stalls with nothing reported. |
| QMux `max_idle_timeout` | left at its default of zero, meaning none | QMux-only, and deliberately unset. Nothing in QMux enforces an idle timeout in either direction ([`../qmux/pending-work.md`](../qmux/pending-work.md)), so advertising a deadline nobody keeps would state a fiction rather than match anything. HTTP/2 has no counterpart to match it against. |
| Warm-up before timing | one complete exchange on the QMux arms; none on the HTTP/2 arms | **No** — and necessarily not. See [`controls.md`](controls.md): the asymmetry exists to make the arms comparable rather than in spite of making them incomparable. |

### A parameter one stack will not admit is refused, not offered

Both QMux fixtures check a requested concurrency against what they configured, before anything
reaches the wire, and panic with a legible message if it does not fit. This is not defensive
tidiness. The characteristic failure on this stack is an exchange that neither
completes nor fails, and since nothing wraps a Criterion measurement in a timeout, a parameter
that gets as far as being offered cannot be recovered from — a panic during the iteration's
setup is the only recovery available.

Concurrency is the case that motivates it: the HTTP/3 server enforces its limit by *resetting*
an exchange that arrives while that many handlers are already running, rather than by queueing
it, so an over-limit sweep would report times for some iterations and fail partway through
others.

**A body is not checked at runtime at all, and that is the honest position rather than an
omission.** No configured value bounds a body: credit is extended per consumed byte at both the
stream and the connection level, so a body larger than the window arrives in window-sized
instalments rather than being refused, and there is consequently no body size for a fixture to
reject. The one thing a multi-instalment body does depend on — that the read-ahead is not below
the connection window, since beneath it the layer stops reading bytes it has already granted and
the transfer stalls silently — is a relation between two constants and nothing to do with the
body's length. It is asserted as a `const` in the bench library, so lowering the read-ahead
fails the *build* rather than waiting to be caught by a run on some machine. A per-body runtime
check would have looked more thorough while being a test whose answer never depended on its
argument.
