# Handing bodies over

**Measurements:** [`legacy-dev-host/04-shared-body`](../data/legacy-dev-host/04-shared-body.md),
ten replicates · [`xeon-8370c-azure/03-shared-body`](../data/xeon-8370c-azure/03-shared-body.md),
five replicates on a quiet host, which **overturned the completion transport's verdict**.

A connection built through the opt-in `handshake_shared`/`serve_shared` entry points hands its
bodies to libnghttp2 as the caller's own `Bytes` rather than copying them into the frame
buffer, and those frames serialise with `NGHTTP2_DATA_FLAG_NO_COPY`. Two benchmark families
measure it against the unchanged push path:
[`transport_shared_body`](../cases/transport-shared-body.md) on real sockets, and
[`shared_body`](../cases/shared-body.md) over a duplex as the mechanism check.

## The result

Negative is faster, and both hosts agree on sign and ordering while disagreeing on magnitude.

| | legacy-dev-host, 7 clean replicates | xeon-8370c-azure, 5 replicates |
| --- | --- | --- |
| tokio, 0 B (control) | +1.0% | −0.33% |
| tokio, 1 KiB | **−35.3%** | **−29.24%** |
| tokio, 64 KiB | **−25.4%** | **−22.83%** |
| tokio, 1 MiB | **−30.6%** | **−24.33%** |
| compio, 0 B (control) | −0.2% | +1.23% |
| compio, 1 KiB | −0.9% | **+1.99%** |
| compio, 64 KiB | −3.3% | −2.26% |
| compio, 1 MiB | **−4.07%** | **−4.55%** |
| Worst control movement | 34.94% (`compio-push`) | 3.19% (`compio-push`) |

The duplex family agrees independently on both hosts: −9.2%, −9.7%, **−14.4%** on the legacy
host, and −7.07%, −8.55%, −8.23% here. The legacy duplex 1 MiB figure lands almost exactly on
the gate's pre-registered ceiling of 14.98% of protocol CPU for that workload — a prediction
made before the code existed, reproduced within half a point.

Tables, controls and per-replicate spreads are in
[the legacy run](../data/legacy-dev-host/04-shared-body.md) and
[the current one](../data/xeon-8370c-azure/03-shared-body.md).

## The dominant mechanism is write-count collapse, not copy removal

The readiness gains are five times larger than the copy alone could explain, and that had to be
accounted for rather than banked. Measured write counts for one upload, pinned by
`http_shared_body.rs::handing_a_body_over_collapses_the_write_count_on_the_gathering_path`:
0 B 1→1, 1 KiB 2→1, 64 KiB 5→2, 1 MiB 65→17.

On the push path libnghttp2 returns one serialised block per `mem_send2` call — a DATA header
joined to its 16 KiB payload — so a large upload is one write per frame; handing the body over
turns each frame into two regions the driver gathers into a single write. **The gain tracks
that ratio and vanishes exactly at 0 B, where the ratio is 1.**

What bounds the batch at 1 MiB is the 64 KiB initial flow-control window, which admits about
four 16 KiB frames per pass — **not** `MAX_REGIONS`, which is a guard rail here rather than the
binding constraint.

## SC-005 verdict: MET on both transports — but it took two machines to say so

**On the readiness transport the verdict was never in doubt**, and both hosts agree.
**On the completion transport the first verdict was NOT MET, and it was wrong about the
transport rather than about the arithmetic**: it failed on a control arm that was misbehaving,
not on its own delta, and a quiet machine settled it the other way. Both readings are kept
below, in the order they were reached.

### First: NOT MET on the completion transport, on the legacy host

The criterion requires the difference to exceed the movement of the drift controls in the same
session. It does not say *which* controls, and the two readings disagreed, so the choice was
made on evidence and recorded: in the three replicates where `compio-push` wandered 24–42%,
`tokio`'s own 0-byte control moved at most 4.6% and its 1 MiB result was indistinguishable from
the clean runs. The disturbance was a property of the compio arms, not a session-wide noise
floor, so each transport was judged against the controls on its own transport.

- **`tokio`, 1 MiB: MET.** −30.6%, consistent in sign and magnitude across all seven clean
  replicates (−28.0% to −35.5%) and −31.10% across all ten, against a largest same-transport
  control movement of 7.22%. It clears the bar by more than four times, and the 0-byte control
  shows no effect exactly where the mechanism predicts none. The duplex family corroborates it
  independently — a separate binary with no compio arm at all, controls under 4.9%.
- **`compio`, 1 MiB: NOT MET.** The measured gain is −4.07%, but its own untouched control arm,
  `compio-push`, moved **34.94%** across the same replicates, and by the criterion as written
  4.07% does not clear that. This was reported as measured, not reworded into a win. The honest
  qualification, which did not rescue the verdict: the *paired* delta is far steadier than that
  spread — all seven replicates agree on sign and fall in a 2.8–5.4% band, as one expects if the
  wander is a common-mode session effect hitting both arms together — but that is a weaker
  statistical argument than SC-005 specifies. The completion transport gains far less because
  its push path already coalesced a pass into one write, so it never had a syscall prize, only
  the copy; and part of even that is spent minting frame headers the copying path got for free.

The headline never rested on the exclusion rule. Recomputed over all ten replicates with
nothing discarded, `tokio` at 1 MiB is **−31.10%** against the −30.6% above, every individual
replicate falls between −28.0% and −35.5%, and the conclusion is unchanged; the rule only ever
mattered to `compio`.

### Then: MET on both, on a host that drifts about 1%

`xeon-8370c-azure` moves ~1% between identical passes, and `compio-push` — the arm that cost
the first verdict — is as steady there as every other. Five replicates:

- **`tokio`, 1 MiB: MET**, −24.33% against a same-transport control spread of 0.64% at that
  size. A factor of 38, with all five replicates inside 1.2 percentage points of each other.
- **`compio`, 1 MiB: MET**, −4.55% against `compio-push`'s own 1.92% spread at that size and
  3.19% at its worst size, with all five replicates negative (−3.18% to −5.62%). **The gain is
  small, and it is a gain.**

The magnitudes did not carry over — tokio is −24.33% here against −30.6% there — but the sign,
the ordering and the 0-byte control did. That is what a finding is entitled to expect from a
new machine.

### And one thing the quiet host could see that the noisy one could not

**Below 64 KiB the completion transport is measurably *slower* with a handed-over body**:
+1.99% at 1 KiB and +1.23% at 0 B, positive in every replicate. The legacy host read −0.9% at
1 KiB and called it noise. The mechanism was already on record — the shared path mints frame
headers the copying path got for free — and on a transport with no syscall to save, that cost
is not paid for until the body is large enough for the copy to dominate. A caller handing over
small bodies on a completion transport is trading a little throughput for a promise about
copying.

## What a new machine should reproduce

The compio verdict has now been settled once, on a quiet host; a third machine either confirms
that or reopens it.

1. **0 B stays level** on both transports. This is mechanistic, not statistical: if it moves,
   the harness is wrong before any other number can be read. Note that `xeon-8370c-azure` reads
   +1.23% on compio at 0 B, small but positive in four replicates of five — at the edge of
   this control's usefulness, and worth watching.
2. **tokio shows a large gain at 1 KiB, 64 KiB and 1 MiB**, consistent in sign across every
   replicate, and far exceeding `tokio-push`'s and `hyper-tokio`'s own movement.
3. **compio shows a small gain at 1 MiB**, single-digit percent, because it has only the copy
   to win and no syscall — and, on the evidence of the quiet host, a small **loss** below
   64 KiB, where the frame headers the shared path must mint are not yet paid for.
4. The gains **track the write-count ratios** above rather than the byte counts. A gain at 0 B,
   or a compio gain the size of tokio's, contradicts the mechanism and not merely the figures.
5. The duplex family shows a smaller gain than the socket family, in the same direction. A
   socket gain with no duplex gain, or the reverse, needs an explanation before it is a result.
