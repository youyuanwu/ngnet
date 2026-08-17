//! What a transmit pass costs when several streams are in flight at once.
//!
//! # Why this exists beside `driver_writes.rs`
//!
//! `driver_writes.rs` pins the cost of a *single* stream carrying a large body: one write per
//! record, and one turn that carries the lot. That is the body axis. This file measures the
//! other axis — the number of streams in flight — because the two are not the same question
//! and the plan's central inference depends on which one is answered.
//!
//! The inference under test is recorded in `.paw/work/qmux-h3-perf/ImplementationPlan.md`: the
//! HTTP/2 stack measured a large gain from writing once per pass rather than once per protocol
//! unit, and the figure that anchors it — 513 writes collapsing to 1 — came from a
//! *multiplexed* pass across eight concurrent streams, not from a large body.
//! `docs/benchmarks/findings/write-path-and-gathering.md` separately records the 1 MiB body
//! point as neutral within noise. So whether the same mechanism exists here is a question about
//! concurrency, and it is answered by counting writes per turn as the number of open streams
//! grows.
//!
//! # What is pinned, and what is expected to invert it
//!
//! Everything asserted here is today's *unoptimized* behaviour, exactly as in
//! `driver_writes.rs`. The claims are deliberately shape claims rather than exact counts —
//! that writes per turn scale with the streams in flight, and that a coalescing buffer of a
//! stated capacity could remove almost all of them — because the exact counts depend on QPACK
//! output sizes and on how the harness's round-robin interleaves the two drivers, neither of
//! which this work is entitled to freeze. Phase 4 (write coalescing) is the phase expected to
//! fail these: once a turn's records leave in one write, `per_turn` collapses towards one and
//! the removable count collapses towards zero.
//!
//! Run with `--nocapture` to see the table these figures were reported from; the numbers are
//! printed rather than only asserted because the screen in
//! `.paw/work/qmux-h3-perf/Phase2Screen.md` quotes them.

mod transmit_harness;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_qmux::DEFAULT_MAX_RECORD_SIZE;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use ngnet_qmux_h3_tests::{Payload, drain, ok, pattern};
use transmit_harness::{Turns, collected};

/// The largest write a connection that writes one record at a time can issue.
///
/// A record is length-prefixed and the whole thing is capped at [`DEFAULT_MAX_RECORD_SIZE`],
/// so a write larger than this is proof that two records travelled together.
const RECORD: usize = DEFAULT_MAX_RECORD_SIZE as usize;

/// The concurrencies measured, matching the benchmark suite's sweep.
///
/// 1, 8 and 64 are what `concurrent_throughput` and `transport_concurrent_throughput` use, so
/// a shape found here can be read against those arms without interpolating.
const CONCURRENCY: [usize; 3] = [1, 8, 64];

/// A body small enough to fit one record, so the record count is driven by the stream count
/// rather than by the payload. Zero bytes: the headers-only exchange the concurrency arms use.
const EMPTY: usize = 0;

/// A body needing several records per stream, so the two axes can be told apart.
///
/// 64 KiB is one of the benchmark suite's body sizes and fills four full records with a
/// remainder, which is enough for the per-stream record count to dominate a single-stream turn
/// and still leave the run cheap at concurrency 64.
const LARGE: usize = 64 * 1024;

/// The body sizes the reported table sweeps.
///
/// The benchmark suite's own four points, so a shape found here can be read against
/// `body_throughput` and `transport_body_throughput` without interpolating. 1 MiB is included
/// even though it is the most expensive point to drive by hand, because it is the size the
/// plan's original argument was made from and the size
/// `docs/benchmarks/findings/write-path-and-gathering.md` predicts moves least.
const BODIES: [usize; 4] = [0, 1024, 64 * 1024, 1024 * 1024];

/// What counts as a small write in the reported table.
///
/// Arbitrary in the sense that no threshold is a natural boundary, and chosen because it is
/// well below any plausible payload and well above the framing a record carries: a write under
/// 64 bytes is a control record or a header record, not a body.
const SMALL: usize = 64;

/// The coalescing-buffer capacities the removable-write figures are computed at.
///
/// FR-004 requires a documented ceiling on the accumulated output rather than an unbounded
/// buffer, and the answer to "how many writes could coalescing remove" is a function of that
/// ceiling — so it is reported at more than one, and the conclusion is only worth anything if
/// it survives all of them. `usize::MAX` stands for "no ceiling at all", which is the upper
/// bound on what any ceiling could achieve rather than a proposal.
const CAPACITIES: [usize; 3] = [64 * 1024, 256 * 1024, usize::MAX];

/// Windows large enough that flow control does not split a pass for reasons unrelated to
/// what is being counted.
///
/// Raised on both ends together, because a QMux end's transport configuration is what it
/// permits its *peer*. The stream allowance is raised for the same reason the benchmark
/// fixtures raise theirs: it is a cumulative budget nothing recycles, and 64 concurrent
/// streams plus a run's worth of preamble would otherwise approach the default of 100.
fn windows() -> TransportConfig {
    TransportConfig::new()
        .initial_max_stream_data(8 << 20)
        .initial_max_data(64 << 20)
        .read_ahead(64 << 20)
        .max_streams_bidi(1 << 20)
}

/// Awaits every future in a list, polling each on every pass.
///
/// The harness deliberately has no runtime — a turn is a poll of the connection future and
/// nothing else may decide when that happens — so `JoinSet`, which the benchmark fixtures use
/// for the same workload, is not available here. This is the smallest thing that puts `n`
/// exchanges in flight simultaneously: it polls every unfinished future on every pass, so all
/// `n` requests are outstanding on the connection at once, which is the property being
/// measured.
struct AllOf {
    pending: Vec<Option<Pin<Box<dyn Future<Output = usize>>>>>,
    finished: Vec<Option<usize>>,
}

impl AllOf {
    fn new(futures: Vec<Pin<Box<dyn Future<Output = usize>>>>) -> Self {
        let finished = vec![None; futures.len()];
        Self {
            pending: futures.into_iter().map(Some).collect(),
            finished,
        }
    }
}

impl Future for AllOf {
    type Output = Vec<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<usize>> {
        let this = self.get_mut();
        let mut outstanding = false;
        for index in 0..this.pending.len() {
            if let Some(future) = this.pending[index].as_mut() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => {
                        this.finished[index] = Some(value);
                        this.pending[index] = None;
                    }
                    Poll::Pending => outstanding = true,
                }
            }
        }
        if outstanding {
            return Poll::Pending;
        }
        Poll::Ready(
            this.finished
                .iter()
                .map(|slot| slot.expect("every exchange finished"))
                .collect(),
        )
    }
}

/// What one measured point produced.
struct Point {
    concurrency: usize,
    /// Writes issued by each turn that wrote anything, in order.
    per_turn: Vec<usize>,
    /// Every write's length, grouped by the turn that issued it.
    turn_lengths: Vec<Vec<usize>>,
    total: usize,
}

impl Point {
    fn busiest(&self) -> usize {
        self.per_turn.iter().copied().max().unwrap_or(0)
    }

    /// How many bytes the client wrote in total.
    ///
    /// Reported beside the write count because the two together say what the average write
    /// carried, and a write-count reduction is only worth a syscall each if the writes it
    /// merges were small enough to have been merged in the first place.
    fn bytes(&self) -> usize {
        self.turn_lengths.iter().flatten().sum()
    }

    /// How many of those writes carried fewer than [`SMALL`] bytes.
    ///
    /// The figure that decides whether a write count is worth reducing. A syscall costs about
    /// the same whatever it carries, so merging writes of a few tens of bytes removes almost
    /// all of their cost, where merging full records mostly moves the same bytes in a different
    /// number of calls.
    fn small(&self) -> usize {
        self.turn_lengths
            .iter()
            .flatten()
            .filter(|len| **len < SMALL)
            .count()
    }

    /// How many writes remain if a turn's records are packed into buffers of `capacity`.
    ///
    /// The model is the one Phase 4 proposes: records are appended whole to a reused buffer in
    /// the order they were produced, and the buffer is written out when the next record will
    /// not fit or when the turn ends. A record larger than the capacity is written on its own,
    /// which cannot happen at any capacity at or above [`RECORD`] but is handled rather than
    /// assumed.
    ///
    /// The figure is an upper bound on what coalescing can achieve, and biased *towards*
    /// coalescing: it assumes every write in a turn is a record that may wait for its
    /// successors, which is precisely the invariant `docs/qmux/design.md:293-305` currently
    /// forbids and which Phase 4 would have to earn.
    fn coalesced(&self, capacity: usize) -> usize {
        let mut writes = 0;
        for turn in &self.turn_lengths {
            let mut held = 0usize;
            for &len in turn {
                if held > 0 && held + len > capacity {
                    writes += 1;
                    held = 0;
                }
                held += len;
            }
            if held > 0 {
                writes += 1;
            }
        }
        writes
    }
}

/// Runs `concurrency` simultaneous uploads of `body` bytes on one hand-driven connection.
fn measure(concurrency: usize, body: usize) -> Point {
    let (client_io, server_io) = stream_pair();
    // Taken before the stream is moved into the connection: the log is a handle to shared
    // state, and there is no way to reach the stream again once the connection owns it.
    let log = client_io.write_log();
    let clock = TestClock::new();
    let transport = windows();
    let http = HttpConfig::default();

    let serving = ngnet_qmux_h3::serve_with(
        server_io,
        clock.clone(),
        |request| async move {
            let (_parts, incoming) = request.into_parts();
            let received = drain(incoming).await.expect("the request body");
            // The length goes back as the response body rather than being asserted in the
            // handler, so a mismatch is reported by the test rather than by whichever poll
            // happened to be running the handler.
            ok(Bytes::from(received.len().to_string()))
        },
        transport,
        http,
    )
    .expect("serving");

    let (sender, connection) =
        ngnet_qmux_h3::connect_with::<_, _, Payload>(client_io, clock, transport, http)
            .expect("a client");

    let payload = pattern(body);
    let exchange = async move {
        // Every request is submitted before any response is awaited, so all `concurrency`
        // streams are open on the connection at the same time. Submitting inside the join
        // would let the first exchange finish before the last had started, which is a serial
        // run wearing a concurrent shape.
        let mut sending = Vec::with_capacity(concurrency);
        for index in 0..concurrency {
            let request = Request::builder()
                .method("POST")
                .uri(format!("https://qmux.test/upload/{index}"))
                .body(Full::new(payload.clone()))
                .expect("a request");
            sending.push(sender.send_request(request));
        }

        let mut exchanges: Vec<Pin<Box<dyn Future<Output = usize>>>> = Vec::new();
        for response in sending {
            exchanges.push(Box::pin(async move {
                let response = response.await.expect("a response");
                assert_eq!(response.status(), 200);
                let echoed = collected(response.into_body()).await;
                core::str::from_utf8(&echoed)
                    .expect("a decimal length")
                    .parse::<usize>()
                    .expect("a decimal length")
            }));
        }
        AllOf::new(exchanges).await
    };

    let (echoed, turns) = Turns::drive(&log, connection, serving, exchange);
    assert_eq!(
        echoed.len(),
        concurrency,
        "every request must produce a response, or the write counts are the cost of something \
         other than the exchange this measures"
    );
    for received in &echoed {
        assert_eq!(
            *received, body,
            "the server received {received} bytes rather than {body}, so this point measures a \
             truncated transfer"
        );
    }

    let mut turn_lengths = Vec::with_capacity(turns.writes.len());
    let mut cursor = 0;
    for count in &turns.writes {
        turn_lengths.push(turns.lengths[cursor..cursor + count].to_vec());
        cursor += count;
    }
    assert_eq!(
        cursor,
        turns.lengths.len(),
        "the per-turn counts do not account for every write, so grouping them by turn would \
         misattribute one"
    );

    Point {
        concurrency,
        per_turn: turns.writes,
        turn_lengths,
        total: turns.lengths.len(),
    }
}

/// Prints the table the screen quotes, and returns the points so they can be asserted on.
fn sweep(body: usize) -> Vec<Point> {
    let points: Vec<Point> = CONCURRENCY
        .iter()
        .map(|&concurrency| measure(concurrency, body))
        .collect();

    println!("\nbody = {body} bytes");
    println!(
        "{:>5} {:>8} {:>11} {:>7} {:>8} {:>9} {:>10} {:>10} {:>8}  writes per turn",
        "N", "writes", "bytes", "small", "turns", "busiest", "c=64KiB", "c=256KiB", "c=inf"
    );
    for point in &points {
        println!(
            "{:>5} {:>8} {:>11} {:>7} {:>8} {:>9} {:>10} {:>10} {:>8}  {:?}",
            point.concurrency,
            point.total,
            point.bytes(),
            point.small(),
            point.per_turn.len(),
            point.busiest(),
            point.coalesced(CAPACITIES[0]),
            point.coalesced(CAPACITIES[1]),
            point.coalesced(CAPACITIES[2]),
            point.per_turn,
        );
    }
    points
}

/// Prints the whole grid the screen quotes, and asserts the one thing that has to hold across
/// all of it.
///
/// Separate from the shape assertions below because it is the expensive one — 1 MiB across 64
/// streams is driven a poll at a time — and because what it exists to show is a *table*, not a
/// single inequality.
#[test]
fn today_every_point_in_the_sweep_writes_once_per_record() {
    for body in BODIES {
        for point in sweep(body) {
            let largest = point
                .turn_lengths
                .iter()
                .flatten()
                .copied()
                .max()
                .unwrap_or(0);
            assert!(
                largest <= RECORD,
                "today the connection flushes each record as it is produced -- `write_record` \
                 in `crates/ngnet-qmux/src/io/conn.rs` flushes, produces one record and flushes \
                 again -- so no write can carry more than one record. A write of {largest} \
                 bytes at concurrency {} with a {body}-byte body means two records travelled \
                 together, which is what Phase 4 is expected to make true everywhere in this \
                 grid",
                point.concurrency
            );
        }
    }
}

#[test]
fn today_writes_per_turn_grow_with_the_streams_in_flight() {
    let points = sweep(EMPTY);
    let single = &points[0];
    let many = &points[points.len() - 1];

    assert_eq!(
        single.concurrency, 1,
        "the first point must be the single-stream one"
    );
    assert_eq!(
        many.concurrency, 64,
        "the last point must be the sixty-four-stream one"
    );

    assert!(
        many.busiest() > 8 * single.busiest(),
        "with empty bodies the only thing that can add writes to a turn is another stream, so \
         a turn at concurrency 64 should carry many times the writes a turn at concurrency 1 \
         does; it carried {} against {}. If this stops being true, the multiplexed pass this \
         stack is being optimized for does not exist and Phase 4's expected gain has no \
         mechanism. Per-turn writes at 64: {:?}",
        many.busiest(),
        single.busiest(),
        many.per_turn
    );

    assert!(
        many.total > 8 * single.total,
        "total writes should scale with the streams in flight when each stream's payload is \
         empty; {} against {}",
        many.total,
        single.total
    );
}

#[test]
fn today_a_multiplexed_turn_is_almost_entirely_coalescable() {
    let points = sweep(EMPTY);
    let many = &points[points.len() - 1];

    for capacity in CAPACITIES {
        let coalesced = many.coalesced(capacity);
        assert!(
            coalesced * 4 < many.total,
            "at concurrency 64 with empty bodies the records of one turn are small and many, so \
             a coalescing buffer of {capacity} bytes should absorb the great majority of them: \
             {} writes should fall well below a quarter of the {} issued today, and fell to \
             {coalesced}. Phase 4 is expected to make this assertion unmeasurable by making the \
             two figures equal",
            coalesced,
            many.total
        );
    }
}

#[test]
fn today_a_large_body_adds_records_that_coalescing_can_also_absorb() {
    let points = sweep(LARGE);

    for point in &points {
        let full = point
            .turn_lengths
            .iter()
            .flatten()
            .filter(|len| **len == RECORD)
            .count();
        assert!(
            full >= 4 * point.concurrency,
            "each {LARGE}-byte body fills four whole records, so {} streams should produce at \
             least {} full-record writes; {full} were seen. A shortfall means the workload is \
             no longer the one these figures were measured for",
            point.concurrency,
            4 * point.concurrency
        );

        // The unbounded figure is the ceiling on what coalescing could ever remove, so a point
        // where it is not below today's count is a point where coalescing has nothing to win.
        assert!(
            point.coalesced(usize::MAX) < point.total,
            "coalescing must be able to remove at least one write at concurrency {} with a \
             {LARGE}-byte body, or there is nothing here for Phase 4 to do",
            point.concurrency
        );
    }
}
