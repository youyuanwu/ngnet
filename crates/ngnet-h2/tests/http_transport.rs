//! The transport abstraction's contract, asserted mostly by compiling (Spec FR-013 to
//! FR-016, SC-015 in part).
//!
//! Four properties matter here and none of them is about behaviour, which is why the
//! assertions are largely type-level:
//!
//! * a completion-based transport can be written without mentioning the borrowed-write
//!   path — or the owned-region path — at all;
//! * a readiness-based one can elect the borrowed path through the single override that
//!   carries both the choice and the write;
//! * a completion-based one can elect the owned-region path through its own split
//!   election — a plain predicate read once per pass, separate from the write that carries
//!   the regions, because a late `None` there would consume and lose owned buffers;
//! * neither is required to be `Send`, because the flagship completion runtimes are
//!   thread-per-core and build their I/O on `Rc`. A `Send` bound in the traits would
//!   exclude exactly the runtimes the abstraction exists to serve.

#![cfg(feature = "http")]

use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use ngnet_h2::http::testing::{
    Duplex, DuplexReader, Full, block_on, duplex, duplex_borrowed, duplex_owned_regions,
    duplex_vectored, http_crate as http,
};
use ngnet_h2::http::transport::{
    BorrowedWrite, Coalesced, Gathering, OwnedRegions, PerRegion, RegionWrite, VectoredWrite,
};
use ngnet_h2::http::{Transport, TransportRead, TransportWrite};

use bytes::{Bytes, BytesMut};
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};

/// A completion-based transport: owns its buffers, ignores the borrowed-write path.
///
/// This is the shape `io_uring`-backed runtimes need, and it compiles without naming
/// `write_borrowed` — the default carries it.
struct Completion {
    written: Vec<u8>,
    to_read: Vec<u8>,
}

struct CompletionReader {
    to_read: Vec<u8>,
}

struct CompletionWriter {
    written: Vec<u8>,
}

impl Transport for Completion {
    type Reader = CompletionReader;
    type Writer = CompletionWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            CompletionReader {
                to_read: self.to_read,
            },
            CompletionWriter {
                written: self.written,
            },
        )
    }
}

impl TransportRead for CompletionReader {
    async fn read(&mut self, mut buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        let take = self.to_read.len().min(buf.capacity().max(1));
        buf.extend_from_slice(&self.to_read[..take]);
        self.to_read.drain(..take);
        (Ok(take), buf)
    }
}

impl TransportWrite for CompletionWriter {
    type Strategy = Coalesced;

    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        self.written.extend_from_slice(&buf);
        let written = buf.len();
        (Ok(written), buf)
    }
}

/// A readiness-based transport: overrides the borrowed path and advertises it.
struct Readiness;

struct ReadinessHalf {
    borrowed: Rc<RefCell<usize>>,
}

impl Transport for Readiness {
    type Reader = ReadinessHalf;
    type Writer = ReadinessHalf;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let counter = Rc::new(RefCell::new(0));
        (
            ReadinessHalf {
                borrowed: Rc::clone(&counter),
            },
            ReadinessHalf { borrowed: counter },
        )
    }
}

impl TransportRead for ReadinessHalf {
    async fn read(&mut self, buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        (Ok(0), buf)
    }
}

impl TransportWrite for ReadinessHalf {
    type Strategy = PerRegion;

    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }
}

impl BorrowedWrite for ReadinessHalf {
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        // The borrowed path is now both elected and written here: declaring `PerRegion` as the
        // strategy obliges this impl, and there is no separate flag that could disagree with
        // it. A short count or an error is the only way to report trouble; there is no way to
        // decline the path once the marker names it.
        *self.borrowed.borrow_mut() += 1;
        core::future::ready(Ok(data.len()))
    }
}

/// A transport that offers only the vectored path, counting **polled** writes.
///
/// The recording sits in the future's `poll`, not in `write_vectored` itself, and that
/// placement is load-bearing: the driver elects a strategy by constructing one of these
/// futures and dropping it without ever polling it. A fixture that recorded at construction
/// would count a write that never happened, on every pass, and quietly corrupt every
/// write-count assertion built on it.
struct VectoredOnly {
    polled_writes: Rc<RefCell<usize>>,
    regions_seen: Rc<RefCell<Vec<usize>>>,
}

/// The future `VectoredOnly::write_vectored` hands back. Inert until polled.
struct RecordOnPoll<'w> {
    regions: &'w [io::IoSlice<'w>],
    polled_writes: Rc<RefCell<usize>>,
    regions_seen: Rc<RefCell<Vec<usize>>>,
}

impl Future for RecordOnPoll<'_> {
    type Output = io::Result<usize>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        let me = self.get_mut();
        *me.polled_writes.borrow_mut() += 1;
        me.regions_seen.borrow_mut().push(me.regions.len());
        let total = me.regions.iter().map(|region| region.len()).sum();
        core::task::Poll::Ready(Ok(total))
    }
}

impl TransportRead for VectoredOnly {
    async fn read(&mut self, buf: BytesMut) -> (io::Result<usize>, BytesMut) {
        (Ok(0), buf)
    }
}

impl TransportWrite for VectoredOnly {
    type Strategy = Gathering;

    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }
}

impl BorrowedWrite for VectoredOnly {
    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        // The live fallback `VectoredWrite` requires: when a stream does not really
        // scatter-gather, the driver writes each region here instead. It must be real, not a
        // stub — so it does the same accounting the vectored path does, one region per call.
        *self.polled_writes.borrow_mut() += 1;
        self.regions_seen.borrow_mut().push(1);
        core::future::ready(Ok(data.len()))
    }
}

impl VectoredWrite for VectoredOnly {
    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> impl Future<Output = io::Result<usize>> + 'w {
        RecordOnPoll {
            regions,
            polled_writes: Rc::clone(&self.polled_writes),
            regions_seen: Rc::clone(&self.regions_seen),
        }
    }
}

/// A completion transport that elects the owned-region path, counting **polled** writes.
///
/// The completion counterpart of the readiness fixtures: it names [`OwnedRegions`] as its
/// strategy, which obliges it — by compile error otherwise — to implement [`RegionWrite`], and
/// nothing else. It cannot implement the borrowed or vectored paths at all, because
/// [`OwnedRegions`] is a completion strategy, not a readiness one; the type system forbids a
/// writer from carrying both models. This is the fixture that carries the owned-region write
/// assertions the old completion writer used to.
struct OwnedRegionsOnly {
    region_writes: Rc<RefCell<usize>>,
}

impl TransportWrite for OwnedRegionsOnly {
    type Strategy = OwnedRegions;

    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }
}

impl RegionWrite for OwnedRegionsOnly {
    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<Bytes>)> {
        // One call is one gathering write; the `Vec` comes in and goes back out so the driver
        // can reuse its allocation — the ownership round-trip a completion API needs.
        *self.region_writes.borrow_mut() += 1;
        let written = regions.iter().map(Bytes::len).sum();
        core::future::ready((Ok(written), regions))
    }
}

#[test]
fn a_transport_can_elect_the_vectored_path_alone() {
    // Electing the vectored path is now a compile-time declaration: `VectoredOnly` names
    // `Gathering` as its strategy, which obliges it to implement `VectoredWrite`. There is no
    // run-time probe to inspect and no `Option` to be `None` — the election happened in the
    // type. What remains to assert is the write itself: a gathering call carries every region
    // in one poll, in order, and nothing is recorded until it is polled.
    let polled_writes = Rc::new(RefCell::new(0));
    let regions_seen = Rc::new(RefCell::new(Vec::new()));
    let mut writer = VectoredOnly {
        polled_writes: Rc::clone(&polled_writes),
        regions_seen: Rc::clone(&regions_seen),
    };

    // Building the future records nothing: an `async`/inert future is not a write until it is
    // polled, which is what lets the driver construct one speculatively and drop it.
    let unpolled = writer.write_vectored(&[]);
    drop(unpolled);
    assert_eq!(
        *polled_writes.borrow(),
        0,
        "constructing a vectored write must not count as one — a fixture recording at \
         construction would inflate every count"
    );

    let regions = [io::IoSlice::new(b"header"), io::IoSlice::new(b"payload")];
    let write = writer.write_vectored(&regions);
    let written = block_on(write).unwrap();

    assert_eq!(written, b"header".len() + b"payload".len());
    assert_eq!(*polled_writes.borrow(), 1, "one polled call is one write");
    assert_eq!(
        &regions_seen.borrow()[..],
        &[2],
        "both regions arrive in a single call, in order — that is the whole point"
    );
}

#[test]
fn a_vectored_duplex_gathers_regions_and_records_them_on_poll() {
    let (client, peer) = duplex_vectored();
    let log = client.vectored_log();
    let counter = client.write_counter();
    let (_reader, mut writer) = Transport::split(client);

    // Building a vectored write records nothing until it is polled: an inert future is not a
    // write, which is what lets the driver construct one speculatively and drop it.
    let unpolled = writer.write_vectored(&[]);
    drop(unpolled);
    assert_eq!(
        (counter.get(), log.calls().len()),
        (0, 0),
        "an unpolled vectored write is not a write and must leave no trace"
    );

    let regions = [
        io::IoSlice::new(b"small blocks gathered"),
        io::IoSlice::new(b"; then a large one"),
    ];
    let written = block_on(writer.write_vectored(&regions)).unwrap();

    assert_eq!(written, b"small blocks gathered; then a large one".len());
    assert_eq!(counter.get(), 1, "one polled call is one write");
    assert_eq!(log.calls(), vec![vec![21, 18]], "two regions, in one call");
    assert_eq!(log.octets(), b"small blocks gathered; then a large one");
    assert_eq!(log.retries(), 0);

    // And the octets really crossed to the peer, in the order they were offered.
    let (mut peer_reader, _peer_writer) = Transport::split(peer);
    let (read, buf) = block_on(peer_reader.read(BytesMut::with_capacity(64)));
    assert_eq!(read.unwrap(), written);
    assert_eq!(&buf[..], b"small blocks gathered; then a large one");
}

#[test]
fn a_vectored_duplex_can_be_told_to_accept_only_a_prefix() {
    let (client, _peer) = duplex_vectored();
    let log = client.vectored_log();
    let counter = client.write_counter();
    // A cut inside the first region, then one landing exactly on the region boundary.
    client.accept_at_most([3, 2]);
    let (_reader, mut writer) = Transport::split(client);

    let regions = [io::IoSlice::new(b"abcde"), io::IoSlice::new(b"fghij")];

    let first = block_on(writer.write_vectored(&regions)).unwrap();
    assert_eq!(
        first, 3,
        "the cap is honoured, and it cut inside region one"
    );

    // The driver would now re-offer the remainder; here the fixture is driven directly, so
    // the regions are trimmed by hand to model exactly that.
    let retry = [io::IoSlice::new(b"de"), io::IoSlice::new(b"fghij")];
    let second = block_on(writer.write_vectored(&retry)).unwrap();
    assert_eq!(second, 2, "the second cap lands exactly on the boundary");

    // Which is the interesting case: the remainder is now the second region alone. Offering
    // it beside a zero-length first region would be the bug — hence one region, not two.
    let last = [io::IoSlice::new(b"fghij")];
    let third = block_on(writer.write_vectored(&last)).unwrap();
    assert_eq!(
        third, 5,
        "caps exhausted, so everything offered is accepted"
    );

    assert_eq!(log.octets(), b"abcdefghij", "no octet lost, none reordered");
    assert_eq!(
        log.calls(),
        vec![vec![5, 5], vec![2, 5], vec![5]],
        "and no call was ever offered an empty region"
    );
    assert_eq!(
        (counter.get(), log.retries()),
        (1, 2),
        "a call following a short one re-offers octets already counted, so it is a retry \
         rather than another logical write — which is what lets a per-pass write bound \
         exclude retries without reconstructing which was which"
    );
}

#[test]
fn a_vectored_duplex_can_report_a_successful_write_of_nothing() {
    let (client, _peer) = duplex_vectored();
    client.accept_at_most([0]);
    let (_reader, mut writer) = Transport::split(client);

    let regions = [io::IoSlice::new(b"offered but not taken")];
    let written = block_on(writer.write_vectored(&regions)).unwrap();

    assert_eq!(
        written, 0,
        "the fault the driver must turn into an error rather than spin on: success \
         reporting no progress"
    );
}

#[test]
fn a_completion_transport_needs_no_borrowed_write_path() {
    // Compiling is most of the assertion: `CompletionWriter` above declares `Coalesced` and
    // implements only `write`. It names none of the borrowed, vectored or owned-region
    // paths, and it does not have to — a coalesced transport is obliged to supply nothing
    // beyond `write`, and the specialised write traits are not even in scope on it, so a test
    // *cannot* demand a specialised capability of it. That is the guarantee: adding
    // strategies to the abstraction leaves a completion transport that wants none of them
    // compiling untouched.
    let (mut reader, mut writer) = Completion {
        written: Vec::new(),
        to_read: b"from the peer".to_vec(),
    }
    .split();

    let (read, buf) = block_on(reader.read(BytesMut::with_capacity(64)));
    assert_eq!(read.unwrap(), b"from the peer".len());
    assert_eq!(&buf[..], b"from the peer");

    // The one write path a coalesced transport supplies, and the only one the driver reaches
    // for it: everything is copied into the owned buffer and written whole.
    let (written, _buf) = block_on(writer.write(Bytes::from_static(b"to the peer")));
    assert_eq!(written.unwrap(), b"to the peer".len());
    assert_eq!(writer.written, b"to the peer");
}

#[test]
fn a_transport_can_elect_the_owned_region_path() {
    // The owned-region write in isolation, exercised directly rather than through the driver:
    // this calls `write_regions` on a transport that declares `OwnedRegions`, so it pins the
    // *contract* the completion write owes its caller — the whole list's length reported, and
    // the `Vec` handed back untouched so the driver can reuse it. The election itself is no
    // longer a run-time predicate to read: declaring `OwnedRegions` as the strategy *is* the
    // election, settled in the type, which is why there is nothing here to consult before the
    // write. Ownership passing in and back out is what lets the completion path never lose an
    // owned buffer.
    let region_writes = Rc::new(RefCell::new(0));
    let mut writer = OwnedRegionsOnly {
        region_writes: Rc::clone(&region_writes),
    };

    let regions = vec![
        Bytes::from_static(b"header"),
        Bytes::from_static(b"payload"),
    ];
    let (result, returned) = block_on(writer.write_regions(regions));
    assert_eq!(
        result.unwrap(),
        b"header".len() + b"payload".len(),
        "the gathering write reports the whole list's length"
    );
    assert_eq!(
        returned,
        vec![
            Bytes::from_static(b"header"),
            Bytes::from_static(b"payload")
        ],
        "and hands the `Vec` back so the driver can reuse it — ownership in and back out"
    );
    assert_eq!(
        *region_writes.borrow(),
        1,
        "one call is one gathering write"
    );
}

/// A request body large enough that its serialisation spans several regions — a run of small
/// handshake blocks, the request head, and a handed-over `DATA` payload riding as its own
/// region — so the gathering write the vectored path performs is a real multi-region one.
const BODY: usize = 4_096;

/// The prefix a forced short write accepts, small enough to cut inside the first region so the
/// remainder is retried. Non-zero, since a zero-octet accept is an error the driver rejects
/// rather than a short write it resumes.
const SHORT_PREFIX: usize = 16;

/// Polls enough to carry the whole request even when short writes stretch it over retries; a
/// silent peer never answers, so extra polls past completion simply park.
const PASSES: usize = 8;

/// What one driven run produced.
#[derive(Debug)]
struct DrivenRun {
    /// Every octet the peer half actually received, in order.
    peer: Vec<u8>,
    /// The region lengths of each gathering write performed, retries included; empty if no
    /// gathering write ran.
    calls: Vec<Vec<usize>>,
    /// Calls that re-offered the remainder of a short write rather than new octets.
    retries: usize,
    /// Times the owned-region write (`write_regions`) actually ran, retries included.
    region_writes: usize,
}

impl DrivenRun {
    /// Whether the peer received the request body — the `4096` `x` octets the body is — so
    /// "the strategy carried the traffic" is a claim about a real request having crossed, not
    /// an empty handshake.
    fn request_reached_peer(&self) -> bool {
        let body: Vec<u8> = vec![b'x'; BODY];
        self.peer
            .windows(body.len())
            .any(|window| window == body.as_slice())
    }
}

/// Wakes a hand-stepped connection.
struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Polls one future once.
fn step<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    let mut context = Context::from_waker(waker);
    future.poll(&mut context)
}

/// Reads whatever the peer half holds right now, stopping the moment its read parks.
fn drain(reader: &mut DuplexReader, waker: &Waker) -> Vec<u8> {
    let mut seen = Vec::new();
    loop {
        let read = reader.read(BytesMut::with_capacity(64 * 1024));
        let mut read = core::pin::pin!(read);
        match step(read.as_mut(), waker) {
            Poll::Ready((result, buf)) => {
                let count = result.expect("reading from the peer half");
                if count == 0 {
                    return seen;
                }
                seen.extend_from_slice(&buf);
            }
            Poll::Pending => return seen,
        }
    }
}

/// Drives one shared-body upload over a caller-chosen transport pair, hand-stepping the
/// connection against a silent peer so the write side is a self-contained, reproducible pass,
/// and reports what the strategy carried.
///
/// Generic over the strategy marker `S`: the driver elects the drain path from the writer's
/// declared strategy at compile time, so a test picks the strategy by handing this the duplex
/// pair built over it. The body is handed over (`handshake_shared`), so its `DATA` payload
/// rides as its own region — the arrangement the owned-region path exists for. `caps`, if any,
/// force short writes so a retry-resuming path is exercised.
fn drive_over<S>(
    sides: (Duplex<S>, Duplex<S>),
    body: usize,
    caps: &[usize],
    passes: usize,
) -> DrivenRun
where
    Duplex<S>: Transport<Reader = DuplexReader>,
{
    let (client_side, server_side) = sides;
    let vectored = client_side.vectored_log();
    let elections = client_side.election_log();
    if !caps.is_empty() {
        client_side.accept_at_most(caps.iter().copied());
    }
    // Split rather than dropped: a dropped writing half closes the pipe, which the connection
    // reads as a peer that hung up and ends before the pass under test.
    let (mut peer_reader, _peer_writer) = Transport::split(server_side);

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("http://example.test/driven")
        .body(Full::new(vec![b'x'; body]))
        .expect("building a request");

    let waker = Waker::from(Arc::new(Flag(AtomicBool::new(false))));

    let (requests, connection) =
        ngnet_h2::http::handshake_shared::<_, Full>(client_side).expect("handshake");
    let response = requests.send_request(request);
    let mut connection = core::pin::pin!(connection);
    let mut response = core::pin::pin!(response);

    for _ in 0..passes {
        let _ = step(connection.as_mut(), &waker);
        let _ = step(response.as_mut(), &waker);
    }

    let peer = drain(&mut peer_reader, &waker);
    // Held until here: dropping the request handle earlier would close the request half before
    // the pass finished.
    drop(requests);

    DrivenRun {
        peer,
        calls: vectored.calls(),
        retries: vectored.retries(),
        region_writes: elections.region_writes(),
    }
}

#[test]
fn an_owned_region_connection_drains_through_write_regions() {
    // The owned-region strategy driven over a real connection, hand-stepped against a silent
    // peer. A shared body's `DATA` payload rides as its own owned region, so the whole request
    // must cross to the peer through `write_regions` — and a forced short write makes the write
    // run more than once, which `region_writes` counts retries included. This is the driven
    // counterpart to the isolated `write_regions` contract test above, over the driver rather
    // than the transport directly.
    let observed = drive_over(duplex_owned_regions(), BODY, &[SHORT_PREFIX], PASSES);

    assert!(
        observed.request_reached_peer(),
        "the peer never received the request body the owned-region path should have carried: \
         {observed:?}",
    );
    assert!(
        observed.retries >= 1,
        "no short write was forced, so the retry-resume path was never exercised: {observed:?}",
    );
    assert!(
        observed.region_writes >= 1,
        "the owned-region write never ran, so the strategy did not carry the traffic: \
         {observed:?}",
    );
    assert!(
        !observed.calls.is_empty(),
        "the owned-region path recorded no write, so it did not carry the traffic: \
         {observed:?}",
    );
}

#[test]
fn a_duplex_can_elect_the_owned_region_path() {
    // The in-memory harness counterpart: a duplex built for the owned-region shape carries the
    // completion strategy in its type, so the driver has exactly one strategy to take and the
    // readiness paths are not even implemented on it. This is what `http_shared_body.rs` drives
    // a whole connection over. Here `write_regions` is exercised directly.
    let (client, peer) = duplex_owned_regions();
    let (_reader, mut writer) = Transport::split(client);

    let regions = vec![
        Bytes::from_static(b"header"),
        Bytes::from_static(b"payload"),
    ];
    let (result, returned) = block_on(writer.write_regions(regions));
    assert_eq!(result.unwrap(), b"header".len() + b"payload".len());
    assert_eq!(
        returned.len(),
        2,
        "the `Vec` comes back for reuse, both regions intact"
    );

    // The octets really crossed to the peer, in the order they were offered.
    let (mut peer_reader, _peer_writer) = Transport::split(peer);
    let (read, buf) = block_on(peer_reader.read(BytesMut::with_capacity(64)));
    assert_eq!(read.unwrap(), b"headerpayload".len());
    assert_eq!(&buf[..], b"headerpayload");
}

#[test]
fn a_readiness_transport_can_take_the_zero_copy_path() {
    let (_reader, mut writer) = Readiness.split();

    // The borrowed path is now a method the `PerRegion` strategy obliges, not an `Option` to
    // inspect: declaring the strategy elected it, and calling it writes.
    let written = block_on(writer.write_borrowed(b"borrowed")).unwrap();
    assert_eq!(written, b"borrowed".len());
    assert_eq!(
        *writer.borrowed.borrow(),
        1,
        "the borrowed path should have been taken, not the coalescing default"
    );
}

#[test]
fn a_transport_need_not_be_send() {
    // The property that matters most for Story P4, and the one a `Send` supertrait would
    // have silently destroyed. `ReadinessHalf` holds an `Rc`, so it is not `Send` — and
    // it still satisfies the traits. Thread-per-core completion runtimes look exactly
    // like this.
    fn accepts_any_transport<T: Transport>(_transport: T) {}
    accepts_any_transport(Readiness);

    fn is_send<T: Send>() {}
    is_send::<Completion>();

    // Deliberately *not* `is_send::<Readiness>()`: it need not be, and requiring it is the
    // mistake this test exists to prevent.
    let (reader, _writer) = Readiness.split();
    let not_send: Rc<()> = Rc::new(());
    drop((reader, not_send));
}

#[test]
fn the_in_memory_duplex_carries_bytes_both_ways() {
    // The scaffolding the later phases build on, exercised here so a fault in it is
    // attributed to the transport rather than to whatever is being tested with it.
    let (client, server) = duplex();
    let (_client_reader, mut client_writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    block_on(async {
        let (result, _buf) = client_writer.write(Bytes::from_static(b"ping")).await;
        assert_eq!(result.unwrap(), 4);

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), 4);
        assert_eq!(&buf[..], b"ping");
    });
}

#[test]
fn a_closed_duplex_reports_end_of_stream() {
    let (client, server) = duplex();
    let (mut server_reader, _sw) = server.split();

    // Dropping the writing half closes the pipe, which is what a peer hanging up looks
    // like. Note the halves must actually be dropped, not merely bound to `_`-prefixed
    // names, which keep them alive to the end of the scope.
    drop(client.split());

    block_on(async {
        let (read, _buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(
            read.unwrap(),
            0,
            "a closed peer should read as end of stream, not hang"
        );
    });
}

#[test]
fn write_counts_stay_observable_across_a_split() {
    // Splitting consumes the transport, so a test that drives a connection can no longer
    // reach it — yet the per-pass write counts are precisely what the later phases must
    // assert. Taking a counter handle first is how that stays possible, and this pins it
    // before anything depends on it.
    let (client, server) = duplex();
    let counter = client.write_counter();
    let (_reader, mut writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    assert_eq!(counter.get(), 0, "nothing written yet");

    block_on(async {
        let (result, _buf) = writer.write(Bytes::from_static(b"one")).await;
        result.unwrap();
        let (result, _buf) = writer.write(Bytes::from_static(b"two")).await;
        result.unwrap();

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), 6);
        assert_eq!(&buf[..], b"onetwo");
    });

    assert_eq!(counter.get(), 2, "two writes should have been counted");
    assert_eq!(
        writer.writes(),
        counter.get(),
        "the writer and the handle should agree"
    );

    counter.reset();
    assert_eq!(counter.get(), 0, "resetting lets a single pass be measured");
}

#[test]
fn a_borrowed_write_duplex_takes_the_zero_copy_path_and_still_counts() {
    // The other of the two shapes the in-memory transport can take. Both are used by the
    // later drain-strategy assertions, so both need coverage here rather than one being
    // assumed to work because the other does.
    let (client, server) = duplex_borrowed();
    let counter = client.write_counter();
    let (_reader, mut writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    // The borrowed path is a method the `PerRegion` strategy obliges, not an `Option` to
    // inspect: calling it writes.
    block_on(async {
        writer.write_borrowed(b"borrowed").await.unwrap();

        let (read, buf) = server_reader.read(BytesMut::with_capacity(16)).await;
        assert_eq!(read.unwrap(), b"borrowed".len());
        assert_eq!(&buf[..], b"borrowed");
    });

    assert_eq!(
        counter.get(),
        1,
        "a borrowed write is still a write, and must be counted as one"
    );
}
