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
    Duplex, DuplexReader, Full, block_on, duplex, duplex_offering_both, duplex_owned_regions,
    duplex_vectored, duplex_vectored_and_owned_regions, http_crate as http,
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
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // The single override: returning `Some` both elects the zero-copy path and is how
        // it writes. There is no separate flag that could disagree with it.
        *self.borrowed.borrow_mut() += 1;
        Some(core::future::ready(Ok(data.len())))
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
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        Some(RecordOnPoll {
            regions,
            polled_writes: Rc::clone(&self.polled_writes),
            regions_seen: Rc::clone(&self.regions_seen),
        })
    }
}

/// A transport that offers **both** fast paths, so precedence has something to arbitrate.
///
/// Both counters live in the futures' `poll`, for the reason `RecordOnPoll` explains and for
/// a second one specific to precedence: what matters is which path was *taken*, not which was
/// *offered*. Counting at construction would answer the wrong question — every path this
/// transport implements is offered on every pass, and the driver's unpolled election probe
/// would be indistinguishable from a real write.
struct OffersBoth {
    borrowed_writes: Rc<RefCell<usize>>,
    vectored_writes: Rc<RefCell<usize>>,
}

impl TransportWrite for OffersBoth {
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }

    fn write_borrowed<'w>(
        &'w mut self,
        data: &'w [u8],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        Some(CountOnPoll {
            len: data.len(),
            counter: Rc::clone(&self.borrowed_writes),
        })
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        Some(CountOnPoll {
            len: regions.iter().map(|region| region.len()).sum(),
            counter: Rc::clone(&self.vectored_writes),
        })
    }
}

/// Reports a fixed length and counts the call, on poll rather than on construction.
struct CountOnPoll {
    len: usize,
    counter: Rc<RefCell<usize>>,
}

impl Future for CountOnPoll {
    type Output = io::Result<usize>;

    fn poll(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        *self.counter.borrow_mut() += 1;
        core::task::Poll::Ready(Ok(self.len))
    }
}

/// A completion transport that elects the owned-region path, and *also* offers the vectored
/// one — the arrangement precedence has to arbitrate. Which write actually runs is counted so
/// a test can see it: the driver's rule is that vectored wins, so a transport advertising both
/// must have its regions carried by the vectored path, never `write_regions`.
struct OffersVectoredAndOwnedRegions {
    vectored_writes: Rc<RefCell<usize>>,
    region_writes: Rc<RefCell<usize>>,
}

impl TransportWrite for OffersVectoredAndOwnedRegions {
    async fn write(&mut self, buf: Bytes) -> (io::Result<usize>, Bytes) {
        let written = buf.len();
        (Ok(written), buf)
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        Some(CountOnPoll {
            len: regions.iter().map(|region| region.len()).sum(),
            counter: Rc::clone(&self.vectored_writes),
        })
    }

    fn gathers_owned_regions(&self) -> bool {
        true
    }

    fn write_regions(
        &mut self,
        regions: Vec<Bytes>,
    ) -> impl Future<Output = (io::Result<usize>, Vec<Bytes>)> {
        *self.region_writes.borrow_mut() += 1;
        let written = regions.iter().map(Bytes::len).sum();
        core::future::ready((Ok(written), regions))
    }
}

#[test]
fn a_transport_can_elect_the_vectored_path_alone() {
    let polled_writes = Rc::new(RefCell::new(0));
    let regions_seen = Rc::new(RefCell::new(Vec::new()));
    let mut writer = VectoredOnly {
        polled_writes: Rc::clone(&polled_writes),
        regions_seen: Rc::clone(&regions_seen),
    };

    assert!(
        writer.write_borrowed(b"unused").is_none(),
        "electing the vectored path says nothing about the borrowed one, which this \
         transport never overrode"
    );

    // Exactly what the driver's election probe does: construct, inspect, drop unpolled.
    let probe = writer.write_vectored(&[]);
    assert!(probe.is_some(), "the vectored path is on offer");
    drop(probe);
    assert_eq!(
        *polled_writes.borrow(),
        0,
        "constructing the election probe must not count as a write — the driver never \
         polls it, and a fixture recording at construction would inflate every count"
    );

    let regions = [io::IoSlice::new(b"header"), io::IoSlice::new(b"payload")];
    let write = writer.write_vectored(&regions).expect("the vectored path");
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
fn a_transport_may_offer_both_fast_paths() {
    let borrowed_writes = Rc::new(RefCell::new(0));
    let vectored_writes = Rc::new(RefCell::new(0));
    let mut writer = OffersBoth {
        borrowed_writes: Rc::clone(&borrowed_writes),
        vectored_writes: Rc::clone(&vectored_writes),
    };

    // Both are on offer. Which one the driver *takes* is asserted where the driver is,
    // since precedence is the driver's rule rather than the transport's; here the point is
    // only that overriding one does not preclude overriding the other.
    assert!(writer.write_borrowed(b"borrowed").is_some());
    let regions = [io::IoSlice::new(b"vectored")];
    assert!(writer.write_vectored(&regions).is_some());
    assert_eq!(
        (*borrowed_writes.borrow(), *vectored_writes.borrow()),
        (0, 0),
        "offering a path is not performing it: neither future was polled, so neither may \
         have counted a write"
    );

    let written = block_on(writer.write_borrowed(b"borrowed").expect("borrowed")).unwrap();
    assert_eq!(written, b"borrowed".len());
    assert_eq!(
        (*borrowed_writes.borrow(), *vectored_writes.borrow()),
        (1, 0),
        "polling the borrowed future counts exactly one borrowed write and no vectored one"
    );

    let written = block_on(writer.write_vectored(&regions).expect("vectored")).unwrap();
    assert_eq!(written, b"vectored".len());
    assert_eq!(
        (*borrowed_writes.borrow(), *vectored_writes.borrow()),
        (1, 1),
        "and the vectored one likewise"
    );
}

#[test]
fn a_vectored_duplex_gathers_regions_and_records_them_on_poll() {
    let (client, peer) = duplex_vectored();
    let log = client.vectored_log();
    let counter = client.write_counter();
    let (_reader, mut writer) = Transport::split(client);

    // The election probe: constructed, inspected, dropped unpolled.
    let probe = writer.write_vectored(&[]);
    assert!(
        probe.is_some(),
        "a vectored duplex offers the vectored path"
    );
    drop(probe);
    assert_eq!(
        (counter.get(), log.calls().len()),
        (0, 0),
        "the probe is not a write and must leave no trace — the driver builds one every \
         pass and never polls it"
    );

    assert!(
        writer.write_borrowed(b"unused").is_none(),
        "a vectored duplex declines the borrowed path, so the driver has one strategy to \
         pick and no ambiguity to resolve"
    );

    let regions = [
        io::IoSlice::new(b"small blocks gathered"),
        io::IoSlice::new(b"; then a large one"),
    ];
    let written = block_on(writer.write_vectored(&regions).expect("vectored")).unwrap();

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

    let first = block_on(writer.write_vectored(&regions).expect("vectored")).unwrap();
    assert_eq!(
        first, 3,
        "the cap is honoured, and it cut inside region one"
    );

    // The driver would now re-offer the remainder; here the fixture is driven directly, so
    // the regions are trimmed by hand to model exactly that.
    let retry = [io::IoSlice::new(b"de"), io::IoSlice::new(b"fghij")];
    let second = block_on(writer.write_vectored(&retry).expect("vectored")).unwrap();
    assert_eq!(second, 2, "the second cap lands exactly on the boundary");

    // Which is the interesting case: the remainder is now the second region alone. Offering
    // it beside a zero-length first region would be the bug — hence one region, not two.
    let last = [io::IoSlice::new(b"fghij")];
    let third = block_on(writer.write_vectored(&last).expect("vectored")).unwrap();
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
    let written = block_on(writer.write_vectored(&regions).expect("vectored")).unwrap();

    assert_eq!(
        written, 0,
        "the fault the driver must turn into an error rather than spin on: success \
         reporting no progress"
    );
}

#[test]
fn a_vectored_duplex_can_be_told_to_decline_the_path_it_elected() {
    let (client, _peer) = duplex_vectored();
    let log = client.vectored_log();
    // Offer the path, then withdraw it after one write has actually happened.
    client.decline_vectored_after(1);
    let (_reader, mut writer) = Transport::split(client);

    let probe = writer.write_vectored(&[]);
    assert!(
        probe.is_some(),
        "the election must still succeed — the probe is not a write, so it may not spend \
         the budget, or this would be a transport that never elected the path at all \
         rather than one that abandoned it partway"
    );
    drop(probe);

    let first = [io::IoSlice::new(b"elected")];
    assert!(block_on(writer.write_vectored(&first).expect("vectored")).is_ok());

    assert!(
        writer
            .write_vectored(&[io::IoSlice::new(b"declined")])
            .is_none(),
        "and now the transport reneges mid-pass, which the contract forbids and the driver \
         must nonetheless survive"
    );
    assert_eq!(log.calls().len(), 1, "the declined call never happened");
}

#[test]
fn a_duplex_can_offer_both_fast_paths_at_once() {
    let (client, _peer) = duplex_offering_both();
    let (_reader, mut writer) = Transport::split(client);

    assert!(
        writer.write_borrowed(b"borrowed").is_some(),
        "both overrides are on offer; precedence between them is the driver's rule, and \
         needs a transport like this one to have anything to arbitrate"
    );
    assert!(
        writer
            .write_vectored(&[io::IoSlice::new(b"vectored")])
            .is_some()
    );
}

#[test]
fn a_completion_transport_needs_no_borrowed_write_path() {
    // Compiling is most of the assertion: `Completion` above never mentions
    // `write_borrowed`. The default still works, which is what lets a completion-based
    // implementation ignore the readiness fast path entirely.
    let (mut reader, mut writer) = Completion {
        written: Vec::new(),
        to_read: b"from the peer".to_vec(),
    }
    .split();

    let (read, buf) = block_on(reader.read(BytesMut::with_capacity(64)));
    assert_eq!(read.unwrap(), b"from the peer".len());
    assert_eq!(&buf[..], b"from the peer");

    assert!(
        writer.write_borrowed(b"to the peer").is_none(),
        "a transport that has not overridden the borrowed path must decline it, so the \
         driver coalesces and writes owned"
    );

    assert!(
        writer
            .write_vectored(&[io::IoSlice::new(b"to the peer")])
            .is_none(),
        "nor may the vectored path be elected by a transport that never mentioned it — a \
         completion transport must keep compiling untouched as strategies are added"
    );

    assert!(
        !writer.gathers_owned_regions(),
        "a transport that has not overridden the owned-region election must decline it too, \
         so the default is the coalescing owned write — the same additive guarantee the \
         borrowed and vectored paths give"
    );

    // The default `write_regions` is unreachable by contract — the driver only calls it
    // after `gathers_owned_regions` returns true — but it must still exist and be safe, so a
    // completion transport that overrides neither keeps compiling. It reports `Unsupported`
    // and, crucially, hands the regions straight back rather than dropping them.
    let regions = vec![Bytes::from_static(b"to the peer")];
    let (result, returned) = block_on(writer.write_regions(regions));
    assert_eq!(
        result.expect_err("the default declines").kind(),
        io::ErrorKind::Unsupported,
        "the default owned-region write must decline rather than pretend to have written"
    );
    assert_eq!(
        returned,
        vec![Bytes::from_static(b"to the peer")],
        "and it must return the regions untouched — losing owned buffers is exactly the \
         failure the split election exists to prevent"
    );

    let (written, _buf) = block_on(writer.write(Bytes::from_static(b"to the peer")));
    assert_eq!(written.unwrap(), b"to the peer".len());
    assert_eq!(writer.written, b"to the peer");
}

#[test]
fn a_transport_can_elect_the_owned_region_path() {
    // The owned-region election and write in isolation, exercised directly rather than
    // through the driver: this calls `gathers_owned_regions` and `write_regions` on the
    // transport itself, so it pins the *contract* those two owe each other, not the driver's
    // choice between them (precedence is pinned separately, driver-driven, below). The
    // transport happens to offer the vectored path as well, which is immaterial here because
    // nothing consults it. What matters is that the election is a plain predicate, read
    // without offering any regions and repeatable — and that it is separate from the write,
    // so a transport can never lose owned buffers by declining late the way a borrowed-path
    // transport may drop a borrowed slice.
    let region_writes = Rc::new(RefCell::new(0));
    let mut writer = OffersVectoredAndOwnedRegions {
        vectored_writes: Rc::new(RefCell::new(0)),
        region_writes: Rc::clone(&region_writes),
    };

    assert!(
        writer.gathers_owned_regions(),
        "the election is a plain predicate, read without offering any regions"
    );
    // Reading it again yields the same answer: it is a fixed property of the transport, not a
    // per-pass decision, and reading it has no side effect that a second read could disturb.
    assert!(
        writer.gathers_owned_regions(),
        "the election is stable across reads within a pass"
    );

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

#[test]
fn the_vectored_path_takes_precedence_over_the_owned_region_one() {
    // Precedence, pinned where it lives: inside the driver, over a real connection. The
    // earlier version of this test was circular — it called `write_vectored` itself and
    // checked the counters, so it asserted the *test's* belief about precedence rather than
    // the driver's rule. Reversing the driver's precedence would not have failed it. This one
    // hands a shared body to a transport that advertises *both* the vectored and the
    // owned-region path and lets the driver choose; the observation is the driver's, so a
    // reversed rule fails it. Verified by mutation: swapping the driver's two elections so
    // the owned-region one is read first makes this test fail.
    //
    // The rule: the vectored election is read first, the owned-region one only when it
    // declines. So the vectored path must carry every octet, and `write_regions` — the
    // owned-region *write* — must never run.
    let observed = drive_precedence(BODY, &[], PASSES);

    assert!(
        !observed.peer.is_empty(),
        "the driver produced nothing, so nothing was driven: {observed:?}",
    );
    assert!(
        observed.request_reached_peer(),
        "the peer never received the request body the vectored path should have carried: \
         {observed:?}",
    );
    assert_eq!(
        observed.region_writes, 0,
        "the owned-region write ran, so the driver did not give the vectored path precedence: \
         {observed:?}",
    );
    assert_eq!(
        observed.owned_region_elections, 0,
        "the driver consulted the owned-region election, so it did not stop at the vectored \
         one — the vectored election is meant to be read first and settle it: {observed:?}",
    );
    assert!(
        !observed.calls.is_empty(),
        "the vectored path performed no write, so it did not carry the traffic: {observed:?}",
    );
    assert!(
        observed.vectored_probes >= 1,
        "the driver never probed the vectored election, so no pass ran: {observed:?}",
    );
    // The vectored path carried the whole request: every octet the peer received came out of
    // a gathering write, and none out of `write_regions`.
    assert_eq!(
        observed.vectored_octets, observed.peer,
        "the octets the vectored path gathered are not the octets the peer received, so some \
         traffic took another path: {observed:?}",
    );
}

#[test]
fn the_owned_region_election_is_read_once_a_pass_not_once_a_write() {
    // Once-per-pass, pinned on the election this test is named for. The contract is that a
    // strategy election is consulted once a pass and its answer does not depend on the regions
    // later offered — never once per write.
    //
    // The shape matters. Over a transport advertising *both* paths the vectored election wins
    // and the owned-region one is never consulted at all, so a run there could say nothing
    // about it: a driver that re-read `gathers_owned_regions` before every `write_regions`
    // would sail through unnoticed. This drives an owned-region-only transport instead, where
    // that election is the one the driver actually reaches.
    //
    // The pass count comes from the vectored probe. The driver opens every pass by probing the
    // vectored election, and the harness counts that probe before the declining shape gates it
    // out — so on this transport `vectored_probes` *is* the number of passes, which turns the
    // claim from an inequality into an equality: exactly one owned-region election per pass.
    // A forced short write guarantees the write ran more often than the election, which is the
    // half that rules out a per-write election.
    let observed = drive_over(duplex_owned_regions(), BODY, &[SHORT_PREFIX], PASSES);

    assert!(
        observed.retries >= 1,
        "no short write was forced, so the once-per-pass claim rests on nothing: {observed:?}",
    );
    assert!(
        observed.region_writes > observed.owned_region_elections,
        "the owned-region write ran {} times against {} elections; without the write \
         outnumbering the election a per-write election would look identical: {observed:?}",
        observed.region_writes,
        observed.owned_region_elections,
    );
    assert_eq!(
        observed.owned_region_elections, observed.vectored_probes,
        "the owned-region election was read {} times across {} passes; it must be read exactly \
         once a pass, not once a write: {observed:?}",
        observed.owned_region_elections, observed.vectored_probes,
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

/// What one driven precedence run produced.
#[derive(Debug)]
struct PrecedenceRun {
    /// Every octet the peer half actually received, in order.
    peer: Vec<u8>,
    /// The region lengths of each gathering write the vectored path performed, retries
    /// included; empty if the vectored path never ran.
    calls: Vec<Vec<usize>>,
    /// Calls that re-offered the remainder of a short write rather than new octets.
    retries: usize,
    /// Every octet the vectored path gathered, concatenated in offer order.
    vectored_octets: Vec<u8>,
    /// Times the driver probed the vectored election — one construct-and-drop per pass.
    vectored_probes: usize,
    /// Times the driver read the owned-region election. Zero when vectored took precedence.
    owned_region_elections: usize,
    /// Times the owned-region write (`write_regions`) actually ran, retries included.
    region_writes: usize,
}

impl PrecedenceRun {
    /// Whether the peer received the request head and body — a `HEADERS` frame (type `0x1`)
    /// and the `4096` `x` octets the body is — so "the vectored path carried the traffic" is a
    /// claim about a real request having crossed, not an empty handshake.
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

/// Drives one shared-body upload over the both-advertising transport, hand-stepping the
/// connection against a silent peer so the write side is a self-contained, reproducible pass,
/// and reports which election the driver consulted and what each path carried.
///
/// The body is handed over (`handshake_shared`), so its `DATA` payload rides as its own region
/// — the arrangement the owned-region path exists for — and the choice between that path and
/// the vectored one is the driver's alone. `caps`, if any, force short writes so the
/// once-per-pass election can be told apart from a per-write one.
fn drive_precedence(body: usize, caps: &[usize], passes: usize) -> PrecedenceRun {
    drive_over(duplex_vectored_and_owned_regions(), body, caps, passes)
}

/// The same drive over a caller-chosen transport pair, so a test can pick which elections the
/// driver will actually face.
///
/// Split out because precedence and once-per-pass need *different* shapes to say anything. A
/// transport advertising both paths proves precedence but can never exercise the owned-region
/// election, since the vectored one wins and short-circuits it; pinning that election needs a
/// shape where it is the one the driver reaches.
fn drive_over(
    sides: (Duplex, Duplex),
    body: usize,
    caps: &[usize],
    passes: usize,
) -> PrecedenceRun {
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
        .uri("http://example.test/precedence")
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

    PrecedenceRun {
        peer,
        calls: vectored.calls(),
        retries: vectored.retries(),
        vectored_octets: vectored.octets(),
        vectored_probes: elections.vectored_probes(),
        owned_region_elections: elections.owned_region_elections(),
        region_writes: elections.region_writes(),
    }
}

#[test]
fn a_duplex_can_elect_the_owned_region_path() {
    // The in-memory harness counterpart: a duplex built for the owned-region shape advertises
    // the completion election and declines both readiness paths, so the driver has exactly one
    // strategy to take. This is what `http_shared_body.rs` drives a whole connection over.
    let (client, peer) = duplex_owned_regions();
    let (_reader, mut writer) = Transport::split(client);

    assert!(
        writer.gathers_owned_regions(),
        "an owned-region duplex advertises the completion election"
    );
    assert!(
        writer.write_vectored(&[]).is_none(),
        "and declines the vectored path, so precedence has nothing to arbitrate"
    );
    assert!(
        writer.write_borrowed(b"unused").is_none(),
        "and the borrowed path likewise"
    );

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

    let write = writer.write_borrowed(b"borrowed");
    assert!(
        write.is_some(),
        "a transport that overrides the borrowed path offers it, which is how the \
         connection chooses zero-copy over coalescing"
    );

    let written = block_on(write.expect("the borrowed path")).unwrap();
    assert_eq!(written, b"borrowed".len());
    assert_eq!(
        *writer.borrowed.borrow(),
        1,
        "the override should have been taken, not the default"
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
    let (client, server) = duplex(false);
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
    let (client, server) = duplex(false);
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
    let (client, server) = duplex(false);
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
    let (client, server) = duplex(true);
    let counter = client.write_counter();
    let (_reader, mut writer) = client.split();
    let (mut server_reader, _server_writer) = server.split();

    let write = writer.write_borrowed(b"borrowed");
    assert!(
        write.is_some(),
        "this shape offers the zero-copy write path"
    );

    block_on(async {
        write.expect("the borrowed path").await.unwrap();

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
