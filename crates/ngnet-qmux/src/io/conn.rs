//! The connection: ownership, the pump, and the operations a caller drives.
//!
//! # What this owns
//!
//! One byte stream, one clock, one [`Conn`], and the three buffers that stand between them:
//! the bytes read from the stream, the bytes waiting to be written -- which is where records
//! are serialised, rather than being serialised elsewhere and copied in -- and the events the
//! handlers recorded. Everything the module documentation calls "the loop written once" is in
//! [`Connection::pump`].
//!
//! # The pump order, which is the whole design
//!
//! **Produce up to the ceiling, write once, then read.** Records accumulate in the outbound
//! buffer and leave together, rather than each one being written before the next is built.
//!
//! This layer used to alternate strictly -- flush, produce one record, flush -- and the rule
//! that came out of it, "at most one record outstanding", was documented here and in
//! `docs/qmux/design.md` with three reasons. Coalescing keeps all three and pays for them
//! differently, because the alternation costs one write per record and a multiplexed pass
//! produces dozens of tiny ones: sixty-four concurrent exchanges with empty bodies issued
//! seventy writes averaging twenty-seven bytes each, where a buffer of any plausible size
//! merges them into three (`.paw/work/qmux-h3-perf/Phase2Screen.md`).
//!
//! - *Bounded memory* was the first reason, and the bound is now [`OUTBOUND_CEILING`] rather
//!   than one record. Production stops while the buffer cannot take another maximum-size
//!   record, so what a slow peer can make this side hold is a documented constant that does
//!   not depend on how much a caller offered or how slowly the peer reads.
//! - *Correct interleaving* was the second, and it holds for a weaker reason than the old
//!   rule gave: a record is appended to the buffer **whole and in order**, and none is begun
//!   before its predecessor is complete. What the peer must never see is a record interleaved
//!   with the tail of another, and appending finished records end to end cannot produce that.
//!   The old rule implied this one; it was never the only thing that could.
//! - *Exactly one place to resume from* was the third, and it is unchanged: `written` is a
//!   byte cursor into the buffer, and a partial accept resumes at a byte offset regardless of
//!   how many record boundaries lie behind it. What is new is that the offset may now fall
//!   *inside* a record rather than only at its end -- which the byte stream cannot tell apart,
//!   since it is handed a slice either way.
//!
//! Reading last, and in a particular order within the read, is what makes a peer's close
//! legible. The bytes go to [`RecordFramer`] *first* and to [`Conn::read`] *second*, and only
//! then is the outcome acted on. dwnx reports `PeerClosed` after consuming the close record,
//! possibly with more bytes still to come in the same chunk; feeding the framer first means
//! the close record is already latched when that report arrives, so the kind, code, frame type
//! and reason can be decoded out of it. Feeding the state machine first and the framer
//! afterwards would work too -- but only by accident, and only until someone reordered two
//! lines that look independent.
//!
//! A read is followed by one more write pass when it left something to say, so a window
//! extension or a ping response provoked by what just arrived leaves in the same wakeup
//! instead of waiting for the next one. It is an extra turn of the same crank, not an
//! exception to it.
//!
//! # Two kinds of flush, and why the distinction is the whole of the gain
//!
//! A *forced* flush offers the buffer until the byte stream has taken all of it or refused.
//! An *opportunistic* one offers it only when it can no longer take another record -- to make
//! room, not to empty it.
//!
//! Every public entry point here ends with a forced flush, because the caller may stop polling
//! the moment it returns and output held back for a pass that never comes is a stalled
//! connection rather than a slow one. The single exception is
//! [`poll_pump_buffered`](Connection::poll_pump_buffered), which exists for a caller that is
//! *mid-turn* -- the HTTP/3 join offers this connection up to sixty-four times before
//! returning to its driver, and pumps in between so the pass can move more than one record.
//! Were that pump to flush, a driver turn would still pay one write per record and coalescing
//! would achieve nothing while every test inside this file still passed. Its contract is that
//! the caller ends its turn with [`poll_pump`](Connection::poll_pump) or an ending path.
//!
//! # What a partial accept leaves behind, and why nothing reclaims it
//!
//! Once part of the buffer has been accepted, the space at its front is free but unreachable:
//! production appends at the back, and `written` is the only cursor. The buffer therefore
//! stops taking records while its *tail* is short, even though its head is empty -- **stop
//! early**.
//!
//! Two alternatives were considered. *Compacting* -- moving the unwritten remainder to the
//! front -- costs a memcpy on the path a later commit exists to remove a memcpy from, and buys
//! room only in the case where the peer is already the bottleneck. *Wrapping* -- treating the
//! buffer as a ring of two regions -- costs the write side its single contiguous slice, which
//! [`AsyncByteStream`] has no way to accept as two. Stopping early is the simplest of the
//! three, and the case it degrades is the case where the peer is not keeping up, where a
//! connection that produced *less* is doing the right thing anyway.
//!
//! The consequence is worth stating in one place because a later question turns on it: the
//! output of this connection is **always one contiguous region**, so a gathering write -- one
//! that hands the byte stream several buffers at once -- would have nothing to gather here.
//! Only the ring would have produced two regions, and the ring is what was rejected.
//!
//! # A record is serialised where it will be sent from
//!
//! There is no staging buffer. [`Conn::record`] is handed a slice of the outbound buffer
//! itself, so the bytes dwnx writes are already in the place the byte stream will be offered
//! them, and the memcpy that used to move a finished record out of a scratch buffer and into
//! the queue is gone -- one per record, of up to [`MAX_RECORD`] bytes, so about a megabyte of
//! copying per megabyte sent.
//!
//! **The buffer is held at its full length, with `filled` as the fill cursor.** That is the
//! arrangement, and the reason for it is that the destination has to be *initialised* memory:
//! `crates/ngnet-qmux/tests/invariants.rs` forbids `unsafe` anywhere under `src/io/`, so
//! writing into a `Vec`'s spare capacity -- the obvious way to serialise into a buffer's tail
//! -- is not available here, since reaching that capacity as a slice needs
//! `set_len` or `spare_capacity_mut` and a promise about initialisation that only `unsafe` can
//! make. Zeroing the buffer once, on the growth that first needs it, and tracking how much of
//! it means something is the safe form of the same thing: the zeroing is paid per connection
//! rather than per record, and `outbound.len()` stops being the interesting quantity. What the
//! queue holds is `outbound[..filled]`; what is still to send is `outbound[written..filled]`;
//! what is past `filled` is scratch space that no reader ever sees.
//!
//! **The slice handed to the record writer is exactly one record wide, never the whole tail.**
//! This is the part that would corrupt the wire in silence rather than fail, so it is stated
//! here as well as at `Connection::produce_within`, which enforces it. dwnx does not cap a
//! record on the write path: `dwnx_qre_start` initialises the record with the whole buffer it
//! is given (`deps/dwnx/lib/dwnx_qre.c:36-41`), `dwnx_qre_stream_max_datalen` bounds a
//! payload only by what is left of that buffer (`:47-80`), and `dwnx_qre_final` writes the
//! record's length as a **fixed two-byte varint** (`:107`) whose encoder asserts the value is
//! below 16384 and, where that assertion is compiled out, truncates it to sixteen bits
//! (`deps/dwnx/lib/dwnx_conv.c:145-157`) -- a record whose declared length is nothing like its
//! real one, and a peer that has lost record framing from that byte onward. Nothing in
//! [`Conn::record`]'s own contract stops it; a buffer of 64 KiB is a perfectly legal argument
//! that produces an illegal record.
//!
//! How that failure presents was checked rather than assumed, because the two answers call for
//! different guards. `crates/ngnet-qmux-sys/build.rs` does not define `NDEBUG` and neither does
//! `cc` on its behalf, so as *this* workspace builds dwnx the assertion holds in the release
//! profile as well as the debug one: handing the writer the whole tail aborts both builds, and
//! it was tried both ways to find that out. The truncation is what the same mistake would do
//! against a dwnx built with assertions off, which is an ordinary way to build C and not a
//! hypothetical. So the guard in `tests/io_writes.rs` asserts the property on the wire -- no
//! record longer than its length prefix can describe -- rather than relying on either
//! behaviour, and this comment states which one this workspace actually has.
//!
//! The rule is therefore "never more than one maximum record", and deliberately not "always
//! exactly one": `Connection::room_for` still hands a *shorter* slice to a record continuing
//! an offer whose remainder fits, which is what keeps a body's last few bytes from travelling
//! alone. Short is safe in the direction that matters, because the record can only be smaller
//! than the two-byte length can describe.
//!
//! **What decides that the tail is too short is arithmetic on the cursors, done before a
//! record is begun.** It is not [`Record::BufferTooSmall`](crate::Record::BufferTooSmall), and
//! the difference is not stylistic: that error fires only below three bytes
//! (`crate::write`'s `MIN_USABLE_BUFFER`), and `Connection::produce_within` reports it as a
//! record of zero bytes with a `Packed` verdict, which `Connection::write_side` reads as
//! "the state machine has nothing queued" and answers by clearing `produce_pending`. A
//! connection with output to send would stop producing it and nothing would say so. So the
//! question "is there room" is answered by `Connection::room_for_record` and
//! `Connection::room_for`, both of which compare cursors against
//! [`OUTBOUND_CEILING`] and neither of which needs a record to have been started to answer.
//!
//! # One call fills records until a bound stops it
//!
//! [`Connection::try_write_stream`] does not stop at a record. It keeps producing for the same
//! offer while the buffer has room and the stream has both bytes and credit, and reports the
//! total. Stopping at a record was the obvious reading of "produce one record" and it was
//! wrong for a reason that is invisible from this file: the caller above is told a *count* and
//! nothing else, and a count short of the offer is the only signal it has for congestion, so
//! it stands the stream down for the rest of its pass. Every large offer answered short, and a
//! stream with a megabyte to send moved one record of it per pass while the buffer sat four
//! fifths empty. Leaving the resumption to that caller instead was rejected because it cannot
//! tell a filled record from a shut window, and re-offering into a shut window spins.
//!
//! The relaxation this needs is in `Connection::room_for`, and it is stated there:
//! a record *continuing* an offer may be built into less than a full reserve when what is left
//! of the offer is smaller than the space, so the last few bytes of a body are not stranded to
//! travel alone in the pass's closing flush.
//!
//! # A push error is fatal, and never retried
//!
//! [`RecordWriter::push`](crate::RecordWriter::push) returning an error drops the writer
//! mid-record. `Drop` finalises the record so dwnx is not left writing through a retained
//! pointer -- that much is safe -- but the produced bytes are discarded, and if the record had
//! already packed stream data then dwnx has *already advanced that stream's send offset*. The
//! bytes are gone and the peer will see a gap it can never fill.
//!
//! So a failed production ends the connection. Retrying the write would send the next chunk at
//! an offset the peer cannot reconcile, which presents as a stream that stalls rather than as
//! an error, and is the most expensive of the failures in this file to diagnose after the
//! fact.
//!
//! # Waiting, and never spinning
//!
//! Three things here cannot proceed on demand: an open the peer's stream limit forbids, a
//! write with no flow-control credit, and a read the caller has not made room for. Each parks
//! against the event that ends it -- a raised limit, an extended window, credit reported back
//! -- through [`Signals`], and none of them wakes itself. See that module for what a self-wake
//! costs and which callback fires which slot.
//!
//! [`try_write_stream`](Connection::try_write_stream) is the deliberate exception: it reports
//! [`StreamWrite::Blocked`] and returns, because the caller it exists for has no [`Context`]
//! to park with.
//!
//! An idle connection therefore arms nothing at all. It has no outbound bytes, so it never
//! offers the byte stream a write; it polls one read, which registers the byte stream's own
//! waker and returns pending; and there is no timer here to fire in the meantime. The next
//! poll happens when the peer says something, and not before.
//!
//! # How much this reads ahead of its caller
//!
//! [`ReadAhead`] bounds bytes *delivered and not yet credited back*, and the pump stops
//! reading from the byte stream while that figure is at the bound. Backpressure then flows
//! where it belongs: bytes pile up in the transport, the peer's own window closes, and the
//! sender stops. Only [`extend_connection_credit`](Connection::extend_connection_credit)
//! moves the figure; the scheduling module explains why counting the stream-level extension
//! as well would make the bound meaningless.
//!
//! # How a connection ends
//!
//! Every ending is latched the first time it is observed, and every later operation reports
//! the same one. There are five, and a caller can tell them apart because they are what
//! [`ErrorKind`] separates: the byte stream failed, it ended between records, it ended partway
//! through a record, the peer violated the protocol, or one of the two endpoints closed
//! deliberately. Only the first report carries the underlying cause as its source -- a boxed
//! error cannot be cloned -- so a caller who wants the transport's own message should keep the
//! first error rather than the last.

use core::task::{Context, Poll};
use std::sync::Arc;

use crate::ccerr::CloseReason;
use crate::conn::{Conn, ReadOutcome, Role};
use crate::error::Error as CoreError;
use crate::handlers::Handlers;
use crate::io::clock::Clock;
use crate::io::close::encode_close_record;
use crate::io::error::{Error, ErrorKind, Result};
use crate::io::event::{Event, EventQueue};
use crate::io::framing::RecordFramer;
use crate::io::scheduling::{ReadAhead, Signals};
use crate::io::stream::{AsyncByteStream, Written};
use crate::params::TransportParams;
use crate::settings::Settings;
use crate::stream::{Directionality, StreamId};
use crate::stream_io::{OpenOutcome, Shutdown};
use crate::time::{Duration, Timestamp};
use crate::write::{Push, WriteRequest};

/// How many bytes the peer may send on any one stream before waiting for credit.
///
/// The same value for all three of the state machine's per-stream limits. Chosen to match
/// `ngnet-quic`'s equivalent, because the question -- how much in flight per stream is worth
/// buffering -- has nothing to do with which transport carries it.
pub const DEFAULT_STREAM_DATA: u64 = 256 * 1024;

/// How many bytes the peer may send across all streams before waiting for credit.
pub const DEFAULT_CONNECTION_DATA: u64 = 1024 * 1024;

/// How many streams of each kind the peer may open.
pub const DEFAULT_MAX_STREAMS: u64 = 100;

/// How many bytes may be delivered to the caller before it must report consuming some.
///
/// The same figure as [`DEFAULT_CONNECTION_DATA`], and deliberately: the connection window is
/// how much the peer may send before this side says anything, so a layer that held less than
/// that would refuse to read bytes the protocol has already permitted -- stalling a caller
/// that credits in batches rather than per event, which is a legitimate and common shape. A
/// larger figure would let the layer hold more than the protocol ever puts in flight, which
/// buys nothing and costs memory.
pub const DEFAULT_READ_AHEAD: u64 = DEFAULT_CONNECTION_DATA;

/// The size of the read buffer, and so the most bytes one read may deliver.
///
/// One record. A larger buffer would let a single read straddle several records, which is
/// harmless -- both the framer and the state machine accept any split -- but buys nothing,
/// since a record is the unit at which anything becomes actionable.
pub(super) const READ_BUFFER: usize = MAX_RECORD;

/// How many retired read buffers a connection watches for reuse.
///
/// A buffer is retired while a caller still holds a delivery cut from it, and becomes reusable
/// when that delivery is dropped. Watching costs a pointer's worth of book-keeping each; the
/// memory is the delivery's either way, so a buffer that falls off this list is not leaked and
/// not held -- it is simply not reused, and is freed when the last view of it goes.
///
/// Eight, which is more than a caller reading its deliveries promptly ever reaches -- such a
/// caller retires nothing and the list stays empty -- and enough that a caller holding a few
/// passes' worth still finds recycled buffers rather than allocating. It bounds what a
/// connection holds in *watched* buffers at eight records, about 128 KiB, and that ceiling is
/// reached only by a caller that is holding at least that much delivered data of its own.
const READ_POOL_LIMIT: usize = 8;

/// The most bytes one produced record can occupy, prefix included.
///
/// This is a property of the buffer a record is serialised into rather than an assertion about
/// dwnx: the record writer is handed a slice of the outbound buffer exactly this long, and it
/// cannot write past what it was given. It is what the reserve in [`OUTBOUND_CEILING`] is for,
/// and the arithmetic below is only sound because it is an upper bound on a single record
/// rather than a typical size.
///
/// It is also the *largest* slice a record may be built into, and not merely a convenient one.
/// dwnx fills whatever buffer it is handed and then describes the result with a two-byte
/// length; a longer slice is how a record gets produced that the length cannot describe. The
/// module documentation has the citations.
const MAX_RECORD: usize = crate::DEFAULT_MAX_RECORD_SIZE as usize;

/// How many bytes one write is **guaranteed** to be able to carry.
///
/// The quantity to reason about when asking how many writes a transfer costs: a connection
/// with this much or more to send offers at least this much in a single call, so P bytes of
/// output cost at most P divided by this figure, rounded up, writes. It is not the same
/// quantity as [`OUTBOUND_CEILING`], and conflating the two is how a bound gets stated that
/// nothing can rely on -- the ceiling includes a reserve that a record already in progress may
/// consume, so it is not available to be carried.
///
/// **64 KiB, and the reduction that buys was predicted before it was measured.** Against the
/// write counts a driver turn issues today, this value removes 96% of them at concurrency 64
/// with an empty body, 79% with a 64 KiB body, and 75% at the worst point measured -- a
/// megabyte across sixty-four streams, where the floor is arithmetic (bytes divided by the
/// carry) rather than anything about the design. A larger carry moves the last of those to
/// 94% at 256 KiB and 99.9% unbounded; it was not taken, because the first two points are
/// where the small writes are and a 64 KiB carry bounds the buffer at four records rather than
/// at a megabyte. The figures are the capacity sweep in `.paw/work/qmux-h3-perf/Phase2Screen.md`.
///
/// Stated in advance deliberately: the arithmetic above is satisfied by *any* value, including
/// one that turns sixty-five writes into thirty-two and is indistinguishable from none at all,
/// so a bound with no predicted reduction beside it is a bound that cannot be judged.
pub const OUTBOUND_CARRY: usize = 64 * 1024;

/// The most memory one connection may hold in produced-but-unwritten output.
///
/// [`OUTBOUND_CARRY`] plus one maximum record. The reserve is what makes the carry a
/// *guarantee*: a record is begun only while the buffer still has a whole record's room, so
/// the last one started can always be finished, and the buffer ends at most this long.
///
/// The figure does not depend on how much a caller offers or on how slowly the peer reads,
/// which is the property the old one-record rule was defending. What it costs is per
/// connection and paid whether or not the peer is slow, since the buffer is reused rather than
/// released: about 80 KiB against the 16382 bytes the alternation held. The buffer grows on
/// demand rather than being allocated at construction -- an idle connection, and one whose
/// peer keeps up, never reaches the ceiling and never pays for it, and the growth is amortised
/// over the life of a connection that does.
///
/// It bounds the buffer's *length*, and since records are serialised into that length rather
/// than appended to it, the length is grown exactly to what a record needs rather than by
/// doubling. A doubling growth would put the capacity above this figure while the queue
/// obeyed it, which is a bound on the wrong quantity: what a slow peer makes this side hold is
/// the memory, not the cursor.
pub const OUTBOUND_CEILING: usize = OUTBOUND_CARRY + MAX_RECORD;

/// What a connection advertises to its peer.
///
/// # Why this exists rather than [`TransportParams`]
///
/// Because [`TransportParams::new`] is all zeros. It reproduces `dwnx_transport_params_default`
/// faithfully and is documented as doing so, which is the right choice for a binding: "the
/// defaults" there means the library's defaults, not a second set invented in Rust. But a
/// connection built from them can open no streams and carry no data -- it has advertised
/// permission for none -- and it fails by hanging rather than by complaining.
///
/// A layer whose job is to be usable cannot inherit that. So this supplies working values of
/// its own, exactly as `ngnet-quic`'s endpoint configuration does for ngtcp2, and
/// `Config::default()` is a configuration that transfers data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    initial_max_stream_data: u64,
    initial_max_data: u64,
    max_streams_bidi: u64,
    max_streams_uni: u64,
    max_idle_timeout: Duration,
    read_ahead: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            initial_max_stream_data: DEFAULT_STREAM_DATA,
            initial_max_data: DEFAULT_CONNECTION_DATA,
            max_streams_bidi: DEFAULT_MAX_STREAMS,
            max_streams_uni: DEFAULT_MAX_STREAMS,
            // Zero means "no idle timeout", and it is the honest value: nothing in dwnx or in
            // this layer enforces one, so advertising a number would invite the peer to
            // believe in a deadline that nobody is keeping. See [`crate::io::Clock`] for why
            // there is no timer here to keep it with.
            max_idle_timeout: Duration::from_nanos(0),
            read_ahead: DEFAULT_READ_AHEAD,
        }
    }
}

impl Config {
    /// The defaults, which are working values rather than the state machine's zeros.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many bytes the peer may send on each stream before waiting for credit.
    ///
    /// Maps onto all three of the state machine's per-stream limits at once --
    /// `initial_max_stream_data_bidi_local`, `_bidi_remote` and `_uni` -- because
    /// distinguishing them is a tuning decision this layer has no opinion about. A caller who
    /// wants them separate builds [`TransportParams`] directly and drives the state machine.
    #[must_use]
    pub const fn initial_max_stream_data(mut self, bytes: u64) -> Self {
        self.initial_max_stream_data = bytes;
        self
    }

    /// How many bytes the peer may send across all streams before waiting for credit.
    ///
    /// Setting this below [`Config::initial_max_stream_data`] lets one stream exhaust the
    /// whole connection window, which is a legitimate thing to want and an easy thing to do by
    /// accident.
    #[must_use]
    pub const fn initial_max_data(mut self, bytes: u64) -> Self {
        self.initial_max_data = bytes;
        self
    }

    /// How many bidirectional streams the peer may open.
    #[must_use]
    pub const fn max_streams_bidi(mut self, count: u64) -> Self {
        self.max_streams_bidi = count;
        self
    }

    /// How many unidirectional streams the peer may open.
    #[must_use]
    pub const fn max_streams_uni(mut self, count: u64) -> Self {
        self.max_streams_uni = count;
        self
    }

    /// How long the connection may sit idle, as advertised to the peer.
    ///
    /// Advertised and **not enforced**, in either direction: dwnx validates this parameter,
    /// encodes it, and has no code path that ends a connection for being idle. Setting it
    /// tells a peer that does enforce one what this side would tolerate; it does not give this
    /// side a timeout. A caller who needs liveness detection applies a deadline around the
    /// operation they are awaiting, or gets it from the substrate.
    #[must_use]
    pub const fn max_idle_timeout(mut self, timeout: Duration) -> Self {
        self.max_idle_timeout = timeout;
        self
    }

    /// How many bytes may be delivered to the caller before it must report consuming some.
    ///
    /// This layer's own bound on what it holds on the caller's behalf, and **not** a
    /// restatement of the protocol's receive window. It counts bytes handed over by
    /// [`Connection::poll_next_event`] that
    /// [`Connection::extend_connection_credit`] has not yet accounted for, so a caller that
    /// drains events into a buffer of its own without crediting them is stopped at this figure
    /// however diligently it drains. The number applies before any credit has been extended;
    /// afterwards each credited byte buys another byte of read-ahead.
    ///
    /// Setting it to zero means the layer reads nothing until the caller credits, which is a
    /// coherent configuration for a caller that wants to pull explicitly, and a deadlock for
    /// one that expects data to arrive unprompted.
    #[must_use]
    pub const fn read_ahead(mut self, bytes: u64) -> Self {
        self.read_ahead = bytes;
        self
    }

    /// The transport parameters this configuration describes.
    fn transport_params(self) -> TransportParams {
        TransportParams::new()
            .with_initial_max_stream_data_bidi_local(self.initial_max_stream_data)
            .with_initial_max_stream_data_bidi_remote(self.initial_max_stream_data)
            .with_initial_max_stream_data_uni(self.initial_max_stream_data)
            .with_initial_max_data(self.initial_max_data)
            .with_initial_max_streams_bidi(self.max_streams_bidi)
            .with_initial_max_streams_uni(self.max_streams_uni)
            .with_max_idle_timeout(self.max_idle_timeout)
    }
}

/// What a non-parking write did.
///
/// The answer [`Connection::try_write_stream`] gives, for a caller that has no
/// [`Context`] to park with. It is this layer's own type: the state machine's
/// [`Push`](crate::Push) describes the state of a *record being built* and invites another
/// push, which is a conversation only the code inside this file is in a position to have.
/// Exposing it would put dwnx's record-building protocol into the signature of every layer
/// above.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StreamWrite {
    /// This many bytes were taken, counted from the front of what was offered.
    ///
    /// May be fewer than offered, and when it is, the shortfall is **backpressure**: either the
    /// peer's flow-control window is exhausted or the connection's output buffer has no room
    /// for a further record. It is not a record filling -- one call fills as many records as
    /// the buffer will hold -- so a caller may treat a short count as a reason to stand this
    /// stream down rather than as an invitation to offer the remainder immediately. The
    /// remainder is not lost and not sent: offer it again. A zero here means the offer was
    /// empty -- an end-of-stream marker carrying no data is accepted this way.
    Accepted(usize),

    /// Nothing was taken, and nothing will be until something changes.
    ///
    /// Either the peer's flow-control credit for this stream is exhausted and it must extend
    /// the window, or the connection's output buffer cannot take another record until the byte
    /// stream has taken some of what is already in it. A caller offers the same bytes again
    /// after the connection has been pumped.
    ///
    /// The second cause is a *bound being reached*, not a record being outstanding: records
    /// accumulate and leave together, and this answer arrives only once the accumulated output
    /// is within one record of [`OUTBOUND_CEILING`]. A caller that met it on every second
    /// offer under the old rule will meet it once in several dozen now.
    Blocked,

    /// The stream's write side is closed, so nothing will ever be taken.
    ///
    /// Distinct from [`StreamWrite::Blocked`] because retrying is pointless: the stream was
    /// finished, reset, or the peer asked this side to stop. A caller should abandon what it
    /// was sending rather than wait.
    Closed,
}

/// An asynchronous QMux connection over a caller-supplied byte stream.
///
/// Created from a byte stream the caller has **already established**; this crate connects
/// nothing and listens for nothing, which is why there is no third constructor. See the
/// [module documentation](super) for why the layer stops there.
///
/// Neither the byte stream nor the clock carries a `Send` bound, so a connection is `Send`
/// exactly when the caller's own values are.
pub struct Connection<S: AsyncByteStream, C: Clock> {
    stream: S,
    clock: C,
    conn: Conn<'static>,
    events: EventQueue,
    framer: RecordFramer,
    /// The buffer being read into.
    ///
    /// Reference-counted because a delivery is handed out as a view of it rather than as a copy
    /// of its bytes, so the connection may read into it again only once every such view has
    /// been dropped -- which is exactly what the strong count returning to one says. A
    /// connection whose caller drops each delivery before the next arrives keeps this one
    /// buffer for its whole life, as it did when the delivery was a copy.
    inbound: Arc<Vec<u8>>,
    /// Read buffers that were retired while a delivery still aliased them.
    ///
    /// Watched rather than owned outright: an entry is reusable once its last view is dropped,
    /// and nothing here forces that to happen. What bounds the set is [`READ_POOL_LIMIT`] and
    /// not the caller's behaviour -- a buffer that would be the ninth is simply not watched,
    /// and its memory is freed by the delivery holding it rather than retained here. That is
    /// what makes a caller holding deliveries unable to stall the reader: there is always a
    /// buffer to read into, reused if one is free and fresh if none is.
    spare: Vec<Arc<Vec<u8>>>,
    /// Produced record bytes on their way to the byte stream.
    ///
    /// Whole records, serialised in the order they were produced, and never holding more than
    /// [`OUTBOUND_CEILING`] bytes of them -- a bound this side enforces by refusing to begin a
    /// record the buffer's tail could not hold in full. Reused for the life of the connection
    /// and grown on demand, so a connection whose peer keeps up never allocates the ceiling.
    ///
    /// **Its length is not how much it holds.** The buffer is kept at full length so that its
    /// tail is initialised memory a record can be serialised straight into; `filled` is what
    /// says how much of it means anything. See the module documentation for why the
    /// alternative -- writing into a `Vec`'s spare capacity -- is not available under a rule
    /// that forbids `unsafe` here.
    outbound: Vec<u8>,
    /// How much of `outbound` holds produced record bytes.
    ///
    /// The queue's real length, and the quantity every bound and every emptiness test is
    /// stated over. Bytes past it are the initialised scratch space the next record is built
    /// in, and no reader of this connection is ever shown them.
    filled: usize,
    /// How much of `outbound` the byte stream has already accepted.
    ///
    /// The single place a partial accept resumes from, and the reason accumulating several
    /// records needs no other bookkeeping. It may now sit *inside* a record as well as between
    /// two of them; the byte stream cannot tell the difference, because what it is offered is
    /// `outbound[written..filled]` either way. The bytes in front of it are dead space until
    /// the buffer empties -- see the module documentation for why nothing reclaims them.
    written: usize,
    /// How many bytes have been copied into `outbound` rather than serialised into it.
    ///
    /// Zero for every record: [`Conn::record`] is handed the buffer's own tail, so a record's
    /// bytes are never moved after they are written. What still counts here is the encoded
    /// connection close, which arrives as an owned buffer from
    /// [`encode_close_record`](crate::io::close::encode_close_record) and is copied in -- once
    /// per connection, at its end, on a path where one more memcpy of a few dozen bytes buys
    /// nothing to remove.
    ///
    /// Exposed through [`Connection::copied_record_bytes`], and gated for the same reason
    /// [`RecordFramer::copied_bytes`] is: a counter that is present in one build of a
    /// benchmark comparison and absent from the other measures the instrument. See that
    /// accessor for why `cfg(debug_assertions)` is the gate rather than `cfg(test)` or a
    /// feature.
    #[cfg(debug_assertions)]
    copied: usize,
    /// Whether the state machine may have something to serialise.
    ///
    /// Set at construction -- which is what makes the transport-parameter announcement leave
    /// unprompted -- and again after every read and every operation that queues a frame.
    produce_pending: bool,
    /// The wakers parked by an operation that cannot proceed yet.
    signals: Signals,
    /// How far the layer has read ahead of the caller, and how far it may.
    read_ahead: ReadAhead,
    closing: Option<Closing>,
    terminal: Option<Terminal>,
}

/// How far a local close has got.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Closing {
    /// The close record is in the outbound buffer.
    Queued,
    /// It has reached the byte stream; the write side is being shut down.
    Written,
    /// The write side is down and the close is complete.
    Complete,
}

/// A latched ending.
///
/// Holds what can be reproduced on every later operation. The source of the original failure
/// is not here, because a `Box<dyn Error>` cannot be cloned and the alternative -- handing the
/// same box out once and nothing afterwards -- would make the error a caller sees depend on
/// how many times they asked.
#[derive(Debug)]
struct Terminal {
    kind: ErrorKind,
    context: &'static str,
    close: Option<CloseReason>,
}

impl Terminal {
    fn error(&self) -> Error {
        let error = Error::new(self.kind, self.context);
        match &self.close {
            Some(close) => error.with_close(close.clone()),
            None => error,
        }
    }
}

/// What one record's production achieved.
struct Produced {
    /// How many bytes of the offered payload went into the record.
    consumed: usize,
    /// How many record bytes were written into the outbound buffer.
    bytes: usize,
    verdict: Verdict,
}

/// The stream-level answer a production came back with.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    /// The record was built; `consumed` says how much of the payload it took.
    Packed,
    /// The nominated stream is flow-control blocked.
    Blocked,
    /// The nominated stream's write side is closed.
    Closed,
}

/// What a write pass does with what it produced.
///
/// The two halves of the flush policy the module documentation sets out. Passed rather than
/// inferred, because the difference cannot be worked out from inside: whether output may wait
/// depends on whether the *caller* is coming back, which only the caller knows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flush {
    /// Write only to make room, leaving the rest to accumulate.
    ///
    /// For a caller still in the middle of its turn. Output left here is not abandoned: the
    /// same caller finishes with [`Flush::Everything`], and until then every further record it
    /// produces joins what is already waiting -- which is the whole of the write-count
    /// reduction.
    WhenFull,
    /// Write everything before returning to the caller.
    ///
    /// For every path a caller can stop polling from. Output held past one of these would wait
    /// for a pass that nothing is obliged to make.
    Everything,
}

impl<S: AsyncByteStream, C: Clock> Connection<S, C> {
    /// A client connection over an established byte stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the state machine rejects the configuration or cannot allocate.
    pub fn client(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::new(Role::Client, stream, clock, config)
    }

    /// A server connection over an established byte stream.
    ///
    /// # Errors
    ///
    /// As [`Connection::client`].
    pub fn server(stream: S, clock: C, config: Config) -> Result<Self> {
        Self::new(Role::Server, stream, clock, config)
    }

    fn new(role: Role, stream: S, clock: C, config: Config) -> Result<Self> {
        let events = EventQueue::new();
        let signals = Signals::new();
        let conn = Conn::builder(role)
            // Starting the connection's clock where the caller's clock is, rather than at
            // zero, so every interval dwnx computes is an interval in the caller's timescale.
            .settings(Settings::new().with_initial_timestamp(clock.now()))
            .transport_params(config.transport_params())
            .handlers(handlers(&events, &signals))
            .build()?;

        Ok(Self {
            stream,
            clock,
            conn,
            events,
            framer: RecordFramer::new(),
            inbound: Arc::new(vec![0; READ_BUFFER]),
            spare: Vec::new(),
            outbound: Vec::new(),
            filled: 0,
            written: 0,
            #[cfg(debug_assertions)]
            copied: 0,
            // The announcement. Nothing can be opened until the peer's parameters arrive, and
            // they arrive only if the peer sent them -- so both sides must speak without being
            // spoken to, or two connections wait for each other and neither reports anything
            // wrong. Scheduling it here means the first pump emits it, whatever the first
            // entry point turns out to be.
            produce_pending: true,
            signals,
            read_ahead: ReadAhead::new(config.read_ahead),
            closing: None,
            terminal: None,
        })
    }

    /// Which side of the connection this is.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.conn.role()
    }

    /// The current time, from the caller's clock.
    #[must_use]
    pub fn now(&self) -> Timestamp {
        self.clock.now()
    }

    /// The timestamp of the most recent operation, as the state machine recorded it.
    ///
    /// A reading of the caller's own clock, not of a second one: every call this layer makes
    /// into the state machine passes [`Clock::now`] straight through, so this is a value the
    /// caller's clock produced.
    #[must_use]
    pub fn timestamp(&self) -> Timestamp {
        self.conn.timestamp()
    }

    /// The peer's transport parameters, once they have arrived.
    #[must_use]
    pub fn peer_transport_params(&self) -> Option<&TransportParams> {
        self.conn.peer_transport_params()
    }

    /// Drives the connection: produce what is pending, write it out, read what has arrived.
    ///
    /// Every other entry point does this first, so a caller never has to. It is public because
    /// a caller who is neither reading events nor writing -- one waiting on something else
    /// entirely -- still has to let the connection make progress.
    ///
    /// [`Poll::Ready`] means everything produced has reached the byte stream. [`Poll::Pending`]
    /// means bytes are still queued and the byte stream cannot take them yet; the waker fires
    /// when it can.
    ///
    /// This is the *forced* half of the flush policy: nothing produced is left waiting for a
    /// later call. See [`Connection::poll_pump_buffered`] for the other half, and the module
    /// documentation for why there are two.
    ///
    /// # Errors
    ///
    /// Reports whichever ending the connection reached, including the orderly ones; see
    /// [`ErrorKind::is_orderly`].
    pub fn poll_pump(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.pump(cx, Flush::Everything) {
            return Poll::Ready(Err(error));
        }
        if self.filled == 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// Drives the connection, but writes only to make room.
    ///
    /// For a caller in the middle of a turn that will make further offers and then finish with
    /// [`Connection::poll_pump`]. The HTTP/3 join is the caller this exists for: it offers this
    /// connection up to sixty-four times before returning to its driver, and pumps in between
    /// so that a large body moves more than one record per wakeup. If that pump wrote
    /// everything, the turn would pay one write per record and the accumulation this layer
    /// does would be worth nothing -- which is a failure that no test of the connection alone
    /// can see, because the connection alone would still be coalescing correctly.
    ///
    /// [`Poll::Ready`] means the connection can take another record. [`Poll::Pending`] means
    /// the accumulated output is within one record of [`OUTBOUND_CEILING`] and the byte stream
    /// would not take enough of it to make room; the waker fires when it will. That is a
    /// different question from [`Connection::poll_pump`]'s, and it is the one an offer loop
    /// needs answered: an offer made into a full buffer collects nothing but
    /// [`StreamWrite::Blocked`].
    ///
    /// **The caller owes a forced flush.** Output accumulated here waits for the next pass, and
    /// nothing else is obliged to make one; a caller that ends its turn on this call strands
    /// whatever it produced. Ending the turn with [`Connection::poll_pump`],
    /// [`Connection::poll_close`] or [`Connection::poll_finish`] discharges the obligation.
    ///
    /// # Errors
    ///
    /// As [`Connection::poll_pump`].
    pub fn poll_pump_buffered(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.pump(cx, Flush::WhenFull) {
            return Poll::Ready(Err(error));
        }
        if self.room_for_record() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    /// How many produced bytes are waiting for the byte stream.
    ///
    /// Never more than [`OUTBOUND_CEILING`], which is the bound this layer offers in place of
    /// the "one record outstanding" it used to keep. Exposed because that bound is a promise
    /// about a connection's memory under a slow peer, and a promise nothing can observe is one
    /// nothing can hold this layer to.
    ///
    /// Counts the bytes the byte stream has already accepted as well as the ones it has not,
    /// because both occupy the buffer: the space in front of the cursor is not reused until the
    /// buffer empties. That makes this the figure the ceiling is stated over rather than the
    /// smaller "still to send" one.
    #[must_use]
    pub fn queued_output(&self) -> usize {
        self.filled
    }

    /// How many bytes this connection has copied on its way to the byte stream.
    ///
    /// Zero for a connection that has only sent records, however many it sent, because a
    /// record is serialised into the outbound buffer rather than into a staging buffer that is
    /// then copied into it. What moves this is the encoded connection close, which is a few
    /// dozen bytes once per connection. It used to grow by a whole record per record, which is
    /// what this exists to make a test rather than a claim.
    ///
    /// Gated exactly as [`RecordFramer::copied_bytes`] is, and for exactly its reason: the
    /// figure this counter is evidence about is compared between two benchmark builds, and a
    /// counter compiled into one of them and not the other is measured along with the code.
    /// `cfg(test)` cannot be the gate -- this crate's integration tests are separate
    /// compilation units and would not see it -- and a cargo feature cannot either, since a
    /// feature off by default is invisible to the verification commands while one on by
    /// default is in the benchmark build. `cfg(debug_assertions)` holds for the dev profile
    /// `cargo test` uses and not for the bench profile, which inherits release.
    ///
    /// The cost is that `cargo test --release` cannot name this, which is why the tests that
    /// use it are gated too.
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn copied_record_bytes(&self) -> usize {
        self.copied
    }

    /// The next thing that happened on the connection.
    ///
    /// Events are delivered in the order the protocol produced them, so several arising from a
    /// single read arrive as one sequence rather than collapsed into the last of them.
    ///
    /// Events queued before the connection ended are delivered *before* the ending is
    /// reported. A peer that sends its last record and its close in one write therefore has
    /// both observed, in that order, which is the difference between a clean shutdown and a
    /// lost final message.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending once the queue is empty.
    pub fn poll_next_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event>> {
        // Forced: a caller reading events may never write at all, and the window extensions
        // and ping answers this pass produces are exactly what the peer is waiting for before
        // it sends the next event. Accumulating those would be a connection that goes quiet in
        // proportion to how well it is being read.
        let pumped = self.pump(cx, Flush::Everything);
        if let Some(event) = self.events.pop() {
            if let Event::StreamData { data, .. } = &event {
                // Delivery is what read-ahead is measured in: from here the bytes are the
                // caller's, and the layer will read no further ahead than the caller has
                // credited back. Counted on the way out rather than when the event was queued,
                // because an event sitting in the queue is bounded by the protocol's window
                // and one the caller is holding is bounded by nothing else.
                self.read_ahead.delivered(data.len() as u64);
            }
            return Poll::Ready(Ok(event));
        }
        match pumped {
            Ok(()) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// How many delivered bytes the caller has yet to report consuming.
    ///
    /// The layer's own read-ahead figure, bounded by [`Config::read_ahead`] plus everything
    /// [`Connection::extend_connection_credit`] has accounted for. A caller that watches this
    /// climb to the bound and stay there is watching backpressure work: the layer has stopped
    /// reading, and the peer's window is closing behind it.
    #[must_use]
    pub const fn read_ahead(&self) -> u64 {
        self.read_ahead.outstanding()
    }

    /// Opens a bidirectional stream.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending. Exhausted stream capacity is not an error: it is
    /// [`Poll::Pending`], because the peer may raise the limit at any time.
    pub fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId>> {
        self.poll_open(cx, OpenKind::Bidi)
    }

    /// Opens a unidirectional stream.
    ///
    /// # Errors
    ///
    /// As [`Connection::poll_open_bidi`].
    pub fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId>> {
        self.poll_open(cx, OpenKind::Uni)
    }

    fn poll_open(&mut self, cx: &mut Context<'_>, kind: OpenKind) -> Poll<Result<StreamId>> {
        // Forced, like every entry point a caller can stop polling after. The open itself
        // queues a record rather than producing one, so what this flush carries is whatever was
        // already waiting -- which is exactly the output a caller who now parks on the returned
        // stream would otherwise strand.
        //
        // An opportunistic pump here was tried and reverted: it measured no fewer writes in
        // any arm of the concurrency sweep, because a driver opens the streams a pass needs
        // before it offers anything onto them, so the buffer it would have left unflushed is
        // empty. What it did cost was the clean rule -- every entry point but
        // `poll_pump_buffered` forces -- and a caller that opened a stream and then parked on
        // something outside this connection would have stranded whatever an earlier
        // `try_write_stream` or `extend_connection_credit` had queued.
        if let Err(error) = self.pump(cx, Flush::Everything) {
            return Poll::Ready(Err(error));
        }

        let opened = match kind {
            OpenKind::Bidi => self.conn.open_bidi_stream(),
            OpenKind::Uni => self.conn.open_uni_stream(),
        };

        match opened {
            Ok(OpenOutcome::Opened(stream)) => {
                self.produce_pending = true;
                Poll::Ready(Ok(stream))
            }
            // Capacity is the peer's to grant, and it grants it in a MAX_STREAMS frame this
            // side has yet to read. Parked against the `extend_max_streams` callback, which is
            // dwnx reporting exactly that frame; waking here instead would spin a whole core
            // for as long as the peer took to answer.
            Ok(OpenOutcome::Blocked) => {
                self.signals.park_open(cx);
                Poll::Pending
            }
            Err(error) => Poll::Ready(Err(Error::from(error))),
        }
    }

    /// Writes to a stream, waiting when there is no credit for any of it.
    ///
    /// Returns how many bytes were taken, which may be fewer than offered: a record holds a
    /// bounded amount and the peer's window may hold less again. The payload is **split**
    /// across as many records as it needs rather than truncated to one, and the remainder is
    /// neither sent nor lost -- offer it again.
    ///
    /// `fin` marks the end of the stream. It is applied to the record that takes the last of
    /// the payload, so it survives a payload split across records; offering no data with `fin`
    /// set sends the end-of-stream marker on its own.
    ///
    /// This is the form for a caller that has a [`Context`]. Where an immediate answer is
    /// needed instead, see [`Connection::try_write_stream`].
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and reports a stream whose write side is closed as
    /// [`ErrorKind::Internal`] -- a request this side should not have made, rather than
    /// anything the peer did.
    pub fn poll_write_stream(
        &mut self,
        cx: &mut Context<'_>,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Poll<Result<usize>> {
        if let Err(error) = self.pump(cx, Flush::WhenFull) {
            return Poll::Ready(Err(error));
        }

        let outcome = self.write_stream_records(cx, stream, data, fin);
        if let Poll::Ready(Err(_)) = &outcome {
            // Nothing to flush towards: the ending is latched, and a write issued on a
            // connection that has just failed can only report the same failure again.
            return outcome;
        }
        // The forced flush, and it is owed on *every* other exit above -- including the ones
        // that park. A payload that filled the peer's window produced records and then reported
        // a refusal, and those records are how the peer learns to extend the window: holding
        // them back until some later pass makes the wait for that extension a wait for
        // something this side is itself preventing.
        //
        // Unconditional rather than skipped where the loop already met a refusal. That costs
        // one repeated offer to a byte stream that has just said no, on the path where the
        // connection is backed up anyway; distinguishing the cases would put a second reason to
        // flush in a second place, which is the shape this policy exists to avoid.
        match self.flush(cx) {
            Ok(_) => outcome,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    /// Produces records for `data` until it is spent or the connection will take no more.
    ///
    /// The loop [`Connection::poll_write_stream`] wraps. Split out so that the forced flush
    /// that follows it is written once and cannot be missed on one of the five ways out.
    fn write_stream_records(
        &mut self,
        cx: &mut Context<'_>,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Poll<Result<usize>> {
        let mut written = 0usize;
        loop {
            let produced = match self.write_record(cx, stream, &data[written..], fin) {
                Err(error) => return Poll::Ready(Err(error)),
                // The output buffer is within one record of its ceiling and the byte stream
                // would not take enough of it to make room, so there is nowhere to put another
                // record. Its own waker is registered, so no re-arm is needed here.
                Ok(None) => {
                    return if written > 0 {
                        Poll::Ready(Ok(written))
                    } else {
                        Poll::Pending
                    };
                }
                Ok(Some(produced)) => produced,
            };

            // Counted before the verdict is read, and for every verdict. A record that took
            // part of the payload and *then* ran out of window reports both the count and the
            // refusal, and the bytes it took are already on their way to the peer: dropping
            // the count because the verdict was a refusal would have the caller offer them a
            // second time and the stream would carry them twice.
            written += produced.consumed;

            match produced.verdict {
                Verdict::Closed if written == 0 => {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::Internal,
                        "the stream's write side is closed",
                    )));
                }
                Verdict::Closed => return Poll::Ready(Ok(written)),
                Verdict::Blocked => return self.park_write(cx, written),
                Verdict::Packed => {
                    if written == data.len() {
                        return Poll::Ready(Ok(written));
                    }
                    // Nothing taken from a non-empty payload: dwnx reports an exhausted
                    // *connection* window this way rather than as a blocked stream, and
                    // producing again would spin.
                    if produced.consumed == 0 {
                        return self.park_write(cx, written);
                    }
                }
            }
        }
    }

    /// Writes to a stream without ever waiting.
    ///
    /// The form for a caller with no [`Context`] to park with -- which is not a hypothetical
    /// audience: the HTTP/3 layer offers its outbound bytes through a synchronous closure that
    /// is handed a stream, some slices and a verdict to return, and a transport that could
    /// only park would have nothing legal to do inside it.
    ///
    /// The payload is split rather than truncated, exactly as in
    /// [`Connection::poll_write_stream`]; what differs is that exhausted credit comes back as
    /// [`StreamWrite::Blocked`] instead of parking, and a finished stream as
    /// [`StreamWrite::Closed`] instead of an error. Nothing is written to the byte stream
    /// here: the records join the outbound buffer and leave on the next pump.
    ///
    /// # One call fills records until something stops it
    ///
    /// The payload is spread over **as many records as the outbound buffer will hold**, not
    /// over one. That is what makes a short answer mean something: this returns fewer bytes
    /// than it was offered only when the peer's flow-control window is exhausted or the buffer
    /// has no room for a further record -- both of which are backpressure a caller must wait
    /// out -- and never merely because a record filled, which is an event only this layer can
    /// see and can always answer by starting another one.
    ///
    /// The distinction is the point. Taking one record per call was the earlier behaviour, and
    /// it made every large offer answer short; the layer above reads a short answer as
    /// congestion and stands the stream down for the rest of its pass, so a stream with a
    /// megabyte to send moved sixteen kilobytes of it per pass however much room the buffer
    /// had. The rejected alternative was to leave the decision up there -- re-offer after a
    /// short accept -- and it is not available: that layer is told a count and nothing else,
    /// so re-offering on a short accept would spin against a stream whose window is shut.
    ///
    /// Where the two are indistinguishable the loop **stops**, biasing towards one more offer
    /// from the caller rather than towards a production that cannot make progress: a caller
    /// that offers again when it need not have costs a call, and a loop that continues when it
    /// must not have costs a core.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and a failed production, which is fatal.
    pub fn try_write_stream(
        &mut self,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<StreamWrite> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }

        // The running total, and the single source of the answer. Every exit below reports
        // this same figure when it is non-zero, because bytes packed into a record are already
        // committed to the peer -- the state machine has advanced the stream's send offset --
        // and a refusal that lost the count would have the caller offer them a second time.
        let mut taken = 0usize;
        loop {
            let Some(room) = self.room_for(data.len() - taken, taken > 0) else {
                // The buffer has reached the ceiling, so there is nowhere to put another
                // record until the byte stream has taken some of what is already there -- and
                // nothing is written from here, so it cannot make room itself. Reporting it as
                // blocked is honest and needs no new variant: the caller's response -- offer
                // the rest again later -- is the same one exhausted credit calls for.
                //
                // This used to refuse while the buffer held *anything*, which cost one record
                // per offer and one write per record. The refusal is now a bound being reached
                // rather than a record being outstanding.
                return Ok(accepted_or(taken, StreamWrite::Blocked));
            };

            let produced = self.produce_within(
                WriteRequest::stream(stream, &data[taken..]).with_fin(fin),
                room,
            )?;
            // Counted before the verdict is read, and for every verdict, for the reason above.
            // `fin` rides the record that takes the last of the payload, which is dwnx's rule
            // and not this loop's: every push carries the flag with whatever is left, and the
            // end-of-stream marker is applied by the push that empties it. So a payload split
            // across records here ends the stream exactly once, on the last of them.
            taken += produced.consumed;

            match produced.verdict {
                Verdict::Closed => return Ok(accepted_or(taken, StreamWrite::Closed)),
                Verdict::Blocked => return Ok(accepted_or(taken, StreamWrite::Blocked)),
                Verdict::Packed => {
                    // The whole offer is packed. An empty payload lands here on its first turn
                    // and answers `Accepted(0)`, which is how an end-of-stream marker carrying
                    // no data is accepted.
                    if taken == data.len() {
                        return Ok(StreamWrite::Accepted(taken));
                    }
                    // Nothing taken from a payload that still has bytes in it: dwnx reports an
                    // exhausted *connection* window this way rather than as a blocked stream.
                    // Producing again would pack another empty record and ask the same
                    // question, which is the one way this loop could fail to terminate.
                    if produced.consumed == 0 {
                        return Ok(accepted_or(taken, StreamWrite::Blocked));
                    }
                }
            }
        }
    }

    /// Shuts down one or both halves of a stream, telling the peer why.
    ///
    /// The read half sends STOP_SENDING and the write half RESET_STREAM, so either is visible
    /// to the peer with the application error code supplied.
    ///
    /// The frames this queues leave on the next pump. Shutting down a stream that does not
    /// exist is not an error -- the state machine looks the id up and reports success when it
    /// finds nothing, and that behaviour is passed through rather than papered over.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and anything the state machine refuses.
    pub fn shutdown_stream(
        &mut self,
        stream: StreamId,
        half: Shutdown,
        app_error_code: u64,
    ) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.shutdown_stream(stream, half, app_error_code)?;
        self.produce_pending = true;
        Ok(())
    }

    /// Reports bytes consumed on a stream, so the peer may send that much more.
    ///
    /// This moves the *protocol's* per-stream window and nothing else. It does not relieve
    /// this layer's read-ahead bound: a caller reporting the same bytes to both windows --
    /// which is what the HTTP/3 layer above does, because stream-level credit does not imply
    /// connection-level credit -- would otherwise credit each consumed byte twice, and a bound
    /// that fell twice as fast as it rose would never bind at all. See
    /// [`Connection::extend_connection_credit`], which is the one that counts.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending, and anything the state machine refuses -- extending a
    /// stream this endpoint never receives on, for instance.
    pub fn extend_stream_credit(&mut self, stream: StreamId, bytes: u64) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.extend_max_stream_data(stream, bytes)?;
        self.produce_pending = true;
        Ok(())
    }

    /// Reports bytes consumed across the connection, so the peer may send that much more.
    ///
    /// Separate from [`Connection::extend_stream_credit`] because the two windows are separate:
    /// stream-level credit does not imply connection-level credit, and a caller who extends
    /// only one leaves the other to run out.
    ///
    /// This is also the call that governs the layer's own read-ahead. Bytes credited here
    /// cancel bytes delivered by [`Connection::poll_next_event`], and a connection that
    /// stopped reading because the caller was behind resumes on this call. A caller that
    /// consumes events and never credits will be read to exactly once and then stopped, which
    /// is the bound doing its job rather than a fault.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending.
    pub fn extend_connection_credit(&mut self, bytes: u64) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        self.conn.extend_max_data(bytes);
        self.read_ahead.credited(bytes);
        if !self.read_ahead.is_exhausted() {
            // The pump stopped reading and registered its waker here rather than with the byte
            // stream, so nothing else will ever fire it: the byte stream has been ready this
            // whole time and the layer was declining to look.
            self.signals.wake_read_ahead();
        }
        self.produce_pending = true;
        Ok(())
    }

    /// Permits the peer to open `count` more streams of one kind.
    ///
    /// The counterpart to [`Connection::extend_stream_credit`] for the other resource a peer
    /// can exhaust. Stream capacity is *not* returned when a stream closes -- neither dwnx nor
    /// this layer recycles it -- so a connection that never calls this stops accepting new
    /// streams for good once the configured limit has been reached, which presents as a peer
    /// whose opens hang rather than as an error at either end.
    ///
    /// The MAX_STREAMS frame this queues leaves on the next pump, and the peer's blocked open
    /// wakes when it arrives.
    ///
    /// # Errors
    ///
    /// Reports the connection's ending.
    pub fn extend_stream_limit(&mut self, kind: Directionality, count: usize) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }
        match kind {
            Directionality::Bidirectional => self.conn.extend_max_streams_bidi(count),
            Directionality::Unidirectional => self.conn.extend_max_streams_uni(count),
        }
        self.produce_pending = true;
        Ok(())
    }

    /// Closes the connection, telling the peer why.
    ///
    /// Four steps, in an order that matters. Whatever is already queued goes out first, so the
    /// close does not overtake a record the peer is midway through reading. The encoded close
    /// record is appended. It is flushed. Then the write side of the byte stream is shut down,
    /// so the peer's read reports end of stream rather than waiting for bytes that will never
    /// come.
    ///
    /// Nothing further is produced once the close is queued: the connection is over, and a
    /// record serialised after the close would arrive after it or not at all.
    ///
    /// Poll until [`Poll::Ready`]. Abandoning this partway leaves the close in a buffer, and a
    /// peer that never receives one cannot tell a deliberate shutdown from a crash.
    ///
    /// # Errors
    ///
    /// Reports a byte-stream failure encountered while writing the close or shutting down.
    pub fn poll_close(&mut self, cx: &mut Context<'_>, reason: &CloseReason) -> Poll<Result<()>> {
        if self.closing == Some(Closing::Complete) {
            return Poll::Ready(Ok(()));
        }

        if self.closing.is_none() {
            // Produced, not merely flushed, and before `closing` is set -- which stops
            // production. A stream this caller reset moments ago has its RESET_STREAM sitting
            // in the state machine, and a close that flushed without producing would leave
            // the peer with a stream that simply stopped.
            match self.drain_pending(cx) {
                Ok(true) => {}
                Ok(false) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
            // The one thing that still reaches the outbound buffer as a copy rather than
            // being serialised into it. `encode_close_record` builds an owned buffer of its
            // own -- it is this layer's encoder, not dwnx's, because dwnx has no writer for a
            // close (`docs/qmux/pending-work.md`) -- so there is a source buffer whether this
            // path wants one or not. It is a few dozen bytes, once, on the way out of a
            // connection, and `copied_record_bytes` counts it rather than pretending
            // otherwise.
            self.append(&encode_close_record(reason));
            self.closing = Some(Closing::Queued);
            // Latched now rather than when the shutdown completes, so an operation issued
            // between the two reports the close that is already on its way rather than
            // appearing to succeed.
            let _ = self.fail(
                Error::new(
                    ErrorKind::LocallyClosed,
                    "the connection was closed locally",
                )
                .with_close(reason.clone()),
            );
        }

        if self.closing == Some(Closing::Queued) {
            match self.flush(cx) {
                Ok(true) => self.closing = Some(Closing::Written),
                Ok(false) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }

        match self.stream.poll_shutdown(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.closing = Some(Closing::Complete);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(
                self.fail_stream(error, "the byte stream failed while shutting down")
            )),
        }
    }

    /// Flushes what is queued and shuts the write side down, without saying why.
    ///
    /// The counterpart to [`Connection::poll_close`] for an ending that carries no close: the
    /// caller failed, or went away, and there is nothing to tell the peer beyond the fact that
    /// nothing more is coming. The bytes already produced still have to reach it.
    ///
    /// Shutting down matters even though dropping a socket produces the same FIN. A byte
    /// stream that wraps another — a buffered writer, a TLS session — has bytes of its own to
    /// flush, and dropping it discards them. That is the whole reason
    /// [`AsyncByteStream::poll_shutdown`](crate::io::AsyncByteStream::poll_shutdown) exists,
    /// and skipping it works only for the one implementation that needs it least.
    ///
    /// Poll until [`Poll::Ready`].
    ///
    /// # Errors
    ///
    /// Reports a byte-stream failure encountered while flushing or shutting down.
    pub fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.closing == Some(Closing::Complete) {
            return Poll::Ready(Ok(()));
        }

        match self.drain_pending(cx) {
            Ok(true) => {}
            Ok(false) => return Poll::Pending,
            Err(error) => return Poll::Ready(Err(error)),
        }

        match self.stream.poll_shutdown(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.closing = Some(Closing::Complete);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(
                self.fail_stream(error, "the byte stream failed while shutting down")
            )),
        }
    }

    /// Produce, write, read -- and one more write pass for whatever the read left to say.
    ///
    /// `flush` is the caller's answer to "am I coming back before anything else polls this
    /// connection?", and it decides whether produced output may wait for the rest of the turn
    /// or must leave now. See [`Flush`].
    fn pump(&mut self, cx: &mut Context<'_>, flush: Flush) -> Result<()> {
        if let Some(terminal) = &self.terminal {
            return Err(terminal.error());
        }

        self.write_side(cx, flush)?;
        self.read_side(cx)?;
        if self.produce_pending {
            self.write_side(cx, flush)?;
        }
        Ok(())
    }

    /// Produces what is pending into the outbound buffer, writing when the buffer is full and,
    /// where the caller asked for it, once more at the end.
    ///
    /// The loop is bounded by the buffer rather than by the byte stream: it produces while
    /// there is room for another whole record, and writes only to make room. That is what puts
    /// several records in one write. A caller that will not be back sets [`Flush::Everything`]
    /// and the last of them leaves before this returns.
    ///
    /// Production stops when the buffer's *tail* is short, even when the byte stream has
    /// already taken bytes off its front -- the stop-early decision, whose alternatives and
    /// consequence are in the module documentation.
    fn write_side(&mut self, cx: &mut Context<'_>, flush: Flush) -> Result<()> {
        loop {
            if !self.room_for_record() && !self.flush(cx)? {
                // The buffer is full and the byte stream would not take it. Nothing more can
                // be produced, and there is nothing left to force out: the flush that just
                // failed registered whatever wake will end the wait.
                return Ok(());
            }
            if !self.produce_pending || self.closing.is_some() {
                break;
            }

            let produced = self.produce(WriteRequest::control_only())?;
            if produced.bytes == 0 || produced.verdict != Verdict::Packed {
                // An empty record means the state machine had nothing queued. The other two
                // verdicts name a stream, and a control-only request names none, so they
                // cannot arise here -- but they are answered rather than ignored, because the
                // alternative is a loop that never ends if that ever stops being true.
                self.produce_pending = false;
                break;
            }
        }

        if flush == Flush::Everything {
            self.flush(cx)?;
        }
        Ok(())
    }

    /// Whether another record may be produced.
    ///
    /// The one question the whole flush policy turns on, asked in the four places that decide
    /// whether to build a record. It is arithmetic on the buffer's length rather than on what
    /// is left to send, because the space in front of `written` is not available to a record:
    /// production appends at the back.
    ///
    /// [`MAX_RECORD`] is the reserve [`OUTBOUND_CEILING`] carries, so a record begun here can
    /// always be finished without the buffer exceeding the ceiling.
    fn room_for_record(&self) -> bool {
        self.filled + MAX_RECORD <= OUTBOUND_CEILING
    }

    /// How large a record may be built now, or [`None`] if none may be.
    ///
    /// [`Connection::room_for_record`] with one addition, and the addition is what keeps a
    /// multi-record offer from stranding its own last few bytes.
    ///
    /// A whole record's reserve is what the ceiling normally holds back, because a record
    /// begun without knowing how large it will be may run to [`MAX_RECORD`]. That is not the
    /// only case. When a call has already packed records for an offer and what is left of the
    /// payload is *smaller than the free space*, the record that would carry it is small too
    /// -- and it can be held to that size rather than assumed to be, by handing the record
    /// writer a buffer only that long. The bound is then enforced the same way [`MAX_RECORD`]
    /// is: the writer cannot write past what it was given.
    ///
    /// Without it, a 64 KiB body offered in one go filled four records, found the reserve one
    /// record wide and forty bytes of payload left, and answered short. Those forty bytes then
    /// travelled alone, in a write of their own, because the pass that produced them ends with
    /// a forced flush -- one extra write per stream, which cost more at concurrency 64 than
    /// the whole of what multi-record production had gained there (130 writes against 69). The
    /// guard is
    /// `tests/ngnet-qmux-h3-tests/tests/concurrent_driver_writes.rs`'s multiplexed-pass ratio,
    /// which is what fails if this clause is removed.
    ///
    /// `continuing` is what limits the concession to that case. A *first* record is always
    /// given the full reserve, so an offer this connection has no room for is refused outright
    /// rather than trickled into whatever space is left -- and an offer of nothing but an
    /// end-of-stream marker keeps today's answer, which matters because a record that could
    /// not be built and a record carrying a fin are both zero bytes of payload and only the
    /// reserve tells them apart.
    ///
    /// The rejected alternative was to compute the record's size from the payload plus a
    /// framing allowance. That is an assertion about dwnx's varint encoding rather than about
    /// the buffer, and it is wrong in the direction that overruns the ceiling.
    fn room_for(&self, remaining: usize, continuing: bool) -> Option<usize> {
        let space = OUTBOUND_CEILING.saturating_sub(self.filled);
        if space >= MAX_RECORD {
            return Some(MAX_RECORD);
        }
        // Strictly greater: the record has to hold the payload *and* its framing, and a space
        // exactly the size of what is left cannot. Where the framing still does not fit, the
        // record takes what it can and the call answers short, which is the same answer it
        // would have given without this clause.
        if continuing && space > remaining {
            return Some(space);
        }
        None
    }

    /// Produces whatever the state machine has queued and writes all of it out.
    ///
    /// Returns whether everything is now on the byte stream. The ending paths need this and
    /// cannot use [`Connection::flush`] alone: a reset or a stop-sending issued just before
    /// the end is *queued inside the state machine*, not in the outbound buffer, and only a
    /// production pass turns it into a record. Flushing alone writes what is already there
    /// and silently drops what is not — which loses exactly the frames that explain to the
    /// peer why the ending is happening.
    ///
    /// [`Flush::Everything`] for the same reason the ending is the reason this exists: there is
    /// no later pass. An ending that accumulated would leave the explanation in a buffer whose
    /// connection is about to be dropped.
    fn drain_pending(&mut self, cx: &mut Context<'_>) -> Result<bool> {
        self.write_side(cx, Flush::Everything)?;
        Ok(self.written >= self.filled)
    }

    /// Offers the outbound buffer to the byte stream until it is empty or refuses.
    ///
    /// Returns whether the buffer is now empty. It offers `outbound[written..filled]` however many
    /// records that spans, which is the whole of what accumulating them costs the write path:
    /// the byte stream is handed a longer slice and reports a count against it exactly as
    /// before, and `written` resumes wherever that count left off -- at a record boundary or
    /// inside one.
    fn flush(&mut self, cx: &mut Context<'_>) -> Result<bool> {
        while self.written < self.filled {
            match self
                .stream
                .poll_write(cx, &self.outbound[self.written..self.filled])
            {
                Poll::Pending | Poll::Ready(Ok(Written::NotNow)) => return Ok(false),
                Poll::Ready(Err(error)) => {
                    return Err(self.fail_stream(error, "the byte stream failed while writing"));
                }
                Poll::Ready(Ok(Written::Accepted(0))) => {
                    // Forbidden by the contract, because zero bytes accepted carries no
                    // obligation to wake and a caller offered it can only spin. This is the
                    // one wake this layer issues to itself, and it is not a scheduling
                    // placeholder: it is what an implementation that broke the rule gets
                    // instead of a connection that stalls in silence, and it is unreachable
                    // for one that keeps it.
                    cx.waker().wake_by_ref();
                    return Ok(false);
                }
                Poll::Ready(Ok(Written::Accepted(taken))) => {
                    self.written = self.filled.min(self.written + taken);
                }
            }
        }

        // The cursors are reset and the buffer is left exactly as long as it was. Clearing it
        // would give back the initialisation that lets the next record be serialised in place,
        // and it is the initialisation rather than the allocation that would then be paid
        // again on every pass.
        self.filled = 0;
        self.written = 0;
        Ok(true)
    }

    /// Makes sure the read buffer is one nothing else is holding.
    ///
    /// Does nothing at all in the ordinary case, which is a caller that has dropped the
    /// deliveries from the last read: the strong count is one, the buffer is reusable, and this
    /// connection reads into the same allocation it has used since it was built.
    ///
    /// Otherwise the buffer is retired -- put aside to be watched for the moment its last
    /// delivery is dropped -- and replaced with a recycled one if any is free and a fresh one if
    /// none is. Allocating rather than waiting is deliberate and is what FR-016 asks for: a
    /// caller is entitled to hold delivered data for as long as it likes, and a reader that
    /// blocked until the caller let go would turn that entitlement into a stall. What bounds the
    /// memory instead is the read-ahead credit, which is unchanged and which is accounted by
    /// bytes delivered against bytes credited rather than by whether the caller still holds
    /// them.
    fn claim_read_buffer(&mut self) {
        if Arc::get_mut(&mut self.inbound).is_some() {
            return;
        }

        // The first spare whose last delivery has been dropped. Scanned rather than popped:
        // buffers come free in the order their deliveries are consumed, which is not the order
        // they were retired in.
        let recycled = self
            .spare
            .iter_mut()
            .position(|buffer| Arc::get_mut(buffer).is_some())
            .map(|index| self.spare.swap_remove(index));

        let fresh = recycled.unwrap_or_else(|| Arc::new(vec![0; READ_BUFFER]));
        let retired = core::mem::replace(&mut self.inbound, fresh);
        if self.spare.len() < READ_POOL_LIMIT {
            self.spare.push(retired);
        }
    }

    /// How many read buffers this connection is holding, the one being read into included.
    ///
    /// Exposed so a test can assert the bound rather than trust it, and gated for the reason
    /// [`Connection::copied_record_bytes`] gives: a counter present in one build of a benchmark
    /// comparison and absent from the other measures the instrument.
    #[cfg(debug_assertions)]
    #[must_use]
    pub fn read_buffers(&self) -> usize {
        1 + self.spare.len()
    }

    /// Reads from the byte stream until it has nothing more, feeding the framer and then the
    /// state machine.
    ///
    /// Stops early when the caller is behind. That is the whole of the read-ahead bound: an
    /// unread byte stream is backpressure the peer can feel, and it costs this side nothing to
    /// hold.
    fn read_side(&mut self, cx: &mut Context<'_>) -> Result<()> {
        loop {
            if self.read_ahead.is_exhausted() {
                // No read is issued, so the byte stream registers nothing -- which is why the
                // waker goes here instead, to be fired by the credit that makes room. A read
                // issued anyway would deliver bytes the caller has no room for and defeat the
                // bound; parking without registering anything would strand the connection.
                self.signals.park_read_ahead(cx);
                return Ok(());
            }

            // A buffer nobody is holding a delivery from, before anything is read into one.
            // The connection reuses the same buffer for its whole life unless a caller is
            // holding data cut from it, at which point it takes another rather than waiting --
            // which is what keeps a held delivery from stalling the reader.
            self.claim_read_buffer();
            let buffer = Arc::get_mut(&mut self.inbound)
                .expect("the read buffer was just claimed, so nothing else holds it");
            let filled = match self.stream.poll_read(cx, buffer) {
                Poll::Pending => return Ok(()),
                Poll::Ready(Err(error)) => {
                    return Err(self.fail_stream(error, "the byte stream failed while reading"));
                }
                Poll::Ready(Ok(0)) => return Err(self.ended()),
                Poll::Ready(Ok(filled)) => filled.min(self.inbound.len()),
            };

            // The framer first. It is what latches the peer's close record, and the state
            // machine may report that close before this chunk is exhausted -- so the record
            // has to be in hand before the outcome below is acted on.
            if let Err(error) = self.framer.consume(&self.inbound[..filled]) {
                return Err(self.fail(error));
            }

            let now = self.clock.now();
            // Sampled across the read because a MAX_DATA frame raises this and raises nothing
            // else: dwnx applies it to the connection's send window and invokes no callback
            // (`deps/dwnx/lib/dwnx_conn.c:1045-1056`), so a write parked on an exhausted
            // connection window has no event to wait for and this comparison is its wakeup.
            // Waking on any inbound bytes would have done as well, and would have spun a
            // blocked writer once per record for as long as the peer kept sending.
            let credit_before = self.conn.max_data_left();
            // The buffer is named to the queue for exactly the duration of this call, which is
            // the only window in which a handler runs. See `super::event` for why the handler
            // needs it and why holding it any longer would stop the buffer ever being reused.
            self.events.begin_read(&self.inbound);
            let outcome = self.conn.read(&self.inbound[..filled], now);
            self.events.end_read();
            if self.conn.max_data_left() > credit_before {
                self.signals.wake_credit();
            }
            // Whatever arrived may have queued a response -- a window extension, a ping
            // answer -- and the pump's trailing write pass is what sends it.
            self.produce_pending = true;

            match outcome {
                Ok(ReadOutcome::Processed) => {}
                Ok(ReadOutcome::PeerClosed) => return Err(self.peer_closed()),
                Err(error) => return Err(self.fail(Error::from(error))),
            }
        }
    }

    /// Produces one record for `stream`, making room for it first if the buffer is full.
    ///
    /// Returns [`None`] when the buffer cannot take another record and the byte stream would
    /// not take enough of it to change that, in which case nothing was produced.
    ///
    /// It used to flush, produce and flush again, which is what made a payload cost one write
    /// per record. Nothing is written here now unless the buffer is full; the records this
    /// produces leave together, in the forced flush
    /// [`Connection::poll_write_stream`] ends with.
    fn write_record(
        &mut self,
        cx: &mut Context<'_>,
        stream: StreamId,
        data: &[u8],
        fin: bool,
    ) -> Result<Option<Produced>> {
        if !self.room_for_record() && !self.flush(cx)? {
            return Ok(None);
        }
        let produced = self.produce(WriteRequest::stream(stream, data).with_fin(fin))?;
        Ok(Some(produced))
    }

    /// Makes sure the buffer's tail holds `room` bytes a record may be serialised into.
    ///
    /// The tail has to be *initialised*, not merely reserved, because there is no `unsafe`
    /// under `src/io/` and so no way to hand out a `Vec`'s spare capacity as a `&mut [u8]`.
    /// Growing the length with zeros is the safe form of the same thing, and the zeroing is
    /// paid once per connection per step of growth rather than once per record: the buffer is
    /// never shortened afterwards, so a connection that has reached its working size never
    /// grows again.
    ///
    /// The growth is to exactly what is needed rather than by doubling, which is deliberate
    /// and costs a handful of reallocations over a connection's whole life. Doubling would put
    /// the *capacity* above [`OUTBOUND_CEILING`] -- 128 KiB behind an 80 KiB queue -- and the
    /// ceiling is a promise about the memory a slow peer can make this side hold, which is the
    /// capacity and not the cursor. `reserve_exact` asks the allocator for the same thing, and
    /// where it can extend the block in place there is no copy at all.
    fn make_room(&mut self, room: usize) {
        let needed = self.filled + room;
        if self.outbound.len() < needed {
            self.outbound.reserve_exact(needed - self.outbound.len());
            self.outbound.resize(needed, 0);
        }
    }

    /// Copies `bytes` into the queue, growing it if it must.
    ///
    /// The one path that puts bytes into the outbound buffer without serialising them there,
    /// used by the close and by nothing else; [`Connection::copied_record_bytes`] is what says
    /// so and what would notice a second caller appearing.
    fn append(&mut self, bytes: &[u8]) {
        self.make_room(bytes.len());
        let at = self.filled;
        self.outbound[at..at + bytes.len()].copy_from_slice(bytes);
        #[cfg(debug_assertions)]
        {
            self.copied += bytes.len();
        }
        self.filled += bytes.len();
    }

    /// Serialises one record into the outbound buffer.
    ///
    /// A failure here is fatal and is latched as such; see the module documentation for why a
    /// retry would desynchronise the stream.
    fn produce(&mut self, request: WriteRequest<'_>) -> Result<Produced> {
        self.produce_within(request, MAX_RECORD)
    }

    /// As [`Connection::produce`], with the record held to `room` bytes.
    ///
    /// `room` is how much of the buffer's tail the record writer is shown, and holding a record
    /// to a size means giving the writer nothing else to write into -- the same mechanism that
    /// makes [`MAX_RECORD`] an upper bound rather than an expectation. A caller that has no
    /// reason to shorten a record passes [`MAX_RECORD`], which is what [`Connection::produce`]
    /// is.
    ///
    /// **The clamp to [`MAX_RECORD`] is a correctness bound, not tidiness.** The tail beyond
    /// the fill cursor is usually longer than a record, and handing all of it over is the one
    /// mistake on this path that produces a wrong wire rather than an error: dwnx fills what it
    /// is given and then describes the result with a fixed two-byte length, which above 16383
    /// aborts where the C keeps its assertions -- both profiles, as this workspace builds it --
    /// and truncates to sixteen bits where they are compiled out. The module documentation
    /// carries the citations and what was measured. Nothing in [`Conn::record`]'s contract
    /// refuses an over-long buffer, so this line is the refusal.
    ///
    /// dwnx is documented as accepting a buffer smaller than a full record, and a buffer too
    /// small to hold anything at all comes back as an empty record rather than as a failure,
    /// so the caller sees a production that took nothing and stops -- see
    /// [`Connection::room_for`] for who asks for a short record and why.
    fn produce_within(&mut self, request: WriteRequest<'_>, room: usize) -> Result<Produced> {
        let now = self.clock.now();
        let room = room.min(MAX_RECORD);
        self.make_room(room);
        let at = self.filled;
        // `split_at_mut` rather than an index range, and the head half is dropped rather than
        // named. What the record writer is given has to be the tail and nothing else: it holds
        // that borrow for as long as the record is being built, which is the property
        // `compile_fail.rs`'s `the_record_buffer_is_borrowed_for_the_whole_record` pins, and
        // binding the head here would be the one way to have a live path to the buffer beside
        // it. Nothing needs the head -- the bytes in front of the cursor are already written or
        // already queued -- so nothing is offered one.
        let (_, tail) = self.outbound.split_at_mut(at);
        // Spelled out as a free function over two fields rather than as a method, because the
        // record writer borrows the connection and the destination for as long as the record is
        // being built. Splitting the borrows by field is what makes that legal.
        match pack(&mut self.conn, &mut tail[..room], request, now) {
            Ok(produced) => {
                self.filled += produced.bytes;
                // Two bounds, checked where records are made rather than where callers happen
                // to look. Every caller of this asks `room_for_record` or `room_for` first and
                // no record can exceed the room it was given, so both hold by construction --
                // which is exactly the kind of claim that stops holding when a fifth caller
                // appears. A test can only observe the buffer between calls, so without these
                // the peak *inside* a production run would be unmeasured.
                debug_assert!(
                    produced.bytes <= MAX_RECORD,
                    "a record of {} bytes was produced, past the {MAX_RECORD}-byte maximum a \
                     two-byte record length can describe",
                    produced.bytes
                );
                debug_assert!(
                    self.filled <= OUTBOUND_CEILING,
                    "the outbound buffer reached {} bytes, past the {OUTBOUND_CEILING}-byte \
                     ceiling: a record was produced without asking for room first",
                    self.filled
                );
                Ok(produced)
            }
            Err(error) => Err(self.fail(Error::from(error).with_context(
                "serialising a record failed, which loses whatever it had already packed",
            ))),
        }
    }

    /// Classifies a byte stream that reported end of stream.
    fn ended(&mut self) -> Error {
        if let Some(close) = self.framer.close_reason() {
            return self.fail(
                Error::new(ErrorKind::PeerClosed, "the peer closed the connection")
                    .with_close(close),
            );
        }
        if self.framer.at_boundary() {
            self.fail(Error::new(
                ErrorKind::EndOfStream,
                "the byte stream ended between records",
            ))
        } else {
            self.fail(Error::new(
                ErrorKind::TruncatedRecord,
                "the byte stream ended partway through a record",
            ))
        }
    }

    /// Builds the peer's close out of the record the framer latched.
    fn peer_closed(&mut self) -> Error {
        let error = Error::new(ErrorKind::PeerClosed, "the peer closed the connection");
        let error = match self.framer.close_reason() {
            Some(close) => error.with_close(close),
            // The state machine reported a close the framer did not find, which means the
            // record carried a frame the decoder could not walk past. The ending is still a
            // peer close; only the explanation is missing.
            None => error,
        };
        self.fail(error)
    }

    /// Latches an ending, if this is the first, and hands the error back.
    fn fail(&mut self, error: Error) -> Error {
        if self.terminal.is_none() {
            self.terminal = Some(Terminal {
                kind: error.kind(),
                context: error.context(),
                close: error.close_reason().cloned(),
            });
        }
        error
    }

    /// Latches a byte-stream failure, keeping the transport's own error as the source.
    fn fail_stream(&mut self, source: S::Error, context: &'static str) -> Error {
        self.fail(Error::new(ErrorKind::ByteStream, context).with_boxed_source(source.into()))
    }

    /// Reports a partial write, or waits for the peer to extend a window.
    ///
    /// Parked against [`Signals::park_credit`], which the `extend_max_stream_data` callback
    /// fires for a stream window and [`Connection::read_side`] fires for the connection window
    /// -- dwnx raises no callback for the latter. A partial write is reported rather than
    /// waited on, because the bytes that were taken are already the peer's problem and a
    /// caller told nothing about them would send them twice.
    fn park_write(&self, cx: &mut Context<'_>, written: usize) -> Poll<Result<usize>> {
        if written > 0 {
            return Poll::Ready(Ok(written));
        }
        self.signals.park_credit(cx);
        Poll::Pending
    }
}

/// Which kind of stream an open is for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OpenKind {
    Bidi,
    Uni,
}

/// The count if there is one, and the refusal otherwise.
///
/// Written once because the choice is the same at all four exits of
/// [`Connection::try_write_stream`] and getting it wrong at one of them is not visible at the
/// exit itself: a refusal that discarded a non-zero count would have the caller offer those
/// bytes again, and the peer would receive them twice.
const fn accepted_or(taken: usize, refusal: StreamWrite) -> StreamWrite {
    if taken > 0 {
        StreamWrite::Accepted(taken)
    } else {
        refusal
    }
}

/// Builds one record into `dest`, which is a slice of the outbound buffer's tail.
///
/// Free rather than a method so the connection and the destination are borrowed as two
/// separate things: the record writer holds both for as long as the record is being built, and
/// a method would have to hold the whole connection instead.
///
/// `dest` is at most one maximum record long and the caller is what makes that true; see
/// `Connection::produce_within` for what happens to a record built into a longer one. The
/// bytes are left where they were written and the count is what comes back, which is the whole
/// of the difference from the arrangement this replaced.
///
/// # The error path
///
/// `?` on a push is what makes a failure fatal. It drops the writer mid-record, whose `Drop`
/// finalises the record so dwnx stops writing through the buffer -- and discards the bytes,
/// having already advanced the send offset of any stream whose data went in. The caller must
/// fail the connection rather than try again.
///
/// One class of failure is exempt, and the exemption is load-bearing. A push naming a stream
/// the state machine no longer has -- because it was reset, by either end, since the caller
/// last looked -- is refused before the record is begun, so nothing has been packed and
/// nothing has been lost. It is reported as a closed stream, which is what it is. Treating it
/// as fatal instead kills a working connection every time a caller offers bytes for a stream
/// that was reset while those bytes were queued, which is the ordinary shape of a cancelled
/// exchange rather than an edge case: the caller above holds a backlog, the reset discards
/// it, and the next offer names a stream that is gone.
///
/// The exemption is conditional on nothing having been consumed yet. Once part of the
/// payload is in the record, a refusal is no longer the simple "this stream cannot take
/// bytes" it appears to be, and the caller has an accepted count to be told about instead.
fn pack(
    conn: &mut Conn<'static>,
    dest: &mut [u8],
    request: WriteRequest<'_>,
    now: Timestamp,
) -> core::result::Result<Produced, CoreError> {
    let mut consumed = 0usize;
    let mut remaining = request.data;
    let mut verdict = Verdict::Packed;

    let mut writer = conn.record(dest, now);
    loop {
        let step = WriteRequest {
            stream: request.stream,
            data: remaining,
            fin: request.fin,
        };
        let pushed = match writer.push(step) {
            Ok(pushed) => pushed,
            Err(error) if consumed == 0 && error.kind() == crate::ErrorKind::Stream => {
                verdict = Verdict::Closed;
                break;
            }
            Err(error) => return Err(error),
        };
        match pushed {
            Push::Accepted { consumed: taken } => {
                let taken = taken.unwrap_or(0);
                consumed += taken;
                remaining = &remaining[taken..];
                if remaining.is_empty() {
                    break;
                }
            }
            Push::Complete { consumed: taken } => {
                consumed += taken.unwrap_or(0);
                break;
            }
            Push::StreamBlocked => {
                verdict = Verdict::Blocked;
                break;
            }
            Push::StreamClosed => {
                verdict = Verdict::Closed;
                break;
            }
        }
    }

    // Finished even when a stream said no: the record may still carry control frames that were
    // packed before the stream was consulted, and abandoning it would discard them.
    //
    // What comes back is a slice of `dest` -- the bytes are already where they belong -- so all
    // that is taken from it is the length. `Record::Empty` and `Record::BufferTooSmall` are
    // both zero, and both mean the same thing to the caller: this production put nothing in the
    // buffer.
    let record = writer.finish()?;
    let bytes = record.bytes().map_or(0, <[u8]>::len);

    Ok(Produced {
        consumed,
        bytes,
        verdict,
    })
}

/// The handlers the layer installs on the state machine.
///
/// They capture the event queue and the signal set and nothing else, which is what satisfies
/// the state machine's `Send` bound on handlers without imposing one on the caller's byte
/// stream or clock. A handler cannot reach the connection by design, so each one records and
/// returns; the pump acts once the entry point that provoked it has returned.
///
/// Two of them do one thing more: they fire a waker. That is not the connection being reached
/// into -- a waker is a scheduling primitive, not a connection handle, and the operation it
/// wakes still runs from a poll like any other. It is how a blocked open and a blocked write
/// learn that the frame they were waiting for has arrived, rather than by asking again on
/// every pass.
fn handlers(events: &EventQueue, signals: &Signals) -> Handlers<'static> {
    let data = events.clone();
    let opened = events.clone();
    let closed = events.clone();
    let reset = events.clone();
    let stop_sending = events.clone();
    let stream_credit = events.clone();
    let limits = events.clone();
    let params = events.clone();
    let credit_signal = signals.clone();
    let limit_signal = signals.clone();

    Handlers::new()
        .on_stream_data(move |event| {
            // `deliver` rather than `push`, because the payload is the one thing a handler
            // cannot simply record: the borrow it is handed is valid only for this call. What
            // the queue holds -- the buffer the state machine is being fed right now -- is what
            // turns that borrow into something the caller can keep, without the handler having
            // to reach the connection to ask. See `super::event` for why that is sound and for
            // when it still copies.
            data.deliver(event.stream_id, event.offset, event.data, event.fin);
            Ok(())
        })
        .on_stream_open(move |stream_id| {
            opened.push(Event::StreamOpened { stream_id });
            Ok(())
        })
        .on_stream_close(move |event| {
            closed.push(Event::StreamClosed {
                stream_id: event.stream_id,
                rx_app_error_code: event.rx_app_error_code,
                tx_app_error_code: event.tx_app_error_code,
            });
            Ok(())
        })
        .on_stream_reset(move |stream_id, final_size, app_error_code| {
            reset.push(Event::StreamReset {
                stream_id,
                final_size,
                app_error_code,
            });
            Ok(())
        })
        .on_recv_stop_sending(move |stream_id, app_error_code| {
            stop_sending.push(Event::StopSending {
                stream_id,
                app_error_code,
            });
            Ok(())
        })
        .on_extend_max_stream_data(move |stream_id, max_data| {
            stream_credit.push(Event::StreamDataCredit {
                stream_id,
                max_data,
            });
            // The event a write parked on an exhausted stream window is waiting for. dwnx
            // raises this both for a MAX_STREAM_DATA frame and for the peer's transport
            // parameters granting a stream its first window, which is exactly the pair of
            // occasions on which a blocked write can make progress.
            credit_signal.wake_credit();
            Ok(())
        })
        .on_extend_max_streams(move |kind, max_streams| {
            limits.push(Event::StreamLimit { kind, max_streams });
            // The event a blocked open is waiting for, and the only one: stream capacity is
            // the peer's to grant and it grants it here.
            limit_signal.wake_open();
            Ok(())
        })
        .on_transport_params(move |received| {
            params.push(Event::PeerTransportParams(received.clone()));
            Ok(())
        })
}

// Written out rather than derived: neither the byte stream nor the clock is required to be
// `Debug`, and the buffers are noise. What a reader wants is the role and how far along the
// connection is.
impl<S: AsyncByteStream, C: Clock> core::fmt::Debug for Connection<S, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Connection")
            .field("role", &self.role())
            .field("outbound", &(self.filled - self.written))
            .field("read_ahead", &self.read_ahead.outstanding())
            .field("closing", &self.closing)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the state machine's own defaults do not have, and the reason this type
    /// exists at all.
    #[test]
    fn the_default_configuration_permits_data_and_streams() {
        let params = Config::default().transport_params();
        assert!(params.initial_max_data() > 0);
        assert!(params.initial_max_stream_data_bidi_local() > 0);
        assert!(params.initial_max_stream_data_bidi_remote() > 0);
        assert!(params.initial_max_stream_data_uni() > 0);
        assert!(params.initial_max_streams_bidi() > 0);
        assert!(params.initial_max_streams_uni() > 0);
    }

    /// The state machine's defaults, for contrast: every one of them is zero, which is what a
    /// connection that inherited them would advertise.
    #[test]
    fn the_state_machines_defaults_permit_nothing() {
        let params = TransportParams::new();
        assert_eq!(params.initial_max_data(), 0);
        assert_eq!(params.initial_max_streams_bidi(), 0);
    }

    #[test]
    fn the_builders_reach_the_parameters_they_name() {
        let params = Config::new()
            .initial_max_stream_data(7)
            .initial_max_data(11)
            .max_streams_bidi(3)
            .max_streams_uni(5)
            .max_idle_timeout(Duration::from_nanos(13))
            .transport_params();

        assert_eq!(params.initial_max_stream_data_bidi_local(), 7);
        assert_eq!(params.initial_max_stream_data_bidi_remote(), 7);
        assert_eq!(params.initial_max_stream_data_uni(), 7);
        assert_eq!(params.initial_max_data(), 11);
        assert_eq!(params.initial_max_streams_bidi(), 3);
        assert_eq!(params.initial_max_streams_uni(), 5);
        assert_eq!(params.max_idle_timeout(), Duration::from_nanos(13));
    }

    /// The read-ahead allowance is the one knob here that never reaches the wire: it governs how
    /// far this layer will run ahead of its caller, not what the peer is told. Getting it wrong in
    /// the direction of zero would stall the connection, so it is checked separately from the
    /// transport parameters.
    #[test]
    fn the_read_ahead_allowance_is_a_local_knob_with_a_non_zero_default() {
        assert_eq!(Config::default().read_ahead, DEFAULT_READ_AHEAD);
        const { assert!(DEFAULT_READ_AHEAD > 0) };
        assert_eq!(Config::new().read_ahead(64).read_ahead, 64);
    }

    /// The configuration dwnx would abort on, rejected as an ordinary error instead.
    #[test]
    fn a_configuration_the_state_machine_asserts_on_is_refused() {
        let params = Config::new().initial_max_data(u64::MAX).transport_params();
        assert!(params.validate().is_err());
    }
}
