//! Per-session state the trampolines reach through the bridge.
//!
//! These types are reached from `extern "C"` callbacks by way of disjoint mutable
//! borrows of individual [`crate::session::Session`] fields, never through the session
//! itself.

use core::ptr::NonNull;
use std::collections::{HashMap, HashSet};

#[cfg(feature = "http")]
use bytes::Bytes;

#[cfg(feature = "http")]
use crate::body::SharedBodySource;
use crate::body::{BodyError, BodySource};
use crate::stream::StreamId;

/// How an outgoing body produces its payload.
///
/// The push arm writes into a buffer libnghttp2 offers; the shared arm hands over octets
/// it already owns for a no-copy `DATA` frame. The enum exists in every configuration so
/// the surrounding code is shape-stable; without the `http` feature (which brings
/// `bytes`) only the push arm is present.
pub(crate) enum Source {
    /// A caller's [`BodySource`], filled into the session's frame buffer.
    Push(Box<dyn BodySource>),
    /// A caller's [`SharedBodySource`], handed to the transport uncopied.
    #[cfg(feature = "http")]
    Shared(Box<dyn SharedBodySource>),
}

/// One outgoing body, at an address libnghttp2 holds for the life of its stream.
pub(crate) struct BodyEntry {
    pub(crate) source: Source,
    /// Set once the source reports that trailers will follow.
    pub(crate) trailers_ready: bool,
    /// The chunk a no-copy read callback set aside for the send callback to hand over.
    ///
    /// `read_shared_body` stages one chunk per frame here and `send_data` takes it. It is
    /// an *overwrite*, never a queue: if libnghttp2 packs a no-copy frame and then never
    /// sends it — the documented case where the stream closes between pack and send — the
    /// stale chunk is released either by the next stage overwriting it or by this entry
    /// being dropped at stream close, so a chunk is never handed over twice.
    #[cfg(feature = "http")]
    pub(crate) staged: Option<Bytes>,
}

impl BodyEntry {
    pub(crate) fn new(source: Box<dyn BodySource>) -> Self {
        Self {
            source: Source::Push(source),
            trailers_ready: false,
            #[cfg(feature = "http")]
            staged: None,
        }
    }

    /// An entry backed by a no-copy [`SharedBodySource`].
    #[cfg(feature = "http")]
    pub(crate) fn new_shared(source: Box<dyn SharedBodySource>) -> Self {
        Self {
            source: Source::Shared(source),
            trailers_ready: false,
            staged: None,
        }
    }
}

/// One no-copy `DATA` frame, waiting to be handed to the driver's sink.
///
/// libnghttp2 serialises only the header for a no-copy frame and leaves the payload to
/// the application; the send callback deposits both here for the driver to collect after
/// [`crate::session::Session::send_into`].
///
/// The header is an inline nine-octet array rather than a `Bytes`: it must be owned (it is
/// copied out of libnghttp2's own buffer, which is invalidated the instant the frame
/// completes), it is exactly nine octets, and allocating a `Bytes` per frame would show
/// up in the steady-state allocation harness. The payload is the caller's own `Bytes`,
/// moved out of the staging slot, so it costs no allocation at all.
#[cfg(feature = "http")]
#[derive(Debug)]
pub(crate) struct SendRecord {
    pub(crate) header: [u8; 9],
    pub(crate) payload: Bytes,
}

/// Outgoing message bodies, keyed by the stream they belong to.
///
/// Entries are owned as raw pointers rather than `Box`es. That is deliberate: libnghttp2
/// keeps the address of each entry in its data-source union and writes through it while a
/// mutable borrow of this registry is live. Holding the entries in `Box`es would assert a
/// uniqueness guarantee those writes violate, so ownership here is manual and released
/// explicitly.
#[derive(Default)]
pub(crate) struct BodyRegistry {
    entries: HashMap<StreamId, NonNull<BodyEntry>>,
}

// SAFETY: the registry owns its entries exclusively, and `BodyEntry` holds only a
// `Source` — a `Box<dyn BodySource>` or, under the `http` feature, a
// `Box<dyn SharedBodySource>`, both bounded `Send` — together with a staged `Bytes`,
// which is itself `Send`. Moving the registry between threads therefore moves only data
// that is itself `Send`.
unsafe impl Send for BodyRegistry {}

impl BodyRegistry {
    /// Takes ownership of `entry`, returning the address libnghttp2 should hold.
    ///
    /// The entry is not yet associated with a stream: submission assigns the identifier,
    /// and only then can it be recorded with [`Self::attach`].
    pub(crate) fn prepare(entry: BodyEntry) -> NonNull<BodyEntry> {
        let raw = Box::into_raw(Box::new(entry));
        // SAFETY: `Box::into_raw` never yields null.
        unsafe { NonNull::new_unchecked(raw) }
    }

    /// Records a prepared entry against the stream it was assigned.
    pub(crate) fn attach(&mut self, stream: StreamId, entry: NonNull<BodyEntry>) {
        if let Some(previous) = self.entries.insert(stream, entry) {
            // Reaching here would mean one stream carried two bodies. Callers prevent it:
            // a request is always given a fresh identifier, and a response requires an
            // open stream that the duplicate guard has not already claimed.
            //
            // The previous entry is deliberately NOT freed. libnghttp2 may still hold its
            // address in a queued outbound item, so freeing it would be a use-after-free
            // waiting to happen, whereas leaking it merely wastes memory. Given the choice
            // between unsound and untidy, this takes untidy.
            debug_assert!(
                false,
                "two bodies attached to stream {stream}; the previous entry was leaked"
            );
            let _ = previous;
        }
    }

    /// Drops a prepared entry that was never attached, because submission failed.
    pub(crate) fn discard(entry: NonNull<BodyEntry>) {
        Self::release_raw(entry);
    }

    /// Drops the entry belonging to `stream`, if any.
    pub(crate) fn detach(&mut self, stream: StreamId) {
        if let Some(entry) = self.entries.remove(&stream) {
            Self::release_raw(entry);
        }
    }

    /// Whether this stream's body has announced that trailers may follow.
    pub(crate) fn trailers_ready(&self, stream: StreamId) -> bool {
        self.entries.get(&stream).is_some_and(|entry| {
            // SAFETY: the pointer came from `prepare` and is owned by this registry, so it
            // is live. No trampoline can be running while this shared borrow exists: both
            // require the session, and the caller holds it.
            unsafe { entry.as_ref() }.trailers_ready
        })
    }

    fn release_raw(entry: NonNull<BodyEntry>) {
        // SAFETY: every pointer in this registry came from `prepare`, which produced it
        // with `Box::into_raw`, and each is released exactly once.
        drop(unsafe { Box::from_raw(entry.as_ptr()) });
    }
}

impl Drop for BodyRegistry {
    fn drop(&mut self) {
        for (_, entry) in self.entries.drain() {
            Self::release_raw(entry);
        }
    }
}

impl core::fmt::Debug for BodyRegistry {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BodyRegistry")
            .field("streams", &self.entries.len())
            .finish()
    }
}

/// Errors reported by a caller's body source, held until the stream closes.
///
/// A body source signals failure by returning, which libnghttp2 turns into a stream
/// reset; the error itself has nowhere to go at that moment. Parking it here lets the
/// stream-close handler hand it back to the caller.
#[derive(Debug, Default)]
pub(crate) struct PendingErrors {
    by_stream: HashMap<StreamId, BodyError>,
}

impl PendingErrors {
    pub(crate) fn park(&mut self, stream: StreamId, error: BodyError) {
        self.by_stream.insert(stream, error);
    }

    pub(crate) fn take(&mut self, stream: StreamId) -> Option<BodyError> {
        self.by_stream.remove(&stream)
    }
}

/// Streams that already carry a response.
///
/// libnghttp2 documents submitting a second response for one stream as a programming
/// error that may crash, so it has to be caught before the call is made.
#[derive(Debug, Default)]
pub(crate) struct ResponseGuard {
    responded: HashSet<StreamId>,
}

impl ResponseGuard {
    /// Records that `stream` now carries a response.
    ///
    /// Returns `false` if one was already submitted, which the caller must treat as a
    /// rejection rather than forwarding to libnghttp2.
    pub(crate) fn claim(&mut self, stream: StreamId) -> bool {
        self.responded.insert(stream)
    }

    /// Forgets a stream, so a later stream reusing the identifier starts clean.
    pub(crate) fn release(&mut self, stream: StreamId) {
        self.responded.remove(&stream);
    }
}

/// Tracks whether the session is part-way through a frame, by counting bytes.
///
/// # Why this is not driven by callbacks
///
/// libnghttp2 offers a callback that fires once a frame header has been parsed, and it is
/// tempting to pair that with the frame-received callback and call the gap "mid-frame".
/// That pairing does not hold. A valid `PRIORITY` frame completes through
/// `session_inbound_frame_reset` without ever reaching the frame-received callback
/// (`deps/nghttp2/lib/nghttp2_session.c:6218`), as do the paths that discard an ignored
/// payload (`:6523`). Any of those would leave such a tracker stuck mid-frame, and a
/// later clean close would then be misreported as a truncated one — the precise error
/// this exists to avoid making.
///
/// So the frame boundaries are counted here instead. After the connection preface, an
/// HTTP/2 connection is nothing but a sequence of frames, each a nine-octet header whose
/// last three octets carry the payload length. Walking that structure needs no cooperation
/// from libnghttp2 and cannot be wrong-footed by which callbacks it chooses to invoke:
/// the arithmetic is over the same bytes the caller already handed us.
///
/// This also detects truncation *inside* a frame header, which the callback approach
/// could not see at all.
#[derive(Debug)]
pub(crate) struct FrameProgress {
    state: Framing,
    /// Header octets seen so far, retained across reads because a header may be split and
    /// its last three octets carry the payload length.
    pending: [u8; FRAME_HEADER],
}

#[derive(Debug)]
enum Framing {
    /// Awaiting the remainder of the client connection preface. Server sessions only.
    Preface(usize),
    /// Between frames, or part-way through a frame header: how many of the nine octets
    /// have arrived.
    Header(usize),
    /// Inside a frame payload: how many octets are still to come.
    Payload(usize),
}

/// The nine-octet frame header every HTTP/2 frame begins with.
const FRAME_HEADER: usize = 9;

/// The client connection preface, which a server receives before any frame.
const CLIENT_PREFACE: usize = 24;

impl FrameProgress {
    /// A tracker for a session in the given role.
    ///
    /// Only a server receives the client connection preface, so only a server has to skip
    /// it before frames begin.
    pub(crate) const fn new(expects_preface: bool) -> Self {
        Self {
            state: if expects_preface {
                Framing::Preface(CLIENT_PREFACE)
            } else {
                Framing::Header(0)
            },
            pending: [0; FRAME_HEADER],
        }
    }

    /// Accounts for bytes handed to the session, advancing through frame boundaries.
    pub(crate) fn advance(&mut self, mut input: &[u8]) {
        while !input.is_empty() {
            match self.state {
                Framing::Preface(remaining) => {
                    let taken = remaining.min(input.len());
                    input = &input[taken..];
                    self.state = if taken == remaining {
                        Framing::Header(0)
                    } else {
                        Framing::Preface(remaining - taken)
                    };
                }
                Framing::Header(have) => {
                    let want = FRAME_HEADER - have;
                    let taken = want.min(input.len());

                    if taken < want {
                        // A header split across reads. Only the length matters, and it is
                        // in the first three octets, so a partial header is remembered by
                        // its length alone — but the octets themselves must be retained
                        // to read that length once it is complete.
                        self.pending[have..have + taken].copy_from_slice(&input[..taken]);
                        self.state = Framing::Header(have + taken);
                        return;
                    }

                    self.pending[have..FRAME_HEADER].copy_from_slice(&input[..taken]);
                    input = &input[taken..];

                    let length =
                        u32::from_be_bytes([0, self.pending[0], self.pending[1], self.pending[2]])
                            as usize;

                    self.state = if length == 0 {
                        Framing::Header(0)
                    } else {
                        Framing::Payload(length)
                    };
                }
                Framing::Payload(remaining) => {
                    let taken = remaining.min(input.len());
                    input = &input[taken..];
                    self.state = if taken == remaining {
                        Framing::Header(0)
                    } else {
                        Framing::Payload(remaining - taken)
                    };
                }
            }
        }
    }

    /// Whether a frame is currently incomplete.
    ///
    /// False between frames and while the preface is still arriving; true once any part
    /// of a frame has arrived and the frame is not yet whole, header included.
    pub(crate) const fn in_frame(&self) -> bool {
        match self.state {
            Framing::Preface(_) => false,
            Framing::Header(have) => have > 0,
            Framing::Payload(_) => true,
        }
    }
}
