//! Serialising outbound records.
//!
//! # Why this is a builder and not a single call
//!
//! The obvious shape for this API is one method taking `&mut [u8]`, returning either the bytes
//! written or a signal to call again. That shape is unsound here, and why is the most
//! important thing in this module.
//!
//! dwnx builds a record incrementally. The first call to `dwnx_conn_writev_stream` reaches
//! `dwnx_qre_start`, which stores the caller's `dest` pointer inside the connection
//! (`dwnx_qre.c`). Subsequent calls in the same sequence -- the ones invited by
//! `DWNX_ERR_WRITE_MORE` -- do not re-read `dest`; they append through the pointer retained
//! from the first. The buffer therefore has to stay alive, and stay put, for the *whole*
//! sequence rather than for one call.
//!
//! A method taking `&mut [u8]` per call cannot express that. Safe code could pass a temporary,
//! see `WriteMore`, and pass a different buffer next time; dwnx would keep writing through the
//! first pointer, which may since have been freed. That is a use-after-free reachable without
//! writing `unsafe`, which is the one thing this crate exists to prevent.
//!
//! So the buffer is borrowed once, by [`RecordWriter`], for as long as the record is being
//! built, and the borrow checker enforces what dwnx's documentation only implies.
//!
//! # The loop, and the outcomes it turns on
//!
//! Three of the negative codes `dwnx_conn_writev_stream` returns are not failures but
//! instructions: `WRITE_MORE` means "there is room left, add something else",
//! `STREAM_DATA_BLOCKED` means "that stream is flow-control blocked, try another", and
//! `STREAM_SHUT_WR` means "that stream's write side is closed". A caller who treats any of the
//! three as an error gets a connection that silently stalls, so each is a [`Push`] variant
//! rather than an [`Error`].
//!
//! # Two cases dwnx cannot express, handled here
//!
//! `dwnx_conn_write_vmsg` returns `0` both when there is nothing to send and when the buffer
//! is under three bytes, which are entirely different situations. The buffer is therefore
//! checked before the first call. The guard is that three-byte floor and not the record size:
//! the C documentation explicitly permits a smaller buffer when the transport cannot take a
//! full record, so rejecting everything below 16382 would refuse supported usage.
//!
//! `DWNX_ERR_NOBUF` never reaches a caller from this path. It is dwnx's internal "this record
//! is full" signal, swallowed at each site that can raise it, after which the record is
//! finalised and its length returned. It stays a mapped [`ErrorKind`] because the *constructor*
//! returns it, but it has no outcome here.

use ngnet_qmux_sys as sys;

use crate::conn::Conn;
use crate::error::{Error, ErrorKind};
use crate::stream::StreamId;
use crate::time::Timestamp;

/// The smallest buffer dwnx can put anything into.
///
/// Below this it returns `0`, which is indistinguishable from an idle connection.
const MIN_USABLE_BUFFER: usize = 3;

/// How many fragments one call to [`RecordWriter::push_vectored`] hands dwnx.
///
/// The array of `dwnx_vec` has to be contiguous and it has to outlive the call, so it is a
/// stack array of a fixed width rather than an allocation made per push -- this is the write
/// path, and a push that allocated would cost more than the record boundary the vectored form
/// exists to remove. Sixteen because that is the widest offer the HTTP/3 layer above can make
/// (`crates/ngnet-h3/src/send.rs`, `MAX_VECTORS`), so the common caller never reaches the
/// truncation below.
///
/// A caller lending more than this is not refused; see [`RecordWriter::push_vectored`] for what
/// happens instead and why the end-of-stream marker is what makes it delicate.
pub(crate) const MAX_VECTORS: usize = 16;

/// What to add to the record being built.
#[derive(Clone, Copy, Debug)]
pub struct WriteRequest<'a> {
    /// The stream to carry data for, if any.
    ///
    /// `None` asks dwnx to serialise pending control frames without adding stream data -- the
    /// equivalent of passing `-1` as the stream id.
    pub stream: Option<StreamId>,
    /// The data to send on that stream.
    pub data: &'a [u8],
    /// Whether this data ends the stream.
    pub fin: bool,
}

impl<'a> WriteRequest<'a> {
    /// Send nothing but whatever control frames are pending.
    #[must_use]
    pub const fn control_only() -> Self {
        Self {
            stream: None,
            data: &[],
            fin: false,
        }
    }

    /// Send data on a stream.
    #[must_use]
    pub const fn stream(stream: StreamId, data: &'a [u8]) -> Self {
        Self {
            stream: Some(stream),
            data,
            fin: false,
        }
    }

    /// Mark this data as ending the stream.
    #[must_use]
    pub const fn with_fin(mut self, fin: bool) -> Self {
        self.fin = fin;
        self
    }
}

/// What to add to the record being built, lent as several fragments.
///
/// The vectored form of [`WriteRequest`]. The fragments are one payload for one stream, not
/// one payload each: dwnx concatenates them into a single STREAM frame, so a caller holding a
/// header and a body separately pays one frame's framing rather than two records'.
///
/// Empty fragments are permitted anywhere in the list and are simply not submitted, which
/// matches what dwnx itself does with a zero-length single slice. A list that is empty, or
/// entirely empty fragments, is a request to serialise nothing -- with `fin` set, that is how
/// a stream that has finished writing is ended.
#[derive(Clone, Copy, Debug)]
pub struct VectoredWriteRequest<'a, 'd> {
    /// The stream to carry data for, if any.
    ///
    /// `None` asks dwnx to serialise pending control frames without adding stream data, and
    /// the fragments are then ignored -- exactly as in [`WriteRequest`].
    pub stream: Option<StreamId>,
    /// The fragments to send on that stream, in order.
    pub data: &'a [&'d [u8]],
    /// Whether these fragments end the stream.
    pub fin: bool,
}

impl<'a, 'd> VectoredWriteRequest<'a, 'd> {
    /// Send several fragments on a stream.
    #[must_use]
    pub const fn stream(stream: StreamId, data: &'a [&'d [u8]]) -> Self {
        Self {
            stream: Some(stream),
            data,
            fin: false,
        }
    }

    /// Mark these fragments as ending the stream.
    #[must_use]
    pub const fn with_fin(mut self, fin: bool) -> Self {
        self.fin = fin;
        self
    }
}

/// The result of adding to a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Push {
    /// Some or all of the data was packed, and the record still has room.
    ///
    /// Push again -- with the remaining data, with another stream, or with
    /// [`WriteRequest::control_only`] to stop adding and let the record close.
    Accepted {
        /// How many bytes of the submitted data were taken.
        ///
        /// `None` means the record carried other frames instead and took none of it. dwnx
        /// signals this with `-1`, which is distinct from taking zero bytes.
        consumed: Option<usize>,
    },
    /// The record is complete. Call [`RecordWriter::finish`] to take it.
    Complete {
        /// How many bytes of the submitted data this final push took.
        consumed: Option<usize>,
    },
    /// The nominated stream is blocked by flow control. Nothing was taken from it, though
    /// another stream may still make progress in this record.
    StreamBlocked,
    /// The nominated stream's write side is closed. Its data will never be taken.
    StreamClosed,
}

/// A finished record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Record<'b> {
    /// Bytes to hand to the transport.
    Bytes(&'b [u8]),
    /// Nothing needed sending.
    Empty,
    /// The buffer was too small to hold anything at all.
    BufferTooSmall,
}

impl<'b> Record<'b> {
    /// The bytes, if any were produced.
    #[must_use]
    pub const fn bytes(self) -> Option<&'b [u8]> {
        match self {
            Self::Bytes(bytes) => Some(bytes),
            Self::Empty | Self::BufferTooSmall => None,
        }
    }
}

/// Builds one record into a caller-supplied buffer.
///
/// The buffer is borrowed for the lifetime of this value because dwnx retains it across the
/// whole sequence; see the module documentation.
///
/// # Always call [`RecordWriter::finish`]
///
/// Dropping a writer mid-record is *safe* — the `Drop` impl closes the record, so dwnx is
/// never left holding a buffer whose borrow has ended — but it is not free. dwnx advances a
/// stream's send offset as soon as data is packed, so any bytes already taken are lost, and
/// the peer will reject the next record on that stream as a gap it can never fill. Use
/// [`Conn::write`] where possible; it finishes the record for you.
pub struct RecordWriter<'c, 'h, 'b> {
    conn: &'c mut Conn<'h>,
    /// `Option` so that `finish` can take the borrow back out and hand the caller a slice of
    /// it. A plain `&'b mut [u8]` cannot be moved out of a type with a `Drop` impl, and the
    /// `Drop` impl is what keeps the API sound.
    buf: Option<&'b mut [u8]>,
    now: Timestamp,
    /// Set once dwnx reports the record finished, so `finish` does not call again.
    len: Option<usize>,
    /// Whether dwnx has an unfinished record pointing into `buf`.
    ///
    /// Set by the first push that dwnx accepts, cleared once the record is finalised. See the
    /// `Drop` impl for why this has to be tracked rather than inferred.
    started: bool,
    too_small: bool,
}

impl<'c, 'h, 'b> RecordWriter<'c, 'h, 'b> {
    pub(crate) fn new(conn: &'c mut Conn<'h>, buf: &'b mut [u8], now: Timestamp) -> Self {
        let too_small = buf.len() < MIN_USABLE_BUFFER;
        Self {
            conn,
            buf: Some(buf),
            now,
            len: None,
            started: false,
            too_small,
        }
    }

    /// Add stream data, or ask for pending control frames.
    ///
    /// # Errors
    ///
    /// Returns an error only for conditions that end the connection; the recoverable signals
    /// are [`Push`] variants.
    pub fn push(&mut self, request: WriteRequest<'_>) -> Result<Push, Error> {
        // One fragment is the degenerate vectored case, and it is spelled that way rather
        // than given a second call into dwnx: `dwnx_conn_write_stream` is itself a wrapper
        // that builds a one-element `dwnx_vec` array and calls `dwnx_conn_writev_stream`
        // (`deps/dwnx/lib/dwnx_conn.c`), so a separate path here would be a second copy of
        // the outcome mapping below with nothing else to distinguish it.
        self.push_vectored(VectoredWriteRequest {
            stream: request.stream,
            data: &[request.data],
            fin: request.fin,
        })
    }

    /// Add several fragments of one stream's data to the record being built.
    ///
    /// The fragments become **one** STREAM frame, not one each: dwnx concatenates them as it
    /// copies them into the record. That is the whole point of the call. A caller holding a
    /// payload in pieces -- the HTTP/3 layer above holds a request's headers and its body
    /// separately, and offers both at once -- would otherwise start a record per piece and
    /// pay a record's framing for each.
    ///
    /// # The count that comes back is a total, not a per-fragment tally
    ///
    /// [`Push::Accepted`] and [`Push::Complete`] report how many bytes were taken **across
    /// all the fragments**, in order (`deps/dwnx/lib/includes/dwnx/dwnx.h`, and
    /// `dwnx_conn_write_stream_frame` in `deps/dwnx/lib/dwnx_conn.c`, which sets `*pdatalen`
    /// to the single figure `wdatalen`). A caller resuming after a short take therefore has
    /// to walk the array to find where the next push starts, and cannot assume whole
    /// fragments were consumed: dwnx copies a *prefix* of the concatenation
    /// (`dwnx_frame_encode_stream`), so a take can and does stop in the middle of a fragment.
    ///
    /// # The fragments are borrowed for the call and no longer
    ///
    /// Only `dest` is retained across a record; the fragments are copied into it during the
    /// call, by `dwnx_cpymem` inside `dwnx_frame_encode_stream`. There is no retransmission
    /// layer to keep them for. That is what lets a caller hand over memory it reclaims as
    /// soon as this returns.
    ///
    /// # More fragments than one call can carry
    ///
    /// At most sixteen fragments are submitted per call, empty ones aside -- see the
    /// `MAX_VECTORS` constant in this module for why sixteen. A caller lending more is not
    /// refused: it gets the first sixteen taken and pushes the rest, which join the same
    /// record.
    ///
    /// **The end-of-stream marker is suppressed on such a call**, and that is not tidiness.
    /// dwnx sets the FIN bit when the data it was *given in this call* fits entirely, not
    /// when the caller's whole payload does (`dwnx_conn_write_stream_frame`'s
    /// `datalen == (uint64_t)wdatalen`). A truncated submission that happened to fit would
    /// otherwise end the stream with fragments still unsent, and the peer would see a stream
    /// that finished early with no error to explain it.
    ///
    /// # Errors
    ///
    /// As [`RecordWriter::push`].
    pub fn push_vectored(&mut self, request: VectoredWriteRequest<'_, '_>) -> Result<Push, Error> {
        if self.too_small || self.len.is_some() {
            // Nothing more can go in: either the buffer never had room, or the record closed.
            return Ok(Push::Complete { consumed: None });
        }

        // Empty fragments are dropped rather than submitted, which is what dwnx's own
        // single-slice wrapper does with a zero-length payload -- it passes a vector count of
        // zero. Dropping them also means a list padded with empties still fits its real
        // fragments into one call.
        let mut vectors = [sys::dwnx_vec {
            base: core::ptr::null_mut(),
            len: 0,
        }; MAX_VECTORS];
        let mut count = 0usize;
        let mut whole = true;
        for fragment in request.data {
            if fragment.is_empty() {
                continue;
            }
            if count == MAX_VECTORS {
                whole = false;
                break;
            }
            vectors[count] = sys::dwnx_vec {
                // `dwnx_vec.base` is `*mut u8` in the generated bindings because the C struct
                // is shared with the receive side, where dwnx does write through it. Nothing
                // on this path does: the vectors reach `dwnx_frame_encode_stream`, which
                // reads them into the record and never writes back.
                base: fragment.as_ptr().cast_mut(),
                len: fragment.len(),
            };
            count += 1;
        }

        let stream_id = request.stream.map_or(-1, StreamId::get);
        let flags = if request.fin && whole {
            sys::DWNX_WRITE_STREAM_FLAG_FIN
        } else {
            sys::DWNX_WRITE_STREAM_FLAG_NONE
        };

        let mut pdatalen: sys::dwnx_ssize = 0;
        let buf = self.buf.as_mut().expect("buffer is taken only by finish");
        let buf_ptr = buf.as_mut_ptr();
        let buf_len = buf.len();
        let data_ptr = vectors.as_ptr();
        let now = self.now.as_nanos();

        let rv = self.conn.with_bridge(|raw| {
            // SAFETY: `raw` is non-null. `buf_ptr` is writable for `buf_len` bytes and stays
            // valid for the whole record, because `self` holds the borrow -- which is the
            // reason this type exists. `data_ptr` is readable for `count` elements, each of
            // which points at one of the caller's fragments and is readable for its own
            // length, for the duration of the call -- which is all dwnx needs: it copies
            // stream data into the record and has no retransmission layer that would keep
            // either the array or what it points at.
            unsafe {
                sys::dwnx_conn_writev_stream(
                    raw,
                    buf_ptr,
                    buf_len,
                    &mut pdatalen,
                    flags,
                    stream_id,
                    data_ptr,
                    count,
                    now,
                )
            }
        });

        // dwnx signals "took no stream data" with -1, which must not become a length.
        let consumed = usize::try_from(pdatalen).ok();

        // Anything but the three per-stream signals means dwnx reached `dwnx_qre_start` and is
        // now holding `buf`. See the `Drop` impl.
        if !matches!(
            rv,
            _ if rv as i64 == i64::from(sys::DWNX_ERR_STREAM_DATA_BLOCKED)
                || rv as i64 == i64::from(sys::DWNX_ERR_STREAM_SHUT_WR)
        ) {
            self.started = true;
        }

        if rv > 0 {
            let len = usize::try_from(rv).map_err(|_| {
                Error::validation(
                    ErrorKind::Internal,
                    "dwnx reported an unrepresentable record length",
                )
            })?;
            self.len = Some(len);
            self.started = false;
            return Ok(Push::Complete { consumed });
        }

        // `dwnx_ssize` is a ptrdiff_t while the error constants are ints; narrow once so they
        // can be compared, which is safe because every negative return is one of them.
        let rv = i32::try_from(rv).unwrap_or(sys::DWNX_ERR_INTERNAL);

        match rv {
            // Nothing to send at all: the record closes empty.
            0 => {
                self.len = Some(0);
                self.started = false;
                Ok(Push::Complete { consumed })
            }
            sys::DWNX_ERR_WRITE_MORE => Ok(Push::Accepted { consumed }),
            sys::DWNX_ERR_STREAM_DATA_BLOCKED => Ok(Push::StreamBlocked),
            sys::DWNX_ERR_STREAM_SHUT_WR => Ok(Push::StreamClosed),
            rv => Err(self.conn.error_from(rv, "serialising a record")),
        }
    }

    /// Close the record and take the bytes.
    ///
    /// If dwnx has not already declared the record finished, this makes a final control-only
    /// push to tell it nothing more is coming.
    ///
    /// # Errors
    ///
    /// As [`RecordWriter::push`].
    pub fn finish(mut self) -> Result<Record<'b>, Error> {
        if self.too_small {
            return Ok(Record::BufferTooSmall);
        }

        if self.len.is_none() {
            // The result is deliberately ignored: whatever it says, the caller has stopped
            // adding, so the record is done. What matters is that dwnx has now finalised it
            // and recorded the length in `self.len`.
            let _ = self.push(WriteRequest::control_only())?;
        }

        // Taking the buffer out both hands the caller its slice and tells `Drop` there is
        // nothing left to finalise.
        let buf = self.buf.take().expect("buffer is taken only once");

        Ok(match self.len {
            Some(0) | None => Record::Empty,
            Some(len) => Record::Bytes(&buf[..len]),
        })
    }
}

/// Finalise a record the caller abandoned.
///
/// This is not tidiness; without it the safe API admits a write-after-free.
///
/// `dwnx_qre_start` stores the caller's buffer in the connection and sets a "started" flag,
/// and only `dwnx_qre_final` clears it (`dwnx_qre.c`). A later call skips `qre_start` while
/// that flag is set, so it appends through the *retained* pointer and ignores whatever buffer
/// it was just given. A `RecordWriter` dropped mid-record therefore leaves the connection
/// holding a pointer into a buffer whose borrow has ended -- and the next write, with a
/// different and entirely valid buffer, scribbles into the old one.
///
/// Reaching that needs no `unsafe`: pushing once, seeing `Accepted`, and returning early is
/// enough, and `Push::StreamBlocked` and `Push::StreamClosed` positively invite a caller to
/// stop pushing. `Conn::write`'s `?` on a failed push is a second route.
///
/// So an unfinished record is closed here. A control-only write always reaches
/// `dwnx_qre_final`, which clears the flag and leaves the connection ready for the next
/// record. The bytes are discarded, which is correct: nobody asked for them.
///
/// This makes abandonment *safe*, not free. Any stream data already packed is lost, because
/// dwnx advanced the stream's send offset when it took the bytes, and the peer will reject the
/// next record on that stream as a gap. `RecordWriter`'s own documentation says so, and a test
/// pins it.
impl Drop for RecordWriter<'_, '_, '_> {
    fn drop(&mut self) {
        let Some(buf) = self.buf.as_mut() else {
            // `finish` took the buffer, so the record is already closed.
            return;
        };
        if !self.started {
            return;
        }

        let mut pdatalen: sys::dwnx_ssize = 0;
        let buf_ptr = buf.as_mut_ptr();
        let buf_len = buf.len();
        let now = self.now.as_nanos();

        self.conn.with_bridge(|raw| {
            // SAFETY: the same buffer this record was started with, still borrowed by `self`
            // and so still valid. Passing `-1` as the stream id adds no stream data, which is
            // what makes this always terminate the record rather than invite another push.
            unsafe {
                sys::dwnx_conn_write_stream(
                    raw,
                    buf_ptr,
                    buf_len,
                    &mut pdatalen,
                    sys::DWNX_WRITE_STREAM_FLAG_NONE,
                    -1,
                    core::ptr::null(),
                    0,
                    now,
                )
            }
        });
    }
}

impl<'h> Conn<'h> {
    /// Begin building a record in `buf`.
    ///
    /// Use this to pack several streams into one record. For a single payload,
    /// [`Conn::write`] drives the loop correctly on its own.
    ///
    /// The buffer should normally be [`crate::DEFAULT_MAX_RECORD_SIZE`] bytes; a smaller one is
    /// permitted and yields a smaller record.
    ///
    /// # A larger one is not permitted, and nothing here refuses it
    ///
    /// dwnx fills whatever buffer it is given and then describes the result with a **fixed
    /// two-byte** record length (`dwnx_qre_final` in `deps/dwnx/lib/dwnx_qre.c`), so a record
    /// built into a longer buffer can reach a length that prefix cannot express. The encoder
    /// asserts the value is below 16384 (`deps/dwnx/lib/dwnx_conv.c`) and, where that assertion
    /// is compiled out, truncates it to sixteen bits — which is a well-formed prefix in front
    /// of the wrong number of bytes, and a peer that has lost record framing with no error
    /// raised anywhere.
    ///
    /// Neither this call nor [`RecordWriter::push`] checks it, because the check belongs where
    /// the buffer is chosen and this crate is a binding rather than a policy. A caller that
    /// serialises into a larger buffer — the asynchronous layer does, into its outbound
    /// queue — passes a subslice of one record's width, not the whole of it.
    pub fn record<'c, 'b>(
        &'c mut self,
        buf: &'b mut [u8],
        now: Timestamp,
    ) -> RecordWriter<'c, 'h, 'b> {
        RecordWriter::new(self, buf, now)
    }

    /// Serialise one record, packing as much of `request` as fits.
    ///
    /// Returns the record and how many bytes of the submitted data went into it; anything left
    /// over belongs in the next record. This is the call that cannot get the loop wrong.
    ///
    /// # Errors
    ///
    /// As [`RecordWriter::push`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use ngnet_qmux::{Conn, Timestamp, WriteRequest};
    /// # fn f(conn: &mut Conn<'_>, stream: ngnet_qmux::StreamId, mut payload: &[u8], now: Timestamp)
    /// #     -> Result<(), ngnet_qmux::Error> {
    /// let mut buf = [0u8; 16_384];
    /// while !payload.is_empty() {
    ///     let (record, consumed) = conn.write(&mut buf, WriteRequest::stream(stream, payload), now)?;
    ///     if let Some(bytes) = record.bytes() {
    ///         // hand `bytes` to the transport
    ///         let _ = bytes;
    ///     }
    ///     if consumed == 0 {
    ///         break; // blocked; wait for the peer to extend the window
    ///     }
    ///     payload = &payload[consumed..];
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn write<'b>(
        &mut self,
        buf: &'b mut [u8],
        request: WriteRequest<'_>,
        now: Timestamp,
    ) -> Result<(Record<'b>, usize), Error> {
        let mut consumed_total = 0usize;
        let mut remaining = request.data;
        let mut record = self.record(buf, now);

        loop {
            let step = WriteRequest {
                stream: request.stream,
                data: remaining,
                fin: request.fin,
            };

            match record.push(step)? {
                Push::Accepted { consumed } => {
                    let consumed = consumed.unwrap_or(0);
                    consumed_total += consumed;
                    remaining = &remaining[consumed..];
                    if remaining.is_empty() {
                        // Everything is packed; let the record close.
                        break;
                    }
                }
                Push::Complete { consumed } => {
                    consumed_total += consumed.unwrap_or(0);
                    break;
                }
                Push::StreamBlocked | Push::StreamClosed => break,
            }
        }

        Ok((record.finish()?, consumed_total))
    }
}
