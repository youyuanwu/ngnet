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
pub struct RecordWriter<'c, 'h, 'b> {
    conn: &'c mut Conn<'h>,
    buf: &'b mut [u8],
    now: Timestamp,
    /// Set once dwnx reports the record finished, so `finish` does not call again.
    len: Option<usize>,
    too_small: bool,
}

impl<'c, 'h, 'b> RecordWriter<'c, 'h, 'b> {
    pub(crate) fn new(conn: &'c mut Conn<'h>, buf: &'b mut [u8], now: Timestamp) -> Self {
        let too_small = buf.len() < MIN_USABLE_BUFFER;
        Self {
            conn,
            buf,
            now,
            len: None,
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
        if self.too_small || self.len.is_some() {
            // Nothing more can go in: either the buffer never had room, or the record closed.
            return Ok(Push::Complete { consumed: None });
        }

        let stream_id = request.stream.map_or(-1, StreamId::get);
        let flags = if request.fin {
            sys::DWNX_WRITE_STREAM_FLAG_FIN
        } else {
            sys::DWNX_WRITE_STREAM_FLAG_NONE
        };

        let mut pdatalen: sys::dwnx_ssize = 0;
        let buf_ptr = self.buf.as_mut_ptr();
        let buf_len = self.buf.len();
        let data_ptr = request.data.as_ptr();
        let data_len = request.data.len();
        let now = self.now.as_nanos();

        let rv = self.conn.with_bridge(|raw| {
            // SAFETY: `raw` is non-null. `buf_ptr` is writable for `buf_len` bytes and stays
            // valid for the whole record, because `self` holds the borrow -- which is the
            // reason this type exists. `data_ptr` is readable for `data_len` for the duration
            // of the call, which is all dwnx needs: it copies stream data into the record and
            // has no retransmission layer that would keep the pointer.
            unsafe {
                sys::dwnx_conn_write_stream(
                    raw,
                    buf_ptr,
                    buf_len,
                    &mut pdatalen,
                    flags,
                    stream_id,
                    data_ptr,
                    data_len,
                    now,
                )
            }
        });

        // dwnx signals "took no stream data" with -1, which must not become a length.
        let consumed = usize::try_from(pdatalen).ok();

        if rv > 0 {
            let len = usize::try_from(rv).map_err(|_| {
                Error::validation(
                    ErrorKind::Internal,
                    "dwnx reported an unrepresentable record length",
                )
            })?;
            self.len = Some(len);
            return Ok(Push::Complete { consumed });
        }

        // `dwnx_ssize` is a ptrdiff_t while the error constants are ints; narrow once so they
        // can be compared, which is safe because every negative return is one of them.
        let rv = i32::try_from(rv).unwrap_or(sys::DWNX_ERR_INTERNAL);

        match rv {
            // Nothing to send at all: the record closes empty.
            0 => {
                self.len = Some(0);
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

        Ok(match self.len {
            Some(0) | None => Record::Empty,
            Some(len) => Record::Bytes(&self.buf[..len]),
        })
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
