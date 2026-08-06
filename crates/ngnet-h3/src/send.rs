//! The send half of the connection: a two-phase transaction made hard to misuse.
//!
//! nghttp3 does not hand over a buffer to copy. `nghttp3_conn_writev_stream` fills an
//! array of vectors that borrow both nghttp3's own serialisation buffers and the
//! application's body buffers, and the caller must afterwards call
//! `nghttp3_conn_add_write_offset` to say how many of those bytes the QUIC stack actually
//! took. The next `writev_stream` invalidates the vectors.
//!
//! That is a borrow spanning two FFI calls with library mutation in between, which is
//! exactly the shape a safe wrapper has to take seriously. [`SendGuard`] does it by
//! borrowing the connection for as long as the vectors are alive and consuming itself in
//! [`SendGuard::commit`], so using the bytes afterwards is a borrow-check error rather
//! than a documented rule.
//!
//! # The zero-length final write
//!
//! `writev_stream` can return zero bytes with a stream identifier and the fin flag set.
//! That is not "nothing to do": it is a stream ending with no further payload, and it
//! still has to be committed, with zero, or the connection never advances past it. The
//! guard surfaces it as an ordinary offer whose payload happens to be empty, so a caller
//! writing the obvious loop handles it without knowing it exists.

use std::io::IoSlice;

use ngnet_h3_sys as sys;

use crate::conn::Conn;
use crate::error::{Error, Result};
use crate::stream::StreamId;

/// How many vectors to ask nghttp3 for at once.
///
/// nghttp3 imposes no maximum here, unlike its data-source callback, which is capped at
/// eight. Sixteen is chosen to comfortably exceed the number of buffers a single pass
/// produces in practice while staying a stack array.
const MAX_VECTORS: usize = 16;

/// Bytes the connection wants written, and the stream they belong to.
///
/// Borrows the connection: nothing else can be done with it until the transaction is
/// closed by [`Self::commit`] or the guard is dropped.
///
/// # Why this is a guard rather than a returned buffer
///
/// The bytes are borrowed from nghttp3's own buffers and from the application's body
/// buffers, and the next call to `writev_stream` invalidates them. Using them after
/// committing would therefore be a use-after-free. `commit` takes `self` by value, so that
/// is a compile error rather than a documented rule:
///
/// ```compile_fail
/// # use ngnet_h3::{ConnBuilder, Role, StreamId};
/// # fn main() -> Result<(), ngnet_h3::Error> {
/// let mut conn = ConnBuilder::<()>::new(Role::Client).build()?;
/// conn.bind_control_stream(StreamId::new(2)?)?;
/// conn.bind_qpack_streams(StreamId::new(6)?, StreamId::new(10)?)?;
///
/// let send = conn.writev_stream()?.unwrap();
/// let borrowed = send.slices();
/// send.commit(0)?;
/// // The bytes are gone; nghttp3 may already have reused the buffer behind them.
/// let _ = borrowed[0];
/// # Ok(())
/// # }
/// ```
///
/// The guard also holds the connection, so a second overlapping transaction cannot be
/// opened while one is in flight:
///
/// ```compile_fail
/// # use ngnet_h3::{ConnBuilder, Role, StreamId};
/// # fn main() -> Result<(), ngnet_h3::Error> {
/// let mut conn = ConnBuilder::<()>::new(Role::Client).build()?;
/// conn.bind_control_stream(StreamId::new(2)?)?;
/// conn.bind_qpack_streams(StreamId::new(6)?, StreamId::new(10)?)?;
///
/// let first = conn.writev_stream()?.unwrap();
/// let second = conn.writev_stream()?;  // `conn` is still borrowed by `first`
/// first.commit(0)?;
/// # Ok(())
/// # }
/// ```
pub struct SendGuard<'a, C> {
    conn: &'a mut Conn<C>,
    slices: [IoSlice<'a>; MAX_VECTORS],
    count: usize,
    stream: StreamId,
    fin: bool,
    total: usize,
}

impl<'a, C> SendGuard<'a, C> {
    pub(crate) fn acquire(conn: &'a mut Conn<C>, context: &mut C) -> Result<Option<Self>> {
        conn.require_ready_to_send()?;

        let mut vectors = [sys::nghttp3_vec {
            base: core::ptr::null_mut(),
            len: 0,
        }; MAX_VECTORS];
        let mut stream_id: i64 = -1;
        let mut fin: i32 = 0;

        // A bridge is installed for the duration of the call and no longer: collecting
        // bytes pulls from outgoing body sources through the data callback, but committing
        // afterwards fires nothing, so the guard itself does not hold the caller's state.
        let count = conn.with_context(context, |raw| {
            // SAFETY: `raw` is live, and `vectors` is a valid array of `MAX_VECTORS`
            // entries that outlives the call. nghttp3 writes the stream id and fin flag
            // through the out-pointers before it does anything else.
            unsafe {
                sys::nghttp3_conn_writev_stream(
                    raw,
                    &mut stream_id,
                    &mut fin,
                    vectors.as_mut_ptr(),
                    vectors.len(),
                )
            }
        });

        if count < 0 {
            let code = i32::try_from(count).unwrap_or(sys::NGHTTP3_ERR_FATAL);
            // Poisons: the header states that after this fails, calling anything but the
            // destructor is undefined behaviour.
            return Err(conn.record_send_failure(code, "could not collect data to send"));
        }

        // Nothing to send at all. Distinguished from a zero-length final write purely by
        // the stream identifier, which is why the fin case cannot simply be folded in.
        if stream_id < 0 {
            return Ok(None);
        }

        let stream = StreamId::new(stream_id)?;
        let count = count as usize;
        debug_assert!(count <= MAX_VECTORS);

        // Converted into `IoSlice` here rather than reinterpreted on demand. The two
        // layouts happen to agree on Unix, but `IoSlice` wraps `WSABUF` on Windows, which
        // orders the length first -- so casting between them would be unsound there and
        // would compile perfectly well.
        let empty: &'a [u8] = &[];
        let mut slices = [IoSlice::new(empty); MAX_VECTORS];
        let mut total = 0usize;
        for (slot, vector) in slices.iter_mut().zip(&vectors[..count]) {
            // nghttp3 never returns a zero-length vector, but a null base with a zero
            // length would still be undefined to pass to `from_raw_parts`.
            let bytes: &'a [u8] = if vector.len == 0 || vector.base.is_null() {
                empty
            } else {
                // SAFETY: nghttp3 guarantees the vector is readable for `len` bytes until
                // the next `writev_stream`, and this guard borrows the connection until it
                // is consumed, so no such call can intervene.
                unsafe { core::slice::from_raw_parts(vector.base, vector.len) }
            };
            total += bytes.len();
            *slot = IoSlice::new(bytes);
        }

        Ok(Some(Self {
            conn,
            slices,
            count,
            stream,
            fin: fin != 0,
            total,
        }))
    }

    /// The stream these bytes belong to.
    pub fn stream(&self) -> StreamId {
        self.stream
    }

    /// Whether writing these bytes ends the stream.
    pub fn fin(&self) -> bool {
        self.fin
    }

    /// The total number of bytes on offer.
    pub fn len(&self) -> usize {
        self.total
    }

    /// Whether there are no bytes on offer.
    ///
    /// True only for a stream that ends without further payload, which still has to be
    /// committed with zero.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// The bytes to write, as slices suitable for a vectored write.
    ///
    /// Reborrowed from `self` rather than from the connection, so the slices cannot
    /// outlive the transaction even if they are copied out of the returned array.
    pub fn slices(&self) -> &[IoSlice<'_>] {
        &self.slices[..self.count]
    }

    /// Reports how many of the offered bytes the transport accepted, closing the
    /// transaction.
    ///
    /// Must be called even when nothing was accepted, and even when there was nothing on
    /// offer: nghttp3 does not advance a stream's send state until it is told, and a
    /// stream ending with an empty final write is committed with zero like any other.
    ///
    /// Consuming `self` is what makes the offered bytes unusable afterwards; the next
    /// `writev_stream` would invalidate them.
    pub fn commit(self, accepted: usize) -> Result<()> {
        if accepted > self.total {
            return Err(Error::invalid_input(
                "committed more bytes than were offered",
            ));
        }

        let stream = self.stream;
        // Deconstructed before the FFI call so the borrow of the connection is exclusive.
        let conn = self.conn;

        // SAFETY: `raw` is live, the stream identifier came from nghttp3 itself moments
        // ago, and `accepted` has been checked not to exceed what was offered --
        // over-reporting would advance the send state past bytes never written.
        let rv = unsafe { sys::nghttp3_conn_add_write_offset(conn.raw(), stream.get(), accepted) };
        if rv != 0 {
            return Err(conn.record_recoverable(rv, "could not record the bytes written"));
        }
        // Recorded only after nghttp3 accepted it, so a failed commit cannot raise the
        // ceiling that bounds-checks acknowledgement.
        conn.record_committed(stream, accepted);
        Ok(())
    }

    /// Abandons the transaction without reporting anything.
    ///
    /// The same bytes are offered again on the next call. Dropping the guard does the
    /// same thing; this exists to say so at the call site.
    ///
    /// Note that abandoning a stream that ends with an empty final write makes no
    /// progress, so a loop that always abandons will not terminate. That is deliberate:
    /// silently committing zero would tell nghttp3 the stream ended when it did not.
    pub fn abandon(self) {}
}

impl<C> core::fmt::Debug for SendGuard<'_, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SendGuard")
            .field("stream", &self.stream)
            .field("fin", &self.fin)
            .field("len", &self.total)
            .field("vectors", &self.count)
            .finish()
    }
}
