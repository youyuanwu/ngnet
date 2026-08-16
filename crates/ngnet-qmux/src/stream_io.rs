//! Opening, shutting down, and flow-controlling streams.

use ngnet_qmux_sys as sys;

use crate::conn::Conn;
use crate::error::Error;
use crate::stream::StreamId;

/// The result of asking to open a stream.
///
/// Deliberately not `#[non_exhaustive]`, unlike [`crate::Push`]: dwnx's open functions
/// document exactly two non-error results, success and `STREAM_ID_BLOCKED`, and a third would
/// be a change in the protocol rather than an addition to the API. Callers get to match
/// exhaustively and be told by the compiler if that ever stops being true.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenOutcome {
    /// The stream was opened.
    Opened(StreamId),
    /// No stream capacity remains right now.
    ///
    /// Not a failure: the peer has not yet permitted this many streams. Wait for the
    /// corresponding `extend_max_streams` event and try again. dwnx reports it as
    /// `STREAM_ID_BLOCKED`, which despite the name is the recoverable one of the three
    /// similarly named stream conditions.
    Blocked,
}

impl OpenOutcome {
    /// The stream id, if one was opened.
    #[must_use]
    pub const fn opened(self) -> Option<StreamId> {
        match self {
            Self::Opened(id) => Some(id),
            Self::Blocked => None,
        }
    }
}

/// Which half of a stream to shut down.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Shutdown {
    /// Stop reading; sends STOP_SENDING.
    Read,
    /// Stop writing; sends RESET_STREAM.
    Write,
    /// Both of the above.
    Both,
}

impl Conn<'_> {
    /// Open a bidirectional stream.
    ///
    /// # Errors
    ///
    /// Returns an error only for conditions that end the connection; exhausted capacity is
    /// [`OpenOutcome::Blocked`].
    pub fn open_bidi_stream(&mut self) -> Result<OpenOutcome, Error> {
        self.open_stream(sys::dwnx_conn_open_bidi_stream, "opening a bidi stream")
    }

    /// Open a unidirectional stream.
    ///
    /// # Errors
    ///
    /// As [`Conn::open_bidi_stream`].
    pub fn open_uni_stream(&mut self) -> Result<OpenOutcome, Error> {
        self.open_stream(sys::dwnx_conn_open_uni_stream, "opening a uni stream")
    }

    fn open_stream(
        &mut self,
        open: unsafe extern "C" fn(*mut sys::dwnx_conn, *mut i64, *mut core::ffi::c_void) -> i32,
        context: &'static str,
    ) -> Result<OpenOutcome, Error> {
        let mut stream_id: i64 = -1;

        let rv = self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null and `stream_id` is a live local. The stream user data
            // is null: this crate does not use dwnx's per-stream pointer, because handlers
            // receive stream ids and carry their own state.
            unsafe { open(raw, &mut stream_id, core::ptr::null_mut()) }
        });

        match rv {
            0 => Ok(OpenOutcome::Opened(StreamId::new(stream_id)?)),
            sys::DWNX_ERR_STREAM_ID_BLOCKED => Ok(OpenOutcome::Blocked),
            rv => Err(self.error_from(rv, context)),
        }
    }

    /// Shut down one or both halves of a stream.
    ///
    /// Shutting down a stream that does not exist is **not** an error: dwnx looks the id up and
    /// returns success when it finds nothing, so the result cannot be used to detect a bad id.
    /// That is dwnx's behaviour rather than this crate's choice, and it is reported faithfully.
    ///
    /// # Errors
    ///
    /// Returns an error only if the connection cannot continue.
    pub fn shutdown_stream(
        &mut self,
        stream: StreamId,
        half: Shutdown,
        app_error_code: u64,
    ) -> Result<(), Error> {
        let id = stream.get();
        let rv = self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null; the flags argument is documented as unused and passed
            // as zero, matching dwnx's own examples.
            unsafe {
                match half {
                    Shutdown::Read => sys::dwnx_conn_shutdown_stream_read(raw, 0, id, app_error_code),
                    Shutdown::Write => {
                        sys::dwnx_conn_shutdown_stream_write(raw, 0, id, app_error_code)
                    }
                    Shutdown::Both => sys::dwnx_conn_shutdown_stream(raw, 0, id, app_error_code),
                }
            }
        });

        if rv == 0 {
            Ok(())
        } else {
            Err(self.error_from(rv, "shutting down a stream"))
        }
    }

    /// Permit the peer to send `bytes` more on a stream.
    ///
    /// # Errors
    ///
    /// Returns an error if dwnx rejects the extension, e.g. for a locally-initiated
    /// unidirectional stream, which this endpoint never receives on.
    pub fn extend_max_stream_data(&mut self, stream: StreamId, bytes: u64) -> Result<(), Error> {
        let id = stream.get();
        let rv = self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null.
            unsafe { sys::dwnx_conn_extend_max_stream_offset(raw, id, bytes) }
        });

        if rv == 0 {
            Ok(())
        } else {
            Err(self.error_from(rv, "extending a stream window"))
        }
    }

    /// Permit the peer to send `bytes` more across the connection.
    pub fn extend_max_data(&mut self, bytes: u64) {
        self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null. Returns nothing and cannot fail.
            unsafe { sys::dwnx_conn_extend_max_offset(raw, bytes) }
        });
    }

    /// Permit the peer to open `count` more bidirectional streams.
    pub fn extend_max_streams_bidi(&mut self, count: usize) {
        self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null. Returns nothing and cannot fail.
            unsafe { sys::dwnx_conn_extend_max_streams_bidi(raw, count) }
        });
    }

    /// Permit the peer to open `count` more unidirectional streams.
    pub fn extend_max_streams_uni(&mut self, count: usize) {
        self.with_bridge(|raw| {
            // SAFETY: `raw` is non-null. Returns nothing and cannot fail.
            unsafe { sys::dwnx_conn_extend_max_streams_uni(raw, count) }
        });
    }

    /// How many more bidirectional streams this endpoint may open.
    #[must_use]
    pub fn streams_bidi_left(&self) -> u64 {
        // SAFETY: `raw` is non-null for the life of this value.
        unsafe { sys::dwnx_conn_get_streams_bidi_left(self.raw()) }
    }

    /// How many more unidirectional streams this endpoint may open.
    #[must_use]
    pub fn streams_uni_left(&self) -> u64 {
        // SAFETY: `raw` is non-null for the life of this value.
        unsafe { sys::dwnx_conn_get_streams_uni_left(self.raw()) }
    }

    /// How much more data this endpoint may send across the connection.
    #[must_use]
    pub fn max_data_left(&self) -> u64 {
        // SAFETY: `raw` is non-null for the life of this value.
        unsafe { sys::dwnx_conn_get_max_data_left(self.raw()) }
    }

    /// Whether a stream was opened by this endpoint.
    #[must_use]
    pub fn is_local_stream(&self, stream: StreamId) -> bool {
        // SAFETY: `raw` is non-null for the life of this value.
        unsafe { sys::dwnx_conn_is_local_stream(self.raw(), stream.get()) != 0 }
    }
}
