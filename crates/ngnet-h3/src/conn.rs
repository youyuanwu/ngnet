//! The HTTP/3 connection.

use core::marker::PhantomData;

use ngnet_h3_sys as sys;

use crate::alloc::Allocator;
use crate::body::BodySource;
use crate::callbacks::{Bridge, BridgeGuard, BridgeSlot};
use crate::error::{Error, ErrorCode, Result};
use crate::handlers::{FieldAction, FieldSection, FieldToken, Handlers, StreamClosed};
use crate::header::Header;
use crate::send::SendGuard;
use crate::settings::Settings;
use crate::state::BodyRegistry;
use crate::stream::{Directionality, Initiator, StreamId};

/// Which side of the connection this endpoint is.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    /// Sends requests.
    Client,
    /// Sends responses.
    Server,
}

impl Role {
    fn initiator(self) -> Initiator {
        match self {
            Role::Client => Initiator::Client,
            Role::Server => Initiator::Server,
        }
    }
}

/// A monotonic timestamp, in nanoseconds.
///
/// nghttp3 requires a non-decreasing clock reading on every read, which it uses to rate
/// limit protocol glitches. This crate performs no I/O and reads no clock — doing so
/// would break the sans-I/O guarantee — so the reading is supplied by the caller. Any
/// steady source will do, as long as it never goes backwards; the connection rejects a
/// reading lower than the last one rather than passing it on.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Wraps a nanosecond reading from a steady clock.
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// The raw reading.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }
}

/// QUIC flow-control credit that may now be extended, in bytes.
///
/// Deliberately not a bare `usize`, because it is easy to mistake for "how many of the
/// bytes I supplied were consumed" — which it is not. All supplied bytes are always
/// consumed; there is never a remainder to re-present. This is the amount by which the
/// caller may raise the peer's stream and connection flow-control limits, and it excludes
/// the payload of data frames: those bytes are credited by the caller once it has handled
/// the body chunks delivered to it, and more credit may arrive later through the
/// deferred-consume handler for streams that were blocked.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct FlowCredit(u64);

impl FlowCredit {
    /// The number of bytes of credit.
    pub const fn bytes(self) -> u64 {
        self.0
    }
}

/// Builds a [`Conn`].
pub struct ConnBuilder<C> {
    role: Role,
    settings: Settings,
    handlers: Handlers<C>,
    _context: PhantomData<fn(&mut C)>,
}

impl<C> ConnBuilder<C> {
    /// Starts building a connection in the given role.
    pub fn new(role: Role) -> Self {
        Self {
            role,
            settings: Settings::new(),
            handlers: Handlers::default(),
            _context: PhantomData,
        }
    }

    /// Replaces the settings this endpoint advertises.
    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// Called when previously blocked stream data has been consumed, and that much more
    /// QUIC flow-control credit may be extended.
    ///
    /// The handler must be `Send`, because [`Conn`] is, and the handler is the only thing
    /// a connection owns that could capture something thread-affine. Without that bound a
    /// non-atomic refcount could be moved across threads by capturing it here:
    ///
    /// ```compile_fail
    /// # use std::rc::Rc;
    /// # use ngnet_h3::{ConnBuilder, Role};
    /// # fn main() -> Result<(), ngnet_h3::Error> {
    /// let shared = Rc::new(0u8);
    /// let conn = ConnBuilder::<()>::new(Role::Client)
    ///     .on_deferred_consume(move |_, _, _| { let _ = Rc::clone(&shared); })
    ///     .build()?;
    /// std::thread::spawn(move || drop(conn));
    /// # Ok(())
    /// # }
    /// ```
    pub fn on_deferred_consume(
        mut self,
        handler: impl FnMut(&mut C, StreamId, u64) + Send + 'static,
    ) -> Self {
        self.handlers.deferred_consume = Some(Box::new(handler));
        self
    }

    /// Called when a field section starts.
    pub fn on_section_begin(
        mut self,
        handler: impl FnMut(&mut C, StreamId, FieldSection) + Send + 'static,
    ) -> Self {
        self.handlers.section_begin = Some(Box::new(handler));
        self
    }

    /// Called for each received field, with its name and value borrowed for the call.
    ///
    /// The slices point into nghttp3's own buffers and are valid only until the handler
    /// returns, which is what makes receiving allocation-free: copy what you need.
    pub fn on_field(
        mut self,
        handler: impl FnMut(
            &mut C,
            StreamId,
            FieldSection,
            Option<FieldToken>,
            &[u8],
            &[u8],
        ) -> FieldAction
        + Send
        + 'static,
    ) -> Self {
        self.handlers.field = Some(Box::new(handler));
        self
    }

    /// Called when a field section ends.
    pub fn on_section_end(
        mut self,
        handler: impl FnMut(&mut C, StreamId, FieldSection) + Send + 'static,
    ) -> Self {
        self.handlers.section_end = Some(Box::new(handler));
        self
    }

    /// Called for each chunk of received body bytes, borrowed for the call.
    ///
    /// The chunk's length is not included in the credit returned by
    /// [`Conn::read_stream`]; extending flow control for body bytes is the caller's to do
    /// once it has handled them.
    pub fn on_data(
        mut self,
        handler: impl FnMut(&mut C, StreamId, &[u8]) + Send + 'static,
    ) -> Self {
        self.handlers.data = Some(Box::new(handler));
        self
    }

    /// Called when the peer finishes sending on a stream.
    pub fn on_end_stream(mut self, handler: impl FnMut(&mut C, StreamId) + Send + 'static) -> Self {
        self.handlers.end_stream = Some(Box::new(handler));
        self
    }

    /// Called when a stream closes, with the application error code it closed with.
    pub fn on_stream_close(
        mut self,
        handler: impl FnMut(&mut C, StreamId, StreamClosed) + Send + 'static,
    ) -> Self {
        self.handlers.stream_close = Some(Box::new(handler));
        self
    }

    /// Creates the connection.
    pub fn build(self) -> Result<Conn<C>> {
        Conn::new(self.role, self.settings, self.handlers)
    }
}

/// One HTTP/3 connection.
///
/// Owns protocol state and nothing else: no socket, no runtime, no QUIC. The caller reads
/// bytes from wherever its QUIC streams come from and hands them to [`Conn::read_stream`],
/// then asks [`Conn::writev_stream`] what to send and writes that back.
///
/// # Poisoning
///
/// nghttp3 documents that after `read_stream` or `writev_stream` fails, calling anything
/// but the destructor is undefined behaviour, and separately marks some error codes fatal
/// wherever they appear. Because this crate promises safe Rust cannot reach undefined
/// behaviour, a connection latches those conditions and refuses all further work with
/// [`crate::ErrorKind::ConnectionUnusable`]. Recoverable errors — a stream already in use,
/// a role already bound, a connection that is closing — do not have that effect.
pub struct Conn<C> {
    raw: *mut sys::nghttp3_conn,
    role: Role,

    // Both outlive `raw`: nghttp3 stores pointers to them, not copies. Declared after
    // `raw` only for readability; the drop order that matters is enforced explicitly in
    // `Drop`, which deletes the connection before either is released.
    allocator: Box<Allocator>,
    slot: Box<BridgeSlot>,

    handlers: Handlers<C>,
    bodies: BodyRegistry,
    control: Option<StreamId>,
    qpack: Option<(StreamId, StreamId)>,
    last_timestamp: Option<Timestamp>,
    poison: Option<&'static str>,
    _context: PhantomData<fn(&mut C)>,
}

// SAFETY: a connection owns its native state exclusively. The only values it owns that
// could carry a thread-affine capture are the handler boxes, which are bounded `Send` for
// exactly this reason (see `handlers::ByteCountHandler`). The state type `C` is never
// stored — it is borrowed at call time — so `C: Send` is required only because a handler
// may hold state derived from it.
//
// Deliberately not `Sync`: nghttp3 has no internal locking, and every operation here takes
// `&mut self`, so shared access from two threads would be unsound.
unsafe impl<C: Send> Send for Conn<C> {}

impl<C> Conn<C> {
    fn new(role: Role, settings: Settings, handlers: Handlers<C>) -> Result<Self> {
        let allocator = Allocator::new();
        let slot = BridgeSlot::new();

        // Zeroed rather than partially assigned: the struct is versioned, and a field the
        // running library reads but this build does not set would otherwise be
        // indeterminate.
        let mut callbacks: sys::nghttp3_callbacks = unsafe { core::mem::zeroed() };
        callbacks.deferred_consume = Some(crate::callbacks::deferred_consume_cb::<C>);
        callbacks.begin_headers = Some(crate::callbacks::begin_headers_cb::<C>);
        callbacks.recv_header = Some(crate::callbacks::recv_header_cb::<C>);
        callbacks.end_headers = Some(crate::callbacks::end_headers_cb::<C>);
        callbacks.begin_trailers = Some(crate::callbacks::begin_trailers_cb::<C>);
        callbacks.recv_trailer = Some(crate::callbacks::recv_trailer_cb::<C>);
        callbacks.end_trailers = Some(crate::callbacks::end_trailers_cb::<C>);
        callbacks.recv_data = Some(crate::callbacks::recv_data_cb::<C>);
        callbacks.acked_stream_data = Some(crate::callbacks::acked_stream_data_cb::<C>);
        callbacks.end_stream = Some(crate::callbacks::end_stream_cb::<C>);
        // `stream_close2` rather than the deprecated `stream_close`.
        callbacks.stream_close2 = Some(crate::callbacks::stream_close_cb::<C>);

        // `rand` is deliberately left unset. nghttp3 uses it for one thing — the seed of
        // its internal stream map's hash — and when the callback is absent it uses a seed
        // of zero, which is exactly what supplying a zero-filling callback would achieve.
        // Supplying real randomness would harden that map against a peer choosing stream
        // identifiers to force collisions, but this crate has no entropy source of its own
        // and inventing one would mean either an I/O dependency or a second crate
        // dependency. Exposing a caller-supplied source is the honest way to do it, and is
        // deferred rather than faked.

        let mut raw: *mut sys::nghttp3_conn = core::ptr::null_mut();
        let make = match role {
            Role::Client => sys::nghttp3_conn_client_new_versioned,
            Role::Server => sys::nghttp3_conn_server_new_versioned,
        };

        // SAFETY: `raw` is a valid out-pointer; the callbacks and settings are fully
        // initialised structs of the versions named; the allocator outlives the
        // connection, which matters because nghttp3 stores the pointer rather than a
        // copy; and the slot pointer is stable for the connection's whole life.
        let rv = unsafe {
            make(
                &mut raw,
                sys::NGHTTP3_CALLBACKS_VERSION as i32,
                &callbacks,
                sys::NGHTTP3_SETTINGS_VERSION as i32,
                settings.as_raw(),
                allocator.as_mem_ptr(),
                slot.as_ptr(),
            )
        };
        if rv != 0 {
            return Err(Error::native(rv, "could not create the connection"));
        }
        debug_assert!(!raw.is_null());

        Ok(Self {
            raw,
            role,
            allocator,
            slot,
            handlers,
            bodies: BodyRegistry::default(),
            control: None,
            qpack: None,
            last_timestamp: None,
            poison: None,
            _context: PhantomData,
        })
    }

    /// This endpoint's role.
    pub fn role(&self) -> Role {
        self.role
    }

    /// Whether the connection has been poisoned by an unrecoverable failure.
    pub fn is_usable(&self) -> bool {
        self.poison.is_none()
    }

    /// Native blocks currently allocated by this connection.
    ///
    /// Exposed for tests, which use it to prove teardown released everything.
    #[doc(hidden)]
    pub fn live_allocations(&self) -> i64 {
        self.allocator.state().live_blocks()
    }

    fn check_usable(&self) -> Result<()> {
        match self.poison {
            Some(_) => Err(Error::unusable()),
            None => Ok(()),
        }
    }

    /// Latches a failure if nghttp3 considers it unrecoverable.
    ///
    /// Two conditions poison, and only two. Any failure of the read or write paths does,
    /// because their documentation states that calling anything but the destructor
    /// afterwards is undefined behaviour. And any code nghttp3's own `is_fatal` predicate
    /// accepts does — out of memory and callback failure — because those can surface from
    /// almost any entry point. Everything else stays recoverable, which is what keeps a
    /// second bind, or a submission onto a closing connection, from killing a connection
    /// that is otherwise perfectly serviceable.
    ///
    /// Poisoning also releases every retained outgoing buffer. A write that fails partway
    /// can queue a prefix of a body's vectors and abandon the rest, so acknowledgements
    /// for them can never arrive; holding the buffers for an acknowledgement that will not
    /// come would be a leak for the connection's remaining lifetime. Nothing will read
    /// them again either, because no further call but the destructor is permitted.
    fn record(&mut self, code: i32, context: &'static str, path_is_fatal: bool) -> Error {
        let error = Error::native(code, context);
        if path_is_fatal || error.is_fatal() {
            self.poison.get_or_insert(context);
            self.bodies.clear();
        }
        error
    }

    /// Declares which locally-opened unidirectional stream carries protocol control data.
    ///
    /// HTTP/3 requires this stream; a connection without it cannot complete an exchange.
    pub fn bind_control_stream(&mut self, stream: StreamId) -> Result<()> {
        self.check_usable()?;
        self.require_local_unidirectional(stream)?;
        if let Some(existing) = self.control {
            if existing == stream {
                return Err(Error::invalid_input(
                    "the control stream has already been bound to this stream",
                ));
            }
            return Err(Error::invalid_input(
                "the control stream has already been bound",
            ));
        }
        self.require_unused(stream)?;

        // SAFETY: `raw` is live, and the identifier has been checked to be locally
        // initiated and unidirectional -- which nghttp3 itself only asserts, so a release
        // build would otherwise accept a wrong one and misbehave silently.
        let rv = unsafe { sys::nghttp3_conn_bind_control_stream(self.raw, stream.get()) };
        if rv != 0 {
            return Err(self.record(rv, "could not bind the control stream", false));
        }
        self.control = Some(stream);
        Ok(())
    }

    /// Declares which locally-opened unidirectional streams carry the QPACK encoder and
    /// decoder.
    ///
    /// Both are required, and both must differ from each other and from the control
    /// stream. nghttp3 assigns the encoder before it creates the decoder, so a duplicate
    /// or otherwise unusable pair would leave the connection half-bound with no way to
    /// retry; the check here happens first so that cannot occur.
    pub fn bind_qpack_streams(&mut self, encoder: StreamId, decoder: StreamId) -> Result<()> {
        self.check_usable()?;
        self.require_local_unidirectional(encoder)?;
        self.require_local_unidirectional(decoder)?;
        if encoder == decoder {
            return Err(Error::invalid_input(
                "the QPACK encoder and decoder cannot share one stream",
            ));
        }
        if self.qpack.is_some() {
            return Err(Error::invalid_input(
                "the QPACK streams have already been bound",
            ));
        }
        self.require_unused(encoder)?;
        self.require_unused(decoder)?;

        // SAFETY: `raw` is live, and both identifiers have been checked to be locally
        // initiated, unidirectional, distinct from each other and unused.
        let rv =
            unsafe { sys::nghttp3_conn_bind_qpack_streams(self.raw, encoder.get(), decoder.get()) };
        if rv != 0 {
            return Err(self.record(rv, "could not bind the QPACK streams", false));
        }
        self.qpack = Some((encoder, decoder));
        Ok(())
    }

    /// Whether the control and QPACK streams have all been declared.
    pub fn is_bound(&self) -> bool {
        self.control.is_some() && self.qpack.is_some()
    }

    fn require_bound(&self) -> Result<()> {
        if self.control.is_none() {
            return Err(Error::invalid_input(
                "the control stream has not been bound; HTTP/3 requires one before an exchange",
            ));
        }
        if self.qpack.is_none() {
            return Err(Error::invalid_input(
                "the QPACK streams have not been bound; HTTP/3 requires them before an exchange",
            ));
        }
        Ok(())
    }

    fn require_local_unidirectional(&self, stream: StreamId) -> Result<()> {
        if stream.is_local_unidirectional(self.role.initiator()) {
            return Ok(());
        }
        Err(Error::invalid_input(match stream.directionality() {
            Directionality::Bidirectional => {
                "a connection-level stream must be unidirectional, not bidirectional"
            }
            Directionality::Unidirectional => {
                "a connection-level stream must be opened by this endpoint, not the peer"
            }
        }))
    }

    fn require_unused(&self, stream: StreamId) -> Result<()> {
        let clash = self.control == Some(stream)
            || self
                .qpack
                .is_some_and(|(encoder, decoder)| encoder == stream || decoder == stream);
        if clash {
            return Err(Error::invalid_input(
                "that stream has already been declared for another connection-level role",
            ));
        }
        Ok(())
    }

    /// Whether this stream is one the peer could legitimately have sent on, given our role.
    ///
    /// nghttp3 asserts this for a stream it already knows about, and asserts are compiled
    /// out of a release build. The streams it already knows about are exactly the three
    /// this endpoint bound, so passing one of our own connection-level streams here would
    /// abort a debug build and, in a release build, parse the peer's bytes into our own
    /// sending stream's state — letting an endpoint accept its own SETTINGS as the peer's.
    fn is_peer_readable(&self, stream: StreamId) -> bool {
        if stream.is_client_bidirectional() {
            // Request streams are always client-initiated and readable by either side.
            return true;
        }
        match (self.role, stream.initiator(), stream.directionality()) {
            // A peer-initiated unidirectional stream: control or QPACK, from them to us.
            (Role::Server, Initiator::Client, Directionality::Unidirectional) => true,
            (Role::Client, Initiator::Server, Directionality::Unidirectional) => true,
            _ => false,
        }
    }

    /// Delivers bytes received on a QUIC stream.
    ///
    /// Every supplied byte is processed; there is never a remainder to re-present. The
    /// returned [`FlowCredit`] is the amount by which QUIC flow control may now be
    /// extended, which deliberately excludes data-frame payload — see [`FlowCredit`].
    ///
    /// `now` must not go backwards. nghttp3 uses it to rate limit protocol glitches, and
    /// this crate reads no clock of its own.
    pub fn read_stream(
        &mut self,
        stream: StreamId,
        src: &[u8],
        fin: bool,
        now: Timestamp,
        context: &mut C,
    ) -> Result<FlowCredit> {
        self.check_usable()?;
        if !self.is_peer_readable(stream) {
            return Err(Error::invalid_input(
                "that stream cannot carry data from the peer; a locally-initiated \
                 unidirectional stream is written to, never read from",
            ));
        }
        if let Some(last) = self.last_timestamp {
            if now < last {
                return Err(Error::invalid_input(
                    "the timestamp went backwards; nghttp3 requires a non-decreasing clock",
                ));
            }
        }
        self.last_timestamp = Some(now);

        let consumed = self.with_context(context, |raw| {
            // SAFETY: `raw` is live; `src` is valid for `src.len()` bytes for the duration
            // of the call; and a bridge is installed for any callback this may fire.
            unsafe {
                sys::nghttp3_conn_read_stream2(
                    raw,
                    stream.get(),
                    src.as_ptr(),
                    src.len(),
                    i32::from(fin),
                    now.as_nanos(),
                )
            }
        });

        if consumed < 0 {
            let code = i32::try_from(consumed).unwrap_or(sys::NGHTTP3_ERR_FATAL);
            // Any failure here poisons: the header states that calling anything but the
            // destructor afterwards is undefined behaviour.
            return Err(self.record(code, "could not read stream data", true));
        }
        Ok(FlowCredit(consumed as u64))
    }

    /// Submits a request on a client-initiated bidirectional stream.
    ///
    /// The caller opens the QUIC stream and chooses its identifier, which is why this
    /// takes one rather than returning it.
    ///
    /// With no body, the request ends the sending direction of the stream at its header
    /// section. With one, the body's bytes are pulled from the source as the transport
    /// takes them, and are held until [`Conn::add_ack_offset`] reports them acknowledged.
    pub fn submit_request(
        &mut self,
        stream: StreamId,
        fields: &[Header<'_>],
        body: Option<Box<dyn BodySource>>,
    ) -> Result<()> {
        self.check_usable()?;
        if self.role != Role::Client {
            return Err(Error::invalid_input(
                "only a client submits requests; a server submits responses",
            ));
        }
        if !stream.is_client_bidirectional() {
            return Err(Error::invalid_input(
                "a request needs a client-initiated bidirectional stream",
            ));
        }
        // nghttp3 asserts the QPACK encoder is bound before encoding a field section, and
        // asserts are compiled out of a release build. FR-002 requires this be a typed
        // error, so it is checked rather than left to abort.
        self.require_bound()?;
        let reader = self.attach_body(stream, body)?;

        let nva: Vec<sys::nghttp3_nv> = fields.iter().map(Header::as_nv).collect();
        // SAFETY: `raw` is live; the role, stream shape and binding state have all been
        // checked; the field array plus everything it points at outlives the call, which
        // is all nghttp3 needs because no no-copy flag is set; and the data reader is
        // copied into the queued frame by value, so a local is enough.
        let rv = unsafe {
            sys::nghttp3_conn_submit_request(
                self.raw,
                stream.get(),
                nva.as_ptr(),
                nva.len(),
                reader
                    .as_ref()
                    .map_or(core::ptr::null(), |reader| reader as *const _),
                core::ptr::null_mut(),
            )
        };
        if rv != 0 {
            // The body was never handed to nghttp3, so nothing points into its buffers.
            self.bodies.detach(stream);
            return Err(self.record(rv, "could not submit the request", false));
        }
        Ok(())
    }

    /// Submits a response on the stream its request arrived on.
    ///
    /// The body behaves as it does for [`Conn::submit_request`].
    pub fn submit_response(
        &mut self,
        stream: StreamId,
        fields: &[Header<'_>],
        body: Option<Box<dyn BodySource>>,
    ) -> Result<()> {
        self.check_usable()?;
        if self.role != Role::Server {
            return Err(Error::invalid_input(
                "only a server submits responses; a client submits requests",
            ));
        }
        if !stream.is_client_bidirectional() {
            return Err(Error::invalid_input(
                "a response belongs on the client-initiated stream its request arrived on",
            ));
        }
        self.require_bound()?;
        let reader = self.attach_body(stream, body)?;

        let nva: Vec<sys::nghttp3_nv> = fields.iter().map(Header::as_nv).collect();
        // SAFETY: as `submit_request`.
        let rv = unsafe {
            sys::nghttp3_conn_submit_response(
                self.raw,
                stream.get(),
                nva.as_ptr(),
                nva.len(),
                reader
                    .as_ref()
                    .map_or(core::ptr::null(), |reader| reader as *const _),
            )
        };
        if rv != 0 {
            self.bodies.detach(stream);
            return Err(self.record(rv, "could not submit the response", false));
        }
        Ok(())
    }

    /// Takes ownership of an outgoing body and builds the data reader that reaches it.
    ///
    /// The body is found again by stream identifier through the installed bridge rather
    /// than through nghttp3's stream user data, which `submit_response` has no parameter
    /// for at all — so the two submission paths work the same way.
    fn attach_body(
        &mut self,
        stream: StreamId,
        body: Option<Box<dyn BodySource>>,
    ) -> Result<Option<sys::nghttp3_data_reader>> {
        let Some(body) = body else {
            return Ok(None);
        };
        self.bodies.attach(stream, body)?;
        Ok(Some(sys::nghttp3_data_reader {
            read_data: Some(crate::callbacks::read_data_cb::<C>),
        }))
    }

    /// Submits a trailing field section, which ends the stream.
    pub fn submit_trailers(&mut self, stream: StreamId, fields: &[Header<'_>]) -> Result<()> {
        self.check_usable()?;
        // Without this, a connection-level stream would be accepted: nghttp3 registers the
        // control and QPACK streams in the same map, so `find_stream` succeeds for them and
        // the trailers would be scheduled onto a critical stream, with the end-of-stream
        // flag set on it.
        if !stream.is_client_bidirectional() {
            return Err(Error::invalid_input(
                "trailers belong on a client-initiated bidirectional stream, not a \
                 connection-level one",
            ));
        }
        self.require_bound()?;

        let nva: Vec<sys::nghttp3_nv> = fields.iter().map(Header::as_nv).collect();
        // SAFETY: as `submit_request`.
        let rv = unsafe {
            sys::nghttp3_conn_submit_trailers(self.raw, stream.get(), nva.as_ptr(), nva.len())
        };
        if rv != 0 {
            return Err(self.record(rv, "could not submit the trailers", false));
        }
        Ok(())
    }

    /// Submits an informational (1xx) response, which precedes the real one.
    pub fn submit_info(&mut self, stream: StreamId, fields: &[Header<'_>]) -> Result<()> {
        self.check_usable()?;
        // nghttp3 asserts both of these, and asserts are compiled out of a release build.
        if self.role != Role::Server {
            return Err(Error::invalid_input(
                "only a server sends informational responses",
            ));
        }
        if !stream.is_client_bidirectional() {
            return Err(Error::invalid_input(
                "an informational response belongs on the request's own stream",
            ));
        }
        self.require_bound()?;

        let nva: Vec<sys::nghttp3_nv> = fields.iter().map(Header::as_nv).collect();
        // SAFETY: as `submit_response`.
        let rv = unsafe {
            sys::nghttp3_conn_submit_info(self.raw, stream.get(), nva.as_ptr(), nva.len())
        };
        if rv != 0 {
            return Err(self.record(rv, "could not submit the informational response", false));
        }
        Ok(())
    }

    /// Tells the connection a stream has closed.
    ///
    /// The application error code is the one the QUIC layer saw. `0x0100`
    /// (`H3_NO_ERROR`) is the code for an ordinary close.
    pub fn close_stream(
        &mut self,
        stream: StreamId,
        code: ErrorCode,
        context: &mut C,
    ) -> Result<()> {
        self.check_usable()?;
        let rv = self.with_context(context, |raw| {
            // SAFETY: `raw` is live and the identifier is validated. This fires the
            // stream-close handler, so a bridge is installed for it.
            unsafe { sys::nghttp3_conn_close_stream(raw, stream.get(), code.get()) }
        });
        if rv != 0 {
            return Err(self.record(rv, "could not close the stream", false));
        }
        Ok(())
    }

    /// Asks what to send next.
    ///
    /// Returns `None` when there is nothing to send. Otherwise the returned guard borrows
    /// the connection and exposes the bytes to write; the caller must report how many the
    /// transport accepted through [`SendGuard::commit`] before the connection will offer
    /// anything further.
    ///
    /// Takes the caller's state because collecting bytes can pull from an outgoing body
    /// source, and a body source belongs to the caller.
    pub fn writev_stream(&mut self, context: &mut C) -> Result<Option<SendGuard<'_, C>>> {
        self.check_usable()?;
        SendGuard::acquire(self, context)
    }

    /// Tells the connection that `n` more bytes on a stream have been acknowledged by the
    /// peer, and the buffers holding them may be released.
    ///
    /// **Reporting acknowledgement is not optional.** It is the only thing that releases
    /// retained outgoing buffers: nghttp3 reaches its release accounting from here and
    /// from nowhere else, so a caller that never reports acknowledgement holds every body
    /// buffer it ever sent for the life of the connection. Reporting bytes written is not
    /// a substitute.
    ///
    /// `n` is a delta, matching the QUIC layer's own view of newly acknowledged bytes, and
    /// counts every byte written on the stream rather than only body payload. Reporting
    /// more than was ever committed is refused, because nghttp3 would then release a
    /// buffer it has not yet written and still points at.
    pub fn add_ack_offset(&mut self, stream: StreamId, n: u64, context: &mut C) -> Result<()> {
        self.check_usable()?;
        self.bodies.record_acked(stream, n)?;
        let rv = self.with_context(context, |raw| {
            // SAFETY: `raw` is live, the identifier is validated, and `n` has been checked
            // against what was actually written. This fires the acknowledgement callback,
            // so a bridge is installed for it.
            unsafe { sys::nghttp3_conn_add_ack_offset(raw, stream.get(), n) }
        });
        if rv != 0 {
            return Err(self.record(rv, "could not record acknowledged bytes", false));
        }
        Ok(())
    }

    /// Marks a stream as blocked because the transport will not accept more bytes for it.
    ///
    /// Without this, a stream whose transport window is exhausted keeps being offered
    /// ahead of every other stream, which starves them and spins the caller's send loop.
    /// This is distinct from a body source having nothing to give, which is signalled by
    /// the source itself and cleared with [`Conn::resume_stream`].
    pub fn block_stream(&mut self, stream: StreamId) -> Result<()> {
        self.check_usable()?;
        // SAFETY: `raw` is live and the identifier is validated. Returns nothing: marking
        // a stream blocked cannot fail, and an unknown stream is simply ignored.
        unsafe { sys::nghttp3_conn_block_stream(self.raw, stream.get()) };
        Ok(())
    }

    /// Clears the blocked state set by [`Conn::block_stream`].
    pub fn unblock_stream(&mut self, stream: StreamId) -> Result<()> {
        self.check_usable()?;
        // SAFETY: `raw` is live and the identifier is validated.
        let rv = unsafe { sys::nghttp3_conn_unblock_stream(self.raw, stream.get()) };
        if rv != 0 {
            return Err(self.record(rv, "could not unblock the stream", false));
        }
        Ok(())
    }

    /// Signals that a stream whose body source was waiting for data may be tried again.
    pub fn resume_stream(&mut self, stream: StreamId) -> Result<()> {
        self.check_usable()?;
        // SAFETY: `raw` is live and the identifier is validated.
        let rv = unsafe { sys::nghttp3_conn_resume_stream(self.raw, stream.get()) };
        if rv != 0 {
            return Err(self.record(rv, "could not resume the stream", false));
        }
        Ok(())
    }

    /// Whether the connection would currently write anything for a stream.
    pub fn is_stream_writable(&self, stream: StreamId) -> bool {
        // SAFETY: `raw` is live and the identifier is validated. `_2` takes a const
        // pointer and is the current entry point; the unsuffixed one is deprecated.
        let writable = unsafe { sys::nghttp3_conn_is_stream_writable2(self.raw, stream.get()) };
        writable != 0
    }

    /// Runs `f` with a bridge installed, so callbacks can reach the caller's state.
    ///
    /// The closure receives the raw connection pointer rather than `&mut self`, because
    /// the bridge already holds a mutable borrow of the handlers; handing out a second
    /// borrow of the whole connection would alias it.
    pub(crate) fn with_context<R>(
        &mut self,
        context: &mut C,
        f: impl FnOnce(*mut sys::nghttp3_conn) -> R,
    ) -> R {
        let raw = self.raw;
        // Disjoint field borrows: the bridge takes the handlers and the body registry,
        // the guard takes the slot.
        let mut bridge = Bridge {
            handlers: &mut self.handlers,
            bodies: &mut self.bodies,
            context,
        };
        // SAFETY: `bridge` outlives the guard, and `C` matches what callbacks recover.
        let guard = unsafe { BridgeGuard::install(&self.slot, &mut bridge) };
        let out = f(raw);
        drop(guard);
        out
    }

    pub(crate) fn raw(&mut self) -> *mut sys::nghttp3_conn {
        self.raw
    }

    /// Records bytes the transport accepted, so acknowledgement can be bounds-checked.
    pub(crate) fn record_committed(&mut self, stream: StreamId, n: usize) {
        self.bodies.record_committed(stream, n);
    }

    /// Outgoing body buffers still held across all streams.
    ///
    /// Exposed for tests, which use it to prove that acknowledgement — and nothing else —
    /// releases them.
    #[doc(hidden)]
    pub fn retained_body_buffers(&self) -> usize {
        self.bodies.retained_buffers()
    }

    pub(crate) fn record_send_failure(&mut self, code: i32, context: &'static str) -> Error {
        self.record(code, context, true)
    }

    pub(crate) fn record_recoverable(&mut self, code: i32, context: &'static str) -> Error {
        self.record(code, context, false)
    }

    pub(crate) fn require_ready_to_send(&self) -> Result<()> {
        self.require_bound()
    }
}

impl<C> Drop for Conn<C> {
    fn drop(&mut self) {
        // The connection is deleted before the allocator and the bridge slot are released,
        // because nghttp3 holds pointers to both and frees through the allocator here.
        // Field order would give the same result today, but stating it means a later
        // reordering cannot quietly break it.
        //
        // SAFETY: `raw` was created by a successful constructor and is deleted once.
        unsafe { sys::nghttp3_conn_del(self.raw) };
        self.raw = core::ptr::null_mut();

        // Only now are the outgoing body buffers released. Doing it here rather than
        // leaving it to field-drop order is what makes the ordering explicit: until the
        // connection is deleted, its send queues still hold pointers into them. Releasing
        // them at all is mandatory rather than tidy — `delete_outq` frees only the buffers
        // nghttp3 allocated itself and deliberately leaves application-owned ones alone.
        self.bodies.clear();

        debug_assert_eq!(
            self.allocator.state().live_blocks(),
            0,
            "nghttp3 leaked native allocations on teardown"
        );
    }
}

impl<C> core::fmt::Debug for Conn<C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Conn")
            .field("role", &self.role)
            .field("bound", &self.is_bound())
            .field("usable", &self.is_usable())
            .finish_non_exhaustive()
    }
}
