//! A quinn-backed harness for driving [`ngnet_h3`] over a real QUIC connection.
//!
//! Everything the wrapper's own test suite proves, it proves in memory with no transport
//! at all — which is the point of a sans-I/O core, and also its blind spot. This crate
//! exists to close it: the same exchanges, over a real QUIC connection with real
//! encryption, on a loopback socket.
//!
//! It is deliberately a separate crate. `ngnet-h3` declares exactly one dependency and a
//! test asserts as much, so quinn, rustls, rcgen and tokio live here and reach the wrapper
//! only through its public API.
//!
//! # How the two halves are joined
//!
//! `ngnet-h3` owns no transport and quinn owns no protocol state, so something has to sit
//! between them. That is the driver: it holds an [`ngnet_h3::Conn`], asks it what to send,
//! writes that to the matching quinn stream, and feeds back whatever arrives.
//!
//! Inbound bytes reach it through a channel rather than being read inline. Each quinn
//! stream gets a small task that does nothing but read and forward, so the driver never
//! has to choose between reading one stream and writing another — which matters, because
//! HTTP/3 puts the peer's control and QPACK streams alongside its request streams and a
//! driver that stopped reading any one of them would stall the connection.
//!
//! # The one thing this cannot prove
//!
//! quinn reports no per-byte acknowledgement. What it does offer is ownership: once
//! `write_all` returns, quinn has taken a copy of the bytes, so the application's buffer
//! is genuinely free. The driver therefore reports acknowledgement immediately after a
//! successful write — sound, but immediate, so it does not exercise the case where a
//! buffer stays retained across many writes. That case is covered in the wrapper's own
//! `body.rs`, where acknowledgement is withheld deliberately.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ngnet_h3::{
    BodyOutcome, BodySource, Conn, ConnBuilder, ErrorCode, FieldSection, FixedBody, Header, Role,
    StreamId, Timestamp,
};
use quinn::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::sync::mpsc;

/// H3_NO_ERROR: an ordinary, blameless close.
const H3_NO_ERROR: u32 = 0x0100;

/// How long to wait for the peer before giving up on an exchange.
///
/// Generous for a loopback socket, and the point is not the number: without it a protocol
/// bug is a hung test rather than a failing one, because a driver waiting on a channel
/// whose sender it holds itself waits forever.
const IDLE_LIMIT: Duration = Duration::from_secs(10);

/// A field name and value, owned, as a test writes and reads them.
pub type Field = (String, String);

/// One request for the harness to send.
#[derive(Clone, Debug)]
pub struct Request {
    /// The `:path` pseudo-field.
    pub path: String,
    /// The request body, which may be empty.
    pub body: Vec<u8>,
    /// Trailing fields, sent after the body. Requires a non-empty body: a message with no
    /// body ends its stream at the header section, leaving nothing for a trailer to follow.
    pub trailers: Vec<Field>,
}

impl Request {
    /// A request with no body and no trailers.
    pub fn get(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            body: Vec::new(),
            trailers: Vec::new(),
        }
    }

    /// A request carrying a body.
    pub fn post(path: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            body: body.into(),
            trailers: Vec::new(),
        }
    }

    /// Adds trailing fields.
    pub fn with_trailers(mut self, trailers: Vec<Field>) -> Self {
        self.trailers = trailers;
        self
    }
}

/// One message as it was received.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Message {
    /// Leading fields, in arrival order.
    pub headers: Vec<Field>,
    /// The body, concatenated across however many chunks it arrived in.
    pub body: Vec<u8>,
    /// Trailing fields, in arrival order.
    pub trailers: Vec<Field>,
    /// Whether the peer finished sending.
    pub ended: bool,
}

impl Message {
    /// The value of a field, if it was present.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Transport limits to run an exchange under.
///
/// The narrow settings exist to force partial writes and stream blocking, which is where
/// the two-phase send transaction and the block/unblock pair earn their keep.
#[derive(Clone, Copy, Debug)]
pub struct Tuning {
    /// Per-stream receive window, in bytes. `None` leaves quinn's default.
    pub stream_receive_window: Option<u64>,
    /// Connection-wide receive window, in bytes. `None` leaves quinn's default.
    pub receive_window: Option<u64>,
}

impl Tuning {
    /// quinn's own defaults, which are generous enough that nothing blocks.
    pub fn roomy() -> Self {
        Self {
            stream_receive_window: None,
            receive_window: None,
        }
    }

    /// Windows small enough that a body of any size is written in many pieces.
    pub fn cramped() -> Self {
        Self {
            stream_receive_window: Some(4 * 1024),
            receive_window: Some(16 * 1024),
        }
    }

    fn apply(self, config: &mut quinn::TransportConfig) {
        if let Some(window) = self.stream_receive_window {
            config.stream_receive_window(window.try_into().expect("a valid varint"));
        }
        if let Some(window) = self.receive_window {
            config.receive_window(window.try_into().expect("a valid varint"));
        }
        // Three unidirectional streams are mandatory in each direction -- control plus the
        // two QPACK streams -- so a limit below that deadlocks the handshake.
        config.max_concurrent_uni_streams(8u32.into());
        config.max_concurrent_bidi_streams(64u32.into());
    }
}

/// What the connection's handlers accumulate, keyed by stream.
#[derive(Default)]
struct Inbox {
    messages: HashMap<i64, Message>,
    /// Streams whose peer has finished sending, in the order they finished.
    finished: Vec<i64>,
}

impl Inbox {
    fn entry(&mut self, stream: StreamId) -> &mut Message {
        self.messages.entry(stream.get()).or_default()
    }
}

fn build(role: Role) -> Conn<Inbox> {
    ConnBuilder::<Inbox>::new(role)
        .on_field(|inbox: &mut Inbox, stream, section, _token, name, value| {
            let field = (
                String::from_utf8_lossy(name).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            );
            let message = inbox.entry(stream);
            match section {
                FieldSection::Headers => message.headers.push(field),
                FieldSection::Trailers => message.trailers.push(field),
            }
            ngnet_h3::FieldAction::Continue
        })
        .on_data(|inbox: &mut Inbox, stream, chunk| {
            inbox.entry(stream).body.extend_from_slice(chunk);
        })
        .on_end_stream(|inbox: &mut Inbox, stream| {
            inbox.entry(stream).ended = true;
            inbox.finished.push(stream.get());
        })
        .build()
        .expect("build a connection")
}

/// Bytes that arrived on one QUIC stream, or the news that it ended.
struct Inbound {
    stream: StreamId,
    bytes: Vec<u8>,
    fin: bool,
}

/// Something the peer did that the driver has to know about.
enum Event {
    /// Bytes arrived on a stream.
    Inbound(Inbound),
    /// The peer opened a bidirectional stream, and this is the half to answer on.
    ///
    /// It has to reach the driver rather than be kept by the accepting task: nghttp3 names
    /// the stream to write to, and only the driver can turn that name back into a stream.
    /// Dropping it instead would reset the stream before a response could be written.
    Opened(quinn::SendStream),
    /// A stream or the connection failed, so nothing more will arrive on it.
    ///
    /// Reported rather than swallowed: a reader task that simply exited would leave the
    /// driver waiting for an end-of-stream that is never coming.
    Lost,
}

/// Joins one [`Conn`] to one quinn connection.
struct Driver {
    conn: Conn<Inbox>,
    inbox: Inbox,
    quic: quinn::Connection,
    /// The quinn sending halves, by stream identifier. nghttp3 names a stream to write to;
    /// this is how that name is turned back into something to write into.
    sends: HashMap<i64, quinn::SendStream>,
    /// Streams already told to quinn that they have ended, so `finish` is called once.
    finished: Vec<i64>,
    /// Streams taken out of the running because quinn would not take all their bytes.
    blocked: Vec<i64>,
    events: mpsc::UnboundedReceiver<Event>,
    /// Whether the transport has reported that nothing further will arrive.
    lost: bool,
    /// Request streams already closed, so each is closed exactly once.
    closed: Vec<i64>,
    /// Handed to a reader task for every stream this endpoint opens itself.
    to_driver: mpsc::UnboundedSender<Event>,
    started: Instant,
}

impl Driver {
    /// Binds the three connection-level streams HTTP/3 requires and starts reading.
    ///
    /// The caller must [`Driver::flush`] before awaiting anything: quinn does not tell the
    /// peer a stream exists until something is written to it, so a driver that opened its
    /// control stream and then waited would be waiting for a peer that had not been told
    /// there was anything to answer.
    async fn start(
        role: Role,
        quic: quinn::Connection,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = build(role);
        let mut sends = HashMap::new();

        // Opened in the order nghttp3 expects to be told about them. quinn assigns the
        // identifiers, which is exactly the division of labour the wrapper assumes: the
        // caller owns the streams and merely declares what each is for.
        let control = quic.open_uni().await?;
        let encoder = quic.open_uni().await?;
        let decoder = quic.open_uni().await?;
        let (control_id, encoder_id, decoder_id) = (
            stream_id(control.id()),
            stream_id(encoder.id()),
            stream_id(decoder.id()),
        );
        conn.bind_control_stream(control_id)?;
        conn.bind_qpack_streams(encoder_id, decoder_id)?;
        for (id, send) in [
            (control_id, control),
            (encoder_id, encoder),
            (decoder_id, decoder),
        ] {
            sends.insert(id.get(), send);
        }

        let (tx, events) = mpsc::unbounded_channel();
        spawn_acceptor(quic.clone(), tx.clone());

        Ok(Self {
            conn,
            inbox: Inbox::default(),
            quic,
            sends,
            finished: Vec::new(),
            blocked: Vec::new(),
            events,
            lost: false,
            closed: Vec::new(),
            to_driver: tx,
            started: Instant::now(),
        })
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.started.elapsed().as_nanos() as u64)
    }

    /// Writes everything the connection currently wants to send.
    ///
    /// Returns whether anything moved, so a caller can tell progress from a settled
    /// connection without re-entering the wrapper to ask.
    ///
    /// # Why a short write blocks the stream
    ///
    /// nghttp3 offers the highest-priority writable stream, and goes on offering the same
    /// one until it has nothing left for it. So when quinn's window lets only part of an
    /// offer through, writing again immediately would await on that one stream while every
    /// other stream waited behind it — and with a small enough window, that is a livelock
    /// rather than merely unfair. `block_stream` takes it out of the running so the next
    /// offer is a different stream; `unblock_all` puts it back once there is nothing else
    /// to do.
    ///
    /// This is a different mechanism from a body source having nothing to give, which the
    /// source signals itself and which is cleared with `resume_stream`. Conflating the two
    /// is how a send loop spins.
    async fn flush(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut moved = false;
        loop {
            let Some(send) = self.conn.writev_stream(&mut self.inbox)? else {
                if self.unblock_all()? {
                    continue;
                }
                return Ok(moved);
            };
            let stream = send.stream();
            let fin = send.fin();
            // Copied out rather than written through the borrowed slices: quinn writes one
            // contiguous slice at a time, and holding the guard across every await of a
            // vectored write would pin the connection for the whole flush.
            let bytes: Vec<u8> = send.slices().iter().flat_map(|s| s.to_vec()).collect();

            let written = if bytes.is_empty() {
                // A stream ending with no further payload. There is nothing to write, but
                // it still has to be committed or the connection never advances past it.
                0
            } else {
                match self.sends.get_mut(&stream.get()) {
                    // `write` rather than `write_all`: a short count is exactly the
                    // transport refusing the rest, which is what this wants to hear about.
                    Some(quic_stream) => match quic_stream.write(&bytes).await {
                        Ok(written) => written,
                        Err(error) => {
                            send.abandon();
                            return Err(Box::new(error));
                        }
                    },
                    None => {
                        send.abandon();
                        return Err(format!("nothing is open for stream {stream}").into());
                    }
                }
            };

            // Committed only after quinn has accepted the bytes, so a write that fails
            // cannot advance the connection past bytes that were never sent.
            send.commit(written)?;
            if written > 0 {
                // A stand-in for real acknowledgement, and sound only because quinn has
                // taken ownership: `write` copies into quinn's own buffers, so the bytes
                // this reports as releasable genuinely are. A transport that borrowed them
                // instead would have to wait for a real acknowledgement. Deliberately a
                // separate call from `commit`: one says how many bytes went out, the other
                // says they may be freed, and only the second releases anything.
                self.conn
                    .add_ack_offset(stream, written as u64, &mut self.inbox)?;
            }

            if written < bytes.len() {
                self.conn.block_stream(stream)?;
                if !self.blocked.contains(&stream.get()) {
                    self.blocked.push(stream.get());
                }
            } else if fin && !self.finished.contains(&stream.get()) {
                self.finished.push(stream.get());
                if let Some(quic_stream) = self.sends.get_mut(&stream.get()) {
                    quic_stream.finish()?;
                }
            }
            moved = true;
        }
    }

    /// Puts every blocked stream back in the running, reporting whether any were.
    fn unblock_all(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.blocked.is_empty() {
            return Ok(false);
        }
        for stream in std::mem::take(&mut self.blocked) {
            self.conn.unblock_stream(StreamId::new(stream)?)?;
        }
        Ok(true)
    }

    /// Handles everything that has already arrived, without waiting for more.
    fn drain_inbound(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut moved = false;
        while let Ok(event) = self.events.try_recv() {
            self.handle(event)?;
            moved = true;
        }
        Ok(moved)
    }

    /// Waits for the next event and handles it, reporting whether more may follow.
    ///
    /// Bounded by [`IDLE_LIMIT`], which is what makes the caller's failure paths reachable:
    /// the driver holds a sender for the very channel it waits on, so the channel never
    /// closes of its own accord and an unbounded wait would be an unbounded hang.
    async fn await_inbound(&mut self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if self.lost {
            return Ok(false);
        }
        match tokio::time::timeout(IDLE_LIMIT, self.events.recv()).await {
            Ok(Some(event)) => {
                self.handle(event)?;
                Ok(!self.lost)
            }
            Ok(None) => Ok(false),
            Err(_elapsed) => Err(format!(
                "nothing arrived from the peer for {IDLE_LIMIT:?}; the exchange is stuck"
            )
            .into()),
        }
    }

    fn handle(&mut self, event: Event) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match event {
            Event::Inbound(inbound) => self.deliver(inbound),
            Event::Opened(send) => {
                self.sends.insert(stream_id(send.id()).get(), send);
                Ok(())
            }
            Event::Lost => {
                self.lost = true;
                Ok(())
            }
        }
    }

    /// Closes every request stream that has finished in both directions.
    ///
    /// Prompt rather than left to teardown, because closing is what releases a stream's
    /// body buffers and its send accounting; a connection that only closed streams when it
    /// was dropped would hold both for its whole life. Only request streams are closed:
    /// the control and QPACK streams have to outlive the connection, and closing one is
    /// refused for exactly that reason.
    fn close_completed(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ready: Vec<i64> = self
            .inbox
            .finished
            .iter()
            .copied()
            .filter(|stream| {
                stream % 4 == 0 && self.finished.contains(stream) && !self.closed.contains(stream)
            })
            .collect();
        for stream in ready {
            // Clean: both directions finished on their own, and no application error was
            // involved. Reporting `H3_NO_ERROR` in both directions instead would tell the
            // handler an error occurred that never did.
            self.conn
                .close_stream(StreamId::new(stream)?, &mut self.inbox)?;
            self.closed.push(stream);
        }
        Ok(())
    }

    /// Registers a stream this endpoint opened itself, and starts reading its other half.
    fn adopt(&mut self, send: quinn::SendStream, recv: quinn::RecvStream) -> StreamId {
        let stream = stream_id(send.id());
        self.sends.insert(stream.get(), send);
        // Without this the receiving half would be dropped, which quinn turns into a
        // STOP_SENDING -- so the peer's response would be reset before it was written.
        spawn_reader(recv, self.to_driver.clone());
        stream
    }

    fn deliver(
        &mut self,
        inbound: Inbound,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let now = self.now();
        // The credit returned is what QUIC flow control may be extended by. quinn manages
        // its own windows, so there is nothing to hand it here; the value is read anyway so
        // that a change in its meaning shows up as a compile error rather than silence.
        let _credit = self.conn.read_stream(
            inbound.stream,
            &inbound.bytes,
            inbound.fin,
            now,
            &mut self.inbox,
        )?;
        Ok(())
    }
}

/// Turns a quinn stream identifier into an nghttp3 one.
///
/// A straight conversion, deliberately: quinn's identifier *is* the RFC 9000 wire value,
/// which is the same space nghttp3 uses, so anything cleverer here would be a bug.
fn stream_id(id: quinn::StreamId) -> StreamId {
    StreamId::new(u64::from(id) as i64).expect("quinn only produces valid identifiers")
}

/// Reads every stream the peer opens, forwarding the bytes to the driver.
///
/// One task per stream, so a slow or idle stream cannot hold up any other. Errors end the
/// task quietly: a connection closing is the normal way this stops.
fn spawn_acceptor(quic: quinn::Connection, tx: mpsc::UnboundedSender<Event>) {
    let uni = quic.clone();
    let uni_tx = tx.clone();
    tokio::spawn(async move {
        while let Ok(recv) = uni.accept_uni().await {
            spawn_reader(recv, uni_tx.clone());
        }
        let _ = uni_tx.send(Event::Lost);
    });
    tokio::spawn(async move {
        while let Ok((send, recv)) = quic.accept_bi().await {
            if tx.send(Event::Opened(send)).is_err() {
                return;
            }
            spawn_reader(recv, tx.clone());
        }
        let _ = tx.send(Event::Lost);
    });
}

fn spawn_reader(mut recv: quinn::RecvStream, tx: mpsc::UnboundedSender<Event>) {
    let stream = stream_id(recv.id());
    tokio::spawn(async move {
        loop {
            match recv.read_chunk(64 * 1024, true).await {
                Ok(Some(chunk)) => {
                    if tx
                        .send(Event::Inbound(Inbound {
                            stream,
                            bytes: chunk.bytes.to_vec(),
                            fin: false,
                        }))
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(None) => {
                    // End of stream. Reported with no bytes, which the wrapper takes as
                    // the end-of-stream marker it needs to finish the message.
                    let _ = tx.send(Event::Inbound(Inbound {
                        stream,
                        bytes: Vec::new(),
                        fin: true,
                    }));
                    return;
                }
                Err(_) => {
                    // A reset stream or a lost connection. Reported, because a driver that
                    // never heard would wait for an end that is not coming.
                    let _ = tx.send(Event::Lost);
                    return;
                }
            }
        }
    });
}

/// A body that hands over one buffer and then, optionally, keeps the stream open.
struct HarnessBody {
    inner: FixedBody,
    trailers_follow: bool,
    done: bool,
}

impl BodySource for HarnessBody {
    fn next(&mut self) -> BodyOutcome {
        if !self.trailers_follow {
            return self.inner.next();
        }
        if self.done {
            return BodyOutcome::EofWithTrailers(Vec::new());
        }
        self.done = true;
        match self.inner.next() {
            BodyOutcome::Eof(pieces) => BodyOutcome::EofWithTrailers(pieces),
            other => other,
        }
    }
}

fn body_for(request: &Request) -> Option<Box<dyn BodySource>> {
    if request.body.is_empty() {
        return None;
    }
    Some(Box::new(HarnessBody {
        inner: FixedBody::new(request.body.clone()),
        trailers_follow: !request.trailers.is_empty(),
        done: false,
    }))
}

/// A self-signed certificate and the key that signed it.
fn certified() -> (CertificateDer<'static>, PrivatePkcs8KeyDer<'static>) {
    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("generate a cert");
    let key = PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der());
    (certified.cert.into(), key)
}

/// Starts a QUIC endpoint on loopback and returns it with the certificate to trust.
fn server_endpoint(
    tuning: Tuning,
) -> Result<(quinn::Endpoint, CertificateDer<'static>), Box<dyn std::error::Error + Send + Sync>> {
    let (cert, key) = certified();
    let mut config = quinn::ServerConfig::with_single_cert(vec![cert.clone()], key.into())?;
    let mut transport = quinn::TransportConfig::default();
    tuning.apply(&mut transport);
    config.transport_config(Arc::new(transport));

    // Port zero, so tests can run concurrently without choosing ports that collide.
    let socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    let endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(config),
        socket,
        Arc::new(quinn::TokioRuntime),
    )?;
    Ok((endpoint, cert))
}

fn client_endpoint(
    cert: CertificateDer<'static>,
    tuning: Tuning,
) -> Result<quinn::Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut config = quinn::ClientConfig::with_root_certificates(Arc::new(roots))?;
    let mut transport = quinn::TransportConfig::default();
    tuning.apply(&mut transport);
    config.transport_config(Arc::new(transport));

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

/// How a server answers one request.
///
/// Given the request as it arrived, returns the status, the response body and its
/// trailers. The default echoes the body back.
pub type Responder = Arc<dyn Fn(&Message) -> (u16, Vec<u8>, Vec<Field>) + Send + Sync>;

/// A server that echoes each request's body back with a `200`.
pub fn echo() -> Responder {
    Arc::new(|request: &Message| (200, request.body.clone(), Vec::new()))
}

/// Runs `requests` against a server built from `responder`, over a real QUIC connection.
///
/// Returns the responses in the order the requests were given, whatever order they
/// completed in.
pub async fn exchange(
    requests: Vec<Request>,
    responder: Responder,
    tuning: Tuning,
) -> Result<Vec<Message>, Box<dyn std::error::Error + Send + Sync>> {
    let (server_endpoint, cert) = server_endpoint(tuning)?;
    let addr = server_endpoint.local_addr()?;
    let expected = requests.len();

    let server = tokio::spawn(async move {
        let incoming = server_endpoint
            .accept()
            .await
            .ok_or("the endpoint closed before a connection arrived")?;
        let quic = incoming.await?;
        let outcome = serve(quic, responder, expected).await;
        // Held until the exchange is over: dropping an endpoint stops its I/O driver, and
        // doing that early would tear the connection down mid-response.
        drop(server_endpoint);
        outcome
    });

    let endpoint = client_endpoint(cert, tuning)?;
    // Both halves are awaited together. Awaiting the client first would mean a server that
    // failed early left the client waiting out its idle limit before anyone looked at the
    // error that actually explains the failure.
    let client = async {
        let quic = endpoint.connect(addr, "localhost")?.await?;
        request_all(quic, requests).await
    };
    let (responses, served) = tokio::join!(client, server);
    served.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })??;
    let responses = responses?;

    endpoint.wait_idle().await;
    Ok(responses)
}

/// Submits every request on its own stream and collects the responses.
async fn request_all(
    quic: quinn::Connection,
    requests: Vec<Request>,
) -> Result<Vec<Message>, Box<dyn std::error::Error + Send + Sync>> {
    let mut driver = Driver::start(Role::Client, quic).await?;
    // The preface has to reach the server before anything else can be understood.
    driver.flush().await?;

    let mut order = Vec::with_capacity(requests.len());
    let mut trailers_pending: Vec<(StreamId, Vec<Field>)> = Vec::new();
    for request in &requests {
        let (send, recv) = driver.quic.open_bi().await?;
        let stream = driver.adopt(send, recv);
        order.push(stream.get());

        let path = request.path.clone();
        let fields = vec![
            Header::new(
                ":method",
                if request.body.is_empty() {
                    "GET"
                } else {
                    "POST"
                },
            )?,
            Header::new(":scheme", "https")?,
            Header::new(":path", &path)?,
            Header::new(":authority", "localhost")?,
        ];
        driver
            .conn
            .submit_request(stream, &fields, body_for(request))?;
        if !request.trailers.is_empty() {
            trailers_pending.push((stream, request.trailers.clone()));
        }
    }
    driver.flush().await?;

    // Submitted after the body has been handed over, which is when the stream is still
    // open but has nothing further to send.
    for (stream, trailers) in trailers_pending {
        let owned: Vec<(Vec<u8>, Vec<u8>)> = trailers
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect();
        let fields: Vec<Header<'_>> = owned
            .iter()
            .map(|(n, v)| Header::new(n.as_slice(), v.as_slice()))
            .collect::<Result<_, _>>()?;
        driver.conn.submit_trailers(stream, &fields)?;
    }
    driver.flush().await?;

    run_until(&mut driver, |driver| {
        order
            .iter()
            .all(|stream| driver.inbox.finished.contains(stream))
    })
    .await?;

    // Checked rather than assumed: a `close_completed` that quietly stopped matching any
    // stream would leave every test still passing while the close path went unexercised.
    // Counting retained buffers would not catch it -- this harness acknowledges every byte
    // as it writes it, so the retain queue drains whether anything closes or not.
    for stream in &order {
        if !driver.closed.contains(stream) {
            return Err(format!("stream {stream} finished but was never closed").into());
        }
    }

    let responses = order
        .iter()
        .map(|stream| {
            driver
                .inbox
                .messages
                .get(stream)
                .cloned()
                .unwrap_or_default()
        })
        .collect();
    close(&driver.quic);
    Ok(responses)
}

/// Answers `expected` requests and then returns.
async fn serve(
    quic: quinn::Connection,
    responder: Responder,
    expected: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut driver = Driver::start(Role::Server, quic).await?;
    driver.flush().await?;

    let mut answered: Vec<i64> = Vec::new();
    loop {
        // Everything that has finished arriving and has not been answered yet.
        let ready: Vec<i64> = driver
            .inbox
            .finished
            .iter()
            .copied()
            .filter(|stream| !answered.contains(stream) && *stream % 4 == 0)
            .collect();
        for stream in ready {
            let request = driver.inbox.messages[&stream].clone();
            let (status, body, trailers) = responder(&request);
            answer(&mut driver, stream, status, body, trailers).await?;
            answered.push(stream);
        }
        if answered.len() >= expected {
            driver.flush().await?;
            driver.close_completed()?;
            // The same check the client makes, because a close path that regressed on one
            // side only would otherwise be invisible.
            for stream in &answered {
                if !driver.closed.contains(stream) {
                    return Err(format!("stream {stream} was answered but never closed").into());
                }
            }
            // The responses are written but not necessarily delivered. Waiting for the
            // client to close is what keeps the connection -- and with it every stream
            // still in flight -- alive long enough for them to arrive.
            driver.quic.closed().await;
            return Ok(());
        }
        if !driver.await_inbound().await? {
            return Ok(());
        }
        driver.drain_inbound()?;
        driver.flush().await?;
        driver.close_completed()?;
    }
}

async fn answer(
    driver: &mut Driver,
    stream: i64,
    status: u16,
    body: Vec<u8>,
    trailers: Vec<Field>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let stream = StreamId::new(stream)?;
    let status = status.to_string();
    let fields = vec![Header::new(":status", &status)?];
    let request = Request {
        path: String::new(),
        body,
        trailers: trailers.clone(),
    };
    driver
        .conn
        .submit_response(stream, &fields, body_for(&request))?;
    driver.flush().await?;

    if !trailers.is_empty() {
        let owned: Vec<(Vec<u8>, Vec<u8>)> = trailers
            .iter()
            .map(|(n, v)| (n.as_bytes().to_vec(), v.as_bytes().to_vec()))
            .collect();
        let fields: Vec<Header<'_>> = owned
            .iter()
            .map(|(n, v)| Header::new(n.as_slice(), v.as_slice()))
            .collect::<Result<_, _>>()?;
        driver.conn.submit_trailers(stream, &fields)?;
        driver.flush().await?;
    }
    Ok(())
}

/// Pumps the driver until `done` holds, or the connection stops producing anything.
async fn run_until(
    driver: &mut Driver,
    done: impl Fn(&Driver) -> bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Bounded rather than open-ended, so a protocol bug surfaces as a failing test instead
    // of a hung one. The bound is generous: a settled connection exits through `done`.
    for _ in 0..100_000 {
        driver.drain_inbound()?;
        driver.flush().await?;
        driver.close_completed()?;
        if done(driver) {
            return Ok(());
        }
        if !driver.await_inbound().await? {
            return if done(driver) {
                Ok(())
            } else {
                Err("the connection ended before the exchange completed".into())
            };
        }
    }
    Err("the exchange never settled".into())
}

/// Closes a connection with `H3_NO_ERROR`, the way an application should.
pub fn close(quic: &quinn::Connection) {
    // Routed through the wrapper's own type so the two cannot drift: this is the same code
    // the in-memory tests close streams with.
    let code = ErrorCode::new(u64::from(H3_NO_ERROR));
    quic.close(
        u32::try_from(code.get())
            .expect("an HTTP/3 code fits a varint")
            .into(),
        b"done",
    );
}

/// A connected pair of QUIC connections on loopback, and the endpoints behind them.
///
/// The endpoints are handed back rather than disposed of internally because each owns a UDP
/// socket and the task driving it. Returning them makes that ownership the caller's, which
/// is the only arrangement under which the resources are actually released when the caller
/// is finished — an earlier version leaked them instead, which worked but never gave
/// anything back.
///
/// They are **not** load-bearing for connection liveness, despite how it looks. quinn's
/// endpoint driver shuts down only once its reference count has reached zero *and* no
/// connections remain (`quinn-0.11.11/src/endpoint.rs:384-388`), so a connection outlives
/// the handle it was created from. Dropping [`endpoints`](Self::endpoints) early is
/// therefore safe; it simply gives up the ability to open further connections on that
/// socket.
pub struct ConnectedPair {
    /// The connection the client end drives.
    pub client: quinn::Connection,
    /// The connection the server end drives.
    pub server: quinn::Connection,
    /// The client and server endpoints, in that order.
    ///
    /// Owned by the caller so they are released rather than leaked. See the type
    /// documentation for why holding them is not required for the connections to work.
    pub endpoints: (quinn::Endpoint, quinn::Endpoint),
}

/// Connects a pair of QUIC connections on loopback.
///
/// Reuses the endpoint, certificate and transport setup the sans-I/O harness already has.
/// None of it belongs behind [`ngnet_h3::http::QuicConnection`] — that trait begins with an
/// established connection precisely so that certificates, ALPN and endpoint configuration
/// stay the caller's business and never reach the wrapper.
pub async fn connected_pair(
    tuning: Tuning,
) -> Result<ConnectedPair, Box<dyn std::error::Error + Send + Sync>> {
    let (server, cert) = server_endpoint(tuning)?;
    let address = server.local_addr()?;
    let client = client_endpoint(cert, tuning)?;

    let accepting = tokio::spawn(async move {
        let incoming = server.accept().await.ok_or("no connection arrived")?;
        let connection = incoming.await?;
        // Returned alongside the connection so ownership reaches the caller rather than
        // ending with this task.
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((connection, server))
    });

    let connecting = client.connect(address, "localhost")?.await?;
    let (accepted, server_endpoint) = accepting.await??;

    Ok(ConnectedPair {
        client: connecting,
        server: accepted,
        endpoints: (client, server_endpoint),
    })
}
