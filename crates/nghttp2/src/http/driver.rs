//! The connection driver: the one place the session is touched.
//!
//! # Shape
//!
//! The driver is an `async` block rather than a hand-written [`Future`]. That is a
//! constraint, not a preference: the transport traits return `impl Future`, whose types
//! cannot be named, so a hand-written `poll` could only hold an in-flight read or write
//! across calls by boxing it — which allocates on every operation, or demands
//! `Box<dyn Future + Send>` and reintroduces the very `Send` bound the transport traits
//! exist without. An `async` block holds them in its own generated state instead.
//!
//! # Why one half owns the session
//!
//! Reading and writing must proceed at once, or a large upload stalls every download
//! behind it. But both directions want the session, and the session is `!Sync`.
//!
//! The split here is by *responsibility* rather than by direction. The reading half only
//! moves octets: it reads into a pooled buffer and posts it. The session half does
//! everything else — drains commands, resumes deferred bodies, feeds received octets to
//! the session, and writes out whatever the session produces. Only the session half ever
//! touches the session, so no lock and no shared mutability is needed for it, and a read
//! stays in flight across the whole of a write.
//!
//! # One pass
//!
//! Refresh the driver's waker slot, drain the command queue, drain the ready set and
//! resume those streams, feed everything the reading half posted, dispatch what the
//! handlers observed, write out what the session produced, then park. Every wake — from a
//! handle, from a body, from the reading half — starts another pass.
//!
//! [`Future`]: core::future::Future

use core::future::Future;
use core::task::Poll;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::Waker;

use bytes::BytesMut;
use http_body::Body;

use crate::{
    ErrorCode, FrameType, HeaderAction, HeaderCategory, Session, SessionBuilder, StreamId,
};

use super::error::{Error, ErrorKind, Result};
use super::head;
use super::outgoing::Outgoing;
use super::shared::{Command, HandleToken, Queue, Registry, Shared, Slot};
use super::transport::{Transport, TransportRead, TransportWrite};
use super::waker::StreamWaker;

/// Receive windows are replenished by this layer, not by libnghttp2.
///
/// Automatic replenishment credits the peer the moment octets arrive, which is the
/// opposite of backpressure: a slow reader would still invite the peer to send more.
/// Reporting consumption explicitly is what lets the window track what the application
/// has actually taken.
pub(crate) const MANUAL_FLOW_CONTROL: bool = true;

/// How much a single read may take in.
const READ_BUFFER: usize = 16 * 1024;

/// How many filled buffers may wait to be fed before reading pauses.
///
/// Without a bound the reading half would keep pulling octets in as fast as the peer
/// produced them, since it never has to wait for the session half.
const READ_AHEAD: usize = 2;

/// A header block as it arrives: names and values, in order, uninterpreted.
type Fields = Vec<(Vec<u8>, Vec<u8>)>;

/// What the session's handlers observed, accumulated for the driver to act on.
///
/// Handlers run inside `Session::send` and `Session::recv` and are handed only this, so
/// they cannot reach the session they are running inside. Everything that needs the
/// session — replenishing a window, resetting a stream — is therefore recorded here and
/// done afterwards.
#[derive(Debug, Default)]
pub(crate) struct Events {
    /// Header blocks still arriving, by stream.
    open: std::collections::BTreeMap<i32, Fields>,
    /// Header blocks that completed.
    heads: Vec<(i32, Fields)>,
    /// Received payload, by stream, awaiting a window credit.
    data: Vec<(i32, usize)>,
    /// Streams that closed, with the code they closed under.
    closes: Vec<(i32, ErrorCode)>,
}

/// Builds the session a client connection runs on.
///
/// Kept separate from [`run`] so the flow-control choice above can be asserted against a
/// real session rather than read off a constant.
pub(crate) fn client_session() -> crate::Result<Session<Events>> {
    SessionBuilder::<Events>::client()
        .manual_flow_control(MANUAL_FLOW_CONTROL)
        .on_begin_headers(|events: &mut Events, frame| {
            if frame.category() == Some(HeaderCategory::Response) {
                events.open.insert(frame.stream_id().get(), Vec::new());
            }
            HeaderAction::Continue
        })
        .on_header(|events: &mut Events, frame, name: &[u8], value: &[u8]| {
            if let Some(fields) = events.open.get_mut(&frame.stream_id().get()) {
                fields.push((name.to_vec(), value.to_vec()));
            }
            HeaderAction::Continue
        })
        .on_frame(|events: &mut Events, frame| {
            if frame.kind() == FrameType::HEADERS && frame.is_end_headers() {
                if let Some(fields) = events.open.remove(&frame.stream_id().get()) {
                    events.heads.push((frame.stream_id().get(), fields));
                }
            }
        })
        .on_data_chunk(|events: &mut Events, stream, chunk: &[u8]| {
            events.data.push((stream.get(), chunk.len()));
        })
        .on_stream_close(|events: &mut Events, stream, code, _failure| {
            events.open.remove(&stream.get());
            events.closes.push((stream.get(), code));
        })
        .build()
}

/// Fails everything still waiting when the driver goes away.
///
/// Taken as an argument to [`run`] rather than created inside it, because an `async fn`
/// stores its arguments in the future the moment it is called. A driver that is dropped
/// without ever being polled therefore still runs this, which is what makes "dropping the
/// connection resolves every pending request" true rather than nearly true.
pub(crate) struct DriverGuard<B> {
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    registry: Arc<Registry>,
}

impl<B> DriverGuard<B> {
    pub(crate) const fn new(
        shared: Arc<Shared>,
        queue: Arc<Queue<B>>,
        registry: Arc<Registry>,
    ) -> Self {
        Self {
            shared,
            queue,
            registry,
        }
    }
}

impl<B> Drop for DriverGuard<B> {
    fn drop(&mut self) {
        // Marked gone first, so a handle racing this sees a closed connection rather than
        // enqueueing a command nothing will ever drain.
        self.shared.set_gone();
        for entry in self.registry.take_all() {
            entry.slot.fail(Error::closed());
        }
        for command in self.queue.drain() {
            let Command::SendRequest { slot, .. } = command;
            slot.fail(Error::closed());
        }
    }
}

/// What the reading half has learned about the far end.
#[derive(Debug, Default)]
struct Intake {
    /// The peer stopped sending, cleanly or otherwise.
    finished: bool,
    failure: Option<std::io::Error>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs the connection until the peer goes away or nothing is left to do.
pub(crate) async fn run<T, B>(
    transport: T,
    mut session: Session<Events>,
    shared: Arc<Shared>,
    queue: Arc<Queue<B>>,
    registry: Arc<Registry>,
    handles: Weak<HandleToken>,
    guard: DriverGuard<B>,
) -> Result<()>
where
    T: Transport,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let (mut reader, mut writer) = transport.split();

    // Locals of this future, borrowed by both halves. Mutexes rather than a `RefCell`
    // because `&RefCell<T>` is never `Sync`, which would make the whole driver non-`Send`
    // and defeat the auto-trait inference the transport traits are shaped around. The
    // locks are never contended — the halves are polled one at a time on one task.
    let inbox = Mutex::new(VecDeque::<BytesMut>::new());
    let pool = Mutex::new(Vec::<BytesMut>::new());
    let intake = Mutex::new(Intake::default());

    let reading = async {
        loop {
            // Pause while the session half is behind. The wake that releases this comes
            // from the session half once it has drained the inbox.
            core::future::poll_fn(|_cx| {
                if lock(&inbox).len() >= READ_AHEAD {
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;

            let mut buf = lock(&pool)
                .pop()
                .unwrap_or_else(|| BytesMut::with_capacity(READ_BUFFER));
            buf.clear();

            let (result, buf) = reader.read(buf).await;
            match result {
                Ok(0) => {
                    lock(&intake).finished = true;
                    shared.wake_driver();
                    return;
                }
                Ok(_) => {
                    lock(&inbox).push_back(buf);
                    shared.wake_driver();
                }
                Err(failure) => {
                    let mut intake = lock(&intake);
                    intake.finished = true;
                    intake.failure = Some(failure);
                    drop(intake);
                    shared.wake_driver();
                    return;
                }
            }
        }
    };

    let driving = async {
        let mut events = Events::default();

        loop {
            for command in queue.drain() {
                let Command::SendRequest { request, slot } = command;
                if let Err(error) = submit(&mut session, &shared, &registry, request, &slot) {
                    slot.fail(error);
                }
            }

            for stream in shared.take_ready() {
                match session.resume_body(StreamId::new(stream)) {
                    Ok(()) => {}
                    // A readiness note that arrived after its stream closed, or after the
                    // body already finished. Benign, and swallowed only here: the same
                    // error from any other call still fails the connection.
                    Err(error) if error.kind() == crate::ErrorKind::InvalidInput => {}
                    Err(error) => return Err(Error::from(error)),
                }
            }

            let mut fed = false;
            loop {
                let next = lock(&inbox).pop_front();
                let Some(mut buf) = next else { break };
                session.recv(&buf, &mut events)?;
                buf.clear();
                lock(&pool).push(buf);
                fed = true;
            }
            if fed {
                // The reading half may be parked on the read-ahead bound; there is room
                // now.
                shared.wake_driver();
            }

            dispatch(&mut session, &mut events, &registry)?;
            flush(&mut session, &mut writer, &mut events).await?;
            // Serialising fires the stream-close handler, so what it observed is
            // dispatched too rather than waiting for the next pass.
            dispatch(&mut session, &mut events, &registry)?;

            if lock(&intake).finished && lock(&inbox).is_empty() {
                if let Some(failure) = lock(&intake).failure.take() {
                    return Err(Error::from(failure));
                }
                if session.mid_frame() {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "the peer stopped sending part-way through a frame",
                    ));
                }
                return Ok(());
            }

            if handles.strong_count() == 0 && registry.is_empty() && !session.want_write() {
                return Ok(());
            }

            // Read before the park rather than inside it: `&Session` is not `Send`, and a
            // shared borrow captured by the closure would be held across the `await`,
            // making the whole driver non-`Send` and defeating the auto-trait inference
            // the transport traits are shaped around. A snapshot is enough — nothing can
            // make the session want to write again except a command, a resume or received
            // octets, and the predicate already watches all three.
            let wants_write = session.want_write();

            core::future::poll_fn(|_cx| {
                // `wants_write` is redundant today — every path above flushes before
                // reaching here — but not structurally so: a later phase that touches the
                // session after the flush would leave octets queued behind a park that no
                // wake is coming for.
                let idle = queue.is_empty()
                    && shared.ready_len() == 0
                    && lock(&inbox).is_empty()
                    && !lock(&intake).finished
                    && !wants_write
                    && !(handles.strong_count() == 0 && registry.is_empty());
                if idle { Poll::Pending } else { Poll::Ready(()) }
            })
            .await;
        }
    };

    let outcome = poll_both(&shared, reading, driving).await;
    drop(guard);
    outcome
}

/// Polls both halves on one task, finishing when the session half does.
///
/// The reading half's completion is noted but does not end the driver: the octets it
/// already posted still have to be fed, and the exchanges they complete still have to be
/// answered. It is the session half that decides when there is nothing left.
///
/// This is also the single place the driver's waker slot is refreshed, which is what makes
/// "refreshed every poll, never captured at submission" a property of one line rather than
/// a convention spread across the file.
async fn poll_both<R, D>(shared: &Shared, reading: R, driving: D) -> D::Output
where
    R: Future<Output = ()>,
    D: Future,
{
    let mut reading = core::pin::pin!(reading);
    let mut driving = core::pin::pin!(driving);
    let mut read_done = false;

    core::future::poll_fn(move |cx| {
        shared.refresh_driver(cx.waker());
        if !read_done && reading.as_mut().poll(cx).is_ready() {
            read_done = true;
        }
        driving.as_mut().poll(cx)
    })
    .await
}

/// Submits one request, linking its stream to the slot that will answer it.
fn submit<B>(
    session: &mut Session<Events>,
    shared: &Arc<Shared>,
    registry: &Registry,
    request: http::Request<B>,
    slot: &Arc<Slot>,
) -> Result<()>
where
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    let (parts, body) = request.into_parts();
    let headers = head::request_headers(&parts)?;
    let views = headers.views();

    // Dropped when the stream leaves the registry, at which point every waker that names
    // this stream stops being able to enqueue anything.
    let liveness = Arc::new(());

    let stream = if body.is_end_stream() {
        session.submit_request(&views)?
    } else {
        let waker = Arc::new(StreamWaker::new(
            Arc::clone(shared),
            Arc::downgrade(&liveness),
        ));
        let outgoing = Outgoing::new(body, Waker::from(Arc::clone(&waker)));
        let stream = session.submit_request_with_body(&views, outgoing)?;
        // The identifier only exists now. Nothing can have consulted the body yet — that
        // happens inside `Session::send` — so no wake can have been lost.
        waker.bind(stream.get());
        stream
    };

    registry.insert(stream.get(), Arc::clone(slot), liveness);
    Ok(())
}

/// Acts on everything the handlers observed.
fn dispatch(session: &mut Session<Events>, events: &mut Events, registry: &Registry) -> Result<()> {
    for (stream, len) in events.data.drain(..) {
        // Phase 4 moves this to the receiving body, where the credit follows what the
        // application has actually read. Crediting on arrival keeps the window open in
        // the meantime, and exercises the manual flow-control path from the start.
        //
        // A stream that has since closed is not an error: the connection-level window
        // still needs the credit, which is what this call gives it.
        let _ = session.consume(StreamId::new(stream), len);
    }

    for (stream, fields) in core::mem::take(&mut events.heads) {
        let Some(slot) = registry.slot(stream) else {
            continue;
        };
        match head::response_head(&fields) {
            Ok(head) => slot.complete(head),
            Err(error) => {
                slot.fail(error);
                session.reset_stream(StreamId::new(stream), ErrorCode::PROTOCOL_ERROR)?;
            }
        }
    }

    for (stream, code) in events.closes.drain(..) {
        let Some(entry) = registry.remove(stream) else {
            continue;
        };
        if !entry.slot.is_settled() {
            entry.slot.fail(if code == ErrorCode::NO_ERROR {
                Error::new(
                    ErrorKind::Stream,
                    "the stream closed before a response head arrived",
                )
            } else {
                Error::new(ErrorKind::Stream, "the peer reset the stream")
            });
        }
    }

    Ok(())
}

/// Writes out everything the session currently has to say.
///
/// Which of the two strategies runs is the transport's choice, because they cannot be
/// combined: the session invalidates each block when the next is asked for, so blocks can
/// only be gathered into one write by copying them.
async fn flush<W: TransportWrite>(
    session: &mut Session<Events>,
    writer: &mut W,
    events: &mut Events,
) -> Result<()> {
    if writer.writes_borrowed() {
        while let Some(block) = session.send(events)? {
            let mut offset = 0;
            while offset < block.len() {
                let written = writer.write_borrowed(&block[offset..]).await?;
                if written == 0 {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "the transport accepted no octets and reported no error",
                    ));
                }
                offset += written;
            }
        }
        return Ok(());
    }

    let mut out = BytesMut::new();
    while let Some(block) = session.send(events)? {
        out.extend_from_slice(block);
    }
    let mut pending = out.freeze();
    while !pending.is_empty() {
        let (result, returned) = writer.write(pending).await;
        let written = result?;
        if written == 0 {
            return Err(Error::new(
                ErrorKind::Transport,
                "the transport accepted no octets and reported no error",
            ));
        }
        pending = returned.slice(written..);
    }
    Ok(())
}
