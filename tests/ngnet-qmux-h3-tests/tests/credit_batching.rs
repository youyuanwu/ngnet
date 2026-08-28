//! How often the HTTP/3 driver reports flow-control credit, and how often that reaches QMux.
//!
//! # The question, and the answer
//!
//! Spec FR-037 asks whether window extensions and the wakeups they cause are *already*
//! coalesced within a single transmit pass, and requires the record to state which it turned
//! out to be. `CodeResearch.md` left it open because it did not read
//! `crates/ngnet-h3/src/http/driver.rs`.
//!
//! **They are not.** `Driver::extend` makes two `extend_credit` calls for the same bytes -- the
//! stream's window and the connection's, which are separate and neither implies the other --
//! and it is reached from three places within one pass: once per `QuicEvent::Data` the driver
//! applied, once per stream whose QPACK-deferred credit was released, and once per credit entry
//! the caller returned by reading. Nothing between those sites accumulates. This file is the
//! evidence, taken by counting the calls the driver actually made rather than by reading its
//! loop, so that the answer cannot silently change: a driver that started coalescing would fail
//! [`the_http3_driver_does_not_coalesce_its_credit_reports`], and that failure would be the
//! finding being overturned rather than a defect.
//!
//! # What was done about it
//!
//! Batched at the QMux--HTTP/3 seam, in `crates/ngnet-qmux-h3/src/connection.rs`: a run of
//! `extend_credit` calls is accumulated -- one sum per stream, one for the connection -- and
//! applied at the first interaction with the layer below that follows. `ngnet-h3` is untouched,
//! which matters because it is shared with the QUIC stack, a stack that cannot be fully built
//! on this host and so could not have been verified here.
//!
//! # Why the assertions are counts
//!
//! For the same reason the vectored-write assertions are (see `fragmented_offers.rs`, and Spec
//! FR-021 and FR-038): what this removes is calls into dwnx and firings of the read-ahead
//! waker, not work with a shape a benchmark identifier could resolve. A timed comparison would
//! report a number inside its own noise. The counts below are properties of the code, and they
//! fail if the batching stops happening.
//!
//! # Why the exchange is concurrent
//!
//! Because that is where the batching has something to save. Each stream that delivered in a
//! pass needs its own stream-window extension whether or not the reports are batched, so the
//! stream half of the saving only appears when one stream delivers twice in a pass. The
//! connection window is one window shared by every stream, and the driver reports it once per
//! delivery -- so on a connection carrying several streams at once, a pass reports it many
//! times over and the batching turns all of them into one. That is also the extension that
//! wakes the read-ahead pump, which makes it the figure worth stating.

mod transmit_harness;

use core::cell::RefCell;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::rc::Rc;

use bytes::Bytes;
use http::Request;
use http_body_util::Full;
use ngnet_h3::http::{QuicConnection, QuicEvent, StreamSource};
use ngnet_h3::{ErrorCode, StreamId, Timestamp};
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use ngnet_qmux::io::{AsyncByteStream, Clock};
use ngnet_qmux_h3::{HttpConfig, QmuxConnection, TransportConfig};
use ngnet_qmux_h3_tests::{Payload, drain, ok, pattern};
use transmit_harness::{Turns, collected};

/// How many requests are in flight at once.
///
/// Enough that a pass routinely delivers on several streams, which is the arrangement the
/// connection window is reported many times over in. Small enough that the whole exchange still
/// runs inside a hand-driven harness in well under a second.
const CONCURRENCY: usize = 8;

/// How large each response body is.
///
/// Sized so that a body arrives in several deliveries rather than one, since a delivery is what
/// a credit report is per.
const BODY: usize = 64 * 1024;

/// Wide enough that flow control never becomes the thing being measured.
///
/// A pass cut short by an exhausted window would report credit at a rhythm set by the window
/// rather than by the driver, and the count would then be a fact about this test's
/// configuration.
fn windows() -> TransportConfig {
    TransportConfig::new()
        .initial_max_stream_data(8 << 20)
        .initial_max_data(64 << 20)
}

/// What one run of the driver asked of its transport, and what reached the layer below.
#[derive(Clone, Default)]
struct Counts {
    /// Every `extend_credit` call naming a stream.
    stream_calls: usize,
    /// Every `extend_credit` call naming the connection.
    connection_calls: usize,
    /// Calls of either kind since the current pass began.
    in_pass: usize,
    /// The most calls any one pass made.
    ///
    /// A pass is delimited by `poll_transmit`, which the driver reaches once per iteration of
    /// its loop and after every credit site (`driver.rs`, steps 1-2, 4 and 8). Counting between
    /// two of those is therefore counting within a pass, which is the unit FR-037 is stated
    /// over.
    busiest_pass: usize,
    /// How many transmit passes there were.
    passes: usize,
    /// Applications of either kind on the connection below.
    applications: u64,
    /// Applications of the connection window on the connection below.
    connection_applications: u64,
}

impl Counts {
    /// Every credit call the driver made, of either kind.
    fn calls(&self) -> usize {
        self.stream_calls + self.connection_calls
    }
}

/// A transport that counts what the driver asks of it and forwards everything unchanged.
///
/// Deliberately a decorator rather than a counter inside `ngnet-qmux-h3`: the question FR-037
/// asks is what the *driver* does, and a counter on the far side of the seam would measure the
/// seam's answer to it instead. The application counts do come from the far side, because they
/// are the other half of the comparison and there is no way to see them from here.
struct Counting<T> {
    inner: T,
    counts: Rc<RefCell<Counts>>,
}

impl<S: AsyncByteStream, C: Clock> Counting<QmuxConnection<S, C>> {
    /// Copies the layer below's application counts into the shared record.
    ///
    /// Called after every forwarded call rather than at one chosen point, because credit is
    /// applied at whichever interaction happens to come next and there is no interaction that
    /// is reliably the last one.
    fn sync(&self) {
        let applications = self.inner.credit_applications();
        let connection = self.inner.connection_credit_applications();
        let mut counts = self.counts.borrow_mut();
        counts.applications = applications;
        counts.connection_applications = connection;
    }
}

impl<S: AsyncByteStream, C: Clock> QuicConnection for Counting<QmuxConnection<S, C>> {
    type Error = <QmuxConnection<S, C> as QuicConnection>::Error;

    const RETAINS_BUFFERS: bool = QmuxConnection::<S, C>::RETAINS_BUFFERS;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        let event = self.inner.poll_event(cx);
        self.sync();
        event
    }

    fn poll_transmit<Src: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut Src,
    ) -> Poll<Result<(), Self::Error>> {
        {
            let mut counts = self.counts.borrow_mut();
            counts.passes += 1;
            counts.busiest_pass = counts.busiest_pass.max(counts.in_pass);
            counts.in_pass = 0;
        }
        let outcome = self.inner.poll_transmit(cx, source);
        self.sync();
        outcome
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let outcome = self.inner.poll_flush(cx);
        self.sync();
        outcome
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let opened = self.inner.poll_open_uni(cx);
        self.sync();
        opened
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let opened = self.inner.poll_open_bi(cx);
        self.sync();
        opened
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        let outcome = self.inner.reset(stream, code);
        self.sync();
        outcome
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        let outcome = self.inner.stop_sending(stream, code);
        self.sync();
        outcome
    }

    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        {
            let mut counts = self.counts.borrow_mut();
            if stream.is_some() {
                counts.stream_calls += 1;
            } else {
                counts.connection_calls += 1;
            }
            counts.in_pass += 1;
        }
        self.inner.extend_credit(stream, bytes)
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error> {
        let outcome = self.inner.close(code, reason);
        self.sync();
        outcome
    }

    fn now(&self) -> Timestamp {
        self.inner.now()
    }
}

/// Polls every future it was given until all of them are done.
///
/// A local join, because this crate has no futures library and one combinator is cheaper than
/// one dependency. Order is not meaningful: the point is that no exchange is awaited to
/// completion before the others have been polled, so all `CONCURRENCY` streams deliver in the
/// same passes.
struct AllOf<T> {
    pending: Vec<Pin<Box<dyn Future<Output = T>>>>,
    done: Vec<T>,
}

impl<T> AllOf<T> {
    fn new(pending: Vec<Pin<Box<dyn Future<Output = T>>>>) -> Self {
        let done = Vec::with_capacity(pending.len());
        Self { pending, done }
    }
}

impl<T: Unpin> Future for AllOf<T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
        let this = self.get_mut();
        let mut index = 0;
        while index < this.pending.len() {
            match this.pending[index].as_mut().poll(cx) {
                Poll::Ready(value) => {
                    this.done.push(value);
                    drop(this.pending.remove(index));
                }
                Poll::Pending => index += 1,
            }
        }
        if this.pending.is_empty() {
            Poll::Ready(core::mem::take(&mut this.done))
        } else {
            Poll::Pending
        }
    }
}

/// Runs `CONCURRENCY` simultaneous downloads, counting the client's credit reports.
///
/// The client is built by hand rather than through `ngnet_qmux_h3::connect_with`, because the
/// counting decorator has to sit between the HTTP/3 driver and the QMux connection and
/// `connect_with` has no seam to put it at. Everything else here is that function's own wiring.
/// `Turns::drive` leaves both drivers unfinished on purpose, so the close tail `connect_with`
/// would also have built has nothing to do.
fn exchange() -> Counts {
    let (client_io, server_io) = stream_pair();
    // Taken before the stream is moved into the connection: the log is a handle to shared
    // state, and there is no way to reach the stream again once the connection owns it.
    let log = client_io.write_log();
    let clock = TestClock::new();
    let transport = windows();
    let http = HttpConfig::default();

    let serving = ngnet_qmux_h3::serve_with(
        server_io,
        clock.clone(),
        |request| async move {
            let (_parts, incoming) = request.into_parts();
            let _ = drain(incoming).await.expect("the request body");
            ok(pattern(BODY))
        },
        transport,
        http,
    )
    .expect("serving");

    let counts = Rc::new(RefCell::new(Counts::default()));
    let backend = Counting {
        inner: QmuxConnection::client_with(client_io, clock, transport).expect("a client"),
        counts: Rc::clone(&counts),
    };
    let (sender, connection) =
        ngnet_h3::http::handshake_with::<_, Payload>(backend, http).expect("a client driver");

    let exchange = async move {
        // Every request is submitted before any response is awaited, so all `CONCURRENCY`
        // streams are open at once. Submitting inside the join would let the first exchange
        // finish before the last had started, which is a serial run wearing a concurrent shape.
        let mut sending = Vec::with_capacity(CONCURRENCY);
        for index in 0..CONCURRENCY {
            let request = Request::builder()
                .method("GET")
                .uri(format!("https://qmux.test/download/{index}"))
                .body(Full::new(Bytes::new()))
                .expect("a request");
            sending.push(sender.send_request(request));
        }

        let mut exchanges: Vec<Pin<Box<dyn Future<Output = usize>>>> = Vec::new();
        for response in sending {
            exchanges.push(Box::pin(async move {
                let response = response.await.expect("a response");
                assert_eq!(response.status(), 200);
                collected(response.into_body()).await.len()
            }));
        }
        AllOf::new(exchanges).await
    };

    let (lengths, _turns) = Turns::drive(&log, connection, serving, exchange);
    assert_eq!(
        lengths.len(),
        CONCURRENCY,
        "every request must produce a response, or the counts below are the counts of something \
         other than the exchange this measures"
    );
    for length in lengths {
        assert_eq!(
            length, BODY,
            "a response body arrived short, so the credit reported for it is not the credit a \
             whole exchange reports"
        );
    }

    let counts = counts.borrow();
    counts.clone()
}

/// FR-037's question, answered by counting rather than by reading the loop.
///
/// The claim is negative -- the driver does **not** batch -- so the assertion has to be that a
/// single pass forwarded several calls. Two would be what a driver that batched *per level*
/// produced, and one what a driver that batched outright produced, so the bar is set above
/// both.
#[test]
fn the_http3_driver_does_not_coalesce_its_credit_reports() {
    let counts = exchange();

    assert!(
        counts.busiest_pass > 2,
        "the busiest of {} passes forwarded {} credit calls. More than two is what says the \
         driver reports credit per delivery rather than once per level per pass; at two or \
         fewer the HTTP/3 layer has started coalescing them itself, and FR-037's answer has \
         changed from 'no' to 'yes'. That is a finding to re-record, not a defect: the batching \
         in `ngnet-qmux-h3` would then be redundant rather than wrong",
        counts.passes,
        counts.busiest_pass
    );
    assert!(
        counts.calls() > counts.passes,
        "{} credit calls across {} passes is at most one per pass, which is coalescing by \
         another name",
        counts.calls(),
        counts.passes
    );
    assert_eq!(
        counts.stream_calls, counts.connection_calls,
        "the driver reports the same bytes to both levels, one call each, so the two counts \
         move together. They have come apart, which means `Driver::extend` no longer does what \
         the finding above says it does"
    );
}

/// And what the seam does with them: a run of reports becomes one extension per window.
///
/// The other half of the measurement FR-037 asks for. `connection_credit_applications` counts
/// the connection-window extensions that reached the QMux connection, and before the batching
/// landed that figure was equal to the driver's connection-level call count by construction,
/// because each call was forwarded straight through. So the driver's count *is* the "before"
/// figure and no stashed build is needed to compare against it.
///
/// The bound is a ratio rather than an exact figure because how many deliveries a body arrives
/// in depends on the record size, the pump's read-ahead and the peer's write rhythm, none of
/// which this test should have an opinion about.
///
/// One bias, stated: credit still held when the run ends is never applied, because
/// `Turns::drive` stops at the exchange rather than at a closed connection. That flatters the
/// "after" figure by at most one flush -- one connection extension -- which is far inside the
/// margin asserted below.
#[test]
fn a_run_of_credit_reports_reaches_the_connection_as_one_extension_per_window() {
    let counts = exchange();

    let reported = counts.connection_calls as u64;
    assert!(
        counts.connection_applications < reported,
        "{reported} connection-window reports became {} extensions of it on the connection \
         below, which is not fewer: the batching is not happening, and every report is still a \
         call into dwnx and a read-ahead wakeup",
        counts.connection_applications
    );
    assert!(
        counts.connection_applications * 2 <= reported,
        "{reported} connection-window reports became {} extensions, less than a halving. With \
         {CONCURRENCY} streams delivering at once a pass reports the shared window many times \
         over, so anything near parity means runs are being broken up by something that need \
         not break them",
        counts.connection_applications
    );
    assert!(
        counts.applications < counts.calls() as u64,
        "{} credit calls of both kinds became {} applications, which is not fewer",
        counts.calls(),
        counts.applications
    );
}
