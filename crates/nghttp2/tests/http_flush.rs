//! The driver's flush contract: octets it produces reach the peer before it waits on one.
//!
//! A transport is allowed to buffer its writes — `tokio::io::BufWriter` and `BufStream` do,
//! and they satisfy the `AsyncWrite` bound `TokioIo` accepts. For such a transport `write`
//! only fills a buffer; the octets become peer-visible when it is flushed. The driver's
//! obligation is to flush ([`TransportWrite::commit`]) after draining a write pass and
//! before it parks awaiting readable input, so it never blocks on a response to a request
//! still sitting in a buffer.
//!
//! `testing::buffering()` is exactly such a transport. This exercise drives a full request
//! and response over it and asserts the exchange completes. Remove the driver's `commit`
//! call and it does not: the request never leaves the buffer, the peer never answers, and
//! the client waits forever. The budget below turns that regression into a failure rather
//! than a hung suite.

#![cfg(feature = "http")]

use core::future::{Future, poll_fn};
use core::task::{Context, Poll};

use std::cell::Cell;
use std::io;
use std::rc::Rc;

use nghttp2::http::testing::{
    Duplex, DuplexReader, DuplexWriter, Empty, Full, alongside, block_on, buffering,
    bytes_crate as bytes, duplex, http_crate as http,
};
use nghttp2::http::transport::{Transport, TransportWrite};
use nghttp2::http::{IncomingBody, server};

/// Drives `work`, but gives up after `budget` self-woken polls.
///
/// The in-memory executor parks on a condvar when every future returns `Pending`, so a
/// genuine deadlock would block the test thread forever. Self-waking each poll keeps the
/// executor re-polling until either `work` finishes or the budget runs out, at which point
/// this returns `None` and the caller can fail deliberately instead of hanging.
async fn within_budget<F: Future>(work: F, budget: usize) -> Option<F::Output> {
    let mut work = Box::pin(work);
    let mut left = budget;
    poll_fn(move |cx: &mut Context<'_>| {
        if let Poll::Ready(value) = work.as_mut().poll(cx) {
            return Poll::Ready(Some(value));
        }
        if left == 0 {
            return Poll::Ready(None);
        }
        left -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

fn get(path: &str) -> http::Request<Empty> {
    http::Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(Empty)
        .expect("building a request")
}

#[test]
fn a_buffering_transport_still_completes_an_exchange() {
    // The client's writing half buffers until `commit`; the peer is an ordinary duplex.
    let (client_transport, server_transport) = buffering();

    let (requests, connection) =
        nghttp2::http::handshake::<_, Empty>(client_transport).expect("handshake");

    let serving = server::serve(server_transport, |request: http::Request<IncomingBody>| {
        drop(request.into_body());
        async move {
            http::Response::builder()
                .status(200)
                .header("x-answered", "yes")
                .body(Empty)
                .expect("a response")
        }
    })
    .expect("serving");

    let exchange = async {
        let response = requests
            .send_request(get("/buffered"))
            .await
            .expect("a response");
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-answered")
                .and_then(|value| value.to_str().ok()),
            Some("yes"),
            "the response did not round-trip through the buffering transport",
        );
        drop(requests);
    };

    // A healthy exchange settles in well under this many polls; the budget only bites if the
    // driver stops flushing, which is the regression this test exists to catch.
    let outcome = block_on(within_budget(
        alongside(alongside(exchange, connection), serving),
        200_000,
    ));

    assert!(
        outcome.is_some(),
        "the exchange never completed: without the driver's commit, a buffering transport \
         holds the request and the peer never sees it",
    );
}

// ----- the same obligation, on the gathering path -----

/// A buffering transport that elects the *vectored* write path.
///
/// `testing::buffering()` leaves both fast-path overrides at their defaults, so it can only
/// ever make its point about the coalesced drain. The gathering drain reaches `commit`
/// through a different sequence of calls, and a driver that flushed only after an owned
/// write would pass the test above while stranding every gathered pass — so the obligation
/// has to be restated against a transport that gathers.
///
/// Defined here rather than in `testing.rs` on purpose: it exists to make one point in one
/// file, and the crate's public testing surface is pinned by `compat_surface.rs`, which is
/// not a place to add things casually.
struct GatheringBuffer {
    inner: Duplex,
    /// Gathering calls actually polled, shared with the test so it can tell whether the
    /// path it means to exercise was taken at all.
    gathered: Rc<Cell<usize>>,
}

struct GatheringBufferWriter {
    inner: DuplexWriter,
    /// Octets written but not yet handed to the peer — the user-space buffer a `BufWriter`
    /// would keep.
    buffer: Vec<u8>,
    gathered: Rc<Cell<usize>>,
}

impl Transport for GatheringBuffer {
    type Reader = DuplexReader;
    type Writer = GatheringBufferWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (reader, writer) = self.inner.split();
        (
            reader,
            GatheringBufferWriter {
                inner: writer,
                buffer: Vec::new(),
                gathered: self.gathered,
            },
        )
    }
}

impl TransportWrite for GatheringBufferWriter {
    fn write(
        &mut self,
        buf: bytes::Bytes,
    ) -> impl Future<Output = (io::Result<usize>, bytes::Bytes)> {
        self.buffer.extend_from_slice(&buf);
        let written = buf.len();
        core::future::ready((Ok(written), buf))
    }

    fn write_vectored<'w>(
        &'w mut self,
        regions: &'w [io::IoSlice<'w>],
    ) -> Option<impl Future<Output = io::Result<usize>> + 'w> {
        // Nothing happens here: the driver builds one of these with no regions at all to
        // learn which path this transport offers, and drops it without polling. An `async`
        // block is inert until polled, so the copy below only ever runs for a real write.
        Some(async move {
            self.gathered.set(self.gathered.get() + 1);
            let mut written = 0;
            for region in regions {
                self.buffer.extend_from_slice(region);
                written += region.len();
            }
            Ok(written)
        })
    }

    async fn commit(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let data = core::mem::take(&mut self.buffer);
        let (result, _buf) = self.inner.write(bytes::Bytes::from(data)).await;
        result.map(|_| ())
    }
}

#[test]
fn a_buffering_transport_that_gathers_still_completes_an_exchange() {
    // The same exchange as above, over a transport that elects the gathering path. The
    // budget is what turns "the driver stopped committing after a gathered pass" into a
    // failure rather than a hung suite.
    let (client_transport, server_transport) = duplex(false);
    let gathered = Rc::new(Cell::new(0usize));
    let client_transport = GatheringBuffer {
        inner: client_transport,
        gathered: Rc::clone(&gathered),
    };

    let (requests, connection) =
        nghttp2::http::handshake::<_, Full>(client_transport).expect("handshake");

    let serving = server::serve(server_transport, |request: http::Request<IncomingBody>| {
        drop(request.into_body());
        async move {
            http::Response::builder()
                .status(200)
                .header("x-answered", "yes")
                .body(Full::new(&b"gathered"[..]))
                .expect("a response")
        }
    })
    .expect("serving");

    let exchange = async {
        let response = requests
            .send_request(
                http::Request::builder()
                    .method(http::Method::POST)
                    .uri("http://example.test/gathered")
                    // Larger than the driver's threshold, so the pass carries a block that
                    // is gathered beside the accumulation rather than folded into it: both
                    // halves of the strategy run before `commit` is reached.
                    .body(Full::new(vec![b'x'; 4096]))
                    .expect("a request"),
            )
            .await
            .expect("a response");
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("x-answered")
                .and_then(|value| value.to_str().ok()),
            Some("yes"),
            "the response did not round-trip through the gathering transport",
        );
        drop(requests);
    };

    let outcome = block_on(within_budget(
        alongside(alongside(exchange, connection), serving),
        200_000,
    ));

    assert!(
        outcome.is_some(),
        "the exchange never completed: a gathered pass left behind a buffer nobody flushed",
    );
    assert!(
        gathered.get() > 0,
        "no gathering call was ever polled, so this exercised the coalesced path again and \
         proved nothing the test above had not",
    );
}
