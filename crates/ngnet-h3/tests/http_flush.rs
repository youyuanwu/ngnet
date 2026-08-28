#![cfg(feature = "http")]
//! The transport flush which precedes suspension is part of the driver contract.

use core::convert::Infallible;
use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Wake;

use ngnet_h3::http::{ErrorKind, QuicConnection, QuicEvent, StreamSource, handshake};
use ngnet_h3::{ErrorCode, StreamId, Timestamp};

mod support;
use support::{Payload, Stub, empty, stub};

#[derive(Clone, Copy)]
enum ParkAt {
    Bind,
    Open,
    Transmit,
}

#[derive(Clone, Copy)]
enum Flush {
    Ready,
    PendingOnce,
    Fail,
}

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

fn map_poll<T>(poll: Poll<Result<T, Infallible>>) -> Poll<Result<T, TestError>> {
    match poll {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
        Poll::Ready(Err(never)) => match never {},
    }
}

fn map_result<T>(result: Result<T, Infallible>) -> Result<T, TestError> {
    match result {
        Ok(value) => Ok(value),
        Err(never) => match never {},
    }
}

struct Parking {
    inner: Stub,
    park: Option<ParkAt>,
    flush: Flush,
    flushes: Arc<AtomicUsize>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl Parking {
    fn new(
        park: Option<ParkAt>,
        flush: Flush,
    ) -> (Self, Arc<AtomicUsize>, Arc<Mutex<Vec<&'static str>>>) {
        let (inner, _) = stub();
        let flushes = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner,
                park,
                flush,
                flushes: Arc::clone(&flushes),
                calls: Arc::clone(&calls),
            },
            flushes,
            calls,
        )
    }

    fn note(&self, call: &'static str) {
        self.calls.lock().expect("call log").push(call);
    }

    fn parks(&mut self, at: ParkAt) -> bool {
        if matches!(
            (self.park, at),
            (Some(ParkAt::Bind), ParkAt::Bind)
                | (Some(ParkAt::Open), ParkAt::Open)
                | (Some(ParkAt::Transmit), ParkAt::Transmit)
        ) {
            self.park = None;
            true
        } else {
            false
        }
    }
}

impl QuicConnection for Parking {
    type Error = TestError;

    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        self.note("event");
        map_poll(self.inner.poll_event(cx))
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        self.note("transmit");
        if self.parks(ParkAt::Transmit) {
            Poll::Pending
        } else {
            map_poll(self.inner.poll_transmit(cx, source))
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.note("flush");
        self.flushes.fetch_add(1, Ordering::SeqCst);
        match self.flush {
            Flush::Ready => Poll::Ready(Ok(())),
            Flush::PendingOnce => {
                self.flush = Flush::Ready;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Flush::Fail => Poll::Ready(Err(TestError("flush failed"))),
        }
    }

    fn poll_open_uni(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.note("open_uni");
        if self.parks(ParkAt::Bind) {
            Poll::Pending
        } else {
            map_poll(self.inner.poll_open_uni(cx))
        }
    }

    fn poll_open_bi(&mut self, cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        self.note("open_bi");
        if self.parks(ParkAt::Open) {
            Poll::Pending
        } else {
            map_poll(self.inner.poll_open_bi(cx))
        }
    }

    fn reset(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        map_result(self.inner.reset(stream, code))
    }

    fn stop_sending(&mut self, stream: StreamId, code: ErrorCode) -> Result<(), Self::Error> {
        map_result(self.inner.stop_sending(stream, code))
    }

    fn extend_credit(&mut self, stream: Option<StreamId>, bytes: u64) -> Result<(), Self::Error> {
        map_result(self.inner.extend_credit(stream, bytes))
    }

    fn close(&mut self, code: ErrorCode, reason: &[u8]) -> Result<(), Self::Error> {
        map_result(self.inner.close(code, reason))
    }

    fn now(&self) -> Timestamp {
        self.inner.now()
    }
}

fn request(handle: &ngnet_h3::http::SendRequest<Payload>) {
    let _response = handle.send_request(
        http::Request::builder()
            .uri("https://example.test/")
            .body(empty())
            .expect("a request"),
    );
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    future.poll(&mut cx)
}

#[test]
fn every_driver_suspension_flushes_the_transport() {
    for (park, submit, operation) in [
        (Some(ParkAt::Bind), false, "open_uni"),
        (Some(ParkAt::Open), true, "open_bi"),
        (Some(ParkAt::Transmit), true, "transmit"),
        (None, false, "event"),
    ] {
        let (backend, flushes, calls) = Parking::new(park, Flush::Ready);
        let (handle, driver) = handshake::<_, Payload>(backend).expect("constructing a connection");
        if submit {
            request(&handle);
        }
        let mut driver = Box::pin(driver);

        assert!(poll_once(driver.as_mut()).is_pending());
        assert_eq!(
            flushes.load(Ordering::SeqCst),
            1,
            "one impending suspension must perform one flush"
        );
        let calls = calls.lock().expect("call log");
        assert!(
            calls.ends_with(&[operation, "flush"]),
            "the driver must poll the operation before flushing at its suspension: {calls:?}"
        );
    }
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn a_pending_flush_registers_the_wake_that_resumes_it() {
    for (park, submit, operation) in [
        (Some(ParkAt::Bind), false, "open_uni"),
        (Some(ParkAt::Open), true, "open_bi"),
        (Some(ParkAt::Transmit), true, "transmit"),
        (None, false, "event"),
    ] {
        let (backend, flushes, calls) = Parking::new(park, Flush::PendingOnce);
        let (handle, driver) = handshake::<_, Payload>(backend).expect("constructing a connection");
        if submit {
            request(&handle);
        }
        let mut driver = Box::pin(driver);
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);

        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert_eq!(
            wakes.0.load(Ordering::SeqCst),
            1,
            "the pending flush at {operation} must provide the only continuation"
        );
        assert!(
            calls
                .lock()
                .expect("call log")
                .ends_with(&[operation, "flush"])
        );

        assert!(driver.as_mut().poll(&mut cx).is_pending());
        assert!(
            flushes.load(Ordering::SeqCst) >= 2,
            "the resumed driver must reach its ordinary idle flush"
        );
    }
}

#[test]
fn a_flush_failure_is_a_transport_error() {
    for (park, submit, operation) in [
        (Some(ParkAt::Bind), false, "open_uni"),
        (Some(ParkAt::Open), true, "open_bi"),
        (Some(ParkAt::Transmit), true, "transmit"),
        (None, false, "event"),
    ] {
        let (backend, _flushes, calls) = Parking::new(park, Flush::Fail);
        let (handle, driver) = handshake::<_, Payload>(backend).expect("constructing a connection");
        if submit {
            request(&handle);
        }
        let mut driver = Box::pin(driver);

        let outcome = match poll_once(driver.as_mut()) {
            Poll::Ready(outcome) => outcome,
            Poll::Pending => panic!("the flush failure at {operation} was swallowed"),
        };
        let error = outcome.expect_err("the connection should fail");
        assert_eq!(error.kind(), ErrorKind::Transport);
        assert!(
            calls
                .lock()
                .expect("call log")
                .windows(2)
                .any(|pair| pair == [operation, "flush"]),
            "the failing flush did not follow {operation}"
        );
    }
}
