//! SC-025. Both surfaces are usable from a caller whose types are not `Send`.
//!
//! The claim is a compile-time one, and it is checked the only way a compile-time claim can
//! be: by writing the code that would fail to compile if it were false. `connect` and
//! `serve` are instantiated over an in-memory byte stream built on `Rc` and `RefCell`, and
//! the futures they return are required to be nothing more than futures.
//!
//! It matters because the alternative is invisible. A `Send` bound added anywhere in this
//! crate — on a lock, on a spawned task, on an error type — would compile perfectly well and
//! would simply make the whole thing unusable on a thread-per-core runtime, which is the
//! arrangement a QUIC-adjacent stack is most likely to be deployed in. The mirror assertions
//! are as important: a `Send` byte stream must still yield a `Send` connection, or the
//! surface is unusable on a work-stealing runtime instead.

use core::future::Future;

use ngnet_qmux::io::testing::{TestByteStream, TestClock};
use ngnet_qmux::io::{TokioClock, TokioStream};
use ngnet_qmux_h3::QmuxConnection;
use ngnet_qmux_h3_tests::{LIMIT, Payload, drain, get, memory_pair, memory_streams, ok};
use tokio::net::TcpStream;
use tokio::task::LocalSet;
use tokio::time::timeout;

/// Answers whether a concrete type is `Send`, without requiring that it is.
///
/// The inherent method wins name resolution when its bound is satisfied and the trait method
/// is found otherwise, which is what lets a test assert the *absence* of an auto trait — a
/// thing the language offers no direct way to state.
struct Probe<T>(core::marker::PhantomData<T>);

impl<T: Send> Probe<T> {
    fn is_send(&self) -> bool {
        true
    }
}

trait NotSend {
    fn is_send(&self) -> bool {
        false
    }
}

impl<T> NotSend for Probe<T> {}

fn probe<T>() -> Probe<T> {
    Probe(core::marker::PhantomData)
}

/// The instantiations themselves. Building them is the whole of the claim.
///
/// Neither connection is polled, and neither needs to be: what is under test is that these
/// lines compile at all. The bound the futures are held to is `Future` and nothing else, so
/// a `Send` requirement anywhere in the returned types would be a compilation failure here
/// rather than a surprise in a caller's crate.
#[test]
fn both_surfaces_build_over_a_non_send_byte_stream() {
    fn requires_only_a_future<F: Future>(_: F) {}

    let (client_io, server_io, clock) = memory_streams();

    let (sender, connection) =
        ngnet_qmux_h3::connect::<_, _, Payload>(client_io, clock.clone()).expect("a client");
    requires_only_a_future(connection);
    drop(sender);

    let served = ngnet_qmux_h3::serve(server_io, clock, |_request| async {
        ok("no send bound anywhere in sight")
    })
    .expect("a server");
    requires_only_a_future(served);
}

#[test]
fn the_transport_follows_its_byte_stream_rather_than_imposing_send() {
    assert!(
        !probe::<QmuxConnection<TestByteStream, TestClock>>().is_send(),
        "a connection over an `Rc`-based byte stream must not be `Send`; if it were, the \
         bound would be coming from this crate rather than from the caller's own types, and \
         the assertion below would be vacuous",
    );
    assert!(
        probe::<QmuxConnection<TokioStream<TcpStream>, TokioClock>>().is_send(),
        "and a connection over a `Send` byte stream must be `Send`, or nothing built here \
         could be handed to a work-stealing runtime",
    );
}

/// And it runs, not merely compiles.
#[tokio::test]
async fn a_non_send_connection_completes_an_exchange() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|_request| async { ok("answered without a Send bound") });
            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
                .await
                .expect("the request must not hang")
                .expect("a response");
            assert_eq!(response.status(), 200);
            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");
            assert_eq!(body.as_ref(), b"answered without a Send bound");
        })
        .await;
}
