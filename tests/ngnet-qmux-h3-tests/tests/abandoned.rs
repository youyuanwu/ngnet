//! A request abandoned partway through its body.
//!
//! An exchange that a caller walks away from is not an error, but it is not nothing either:
//! the peer is holding a half-read request, a stream, and the flow-control credit that went
//! with it. It has to be told. This test asserts both halves of that — the peer learns, and
//! the connection is left in a state where the next request works.

use core::cell::RefCell;
use std::rc::Rc;

use bytes::Bytes;
use ngnet_qmux_h3_tests::{LIMIT, Stalling, drain, memory_pair_sending, ok, pattern};
use tokio::task::LocalSet;
use tokio::time::timeout;

/// Big enough that the server is certain to be mid-body when the client walks away.
const FIRST: usize = 32 * 1024;

#[tokio::test]
async fn an_abandoned_request_body_informs_the_peer_and_leaves_the_connection_usable() {
    LocalSet::new()
        .run_until(async {
            // What the server made of the abandoned request, recorded where the test can
            // read it. An `Rc` rather than a channel because nothing here crosses a thread,
            // which is the arrangement the whole in-memory suite is built to exercise.
            let observed: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
            let recorder = Rc::clone(&observed);

            let sender = memory_pair_sending::<Stalling, _, _, _>(move |request| {
                let recorder = Rc::clone(&recorder);
                async move {
                    if request.uri().path() == "/abandoned" {
                        // Reading the body is what makes the abandonment observable: the
                        // read is what fails when the peer resets the stream, and a handler
                        // that never read would notice nothing.
                        let outcome = drain(request.into_body()).await;
                        *recorder.borrow_mut() = Some(outcome.map_or_else(
                            |error| error.to_string(),
                            |bytes| format!("a complete body of {} bytes", bytes.len()),
                        ));
                    }
                    ok("acknowledged")
                }
            });

            let request = http::Request::builder()
                .method("POST")
                .uri("https://qmux.test/abandoned")
                .body(Stalling::new(pattern(FIRST)))
                .expect("a request");

            // Polled until the peer has the request and its first chunk, and only then
            // dropped: a caller that has lost interest partway is the situation the HTTP/3
            // layer turns into a reset, and a request dropped before it was ever sent would
            // prove nothing about what the peer is told. The response never arrives, because
            // the handler is waiting on a body that has stopped.
            let abandoned = sender.send_request(request);
            let outcome = timeout(core::time::Duration::from_millis(50), abandoned).await;
            assert!(
                outcome.is_err(),
                "the handler cannot answer a body that never ends, so this must time out and \
                 drop the exchange: {:?}",
                outcome.map(|response| response.map(|response| response.status())),
            );

            // The later request is also what drives the connection far enough for the reset
            // to have been written and read.
            let later = http::Request::builder()
                .method("GET")
                .uri("https://qmux.test/later")
                .body(Stalling::empty())
                .expect("a request");
            let later = timeout(LIMIT, sender.send_request(later))
                .await
                .expect("the later request must not hang")
                .expect("the connection must still be usable");
            assert_eq!(later.status(), 200);
            let body = timeout(LIMIT, drain(later.into_body()))
                .await
                .expect("the later body must not hang")
                .expect("a body");
            assert_eq!(body, Bytes::from_static(b"acknowledged"));

            let observed = observed.borrow().clone().expect(
                "the server must have seen the abandoned request at all; a client that \
                 dropped an exchange without telling the peer leaves it waiting on a stream \
                 nobody will finish",
            );
            assert!(
                observed.contains("reset") || observed.contains("cancel"),
                "and it must have learned the request was abandoned rather than completed: \
                 {observed}",
            );
        })
        .await;
}
