//! What happens when there is nothing to connect to.
//!
//! The interesting claim in this file is not that a failure is reported — that is hard to get
//! wrong — but that the failure is *not remembered*. A pool that caches a dial error has
//! turned a transient outage into a permanent one for the life of the process, and the only
//! way to show it does not is to fail a dial, then make the origin work, and send again on
//! the same client.

mod support;

use std::net::{Ipv4Addr, SocketAddr};

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::{Client, ErrorKind};
use support::{TestServer, get, within};

/// A loopback port with nothing listening on it.
///
/// Bound and then dropped, rather than picked out of the air: an arbitrary high port might
/// have something on it, which would make this test pass or fail for reasons belonging to
/// whatever else is running on the machine. Binding first proves the port was free, and
/// dropping the listener leaves it free with a connect that is refused rather than dropped.
async fn a_free_port() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("a bound listener");
    let address = listener.local_addr().expect("a bound address");
    drop(listener);
    address
}

#[tokio::test]
async fn nothing_listening_is_a_connect_failure() {
    let address = a_free_port().await;
    let client: Client<Full<Bytes>> = Client::new();

    let uri: http::Uri = format!("http://{address}/nowhere")
        .parse()
        .expect("a valid URI");
    let error = within("the request", client.request(get(uri)))
        .await
        .expect_err("nothing is listening");

    assert_eq!(error.kind(), ErrorKind::Connect);
    // The origin is named, because a client with several origins reporting "connection
    // refused" and nothing else is a client whose errors cannot be acted on.
    assert!(
        error.to_string().contains(&address.to_string()),
        "the error should name the origin it failed to reach, got: {error}"
    );
}

#[tokio::test]
async fn a_failed_dial_is_not_remembered() {
    // The test this file exists for. `Dial::Failed` is a real state in the pool, and the
    // question is whether a *later* caller inherits it. It must not: nothing about a refused
    // connect says the origin will still be down a second later.
    let address = a_free_port().await;
    let client: Client<Full<Bytes>> = Client::new();

    let uri: http::Uri = format!("http://{address}/late")
        .parse()
        .expect("a valid URI");

    let error = within("the doomed request", client.request(get(uri.clone())))
        .await
        .expect_err("nothing is listening yet");
    assert_eq!(error.kind(), ErrorKind::Connect);

    // Now put a server on exactly that address. The same client, the same origin, and the
    // pool's slot for it currently holding a failure.
    let server = TestServer::start_at(address).await;

    let response = within("the second request", client.request(get(uri)))
        .await
        .expect("the origin is up now, so the request must succeed");
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(server.accepts(), 1);
}

#[tokio::test]
async fn a_failed_dial_leaves_nothing_pooled() {
    let address = a_free_port().await;
    let client: Client<Full<Bytes>> = Client::new();

    let uri: http::Uri = format!("http://{address}/nothing")
        .parse()
        .expect("a valid URI");
    let _ = within("the request", client.request(get(uri)))
        .await
        .expect_err("nothing is listening");

    assert!(
        !ngnet_util::testing::has_eligible_connection(&client, &address.to_string()),
        "a dial that failed must not leave a connection behind"
    );
}

#[tokio::test]
async fn a_host_that_does_not_resolve_is_a_connect_failure() {
    // `.invalid` is reserved by RFC 2606 precisely so that it never resolves, which means
    // this test does not depend on the machine's DNS answering — or on it existing.
    let client: Client<Full<Bytes>> = Client::new();

    let uri: http::Uri = "http://nothing.invalid:8080/x"
        .parse()
        .expect("a valid URI");
    let error = within("the request", client.request(get(uri)))
        .await
        .expect_err("the name cannot resolve");

    assert_eq!(
        error.kind(),
        ErrorKind::Connect,
        "a name that will not resolve is a connect failure, not a URI one: the URI was fine"
    );
}

#[tokio::test]
async fn a_uri_with_no_authority_is_a_uri_failure() {
    let client: Client<Full<Bytes>> = Client::new();

    // Reported before anything is dialled, which is the distinction `ErrorKind::Uri` exists
    // to draw: the caller made a mistake, no peer was involved, and retrying cannot help.
    let uri: http::Uri = "/relative".parse().expect("a valid relative URI");
    let error = within("the request", client.request(get(uri)))
        .await
        .expect_err("there is no origin to send this to");

    assert_eq!(error.kind(), ErrorKind::Uri);
    assert!(!error.is_retriable());
}

#[tokio::test]
async fn concurrent_requests_to_a_dead_origin_all_fail() {
    // The fan-out rule, in its failing direction. Ten callers, one dial: every one of them
    // must come back, and none may be left parked behind a dial that has already settled.
    let address = a_free_port().await;
    let client: Client<Full<Bytes>> = Client::new();

    let mut tasks = Vec::new();
    for index in 0..10 {
        let client = client.clone();
        let uri: http::Uri = format!("http://{address}/dead-{index}")
            .parse()
            .expect("a valid URI");
        tasks.push(tokio::spawn(async move {
            client.request(get(uri)).await.map(|_| ())
        }));
    }

    for task in tasks {
        let outcome = within("a concurrent request", task)
            .await
            .expect("the task completes");
        assert_eq!(
            outcome.expect_err("nothing is listening").kind(),
            ErrorKind::Connect
        );
    }
}
