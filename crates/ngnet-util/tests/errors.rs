//! The four error categories, each produced by the thing that is supposed to produce it.
//!
//! Categories only earn their keep if a caller can act on them differently, and they only
//! stay trustworthy if each is actually reachable. A category nothing produces is a lie in
//! the documentation; two categories produced by the same cause are one category with two
//! names. This file drives one real failure per kind, in one place, so that the distinction
//! survives changes to the code that makes it.

mod support;

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::{Client, ErrorKind};
use support::raw::{Behaviour, RawPeer};
use support::{TestServer, get, within};

#[tokio::test]
async fn the_four_kinds_are_each_reachable_and_distinct() {
    let mut produced = Vec::new();

    // Uri: the caller's mistake. No peer involved, nothing dialled, retrying cannot help.
    {
        let client: Client<Full<Bytes>> = Client::new();
        let uri: http::Uri = "/relative".parse().expect("a valid relative URI");
        let error = within("the URI failure", client.request(get(uri)))
            .await
            .expect_err("a relative URI has no origin");
        assert!(!error.is_retriable());
        produced.push(error.kind());
    }

    // Connect: this end could not reach the peer. Nothing was sent, so nothing was acted on,
    // but the request is not retriable *on this client* — the origin is what failed.
    {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");
        drop(listener);

        let client: Client<Full<Bytes>> = Client::new();
        let uri: http::Uri = format!("http://{address}/x").parse().expect("a valid URI");
        let error = within("the connect failure", client.request(get(uri)))
            .await
            .expect_err("nothing is listening");
        produced.push(error.kind());
    }

    // Closed: this end is going away. Distinguished from Connect because no dial was even
    // attempted, and from Exchange because no peer was involved.
    {
        let server = TestServer::start().await;
        let client: Client<Full<Bytes>> = Client::new();
        within("shutdown", client.shutdown()).await;
        let error = within("the late request", client.request(get(server.uri("/x"))))
            .await
            .expect_err("the client is closed");
        assert!(!error.is_retriable());
        produced.push(error.kind());
    }

    // Exchange: the connection was made and the request failed on it. The only kind that can
    // be retriable, and only when the peer said the stream was never begun.
    {
        let peer = RawPeer::start(Behaviour::RefuseEverything).await;
        let client: Client<Full<Bytes>> = Client::new();
        let error = within("the exchange failure", client.request(get(peer.uri("/x"))))
            .await
            .expect_err("the peer refuses everything");
        assert!(error.is_retriable());
        produced.push(error.kind());
    }

    assert_eq!(
        produced,
        vec![
            ErrorKind::Uri,
            ErrorKind::Connect,
            ErrorKind::Closed,
            ErrorKind::Exchange,
        ],
        "each documented kind must be produced by the cause it documents"
    );
}

#[tokio::test]
async fn only_an_exchange_failure_is_ever_retriable() {
    // The conservative half of the retry rule. Everything except a refused stream either was
    // or might have been acted on, or will fail identically next time.
    let peer = RawPeer::start(Behaviour::RefuseEverything).await;
    let client: Client<Full<Bytes>> = Client::new();

    let error = within("the request", client.request(get(peer.uri("/x"))))
        .await
        .expect_err("the peer refuses everything");
    assert_eq!(error.kind(), ErrorKind::Exchange);
    assert!(error.is_retriable());

    let uri: http::Uri = "/relative".parse().expect("a valid relative URI");
    assert!(
        !within("the URI failure", client.request(get(uri)))
            .await
            .expect_err("no origin")
            .is_retriable()
    );
}

#[tokio::test]
async fn an_exchange_failure_keeps_the_protocol_error_as_its_source() {
    // A caller wanting the HTTP/2 detail must be able to reach it. Flattening the cause into
    // a string would leave them parsing prose.
    let peer = RawPeer::start(Behaviour::RefuseEverything).await;
    let client: Client<Full<Bytes>> = Client::new();

    let error = within("the request", client.request(get(peer.uri("/x"))))
        .await
        .expect_err("the peer refuses everything");

    let source = std::error::Error::source(&error).expect("an exchange failure has a cause");
    assert!(
        source.downcast_ref::<ngnet_h2::http::Error>().is_some(),
        "the cause should be the protocol error itself, got: {source}"
    );
}
