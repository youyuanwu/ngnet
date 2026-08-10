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
    // and the delivery claim `is_retriable` makes is therefore true — repeating this request
    // cannot duplicate an effect, because it had none.
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
        assert!(error.is_retriable());
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
async fn a_refused_stream_is_retriable_and_a_bad_uri_is_not() {
    // The conservative half of the retry rule, and the name matters: an earlier draft called
    // this `only_an_exchange_failure_is_ever_retriable`, which is false. `Connect` is
    // retriable too, and unconditionally so — nothing reached a peer, so nothing was acted
    // on. The name asserted a contract the crate does not implement and the test did not
    // check, which is the quiet kind of wrong: it would have been read as authority for the
    // opposite rule. The connect case is asserted in the matrix test above.
    //
    // What this pins is the *conditional* half. `Exchange` is retriable only when `ngnet-h2`
    // was willing to say the stream was never begun, and `Uri` never is.
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

#[tokio::test]
async fn a_connect_failure_keeps_the_io_error_as_its_source() {
    // The same claim as above, made separately because it is the one that was false. The
    // connect path formatted its cause into the message — `"connecting to {origin} failed:
    // {source}"` — which reads perfectly and leaves nothing to downcast to. A caller wanting
    // to distinguish `ConnectionRefused` from `HostUnreachable`, to decide whether another
    // address is worth trying, could only have got there by parsing English.
    //
    // The chain is two links: this crate's context, then the `io::Error` under it.
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

    assert_eq!(error.kind(), ErrorKind::Connect);

    // Walked rather than indexed. The chain has an intermediate link — a dial failure is
    // shared between everyone who waited on it, so it arrives wrapped — and a caller has no
    // business knowing that. Walking to the end is what `anyhow`, `eyre` and hand-written
    // matches all do, and it keeps this test pinning the property (the `io::Error` is
    // reachable) rather than the pool's current internal shape.
    let io = sources(&error)
        .find_map(|link| link.downcast_ref::<std::io::Error>())
        .unwrap_or_else(|| {
            let chain: Vec<String> = sources(&error).map(|link| link.to_string()).collect();
            panic!("no io::Error anywhere in the chain: {chain:#?}")
        });
    assert_eq!(io.kind(), std::io::ErrorKind::ConnectionRefused);

    // And the message is unchanged by keeping the cause typed: the whole chain still renders.
    let rendered = error.to_string();
    assert!(
        rendered.contains("connecting to") && rendered.contains(&address.to_string()),
        "the message should still name the origin, got: {rendered}"
    );
}

/// Every link below an error, in order.
///
/// `std::error::Error::sources` is still unstable, so this is the two lines it will one day
/// replace.
fn sources<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(error.source(), |link| link.source())
}
