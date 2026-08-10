//! Connection reuse: what makes this a pool rather than a connect helper.
//!
//! Every assertion here is on the server's accept count, because every one of these tests
//! passes trivially if you only check the responses. That is the point.

mod support;

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::Client;
use support::{TestServer, collect, get, within};

#[tokio::test]
async fn sequential_requests_share_one_connection() {
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    for path in ["/one", "/two", "/three"] {
        let response = within("the request", client.request(get(server.uri(path))))
            .await
            .expect("the request succeeds");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(collect(response).await, Bytes::from(path));
    }

    // The whole test. Without this line it passes against a client that opens three sockets,
    // which is precisely the implementation this crate exists to replace.
    assert_eq!(
        server.accepts(),
        1,
        "three requests to one origin should share one connection"
    );

    // And all three arrived on that one connection rather than the count merely being right.
    let seen = server.seen();
    assert_eq!(seen.len(), 3);
    assert!(seen.iter().all(|request| request.connection == 1));
}

#[tokio::test]
async fn a_pooled_request_resolves_no_name() {
    // "No resolution happened" has no observable at a peer, which saw no new connection
    // either way — so this is asserted on the client's own counter, through the hidden
    // testing module. It is one of the three deliberate exceptions to the assert-at-the-peer
    // rule, and the reason is that there is nothing else to assert on.
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first request", client.request(get(server.uri("/one"))))
        .await
        .expect("the request succeeds");
    let after_first = ngnet_util::testing::resolution_count(&client);
    assert_eq!(after_first, 1, "the first request must dial");

    let _ = within(
        "the second request",
        client.request(get(server.uri("/two"))),
    )
    .await
    .expect("the request succeeds");

    assert_eq!(
        ngnet_util::testing::resolution_count(&client),
        after_first,
        "a request served by a pooled connection must not resolve the name again"
    );
}

#[tokio::test]
async fn two_origins_get_a_connection_each() {
    let first = TestServer::start().await;
    let second = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let _ = within("the first", client.request(get(first.uri("/a"))))
        .await
        .expect("succeeds");
    let _ = within("the second", client.request(get(second.uri("/b"))))
        .await
        .expect("succeeds");

    assert_eq!(first.accepts(), 1);
    assert_eq!(second.accepts(), 1);

    // Neither server saw the other's request. A pool keyed on host alone, or on nothing,
    // would send both to whichever it dialled first.
    assert_eq!(first.seen().len(), 1);
    assert_eq!(first.seen()[0].path, "/a");
    assert_eq!(second.seen().len(), 1);
    assert_eq!(second.seen()[0].path, "/b");
}

#[tokio::test]
async fn host_case_and_an_explicit_default_port_do_not_split_the_pool() {
    // A server on port 80 is not available to a test, so the two halves of this are checked
    // separately: case-insensitivity end to end against a real server, and default-port
    // equivalence at the origin level in `origins.rs`. Localhost gives a name whose case can
    // be varied while still resolving.
    let server = TestServer::start().await;
    let port = server.address.port();
    let client: Client<Full<Bytes>> = Client::new();

    for authority in [format!("localhost:{port}"), format!("LOCALHOST:{port}")] {
        let uri: http::Uri = format!("http://{authority}/case")
            .parse()
            .expect("a valid URI");
        let _ = within("the request", client.request(get(uri)))
            .await
            .expect("succeeds");
    }

    assert_eq!(
        server.accepts(),
        1,
        "host case must not produce a second connection"
    );
}

#[tokio::test]
async fn an_ipv6_origin_connects_and_shares_one_connection_across_spellings() {
    let server = TestServer::start_v6().await;
    let port = server.address.port();
    let client: Client<Full<Bytes>> = Client::new();

    // The first half: an IPv6 literal works at all. `Uri::host` returns `[::1]`, brackets
    // included, and no resolver accepts that — so without unbracketing this fails as though
    // the host were unreachable.
    let response = within(
        "the compressed form",
        client.request(get(server.uri("/v6"))),
    )
    .await
    .expect("an IPv6 origin is reachable");
    assert_eq!(response.status(), http::StatusCode::OK);

    // The second half: the same address written the long way is the same origin. All three
    // spellings are already lower-case, so lower-casing does not collapse them — only parsing
    // the address does.
    for authority in [
        format!("[0:0:0:0:0:0:0:1]:{port}"),
        format!("[0000:0000:0000:0000:0000:0000:0000:0001]:{port}"),
    ] {
        let uri: http::Uri = format!("http://{authority}/v6")
            .parse()
            .expect("a valid URI");
        let _ = within("a long-form IPv6 request", client.request(get(uri)))
            .await
            .expect("succeeds");
    }

    assert_eq!(
        server.accepts(),
        1,
        "three spellings of one address must share one connection"
    );
}

#[tokio::test]
async fn concurrent_requests_to_a_cold_origin_open_one_connection() {
    // The test that a sequential suite cannot make. The obvious pool — look up, find nothing,
    // dial — passes every test above and opens ten sockets here.
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let mut tasks = Vec::new();
    for index in 0..10 {
        let client = client.clone();
        let uri = server.uri(&format!("/concurrent-{index}"));
        tasks.push(tokio::spawn(async move {
            client.request(get(uri)).await.map(|_| ())
        }));
    }

    for task in tasks {
        within("a concurrent request", task)
            .await
            .expect("the task completes")
            .expect("the request succeeds");
    }

    assert_eq!(
        server.accepts(),
        1,
        "ten concurrent requests to a cold origin must open exactly one connection"
    );
    assert_eq!(server.seen().len(), 10, "and all ten must be answered");
}

#[tokio::test]
async fn a_request_body_reaches_the_server() {
    let server = TestServer::start().await;
    let client: Client<Full<Bytes>> = Client::new();

    let request = http::Request::post(server.uri("/echo"))
        .body(support::body("the payload"))
        .expect("a valid request");

    let _ = within("the request", client.request(request))
        .await
        .expect("succeeds");

    assert_eq!(server.seen()[0].body, Bytes::from_static(b"the payload"));
}
