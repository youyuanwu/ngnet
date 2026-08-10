//! Properties of the public API that are invisible to the other tests.
//!
//! Everything else in this suite drives the client over a real socket, which is the right way
//! to test behaviour but says nothing about whether the API can be *used* from outside. A
//! caller depending only on `ngnet-util` is a situation no behavioural test reproduces,
//! because the tests can reach whatever the crate re-exports and would not notice if the
//! re-export vanished.
//!
//! Every path below is deliberately spelled `ngnet_util::…`. Reaching for `ngnet_h2::…` here
//! would defeat the entire point of the file.

use std::future::Future;

use bytes::Bytes;
use http_body_util::Full;
use ngnet_util::{Builder, Client, Config, Error, ErrorKind, IncomingBody, Origin, ResponseFuture};

/// Every type a caller touches can be written down without naming another crate (Rust API
/// guideline C-UNNAMEABLE).
///
/// The response type in particular: `IncomingBody` is `ngnet-h2`'s, and without the re-export
/// a caller could call `request` but could not store its output in a struct, return it from a
/// function, or name it in a signature.
#[allow(dead_code)]
fn the_request_type_is_nameable(
    client: &Client<Full<Bytes>>,
    request: http::Request<Full<Bytes>>,
) -> ResponseFuture {
    client.request(request)
}

#[allow(dead_code)]
fn the_response_type_is_nameable(
    future: ResponseFuture,
) -> impl Future<Output = Result<http::Response<IncomingBody>, Error>> {
    future
}

/// The configuration type is `ngnet-h2`'s, and is re-exported for the same reason.
#[allow(dead_code)]
fn a_client_can_be_configured(config: Config) -> Client<Full<Bytes>> {
    Builder::new().config(config).build()
}

#[test]
fn a_client_is_send_sync_and_clone() {
    // All three are load-bearing rather than incidental. `Send + Sync` is what makes cloning
    // a client into tasks the intended way to use it; `Clone` is what makes those clones
    // share one pool rather than becoming independent clients.
    fn assert_shareable<T: Send + Sync + Clone + 'static>() {}
    assert_shareable::<Client<Full<Bytes>>>();
}

#[test]
fn a_response_future_is_send() {
    // Without this, a request cannot be `tokio::spawn`ed, which is how most callers will use
    // one. A future that is accidentally `!Send` compiles fine until somebody tries.
    fn assert_send<T: Send>() {}
    assert_send::<ResponseFuture>();
}

#[test]
fn the_error_is_a_standard_error() {
    // `Send + Sync + 'static` is what lets a caller put this in `Box<dyn Error>`, in
    // `anyhow`, or across a task boundary. `std::error::Error` is what lets them walk
    // `source()` to the protocol failure underneath.
    fn assert_usable<T: std::error::Error + Send + Sync + 'static>() {}
    assert_usable::<Error>();
}

#[test]
fn the_error_kind_can_be_matched_and_compared() {
    fn assert_matchable<T: Copy + PartialEq + Eq + std::fmt::Debug>() {}
    assert_matchable::<ErrorKind>();

    // `#[non_exhaustive]`, so a caller must have a wildcard arm. This is that shape, and it
    // compiles only while the kinds a caller is told about still exist.
    let kind = ErrorKind::Connect;
    let described = match kind {
        ErrorKind::Uri => "uri",
        ErrorKind::Connect => "connect",
        ErrorKind::Closed => "closed",
        ErrorKind::Exchange => "exchange",
        _ => "something added later",
    };
    assert_eq!(described, "connect");
}

#[test]
fn an_origin_can_be_inspected() {
    // Public because it appears in error messages and in the pool's keying rules, both of
    // which a caller may reasonably want to reason about.
    let uri: http::Uri = "http://EXAMPLE.com/path".parse().expect("a valid URI");
    let origin = Origin::from_uri(&uri).expect("a valid origin");
    assert_eq!(origin.host(), "example.com");
    assert_eq!(origin.port(), 80);
    assert_eq!(origin.to_string(), "example.com:80");
}

#[test]
fn a_client_can_be_built_without_a_configuration() {
    // `Default` and `new` must agree, because a caller who writes one and a caller who writes
    // the other should not get different clients.
    let _: Client<Full<Bytes>> = Client::new();
    let _: Client<Full<Bytes>> = Client::default();
    let _: Client<Full<Bytes>> = Builder::new().build();
    // And through `Client::builder`, which needs `B` named because it has one to infer.
    let _: Client<Full<Bytes>> = Client::<Full<Bytes>>::builder().build();
}

#[test]
fn the_public_types_compose_from_this_crate_alone() {
    // The assertion is the signatures above; reaching here means they compiled.
}
