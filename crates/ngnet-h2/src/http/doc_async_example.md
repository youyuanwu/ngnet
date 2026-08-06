# The asynchronous API

The default `http` feature adds an asynchronous HTTP/2 client and server over this same
core, reached through the [`http`](crate::http) module. It pulls in `http`, `http-body`
and `bytes` and speaks entirely in their types: a request is an `http::Request`, a
response body is an `http_body::Body`. Disabling the feature returns the crate to exactly
the pure state machine above — one dependency, no async, no I/O of any kind. Turn it off
when you already have your own HTTP types, or want the smallest possible dependency set;
the sans-I/O API loses nothing when you do.

A whole client exchange follows, using only ecosystem types and no runtime. Real code
spawns the driver onto its runtime and awaits the response elsewhere; here the two share
one task so the example depends on nothing of its own.

```
# use core::future::poll_fn;
# use core::pin::Pin;
# use ngnet_h2::http::{handshake, serve, IncomingBody};
# use ngnet_h2::http::testing::{
#     alongside, block_on, duplex, http_crate as http, Empty, Full,
# };
# use ngnet_h2::http::testing::http_body_crate::Body;
# fn main() {
# let (client_io, server_io) = duplex(false);
# let server = serve(server_io, |_request: http::Request<IncomingBody>| async {
#     http::Response::builder().status(200).body(Full::new("hello")).unwrap()
# })
# .expect("a server session");
// `Empty` and `Full` stand in for `http_body_util`'s bodies of the same names, so this
// example needs no dependency of its own; real code would use that crate.
let (requests, connection) = handshake::<_, Empty>(client_io).expect("a client session");

let exchange = async {
    let request = http::Request::get("http://example.test/").body(Empty).unwrap();
    let response = requests.send_request(request).await.expect("a response head");
    assert_eq!(response.status(), 200);

    // `http_body` ships no combinators, so the frames are read out directly.
    let mut body = response.into_body();
    let mut received = Vec::new();
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        if let Some(chunk) = frame.expect("a body frame").data_ref() {
            received.extend_from_slice(chunk);
        }
    }
    assert_eq!(received, b"hello");
};

// One task drives the connection and awaits the exchange together. A real caller spawns
// `connection` and awaits the response on whatever task it likes.
# block_on(alongside(exchange, alongside(connection, server)));
# }
```
