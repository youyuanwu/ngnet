# A worked exchange

A client and a server, both from this crate, over one connection. The backend here is the
in-memory one so the example runs; a real caller substitutes their own QUIC library behind
the same trait, and nothing else changes.

```rust
use ngnet_h3::http::testing::bytes_crate::Bytes;
use ngnet_h3::http::testing::http_body_crate::{Body, Frame};
use ngnet_h3::http::{IncomingBody, handshake, serve};
use std::pin::Pin;
use std::task::{Context, Poll};

// `bytes::Bytes` is not itself an `http_body::Body`, and the usual adapter lives in
// `http-body-util`. One chunk is enough here.
struct Once(Option<Bytes>);

impl Body for Once {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>)
        -> Poll<Option<Result<Frame<Bytes>, Self::Error>>>
    {
        Poll::Ready(self.0.take().map(|chunk| Ok(Frame::data(chunk))))
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// Two ends of one connection. A real caller brings an established `quinn::Connection`,
// or msquic, or ngtcp2 — this crate never opens one.
let (client_backend, server_backend, _knobs) = ngnet_h3::http::testing::loopback();

// A client is a handle plus a driver. A server is a handler plus a driver.
let (handle, client_driver) = handshake::<_, Once>(client_backend)?;
let server_driver = serve(server_backend, |request: http::Request<IncomingBody>| {
    let path = request.uri().path().to_string();
    async move {
        http::Response::builder()
            .status(200)
            .body(Once(Some(Bytes::from(path))))
            .expect("a response")
    }
})?;

let pending = handle.send_request(
    http::Request::builder()
        .uri("https://example.test/hello")
        .body(Once(None))?,
);

// Nothing has moved yet. **Both drivers must be polled**, and where is the caller's
// business — spawn them, join them, or drive them by hand as here. This crate takes no
// executor, spawner or timer, which is why it cannot do it for you.
# use core::future::Future;
# use std::task::Waker;
# let mut client_driver = Box::pin(client_driver);
# let mut server_driver = Box::pin(server_driver);
# let mut pending = Box::pin(pending);
# let waker = Waker::noop();
# let mut cx = Context::from_waker(waker);
# let mut response = None;
# for _ in 0..1_000 {
#     let _ = client_driver.as_mut().poll(&mut cx);
#     let _ = server_driver.as_mut().poll(&mut cx);
#     if let Poll::Ready(answer) = pending.as_mut().poll(&mut cx) {
#         response = Some(answer?);
#         break;
#     }
# }
let response = response.expect("the drivers were polled, so this resolves");
assert_eq!(response.status(), 200);

// The response body is an `http_body::Body`; reading it is what returns flow-control
// credit to the peer.
# let mut body = response.into_body();
# let mut received = Vec::new();
# for _ in 0..1_000 {
#     match Pin::new(&mut body).poll_frame(&mut cx) {
#         Poll::Ready(Some(Ok(frame))) => {
#             if let Ok(data) = frame.into_data() {
#                 received.extend_from_slice(&data);
#             }
#         }
#         Poll::Ready(None) | Poll::Ready(Some(Err(_))) => break,
#         Poll::Pending => {
#             let _ = client_driver.as_mut().poll(&mut cx);
#             let _ = server_driver.as_mut().poll(&mut cx);
#         }
#     }
# }
assert_eq!(&received[..], b"/hello");
# Ok(())
# }
```
