# The driver must be polled

A connection is two things: a handle, and this future. The handle queues work; the future
performs it. Until it is polled, no request is sent, no response arrives, and a response
future never resolves.

That makes discarding the driver the trap, and it is why this type is `#[must_use]` — writing

```compile_fail
#![deny(unused_must_use)]
# use ngnet_h3::http::testing::loopback;
# use ngnet_h3::http::testing::bytes_crate::Bytes;
# struct Empty;
# impl ngnet_h3::http::testing::http_body_crate::Body for Empty {
#     type Data = Bytes;
#     type Error = std::convert::Infallible;
#     fn poll_frame(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>)
#         -> std::task::Poll<Option<Result<ngnet_h3::http::testing::http_body_crate::Frame<Bytes>, Self::Error>>> {
#         std::task::Poll::Ready(None)
#     }
# }
# fn main() {
let (backend, _server, _knobs) = loopback();
ngnet_h3::http::handshake::<_, Empty>(backend).expect("handshake");
# }
```

Note what that `deny` is doing. `#[must_use]` raises a *warning*, so the protection is only as
strong as the caller's lint settings — a crate that does not deny it gets a diagnostic rather
than a refusal. It is still worth having, because the warning names the exact trap and appears
at the moment the mistake is made; it is not a guarantee, and this page would be lying if it
implied otherwise.

is a compile error rather than a connection that silently does nothing.

Holding the handle while dropping the driver is the same mistake with the compiler unable to
help, so it is defined instead of undefined: every request in flight fails with
[`ErrorKind::Closed`](crate::http::ErrorKind::Closed), immediately, rather than hanging.
