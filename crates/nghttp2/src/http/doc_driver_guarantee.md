## A connection makes no progress until its driver is polled

The asynchronous API hands back a driver rather than starting one, because where it runs
is the caller's business — this crate spawns nothing. [`http::handshake`]
returns a request handle *and* the driver; [`http::serve`] returns the
driver alone. Until it is polled nothing moves: no request is sent, no response arrives,
and a response future simply never resolves. Keeping the handle and dropping the driver is
a real trap — a connection that compiles and never sends a byte — so the driver is
[`#[must_use]`](crate::http::Connection) and discarding it is a compile error:

```compile_fail
#![deny(unused_must_use)]
# use nghttp2::http::testing::{Duplex, Empty, duplex};
# fn example() -> Result<(), nghttp2::http::Error> {
let (transport, _peer) = duplex(false);
// The handle is kept and the driver thrown away, so nothing will ever be sent.
nghttp2::http::handshake::<Duplex, Empty>(transport)?;
# Ok(())
# }
```

Keeping it is not an error:

```
# use nghttp2::http::testing::{Duplex, Empty, duplex};
# fn example() -> Result<(), nghttp2::http::Error> {
let (transport, _peer) = duplex(false);
let (requests, connection) = nghttp2::http::handshake::<Duplex, Empty>(transport)?;
# let _ = (requests, connection);
# Ok(())
# }
```
