//! A tiny h2c server on tokio, using the asynchronous API.
//!
//! Run it, then talk to it with any HTTP/2 client that speaks cleartext with prior
//! knowledge — there is no TLS and no upgrade dance:
//!
//! ```text
//! cargo run -p ngnet-h2 --features tokio --example h2c_async_server
//! curl --http2-prior-knowledge -i http://127.0.0.1:8080/hello
//! curl --http2-prior-knowledge -i --data 'ping' http://127.0.0.1:8080/echo
//! ```
//!
//! A request to `/echo` is answered with its own body; anything else gets a greeting.
//! Pass an address as the first argument to bind somewhere other than `127.0.0.1:8080`.
//!
//! Compare this with `h2c_server.rs`, which does the same job over `std::net` and the
//! sans-I/O core. That one owns the framing loop; this one hands the socket over and writes
//! a handler. The runtime-specific part of this file is three lines: the listener, the
//! spawn, and `TokioIo::new`.

use std::error::Error as StdError;

use ngnet_h2::http::transport::TokioIo;
use ngnet_h2::http::{IncomingBody, server};

use bytes::Bytes;
use http_body::{Body, Frame};

type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::main]
async fn main() -> Fallible {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned());

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("h2c server listening on http://{}", listener.local_addr()?);
    println!("try: curl --http2-prior-knowledge -i http://{addr}/hello");

    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true)?;

        // One task per connection. The connection itself needs no task of its own: it *is*
        // the future, and its handlers run inside it.
        tokio::spawn(async move {
            let connection = match server::serve(TokioIo::new(stream), answer) {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("{peer}: could not start: {error}");
                    return;
                }
            };
            if let Err(error) = connection.await {
                eprintln!("{peer}: {error}");
            }
        });
    }
}

/// Answers one request.
///
/// An ordinary `async` function. Whatever it awaits, every other stream on the same
/// connection carries on — but it must not *block*, since there is no other thread for the
/// connection to run on while it does.
async fn answer(request: http::Request<IncomingBody>) -> http::Response<Answer> {
    let echo = request.uri().path() == "/echo";
    let mut body = request.into_body();

    let mut received = Vec::new();
    while let Some(frame) = next_frame(&mut body).await {
        match frame {
            Ok(frame) => {
                if let Some(data) = frame.data_ref() {
                    received.extend_from_slice(data);
                }
            }
            Err(error) => {
                eprintln!("reading a request body: {error}");
                return http::Response::builder()
                    .status(http::StatusCode::BAD_REQUEST)
                    .body(Answer::new(&b"could not read the request body\n"[..]))
                    .expect("a well-formed response");
            }
        }
    }

    let payload = if echo {
        received
    } else {
        b"hello from ngnet-h2\n".to_vec()
    };

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header("content-type", "text/plain")
        .body(Answer::new(payload))
        .expect("a well-formed response")
}

/// The next frame of a received body, written out because `http_body` ships no combinators
/// and this example takes no further dependencies.
async fn next_frame(body: &mut IncomingBody) -> Option<Result<Frame<Bytes>, ngnet_h2::http::Error>> {
    core::future::poll_fn(|context| core::pin::Pin::new(&mut *body).poll_frame(context)).await
}

/// A response body already held in memory.
struct Answer {
    data: Option<Bytes>,
}

impl Answer {
    fn new(data: impl Into<Bytes>) -> Self {
        Self {
            data: Some(data.into()),
        }
    }
}

impl Body for Answer {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: core::pin::Pin<&mut Self>,
        _context: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        core::task::Poll::Ready(self.data.take().map(|data| Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none()
    }
}
