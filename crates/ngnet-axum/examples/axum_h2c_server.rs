//! A tiny h2c server on tokio whose handler is an axum [`Router`].
//!
//! Run it, then talk to it with any HTTP/2 client that speaks cleartext with prior
//! knowledge — there is no TLS and no upgrade dance:
//!
//! ```text
//! cargo run -p ngnet-axum --example axum_h2c_server
//! curl --http2-prior-knowledge -i http://127.0.0.1:8080/hello
//! curl --http2-prior-knowledge -i --data 'ping' http://127.0.0.1:8080/echo
//! curl --http2-prior-knowledge -i http://127.0.0.1:8080/whoami
//! ```
//!
//! Compare this with `ngnet-h2`'s own `h2c_async_server.rs`, which answers the same first
//! two routes. That example owns its dispatch: it matches on the path by hand, builds each
//! response by hand, and defines its own body type. This one registers routes and lets axum
//! do all of it — which is the entire point of the crate, and the comparison says more than
//! either file does alone.
//!
//! What is *not* different is worth noticing too. There is no adapter anywhere in this
//! file, and none hidden in the crate: `ngnet-h2`'s request body is already the shape axum
//! wants, and axum's response body is already the shape `ngnet-h2` wants.
//!
//! [`Router`]: axum::Router

use std::error::Error as StdError;

use axum::Router;
use axum::extract::Request;
use axum::routing::{get, post};
use ngnet_axum::PeerAddr;
use tokio::net::TcpListener;

type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

#[tokio::main]
async fn main() -> Fallible {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned());

    let router = Router::new()
        .route("/hello", get(hello))
        .route("/echo", post(echo))
        .route("/whoami", get(whoami));

    let listener = TcpListener::bind(&address).await?;
    println!("listening on http://{}", listener.local_addr()?);
    println!("this is h2c: use --http2-prior-knowledge, not --http2");

    // Ctrl-C stops the server accepting. Established connections are *not* torn down: they
    // end when their peers end them, which is quiescence rather than a drain. A connection
    // an idle peer holds open holds the server open with it, so a real deployment wants a
    // timeout around this rather than an unbounded wait.
    ngnet_axum::serve(listener, router)
        .on_error(|error| eprintln!("connection failed: {error}"))
        .with_stop_signal(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("stopping: no new connections will be accepted");
        })
        .await;

    println!("all connections finished");
    Ok(())
}

/// A greeting, to show routing works at all.
async fn hello() -> &'static str {
    "hello from ngnet-h2, with no hyper underneath\n"
}

/// Answers a request with its own body.
///
/// The `String` extractor reads the request body — the same body `ngnet-h2` produced, handed
/// to axum without conversion.
async fn echo(body: String) -> String {
    body
}

/// The peer's address, which arrives as an extension rather than through `ConnectInfo`.
///
/// `ConnectInfo` is gated behind axum's `tokio` feature, which depends on `hyper-util`.
/// Using it would put hyper back into the dependency graph this crate exists to avoid, so
/// the address is inserted as [`PeerAddr`] instead and read like any other extension.
async fn whoami(request: Request) -> String {
    match request.extensions().get::<PeerAddr>() {
        Some(peer) => format!("you are {peer}\n"),
        None => "no peer address was recorded\n".to_owned(),
    }
}
