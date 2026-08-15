//! Properties of the public API that are invisible to the other tests.
//!
//! Everything else in this suite drives the server over a real socket, which is the right
//! way to test behaviour but says nothing about whether the API can be *used* from outside.
//! A caller depending only on `ngnet-axum` is a situation no behavioural test reproduces,
//! because the tests are inside the crate and can reach anything.

use std::error::Error as _;
use std::future::Future;
use std::net::SocketAddr;

use axum::Router;
use ngnet_axum::{Config, Connection, EngineResult, TokioIo, serve, serve_connection};
use tokio::net::TcpStream;

/// `serve_connection`'s return type can be written down by a caller (Rust API guideline
/// C-UNNAMEABLE).
///
/// The function returns `ngnet-h2`'s `Connection` wrapped in `ngnet-h2`'s `Result`. Both
/// were once reachable only through a crate a caller has no other reason to depend on,
/// which meant the function could be called but its result could not be stored in a struct,
/// returned from a function, or named in a signature. Re-exporting fixes it; this compiles
/// only while that stays true, and it deliberately uses *only* `ngnet_axum` paths.
#[allow(dead_code)]
fn the_connection_type_is_nameable(
    stream: TcpStream,
    router: Router,
    peer: SocketAddr,
    config: Config,
) -> EngineResult<Connection<impl Future<Output = EngineResult<()>>>> {
    serve_connection(TokioIo::new(stream), router, peer, config)
}

#[test]
fn the_public_types_compose_from_this_crate_alone() {
    // The assertion is the signature above; reaching here means it compiled.
}

/// SC-017: the same property holds for a transport that is not a TCP stream.
///
/// The signature above would compile even if the crate had stayed TCP-shaped, since a
/// `TcpStream` is what it always took. This one uses an in-memory pipe and an address type
/// that is not a socket address, so it compiles only while `serve_connection` is genuinely
/// generic over both -- and, as above, using only `ngnet_axum` paths.
#[allow(dead_code)]
fn the_connection_type_is_nameable_for_a_non_socket_transport(
    pipe: tokio::io::DuplexStream,
    router: Router,
    peer: String,
    config: Config,
) -> EngineResult<Connection<impl Future<Output = EngineResult<()>>>> {
    serve_connection(TokioIo::new(pipe), router, peer, config)
}

/// A third-party listener is writable using only this crate's public API.
///
/// `transports.rs` proves this behaviourally; this pins it as a compile-time property, so
/// that a change which made [`Listener`](ngnet_axum::Listener) unimplementable from outside
/// would fail here even if the behavioural test were deleted.
///
/// It is written as a direct impl because that is now the only way to write one. There used
/// to be two traits here -- an easier fallible one and a wrapper supplying retry -- which
/// existed because the server's accept loop dropped and rebuilt this future constantly. The
/// loop has two arms now and they are gone; what an implementor writes is what axum's
/// implementors write.
#[allow(dead_code)]
fn a_listener_is_implementable_from_outside() {
    struct Outside;

    impl ngnet_axum::Listener for Outside {
        type Io = TokioIo<tokio::io::DuplexStream>;
        type Addr = String;

        async fn accept(&mut self) -> (Self::Io, Self::Addr) {
            std::future::pending().await
        }
    }

    // And that it is something `serve` accepts, with no wrapping at all.
    let _ = |listener: Outside, router: Router| serve(listener, router);
}

/// `HandlerPanic` is part of the public surface and can be reached from a reported error.
///
/// The panic message is the only thing a caller can act on when a handler unwinds, and it
/// arrives through `Error::source`. This pins that both the type and that path stay public.
#[allow(dead_code)]
fn a_handler_panic_is_nameable_and_readable(error: &ngnet_axum::Error) -> Option<String> {
    let panic: &ngnet_axum::HandlerPanic = error.source()?.downcast_ref()?;
    Some(panic.message().to_owned())
}
