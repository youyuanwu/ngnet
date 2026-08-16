//! An asynchronous QMux connection over a byte stream the caller supplies.
//!
//! Enabled by the default `io` feature. Disabling it returns the crate to the sans-I/O state
//! machine described in [the crate documentation](crate), with exactly one dependency and no
//! asynchrony of any kind.
//!
//! Everything asynchronous is confined to this subtree: nothing outside `src/io/` names a
//! waker, a future or a clock, and a structural test enforces that. The subtree contains no
//! `unsafe` either -- it is declared in `lib.rs` without an `#[allow(unsafe_code)]`, so the
//! crate-level `#![deny(unsafe_code)]` rejects any, and a test pins that no file below here
//! grants itself the allowance that would silence it. Every foreign call lives beneath this
//! layer, in the state machine, which is unchanged by the layer's existence.
//!
//! # What this layer is for
//!
//! [`Conn`](crate::Conn) is complete and it is also awkward to use over a real transport. A
//! caller must read from the byte stream, feed what arrives to [`Conn::read`](crate::Conn::read)
//! with a timestamp, notice that a record was cut in half by the read boundary, serialise
//! outbound records into a buffer, write that buffer out across however many partial accepts
//! the transport chooses, and translate callbacks that receive no connection handle into
//! something the surrounding code can act on. Each of those has a way of being subtly wrong
//! that presents as a connection which stalls rather than as an error. This layer exists so
//! that loop is written once.
//!
//! # What it deliberately does not supply
//!
//! **No endpoint, no listener, no accept loop and no driver task.** The QUIC equivalent has
//! all four because its unit of ownership is a UDP socket shared by every connection on it:
//! something has to demultiplex arriving datagrams, and that something is a driver. A QMux
//! connection owns *one* byte stream and shares it with nothing, so there is no
//! demultiplexing to do and no reason for a task to sit between the caller and the
//! connection. The caller establishes the byte stream -- connecting, listening, accepting and
//! any TLS on it -- and hands it over already established.
//!
//! **No runtime.** This subtree names no executor and spawns nothing. The caller describes
//! their byte stream and their clock through [`AsyncByteStream`] and [`Clock`], and a
//! ready-made description for one widely used runtime ships behind the off-by-default `tokio`
//! feature. [`testing`] contains a second implementation that moves bytes in memory, which is
//! what makes "this is not shaped around one runtime" evidence rather than an assertion.
//!
//! **No timer.** [`Clock`] reports the time and offers no way to wait for one; see its module
//! documentation for why an idle timeout is not enforced here.
//!
//! # Two things it must do that the state machine cannot
//!
//! [`RecordFramer`] counts record boundaries in parallel with dwnx, because dwnx tolerates a
//! partial record by asking for more input and so cannot say whether a byte stream ended
//! between records or partway through one. [`encode_close_record`] and [`decode_close_frame`]
//! are the CONNECTION_CLOSE codec, because dwnx serialises no close at all and parses an
//! incoming one into a private struct it exposes no accessor for. Neither is duplicated work
//! chosen for its own sake; each module argues its case where a reader will find it.
//!
//! # Why poll-shaped, when the closer precedent is future-shaped
//!
//! `ngnet-h2` also runs a protocol over a byte stream, and its transport abstraction is
//! future-returning and split into a reader half and a writer half. That is the nearer
//! analogy by subject matter, and this layer follows `ngnet-quic`'s poll-shaped socket seam
//! instead. Two reasons, both about what the connection has to do rather than about taste.
//!
//! The first is composition. The HTTP/3 transport abstraction this work must eventually
//! satisfy, `ngnet_h3`'s `QuicConnection`, is itself poll-shaped: it hands the transport a
//! `Context` and expects an answer now. A future-shaped byte stream underneath a poll-shaped
//! transport needs an adapter that stores an in-flight future between calls, and that adapter
//! has to be cancellation-correct -- dropping a partially completed read would lose bytes off
//! a stream that cannot resend them.
//!
//! The second is that a connection must drain reads *and* produce writes in one wakeup. It
//! has to ask "are there bytes right now?", carry on if the answer is no, and go on to flush
//! whatever the state machine produced -- which is exactly what [`Poll`](core::task::Poll)
//! expresses and what awaiting a read does not. An awaited read parks the whole connection
//! until bytes arrive, and the records already queued for the peer sit unwritten behind it.
//! For a protocol whose peer may be waiting for precisely those records before it sends
//! anything, that is a deadlock rather than a latency cost.
//!
//! `ngnet-h2`'s abstraction is also unreachable from here regardless: the workspace's layering
//! forbids one protocol family depending on another.
//!
//! # No `Send` bound
//!
//! There is none on the byte stream or the clock, here or anywhere in this subtree.
//! Thread-per-core runtimes build their I/O on `Rc`, and requiring `Send` would exclude them
//! for the benefit of nobody. Auto traits propagate instead: a connection over a `Send` byte
//! stream is `Send` without anything saying so.
//!
//! The one place a bound does appear is [`AsyncByteStream::Error`], which must convert into a
//! sendable, shareable boxed error. That is a constraint on the *failure type*, not on the
//! stream, and it is there because the HTTP/3 transport abstraction requires it of any
//! transport plugged into it.
//!
//! Note the asymmetry with the state machine, which *does* require `Send` of its handlers.
//! That is not an inconsistency: [`Conn`](crate::Conn) is `Send`, so a non-`Send` handler
//! inside it would be unsound. Nothing here is `Send` by declaration, so nothing here needs
//! the bound.

mod clock;
mod close;
mod error;
mod framing;
mod stream;

#[doc(hidden)]
pub mod testing;

pub use clock::Clock;
pub use close::{decode_close_frame, encode_close_record};
pub use error::{Error, ErrorKind, Result};
pub use framing::RecordFramer;
pub use stream::{AsyncByteStream, Written};
