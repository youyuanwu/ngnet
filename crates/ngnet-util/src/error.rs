//! What this client reports when a request does not produce a response.
//!
//! A pooling client fails in categories a caller acts on *differently*, and collapsing them
//! into one error is how a caller ends up retrying a request the server already executed.
//! Four categories are enough, and the interesting part is not the four — it is where the
//! lines fall between them, because two of the lines are not where they first appear to be.
//!
//! The first is that [`ErrorKind::Connect`] stops short of the HTTP/2 handshake. `ngnet-h2`
//! establishes a connection *synchronously*: [`handshake_shared_with`] fails only if the
//! local session cannot be constructed, and the settings exchange happens afterwards, on the
//! driver task. So a peer that accepts a TCP connection and then says nothing, or says
//! something that is not HTTP/2, is not distinguishable from a connection that worked and
//! then broke — by the time the failure is observable the request has already been handed
//! over. Calling that a connect failure would be comfortable and wrong, and dangerous
//! specifically because `Connect` is the one category whose [`Error::is_retriable`] is
//! unconditionally true.
//!
//! The second is that [`ErrorKind::Closed`] absorbs a case that looks like a protocol
//! failure. `ngnet-h2` reports "the peer never began this exchange" both when a `GOAWAY`
//! refuses a stream *and* when this end has asked its own handle to shut down. The
//! classification is correct either way — nothing was begun — but only the first is evidence
//! about the peer. This crate knows which of the two it caused, so a refusal observed while
//! shutting down is reported as `Closed`, and is not retriable: repeating it against a client
//! that is closing would fail the same way for ever.
//!
//! There is deliberately no *queue* category, though a pool might be expected to have one.
//! Waiting for a dial is unbounded by design, so it cannot overflow, and the only way a wait
//! ends other than with that dial's outcome is that the client was shut down underneath it —
//! which is `Closed`. A category no code path can construct would be a worse answer than
//! saying why there isn't one.
//!
//! [`handshake_shared_with`]: ngnet_h2::http::client::handshake_shared_with

use std::error::Error as StdError;
use std::fmt;

/// The category of a request failure.
///
/// Marked non-exhaustive: adding a category later must be additive, so matching on this has
/// to carry a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The request URI could not be used: no scheme, a scheme this client does not serve, or
    /// no host. Nothing was resolved and nothing was dialled.
    ///
    /// Never retriable — the same request would fail identically, for ever.
    Uri,
    /// Reaching the origin failed: name resolution, the TCP connection, or construction of
    /// the local protocol session. No octet of the request reached any peer.
    ///
    /// Always retriable. Nothing was begun anywhere, so repeating the request cannot
    /// duplicate an effect, and the origin may be reachable a moment later.
    Connect,
    /// This client has been shut down, or was shutting down when the request was refused.
    ///
    /// Never retriable *against this client* — it will not serve the request however many
    /// times it is offered. A caller with a new client is free to try again.
    Closed,
    /// A connection existed and the exchange on it failed.
    ///
    /// Retriable only when `ngnet-h2` states the peer never began the exchange. Otherwise
    /// the request may have been delivered and acted upon before the failure, and repeating
    /// it could duplicate a side effect the caller cannot see.
    Exchange,
}

impl ErrorKind {
    const fn describe(self) -> &'static str {
        match self {
            Self::Uri => "uri",
            Self::Connect => "connect",
            Self::Closed => "closed",
            Self::Exchange => "exchange",
        }
    }
}

/// A request failure, with the category a caller acts on and the cause it came from.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    retriable: bool,
    source: Box<dyn StdError + Send + Sync>,
}

impl Error {
    /// The category of the failure.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether repeating the request is safe *as far as this client can tell*.
    ///
    /// This is a claim about delivery, not about the request's semantics. It is true only
    /// when nothing of the request reached a peer, so repeating it cannot duplicate an
    /// effect. It says nothing about whether the *method* is idempotent — a caller repeating
    /// a `POST` on the strength of this is relying on the delivery claim, which is sound, and
    /// is responsible for the rest.
    ///
    /// The value is fixed when the error is built rather than derived from the kind on each
    /// call, because [`ErrorKind::Exchange`] is conditional: whether a failed exchange may be
    /// repeated depends on what `ngnet-h2` was willing to say about that particular exchange,
    /// and that information is available exactly once, at the point of failure.
    pub fn is_retriable(&self) -> bool {
        self.retriable
    }

    /// Re-reports a shared failure to another caller that waited on the same dial.
    ///
    /// One dial failure is the answer for arbitrarily many waiters, and [`Error`] is not
    /// `Clone` — its cause is a boxed error that may not be. So the shared original becomes
    /// the *cause* of each caller's error, which keeps the chain intact and reads correctly:
    /// this request failed because that dial failed.
    pub(crate) fn from_shared(shared: &std::sync::Arc<Self>) -> Self {
        Self {
            kind: shared.kind,
            retriable: shared.retriable,
            source: Box::new(SharedCause(std::sync::Arc::clone(shared))),
        }
    }

    /// A URI that cannot be used. Never retriable.
    pub(crate) fn uri(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self {
            kind: ErrorKind::Uri,
            retriable: false,
            source: source.into(),
        }
    }

    /// A failure reaching the origin. Always retriable — see [`ErrorKind::Connect`].
    pub(crate) fn connect(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self {
            kind: ErrorKind::Connect,
            retriable: true,
            source: source.into(),
        }
    }

    /// A request offered to, or refused by, a client that is shutting down. Never retriable.
    pub(crate) fn closed(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self {
            kind: ErrorKind::Closed,
            retriable: false,
            source: source.into(),
        }
    }

    /// A failed exchange on a connection that existed.
    ///
    /// `retriable` is supplied by the caller because only the code holding the `ngnet-h2`
    /// error knows whether that error says the peer never began the exchange.
    pub(crate) fn exchange(
        retriable: bool,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self {
            kind: ErrorKind::Exchange,
            retriable,
            source: source.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} error: {}", self.kind.describe(), self.source)
    }
}

impl StdError for Error {
    /// The cause, retained rather than flattened into the message.
    ///
    /// A caller that needs to know *which* I/O error refused the connection can downcast
    /// through here; one that only wants to log it gets the whole chain from `Display`.
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

/// A marker cause for failures this crate originates itself, so that every [`Error`] has a
/// source and the chain never ends in a surprise.
#[derive(Debug)]
pub(crate) struct Reason(pub(crate) &'static str);

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl StdError for Reason {}

/// Adds context to a cause without consuming it.
///
/// The obvious way to say where a failure happened is to format it into the message —
/// `format!("connecting to {origin} failed: {source}")` — and it reads correctly, which is
/// what makes it a trap. It turns the cause into text. [`Error::source`] promises a caller
/// can downcast to find *which* [`std::io::Error`] refused a connection, and a stringified
/// cause has no type left to downcast to; the promise silently becomes false while every
/// log message continues to look exactly right.
///
/// So the context goes in front of the cause rather than around it, and `Display` renders
/// both. The message a caller logs is unchanged, and the `io::Error` is still there to be
/// found.
#[derive(Debug)]
pub(crate) struct Context<E> {
    context: String,
    source: E,
}

impl<E> Context<E>
where
    E: StdError + Send + Sync + 'static,
{
    pub(crate) fn new(context: String, source: E) -> Self {
        Self { context, source }
    }
}

impl<E: fmt::Display> fmt::Display for Context<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.context, self.source)
    }
}

impl<E: StdError + 'static> StdError for Context<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

/// Wraps a shared failure so it can be the `source` of several errors at once.
#[derive(Debug)]
struct SharedCause(std::sync::Arc<Error>);

impl fmt::Display for SharedCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl StdError for SharedCause {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.source()
    }
}
