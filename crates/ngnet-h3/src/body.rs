//! Outgoing message bodies.
//!
//! The trait and outcome type live here from the phase that defines the submission
//! signatures, so those signatures are final before the machinery that drives them exists.
//! Submitting a body is currently refused with a typed error rather than silently
//! ignored — see [`Conn::submit_request`].
//!
//! # Why there is no copying variant
//!
//! nghttp2 offers a body source that writes into a buffer the library owns. nghttp3 has no
//! such thing: its data callback takes vectors that point at the *application's* memory,
//! and the contract is that the application keeps those bytes alive until nghttp3 reports
//! them acknowledged. Zero-copy is therefore not an optimisation to opt into here, it is
//! the only shape available, and it is why [`RetainedBytes`] exists — a source can hand
//! out the same allocation across several calls, so the buffers must be shareable rather
//! than owned by one vector each.
//!
//! [`Conn::submit_request`]: crate::Conn::submit_request

use std::sync::Arc;

/// A reference-counted byte buffer handed to the connection.
///
/// Shareable because one allocation can span several of the vectors nghttp3 asks for, and
/// across several calls: the buffer is released when its last byte is acknowledged, which
/// may be long after the call that offered it.
///
/// This is deliberately not [`bytes::Bytes`]. That would be the obvious choice, but this
/// crate declares exactly one non-optional dependency and a test enforces it, so the
/// handle is defined here instead.
///
/// [`bytes::Bytes`]: https://docs.rs/bytes
#[derive(Clone, Debug)]
pub struct RetainedBytes {
    buffer: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl RetainedBytes {
    /// Takes ownership of a buffer.
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        let buffer: Arc<[u8]> = bytes.into();
        let end = buffer.len();
        Self {
            buffer,
            start: 0,
            end,
        }
    }

    /// The bytes this handle refers to.
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }

    /// How many bytes this handle refers to.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether this handle refers to no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Splits off the first `n` bytes, leaving the remainder here.
    ///
    /// Both halves share one allocation, which is the point: a source can offer a large
    /// buffer in pieces without copying it or losing track of when it may be released.
    pub fn split_to(&mut self, n: usize) -> Self {
        let n = n.min(self.len());
        let head = Self {
            buffer: Arc::clone(&self.buffer),
            start: self.start,
            end: self.start + n,
        };
        self.start += n;
        head
    }
}

impl From<Vec<u8>> for RetainedBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&[u8]> for RetainedBytes {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }
}

/// What a body source has for the connection.
#[derive(Clone, Debug)]
pub enum BodyOutcome {
    /// Here are some bytes; there may be more later.
    Wrote(Vec<RetainedBytes>),
    /// Here are the last bytes, and the stream ends after them.
    Eof(Vec<RetainedBytes>),
    /// Here are the last bytes, but a trailing field section follows.
    EofWithTrailers(Vec<RetainedBytes>),
    /// Nothing is available yet.
    ///
    /// The stream is deferred until [`Conn::resume_stream`] is called. Returning no bytes
    /// without deferring is not expressible, because nghttp3 would take it as a
    /// zero-length frame rather than as "ask me again".
    ///
    /// [`Conn::resume_stream`]: crate::Conn::resume_stream
    Defer,
}

/// Supplies the bytes of an outgoing message body.
///
/// `Send` because a connection is, and a source is stored inside one.
pub trait BodySource: Send {
    /// Asks for the next piece of the body.
    ///
    /// May return at most eight buffers per call: that is the size of the array nghttp3
    /// offers, and anything beyond it would be silently dropped.
    fn next(&mut self) -> BodyOutcome;
}

/// A body that is already entirely in memory.
#[derive(Debug)]
pub struct FixedBody {
    remaining: Option<RetainedBytes>,
}

impl FixedBody {
    /// Wraps a complete body.
    pub fn new(bytes: impl Into<RetainedBytes>) -> Self {
        Self {
            remaining: Some(bytes.into()),
        }
    }
}

impl BodySource for FixedBody {
    fn next(&mut self) -> BodyOutcome {
        match self.remaining.take() {
            Some(bytes) if !bytes.is_empty() => BodyOutcome::Eof(vec![bytes]),
            _ => BodyOutcome::Eof(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitting_shares_one_allocation() {
        let mut bytes = RetainedBytes::new(b"hello world".to_vec());
        let head = bytes.split_to(5);
        assert_eq!(head.as_slice(), b"hello");
        assert_eq!(bytes.as_slice(), b" world");
        // Both halves refer into the same buffer, which is what lets release accounting
        // hold one handle per offered vector without duplicating the payload.
        assert!(Arc::ptr_eq(&head.buffer, &bytes.buffer));
    }

    #[test]
    fn splitting_past_the_end_is_clamped() {
        let mut bytes = RetainedBytes::new(b"abc".to_vec());
        let head = bytes.split_to(99);
        assert_eq!(head.as_slice(), b"abc");
        assert!(bytes.is_empty());
    }

    #[test]
    fn a_fixed_body_yields_everything_then_ends() {
        let mut body = FixedBody::new(b"payload".to_vec());
        match body.next() {
            BodyOutcome::Eof(pieces) => {
                assert_eq!(pieces.len(), 1);
                assert_eq!(pieces[0].as_slice(), b"payload");
            }
            other => panic!("expected Eof, got {other:?}"),
        }
        // A second call yields an empty end rather than repeating the payload.
        match body.next() {
            BodyOutcome::Eof(pieces) => assert!(pieces.is_empty()),
            other => panic!("expected empty Eof, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_fixed_body_ends_immediately() {
        let mut body = FixedBody::new(Vec::new());
        match body.next() {
            BodyOutcome::Eof(pieces) => assert!(pieces.is_empty()),
            other => panic!("expected empty Eof, got {other:?}"),
        }
    }
}
