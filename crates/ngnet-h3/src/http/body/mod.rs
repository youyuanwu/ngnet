//! Message bodies for the asynchronous layer.
//!
//! The two directions are asymmetric and the asymmetry is nghttp3's, not this crate's.
//!
//! Received bodies are ordinary: bytes arrive, the caller reads them, and reading returns
//! flow-control credit to the peer.
//!
//! Bodies being *sent* are where the memory-safety-critical work is. nghttp3 has no copying
//! data source; it borrows the application's buffers and reads through them on every write
//! until the transport reports them released. That is why the outgoing adapter requires
//! `Bytes` chunks and hands them over with [`crate::RetainedBytes::from_owner`] rather than
//! copying, and why release is reported by exactly three things — acknowledgement, stream
//! close, and dropping the connection — and nothing else.

mod incoming;
mod outgoing;

pub use incoming::IncomingBody;
pub(crate) use outgoing::{Ending, Outgoing, ending_pending};
