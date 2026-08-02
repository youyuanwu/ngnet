//! The two directions a message body travels.
//!
//! Both directions bridge the same two worlds — `http_body`'s pull-based asynchronous
//! [`Body`](http_body::Body) and the sans-I/O core's synchronous callbacks — but they
//! bridge it in opposite directions, and neither shares state with the other.
//!
//! [`outgoing`] presents a caller's body *to* the session, which asks for octets and is
//! told to wait when there are none. [`incoming`] presents the session's received octets
//! *to* the caller, and is the side flow control is driven from: the peer is credited when
//! the application takes a chunk, not when the chunk arrives.

pub(crate) mod incoming;
pub(crate) mod outgoing;

pub use incoming::IncomingBody;
