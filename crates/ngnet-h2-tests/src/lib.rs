//! Driving a sans-I/O [`ngnet-h2`] session over an asynchronous transport.
//!
//! The wrapper crate owns no transport and depends on no runtime, which is what makes it
//! usable from an async program at all — but it also means the adapter between session
//! and socket is the caller's to write. This crate is where that adapter is written
//! against [`tokio`] and tested over real sockets, so that `ngnet-h2` itself keeps its
//! single dependency.
//!
//! The adapter is three functions, and they are the same three the blocking version
//! needs; only the `.await` points differ. That is the property worth demonstrating: the
//! session has no opinion about who moves the bytes.
//!
//! # The rules the loop has to respect
//!
//! * A read yields an arbitrary slice of the byte stream — several frames, part of one,
//!   or frames belonging to several streams at once. [`feed`] hands it over in as many
//!   bites as the session asks for.
//! * Output only becomes available as a consequence of input, so every pass must flush
//!   before it awaits a read. Awaiting first is how a connection wedges.
//! * Handlers are never given the session, so a peer acts on what arrived *between*
//!   reads. That is the `step` hook of [`drive`].

use std::error::Error as StdError;

use ngnet_h2::Session;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Errors cross task boundaries when a spawned connection is joined, hence `Send + Sync`.
pub type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// How much is read at once. Any size is correct; this one just avoids excessive syscalls.
const READ_CHUNK: usize = 16 * 1024;

/// Writes everything the session currently has queued.
///
/// Each block must reach the writer before the next is asked for: libnghttp2 invalidates
/// the previous one, which is why the slice returned by [`Session::send`] borrows the
/// session and is released at the end of each iteration.
///
/// # Cancellation
///
/// Not cancel-safe. A block collected from the session has already left its queue, so
/// dropping this future part-way through a write loses those octets and desynchronises
/// the connection. Await it to completion, or abandon the connection.
pub async fn flush<C, W>(session: &mut Session<C>, ctx: &mut C, writer: &mut W) -> Fallible
where
    W: AsyncWrite + Unpin,
{
    while let Some(block) = session.send(ctx)? {
        writer.write_all(block).await?;
    }
    writer.flush().await?;
    Ok(())
}

/// Hands one read to the session, which may take it in several bites.
///
/// Synchronous on purpose: parsing touches no I/O, so there is nothing here to await.
pub fn feed<C>(session: &mut Session<C>, mut input: &[u8], ctx: &mut C) -> Fallible {
    while !input.is_empty() {
        let consumed = session.recv(input, ctx)?;
        if consumed == 0 {
            // The session has been terminated and wants no more input. The remainder is
            // not an error; it is simply no longer of interest.
            break;
        }
        input = &input[consumed..];
    }
    Ok(())
}

/// Runs one session over `socket` until it has nothing left to do.
///
/// `step` is called after each batch of received bytes, with the session available. It is
/// where a server submits the responses its handlers just recorded, or where a client
/// decides the exchange is over; returning `false` stops the loop after a final flush.
///
/// Returns when the session no longer wants to read, when the peer closes, or when `step`
/// asks to stop.
pub async fn drive<C, S, F>(
    session: &mut Session<C>,
    ctx: &mut C,
    socket: &mut S,
    mut step: F,
) -> Fallible
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(&mut Session<C>, &mut C) -> Fallible<bool>,
{
    let mut buf = vec![0u8; READ_CHUNK];

    loop {
        flush(session, ctx, socket).await?;

        if !session.want_read() {
            return Ok(());
        }

        let read = socket.read(&mut buf).await?;
        if read == 0 {
            // The peer closed. Anything still queued has nowhere to go.
            return Ok(());
        }

        feed(session, &buf[..read], ctx)?;

        if !step(session, ctx)? {
            return flush(session, ctx, socket).await;
        }
    }
}
