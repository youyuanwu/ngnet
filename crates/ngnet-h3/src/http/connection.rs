//! The future that moves bytes.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

use super::error::Result;

/// The driver for one HTTP/3 connection.
///
/// Nothing happens until this is polled. Where it is polled is entirely the caller's
/// business — spawn it, join it, or poll it alongside whatever else there is; this crate
/// takes no executor, spawner or timer.
///
#[doc = include_str!("doc_driver_guarantee.md")]
#[must_use = "a connection does nothing until its driver is polled: requests submitted to \
              its handle will queue and never be sent"]
pub struct Connection<F> {
    driving: Pin<Box<F>>,
}

impl<F: Future<Output = Result<()>>> Connection<F> {
    pub(crate) fn new(driving: F) -> Self {
        Self {
            driving: Box::pin(driving),
        }
    }
}

impl<F: Future<Output = Result<()>>> Future for Connection<F> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.driving.as_mut().poll(context)
    }
}
