use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, PoisonError};
use std::task::{Context, Poll, Waker};

use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::error::Error;
use crate::state::Effects;
use crate::stream::{Shared, apply_effects};

/// The single caller-polled owner of lower-I/O liveness and close completion.
///
/// No task is spawned by this crate. The caller must poll this future concurrently with the
/// hyperium connection for as long as any connection or stream handle is in use. Hyperium's
/// synchronous `close` records intent only; this future is what flushes the QMux close and
/// shuts down the established byte stream.
///
/// A locally requested close resolves successfully once delivered. A peer application close,
/// adapter invariant failure, or underlying QMux/byte-stream failure resolves with a stable
/// [`crate::Error`] classification.
#[must_use = "the QMux adapter makes no lower-I/O progress unless its driver is polled"]
pub struct Driver<S: AsyncByteStream, C: Clock> {
    pub(crate) shared: Shared<S, C>,
}

impl<S: AsyncByteStream, C: Clock> Driver<S, C> {
    pub(crate) fn new(shared: Shared<S, C>) -> Self {
        Self { shared }
    }
}

impl<S: AsyncByteStream, C: Clock> Future for Driver<S, C> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(feature = "diagnostics")]
        crate::diagnostics::driver_poll();
        let this = self.get_mut();
        let displaced = this.shared.lower_wake.register_driver(cx.waker());
        if let Some(displaced) = displaced {
            displaced.wake();
        }
        let _ = this.shared.lower_wake.take_ready();
        let mut effects = Effects::default();

        this.shared.lower_wake.begin_defer();
        let result = {
            let mut core = this
                .shared
                .core
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if core.driver_complete {
                match &core.driver_error {
                    Some(error) => Poll::Ready(Err(error.clone())),
                    None => Poll::Ready(Ok(())),
                }
            } else if let Some(reason) = core.close.clone() {
                let lower_waker = Waker::from(Arc::clone(&this.shared.lower_wake));
                let mut lower_cx = Context::from_waker(&lower_waker);
                match core.lower.poll_close(&mut lower_cx, &reason) {
                    Poll::Ready(Ok(())) => {
                        core.driver_complete = true;
                        Poll::Ready(Ok(()))
                    }
                    Poll::Ready(Err(error)) => {
                        let error = Error::undefined(error.to_string());
                        core.driver_error = Some(error.clone());
                        core.driver_complete = true;
                        Poll::Ready(Err(error))
                    }
                    Poll::Pending => Poll::Pending,
                }
            } else {
                effects = core.drive_turn(&this.shared.lower_wake);
                if let Some(terminal) = core.terminal.clone() {
                    let error = terminal.driver_error();
                    core.driver_error = Some(error.clone());
                    core.driver_complete = true;
                    Poll::Ready(Err(error))
                } else if effects.continuation {
                    // The routing budget was exhausted while decoded lower events remain.
                    // `apply_effects` schedules the next turn, so this is an internal
                    // self-woken boundary rather than a suspension. Do not ask the forced
                    // pump for its terminal yet: QMux deliberately reports a latched ending
                    // there even while pre-ending events remain queued.
                    Poll::Pending
                } else {
                    let lower_waker = Waker::from(Arc::clone(&this.shared.lower_wake));
                    let mut lower_cx = Context::from_waker(&lower_waker);
                    match core.lower.poll_pump(&mut lower_cx) {
                        Poll::Ready(Err(error)) => {
                            let terminal = crate::error::ConnectionTerminal::from_lower(&error);
                            effects.merge(core.fail(terminal.clone()));
                            let error = terminal.driver_error();
                            core.driver_error = Some(error.clone());
                            core.driver_complete = true;
                            Poll::Ready(Err(error))
                        }
                        Poll::Ready(Ok(())) | Poll::Pending => Poll::Pending,
                    }
                }
            }
        };
        this.shared.lower_wake.end_defer();
        apply_effects(&this.shared.lower_wake, effects);
        result
    }
}

impl<S: AsyncByteStream, C: Clock> Unpin for Driver<S, C> {}
