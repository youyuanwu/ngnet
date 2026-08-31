use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use bytes::Buf;
use ngnet_qmux::io::{AsyncByteStream, Clock};

use crate::error::Error;
use crate::state::Effects;
use crate::stream::{SendSlots, Shared, apply_effects};

/// The single caller-polled owner of lower-I/O liveness and close completion.
///
/// No task is spawned by this crate. The caller must poll this future concurrently with the
/// hyperium connection for as long as any connection or stream handle is in use. Hyperium's
/// synchronous `close` records intent only; this future is what flushes the QMux close and
/// shuts down the established byte stream.
#[must_use = "the QMux adapter makes no lower-I/O progress unless its driver is polled"]
pub struct Driver<S: AsyncByteStream, C: Clock, B: Buf> {
    pub(crate) shared: Shared<S, C>,
    pub(crate) slots: Arc<Mutex<SendSlots<B>>>,
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Driver<S, C, B> {
    pub(crate) fn new(shared: Shared<S, C>, slots: Arc<Mutex<SendSlots<B>>>) -> Self {
        Self { shared, slots }
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Future for Driver<S, C, B> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let displaced = this.shared.lower_wake.register_driver(cx.waker());
        if let Some(displaced) = displaced {
            displaced.wake();
        }
        let lower_ready = this.shared.lower_wake.take_ready();
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
                        let error = Error::new(error.to_string());
                        core.driver_error = Some(error.clone());
                        core.driver_complete = true;
                        Poll::Ready(Err(error))
                    }
                    Poll::Pending => Poll::Pending,
                }
            } else {
                effects = core.drive_turn(&this.shared.lower_wake);
                if lower_ready {
                    core.wake_all_senders(&mut effects);
                }
                if let Some(terminal) = core.terminal.clone() {
                    let error = Error::new(format!("{terminal:?}"));
                    core.driver_error = Some(error.clone());
                    core.driver_complete = true;
                    Poll::Ready(Err(error))
                } else {
                    Poll::Pending
                }
            }
        };
        this.shared.lower_wake.end_defer();
        apply_effects(&this.shared.lower_wake, &this.slots, effects);
        result
    }
}

impl<S: AsyncByteStream, C: Clock, B: Buf> Unpin for Driver<S, C, B> {}
