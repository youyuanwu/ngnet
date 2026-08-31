//! Feature-gated, explicitly armed adapter and lower-I/O diagnostics.
//!
//! The module is absent from default builds. Feature-enabled builds remain inert until
//! [`arm`] is called; armed runs are diagnostic evidence and must not be mixed with default
//! timing runs.
//!
//! Counters are process-global. Every observed stream and adapter connection in the process
//! contributes to one interval, and `snapshot`, `drain`, or any [`LowerIoHandle`] observes
//! that aggregate. Run one isolated diagnostic workload per process; handles are not
//! per-connection accounting objects.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll};

use ngnet_qmux::io::{AsyncByteStream, Written};

/// A cumulative process-wide diagnostic snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Lower read polls.
    pub lower_read_calls: u64,
    /// Bytes returned by lower reads.
    pub lower_read_bytes: u64,
    /// Lower write polls.
    pub lower_write_calls: u64,
    /// Bytes accepted by lower writes.
    pub lower_write_bytes: u64,
    /// Lower writes returning `NotNow`.
    pub lower_write_not_now: u64,
    /// Lower shutdown polls.
    pub lower_shutdown_calls: u64,
    /// Lower operation failures.
    pub lower_failures: u64,
    /// Hyperium-facing adapter polls.
    pub adapter_polls: u64,
    /// Driver polls.
    pub driver_polls: u64,
    /// Bounded pump turns making no routing progress.
    pub no_progress_polls: u64,
    /// Bounded lower pump attempts.
    pub pump_attempts: u64,
    /// Pump turns routing at least one event.
    pub productive_turns: u64,
    /// Routed QMux events.
    pub routed_events: u64,
    /// Routed stream-scoped events.
    pub stream_events: u64,
    /// Routed connection-scoped events.
    pub connection_events: u64,
    /// Stream-window credit applications.
    pub stream_credit_applications: u64,
    /// Connection-window credit applications.
    pub connection_credit_applications: u64,
    /// Waiter registrations.
    pub waiter_registrations: u64,
    /// Current-waker replacements.
    pub waiter_replacements: u64,
    /// Deferred wake deliveries.
    pub wake_deliveries: u64,
    /// Logical framed-send chunks retained.
    pub logical_send_chunks: u64,
    /// Current retained framed-send bytes.
    pub retained_send_bytes: u64,
    /// High-water retained framed-send bytes.
    pub retained_send_high_water: u64,
    /// Current routed receive bytes.
    pub retained_receive_bytes: u64,
    /// High-water routed receive bytes.
    pub retained_receive_high_water: u64,
    /// Reclaimed stream entries.
    pub cleanups: u64,
    /// Connection-terminal fan-outs.
    pub terminal_fanouts: u64,
    /// True when any counter or gauge conversion saturated.
    pub overflowed: bool,
}

struct Counters {
    armed: AtomicBool,
    overflowed: AtomicBool,
    lower_read_calls: AtomicU64,
    lower_read_bytes: AtomicU64,
    lower_write_calls: AtomicU64,
    lower_write_bytes: AtomicU64,
    lower_write_not_now: AtomicU64,
    lower_shutdown_calls: AtomicU64,
    lower_failures: AtomicU64,
    adapter_polls: AtomicU64,
    driver_polls: AtomicU64,
    no_progress_polls: AtomicU64,
    pump_attempts: AtomicU64,
    productive_turns: AtomicU64,
    routed_events: AtomicU64,
    stream_events: AtomicU64,
    connection_events: AtomicU64,
    stream_credit_applications: AtomicU64,
    connection_credit_applications: AtomicU64,
    waiter_registrations: AtomicU64,
    waiter_replacements: AtomicU64,
    wake_deliveries: AtomicU64,
    logical_send_chunks: AtomicU64,
    retained_send_bytes: AtomicU64,
    retained_send_high_water: AtomicU64,
    retained_receive_bytes: AtomicU64,
    retained_receive_high_water: AtomicU64,
    cleanups: AtomicU64,
    terminal_fanouts: AtomicU64,
}

macro_rules! counters {
    ($($field:ident),+ $(,)?) => {
        Counters {
            armed: AtomicBool::new(false),
            overflowed: AtomicBool::new(false),
            $($field: AtomicU64::new(0),)+
        }
    };
}

static COUNTERS: Counters = counters!(
    lower_read_calls,
    lower_read_bytes,
    lower_write_calls,
    lower_write_bytes,
    lower_write_not_now,
    lower_shutdown_calls,
    lower_failures,
    adapter_polls,
    driver_polls,
    no_progress_polls,
    pump_attempts,
    productive_turns,
    routed_events,
    stream_events,
    connection_events,
    stream_credit_applications,
    connection_credit_applications,
    waiter_registrations,
    waiter_replacements,
    wake_deliveries,
    logical_send_chunks,
    retained_send_bytes,
    retained_send_high_water,
    retained_receive_bytes,
    retained_receive_high_water,
    cleanups,
    terminal_fanouts,
);
static EXCLUSIVE: Mutex<()> = Mutex::new(());
static TEST_SERIAL: Mutex<()> = Mutex::new(());

fn exclusive() -> MutexGuard<'static, ()> {
    EXCLUSIVE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Serializes process-global diagnostic tests.
#[doc(hidden)]
pub fn lock_for_test() -> MutexGuard<'static, ()> {
    TEST_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn armed() -> bool {
    COUNTERS.armed.load(Ordering::Relaxed)
}

fn add(counter: &AtomicU64, value: u64) {
    if !armed() || value == 0 {
        return;
    }
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let (next, overflow) = current.overflowing_add(value);
        let next = if overflow { u64::MAX } else { next };
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                if overflow {
                    COUNTERS.overflowed.store(true, Ordering::Relaxed);
                }
                return;
            }
            Err(actual) => current = actual,
        }
    }
}

fn gauge(current: &AtomicU64, high: &AtomicU64, value: usize) {
    if !armed() {
        return;
    }
    let value = u64::try_from(value).unwrap_or_else(|_| {
        COUNTERS.overflowed.store(true, Ordering::Relaxed);
        u64::MAX
    });
    current.store(value, Ordering::Relaxed);
    high.fetch_max(value, Ordering::Relaxed);
}

macro_rules! snapshot {
    ($load:ident) => {
        Snapshot {
            lower_read_calls: COUNTERS.lower_read_calls.$load(Ordering::Relaxed),
            lower_read_bytes: COUNTERS.lower_read_bytes.$load(Ordering::Relaxed),
            lower_write_calls: COUNTERS.lower_write_calls.$load(Ordering::Relaxed),
            lower_write_bytes: COUNTERS.lower_write_bytes.$load(Ordering::Relaxed),
            lower_write_not_now: COUNTERS.lower_write_not_now.$load(Ordering::Relaxed),
            lower_shutdown_calls: COUNTERS.lower_shutdown_calls.$load(Ordering::Relaxed),
            lower_failures: COUNTERS.lower_failures.$load(Ordering::Relaxed),
            adapter_polls: COUNTERS.adapter_polls.$load(Ordering::Relaxed),
            driver_polls: COUNTERS.driver_polls.$load(Ordering::Relaxed),
            no_progress_polls: COUNTERS.no_progress_polls.$load(Ordering::Relaxed),
            pump_attempts: COUNTERS.pump_attempts.$load(Ordering::Relaxed),
            productive_turns: COUNTERS.productive_turns.$load(Ordering::Relaxed),
            routed_events: COUNTERS.routed_events.$load(Ordering::Relaxed),
            stream_events: COUNTERS.stream_events.$load(Ordering::Relaxed),
            connection_events: COUNTERS.connection_events.$load(Ordering::Relaxed),
            stream_credit_applications: COUNTERS
                .stream_credit_applications
                .$load(Ordering::Relaxed),
            connection_credit_applications: COUNTERS
                .connection_credit_applications
                .$load(Ordering::Relaxed),
            waiter_registrations: COUNTERS.waiter_registrations.$load(Ordering::Relaxed),
            waiter_replacements: COUNTERS.waiter_replacements.$load(Ordering::Relaxed),
            wake_deliveries: COUNTERS.wake_deliveries.$load(Ordering::Relaxed),
            logical_send_chunks: COUNTERS.logical_send_chunks.$load(Ordering::Relaxed),
            retained_send_bytes: COUNTERS.retained_send_bytes.load(Ordering::Relaxed),
            retained_send_high_water: COUNTERS.retained_send_high_water.$load(Ordering::Relaxed),
            retained_receive_bytes: COUNTERS.retained_receive_bytes.load(Ordering::Relaxed),
            retained_receive_high_water: COUNTERS
                .retained_receive_high_water
                .$load(Ordering::Relaxed),
            cleanups: COUNTERS.cleanups.$load(Ordering::Relaxed),
            terminal_fanouts: COUNTERS.terminal_fanouts.$load(Ordering::Relaxed),
            overflowed: COUNTERS.overflowed.load(Ordering::Relaxed),
        }
    };
}

/// Enables or disables recording. Enabling does not reset existing counters.
pub fn arm(enabled: bool) {
    COUNTERS.armed.store(enabled, Ordering::Release);
}

/// Whether recording is currently armed.
#[must_use]
pub fn is_armed() -> bool {
    armed()
}

/// Returns a non-destructive snapshot.
#[must_use]
pub fn snapshot() -> Snapshot {
    let _guard = exclusive();
    snapshot!(load)
}

/// Drains interval counters while retaining current gauges.
pub fn drain() -> Snapshot {
    let _guard = exclusive();
    let mut snapshot = snapshot!(swap_zero);
    snapshot.overflowed = COUNTERS.overflowed.swap(false, Ordering::Relaxed);
    COUNTERS
        .retained_send_high_water
        .store(snapshot.retained_send_bytes, Ordering::Relaxed);
    COUNTERS
        .retained_receive_high_water
        .store(snapshot.retained_receive_bytes, Ordering::Relaxed);
    snapshot
}

/// Resets all counters and gauges and disables recording.
pub fn reset() {
    let _guard = exclusive();
    COUNTERS.armed.store(false, Ordering::Release);
    let _ = snapshot!(swap_zero);
    COUNTERS.overflowed.store(false, Ordering::Relaxed);
    COUNTERS.retained_send_bytes.store(0, Ordering::Relaxed);
    COUNTERS.retained_receive_bytes.store(0, Ordering::Relaxed);
}

/// Forces one saturation for overflow-semantics tests.
#[doc(hidden)]
pub fn force_overflow_for_test() {
    COUNTERS.adapter_polls.store(u64::MAX, Ordering::Relaxed);
    COUNTERS.armed.store(true, Ordering::Release);
    add(&COUNTERS.adapter_polls, 1);
}

trait SwapZero {
    fn swap_zero(&self, ordering: Ordering) -> u64;
}

impl SwapZero for AtomicU64 {
    fn swap_zero(&self, ordering: Ordering) -> u64 {
        self.swap(0, ordering)
    }
}

/// Handle returned with an [`ObservedStream`] for controlling the process-wide interval.
#[derive(Clone, Copy, Debug)]
pub struct LowerIoHandle {
    _private: (),
}

impl LowerIoHandle {
    /// Enables or disables the shared interval.
    pub fn arm(self, enabled: bool) {
        arm(enabled);
    }

    /// Returns the combined process-wide lower and adapter snapshot.
    #[must_use]
    pub fn snapshot(self) -> Snapshot {
        snapshot()
    }

    /// Drains the combined interval.
    pub fn drain(self) -> Snapshot {
        drain()
    }
}

/// Lower stream wrapper recording calls and exact returned byte counts.
pub struct ObservedStream<S> {
    inner: S,
}

/// Wraps a lower stream and returns its diagnostic handle.
pub fn observe<S>(stream: S) -> (ObservedStream<S>, LowerIoHandle) {
    (
        ObservedStream { inner: stream },
        LowerIoHandle { _private: () },
    )
}

impl<S: AsyncByteStream> AsyncByteStream for ObservedStream<S> {
    type Error = S::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        add(&COUNTERS.lower_read_calls, 1);
        let result = self.inner.poll_read(cx, buffer);
        match &result {
            Poll::Ready(Ok(bytes)) => add(&COUNTERS.lower_read_bytes, *bytes as u64),
            Poll::Ready(Err(_)) => add(&COUNTERS.lower_failures, 1),
            Poll::Pending => {}
        }
        result
    }

    fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<Written, Self::Error>> {
        add(&COUNTERS.lower_write_calls, 1);
        let result = self.inner.poll_write(cx, buffer);
        match &result {
            Poll::Ready(Ok(Written::Accepted(bytes))) => {
                add(&COUNTERS.lower_write_bytes, *bytes as u64);
            }
            Poll::Ready(Ok(Written::NotNow)) => add(&COUNTERS.lower_write_not_now, 1),
            Poll::Ready(Err(_)) => add(&COUNTERS.lower_failures, 1),
            Poll::Pending => {}
        }
        result
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        add(&COUNTERS.lower_shutdown_calls, 1);
        let result = self.inner.poll_shutdown(cx);
        if matches!(result, Poll::Ready(Err(_))) {
            add(&COUNTERS.lower_failures, 1);
        }
        result
    }
}

pub(crate) fn adapter_poll() {
    add(&COUNTERS.adapter_polls, 1);
}
pub(crate) fn driver_poll() {
    add(&COUNTERS.driver_polls, 1);
}
pub(crate) fn pump(productive: bool) {
    add(&COUNTERS.pump_attempts, 1);
    if productive {
        add(&COUNTERS.productive_turns, 1);
    } else {
        add(&COUNTERS.no_progress_polls, 1);
    }
}
pub(crate) fn route(stream_scoped: bool) {
    add(&COUNTERS.routed_events, 1);
    add(
        if stream_scoped {
            &COUNTERS.stream_events
        } else {
            &COUNTERS.connection_events
        },
        1,
    );
}
pub(crate) fn stream_credit() {
    add(&COUNTERS.stream_credit_applications, 1);
}
pub(crate) fn connection_credit() {
    add(&COUNTERS.connection_credit_applications, 1);
}
pub(crate) fn waiter(replaced: bool) {
    add(&COUNTERS.waiter_registrations, 1);
    if replaced {
        add(&COUNTERS.waiter_replacements, 1);
    }
}
pub(crate) fn wakes(count: usize) {
    add(&COUNTERS.wake_deliveries, count as u64);
}
pub(crate) fn send_chunk() {
    add(&COUNTERS.logical_send_chunks, 1);
}
pub(crate) fn send_gauge(bytes: usize) {
    gauge(
        &COUNTERS.retained_send_bytes,
        &COUNTERS.retained_send_high_water,
        bytes,
    );
}
pub(crate) fn receive_gauge(bytes: usize) {
    gauge(
        &COUNTERS.retained_receive_bytes,
        &COUNTERS.retained_receive_high_water,
        bytes,
    );
}
pub(crate) fn cleanup() {
    add(&COUNTERS.cleanups, 1);
}
pub(crate) fn terminal() {
    add(&COUNTERS.terminal_fanouts, 1);
}
