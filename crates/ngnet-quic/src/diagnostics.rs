//! Feature-gated diagnostics for deterministic QUIC stress runs.
//!
//! The module exists only with the non-default `diagnostics` feature. Even then it is
//! inert until [`arm`] is called: hot-path hooks perform one atomic arming check and return,
//! without allocating or changing protocol decisions. Armed runs are diagnostic processes,
//! separate from unarmed timing runs.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard};

use crate::Role;

/// One stream-write outcome recorded after ngtcp2 returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// A datagram was produced.
    Datagram,
    /// Stream flow control prevented acceptance.
    StreamBlocked,
    /// Connection flow control prevented acceptance.
    ConnectionBlocked,
    /// Congestion, pacing, or datagram capacity prevented acceptance.
    Blocked,
    /// The connection had nothing to write.
    Idle,
}

/// One complete stream-write attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attempt {
    /// Monotonic process-local diagnostic sequence.
    pub sequence: u64,
    /// Monotonic process-local connection identity.
    pub connection_id: u64,
    /// Endpoint role making the offer.
    pub role: Role,
    /// Direction relative to `role`; currently always `"outbound"`.
    pub direction: &'static str,
    /// QUIC stream identifier.
    pub stream_id: i64,
    /// Logical stream offset of the offered prefix.
    pub stream_offset: u64,
    /// Bytes supplied by the caller.
    pub offered_bytes: u64,
    /// Path payload limit sampled immediately before staging.
    pub sampled_payload_limit: u64,
    /// Complete backing capacity prepared for this attempt.
    pub prepared_backing_capacity: u64,
    /// Prefix accepted by ngtcp2.
    pub accepted_prefix: u64,
    /// Whether FIN was handed to ngtcp2 with this staged prefix.
    pub fin_offered: bool,
    /// Whether a non-empty offer accepted no bytes.
    pub zero_acceptance: bool,
    /// Logical bytes retained after this attempt.
    pub logical_retained_bytes: u64,
    /// Complete backing capacity retained after this attempt.
    pub retained_backing_capacity: u64,
    /// Outcome category.
    pub outcome: AttemptOutcome,
}

/// Category of a liveness observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LivenessKind {
    /// Something occurred that can make a parked write retry meaningful.
    EnablingEvent,
    /// The local adapter produced output; this does not make a retry sendable.
    LocalProduction,
    /// A task was polled again; the wake alone does not prove external progress.
    DriverWake,
    /// A path stopped retrying until another event.
    Park,
    /// A previously parked path attempted progress again.
    Retry,
}

/// A reasoned, sequenced liveness observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LivenessEvent {
    /// Monotonic process-local diagnostic sequence.
    pub sequence: u64,
    /// Monotonic process-local connection identity.
    pub connection_id: u64,
    /// Endpoint role observing the event.
    pub role: Role,
    /// Whether this is an enabling event, park, or retry.
    pub kind: LivenessKind,
    /// Stable machine-readable reason.
    pub reason: &'static str,
    /// Stream-write attempt that parked or retried, when applicable.
    pub attempt_sequence: Option<u64>,
    /// Earlier zero-accept attempt for the same role, stream, and offset.
    pub parked_attempt_sequence: Option<u64>,
    /// Enabling event preceding a retry, or `None` when no such event was observed.
    pub enabling_sequence: Option<u64>,
}

/// Aggregate observations for one endpoint role.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RoleSnapshot {
    /// QUIC stream bytes offered across writes, including HTTP/3 framing and control data.
    pub offered_bytes: u64,
    /// Complete backing capacity prepared across attempts.
    pub prepared_backing_capacity: u64,
    /// QUIC stream bytes accepted across writes, including HTTP/3 framing and control data.
    pub accepted_bytes: u64,
    /// Non-empty attempts accepting no bytes.
    pub zero_acceptances: u64,
    /// Current logical retained-byte observation.
    pub logical_retained_bytes: u64,
    /// Highest logical retained-byte observation.
    pub logical_retained_high_water: u64,
    /// Current complete retained backing capacity.
    pub retained_backing_capacity: u64,
    /// Highest complete retained backing capacity.
    pub retained_backing_high_water: u64,
    /// QUIC stream bytes released back to HTTP/3 after copying.
    pub release_event_bytes: u64,
    /// Bytes acknowledged by the peer.
    pub acknowledged_bytes: u64,
    /// Complete retained backing capacity freed by acknowledgement or close.
    pub released_backing_capacity: u64,
    /// Packets produced without newly accepted stream data.
    pub transport_only_packets: u64,
    /// Packets produced while accepting stream data.
    pub stream_carrying_packets: u64,
    /// All packets observed at the adapter production boundary.
    pub produced_packets: u64,
    /// Timer deadline changes that created a replacement sleep.
    pub timer_rearms: u64,
    /// Armed timers observed ready.
    pub timer_fires: u64,
    /// General inbound/work wake registrations.
    pub wake_registrations: u64,
    /// Inbound deliveries that consumed general registrations.
    pub inbound_wakes: u64,
    /// Full-queue capacity registrations.
    pub capacity_registrations: u64,
    /// Full-to-available queue transitions that woke a producer.
    pub capacity_wakes: u64,
    /// Retries following an enabling event.
    pub retries: u64,
    /// Poll paths that parked for lack of an enabling event.
    pub parks: u64,
    /// Retries of zero-accept stream attempts.
    pub zero_accept_retries: u64,
    /// Zero-accept retries with no later enabling event recorded first.
    pub zero_accept_retries_without_enable: u64,
    /// Current inbound queue depth.
    pub inbound_queue_depth: u64,
    /// Highest inbound queue depth.
    pub inbound_queue_high_water: u64,
    /// Inbound datagrams dropped at the queue bound.
    pub inbound_drops: u64,
    /// Current outbound queue depth.
    pub outbound_queue_depth: u64,
    /// Highest outbound queue depth.
    pub outbound_queue_high_water: u64,
    /// Full-to-available outbound queue transitions.
    pub outbound_capacity_transitions: u64,
    /// Inbound datagrams deliberately discarded when an owner marked a connection terminal.
    pub terminal_discarded_inbound: u64,
    /// Outbound datagrams deliberately discarded at terminal transition.
    ///
    /// The detached endpoint preserves queued output, including CONNECTION_CLOSE, so this
    /// is expected to remain zero.
    pub terminal_discarded_outbound: u64,
}

/// A process-wide snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    /// Client-role observations.
    pub client: RoleSnapshot,
    /// Server-role observations.
    pub server: RoleSnapshot,
    /// True if any checked counter saturated at `u64::MAX`.
    pub overflowed: bool,
    /// Packet retransmission attribution is not exposed by the current safe ngtcp2 API.
    pub retransmissions_available: bool,
}

/// One exclusive interval observation.
///
/// Cumulative counters, attempts, liveness events, and overflow state are drained at one
/// instant while recorders are excluded. Live gauges remain at their current values, and
/// each high-water mark is re-seeded from its corresponding live gauge for the next
/// interval. Cross-interval zero-accept wait/enabling state is preserved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainedDiagnostics {
    /// Counter and live-gauge values for the completed interval.
    pub snapshot: Snapshot,
    /// Stream-write attempts recorded during the interval.
    pub attempts: Vec<Attempt>,
    /// Liveness events recorded during the interval.
    pub liveness: Vec<LivenessEvent>,
}

struct AtomicRole {
    offered_bytes: AtomicU64,
    prepared_backing_capacity: AtomicU64,
    accepted_bytes: AtomicU64,
    zero_acceptances: AtomicU64,
    logical_retained_bytes: AtomicU64,
    logical_retained_high_water: AtomicU64,
    retained_backing_capacity: AtomicU64,
    retained_backing_high_water: AtomicU64,
    release_event_bytes: AtomicU64,
    acknowledged_bytes: AtomicU64,
    released_backing_capacity: AtomicU64,
    transport_only_packets: AtomicU64,
    stream_carrying_packets: AtomicU64,
    produced_packets: AtomicU64,
    timer_rearms: AtomicU64,
    timer_fires: AtomicU64,
    wake_registrations: AtomicU64,
    inbound_wakes: AtomicU64,
    capacity_registrations: AtomicU64,
    capacity_wakes: AtomicU64,
    retries: AtomicU64,
    parks: AtomicU64,
    zero_accept_retries: AtomicU64,
    zero_accept_retries_without_enable: AtomicU64,
    inbound_queue_depth: AtomicU64,
    inbound_queue_high_water: AtomicU64,
    inbound_drops: AtomicU64,
    outbound_queue_depth: AtomicU64,
    outbound_queue_high_water: AtomicU64,
    outbound_capacity_transitions: AtomicU64,
    terminal_discarded_inbound: AtomicU64,
    terminal_discarded_outbound: AtomicU64,
}

impl AtomicRole {
    const fn new() -> Self {
        Self {
            offered_bytes: AtomicU64::new(0),
            prepared_backing_capacity: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            zero_acceptances: AtomicU64::new(0),
            logical_retained_bytes: AtomicU64::new(0),
            logical_retained_high_water: AtomicU64::new(0),
            retained_backing_capacity: AtomicU64::new(0),
            retained_backing_high_water: AtomicU64::new(0),
            release_event_bytes: AtomicU64::new(0),
            acknowledged_bytes: AtomicU64::new(0),
            released_backing_capacity: AtomicU64::new(0),
            transport_only_packets: AtomicU64::new(0),
            stream_carrying_packets: AtomicU64::new(0),
            produced_packets: AtomicU64::new(0),
            timer_rearms: AtomicU64::new(0),
            timer_fires: AtomicU64::new(0),
            wake_registrations: AtomicU64::new(0),
            inbound_wakes: AtomicU64::new(0),
            capacity_registrations: AtomicU64::new(0),
            capacity_wakes: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            parks: AtomicU64::new(0),
            zero_accept_retries: AtomicU64::new(0),
            zero_accept_retries_without_enable: AtomicU64::new(0),
            inbound_queue_depth: AtomicU64::new(0),
            inbound_queue_high_water: AtomicU64::new(0),
            inbound_drops: AtomicU64::new(0),
            outbound_queue_depth: AtomicU64::new(0),
            outbound_queue_high_water: AtomicU64::new(0),
            outbound_capacity_transitions: AtomicU64::new(0),
            terminal_discarded_inbound: AtomicU64::new(0),
            terminal_discarded_outbound: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        for counter in [
            &self.offered_bytes,
            &self.prepared_backing_capacity,
            &self.accepted_bytes,
            &self.zero_acceptances,
            &self.logical_retained_bytes,
            &self.logical_retained_high_water,
            &self.retained_backing_capacity,
            &self.retained_backing_high_water,
            &self.release_event_bytes,
            &self.acknowledged_bytes,
            &self.released_backing_capacity,
            &self.transport_only_packets,
            &self.stream_carrying_packets,
            &self.produced_packets,
            &self.timer_rearms,
            &self.timer_fires,
            &self.wake_registrations,
            &self.inbound_wakes,
            &self.capacity_registrations,
            &self.capacity_wakes,
            &self.retries,
            &self.parks,
            &self.zero_accept_retries,
            &self.zero_accept_retries_without_enable,
            &self.inbound_queue_depth,
            &self.inbound_queue_high_water,
            &self.inbound_drops,
            &self.outbound_queue_depth,
            &self.outbound_queue_high_water,
            &self.outbound_capacity_transitions,
            &self.terminal_discarded_inbound,
            &self.terminal_discarded_outbound,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> RoleSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        RoleSnapshot {
            offered_bytes: load(&self.offered_bytes),
            prepared_backing_capacity: load(&self.prepared_backing_capacity),
            accepted_bytes: load(&self.accepted_bytes),
            zero_acceptances: load(&self.zero_acceptances),
            logical_retained_bytes: load(&self.logical_retained_bytes),
            logical_retained_high_water: load(&self.logical_retained_high_water),
            retained_backing_capacity: load(&self.retained_backing_capacity),
            retained_backing_high_water: load(&self.retained_backing_high_water),
            release_event_bytes: load(&self.release_event_bytes),
            acknowledged_bytes: load(&self.acknowledged_bytes),
            released_backing_capacity: load(&self.released_backing_capacity),
            transport_only_packets: load(&self.transport_only_packets),
            stream_carrying_packets: load(&self.stream_carrying_packets),
            produced_packets: load(&self.produced_packets),
            timer_rearms: load(&self.timer_rearms),
            timer_fires: load(&self.timer_fires),
            wake_registrations: load(&self.wake_registrations),
            inbound_wakes: load(&self.inbound_wakes),
            capacity_registrations: load(&self.capacity_registrations),
            capacity_wakes: load(&self.capacity_wakes),
            retries: load(&self.retries),
            parks: load(&self.parks),
            zero_accept_retries: load(&self.zero_accept_retries),
            zero_accept_retries_without_enable: load(&self.zero_accept_retries_without_enable),
            inbound_queue_depth: load(&self.inbound_queue_depth),
            inbound_queue_high_water: load(&self.inbound_queue_high_water),
            inbound_drops: load(&self.inbound_drops),
            outbound_queue_depth: load(&self.outbound_queue_depth),
            outbound_queue_high_water: load(&self.outbound_queue_high_water),
            outbound_capacity_transitions: load(&self.outbound_capacity_transitions),
            terminal_discarded_inbound: load(&self.terminal_discarded_inbound),
            terminal_discarded_outbound: load(&self.terminal_discarded_outbound),
        }
    }

    fn drain_interval(&self) -> RoleSnapshot {
        let take = |counter: &AtomicU64| counter.swap(0, Ordering::Relaxed);
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);

        let logical_retained_bytes = load(&self.logical_retained_bytes);
        let retained_backing_capacity = load(&self.retained_backing_capacity);
        let inbound_queue_depth = load(&self.inbound_queue_depth);
        let outbound_queue_depth = load(&self.outbound_queue_depth);

        RoleSnapshot {
            offered_bytes: take(&self.offered_bytes),
            prepared_backing_capacity: take(&self.prepared_backing_capacity),
            accepted_bytes: take(&self.accepted_bytes),
            zero_acceptances: take(&self.zero_acceptances),
            logical_retained_bytes,
            logical_retained_high_water: self
                .logical_retained_high_water
                .swap(logical_retained_bytes, Ordering::Relaxed),
            retained_backing_capacity,
            retained_backing_high_water: self
                .retained_backing_high_water
                .swap(retained_backing_capacity, Ordering::Relaxed),
            release_event_bytes: take(&self.release_event_bytes),
            acknowledged_bytes: take(&self.acknowledged_bytes),
            released_backing_capacity: take(&self.released_backing_capacity),
            transport_only_packets: take(&self.transport_only_packets),
            stream_carrying_packets: take(&self.stream_carrying_packets),
            produced_packets: take(&self.produced_packets),
            timer_rearms: take(&self.timer_rearms),
            timer_fires: take(&self.timer_fires),
            wake_registrations: take(&self.wake_registrations),
            inbound_wakes: take(&self.inbound_wakes),
            capacity_registrations: take(&self.capacity_registrations),
            capacity_wakes: take(&self.capacity_wakes),
            retries: take(&self.retries),
            parks: take(&self.parks),
            zero_accept_retries: take(&self.zero_accept_retries),
            zero_accept_retries_without_enable: take(&self.zero_accept_retries_without_enable),
            inbound_queue_depth,
            inbound_queue_high_water: self
                .inbound_queue_high_water
                .swap(inbound_queue_depth, Ordering::Relaxed),
            inbound_drops: take(&self.inbound_drops),
            outbound_queue_depth,
            outbound_queue_high_water: self
                .outbound_queue_high_water
                .swap(outbound_queue_depth, Ordering::Relaxed),
            outbound_capacity_transitions: take(&self.outbound_capacity_transitions),
            terminal_discarded_inbound: take(&self.terminal_discarded_inbound),
            terminal_discarded_outbound: take(&self.terminal_discarded_outbound),
        }
    }
}

static ARMED: AtomicBool = AtomicBool::new(false);
static OVERFLOWED: AtomicBool = AtomicBool::new(false);
static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
static TEST_STAGING_LIMIT: AtomicU64 = AtomicU64::new(u64::MAX);
static ROLES: [AtomicRole; 2] = [AtomicRole::new(), AtomicRole::new()];
static RECORDING_GATE: RwLock<()> = RwLock::new(());
static LAST_ENABLING: Mutex<BTreeMap<u64, u64>> = Mutex::new(BTreeMap::new());
static ATTEMPTS: Mutex<Vec<Attempt>> = Mutex::new(Vec::new());
static LIVENESS: Mutex<Vec<LivenessEvent>> = Mutex::new(Vec::new());
static ZERO_WAITING: Mutex<BTreeMap<(u64, i64, u64), u64>> = Mutex::new(BTreeMap::new());
#[cfg(test)]
pub(crate) static TEST_LOCK: Mutex<()> = Mutex::new(());

fn role_index(role: Role) -> usize {
    match role {
        Role::Client => 0,
        Role::Server => 1,
    }
}

fn counters(role: Role) -> &'static AtomicRole {
    &ROLES[role_index(role)]
}

fn add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |current| match current.checked_add(value) {
            Some(next) => Some(next),
            None => {
                OVERFLOWED.store(true, Ordering::Relaxed);
                Some(u64::MAX)
            }
        },
    );
}

fn high_water(counter: &AtomicU64, value: u64) {
    counter.fetch_max(value, Ordering::Relaxed);
}

fn next_sequence() -> u64 {
    let mut sequence = 0;
    let _ = NEXT_SEQUENCE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        sequence = current;
        match current.checked_add(1) {
            Some(next) => Some(next),
            None => {
                OVERFLOWED.store(true, Ordering::Relaxed);
                Some(u64::MAX)
            }
        }
    });
    sequence
}

/// Allocates a process-local identity for one connection.
#[doc(hidden)]
pub fn next_connection_id() -> u64 {
    let mut id = 0;
    let _ = NEXT_CONNECTION_ID.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        id = current;
        match current.checked_add(1) {
            Some(next) => Some(next),
            None => {
                OVERFLOWED.store(true, Ordering::Relaxed);
                Some(u64::MAX)
            }
        }
    });
    id
}

fn recording_guard() -> Option<RwLockReadGuard<'static, ()>> {
    if !armed() {
        return None;
    }
    let guard = RECORDING_GATE
        .read()
        .unwrap_or_else(|error| error.into_inner());
    armed().then_some(guard)
}

fn push_liveness(
    connection_id: u64,
    role: Role,
    kind: LivenessKind,
    reason: &'static str,
    attempt_sequence: Option<u64>,
    parked_attempt_sequence: Option<u64>,
    enabling_sequence: Option<u64>,
) -> u64 {
    let sequence = next_sequence();
    LIVENESS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(LivenessEvent {
            sequence,
            connection_id,
            role,
            kind,
            reason,
            attempt_sequence,
            parked_attempt_sequence,
            enabling_sequence,
        });
    sequence
}

fn record_enabling(connection_id: u64, role: Role, reason: &'static str) {
    let sequence = push_liveness(
        connection_id,
        role,
        LivenessKind::EnablingEvent,
        reason,
        None,
        None,
        None,
    );
    LAST_ENABLING
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .insert(connection_id, sequence);
}

fn armed() -> bool {
    ARMED.load(Ordering::Relaxed)
}

/// Clears all observations and leaves diagnostics unarmed.
pub fn reset() {
    let _guard = RECORDING_GATE
        .write()
        .unwrap_or_else(|error| error.into_inner());
    ARMED.store(false, Ordering::SeqCst);
    for role in &ROLES {
        role.reset();
    }
    OVERFLOWED.store(false, Ordering::Relaxed);
    NEXT_SEQUENCE.store(1, Ordering::Relaxed);
    TEST_STAGING_LIMIT.store(u64::MAX, Ordering::Relaxed);
    LAST_ENABLING
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clear();
    ZERO_WAITING
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clear();
    ATTEMPTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clear();
    LIVENESS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clear();
}

/// Enables or disables observation recording.
pub fn arm(enabled: bool) {
    let _guard = RECORDING_GATE
        .write()
        .unwrap_or_else(|error| error.into_inner());
    ARMED.store(enabled, Ordering::SeqCst);
}

/// Whether recording is currently armed.
pub fn is_armed() -> bool {
    armed()
}

/// Sets a deterministic stricter pre-native staging limit for diagnostic tests.
///
/// Production always bounds borrowing staging by the sampled path payload limit. `Some`
/// further lowers that bound so tests can force exact slice boundaries; `None` preserves the
/// production limit. This control exists only with the non-default diagnostics feature and
/// is reset by [`reset`].
#[doc(hidden)]
pub fn set_test_staging_limit(limit: Option<usize>) {
    let value = limit
        .and_then(|limit| u64::try_from(limit).ok())
        .unwrap_or(u64::MAX);
    TEST_STAGING_LIMIT.store(value, Ordering::Relaxed);
}

pub(crate) fn test_staging_limit() -> Option<usize> {
    let limit = TEST_STAGING_LIMIT.load(Ordering::Relaxed);
    (limit != u64::MAX).then(|| usize::try_from(limit).unwrap_or(usize::MAX))
}

/// Returns current aggregate observations.
///
/// This is a point read only. Use [`drain`] when counters, attempts, and liveness must
/// describe one coherent interval.
pub fn snapshot() -> Snapshot {
    let _guard = RECORDING_GATE
        .read()
        .unwrap_or_else(|error| error.into_inner());
    Snapshot {
        client: ROLES[0].snapshot(),
        server: ROLES[1].snapshot(),
        overflowed: OVERFLOWED.load(Ordering::Relaxed),
        retransmissions_available: false,
    }
}

/// Removes and returns stream-write attempts recorded since the previous call.
///
/// This is not coherent with separate calls to [`snapshot`] or
/// [`take_liveness_events`]. Use [`drain`] for reconciliation.
pub fn take_attempts() -> Vec<Attempt> {
    let _guard = RECORDING_GATE
        .read()
        .unwrap_or_else(|error| error.into_inner());
    core::mem::take(&mut *ATTEMPTS.lock().unwrap_or_else(|error| error.into_inner()))
}

/// Removes and returns liveness events recorded since the previous call.
///
/// This is not coherent with separate calls to [`snapshot`] or [`take_attempts`]. Use
/// [`drain`] for reconciliation.
pub fn take_liveness_events() -> Vec<LivenessEvent> {
    let _guard = RECORDING_GATE
        .read()
        .unwrap_or_else(|error| error.into_inner());
    core::mem::take(&mut *LIVENESS.lock().unwrap_or_else(|error| error.into_inner()))
}

/// Exclusively drains one coherent diagnostic interval.
///
/// Recorders cannot interleave between the snapshot and event drains. Cumulative counters
/// reset to zero. Live retained-byte and queue-depth gauges keep their current values, while
/// their high-water marks start the next interval at those live values.
pub fn drain() -> DrainedDiagnostics {
    let _guard = RECORDING_GATE
        .write()
        .unwrap_or_else(|error| error.into_inner());
    DrainedDiagnostics {
        snapshot: Snapshot {
            client: ROLES[0].drain_interval(),
            server: ROLES[1].drain_interval(),
            overflowed: OVERFLOWED.swap(false, Ordering::Relaxed),
            retransmissions_available: false,
        },
        attempts: core::mem::take(&mut *ATTEMPTS.lock().unwrap_or_else(|error| error.into_inner())),
        liveness: core::mem::take(&mut *LIVENESS.lock().unwrap_or_else(|error| error.into_inner())),
    }
}

pub(crate) fn record_attempt(mut attempt: Attempt) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    attempt.sequence = next_sequence();
    let role = counters(attempt.role);
    add(&role.offered_bytes, attempt.offered_bytes);
    add(
        &role.prepared_backing_capacity,
        attempt.prepared_backing_capacity,
    );
    add(&role.accepted_bytes, attempt.accepted_prefix);
    if attempt.zero_acceptance {
        add(&role.zero_acceptances, 1);
    }
    role.logical_retained_bytes
        .store(attempt.logical_retained_bytes, Ordering::Relaxed);
    high_water(
        &role.logical_retained_high_water,
        attempt.logical_retained_bytes,
    );
    role.retained_backing_capacity
        .store(attempt.retained_backing_capacity, Ordering::Relaxed);
    high_water(
        &role.retained_backing_high_water,
        attempt.retained_backing_capacity,
    );
    let wait_key = (
        attempt.connection_id,
        attempt.stream_id,
        attempt.stream_offset,
    );
    if attempt.zero_acceptance {
        let waiting = ZERO_WAITING
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(wait_key, attempt.sequence);
        if let Some(waiting) = waiting {
            let enabling = LAST_ENABLING
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&attempt.connection_id)
                .copied()
                .unwrap_or(0);
            let enabling = (enabling > waiting).then_some(enabling);
            add(&role.retries, 1);
            add(&role.zero_accept_retries, 1);
            if enabling.is_none() {
                add(&role.zero_accept_retries_without_enable, 1);
            }
            push_liveness(
                attempt.connection_id,
                attempt.role,
                LivenessKind::Retry,
                "zero-accept",
                Some(attempt.sequence),
                Some(waiting),
                enabling,
            );
        }
        add(&role.parks, 1);
        push_liveness(
            attempt.connection_id,
            attempt.role,
            LivenessKind::Park,
            match attempt.outcome {
                AttemptOutcome::Datagram => "zero-accept-datagram",
                AttemptOutcome::StreamBlocked => "stream-flow-control",
                AttemptOutcome::ConnectionBlocked => "connection-flow-control",
                AttemptOutcome::Blocked => "transport-blocked",
                AttemptOutcome::Idle => "transport-idle",
            },
            Some(attempt.sequence),
            None,
            None,
        );
    } else {
        let waiting = ZERO_WAITING
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&wait_key);
        if let Some(waiting) = waiting {
            let enabling = LAST_ENABLING
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&attempt.connection_id)
                .copied()
                .unwrap_or(0);
            let enabling = (enabling > waiting).then_some(enabling);
            add(&role.retries, 1);
            add(&role.zero_accept_retries, 1);
            if enabling.is_none() {
                add(&role.zero_accept_retries_without_enable, 1);
            }
            push_liveness(
                attempt.connection_id,
                attempt.role,
                LivenessKind::Retry,
                "zero-accept",
                Some(attempt.sequence),
                Some(waiting),
                enabling,
            );
        }
    }
    ATTEMPTS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(attempt);
}

pub(crate) fn record_retained(
    role: Role,
    logical: usize,
    backing: usize,
    acknowledged: u64,
    released_backing: usize,
) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    let role = counters(role);
    let logical = logical as u64;
    let backing = backing as u64;
    role.logical_retained_bytes
        .store(logical, Ordering::Relaxed);
    high_water(&role.logical_retained_high_water, logical);
    role.retained_backing_capacity
        .store(backing, Ordering::Relaxed);
    high_water(&role.retained_backing_high_water, backing);
    add(&role.acknowledged_bytes, acknowledged);
    add(&role.released_backing_capacity, released_backing as u64);
}

/// Records bytes released back to the HTTP/3 source after transport copying.
#[doc(hidden)]
pub fn record_release(_connection_id: u64, role: Role, bytes: usize) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).release_event_bytes, bytes as u64);
}

/// Records one packet produced by the HTTP/3 transport adapter.
#[doc(hidden)]
pub fn record_packet(connection_id: u64, role: Role, stream_carrying: bool) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    let role_counters = counters(role);
    add(&role_counters.produced_packets, 1);
    if stream_carrying {
        add(&role_counters.stream_carrying_packets, 1);
    } else {
        add(&role_counters.transport_only_packets, 1);
        push_liveness(
            connection_id,
            role,
            LivenessKind::LocalProduction,
            "transport-packet",
            None,
            None,
            None,
        );
    }
}

/// Records replacement of the adapter's armed sleep.
#[doc(hidden)]
pub fn record_timer_rearm(_connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).timer_rearms, 1);
}

/// Records an adapter sleep resolving at its deadline.
#[doc(hidden)]
pub fn record_timer_fire(connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).timer_fires, 1);
    record_enabling(connection_id, role, "timer");
}

/// Records a new general connection-waker registration.
#[doc(hidden)]
pub fn record_wake_registration(_connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).wake_registrations, 1);
}

/// Records a connection future being polled again after it parked.
#[doc(hidden)]
pub fn record_driver_wake(connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    push_liveness(
        connection_id,
        role,
        LivenessKind::DriverWake,
        "driver-wake",
        None,
        None,
        None,
    );
}

pub(crate) fn record_inbound_wakes(connection_id: u64, role: Role, count: usize) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).inbound_wakes, count as u64);
    record_enabling(connection_id, role, "inbound-datagram");
}

pub(crate) fn record_capacity_registration(connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).capacity_registrations, 1);
    add(&counters(role).parks, 1);
    push_liveness(
        connection_id,
        role,
        LivenessKind::Park,
        "outbound-capacity",
        None,
        None,
        None,
    );
}

pub(crate) fn record_capacity_wakes(connection_id: u64, role: Role, count: usize) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).capacity_wakes, count as u64);
    record_enabling(connection_id, role, "outbound-capacity");
}

/// Records an actual retry after an earlier full-queue park.
#[doc(hidden)]
pub fn record_retry(connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).retries, 1);
    let enabling = LAST_ENABLING
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .get(&connection_id)
        .copied()
        .unwrap_or(0);
    push_liveness(
        connection_id,
        role,
        LivenessKind::Retry,
        "outbound-capacity",
        None,
        None,
        (enabling != 0).then_some(enabling),
    );
}

/// Records a connection poll parking without an immediately ready event.
#[doc(hidden)]
pub fn record_park(connection_id: u64, role: Role) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    add(&counters(role).parks, 1);
    push_liveness(
        connection_id,
        role,
        LivenessKind::Park,
        "poll-idle",
        None,
        None,
        None,
    );
}

pub(crate) fn record_inbound_queue(_connection_id: u64, role: Role, depth: usize, dropped: bool) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    let role = counters(role);
    role.inbound_queue_depth
        .store(depth as u64, Ordering::Relaxed);
    high_water(&role.inbound_queue_high_water, depth as u64);
    if dropped {
        add(&role.inbound_drops, 1);
    }
}

pub(crate) fn record_outbound_queue(
    _connection_id: u64,
    role: Role,
    depth: usize,
    capacity_transition: bool,
) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    let role = counters(role);
    role.outbound_queue_depth
        .store(depth as u64, Ordering::Relaxed);
    high_water(&role.outbound_queue_high_water, depth as u64);
    if capacity_transition {
        add(&role.outbound_capacity_transitions, 1);
    }
}

pub(crate) fn record_terminal_inventory(
    role: Role,
    inbound_discarded: usize,
    outbound_discarded: usize,
) {
    let Some(_guard) = recording_guard() else {
        return;
    };
    let role = counters(role);
    role.inbound_queue_depth.store(0, Ordering::Relaxed);
    add(&role.terminal_discarded_inbound, inbound_discarded as u64);
    add(&role.terminal_discarded_outbound, outbound_discarded as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_but_unarmed_hooks_change_nothing() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset();
        record_packet(1, Role::Client, true);
        record_release(1, Role::Client, 7);
        assert_eq!(snapshot(), Snapshot::default());
        assert!(take_attempts().is_empty());
    }

    #[test]
    fn armed_attempts_reconcile_and_report_unavailable_fields() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset();
        arm(true);
        record_attempt(Attempt {
            sequence: 0,
            connection_id: 1,
            role: Role::Server,
            direction: "outbound",
            stream_id: 0,
            stream_offset: 0,
            offered_bytes: 16,
            sampled_payload_limit: 1200,
            prepared_backing_capacity: 16,
            accepted_prefix: 7,
            fin_offered: true,
            zero_acceptance: false,
            logical_retained_bytes: 7,
            retained_backing_capacity: 16,
            outcome: AttemptOutcome::Datagram,
        });
        record_release(1, Role::Server, 7);
        record_packet(1, Role::Server, true);
        record_inbound_wakes(1, Role::Server, 1);

        let snapshot = snapshot();
        assert_eq!(snapshot.server.accepted_bytes, 7);
        assert_eq!(snapshot.server.release_event_bytes, 7);
        assert_eq!(snapshot.server.stream_carrying_packets, 1);
        assert_eq!(
            snapshot.server.produced_packets,
            snapshot.server.transport_only_packets + snapshot.server.stream_carrying_packets
        );
        assert!(!snapshot.retransmissions_available);
        assert_eq!(take_attempts().len(), 1);
        assert!(
            take_liveness_events()
                .iter()
                .any(|event| event.kind == LivenessKind::EnablingEvent)
        );
        reset();
    }

    #[test]
    fn equal_streams_on_different_connections_do_not_become_retries() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset();
        arm(true);
        for connection_id in [10, 11] {
            record_attempt(Attempt {
                sequence: 0,
                connection_id,
                role: Role::Client,
                direction: "outbound",
                stream_id: 0,
                stream_offset: 0,
                offered_bytes: 8,
                sampled_payload_limit: 1200,
                prepared_backing_capacity: 8,
                accepted_prefix: 0,
                fin_offered: false,
                zero_acceptance: true,
                logical_retained_bytes: 0,
                retained_backing_capacity: 0,
                outcome: AttemptOutcome::Blocked,
            });
        }
        let snapshot = snapshot();
        assert_eq!(snapshot.client.zero_acceptances, 2);
        assert_eq!(snapshot.client.zero_accept_retries, 0);
        reset();
    }

    #[test]
    fn local_output_and_driver_wakes_do_not_enable_zero_accept_retries() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset();
        arm(true);
        let attempt = |accepted_prefix| Attempt {
            sequence: 0,
            connection_id: 77,
            role: Role::Client,
            direction: "outbound",
            stream_id: 0,
            stream_offset: 0,
            offered_bytes: 8,
            sampled_payload_limit: 1200,
            prepared_backing_capacity: 8,
            accepted_prefix,
            fin_offered: false,
            zero_acceptance: accepted_prefix == 0,
            logical_retained_bytes: 0,
            retained_backing_capacity: 0,
            outcome: AttemptOutcome::Datagram,
        };

        record_attempt(attempt(0));
        record_packet(77, Role::Client, false);
        record_driver_wake(77, Role::Client);
        record_attempt(attempt(0));

        let drained = drain();
        assert_eq!(drained.snapshot.client.zero_accept_retries, 1);
        assert_eq!(
            drained.snapshot.client.zero_accept_retries_without_enable,
            1
        );
        assert!(drained.liveness.iter().any(|event| {
            event.kind == LivenessKind::LocalProduction && event.reason == "transport-packet"
        }));
        assert!(drained.liveness.iter().any(|event| {
            event.kind == LivenessKind::DriverWake && event.reason == "driver-wake"
        }));
        reset();
    }

    #[test]
    fn drain_preserves_live_gauges_and_reseeds_high_water_marks() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset();
        arm(true);
        record_retained(Role::Server, 7, 16, 0, 0);
        record_inbound_queue(1, Role::Server, 3, false);

        let first = drain();
        assert_eq!(first.snapshot.server.logical_retained_bytes, 7);
        assert_eq!(first.snapshot.server.logical_retained_high_water, 7);
        assert_eq!(first.snapshot.server.inbound_queue_depth, 3);
        assert_eq!(first.snapshot.server.inbound_queue_high_water, 3);

        let second = drain();
        assert_eq!(second.snapshot.server.logical_retained_bytes, 7);
        assert_eq!(second.snapshot.server.logical_retained_high_water, 7);
        assert_eq!(second.snapshot.server.inbound_queue_depth, 3);
        assert_eq!(second.snapshot.server.inbound_queue_high_water, 3);
        reset();
    }
}
