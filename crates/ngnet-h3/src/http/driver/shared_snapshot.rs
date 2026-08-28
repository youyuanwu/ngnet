use core::fmt;
use core::task::{Context, Poll};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{Driver, DriverGuard, Role, TransportAction, build_conn, run};
use crate::conn::{Conn, ConnBuilder, Role as CoreRole};
use crate::error::ErrorCode;
use crate::handlers::Shutdown;
use crate::http::config::Config;
use crate::http::events::Events;
use crate::http::quic::{QuicConnection, QuicEvent, StreamSource, Timestamp, WriteOutcome};
use crate::http::shared::{Registry, Shared};
use crate::stream::{Directionality, Initiator, StreamId};

#[derive(Clone, Copy)]
enum QueueWork {
    AfterFirstTransmit,
    DuringResetAction,
    ResetAfterFirstTransmit,
}

#[derive(Default)]
struct Observed {
    transmits: AtomicUsize,
    resets: AtomicUsize,
    stop_sendings: AtomicUsize,
    shutdown_pass: AtomicUsize,
    closed: AtomicBool,
    shutdowns: Mutex<Vec<Shutdown>>,
}

#[derive(Debug)]
struct TestError;

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("injected backend failure")
    }
}

impl core::error::Error for TestError {}

struct TestBackend {
    shared: Arc<Shared>,
    observed: Arc<Observed>,
    peer: Conn<()>,
    trigger: QueueWork,
    fail_reset: bool,
    triggered: bool,
    next_uni: u64,
    now: u64,
}

impl TestBackend {
    fn new(
        shared: Arc<Shared>,
        observed: Arc<Observed>,
        trigger: QueueWork,
        fail_reset: bool,
    ) -> Self {
        let for_shutdown = Arc::clone(&observed);
        let mut peer = ConnBuilder::<()>::new(CoreRole::Client)
            .on_shutdown(move |_, shutdown| {
                for_shutdown
                    .shutdowns
                    .lock()
                    .expect("shutdown observations")
                    .push(shutdown);
                for_shutdown.shutdown_pass.store(
                    for_shutdown.transmits.load(Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            })
            .build()
            .expect("a peer connection");
        peer.bind_control_stream(StreamId::new(2).expect("client control"))
            .expect("bind client control");
        peer.bind_qpack_streams(
            StreamId::new(6).expect("client encoder"),
            StreamId::new(10).expect("client decoder"),
        )
        .expect("bind client QPACK streams");
        Self {
            shared,
            observed,
            peer,
            trigger,
            fail_reset,
            triggered: false,
            next_uni: 0,
            now: 0,
        }
    }

    fn queue_shutdown(&mut self) {
        self.shared.request_shutdown();
        self.shared.wake_driver();
        self.triggered = true;
    }
}

impl QuicConnection for TestBackend {
    type Error = TestError;

    const RETAINS_BUFFERS: bool = false;

    fn poll_event(&mut self, _cx: &mut Context<'_>) -> Poll<Result<QuicEvent, Self::Error>> {
        Poll::Pending
    }

    fn poll_transmit<S: StreamSource>(
        &mut self,
        _cx: &mut Context<'_>,
        source: &mut S,
    ) -> Poll<Result<(), Self::Error>> {
        self.observed.transmits.fetch_add(1, Ordering::SeqCst);
        while source.write_next(&mut |stream, slices, fin| {
            let bytes: Vec<u8> = slices
                .iter()
                .flat_map(|slice| slice.iter().copied())
                .collect();
            self.now += 1;
            self.peer
                .read_stream(
                    stream,
                    &bytes,
                    fin,
                    Timestamp::from_nanos(self.now),
                    &mut (),
                )
                .expect("the peer accepts driver output");
            WriteOutcome::Accepted(bytes.len())
        }) {}
        if !self.triggered && matches!(self.trigger, QueueWork::AfterFirstTransmit) {
            self.queue_shutdown();
        }
        if !self.triggered && matches!(self.trigger, QueueWork::ResetAfterFirstTransmit) {
            self.shared
                .reset(StreamId::new(0).expect("a stream"), ErrorCode::new(0x10c));
            self.shared.wake_driver();
            self.triggered = true;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_open_uni(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        let stream = StreamId::compose(
            Initiator::Server,
            Directionality::Unidirectional,
            self.next_uni,
        )
        .expect("a server unidirectional stream");
        self.next_uni += 1;
        Poll::Ready(Ok(stream))
    }

    fn poll_open_bi(&mut self, _cx: &mut Context<'_>) -> Poll<Result<StreamId, Self::Error>> {
        unreachable!("the completed server role opens no request streams")
    }

    fn reset(&mut self, _stream: StreamId, _code: ErrorCode) -> Result<(), Self::Error> {
        self.observed.resets.fetch_add(1, Ordering::SeqCst);
        if !self.triggered && matches!(self.trigger, QueueWork::DuringResetAction) {
            self.queue_shutdown();
        }
        if self.fail_reset {
            Err(TestError)
        } else {
            Ok(())
        }
    }

    fn stop_sending(&mut self, _stream: StreamId, _code: ErrorCode) -> Result<(), Self::Error> {
        self.observed.stop_sendings.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn extend_credit(&mut self, _stream: Option<StreamId>, _bytes: u64) -> Result<(), Self::Error> {
        Ok(())
    }

    fn close(&mut self, _code: ErrorCode, _reason: &[u8]) -> Result<(), Self::Error> {
        self.observed.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn now(&self) -> Timestamp {
        Timestamp::from_nanos(self.now)
    }
}

struct DoneRole;

impl Role for DoneRole {
    fn advance(&mut self, _conn: &mut Conn<Events>, _events: &mut Events) -> super::Result<()> {
        Ok(())
    }

    fn settle(&mut self, _conn: &mut Conn<Events>) -> super::Result<()> {
        Ok(())
    }

    fn head(
        &mut self,
        _conn: &mut Conn<Events>,
        _events: &mut Events,
        _stream: StreamId,
        _fields: &[crate::http::head::ReceivedField],
    ) -> super::Result<()> {
        Ok(())
    }

    fn closed(&mut self, _stream: StreamId) {}

    fn busy(&self) -> bool {
        false
    }

    fn done(&self) -> bool {
        true
    }

    fn abandon(&mut self) {}
}

fn run_completed_server(trigger: QueueWork, with_action: bool) -> Arc<Observed> {
    let shared = Arc::new(Shared::new());
    if with_action {
        shared.push_action(TransportAction::Reset {
            stream: StreamId::new(0).expect("a stream"),
            code: ErrorCode::new(1),
        });
    }
    let registry = Arc::new(Registry::new());
    let config = Config::default();
    let conn = build_conn(CoreRole::Server, &config, &shared).expect("a server connection");
    let observed = Arc::new(Observed::default());
    let backend = TestBackend::new(Arc::clone(&shared), Arc::clone(&observed), trigger, false);
    let driver = Driver::new(
        backend,
        conn,
        Arc::clone(&shared),
        Arc::clone(&registry),
        config,
    );
    let guard = DriverGuard::new(Arc::clone(&shared), registry, DoneRole);

    crate::http::testing::block_on(run(driver, guard)).expect("the driver completes");
    assert_eq!(
        shared.operation_counts(),
        (2, 0, 2, 0),
        "two productive passes use one drain and one completion probe each"
    );
    observed
}

#[test]
fn work_queued_during_final_processing_gets_a_next_pass_and_sends_goaway() {
    let observed = run_completed_server(QueueWork::AfterFirstTransmit, false);
    assert_eq!(observed.transmits.load(Ordering::SeqCst), 2);
    assert_eq!(observed.shutdown_pass.load(Ordering::SeqCst), 2);
    assert_eq!(
        observed.shutdowns.lock().expect("shutdowns").len(),
        1,
        "the peer decodes the shutdown queued after the first snapshot"
    );
    assert!(observed.closed.load(Ordering::SeqCst));
}

#[test]
fn work_induced_by_an_earlier_category_waits_for_the_next_snapshot() {
    let observed = run_completed_server(QueueWork::DuringResetAction, true);
    assert_eq!(
        observed.shutdown_pass.load(Ordering::SeqCst),
        2,
        "shutdown queued by action processing cannot cross into its source snapshot"
    );
    assert_eq!(observed.shutdowns.lock().expect("shutdowns").len(), 1);
}

#[test]
fn a_fatal_backend_error_drops_the_owned_tail_and_ends_the_driver() {
    let shared = Arc::new(Shared::new());
    let first = StreamId::new(0).expect("a stream");
    let second = StreamId::new(4).expect("a stream");
    shared.push_action(TransportAction::Reset {
        stream: first,
        code: ErrorCode::new(1),
    });
    shared.push_action(TransportAction::StopSending {
        stream: second,
        code: ErrorCode::new(2),
    });

    let registry = Arc::new(Registry::new());
    let config = Config::default();
    let conn = build_conn(CoreRole::Server, &config, &shared).expect("a server connection");
    let observed = Arc::new(Observed::default());
    let backend = TestBackend::new(
        Arc::clone(&shared),
        Arc::clone(&observed),
        QueueWork::DuringResetAction,
        true,
    );
    let driver = Driver::new(
        backend,
        conn,
        Arc::clone(&shared),
        Arc::clone(&registry),
        config,
    );
    let guard = DriverGuard::new(Arc::clone(&shared), registry, DoneRole);

    crate::http::testing::block_on(run(driver, guard))
        .expect_err("the injected backend failure must end the driver");
    assert_eq!(observed.resets.load(Ordering::SeqCst), 1);
    assert_eq!(
        observed.stop_sendings.load(Ordering::SeqCst),
        0,
        "the unprocessed tail of the destructive snapshot is not replayed"
    );
    assert!(!observed.closed.load(Ordering::SeqCst));
    assert!(shared.is_gone(), "driver teardown marks every handle gone");
    assert!(
        shared.work_pending_for_completion(),
        "work queued by the failing operation stays beyond its source snapshot, but the gone \
         driver cannot replay it"
    );
}

#[test]
fn failing_work_found_at_completion_is_a_terminal_driver_error() {
    let shared = Arc::new(Shared::new());
    let registry = Arc::new(Registry::new());
    let config = Config::default();
    let conn = build_conn(CoreRole::Server, &config, &shared).expect("a server connection");
    let observed = Arc::new(Observed::default());
    let backend = TestBackend::new(
        Arc::clone(&shared),
        Arc::clone(&observed),
        QueueWork::ResetAfterFirstTransmit,
        true,
    );
    let driver = Driver::new(
        backend,
        conn,
        Arc::clone(&shared),
        Arc::clone(&registry),
        config,
    );
    let guard = DriverGuard::new(Arc::clone(&shared), registry, DoneRole);

    crate::http::testing::block_on(run(driver, guard))
        .expect_err("completion-deferred backend failures retain normal error propagation");
    assert_eq!(observed.transmits.load(Ordering::SeqCst), 1);
    assert_eq!(observed.resets.load(Ordering::SeqCst), 1);
    assert_eq!(observed.stop_sendings.load(Ordering::SeqCst), 0);
    assert!(!observed.closed.load(Ordering::SeqCst));
    assert!(shared.is_gone());
}
