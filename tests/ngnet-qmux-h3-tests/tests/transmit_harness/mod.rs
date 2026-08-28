//! Driving an HTTP/3-over-QMux exchange by hand, so that a driver turn is a thing a test can
//! point at.
//!
//! # Why not a runtime
//!
//! Every other test in this crate spawns the two drivers onto a [`LocalSet`] and lets tokio
//! decide when they run. That is the right shape for asking whether an exchange completes, and
//! the wrong one for asking what a *pass* costs: once a driver is spawned, the boundaries
//! between its polls are the runtime's business and nothing outside can see them.
//!
//! Spec FR-001 is stated over the driver-visible transmit pass -- the bounded run of at most
//! sixty-four offers the HTTP/3 layer makes to the transport, together with every write those
//! offers cause, ending when the driver is returned to. So a test that wants to count the
//! writes a pass issues has to be the thing that polls the driver, which is what this module
//! is. One call to [`Turns::drive`] polls three futures round-robin -- the client's connection,
//! the server's, and the exchange itself -- and records the client's write count across each
//! poll of the client's connection future.
//!
//! Nothing here needs a runtime to do that: neither `ngnet-h3` nor `ngnet-qmux-h3` names one,
//! the byte streams are in memory, and the clock only moves when it is told to.
//!
//! # Why a turn is one poll of the connection future
//!
//! `QmuxConnection::poll_transmit` is where the bounded run of offers happens
//! (`crates/ngnet-qmux-h3/src/transmit.rs`), and it is reached only from inside the HTTP/3
//! driver, which is inside the connection future. Nothing outside that future can call it, and
//! [`Turns::drive`] checks rather than assumes it: the write log is read before and after every
//! poll of every *other* future too, and a write attributed to one of them fails the run. A
//! turn is therefore exactly the writes one poll of the connection future produced.
//!
//! [`LocalSet`]: tokio::task::LocalSet

// Each test target uses a different part of this module, and an unused helper in one of them
// is not a defect.
#![allow(dead_code)]

use core::future::Future;
use core::task::{Context, Poll, Waker};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;

use ngnet_qmux::io::testing::WriteLog;

/// How many round-robin passes before a run is declared broken.
///
/// Reached only by a run that is woken on every pass and never finishes, which the stall
/// detector below cannot see. The bound was sized for a multi-megabyte transfer moving one
/// record per turn, which is what the connection did before write coalescing. It now moves up
/// to `OUTBOUND_CEILING`'s worth per turn, so the bound is several times looser than anything
/// these tests need — left where it is deliberately, because a limit that only ever fires on a
/// livelock should be far from the workload rather than near it, and lowering it would trade a
/// diagnostic for nothing.
const MAX_PASSES: usize = 2_000_000;

/// A waker that remembers it was fired.
#[derive(Default)]
struct Flag {
    woken: AtomicBool,
}

impl Flag {
    fn take(&self) -> bool {
        self.woken.swap(false, Ordering::SeqCst)
    }
}

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}

/// What each poll of the client's connection future wrote.
pub struct Turns {
    /// How many writes each turn that wrote anything issued, in order.
    ///
    /// A poll that wrote nothing is not a transmit pass that cost nothing -- it is a pass that
    /// had nothing to send -- and keeping those would make every figure here depend on how
    /// often the executor happened to poll an idle connection.
    pub writes: Vec<usize>,
    /// Every write length the client issued, in order, across the whole run.
    pub lengths: Vec<usize>,
    /// Every write length the server issued when both endpoints are being measured.
    pub server_lengths: Vec<usize>,
}

impl Turns {
    /// The most writes any one turn issued.
    #[must_use]
    pub fn busiest(&self) -> usize {
        self.writes.iter().copied().max().unwrap_or(0)
    }

    /// How many writes the client issued in total.
    #[must_use]
    pub fn total(&self) -> usize {
        self.lengths.len()
    }

    /// How many writes both endpoints issued in total.
    #[must_use]
    pub fn whole_total(&self) -> usize {
        self.lengths.len() + self.server_lengths.len()
    }

    /// Drives three futures to the exchange's completion, recording the client's writes.
    ///
    /// The exchange is polled first on every pass, so a request is made before the drivers are
    /// asked to carry it; the run ends when the exchange completes, which leaves both drivers
    /// unfinished on purpose -- a driver runs until its connection closes, and closing it would
    /// add a round trip that is not part of what is being measured.
    ///
    /// # Panics
    ///
    /// If nothing completes and nothing is woken, which is a stall rather than slow progress;
    /// if the run exceeds `MAX_PASSES`; or if a write is issued outside a poll of the client's
    /// connection future, which would mean a turn is not what this module says it is.
    pub fn drive<Conn, Serve, Exchange, Out>(
        client_log: &WriteLog,
        connection: Conn,
        serving: Serve,
        exchange: Exchange,
    ) -> (Out, Self)
    where
        Conn: Future,
        Serve: Future,
        Exchange: Future<Output = Out>,
    {
        Self::drive_inner(client_log, None, connection, serving, exchange)
    }

    /// Drives an exchange while recording both endpoint byte streams.
    pub fn drive_both<Conn, Serve, Exchange, Out>(
        client_log: &WriteLog,
        server_log: &WriteLog,
        connection: Conn,
        serving: Serve,
        exchange: Exchange,
    ) -> (Out, Self)
    where
        Conn: Future,
        Serve: Future,
        Exchange: Future<Output = Out>,
    {
        Self::drive_inner(client_log, Some(server_log), connection, serving, exchange)
    }

    fn drive_inner<Conn, Serve, Exchange, Out>(
        client_log: &WriteLog,
        server_log: Option<&WriteLog>,
        connection: Conn,
        serving: Serve,
        exchange: Exchange,
    ) -> (Out, Self)
    where
        Conn: Future,
        Serve: Future,
        Exchange: Future<Output = Out>,
    {
        let flag = Arc::new(Flag::default());
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);

        let mut connection = Box::pin(connection);
        let mut serving = Box::pin(serving);
        let mut exchange = Box::pin(exchange);
        let mut turns = Vec::new();

        for _ in 0..MAX_PASSES {
            let before_exchange = client_log.writes();
            let finished = exchange.as_mut().poll(&mut cx);
            assert_eq!(
                client_log.writes(),
                before_exchange,
                "the exchange future wrote to the byte stream itself; a turn is then not the \
                 unit this module claims to measure"
            );

            let before_turn = client_log.writes();
            let _ = connection.as_mut().poll(&mut cx);
            let wrote = client_log.writes() - before_turn;
            if wrote > 0 {
                turns.push(wrote);
            }

            let before_server = client_log.writes();
            let _ = serving.as_mut().poll(&mut cx);
            assert_eq!(
                client_log.writes(),
                before_server,
                "polling the server wrote on the client's byte stream, which is impossible \
                 unless the two ends have been wired together wrongly"
            );

            if let Poll::Ready(output) = finished {
                return (
                    output,
                    Self {
                        writes: turns,
                        lengths: client_log.lengths(),
                        server_lengths: server_log.map_or_else(Vec::new, WriteLog::lengths),
                    },
                );
            }

            // A pass in which nothing finished and nothing was woken cannot be followed by a
            // pass that differs, so the run is stuck. Said here rather than left to a timeout,
            // because a hung test reports as a job with no output.
            assert!(
                flag.take(),
                "the run stalled: nothing completed and nothing registered a wake"
            );
        }
        panic!("the run never finished; it is being woken without making progress");
    }
}

/// Collects a body to its end.
///
/// The response body arrives through the connection, so collecting it has to happen inside the
/// exchange future rather than after [`Turns::drive`] returns: by then nothing is polling the
/// drivers that would carry the rest of it.
pub async fn collected<B: http_body::Body<Data = bytes::Bytes>>(body: B) -> bytes::Bytes
where
    B::Error: core::fmt::Debug,
{
    use http_body_util::BodyExt;
    body.collect().await.expect("a response body").to_bytes()
}
