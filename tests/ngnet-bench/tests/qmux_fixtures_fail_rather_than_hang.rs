//! The QMux arms must fail loudly, because the alternative on this stack is silence.
//!
//! Every other arm in this crate answers work it cannot do with an error: a closed HTTP/2
//! connection fails the next request, and the fixture's `expect` turns that into a panic that
//! ends the run. HTTP/3 over QMux has a second failure mode that is worse than an error and is
//! not hypothetical — an exhausted stream allowance, a peer that never opened its control
//! streams, a window nobody extended — where the request future simply never resolves, at
//! either end, with nothing logged. Criterion puts no timeout around a measurement, so an arm
//! that reaches that state does not fail a benchmark run; it wedges one.
//!
//! Two properties keep that from happening, and neither can be established by reading the
//! code, so both are exercised here:
//!
//! - A failed exchange **panics out of the timed closure** rather than being recorded as a
//!   very fast iteration (SC-014). Shown by taking a fixture's server away and asking it for
//!   another exchange.
//! - A workload parameter the configuration will not admit is **refused before it is
//!   offered** (SC-015). Shown by asking for one, and by checking that the refusal is the
//!   harness's own and not something that came back from the wire.
//!
//! Every test that awaits a fixture does so under [`LIMIT`]. That is not belt-and-braces: a
//! test asserting a panic that instead hangs would report as a job that never finished, which
//! is precisely the failure mode this file exists to rule out. Under a timeout it reports as
//! the wrong panic, which is a test failure with a message.

use bytes::Bytes;
use tokio::time::{Duration, timeout};

use ngnet_bench::{
    MAX_CONCURRENT_STREAMS, NgnetQmuxH3, NgnetQmuxH3Socket, body_of, current_thread_runtime,
};

/// How long a fixture may take before a test treats it as hung.
///
/// Generous, because a loaded machine is not a deadlock; bounded, because a deadlock here is
/// exactly what is being ruled out.
const LIMIT: Duration = Duration::from_secs(30);

/// A failed exchange over the duplex arm ends the run rather than reporting a time.
///
/// The server is taken away mid-life, which is the closest a test can get to the thing that
/// actually worries: a connection that was working when the measurement started and is not
/// working now. What must not happen is `round_trip` returning a small number.
#[test]
#[should_panic(expected = "a response head")]
fn a_failed_duplex_exchange_panics() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3::establish());
    fixture.abandon_server();
    runtime.block_on(async {
        timeout(LIMIT, fixture.round_trip(body_of(1024)))
            .await
            .expect("the exchange must resolve one way or the other, not hang")
    });
}

/// The same over the loopback-socket arm, where the peer going away closes a real socket.
///
/// Worth exercising separately from the duplex arm: the two substrates report a vanished peer
/// through different code — an in-memory pipe's end-of-stream against a socket's — and it is
/// the fixture's treatment of the result, not the substrate's, that has to be the same.
#[test]
#[should_panic(expected = "a response head")]
fn a_failed_socket_exchange_panics() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3Socket::establish());
    fixture.abandon_server();
    runtime.block_on(async {
        timeout(LIMIT, fixture.round_trip(body_of(1024)))
            .await
            .expect("the exchange must resolve one way or the other, not hang")
    });
}

/// A concurrency beyond the configured limit is refused rather than offered.
///
/// The message is asserted on, not just the panic: what makes this evidence for SC-015 is that
/// the refusal comes from the harness's own admissibility check. A test that accepted any
/// panic would also pass when the requests were offered and the server reset the ones over its
/// limit — which is the very outcome being prevented, since whether it happens at all depends
/// on how many handlers are in flight at the moment each head arrives.
#[test]
#[should_panic(expected = "exceeds the 128 concurrent exchanges")]
fn an_inadmissible_concurrency_is_refused() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3::establish());
    runtime.block_on(async {
        timeout(
            LIMIT,
            fixture.concurrent(MAX_CONCURRENT_STREAMS as usize + 1),
        )
        .await
        .expect("a refusal, not a wait")
    });
}

/// The refusal happens before anything reaches the connection.
///
/// A check that ran after the requests were queued would still panic, and would still look
/// like this test passing — so the fixture is given no server at all first. If the parameter
/// were offered, the exchange would fail against a dead peer and the panic would name the
/// response head instead; that it names the limit is what places the check ahead of the wire.
#[test]
#[should_panic(expected = "exceeds the 128 concurrent exchanges")]
fn an_inadmissible_concurrency_is_refused_before_anything_is_offered() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3::establish());
    fixture.abandon_server();
    runtime.block_on(async {
        timeout(
            LIMIT,
            fixture.concurrent(MAX_CONCURRENT_STREAMS as usize + 1),
        )
        .await
        .expect("a refusal, not a wait")
    });
}

/// The largest concurrency the benchmark sweep uses, and the largest the configuration
/// admits, both complete.
///
/// The counterpart to the two refusals above: a guard that refused everything would satisfy
/// them and leave the arms unable to measure anything. 64 is the sweep's top point; 128 is the
/// configured limit, and running it here is also the one place the boundary itself is
/// exercised.
#[test]
fn the_admissible_concurrencies_complete() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3::establish());
    for n in [1, 8, 64, MAX_CONCURRENT_STREAMS as usize] {
        runtime.block_on(async {
            timeout(LIMIT, fixture.concurrent(n))
                .await
                .unwrap_or_else(|_| panic!("{n} concurrent exchanges must complete"));
        });
    }
}

/// One connection serves far more exchanges than a Criterion run will ask of it.
///
/// This is the test for the constant that has no other way of being wrong: QMux stream
/// capacity is a lifetime budget that nothing recycles, and the default of 100 would be spent
/// before a single benchmark finished its warm-up. The failure it guards against is not an
/// error but a hang, so the assertion is only that this returns at all — 1,500 sequential
/// exchanges being fifteen times the default and a plausible sample count.
#[test]
fn one_connection_outlives_the_default_stream_allowance() {
    let runtime = current_thread_runtime();
    let fixture = runtime.block_on(NgnetQmuxH3::establish());
    runtime.block_on(async {
        timeout(LIMIT, async {
            for _ in 0..1_500 {
                assert_eq!(fixture.round_trip(Bytes::new()).await, 0);
            }
        })
        .await
        .expect("1,500 exchanges on one connection must complete, not stop part-way")
    });
}
