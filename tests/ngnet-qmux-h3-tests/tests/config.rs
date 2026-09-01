//! The configuration-taking entry points, and what a connection built through them advertises.
//!
//! This file exists because a configuration passthrough is easy to write and easy to write
//! *wrongly* in a way nothing notices: a value that is accepted, stored, and then not sent
//! leaves every exchange working exactly as it did, so a test that only checks an exchange
//! succeeds would pass over a passthrough that dropped its argument on the floor. Every
//! assertion here is therefore made against what the **peer** received, not against what this
//! end was told.
//!
//! # Why there is a bare QMux connection in a test about HTTP/3
//!
//! [`ngnet_qmux_h3::QmuxConnection`] exposes nothing about the connection underneath it —
//! `docs/qmux-h3/pending-work.md` records that as "the connection is not observable" — so
//! there is no accessor to assert against, and adding one is a separate question from this
//! one. But a transport parameter is by definition something the *other* end reads, and the
//! other end need not be an HTTP/3 one: a plain [`ngnet_qmux::io::Connection`] opposite the
//! subject sees exactly the bytes a real peer would and hands them over as
//! [`Event::PeerTransportParams`]. That is a stronger assertion than any accessor would give,
//! because it is made on the received parameters rather than on the configured ones.
//!
//! The same observer serves for the HTTP/3 settings, which are not transport parameters at
//! all: they travel as a SETTINGS frame on the control stream, which arrives as ordinary
//! stream data that this file decodes.
//!
//! # What is pinned here that is not a property of this crate's own code
//!
//! Two of these tests describe defects rather than features, and they assert the defective
//! behaviour on purpose, so that a later change to it is a test failure and a decision rather
//! than a silent difference:
//!
//! - QMux stream capacity is never recycled and this crate never extends it, so a connection
//!   admits exactly as many client-opened streams as the server's `max_streams_bidi` allowed,
//!   and the next request **hangs** rather than failing.
//! - A stream allowance above dwnx's `DWNX_MAX_STREAMS` (`1 << 60`) is not rejected where it
//!   is configured — it is rejected by the peer decoding it, which fails the connection during
//!   setup.

use core::future::poll_fn;
use std::collections::HashMap;

use bytes::Bytes;
use ngnet_qmux::io::testing::{TestByteStream, TestClock};
use ngnet_qmux::io::{Connection as LayerConnection, Event};
use ngnet_qmux::{Duration as IdleTimeout, TransportParams};
use ngnet_qmux_h3::{HttpConfig, TransportConfig};
use ngnet_qmux_h3_tests::{
    LIMIT, Payload, drain, get, memory_pair, memory_pair_with, memory_streams, ok, pattern, post,
};
use tokio::task::LocalSet;
use tokio::time::{Duration, timeout};

/// A transport configuration in which no field is left at its default.
///
/// The values are the ones the cross-protocol benchmark work needs — 65535 of credit in both
/// places, because that is what libnghttp2 fixes an HTTP/2 connection at — but what matters
/// for these tests is only that every one of them differs from the default, so an assertion
/// that finds a default value has found a value that did not travel.
fn distinctive_transport() -> TransportConfig {
    TransportConfig::new()
        .initial_max_stream_data(65_535)
        .initial_max_data(65_535)
        .max_streams_bidi(4_096)
        .max_streams_uni(7)
        .max_idle_timeout(IdleTimeout::from_secs(90))
}

/// An HTTP/3 configuration in which every setting that reaches the wire differs from its
/// default.
///
/// Three of the five fields on `ngnet_h3::http::Config` become SETTINGS; the other two —
/// `max_concurrent_streams` and `events_per_pass` — never leave the end that holds them, so
/// they are not what these tests assert on.
fn distinctive_http() -> HttpConfig {
    HttpConfig::default()
        .max_field_section_size(4_321)
        .qpack_max_dtable_capacity(777)
        .qpack_blocked_streams(9)
}

/// Runs a bare QMux end until the subject opposite it announces its transport parameters.
///
/// The observer is an ordinary connection with ordinary defaults; it is a peer, not an
/// instrument. Its manual executor forces buffered output before suspending.
async fn parameters_announced_to(
    mut observer: LayerConnection<TestByteStream, TestClock>,
) -> TransportParams {
    timeout(LIMIT, async move {
        loop {
            let event = poll_fn(|cx| match observer.poll_next_event(cx) {
                core::task::Poll::Pending => match observer.poll_pump(cx) {
                    core::task::Poll::Ready(Err(error)) => core::task::Poll::Ready(Err(error)),
                    core::task::Poll::Ready(Ok(())) | core::task::Poll::Pending => {
                        core::task::Poll::Pending
                    }
                },
                ready => ready,
            })
            .await
            .expect("the observer's connection must not fail");
            if let Event::PeerTransportParams(params) = event {
                return params;
            }
        }
    })
    .await
    .expect("the transport parameters must arrive")
}

/// Decodes one variable-length integer, advancing `bytes` past it.
///
/// HTTP/3 and QMux share QUIC's encoding: the top two bits of the first byte give the length
/// as a power of two, and the remaining six bits are the value's most significant ones.
/// Returns `None` when the buffer holds less than a whole integer, which is how the caller
/// tells "not yet arrived" from "malformed".
fn varint(bytes: &mut &[u8]) -> Option<u64> {
    let first = *bytes.first()?;
    let length = 1usize << (first >> 6);
    if bytes.len() < length {
        return None;
    }
    let mut value = u64::from(first & 0x3f);
    for byte in &bytes[1..length] {
        value = (value << 8) | u64::from(*byte);
    }
    *bytes = &bytes[length..];
    Some(value)
}

/// The HTTP/3 settings carried by a control stream's first frame, by setting identifier.
///
/// `bytes` is the stream from its very beginning: a stream type, then frames. Returns `None`
/// while the SETTINGS frame is still incomplete, so a caller can keep reading and try again.
/// RFC 9114 requires SETTINGS to be the first frame on the control stream, so no frame needs
/// to be skipped to find it.
fn settings_frame(bytes: &[u8]) -> Option<HashMap<u64, u64>> {
    let mut rest = bytes;
    let stream_type = varint(&mut rest)?;
    assert_eq!(stream_type, 0x00, "this must be the control stream");
    let frame_type = varint(&mut rest)?;
    assert_eq!(
        frame_type, 0x04,
        "SETTINGS must be the first frame on the control stream"
    );
    let length = usize::try_from(varint(&mut rest)?).expect("a frame length that fits");
    if rest.len() < length {
        return None;
    }
    let mut payload = &rest[..length];
    let mut settings = HashMap::new();
    while !payload.is_empty() {
        let identifier = varint(&mut payload).expect("a whole setting identifier");
        let value = varint(&mut payload).expect("a whole setting value");
        settings.insert(identifier, value);
    }
    Some(settings)
}

/// Runs a bare QMux end until the subject opposite it sends its HTTP/3 SETTINGS.
///
/// Stream data is accumulated per stream rather than assumed to arrive whole: it does arrive
/// whole today, in the one record the subject writes first, but a test that depended on that
/// would be pinning the record boundaries rather than the settings.
async fn settings_announced_to(
    mut observer: LayerConnection<TestByteStream, TestClock>,
) -> HashMap<u64, u64> {
    timeout(LIMIT, async move {
        let mut streams: HashMap<i64, Vec<u8>> = HashMap::new();
        loop {
            let event = poll_fn(|cx| match observer.poll_next_event(cx) {
                core::task::Poll::Pending => match observer.poll_pump(cx) {
                    core::task::Poll::Ready(Err(error)) => core::task::Poll::Ready(Err(error)),
                    core::task::Poll::Ready(Ok(())) | core::task::Poll::Pending => {
                        core::task::Poll::Pending
                    }
                },
                ready => ready,
            })
            .await
            .expect("the observer's connection must not fail");
            if let Event::StreamData {
                stream_id, data, ..
            } = event
            {
                let buffered = streams.entry(stream_id.get()).or_default();
                buffered.extend_from_slice(&data);
                // Only the control stream begins with a zero; the QPACK encoder and decoder
                // streams announce themselves as 2 and 3.
                if buffered.first() == Some(&0x00)
                    && let Some(settings) = settings_frame(buffered)
                {
                    return settings;
                }
            }
        }
    })
    .await
    .expect("the HTTP/3 settings must arrive")
}

/// SETTINGS_QPACK_MAX_TABLE_CAPACITY, RFC 9204.
const QPACK_MAX_TABLE_CAPACITY: u64 = 0x01;
/// SETTINGS_MAX_FIELD_SECTION_SIZE, RFC 9114.
const MAX_FIELD_SECTION_SIZE: u64 = 0x06;
/// SETTINGS_QPACK_BLOCKED_STREAMS, RFC 9204.
const QPACK_BLOCKED_STREAMS: u64 = 0x07;

#[tokio::test]
async fn a_connection_built_with_a_supplied_configuration_exchanges_bodies_both_ways() {
    // The ordinary case first: whatever else the configuration does, a connection built with
    // one has to work. The body is deliberately larger than the credit either end granted, so
    // this also shows the passthrough did not break the credit extensions the join makes as it
    // consumes bytes -- a connection whose window was narrowed and whose window updates had
    // stopped would stall here rather than complete.
    LocalSet::new()
        .run_until(async {
            let body = pattern(256 * 1024);
            let echoed = body.clone();
            let sender = memory_pair_with(
                move |request| {
                    let echoed = echoed.clone();
                    async move {
                        let received = drain(request.into_body()).await.expect("the request body");
                        assert_eq!(received, echoed, "the request body must arrive whole");
                        ok(echoed)
                    }
                },
                distinctive_transport(),
                distinctive_http(),
            );

            let response = timeout(
                LIMIT,
                sender.send_request(post("https://qmux.test/echo", body.clone())),
            )
            .await
            .expect("the request must not hang")
            .expect("a response");
            assert_eq!(response.status(), 200);

            let received = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");
            assert_eq!(received, body, "the response body must arrive whole");
        })
        .await;
}

#[tokio::test]
async fn a_server_advertises_the_transport_configuration_it_was_given() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let server = ngnet_qmux_h3::serve_with(
                server_io,
                clock.clone(),
                |_request| async move { ok("configured") },
                distinctive_transport(),
                HttpConfig::default(),
            )
            .expect("serving");
            tokio::task::spawn_local(async move {
                let _ = server.await;
            });

            let observer = LayerConnection::client(client_io, clock, TransportConfig::new())
                .expect("an observing peer");
            let params = parameters_announced_to(observer).await;

            // One `initial_max_stream_data` on the configuration becomes all three per-stream
            // limits on the wire, so all three are checked: a passthrough that reached only
            // one of them would leave a body stalling in the direction nobody tested.
            assert_eq!(params.initial_max_stream_data_bidi_local(), 65_535);
            assert_eq!(params.initial_max_stream_data_bidi_remote(), 65_535);
            assert_eq!(params.initial_max_stream_data_uni(), 65_535);
            assert_eq!(params.initial_max_data(), 65_535);
            assert_eq!(params.initial_max_streams_bidi(), 4_096);
            assert_eq!(params.initial_max_streams_uni(), 7);
            assert_eq!(
                params.max_idle_timeout(),
                IdleTimeout::from_secs(90),
                "the idle timeout is advertised, whether or not anything enforces it"
            );
        })
        .await;
}

#[tokio::test]
async fn a_client_advertises_the_transport_configuration_it_was_given() {
    // The mirror of the test above, and not a formality: the client and the server reach the
    // layer below by two different calls, so a passthrough can be complete on one side and
    // absent on the other.
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let (sender, connection) = ngnet_qmux_h3::connect_with::<_, _, Payload>(
                client_io,
                clock.clone(),
                distinctive_transport(),
                HttpConfig::default(),
            )
            .expect("starting the client");
            tokio::task::spawn_local(async move {
                let _ = connection.await;
            });
            // Held rather than dropped: a client whose last handle goes away winds its
            // connection down, and this one has to stay up long enough to be observed.
            let _sender = sender;

            let observer = LayerConnection::server(server_io, clock, TransportConfig::new())
                .expect("an observing peer");
            let params = parameters_announced_to(observer).await;

            assert_eq!(params.initial_max_stream_data_bidi_local(), 65_535);
            assert_eq!(params.initial_max_stream_data_bidi_remote(), 65_535);
            assert_eq!(params.initial_max_stream_data_uni(), 65_535);
            assert_eq!(params.initial_max_data(), 65_535);
            assert_eq!(params.initial_max_streams_bidi(), 4_096);
            assert_eq!(params.initial_max_streams_uni(), 7);
        })
        .await;
}

#[tokio::test]
async fn the_defaulting_entry_points_advertise_what_they_always_did() {
    // The other half of the passthrough's obligation, and the one a caller who never touches a
    // configuration depends on. The numbers are written out rather than read from
    // `ngnet_qmux::io::DEFAULT_*` deliberately: the point is that these particular values are
    // what a connection has always advertised, which a test that recomputed them from the
    // constants would stop checking the moment a constant moved.
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let server =
                ngnet_qmux_h3::serve(
                    server_io,
                    clock.clone(),
                    |_request| async move { ok("default") },
                )
                .expect("serving");
            tokio::task::spawn_local(async move {
                let _ = server.await;
            });

            let observer = LayerConnection::client(client_io, clock, TransportConfig::new())
                .expect("an observing peer");
            let params = parameters_announced_to(observer).await;

            assert_eq!(params.initial_max_stream_data_bidi_local(), 256 * 1024);
            assert_eq!(params.initial_max_stream_data_bidi_remote(), 256 * 1024);
            assert_eq!(params.initial_max_stream_data_uni(), 256 * 1024);
            assert_eq!(params.initial_max_data(), 1024 * 1024);
            assert_eq!(params.initial_max_streams_bidi(), 100);
            assert_eq!(params.initial_max_streams_uni(), 100);
            assert_eq!(params.max_idle_timeout(), IdleTimeout::from_nanos(0));
        })
        .await;
}

#[tokio::test]
async fn the_http3_configuration_reaches_the_connections_settings() {
    // The half of the passthrough that is easiest to leave out, because the HTTP/3 defaults
    // happen to equal the HTTP/2 ones this workspace compares against: an entry point that
    // kept calling the defaulting `handshake` and `serve` would look right in every exchange
    // and would be wrong the moment either crate changed a default. Asserting on the SETTINGS
    // frame is what makes "the configuration was used" a fact rather than a coincidence.
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let server = ngnet_qmux_h3::serve_with(
                server_io,
                clock.clone(),
                |_request| async move { ok("configured") },
                TransportConfig::new(),
                distinctive_http(),
            )
            .expect("serving");
            tokio::task::spawn_local(async move {
                let _ = server.await;
            });

            let observer = LayerConnection::client(client_io, clock, TransportConfig::new())
                .expect("an observing peer");
            let settings = settings_announced_to(observer).await;

            assert_eq!(settings.get(&MAX_FIELD_SECTION_SIZE), Some(&4_321));
            assert_eq!(settings.get(&QPACK_MAX_TABLE_CAPACITY), Some(&777));
            assert_eq!(settings.get(&QPACK_BLOCKED_STREAMS), Some(&9));
        })
        .await;
}

/// How long a request that is never going to complete is given before it is called a hang.
///
/// Short on purpose, and short is safe here only because the exchanges before it have already
/// shown the connection working: this wait is not measuring whether the machine is slow, it is
/// waiting on an open that no code path will ever grant.
const BRIEF: Duration = Duration::from_millis(500);

#[tokio::test]
async fn a_small_stream_allowance_is_spent_once_and_never_returned() {
    // A known defect, asserted so that fixing it is a decision. Stream capacity is not
    // recycled when a stream closes -- neither dwnx nor `ngnet-qmux` returns it, and
    // `ngnet-qmux-h3` never calls `extend_stream_limit` -- so the allowance is a lifetime
    // budget rather than a concurrency limit, even for requests made strictly one at a time.
    //
    // What makes it worth a test of its own is how it ends: not with an error, which a caller
    // could handle, but with a request future that never resolves.
    const ALLOWANCE: u64 = 3;

    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            // Asymmetric on purpose: what bounds a client's opens is what the *server*
            // advertised, so only the server's configuration is narrowed here.
            let server = ngnet_qmux_h3::serve_with(
                server_io,
                clock.clone(),
                |_request| async move { ok("spent") },
                TransportConfig::new().max_streams_bidi(ALLOWANCE),
                HttpConfig::default(),
            )
            .expect("serving");
            tokio::task::spawn_local(async move {
                let _ = server.await;
            });
            let (sender, connection) =
                ngnet_qmux_h3::connect::<_, _, Payload>(client_io, clock).expect("the client");
            tokio::task::spawn_local(async move {
                let _ = connection.await;
            });

            for attempt in 0..ALLOWANCE {
                let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
                    .await
                    .unwrap_or_else(|_| panic!("request {attempt} must not hang"))
                    .unwrap_or_else(|error| panic!("request {attempt} failed: {error}"));
                assert_eq!(response.status(), 200);
                let body = timeout(LIMIT, drain(response.into_body()))
                    .await
                    .expect("the body must not hang")
                    .expect("a body");
                assert_eq!(body, Bytes::from_static(b"spent"));
            }

            // The assertion is on the hang itself, which is why it is a bounded wait and not a
            // request this test sits behind: the failure being pinned is precisely that no
            // answer arrives, so the test has to state how long "no answer" is.
            let beyond = timeout(BRIEF, sender.send_request(get("https://qmux.test/"))).await;
            assert!(
                beyond.is_err(),
                "the request past the allowance must hang rather than complete or fail, and it \
                 resolved instead: {:?}",
                beyond.map(|outcome| outcome.map(|response| response.status()))
            );
        })
        .await;
}

#[tokio::test]
async fn a_stream_allowance_at_the_transport_maximum_is_accepted() {
    // The upper bound, from below. `DWNX_MAX_STREAMS` is `1 << 60`
    // (`crates/ngnet-qmux-sys/vendor/dwnx/lib/dwnx_transport_params.h:63`) and it is
    // inclusive, which is worth pinning
    // beside the test that a value above it fails: without this one, that test would be
    // consistent with the whole region near the ceiling being unusable.
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair_with(
                |_request| async move { ok("at the ceiling") },
                TransportConfig::new()
                    .max_streams_bidi(1 << 60)
                    .max_streams_uni(1 << 60),
                HttpConfig::default(),
            );

            let response = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
                .await
                .expect("the request must not hang")
                .expect("a response");
            assert_eq!(response.status(), 200);
        })
        .await;
}

#[tokio::test]
async fn a_stream_allowance_above_the_transport_maximum_fails_the_connection() {
    // The other known defect worth stating: a value above `DWNX_MAX_STREAMS` is not rejected
    // where it is configured. It is below the variable-length integer bound that
    // `TransportParams::validate` checks, so the entry point accepts it and the connection is
    // built; the peer is what rejects it, when it decodes the parameters and answers
    // `DWNX_ERR_MALFORMED_TRANSPORT_PARAM`. So the failure arrives during setup, on the other
    // end, and reaches this end as a connection that has gone away.
    //
    // Asserted at the granularity of "fails, promptly" rather than on a particular error kind:
    // the observed shapes on this host are a request that resolves to `ErrorKind::Closed` and a
    // client driver that resolves to a transport error naming a protocol violation, and neither
    // is something this crate chooses.
    const OVER: u64 = (1 << 60) + 1;

    LocalSet::new()
        .run_until(async {
            let (client_io, server_io, clock) = memory_streams();
            let server = ngnet_qmux_h3::serve_with(
                server_io,
                clock.clone(),
                |_request| async move { ok("never answered") },
                TransportConfig::new().max_streams_bidi(OVER),
                HttpConfig::default(),
            )
            .expect("the entry point accepts it; the peer is what refuses it");
            tokio::task::spawn_local(async move {
                let _ = server.await;
            });

            let (sender, connection) =
                ngnet_qmux_h3::connect::<_, _, Payload>(client_io, clock).expect("the client");
            let driving = tokio::task::spawn_local(connection);

            let outcome = timeout(LIMIT, sender.send_request(get("https://qmux.test/")))
                .await
                .expect("the request must fail rather than hang");
            assert!(
                outcome.is_err(),
                "an allowance above the transport maximum must not yield a working connection"
            );

            let driven = timeout(LIMIT, driving)
                .await
                .expect("the client driver must resolve rather than hang")
                .expect("the client driver task");
            assert!(
                driven.is_err(),
                "the connection must fail during setup, not merely refuse the request"
            );
        })
        .await;
}

#[tokio::test]
async fn the_defaulting_entry_points_still_exchange() {
    // The existing entry points, exercised through the harness helper the existing test files
    // use, so that "nothing changed for a caller who passes no configuration" is checked here
    // as well as by those files compiling and passing untouched.
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair(|request| async move {
                let received = drain(request.into_body()).await.expect("the request body");
                assert_eq!(received.as_ref(), b"unchanged");
                ok(Bytes::from_static(b"unchanged"))
            });

            let response = timeout(
                LIMIT,
                sender.send_request(post("https://qmux.test/unchanged", "unchanged")),
            )
            .await
            .expect("the request must not hang")
            .expect("a response");
            assert_eq!(response.status(), 200);

            let body = timeout(LIMIT, drain(response.into_body()))
                .await
                .expect("the body must not hang")
                .expect("a body");
            assert_eq!(body.as_ref(), b"unchanged");
        })
        .await;
}
