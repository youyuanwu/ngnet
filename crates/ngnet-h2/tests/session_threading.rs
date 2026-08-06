//! A session may be moved between threads, but never shared (Spec FR-004, SC-009).
//!
//! This test lives in `tests/` rather than `src/` because it uses `std::thread`, which
//! the SC-021 source scan forbids in the crate's own source.
//!
//! The negative half — that a session reference cannot be *shared* across threads — is a
//! compile-fail case, added with the rest of the `trybuild` suite in a later phase.

use ngnet_h2::{Session, SessionBuilder, Setting};

/// Drains everything a session currently wants to transmit.
fn drain(session: &mut Session<()>) -> Vec<u8> {
    let mut wire = Vec::new();
    while let Some(block) = session.send(&mut ()).expect("send failed") {
        wire.extend_from_slice(block);
    }
    wire
}

#[test]
fn a_session_can_be_built_on_one_thread_and_used_on_another() {
    let mut session = SessionBuilder::<()>::client()
        .setting(Setting::MaxConcurrentStreams(5))
        .build()
        .expect("failed to build session");

    // Use it a little on this thread first, so the move carries real state rather than a
    // freshly constructed object.
    let first = session
        .send(&mut ())
        .expect("send failed")
        .expect("a fresh client has the preface pending")
        .to_vec();
    assert!(first.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));

    let handle = std::thread::spawn(move || {
        let remainder = drain(&mut session);
        // The session is dropped here, on the second thread, which also exercises
        // teardown-after-move. The debug assertion in `Drop` checks it leaked nothing.
        remainder
    });

    let remainder = handle.join().expect("worker thread panicked");
    assert!(
        !remainder.is_empty(),
        "the SETTINGS frame should still have been pending after the move"
    );
}

#[test]
fn sessions_moved_to_threads_are_independent() {
    let workers: Vec<_> = (0..8)
        .map(|index| {
            let mut session = SessionBuilder::<()>::client()
                .setting(Setting::MaxConcurrentStreams(index))
                .build()
                .expect("failed to build session");

            std::thread::spawn(move || drain(&mut session))
        })
        .collect();

    for worker in workers {
        let wire = worker.join().expect("worker thread panicked");
        assert!(wire.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
    }
}

/// `Session` must be `Send` so it can move between threads, and must NOT be `Sync` so it
/// cannot be shared. This asserts the positive half statically; the negative half is a
/// compile-fail case.
#[test]
fn session_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Session<()>>();
    assert_send::<Session<Vec<u8>>>();
}
