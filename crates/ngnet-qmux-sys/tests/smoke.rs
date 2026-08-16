//! Proves the bindings match the library that was actually built.
//!
//! A `-sys` crate can compile perfectly and still be wrong: an allowlist that misses a symbol,
//! a struct whose layout disagrees with the C compiler's, a restated macro that has drifted.
//! None of those show up until something tries to use them. These tests call into the built
//! archive so that they show up here instead.

use std::mem;
use std::ptr;

use ngnet_qmux_sys as sys;

/// The version of the vendored dwnx checkout, from its `configure.ac`.
///
/// dwnx is pre-release and has never been tagged, so this is `AC_INIT`'s placeholder rather
/// than a release number. It is pinned anyway: if the submodule moves to a version that
/// changes it, the generated header and this test disagree, which is the point.
const EXPECTED_VERSION: &str = "0.0.0-DEV";

#[test]
fn version_matches_vendored_source() {
    let version = unsafe { std::ffi::CStr::from_ptr(sys::DWNX_VERSION.as_ptr().cast()) };
    // The binding is a byte string including its NUL; compare the text either way.
    let version = version.to_str().expect("version is not valid UTF-8");
    assert_eq!(version, EXPECTED_VERSION);
    assert_eq!(sys::DWNX_VERSION_NUM, 0x000000);
}

#[test]
fn default_settings_are_zero() {
    let mut settings = mem::MaybeUninit::<sys::dwnx_settings>::uninit();
    let settings = unsafe {
        sys::dwnx_settings_default(settings.as_mut_ptr());
        settings.assume_init()
    };

    assert_eq!(settings.conn_id, 0);
    assert_eq!(settings.initial_ts, 0);
    assert!(settings.log_write.is_none());
}

/// The C defaults leave every limit at zero and set only the record size.
///
/// Worth pinning rather than assuming: a connection built from these defaults can carry no
/// application data at all until the limits are raised, which is surprising enough that the
/// safe crate documents it prominently. If upstream ever seeds them differently, this fails.
#[test]
fn default_transport_params_set_only_record_size() {
    let params = default_params();

    assert_eq!(params.initial_max_stream_data_bidi_local, 0);
    assert_eq!(params.initial_max_stream_data_bidi_remote, 0);
    assert_eq!(params.initial_max_stream_data_uni, 0);
    assert_eq!(params.initial_max_data, 0);
    assert_eq!(params.initial_max_streams_bidi, 0);
    assert_eq!(params.initial_max_streams_uni, 0);
    assert_eq!(params.max_idle_timeout, 0);
    assert_eq!(
        u32::try_from(params.max_record_size).unwrap(),
        sys::DWNX_DEFAULT_MAX_RECORD_SIZE
    );
}

/// Constructing and deleting a connection in each role.
///
/// This is the test that proves struct layout at run time. `dwnx_conn_client_new` reads
/// `callbacks`, `settings` and `params` through pointers the C compiler laid out; if bindgen
/// disagreed about a field offset the call would read garbage, and the assertions afterwards
/// would fail or the process would crash.
#[test]
fn client_and_server_connections_construct_and_free() {
    for server in [false, true] {
        let callbacks = callbacks();
        let mut settings = mem::MaybeUninit::<sys::dwnx_settings>::uninit();
        let settings = unsafe {
            sys::dwnx_settings_default(settings.as_mut_ptr());
            settings.assume_init()
        };
        let params = default_params();

        let mut conn: *mut sys::dwnx_conn = ptr::null_mut();
        let rv = unsafe {
            let new = if server {
                sys::dwnx_conn_server_new
            } else {
                sys::dwnx_conn_client_new
            };
            new(
                &mut conn,
                &callbacks,
                &settings,
                &params,
                // Default allocator, and no user data: this test never takes a callback.
                ptr::null(),
                ptr::null_mut(),
            )
        };

        assert_eq!(rv, 0, "constructing a connection failed");
        assert!(!conn.is_null());

        assert_eq!(unsafe { sys::dwnx_conn_is_server(conn) } != 0, server);

        unsafe { sys::dwnx_conn_del(conn) };
    }
}

/// The error helpers are pure functions over the code space, so they can be checked directly.
#[test]
fn error_helpers_agree_with_the_header() {
    // `dwnx_err_is_fatal` is `liberr < DWNX_ERR_FATAL`, which makes the -2xx codes non-fatal
    // and only the two -5xx ones fatal. Pinned because the safe crate cannot rely on this
    // predicate alone to decide whether a connection survives, and the reason why should stay
    // visible.
    assert_eq!(unsafe { sys::dwnx_err_is_fatal(sys::DWNX_ERR_NOMEM) }, 1);
    assert_eq!(
        unsafe { sys::dwnx_err_is_fatal(sys::DWNX_ERR_CALLBACK_FAILURE) },
        1
    );
    assert_eq!(unsafe { sys::dwnx_err_is_fatal(sys::DWNX_ERR_PROTO) }, 0);
    assert_eq!(
        unsafe { sys::dwnx_err_is_fatal(sys::DWNX_ERR_STREAM_DATA_BLOCKED) },
        0
    );

    let text = unsafe { std::ffi::CStr::from_ptr(sys::dwnx_strerror(sys::DWNX_ERR_PROTO)) };
    assert_eq!(text.to_str().unwrap(), "ERR_PROTO");

    assert_eq!(
        unsafe { sys::dwnx_err_infer_quic_transport_error_code(sys::DWNX_ERR_FLOW_CONTROL) },
        u64::from(sys::DWNX_FLOW_CONTROL_ERROR)
    );
}

#[test]
fn stream_id_helper_matches_quic_encoding() {
    // Low bit clear is bidirectional, set is unidirectional -- the QUIC encoding, which QMux
    // reuses unchanged.
    assert_eq!(unsafe { sys::dwnx_is_bidi_stream(0) }, 1);
    assert_eq!(unsafe { sys::dwnx_is_bidi_stream(1) }, 1);
    assert_eq!(unsafe { sys::dwnx_is_bidi_stream(2) }, 0);
    assert_eq!(unsafe { sys::dwnx_is_bidi_stream(3) }, 0);
}

/// The close-reason helpers, which the safe crate wraps directly.
#[test]
fn ccerr_helpers_set_the_expected_type() {
    let mut ccerr = mem::MaybeUninit::<sys::dwnx_ccerr>::uninit();
    let mut ccerr = unsafe {
        sys::dwnx_ccerr_default(ccerr.as_mut_ptr());
        ccerr.assume_init()
    };
    assert_eq!(ccerr.type_, sys::DWNX_CCERR_TYPE_TRANSPORT);

    unsafe { sys::dwnx_ccerr_set_application_error(&mut ccerr, 7, ptr::null(), 0) };
    assert_eq!(ccerr.type_, sys::DWNX_CCERR_TYPE_APPLICATION);
    assert_eq!(ccerr.error_code, 7);

    // The one liberr that maps to a distinct close type rather than to a transport code.
    unsafe { sys::dwnx_ccerr_set_liberr(&mut ccerr, sys::DWNX_ERR_IDLE_CLOSE, ptr::null(), 0) };
    assert_eq!(ccerr.type_, sys::DWNX_CCERR_TYPE_IDLE_CLOSE);
}

/// Pins the constants `wrapper.h` restates, and their widths.
///
/// The restatements exist because bindgen silently drops dwnx's cast-style macros. A
/// `_Static_assert` in `wrapper.h` already pins each *value* against the header at compile
/// time; what it cannot pin is the Rust type bindgen infers, which follows the literal's
/// magnitude rather than `dwnx_duration`. Anything doing arithmetic in these units needs to
/// know that, so it is asserted here.
#[test]
fn restated_constants_have_the_expected_values_and_widths() {
    assert_eq!(u64::from(sys::NGNET_QMUX_NANOSECONDS), 1);
    assert_eq!(u64::from(sys::NGNET_QMUX_MICROSECONDS), 1_000);
    assert_eq!(u64::from(sys::NGNET_QMUX_MILLISECONDS), 1_000_000);
    assert_eq!(u64::from(sys::NGNET_QMUX_SECONDS), 1_000_000_000);
    assert_eq!(sys::NGNET_QMUX_MINUTES, 60_000_000_000);
    assert_eq!(sys::NGNET_QMUX_MAX_VARINT, (1 << 62) - 1);

    // bindgen sizes each constant to its literal, so the smaller units come out narrower than
    // the `dwnx_duration` they represent. A caller must widen before multiplying.
    assert_eq!(mem::size_of_val(&sys::NGNET_QMUX_NANOSECONDS), 4);
    assert_eq!(mem::size_of_val(&sys::NGNET_QMUX_MINUTES), 8);
}

/// Names every public dwnx function so an allowlist regression fails here.
///
/// Without this, dropping a symbol from the bindgen allowlist -- or upstream renaming one --
/// surfaces as a confusing error inside the safe crate rather than as a failure in the crate
/// that owns the bindings.
#[test]
fn every_public_function_is_reachable() {
    let _: [*const (); 33] = [
        sys::dwnx_mem_default as *const (),
        sys::dwnx_transport_params_default as *const (),
        sys::dwnx_settings_default as *const (),
        sys::dwnx_conn_server_new as *const (),
        sys::dwnx_conn_client_new as *const (),
        sys::dwnx_conn_del as *const (),
        sys::dwnx_conn_read as *const (),
        sys::dwnx_conn_extend_max_stream_offset as *const (),
        sys::dwnx_conn_extend_max_offset as *const (),
        sys::dwnx_conn_extend_max_streams_bidi as *const (),
        sys::dwnx_conn_extend_max_streams_uni as *const (),
        sys::dwnx_conn_open_bidi_stream as *const (),
        sys::dwnx_conn_open_uni_stream as *const (),
        sys::dwnx_conn_shutdown_stream as *const (),
        sys::dwnx_conn_shutdown_stream_write as *const (),
        sys::dwnx_conn_shutdown_stream_read as *const (),
        sys::dwnx_conn_get_streams_bidi_left as *const (),
        sys::dwnx_conn_get_streams_uni_left as *const (),
        sys::dwnx_conn_writev_stream as *const (),
        sys::dwnx_conn_write_stream as *const (),
        sys::dwnx_conn_is_local_stream as *const (),
        sys::dwnx_conn_is_server as *const (),
        sys::dwnx_conn_get_timestamp as *const (),
        sys::dwnx_conn_get_max_data_left as *const (),
        sys::dwnx_conn_get_local_transport_params as *const (),
        sys::dwnx_strerror as *const (),
        sys::dwnx_err_is_fatal as *const (),
        sys::dwnx_err_infer_quic_transport_error_code as *const (),
        sys::dwnx_ccerr_default as *const (),
        sys::dwnx_ccerr_set_transport_error as *const (),
        sys::dwnx_ccerr_set_liberr as *const (),
        sys::dwnx_ccerr_set_application_error as *const (),
        sys::dwnx_is_bidi_stream as *const (),
    ];
}

/// The callback table has twelve members, each an `Option<unsafe extern "C" fn>`.
#[test]
fn callback_table_is_fully_optional() {
    let callbacks = callbacks();

    assert!(callbacks.recv_transport_params.is_none());
    assert!(callbacks.recv_stream_data.is_none());
    assert!(callbacks.stream_open.is_none());
    assert!(callbacks.stream_close.is_none());
    assert!(callbacks.stream_reset.is_none());
    assert!(callbacks.stream_stop_sending.is_none());
    assert!(callbacks.recv_stop_sending.is_none());
    assert!(callbacks.extend_max_stream_data.is_none());
    assert!(callbacks.extend_max_local_streams_bidi.is_none());
    assert!(callbacks.extend_max_local_streams_uni.is_none());
    assert!(callbacks.extend_max_remote_streams_bidi.is_none());
    assert!(callbacks.extend_max_remote_streams_uni.is_none());
}

fn callbacks() -> sys::dwnx_callbacks {
    // `Default` is derived by bindgen; all twelve members are null function pointers.
    sys::dwnx_callbacks::default()
}

fn default_params() -> sys::dwnx_transport_params {
    let mut params = mem::MaybeUninit::<sys::dwnx_transport_params>::uninit();
    unsafe {
        sys::dwnx_transport_params_default(params.as_mut_ptr());
        params.assume_init()
    }
}
