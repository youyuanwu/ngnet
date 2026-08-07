//! End-to-end check that the generated bindings can drive a real HTTP/3
//! connection object. No QUIC and no TLS are involved: nghttp3 produces the
//! bytes that *would* be written to QUIC streams, which is exactly the sans-I/O
//! shape the safe wrapper is built on.

use std::mem::MaybeUninit;
use std::ptr;

use ngnet_h3_sys::*;

/// The vendored submodule this crate builds against.
const EXPECTED_VERSION: &str = "1.18.0";

#[test]
fn reports_the_vendored_version() {
    unsafe {
        let info = nghttp3_version(0);
        assert!(!info.is_null());
        let version = std::ffi::CStr::from_ptr((*info).version_str)
            .to_str()
            .expect("version string is not UTF-8");
        assert_eq!(
            version, EXPECTED_VERSION,
            "linked against a different nghttp3 than the vendored submodule"
        );
    }
}

/// Drives a client connection far enough to emit its control-stream preface.
///
/// This exercises, in one test, every part of the build that could plausibly be
/// wrong: the versioned constructor macros expanded to the right struct
/// versions, the callbacks and settings structs have the layout the headers
/// describe, and the two-phase send API links and returns sensible values.
#[test]
fn client_connection_emits_control_stream_preface() {
    unsafe {
        // Both structs are versioned; a partially initialised one would carry
        // indeterminate values for fields the library reads.
        let mut callbacks: nghttp3_callbacks = std::mem::zeroed();
        callbacks.rand = Some(fill_with_zeroes);

        let mut settings = MaybeUninit::<nghttp3_settings>::uninit();
        nghttp3_settings_default_versioned(NGHTTP3_SETTINGS_VERSION as i32, settings.as_mut_ptr());
        let settings = settings.assume_init();

        let mut conn: *mut nghttp3_conn = ptr::null_mut();
        let rv = nghttp3_conn_client_new_versioned(
            &mut conn,
            NGHTTP3_CALLBACKS_VERSION as i32,
            &callbacks,
            NGHTTP3_SETTINGS_VERSION as i32,
            &settings,
            ptr::null(),
            ptr::null_mut(),
        );
        assert_eq!(rv, 0, "nghttp3_conn_client_new_versioned failed");
        assert!(!conn.is_null());

        // Client-initiated unidirectional streams: id & 0b11 == 0b10.
        assert_eq!(nghttp3_conn_bind_control_stream(conn, 2), 0);
        assert_eq!(nghttp3_conn_bind_qpack_streams(conn, 6, 10), 0);

        // Phase one of the send transaction: ask what to write.
        let mut vecs = [nghttp3_vec {
            base: ptr::null_mut(),
            len: 0,
        }; 8];
        let mut stream_id: i64 = -1;
        let mut fin: i32 = 0;
        let count = nghttp3_conn_writev_stream(
            conn,
            &mut stream_id,
            &mut fin,
            vecs.as_mut_ptr(),
            vecs.len(),
        );

        assert!(
            count > 0,
            "expected the control stream preface to be queued"
        );
        assert_eq!(
            stream_id, 2,
            "the first thing written should be on the control stream"
        );
        assert_eq!(fin, 0, "the control stream is never finished");

        let written: usize = vecs[..count as usize].iter().map(|v| v.len).sum();
        assert!(written > 0);

        // The control stream opens with its stream type, the varint 0x00.
        assert_eq!(*vecs[0].base, 0x00, "control stream type prefix missing");

        // Phase two: report what the (imaginary) QUIC stack accepted. Omitting
        // this is what stalls a real connection.
        assert_eq!(nghttp3_conn_add_write_offset(conn, stream_id, written), 0);

        nghttp3_conn_del(conn);
    }
}

/// nghttp3 asks for unpredictable bytes when it needs them. Determinism is
/// preferable in a test, and nothing here depends on the values.
unsafe extern "C" fn fill_with_zeroes(dest: *mut u8, destlen: usize) {
    unsafe { ptr::write_bytes(dest, 0, destlen) }
}

/// The symbols the safe wrapper is built on must all be reachable. Naming them
/// here means a bindgen allowlist regression fails this crate's own tests
/// rather than surfacing later as a confusing error in a dependent crate.
#[test]
fn the_symbols_the_wrapper_needs_are_generated() {
    let _: unsafe extern "C" fn(_, _, _, _, _, _, _) -> _ = nghttp3_conn_client_new_versioned;
    let _: unsafe extern "C" fn(_, _, _, _, _, _, _) -> _ = nghttp3_conn_server_new_versioned;
    let _: unsafe extern "C" fn(_, _, _, _, _) -> _ = nghttp3_conn_writev_stream;
    let _: unsafe extern "C" fn(_, _, _) -> _ = nghttp3_conn_add_write_offset;
    let _: unsafe extern "C" fn(_, _, _, _, _, _) -> _ = nghttp3_conn_read_stream2;
    let _: unsafe extern "C" fn(_, _, _) -> _ = nghttp3_conn_add_ack_offset;
    let _: unsafe extern "C" fn(_) -> _ = nghttp3_err_infer_quic_app_error_code;
    let _: unsafe extern "C" fn(_) -> _ = nghttp3_err_is_fatal;
    let _: unsafe extern "C" fn(_) -> _ = nghttp3_rcbuf_get_buf;
    let _: unsafe extern "C" fn(_, _) -> _ = nghttp3_conn_block_stream;
    let _: unsafe extern "C" fn(_, _) -> _ = nghttp3_conn_unblock_stream;
}
