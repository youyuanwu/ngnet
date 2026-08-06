//! End-to-end check that the generated bindings can drive a real HTTP/2
//! session. Uses the memory-based send API, so no sockets and no TLS are
//! involved -- exactly the cleartext (h2c) shape this repo targets.

use std::ptr;

use ngnet_h2_sys::*;

/// The client connection preface that opens every h2c connection.
const CLIENT_MAGIC: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[test]
fn client_session_emits_h2c_preface() {
    unsafe {
        let mut callbacks: *mut nghttp2_session_callbacks = ptr::null_mut();
        assert_eq!(nghttp2_session_callbacks_new(&mut callbacks), 0);
        assert!(!callbacks.is_null());

        let mut session: *mut nghttp2_session = ptr::null_mut();
        assert_eq!(
            nghttp2_session_client_new(&mut session, callbacks, ptr::null_mut()),
            0
        );
        assert!(!session.is_null());

        // Callbacks are copied into the session, so they can be freed now.
        nghttp2_session_callbacks_del(callbacks);

        let settings = [nghttp2_settings_entry {
            settings_id: NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS as i32,
            value: 100,
        }];
        assert_eq!(
            nghttp2_submit_settings(
                session,
                NGHTTP2_FLAG_NONE as u8,
                settings.as_ptr(),
                settings.len(),
            ),
            0
        );

        // Drain everything nghttp2 wants to put on the wire.
        let mut wire = Vec::new();
        loop {
            let mut data: *const u8 = ptr::null();
            let len = nghttp2_session_mem_send2(session, &mut data);
            assert!(len >= 0, "nghttp2_session_mem_send2 failed: {len}");
            if len == 0 {
                break;
            }
            wire.extend_from_slice(std::slice::from_raw_parts(data, len as usize));
        }

        nghttp2_session_del(session);

        assert!(
            wire.starts_with(CLIENT_MAGIC),
            "expected the h2c client preface, got {:?}",
            &wire[..wire.len().min(24)]
        );

        // A SETTINGS frame must immediately follow the preface: a 9 byte header
        // whose type byte (offset 3) is SETTINGS.
        let frame = &wire[CLIENT_MAGIC.len()..];
        assert!(frame.len() >= 9, "SETTINGS frame is missing");
        assert_eq!(frame[3], NGHTTP2_SETTINGS as u8, "expected a SETTINGS frame");
    }
}

#[test]
fn library_reports_expected_version() {
    unsafe {
        let info = nghttp2_version(0);
        assert!(!info.is_null());
        let version = std::ffi::CStr::from_ptr((*info).version_str);
        assert_eq!(version.to_str().unwrap(), "1.70.0");
    }
}
