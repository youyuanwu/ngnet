#![cfg(feature = "http")]
//! Retaining a buffer this crate cannot name, without copying it.
//!
//! nghttp3 borrows outgoing body buffers rather than copying them, so the async layer has to
//! hand it the caller's own allocation. The caller's allocation is a [`bytes::Bytes`], and
//! this crate has no `bytes` dependency to name in the core's types — hence
//! [`RetainedBytes::from_owner`], and hence this suite.
//!
//! The assertions are about *addresses*, not about bytes. Comparing contents would pass just
//! as well for a copy, which is exactly the thing that must not happen: a copy here would
//! make the zero-copy claim false while every functional test still went green.

use ngnet_h3::RetainedBytes;
use ngnet_h3::http::testing::bytes_crate::Bytes;

#[test]
fn a_bytes_is_retained_without_being_copied() {
    let source = Bytes::from_static(b"hello world");
    let address = source.as_ptr();

    let retained = RetainedBytes::from_owner(source);

    assert_eq!(retained.as_slice(), b"hello world");
    assert!(
        std::ptr::eq(retained.as_slice().as_ptr(), address),
        "the buffer was copied; the zero-copy body path is not zero-copy"
    );
}

#[test]
fn an_owned_bytes_keeps_its_allocation_alive() {
    // The retained handle must own the buffer, not borrow it: nghttp3 reads through the
    // address long after the call that offered it, and the caller's `Bytes` may be dropped
    // in between.
    let address;
    let retained = {
        let source = Bytes::from(vec![7u8; 4096]);
        address = source.as_ptr();
        RetainedBytes::from_owner(source)
    };

    assert!(std::ptr::eq(retained.as_slice().as_ptr(), address));
    assert!(retained.as_slice().iter().all(|byte| *byte == 7));
}

#[test]
fn splitting_a_retained_bytes_copies_neither_half() {
    // One allocation can span several of the vectors nghttp3 asks for, so the split has to
    // stay inside the original buffer or the release accounting would be tracking copies.
    let source = Bytes::from_static(b"hello world");
    let address = source.as_ptr();

    let mut retained = RetainedBytes::from_owner(source);
    let head = retained.split_to(5);

    assert_eq!(head.as_slice(), b"hello");
    assert_eq!(retained.as_slice(), b" world");
    assert!(std::ptr::eq(head.as_slice().as_ptr(), address));
    assert!(std::ptr::eq(
        retained.as_slice().as_ptr(),
        address.wrapping_add(5)
    ));
}

#[test]
fn a_slice_of_a_bytes_is_retained_at_its_own_offset() {
    // The common case in a body: the adapter hands over a window into a larger buffer the
    // caller still holds.
    let whole = Bytes::from_static(b"0123456789");
    let window = whole.slice(3..7);
    let address = window.as_ptr();

    let retained = RetainedBytes::from_owner(window);

    assert_eq!(retained.as_slice(), b"3456");
    assert!(std::ptr::eq(retained.as_slice().as_ptr(), address));
}

#[test]
fn an_empty_buffer_is_retainable() {
    let retained = RetainedBytes::from_owner(Bytes::new());
    assert!(retained.is_empty());
    assert_eq!(retained.len(), 0);
}

#[test]
fn the_owned_constructor_still_works_alongside_the_erased_one() {
    // `new` is what the crate's own `FixedBody` uses and it stays flat; adding the erased
    // case must not have disturbed it.
    let mut retained = RetainedBytes::new(b"hello world".to_vec());
    let head = retained.split_to(5);
    assert_eq!(head.as_slice(), b"hello");
    assert_eq!(retained.as_slice(), b" world");
}

#[test]
fn a_retained_handle_can_be_cloned_without_copying() {
    let source = Bytes::from_static(b"shared");
    let address = source.as_ptr();

    let retained = RetainedBytes::from_owner(source);
    let clone = retained.clone();

    assert!(std::ptr::eq(clone.as_slice().as_ptr(), address));
    assert!(std::ptr::eq(retained.as_slice().as_ptr(), address));
}
