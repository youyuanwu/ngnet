//! Every trait hyperium H3 requires of a transport is implemented, checked at compile time.
//!
//! A missing associated type or a mismatched bound is a compile error rather than a runtime
//! surprise, so these assertions are the cheapest coverage in the crate.

use bytes::Bytes;
use h3::quic;
use h3_ngnet_quic::{BidiStream, Connection, OpenStreams, RecvStream, SendStream};
use ngnet_quic::OsslSession;

type S = OsslSession;

fn implements_connection<C: quic::Connection<Bytes>>() {}
fn implements_open_streams<O: quic::OpenStreams<Bytes>>() {}
fn implements_send_stream<T: quic::SendStream<Bytes>>() {}
fn implements_send_stream_unframed<T: quic::SendStreamUnframed<Bytes>>() {}
fn implements_recv_stream<T: quic::RecvStream>() {}
fn implements_bidi_stream<T: quic::BidiStream<Bytes>>() {}
fn is_send<T: Send>() {}
fn is_sync<T: Sync>() {}
fn is_static<T: 'static>() {}

#[test]
fn every_hyperium_trait_and_associated_type_is_implemented() {
    implements_connection::<Connection<S>>();
    implements_open_streams::<Connection<S>>();
    implements_open_streams::<OpenStreams<S>>();
    implements_send_stream::<SendStream<S>>();
    implements_send_stream_unframed::<SendStream<S>>();
    implements_recv_stream::<RecvStream<S>>();
    implements_send_stream::<BidiStream<S>>();
    implements_send_stream_unframed::<BidiStream<S>>();
    implements_recv_stream::<BidiStream<S>>();
    implements_bidi_stream::<BidiStream<S>>();
}

/// The associated types hyperium threads through must line up with the concrete handles.
#[test]
fn the_associated_types_are_the_concrete_handles() {
    fn assert_connection_types<C>()
    where
        C: quic::Connection<Bytes, RecvStream = RecvStream<S>, OpenStreams = OpenStreams<S>>,
    {
    }
    fn assert_opener_types<O>()
    where
        O: quic::OpenStreams<Bytes, BidiStream = BidiStream<S>, SendStream = SendStream<S>>,
    {
    }
    fn assert_split_types<B>()
    where
        B: quic::BidiStream<Bytes, SendStream = SendStream<S>, RecvStream = RecvStream<S>>,
    {
    }
    assert_connection_types::<Connection<S>>();
    assert_opener_types::<Connection<S>>();
    assert_opener_types::<OpenStreams<S>>();
    assert_split_types::<BidiStream<S>>();
}

/// The handles must be spawnable.
///
/// Not required by hyperium — it imposes no `Send` bound — but required by any caller that
/// puts the HTTP/3 driver on a multi-thread runtime, which is the ordinary case. The
/// transport's `Conn` is `Send` but not `Sync`, so this holds only because the shared core
/// sits behind a mutex.
#[test]
fn the_handles_can_cross_a_spawn_boundary() {
    is_send::<Connection<S>>();
    is_sync::<Connection<S>>();
    is_static::<Connection<S>>();
    is_send::<OpenStreams<S>>();
    is_send::<SendStream<S>>();
    is_send::<RecvStream<S>>();
    is_send::<BidiStream<S>>();
}

/// The opener is cloneable, because hyperium clones it.
#[test]
fn the_opener_is_cloneable() {
    fn is_clone<T: Clone>() {}
    is_clone::<OpenStreams<S>>();
}
