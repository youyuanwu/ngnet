mod common;

use bytes::Bytes;
use h3::quic::{BidiStream as _, RecvStream as _};
use ngnet_qmux::io::Config;

use common::{accept_bidi, accept_uni, finish, open_bidi, open_uni, pair, receive_all, run_pair};

#[test]
fn peer_uni_and_bidi_streams_are_routed_to_exactly_one_accept_path() {
    let (mut client, mut client_driver, mut server, mut server_driver) = pair(Config::new());

    let client_task = async {
        let mut uni = open_uni(&mut client).await;
        common::send_all_unframed(&mut uni, Bytes::from_static(b"uni")).await;
        finish(&mut uni).await;

        let mut bidi = open_bidi(&mut client).await;
        common::send_all_unframed(&mut bidi, Bytes::from_static(b"bidi")).await;
        finish(&mut bidi).await;
    };

    let server_task = async {
        let mut uni = accept_uni(&mut server).await;
        let uni_body = receive_all(&mut uni).await;
        let mut bidi = accept_bidi(&mut server).await;
        let bidi_body = receive_all(&mut bidi).await;
        (uni_body, bidi_body)
    };

    let (_, (uni, bidi)) = run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(uni, b"uni");
    assert_eq!(bidi, b"bidi");
    assert_eq!(server.snapshot().pending_accepts, 0);
}

#[test]
fn data_and_fin_are_ordered_and_fin_is_stable() {
    let lower = Config::new().initial_max_stream_data(4).initial_max_data(4);
    let (mut client, mut client_driver, mut server, mut server_driver) = pair(lower);

    let client_task = async {
        let mut stream = open_bidi(&mut client).await;
        common::send_all_unframed(&mut stream, Bytes::from_static(b"abcdefgh")).await;
        finish(&mut stream).await;
    };
    let server_task = async {
        let mut stream = accept_bidi(&mut server).await;
        let body = receive_all(&mut stream).await;
        let second_fin = std::future::poll_fn(|cx| stream.poll_data(cx))
            .await
            .expect("stable receive result");
        (body, second_fin)
    };

    let (_, (body, second_fin)) = run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(body, b"abcdefgh");
    assert!(second_fin.is_none());
}

#[test]
fn splitting_preserves_both_directional_capabilities() {
    let (mut client, mut client_driver, mut server, mut server_driver) = pair(Config::new());

    let client_task = async {
        let stream = open_bidi(&mut client).await;
        let (mut send, mut recv) = stream.split();
        common::send_all_unframed(&mut send, Bytes::from_static(b"question")).await;
        finish(&mut send).await;
        receive_all(&mut recv).await
    };
    let server_task = async {
        let stream = accept_bidi(&mut server).await;
        let (mut send, mut recv) = stream.split();
        let question = receive_all(&mut recv).await;
        common::send_all_unframed(&mut send, Bytes::from_static(b"answer")).await;
        finish(&mut send).await;
        question
    };

    let (answer, question) = run_pair(
        client_task,
        &mut client_driver,
        server_task,
        &mut server_driver,
    );
    assert_eq!(question, b"question");
    assert_eq!(answer, b"answer");
}
