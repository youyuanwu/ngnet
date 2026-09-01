use bytes::Bytes;
use h3::proto::frame::Frame;
use h3::quic;
use h3_ngnet_qmux::from_qmux;
use h3_ngnet_qmux_tests::{LIMIT, exchange, memory_pair};
use ngnet_qmux::io::Config;
use ngnet_qmux::io::Connection as QmuxConnection;
use ngnet_qmux::io::testing::{TestClock, stream_pair};
use tokio::task::LocalSet;
use tokio::time::timeout;

#[tokio::test]
async fn consecutive_request_streams_have_exact_ids_and_independent_completion() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let first = exchange(&sender, Bytes::from_static(b"first"));
            let second = exchange(&sender, Bytes::from_static(b"second"));
            let ((_, first_body, first_id), (_, second_body, second_id)) =
                timeout(LIMIT, async { tokio::join!(first, second) })
                    .await
                    .expect("concurrent exchanges");
            assert_eq!(first_body, b"first"[..]);
            assert_eq!(second_body, b"second"[..]);
            assert_ne!(first_id, second_id);
            assert_eq!(first_id.into_inner() & 0x3, 0);
            assert_eq!(second_id.into_inner() & 0x3, 0);
        })
        .await;
}

#[tokio::test]
async fn upstream_control_uni_streams_and_data_first_bidi_stream_complete_together() {
    LocalSet::new()
        .run_until(async {
            let sender = memory_pair().await;
            let (_, body, id) = timeout(
                LIMIT,
                exchange(&sender, Bytes::from_static(b"data-first request")),
            )
            .await
            .expect("exchange");
            assert_eq!(body, b"data-first request"[..]);
            assert_eq!(id.into_inner(), 0);
        })
        .await;
}

#[tokio::test]
async fn raw_uni_bidi_split_framed_and_unframed_traits_complete_independently() {
    LocalSet::new()
        .run_until(async {
            let (client_io, server_io) = stream_pair();
            let client_lower =
                QmuxConnection::client(client_io, TestClock::new(), Config::new()).expect("client");
            let server_lower =
                QmuxConnection::server(server_io, TestClock::new(), Config::new()).expect("server");
            let (mut client, client_driver) = from_qmux(client_lower, 128);
            let (mut server, server_driver) = from_qmux(server_lower, 128);
            tokio::task::spawn_local(async move {
                let _ = client_driver.await;
            });
            tokio::task::spawn_local(async move {
                let _ = server_driver.await;
            });

            let client_task = async {
                let mut uni = std::future::poll_fn(|cx| {
                    quic::OpenStreams::<Bytes>::poll_open_send(&mut client, cx)
                })
                .await
                .expect("open uni");
                quic::SendStream::send_data(&mut uni, Frame::Data(Bytes::from_static(b"framed")))
                    .expect("framed data");
                std::future::poll_fn(|cx| quic::SendStream::poll_ready(&mut uni, cx))
                    .await
                    .expect("framed ready");
                std::future::poll_fn(|cx| quic::SendStream::poll_finish(&mut uni, cx))
                    .await
                    .expect("finish uni");

                let bidi = std::future::poll_fn(|cx| {
                    quic::OpenStreams::<Bytes>::poll_open_bidi(&mut client, cx)
                })
                .await
                .expect("open bidi");
                let (mut send, mut recv) = quic::BidiStream::split(bidi);
                let mut question = Bytes::from_static(b"question");
                std::future::poll_fn(|cx| {
                    quic::SendStreamUnframed::poll_send(&mut send, cx, &mut question)
                })
                .await
                .expect("question");
                std::future::poll_fn(|cx| quic::SendStream::poll_finish(&mut send, cx))
                    .await
                    .expect("finish question");
                let mut answer = Vec::new();
                while let Some(chunk) =
                    std::future::poll_fn(|cx| quic::RecvStream::poll_data(&mut recv, cx))
                        .await
                        .expect("answer")
                {
                    answer.extend_from_slice(&chunk);
                }
                answer
            };
            let server_task = async {
                let mut uni = std::future::poll_fn(|cx| {
                    quic::Connection::<Bytes>::poll_accept_recv(&mut server, cx)
                })
                .await
                .expect("accept uni");
                let mut framed = Vec::new();
                while let Some(chunk) =
                    std::future::poll_fn(|cx| quic::RecvStream::poll_data(&mut uni, cx))
                        .await
                        .expect("framed")
                {
                    framed.extend_from_slice(&chunk);
                }

                let bidi = std::future::poll_fn(|cx| {
                    quic::Connection::<Bytes>::poll_accept_bidi(&mut server, cx)
                })
                .await
                .expect("accept bidi");
                let (mut send, mut recv) = quic::BidiStream::split(bidi);
                let mut question = Vec::new();
                while let Some(chunk) =
                    std::future::poll_fn(|cx| quic::RecvStream::poll_data(&mut recv, cx))
                        .await
                        .expect("question")
                {
                    question.extend_from_slice(&chunk);
                }
                let mut answer = Bytes::from_static(b"answer");
                std::future::poll_fn(|cx| {
                    quic::SendStreamUnframed::poll_send(&mut send, cx, &mut answer)
                })
                .await
                .expect("answer");
                std::future::poll_fn(|cx| quic::SendStream::poll_finish(&mut send, cx))
                    .await
                    .expect("finish answer");
                (framed, question)
            };

            let (answer, (framed, question)) =
                timeout(LIMIT, async { tokio::join!(client_task, server_task) })
                    .await
                    .expect("raw trait exchange");
            assert_eq!(answer, b"answer");
            assert_eq!(question, b"question");
            assert_eq!(&framed[2..], b"framed");
        })
        .await;
}
