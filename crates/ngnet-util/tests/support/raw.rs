//! A hand-written HTTP/2 peer, for the things a real server will not do on command.
//!
//! hyper's server is the right peer for almost everything, and is used for almost everything.
//! It is the wrong peer for exactly two claims, both about `GOAWAY`:
//!
//! * A `GOAWAY` naming a stream *below* one the client has already opened — the refusal case.
//!   hyper will not produce one, because hyper does not refuse streams it has accepted; the
//!   frame only appears from a peer that is deliberately unhelpful.
//! * The `GOAWAY` the *client* sends. hyper consumes it and reports the connection closing,
//!   which is indistinguishable from the socket simply going away. Asserting that shutdown
//!   said goodbye rather than hanging up requires reading the frame off the wire.
//!
//! # Why this is only two hundred lines
//!
//! Because it speaks almost none of HTTP/2. It reads the connection preface, sends an empty
//! `SETTINGS`, acknowledges the peer's, and thereafter looks at nothing but the frame type
//! byte. Responses are one byte of HPACK: `0x88` is the static table's entry for
//! `:status: 200`, so a `HEADERS` frame with that payload and `END_HEADERS | END_STREAM` set
//! is a complete, valid, empty `200`. No HPACK encoder, no dynamic table, no flow control
//! accounting — this peer never sends a body, so it never needs a window.
//!
//! That is deliberate. A test peer complicated enough to have bugs is a test peer whose
//! failures have to be diagnosed before the crate's can be.

#![allow(dead_code)] // Each integration test file uses a different part of this.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// The client connection preface, which every HTTP/2 client sends before anything else.
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

const FRAME_DATA: u8 = 0x0;
const FRAME_HEADERS: u8 = 0x1;
const FRAME_SETTINGS: u8 = 0x4;
const FRAME_PING: u8 = 0x6;
const FRAME_GOAWAY: u8 = 0x7;

/// What this peer does with the requests it receives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behaviour {
    /// Answer every request, for ever.
    Answer,
    /// Answer `answer` requests, then send `GOAWAY` naming `last_stream` and keep the socket
    /// open.
    ///
    /// Staying open afterwards is the point: a peer that also closed the socket would let a
    /// pool pass the eviction test by noticing the *disconnection*, which is a much weaker
    /// property than noticing the `GOAWAY`.
    AnswerThenGoAway { answer: usize, last_stream: u32 },
    /// The *first* connection answers `answer` requests and then sends `GOAWAY` naming
    /// `last_stream`; every connection after it answers everything.
    ///
    /// A peer that retired every connection it accepted would make "the pool opened a
    /// replacement" indistinguishable from "the pool opened a replacement and then retired
    /// that one too", which is not the claim. This models the ordinary case: one connection
    /// goes away, its successor is healthy.
    FirstRetires { answer: usize, last_stream: u32 },
    /// Answer in two frames — the head at once, the body after `delay_ms`.
    ///
    /// Exists so that a test can hold a response *open* across an event, which is the only
    /// way to distinguish "the driver was left to finish" from "the driver was cancelled and
    /// the caller happened to have everything already".
    AnswerInTwoParts { delay_ms: u64 },
    /// Send `GOAWAY(0)` the moment the first request arrives, without answering it.
    ///
    /// `0` means "no stream was processed", so a request on stream 1 was provably never
    /// acted on. This is the only case in which a retry would be safe, and the only way to
    /// produce it is to write it by hand.
    RefuseEverything,
}

/// What one connection to this peer received.
#[derive(Debug, Default, Clone)]
pub struct Received {
    /// Frame type bytes, in arrival order, excluding the preface.
    pub frames: Vec<u8>,
    /// How many `HEADERS` frames arrived — one per request.
    pub requests: usize,
}

impl Received {
    /// Whether a `GOAWAY` arrived from the client on this connection.
    pub fn saw_goaway(&self) -> bool {
        self.frames.contains(&FRAME_GOAWAY)
    }
}

/// A scripted HTTP/2 peer on an ephemeral loopback port.
pub struct RawPeer {
    pub address: SocketAddr,
    accepts: Arc<AtomicUsize>,
    connections: Arc<Mutex<Vec<Received>>>,
    task: Option<JoinHandle<()>>,
}

impl RawPeer {
    /// Starts a peer that behaves as described for every connection it accepts.
    pub async fn start(behaviour: Behaviour) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a bound listener");
        let address = listener.local_addr().expect("a bound address");

        let accepts = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(Mutex::new(Vec::new()));

        let accept_sink = Arc::clone(&accepts);
        let connection_sink = Arc::clone(&connections);

        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                accept_sink.fetch_add(1, Ordering::SeqCst);
                let index = {
                    let mut connections = connection_sink.lock().expect("a lock");
                    connections.push(Received::default());
                    connections.len() - 1
                };
                let record = Arc::clone(&connection_sink);
                // Resolved per connection, so that `FirstRetires` can mean what it says.
                let behaviour = match behaviour {
                    Behaviour::FirstRetires {
                        answer,
                        last_stream,
                    } if index == 0 => Behaviour::AnswerThenGoAway {
                        answer,
                        last_stream,
                    },
                    Behaviour::FirstRetires { .. } => Behaviour::Answer,
                    other => other,
                };
                tokio::spawn(async move {
                    let _ = serve(socket, behaviour, record, index).await;
                });
            }
        });

        Self {
            address,
            accepts,
            connections,
            task: Some(task),
        }
    }

    /// How many TCP connections this peer has accepted.
    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    /// What each accepted connection has received so far, in accept order.
    pub fn connections(&self) -> Vec<Received> {
        self.connections.lock().expect("a lock").clone()
    }

    /// The authority a client should use to reach this peer.
    pub fn authority(&self) -> String {
        self.address.to_string()
    }

    /// A `http://<authority><path>` URI naming this peer.
    pub fn uri(&self, path: &str) -> http::Uri {
        format!("http://{}{}", self.authority(), path)
            .parse()
            .expect("a valid test URI")
    }
}

impl Drop for RawPeer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Reads the preface, then loops over frames doing as little as possible.
async fn serve(
    mut socket: TcpStream,
    behaviour: Behaviour,
    record: Arc<Mutex<Vec<Received>>>,
    index: usize,
) -> std::io::Result<()> {
    let mut preface = [0u8; PREFACE.len()];
    socket.read_exact(&mut preface).await?;
    if preface != PREFACE {
        return Ok(());
    }

    // An empty `SETTINGS` is a complete and valid one: every setting has a default, and this
    // peer is content with all of them. The client will not send a request until it arrives.
    write_frame(&mut socket, FRAME_SETTINGS, 0, 0, &[]).await?;

    let mut answered = 0usize;
    let mut goaway_sent = false;

    loop {
        let mut header = [0u8; 9];
        socket.read_exact(&mut header).await?;
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let kind = header[3];
        let flags = header[4];
        let stream = u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);

        let mut payload = vec![0u8; length];
        socket.read_exact(&mut payload).await?;

        {
            let mut connections = record.lock().expect("a lock");
            let received = &mut connections[index];
            received.frames.push(kind);
            if kind == FRAME_HEADERS {
                received.requests += 1;
            }
        }

        match kind {
            // Acknowledge a settings frame that is not itself an acknowledgement. Without
            // this the client's own settings are never confirmed and some clients will wait.
            FRAME_SETTINGS if flags & 0x1 == 0 => {
                write_frame(&mut socket, FRAME_SETTINGS, 0x1, 0, &[]).await?;
            }
            FRAME_PING if flags & 0x1 == 0 => {
                write_frame(&mut socket, FRAME_PING, 0x1, 0, &payload).await?;
            }
            FRAME_HEADERS => match behaviour {
                Behaviour::RefuseEverything => {
                    if !goaway_sent {
                        goaway_sent = true;
                        write_goaway(&mut socket, 0).await?;
                    }
                }
                Behaviour::Answer | Behaviour::FirstRetires { .. } => {
                    respond(&mut socket, stream).await?;
                }
                Behaviour::AnswerInTwoParts { delay_ms } => {
                    // Head first, without `END_STREAM`, so the exchange stays open.
                    write_frame(&mut socket, FRAME_HEADERS, 0x4, stream, &[0x88]).await?;
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    write_frame(&mut socket, FRAME_DATA, 0x1, stream, b"late").await?;
                }
                Behaviour::AnswerThenGoAway {
                    answer,
                    last_stream,
                } => {
                    if answered < answer {
                        answered += 1;
                        respond(&mut socket, stream).await?;
                        if answered == answer && !goaway_sent {
                            goaway_sent = true;
                            write_goaway(&mut socket, last_stream).await?;
                        }
                    }
                }
            },
            // A `GOAWAY` from the client is recorded above and needs no reply. Everything
            // else — `DATA`, `WINDOW_UPDATE`, `PRIORITY`, `RST_STREAM` — is deliberately
            // ignored: this peer sends no bodies, so it needs no flow control accounting, and
            // a request body is read only to get it off the socket.
            FRAME_GOAWAY | FRAME_DATA => {}
            _ => {}
        }
    }
}

/// Writes an empty `200`, which is one byte of HPACK.
///
/// `0x88` is an indexed header field naming static table entry 8, which is `:status: 200`.
/// `END_HEADERS | END_STREAM` says the response is complete, so no `DATA` frame follows and
/// no encoder state is needed.
async fn respond(socket: &mut TcpStream, stream: u32) -> std::io::Result<()> {
    write_frame(socket, FRAME_HEADERS, 0x4 | 0x1, stream, &[0x88]).await
}

async fn write_goaway(socket: &mut TcpStream, last_stream: u32) -> std::io::Result<()> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&last_stream.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes()); // NO_ERROR
    write_frame(socket, FRAME_GOAWAY, 0, 0, &payload).await
}

async fn write_frame(
    socket: &mut TcpStream,
    kind: u8,
    flags: u8,
    stream: u32,
    payload: &[u8],
) -> std::io::Result<()> {
    let length = payload.len() as u32;
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes()[1..]);
    frame.push(kind);
    frame.push(flags);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await?;
    socket.flush().await
}
