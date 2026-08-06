//! A tiny h2c server built on `std::net`, with no runtime and no dependencies.
//!
//! Run it, then talk to it with any HTTP/2 client that speaks cleartext with prior
//! knowledge — there is no TLS and no upgrade dance:
//!
//! ```text
//! cargo run -p ngnet-h2 --example h2c_server
//! curl --http2-prior-knowledge -i http://127.0.0.1:8080/hello
//! curl --http2-prior-knowledge -i --data 'ping' http://127.0.0.1:8080/echo
//! ```
//!
//! A request to `/echo` is answered with its own body; anything else gets a greeting.
//! Pass an address as the first argument to bind somewhere other than `127.0.0.1:8080`.
//!
//! The session type itself owns no socket and starts no thread. Everything below —
//! accepting, reading, writing, threading — is this example's business, which is the
//! whole point of a sans-I/O core: the transport is yours to choose.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use ngnet_h2::{BytesBody, FrameInfo, Header, HeaderAction, Session, SessionBuilder, StreamId};

type Fallible<T = ()> = Result<T, Box<dyn StdError + Send + Sync>>;

/// What one connection has seen so far.
#[derive(Default)]
struct Connection {
    paths: BTreeMap<i32, String>,
    methods: BTreeMap<i32, String>,
    bodies: BTreeMap<i32, Vec<u8>>,
    /// Streams whose request has fully arrived and which owe a response.
    complete: Vec<StreamId>,
}

fn main() -> Fallible {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned());

    let listener = TcpListener::bind(&addr)?;
    println!("h2c server listening on http://{}", listener.local_addr()?);
    println!("try: curl --http2-prior-knowledge -i http://{addr}/hello");

    for incoming in listener.incoming() {
        let socket = incoming?;
        // One blocking thread per connection keeps the example readable. The same session
        // API drives equally well from a poll loop or an async runtime.
        thread::spawn(move || {
            let peer = socket.peer_addr().ok();
            if let Err(error) = serve(socket) {
                eprintln!("connection from {peer:?} ended: {error}");
            }
        });
    }

    Ok(())
}

/// Runs one connection to completion.
fn serve(mut socket: TcpStream) -> Fallible {
    socket.set_nodelay(true)?;

    let mut session = build_session();
    let mut connection = Connection::default();
    let mut buf = vec![0u8; 16 * 1024];

    loop {
        // Always flush before blocking. Output only becomes available as a consequence of
        // input, so a loop that reads first can wedge with bytes still queued.
        flush(&mut session, &mut connection, &mut socket)?;

        if !session.want_read() {
            return Ok(());
        }

        let read = socket.read(&mut buf)?;
        if read == 0 {
            return Ok(());
        }

        // One read is an arbitrary slice of the stream: it may hold several frames, part
        // of a frame, or frames belonging to different streams. The session sorts that
        // out, but it may take the buffer in more than one bite.
        let mut input = &buf[..read];
        while !input.is_empty() {
            let consumed = session.recv(input, &mut connection)?;
            if consumed == 0 {
                break;
            }
            input = &input[consumed..];
        }

        // Handlers are never handed the session, so responses are submitted out here,
        // between reads, from what the handlers recorded.
        respond(&mut session, &mut connection)?;
    }
}

fn build_session() -> Session<Connection> {
    SessionBuilder::<Connection>::server()
        .on_header(
            |conn: &mut Connection, frame: FrameInfo, name: &[u8], value: &[u8]| {
                let stream = frame.stream_id().get();
                match name {
                    b":path" => {
                        conn.paths
                            .insert(stream, String::from_utf8_lossy(value).into_owned());
                    }
                    b":method" => {
                        conn.methods
                            .insert(stream, String::from_utf8_lossy(value).into_owned());
                    }
                    _ => {}
                }
                HeaderAction::Continue
            },
        )
        .on_data_chunk(|conn: &mut Connection, stream: StreamId, chunk: &[u8]| {
            conn.bodies
                .entry(stream.get())
                .or_default()
                .extend_from_slice(chunk);
        })
        .on_frame(|conn: &mut Connection, frame: FrameInfo| {
            if frame.is_end_stream() {
                conn.complete.push(frame.stream_id());
            }
        })
        .build()
        .expect("building the server session")
}

/// Answers every request that completed during the last read.
fn respond(session: &mut Session<Connection>, conn: &mut Connection) -> Fallible {
    for stream in std::mem::take(&mut conn.complete) {
        let id = stream.get();
        let path = conn.paths.remove(&id).unwrap_or_else(|| "/".to_owned());
        let method = conn.methods.remove(&id).unwrap_or_else(|| "?".to_owned());
        let body = conn.bodies.remove(&id).unwrap_or_default();

        println!("stream {id}: {method} {path} ({} body octets)", body.len());

        let payload = if path == "/echo" && !body.is_empty() {
            body
        } else {
            format!("hello from the ngnet-h2 crate - you asked for {path}\n").into_bytes()
        };

        session.submit_response_with_body(
            stream,
            &[
                Header::new(":status", "200"),
                Header::new("content-type", "text/plain; charset=utf-8"),
            ],
            BytesBody::new(payload),
        )?;
    }

    Ok(())
}

/// Writes everything the session has queued.
///
/// Each block is handed over before the next is requested, because libnghttp2 invalidates
/// the previous one — a constraint the borrow checker enforces here rather than leaving
/// to care.
fn flush(
    session: &mut Session<Connection>,
    conn: &mut Connection,
    socket: &mut TcpStream,
) -> Fallible {
    while let Some(block) = session.send(conn)? {
        socket.write_all(block)?;
    }
    socket.flush()?;
    Ok(())
}
