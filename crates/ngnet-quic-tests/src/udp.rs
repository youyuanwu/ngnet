//! Driving a connection over a real loopback UDP socket.
//!
//! The in-process relay in the crate root proves the state machine. This proves the same
//! thing through the kernel, which is a different claim: address handling, datagram
//! boundaries and buffer sizes all become real here.

use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration as StdDuration;

use ngnet_quic::{ExpiryOutcome, ReadOutcome, WriteOutcome};

use crate::{TestClock, TestConn, drain};

/// A UDP socket bound to loopback, with a short read timeout.
pub struct LoopbackSocket {
    socket: UdpSocket,
}

impl LoopbackSocket {
    /// Binds an ephemeral loopback port.
    pub fn bind() -> io::Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0")?;
        // Short, so a test that is waiting for something that will never arrive fails in
        // seconds rather than hanging the suite.
        socket.set_read_timeout(Some(StdDuration::from_millis(50)))?;
        Ok(Self { socket })
    }

    /// The address this socket is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Sends a datagram.
    pub fn send_to(&self, datagram: &[u8], to: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(datagram, to)
    }

    /// Receives a datagram, or `None` if the read timed out.
    pub fn recv(&self, buf: &mut [u8]) -> io::Result<Option<usize>> {
        match self.socket.recv_from(buf) {
            Ok((len, _)) => Ok(Some(len)),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

/// Sends everything a connection has to send, over a socket.
pub fn flush(
    conn: &mut TestConn<'_>,
    clock: &TestClock,
    socket: &LoopbackSocket,
    peer: SocketAddr,
) -> Result<usize, Box<dyn std::error::Error>> {
    let datagrams = drain(conn, clock)?;
    let count = datagrams.len();
    for datagram in datagrams {
        socket.send_to(&datagram, peer)?;
    }
    Ok(count)
}

/// Receives whatever is waiting and feeds it to a connection.
///
/// Returns how many datagrams were consumed.
pub fn absorb(
    conn: &mut TestConn<'_>,
    clock: &TestClock,
    socket: &LoopbackSocket,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut buf = vec![0u8; 2048];
    let mut count = 0;
    while let Some(len) = socket.recv(&mut buf)? {
        count += 1;
        match conn.read_pkt(&buf[..len], clock.now())? {
            ReadOutcome::Processed => {}
            other => {
                eprintln!("connection ended the exchange: {other:?}");
                break;
            }
        }
    }
    Ok(count)
}

/// Runs both ends over real sockets until the handshake completes or the rounds run out.
pub fn pump_sockets(
    client: &mut TestConn<'_>,
    client_socket: &LoopbackSocket,
    server: &mut TestConn<'_>,
    server_socket: &LoopbackSocket,
    clock: &TestClock,
    rounds: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_addr = client_socket.local_addr()?;
    let server_addr = server_socket.local_addr()?;

    for _ in 0..rounds {
        let sent = flush(client, clock, client_socket, server_addr)?;
        let received = absorb(server, clock, server_socket)?;
        let replied = flush(server, clock, server_socket, client_addr)?;
        let answered = absorb(client, clock, client_socket)?;

        if client.is_handshake_completed() && server.is_handshake_completed() {
            return Ok(());
        }

        if sent + received + replied + answered == 0 {
            // Only a timer can make progress now.
            let next = [client.expiry(), server.expiry()]
                .into_iter()
                .flatten()
                .min();
            let Some(deadline) = next else { break };
            clock.advance_to(deadline);
            clock.advance(1);
            if client.handle_expiry(clock.now())? == ExpiryOutcome::IdleClose {
                break;
            }
            if server.handle_expiry(clock.now())? == ExpiryOutcome::IdleClose {
                break;
            }
        }
    }

    Ok(())
}

/// Sends a datagram and reports whether the write outcome was a datagram at all.
///
/// A small helper, but it keeps the `WriteOutcome` match out of individual tests.
pub fn write_once(
    conn: &mut TestConn<'_>,
    clock: &TestClock,
    buf: &mut [u8],
) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    Ok(match conn.write_pkt(buf, clock.now())? {
        WriteOutcome::Datagram { len } => Some(len),
        WriteOutcome::Idle | WriteOutcome::Blocked => None,
    })
}
