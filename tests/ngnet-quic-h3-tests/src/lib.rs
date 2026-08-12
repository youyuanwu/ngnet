//! Harness for driving HTTP/3 over ngtcp2.
//!
//! Everything here binds real loopback UDP sockets on ephemeral ports and runs on a real
//! runtime. There is no in-memory shortcut: the point of these tests is that datagrams
//! actually cross a socket, because that is where wire-format and timing defects live.

use std::sync::Arc;

use ngnet_quic::endpoint::{
    Config, Endpoint, EndpointBuilder, EndpointDriver, TokioClock, TokioSocket,
};
use ngnet_quic::{Duration, OsslBackend, OsslSession, Role};

/// The application protocol HTTP/3 negotiates.
///
/// Not a test-only name: interoperating with another implementation means agreeing on the
/// identifier the specification defines, and a made-up one would only prove the two ends of
/// this workspace agree with each other.
pub const H3_ALPN: &[u8] = b"h3";

/// The name the test certificate is issued for.
pub const TEST_SERVER_NAME: &str = "localhost";

/// The endpoint driver these tests spawn.
pub type Driver = EndpointDriver<TokioSocket, TokioClock, OsslBackend>;

/// A self-signed certificate and its key.
pub struct Credentials {
    /// PEM certificate chain.
    pub certificate_pem: String,
    /// PEM private key.
    pub key_pem: String,
    /// The same certificate in DER, which quinn's configuration takes.
    pub certificate_der: rcgen::CertifiedKey<rcgen::KeyPair>,
}

impl Credentials {
    /// Generates a certificate for [`TEST_SERVER_NAME`].
    pub fn generate() -> Self {
        let cert = rcgen::generate_simple_self_signed(vec![TEST_SERVER_NAME.to_string()])
            .expect("generating a self-signed certificate");
        Self {
            certificate_pem: cert.cert.pem(),
            key_pem: cert.signing_key.serialize_pem(),
            certificate_der: cert,
        }
    }
}

/// A deterministic entropy source.
///
/// **Not suitable for anything but tests**: it is a counter. Real endpoints must supply real
/// randomness, which is why the builder makes the caller provide it rather than choosing.
pub struct TestEntropy {
    state: u64,
}

impl TestEntropy {
    /// A source seeded from `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed | 1,
        }
    }
}

impl ngnet_quic::EntropySource for TestEntropy {
    fn fill(&mut self, buffer: &mut [u8]) -> ngnet_quic::Result<()> {
        for slot in buffer.iter_mut() {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            *slot = self.state.to_le_bytes()[0];
        }
        Ok(())
    }
}

/// Binds a client endpoint on an ephemeral port.
///
/// The seed matters because the entropy source is deterministic: two clients from the same
/// seed mint identical connection identifiers, and a server routing by identifier would then
/// deliver one client's datagrams to the other.
pub async fn client_endpoint(
    credentials: &Credentials,
    seed: u64,
) -> (Endpoint<OsslSession>, Driver) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a client socket");
    let backend = OsslBackend::builder(Role::Client)
        .alpn(H3_ALPN)
        .trust_anchor_pem(credentials.certificate_pem.as_str())
        .use_system_trust_store(false)
        .build()
        .expect("a client backend");
    EndpointBuilder::new(socket, TokioClock::new(), backend)
        .config(Config::new().handshake_timeout(Duration::from_nanos(5_000_000_000)))
        .entropy(move || TestEntropy::new(seed))
        .build_detachable()
        .expect("a client endpoint")
}

/// Binds a server endpoint on an ephemeral port, returning the address it landed on.
pub async fn server_endpoint(
    credentials: &Credentials,
) -> (Endpoint<OsslSession>, Driver, core::net::SocketAddr) {
    let socket = TokioSocket::bind("127.0.0.1:0".parse().expect("valid"))
        .await
        .expect("binding a server socket");
    let address = socket.inner().local_addr().expect("a bound address");
    let backend = OsslBackend::builder(Role::Server)
        .alpn(H3_ALPN)
        .certificate_chain_pem(credentials.certificate_pem.as_str())
        .private_key_pem(credentials.key_pem.as_str())
        .build()
        .expect("a server backend");
    let (endpoint, driver) = EndpointBuilder::new(socket, TokioClock::new(), backend)
        .accepts(true)
        .config(Config::new().handshake_timeout(Duration::from_nanos(5_000_000_000)))
        .entropy(|| TestEntropy::new(0xC0FFEE))
        .build_detachable()
        .expect("a server endpoint");
    (endpoint, driver, address)
}

/// A quinn server configured to speak HTTP/3, for interoperability.
///
/// The application protocol is set explicitly. quinn does not set one by default, and
/// `ngnet-quic`'s TLS backend requires one — so an interop handshake without this fails for
/// a reason that has nothing to do with QUIC.
pub fn quinn_server(credentials: &Credentials) -> quinn::ServerConfig {
    let cert = credentials.certificate_der.cert.der().clone();
    let key = rustls_key(credentials);
    let mut config =
        quinn::ServerConfig::with_single_cert(vec![cert], key).expect("a quinn server config");
    let mut crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![credentials.certificate_der.cert.der().clone()],
            rustls_key(credentials),
        )
        .expect("a rustls server config");
    crypto.alpn_protocols = vec![H3_ALPN.to_vec()];
    config.crypto = Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto).expect("a quic server config"),
    );
    config
}

/// A quinn client that trusts the test certificate and speaks HTTP/3.
pub fn quinn_client(credentials: &Credentials) -> quinn::ClientConfig {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(credentials.certificate_der.cert.der().clone())
        .expect("trusting the test certificate");
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![H3_ALPN.to_vec()];
    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).expect("a quic client config"),
    ))
}

fn rustls_key(credentials: &Credentials) -> quinn::rustls::pki_types::PrivateKeyDer<'static> {
    quinn::rustls::pki_types::PrivateKeyDer::Pkcs8(
        credentials.certificate_der.signing_key.serialize_der().into(),
    )
}
