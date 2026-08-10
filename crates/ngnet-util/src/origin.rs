//! Origin derivation: turning a request URI into the key a connection is pooled under.
//!
//! An HTTP/2 connection serves exactly one origin, so the pool is a map from origin to
//! connection and every request must be reduced to one before anything else happens. The
//! reduction is more work than it looks, because [`http::Uri`] normalises *nothing*: it
//! preserves the case the caller wrote, keeps an explicitly written `:80` distinct from an
//! omitted port, and hands back an IPv6 host still wrapped in brackets. Every URI below
//! names one server, and a pool that keyed on the URI's own text would open four connections
//! to it:
//!
//! ```text
//! http://example.com/    http://EXAMPLE.com/    http://example.com:80/    http://Example.COM:80/
//! ```
//!
//! Four connections is not merely wasteful. Each carries its own flow-control windows and its
//! own stream concurrency limit, so requests that should have shared a window compete for
//! four smaller ones, and a server counting connections per client sees four times what it
//! expected.

use std::fmt;
use std::net::IpAddr;

use http::Uri;
use http::uri::Scheme;

use crate::error::Error;

/// The identity of a server: what a pooled connection is keyed by.
///
/// Two URIs produce equal `Origin`s exactly when a connection to one can serve the other.
/// The host is stored in a normalised form (see [`Origin::from_uri`]) and the port is always
/// explicit, so equality is a plain field comparison with no per-lookup normalisation cost.
///
/// The scheme is *not* stored. It is checked — [`Origin::from_uri`] rejects anything but
/// `http` — but having been checked it can only hold one value, and a field with one possible
/// value is a field that will be read as meaningful by the next person to see it. If this
/// crate ever serves a second scheme, the field arrives with the code that needs it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Origin {
    host: Box<str>,
    port: u16,
}

/// The only scheme this crate can serve, and the port it defaults to.
const HTTP_DEFAULT_PORT: u16 = 80;

impl Origin {
    /// Derives the origin a request URI names.
    ///
    /// # Normalisation
    ///
    /// Four things happen here, and each exists because [`Uri`] does not do it:
    ///
    /// 1. **The host is lower-cased.** Host names are case-insensitive; `Uri` preserves the
    ///    case it was given.
    /// 2. **An omitted port becomes 80.** `Uri::port` returns `None` for `http://host/` and
    ///    `Some(80)` for `http://host:80/`, which are the same origin.
    /// 3. **Brackets come off an IPv6 host.** `Uri::host` returns `[::1]`, brackets included.
    ///    That form is URI syntax, not an address: passing it to a resolver fails, so leaving
    ///    it on makes every IPv6 origin look like an unreachable host rather than a bug here.
    /// 4. **An IP literal is stored in the address's own canonical form.** This is the step
    ///    that is easy to leave out, because lower-casing appears to cover it. It does not:
    ///    `::1`, `0:0:0:0:0:0:0:1` and `0000:0000:0000:0000:0000:0000:0000:0001` are one
    ///    address written three legal ways, all lower-case already, and comparing their text
    ///    gives three origins and three connections to one server. Parsing the host as an
    ///    [`IpAddr`] and storing *its* rendering collapses them, and covers IPv4 for free.
    ///
    /// A trailing dot is deliberately **not** stripped. `example.com.` is a fully qualified
    /// name and `example.com` is subject to the resolver's search list; they can resolve to
    /// different servers, so treating them as one origin would be wrong in exactly the cases
    /// where it mattered.
    ///
    /// Normalisation is not applied to names, only to hosts that parse as addresses. A name
    /// is not an address and there is no canonical rendering to reach for.
    ///
    /// # Errors
    ///
    /// Returns an error of kind [`ErrorKind::Uri`] if the URI has no scheme, has a scheme
    /// other than `http`, or has no authority. A secure scheme is refused rather than
    /// attempted: this stack is cleartext-only, so serving `https` would be a silent
    /// downgrade, which is worse than a clear refusal.
    ///
    /// [`ErrorKind::Uri`]: crate::ErrorKind::Uri
    pub fn from_uri(uri: &Uri) -> Result<Self, Error> {
        let scheme = uri.scheme().ok_or_else(|| {
            Error::uri("request URI has no scheme; an absolute `http://` URI is required")
        })?;

        if scheme != &Scheme::HTTP {
            return Err(Error::uri(format!(
                "unsupported URI scheme `{scheme}`; this client speaks cleartext HTTP/2 only, \
                 and attempting `{scheme}` in cleartext would be a silent downgrade"
            )));
        }

        // `filter` and not just `ok_or_else`: `http://:80/` parses, and `Uri::host` returns
        // `Some("")` for it rather than `None`. Without the emptiness check that becomes an
        // origin with no host, which resolves to nothing and fails as a connect error —
        // reporting a malformed URI as a network problem.
        let host = uri.host().filter(|host| !host.is_empty()).ok_or_else(|| {
            Error::uri("request URI has no host; an absolute `http://` URI is required")
        })?;

        let port = uri.port_u16().unwrap_or(HTTP_DEFAULT_PORT);

        Ok(Self {
            host: normalise_host(host),
            port,
        })
    }

    /// The host, normalised, and in the form a resolver accepts.
    ///
    /// An IPv6 host is returned *without* brackets, which is what
    /// [`tokio::net::TcpStream::connect`] wants and the opposite of what [`Uri::host`] gives.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port, always explicit even when the URI omitted it.
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Lower-cases, unbrackets, and canonicalises an IP literal.
///
/// Split out from [`Origin::from_uri`] so the four rules can be read in one place, and so a
/// test can reach them without constructing a URI around each case.
fn normalise_host(host: &str) -> Box<str> {
    // Brackets first: `[::1]` does not parse as an address, and the trimmed form is what both
    // the parse below and any resolver want.
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);

    // `IpAddr`'s `Display` is the canonical rendering: for IPv6 that is RFC 5952 form — lower
    // case, longest zero run compressed, no leading zeros — so every spelling of one address
    // arrives at the same string. `parse` is also the only honest test of "is this an
    // address", since a name can contain hex digits and colons cannot appear in one.
    if let Ok(addr) = unbracketed.parse::<IpAddr>() {
        return addr.to_string().into_boxed_str();
    }

    // A name. Lower-casing is the whole of the normalisation, because DNS is
    // case-insensitive but nothing else about a name is negotiable — in particular the
    // trailing dot stays, since it changes which name is looked up.
    unbracketed.to_ascii_lowercase().into_boxed_str()
}

impl fmt::Display for Origin {
    /// Renders the origin as it would appear in a URI authority, brackets restored.
    ///
    /// The brackets come back for IPv6 because this is for humans and for error messages,
    /// where `[::1]:8080` is unambiguous and `::1:8080` is not.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}
