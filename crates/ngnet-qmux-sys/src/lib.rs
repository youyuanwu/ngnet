//! Raw FFI bindings to [dwnx], the ngtcp2 authors' C implementation of [QMux].
//!
//! This crate is the unopinionated half of the pair: it builds the vendored C library and
//! exposes its declarations exactly as bindgen generates them, with no safety, no ownership,
//! and no ergonomics. [`ngnet-qmux`] is the half that supplies those.
//!
//! # QMux in one paragraph
//!
//! QMux carries QUIC's stream and datagram operations over a single ordered, reliable byte
//! stream. It is not QUIC: there are no packets, no connection IDs, no paths, no loss recovery
//! and no congestion control, because whatever carries it already provides those. Nor is
//! anything encrypted -- the draft delegates confidentiality, integrity and protocol
//! negotiation to the transport, and explicitly permits substrates that supply none of them,
//! such as a unix socket. That is why this crate links no TLS library and has no features.
//!
//! # How the native library is built
//!
//! Unlike every other `-sys` crate in this workspace, the build script compiles the C sources
//! directly with `cc` rather than driving CMake, because dwnx ships autotools and no
//! CMakeLists.txt. The build script also performs the handful of configure-time probes the
//! sources actually consult, and generates the version header autotools would have produced.
//! See `build.rs` for why the probes need two different treatments.
//!
//! [dwnx]: https://github.com/ngtcp2/dwnx
//! [QMux]: https://datatracker.ietf.org/doc/html/draft-ietf-quic-qmux
//! [`ngnet-qmux`]: https://docs.rs/ngnet-qmux

// The generated bindings are C, and are named accordingly.
#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
// dwnx's doc comments are reStructuredText written for its own documentation build. They
// contain bare URLs and `:macro:` roles that rustdoc reads as malformed links.
#![allow(rustdoc::bare_urls, rustdoc::broken_intra_doc_links)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
