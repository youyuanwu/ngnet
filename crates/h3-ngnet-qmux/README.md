# h3-ngnet-qmux

`h3-ngnet-qmux` implements hyperium H3's per-stream transport traits over an
already-established [`ngnet-qmux`](https://docs.rs/ngnet-qmux) asynchronous
connection.

Construction returns an H3-facing connection and one caller-polled driver. The
crate does not connect sockets, select TLS, own an executor, spawn tasks, or
provide a timer. The driver must be polled concurrently for lower-I/O liveness
and for completion of a synchronous H3 close request.

The crate is currently unpublished while QMux remains an evolving draft.
