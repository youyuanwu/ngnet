# Design

Why `ngnet-axum` is shaped the way it is. Behaviour is documented with the code; this
records the decisions, and in particular the two where the obvious answer was assumed first
and turned out to be wrong.

## What the crate is

An axum `Router`, served over `ngnet-h2` instead of hyper. Server-side, h2c only, tokio
only. Every one of those is a boundary rather than a stage on a roadmap. Note that the
transport is *not* among them: the server is generic over a `Listener`, and TCP is one
implementation rather than the shape the crate is built around.

- **Server only.** There is no client surface. `ngnet-h2` has a client, but axum's `Router`
  is a server-side abstraction and there is nothing to integrate on the other side.
- **h2c only.** `ngnet-h2` is cleartext HTTP/2. There is no TLS, so there is no ALPN, so
  there is no protocol negotiation and no HTTP/1.1 upgrade. A peer that is not speaking
  HTTP/2 is a connection error, not a fallback.
- **tokio only.** This one needs restating now that transports are pluggable, because the
  two are easy to conflate. Any transport can be served — a socket, an in-memory pipe, a TLS
  session — but the *runtime* cannot change. The accept loop is built on `tokio::select!`,
  `tokio::time` and `JoinSet`, and `JoinSet::spawn` requires `Send + 'static`. `ngnet-h2`'s
  `completion` transport is thread-per-core over compio and its types are not `Send`, so it
  remains out of reach regardless of what listener is supplied. Serving it would need a
  different accept loop, not a different listener. Pluggable transports did not bring this
  closer and were never going to.

## The integration point is tower, not hyper

axum is usually introduced as being built on hyper, which makes replacing hyper sound like
replacing axum. It is not, and the reason is worth stating precisely because the whole crate
rests on it.

`Router<()>` implements `tower_service::Service<http::Request<B>>`, returning
`http::Response<axum::body::Body>`. Routing, extractors, middleware, state and error
handling are all defined against `http` types. hyper's contribution is turning socket bytes
into an `http::Request` and an `http::Response` back into bytes — a job with no axum in it.
`axum::serve` is a loop that accepts sockets and hands them to hyper along with the
`Router`; this crate is a loop that accepts sockets and hands them to `ngnet-h2` along with
the `Router`.

`tower-service` is depended on directly rather than through `tower`. axum does not re-export
the trait, so `Router::call` cannot be named without it, and `tower` itself would add the
combinator layer — `ServiceBuilder`, retries, timeouts — when what is wanted is the one
trait definition those are written against. It is the same trait either way:
`tower::Service` *is* `tower_service::Service`.

## Decisions that cost a wrong attempt first

- **There are no body adapters, because none is needed.** The crate was planned around a
  pair of them: something to wrap `ngnet-h2`'s incoming body so axum would accept it, and
  something to wrap axum's outgoing body so `ngnet-h2` would send it. Both turned out to be
  unnecessary, and finding that out changed what the crate is — from a translation layer to
  a piece of wiring.

  `ngnet-h2`'s `IncomingBody` is an `http_body::Body` with `Data = Bytes`, `Send + 'static`.
  axum's runnable impl is
  `impl<B> Service<Request<B>> for Router<()> where B: HttpBody<Data = Bytes> + Send + 'static`.
  The request already fits, so it is passed through untouched. In the other direction axum's
  `Body` also has `Data = Bytes`, which is what `ngnet-h2`'s response path requires, so the
  response body is handed back without its payload being copied.

  The qualification matters, because "zero conversion" is the kind of claim that quietly
  becomes false: axum's own `call` boxes the request body internally, one allocation per
  request, which `axum::serve` pays too. What is true is that no payload is copied. This was
  verified by building the thing before writing the specification, not by reading the trait
  bounds and hoping.

- **Graceful shutdown drains, and making it do so meant changing `ngnet-h2`.** The plan was
  `with_graceful_shutdown`, mirroring `axum::serve`. It was not implementable on the public
  API as it stood: `ngnet-h2` had no server-side way to send `GOAWAY` — `shutdown()` existed
  only on the client handle — and the server's completion signal was hard-wired never to
  fire, on the reasoning that a server does not decide when it is finished.

  The first version of this crate therefore shipped mere quiescence under the deliberately
  unfamiliar name `with_stop_signal`, so that the name would not promise a drain. That was
  the right call for an unmodified `ngnet-h2` and the wrong one to leave standing, so the
  gap was closed at the source: `Connection::drain_handle` and a completion signal that can
  actually fire. What that cost, and why the two changes could not be made separately, is in
  [`../h2/pending-work.md`](../h2/pending-work.md).

  **What "drained" means here, precisely.** On the stop signal the listener is dropped, then
  every live connection is sent a `GOAWAY` naming the last request it will answer — the last
  stream nghttp2 actually *processed*, not the highest it has seen, because the highest may
  include one already refused. Requests in flight are answered in full. Requests begun after
  that mark are refused. Each connection closes when its last stream finishes, and the
  server future resolves when the last connection has closed.

  **There is no deadline, and that is a decision rather than an omission.** A handler that
  never returns holds its connection open and holds the server open with it. Imposing a
  bound here would mean guessing what a caller's handlers are allowed to take and what
  should happen to one that overruns — questions only the caller can answer. A caller that
  wants a bound wraps the server future in a timeout, which composes exactly as well and
  says what it means.

  **The order matters and is easy to get wrong.** The listener is dropped *before* the
  connections are drained, so nothing can be accepted into a server that has already
  started saying goodbye. The drain handles are taken *before* each connection is spawned,
  because afterwards the connection has been moved into its task and there is nothing left
  to ask.

## Errors go to a callback, not into the return type

`Serve`'s future resolves to `()`. Connection failures are delivered to a closure given to
`on_error`, defaulting to doing nothing.

The alternative — resolving to `Result` — reads better and is wrong, because it forces a
choice between ending the server on the first bad connection and inventing somewhere to put
the errors it survives. A server that stops because one peer spoke HTTP/1.1 is useless, and
this crate must survive that case: it is not an edge case but the ordinary behaviour of port
scanners and misconfigured clients.

Accept errors are no longer reported here at all. `Listener::accept` yields a connection or
does not return, following `axum::serve`, so acceptance failure is the listener's to
classify and pace. That removed `ErrorKind` — with only connection failures left to report,
an enum with one variant was ceremony — and with it the `Option` on `Error::peer`, since
every remaining failure has a peer by construction.

## The backoff, and why it is an instant

This is the hardest-won piece of reasoning in the crate, and moving retry into listeners
made it *more* important rather than less, because it now has to be got right by people who
did not write the accept loop.

The backoff lives inside the accept future rather than in the `select!` arm that follows it.
An `await` in an arm body runs to completion before the loop arbitrates again, so a backoff
placed there would stop the server observing its stop signal or reaping finished connections
for the whole second. `axum::serve` puts its own sleep in the same place for the same reason.
This was caught in review, not in testing: the wrong version passed every test.

That placement then created a second, subtler bug, and the fix for it is why the backoff is
stored as an *instant* rather than a duration. A branch future that loses the race is dropped
and rebuilt on the next pass, so a relative `sleep(one second)` starts again from zero every
time another arm wins — and on a busy server the other arm, a finished connection, wins
constantly. Measured against this loop's shape, one connection completing every 100 ms
stopped a one-second backoff from *ever* elapsing: zero retries in three seconds, against
four for the same loop sleeping to an absolute deadline. The listener would have stopped
being retried for exactly as long as the server stayed busy.

Two further traps were found by review of the generic design, neither of them obvious:

- **Clearing the deadline before the sleep completes** is as bad as a relative sleep, and
  looks more correct. `if let Some(deadline) = self.backoff.take()` loses the deadline
  whenever the future is dropped mid-sleep, degenerating to a spin: measured at 34 attempts
  in 3.4 seconds against the 3 that correct pacing gives. The deadline is therefore *read*
  and cleared only after the sleep returns.
- **Awaiting a handshake inside `accept`** is unsafe here in a way it is not in axum. A TLS
  listener written the obvious way would have its handshake cancelled part-way through on
  every harvest, dropping the connection underneath it. axum does not have this problem
  because its main accept loop never drops the accept future. The advice is to return the
  un-negotiated transport and let the handshake happen on first use.

`RetryingListener` exists so that none of this has to be rediscovered. A listener supplies
one fallible accept operation and wraps it; classification, pacing and cooperative yielding
come with the wrapper. Both shipped listeners are built that way, and so is the double the
tests drive. The trait's own documentation states the hazard at the point an implementor
reads it, because `Listener` is deliberately left unsealed — axum's is open, and a crate
whose selling point is axum-parity should not be more restrictive — which means a
third-party listener can still bypass the safe path. That residual risk is accepted and
documented rather than designed away.

The other thing the loop does deliberately is rank its branches. The stop signal is
`biased` ahead of everything else, because `select!` chooses at *random* among ready
branches: with a stop signal already fired and a client already queued, an unranked loop
would admit that client about half the time, and then immediately drain it, having
served it nothing. Accepting and harvesting, by contrast, arbitrate unbiased against each other on
purpose — ranking those two would starve whichever came second under sustained load.

## Peer addresses, and the feature that would undo the crate

Handlers read `PeerAddr` from the request extensions. Its address type follows the listener:
`PeerAddr<SocketAddr>` over TCP, which is what `PeerAddr` means written bare, so nothing
changed for existing handlers; `PeerAddr<tokio::net::unix::SocketAddr>` over the Unix
listener. The parameter is defaulted precisely so that the common case reads as it did.

The address had to become generic rather than widen to some union type, and the Unix
listener is why: a client that has not bound a path is unnamed, and there is no `SocketAddr`
that could honestly have been manufactured for it. `Error` carries the same parameter rather
than erasing the address to a string. Erasure would have been cheaper at every signature —
it propagates into `Serve::on_error`'s callback type — but it serves only the caller who
logs the peer, and destroys the case for carrying an address at all: a caller shedding a
client or feeding a rate limiter needs it back as an address. The idiomatic axum answer is
`ConnectInfo<SocketAddr>`, and it is deliberately not supported: `ConnectInfo` is gated
behind axum's `tokio` feature, which depends on `hyper-util`. Enabling it to gain one
extractor would reinstate hyper in the dependency graph — the single thing the crate exists
to remove — and it would arrive transitively, where no manifest shows it and no reviewer
would see it.

That is why CI greps `cargo tree -p ngnet-axum -e normal` for hyper rather than trusting the
manifest. The check reads the *normal* graph only, because hyper is a deliberate
dev-dependency: the acceptance tests drive the server with an independent HTTP/2 client,
since a client from this workspace could only show `ngnet-h2` agreeing with itself.

## Connections are spawned; handlers are not

Each accepted connection becomes a task in a `JoinSet`. Handlers are not spawned — they run
inside their connection's future, concurrently with each other but on one task. That is
`ngnet-h2`'s design, and it has two consequences a user must know, both stated on the crate
front page: a handler that blocks the thread stalls its whole connection, and a handler that
panics fails its whole connection rather than one request.

Peer addresses are kept in a map keyed by `tokio::task::Id` rather than returned from the
task, because a panicked task returns nothing — a `JoinError` carries no value of ours. So
the two harvest arms take the id from different places: the success arm from the value, the
failure arm from `JoinError::id()`. Without the map, the failure that most needs attributing
would be the one that could not be attributed.

**The number of simultaneously accepted connections is not capped.** There is no semaphore
and no limit; the loop accepts what arrives. A caller who needs a bound has to impose it,
and `Config::max_concurrent_streams` is not a substitute — it bounds streams within a
connection, not connections.

## Tests drive the wire

The acceptance suite is built so that the plausible *wrong* implementation fails, not merely
so the right one passes, which repeatedly turned out to be a distinction with teeth.

- The streaming test deadlocks against a buffering implementation: the handler returns
  having queued only the first chunk, and the second is queued only after the client reports
  seeing the first. Nothing depends on timing.
- The concurrency-limit test ships with its own control asserting that handlers *do* run
  concurrently by default, without which it would pass against a server that was never
  concurrent at all.
- The multiplexing test originally proved nothing — hyper enqueues a request when
  `send_request` is called, so a request spawned first still reached the server second, and
  a stream-serialising server passed every assertion. It now waits for the first handler to
  park before issuing the second request, and was verified by inversion: configured with
  `max_concurrent_streams(1)`, it fails.
- Body tests cross the 65 535-byte initial flow-control window, because anything smaller
  passes without flow control being exercised at all.

Panic tests panic in the *handler*. A panic inside a response body is pulled synchronously
from an `extern "C"` callback and aborts the process, so a body-panic test would not fail —
it would kill the test binary. The suite says so where someone might be tempted to simplify
it.

## Things the tests found that nobody predicted

- **The header-list-size setting is advisory.** Advertising 256 octets and then sending a
  64 KiB header field gets a normal 200. The specification had guessed that an over-limit
  request would fail with no handler run; it was left as a spike to be measured rather than
  assumed, and the measurement contradicted the guess. It must not be used as a defence.
- **A client that vanishes mid-request is not an error**, and its in-flight handler is
  *dropped* rather than resumed or cancelled. The second has a real consequence: handler
  cleanup belongs in `Drop`, not after the `await`.
- **An outstanding response body holds a connection open.** hyper's `Incoming` keeps its
  connection alive while it exists, so a client that has not finished reading has not gone
  away, and the drain correctly waits for it — the stream is still open. This cost an afternoon of debugging a
  "hanging shutdown" that was the server being right.
