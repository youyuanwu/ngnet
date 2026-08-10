# Design

Why `ngnet-util` is shaped the way it is. Behaviour is documented with the code; this
records the decisions, and in particular the places where the obvious answer is wrong.

## What the crate is

A pooling HTTP/2 client over `ngnet-h2`. It is to `ngnet-h2`'s client what
`hyper_util::client::legacy::Client` is to `hyper::client::conn`, and what `ngnet-axum` is
to `ngnet-h2`'s server: the layer that turns "drive one connection" into "send a request at
a URI".

Every boundary below is a boundary rather than a stage on a roadmap:

- **Client only.** `ngnet-axum` covers the server side. There is nothing here for it.
- **h2c only.** `ngnet-h2` speaks cleartext HTTP/2 and nothing else, so there is no TLS, no
  ALPN, no protocol negotiation, and no HTTP/1 fallback. This is the single largest
  simplification against `hyper-util`, whose connector spends most of its size deciding
  which protocol both ends agreed on. Here there is nothing to decide.
- **tokio only.** `ngnet-h2` also has a `completion` transport built on compio. A second
  integration would differ only in which runtime spawns the driver task and would test
  nothing about pooling that this one does not.
- **Not published.** The API is new and expected to change, matching `ngnet-axum`,
  `ngnet-h3` and `ngnet-quic`.

## This is the layer that spawns

`ngnet-h2` never spawns a task. It returns a driver future and lets the caller decide where
it runs, which is exactly what makes it runtime-agnostic and what lets it hold a single
non-optional dependency and no dev-dependencies at all.

That property stops here, deliberately and visibly. `ngnet-util` requires tokio and spawns
one task per connection, because the alternative is handing the driver back to the caller —
and a caller managing driver tasks is a caller managing connections, which is the thing this
crate exists to stop doing.

Nothing about that leaks downwards. `ngnet-h2` was not modified to accommodate any of this,
and the boundary entry in [`../h2/pending-work.md`](../h2/pending-work.md) still holds: that
crate is still one connection with no policy layer. What changed is that the layer above it
now exists.

## Multiplexing changes what a pool is

An HTTP/1 pool is a queue of idle sockets, because a connection carries one exchange at a
time: two concurrent requests to one origin need two connections, and the pool's job is to
keep a supply of them and hand them out.

HTTP/2 multiplexes. One connection carries a hundred concurrent exchanges, so "the pool" for
an origin is almost always **one connection**, and the pool's job is not supply but
*identity*: making sure every caller for an origin converges on the same connection, and
that they all move to a new one together when the old one retires.

This has two consequences that shape the whole crate.

**There is no idle queue, so there is no idle timeout, no maximum idle count, and no
`Vec<Connection>` per origin.** A slot holds at most one connection. Most of what an HTTP/1
pool's configuration is *about* has no referent here.

**The interesting concurrency is not "hand out the next idle socket", it is "N callers
arrive at a cold origin at once".** In HTTP/1 that legitimately opens N connections. Here it
must open exactly one, and every plausible naive implementation — look up, find nothing,
dial — opens N and answers every request correctly while doing it.

### One connection per origin, and not two

The obvious extension is a second connection when the first saturates its
`MAX_CONCURRENT_STREAMS`. It is not implemented, and the reason is that it changes the
crate's central invariant to buy something no measurement here has asked for.

With one connection per origin, "the connection for this origin" is a single value that
every caller sees the same way, evictions apply to, and shutdown can drain. With N, every
one of those becomes a selection problem — which connection does a request get, which does
an eviction retire, how does a request in flight on a saturating connection relate to a
fresh one — and each is a place for a race.

A peer's `MAX_CONCURRENT_STREAMS` is commonly 100 or more, and a caller pushing past it on
one origin is a caller who already knows more about its workload than this crate does. That
caller can hold two clients. If real traffic shows the ceiling being hit in a way that
matters, the decision can be revisited with evidence; opening a second connection
speculatively is not the same thing.

## The dial state machine, and why `OnceCell` will not do

The requirement is easy to state: N concurrent requests to an origin with nothing pooled
must open one connection. The standard answer is `tokio::sync::OnceCell::get_or_try_init`,
and it is wrong for this.

When the initialiser fails, `OnceCell` does not deliver the error to the callers already
waiting on it. It releases the permit and lets one of them try again. A burst of ten
requests at an origin that is down therefore makes up to ten serial connection attempts,
each waiting for the one before to fail — the exact fan-out the primitive is being used to
prevent, appearing only on the failure path where it is least welcome and least likely to be
noticed.

So each origin has a slot holding an explicit state:

```
Idle → Dialing → Ready(handle)
              └→ Failed(error)
```

and one generation counter, incremented on every transition, that waiters park on.

The rules that make it work are worth stating because they are not obvious:

**A caller that waited takes `Failed` as its answer.** It waited for the dial that produced
that error, so that error is its result, and it does not get to try again on its own behalf.
That is precisely what turns one failed dial into N serial ones.

**A caller that did *not* wait treats `Failed` as `Idle` and dials.** A failure is a fact
about one attempt at one moment, not a property of the origin. Caching it would turn a
transient outage into a permanent one for the life of the process. This is the reading the
state machine forces, and it is why a dial failure does **not** unconditionally reach
everyone: a newcomer arriving after the failure re-dials rather than inheriting it. Everyone
who was *waiting on that dial* does see it.

**A waiter may be overtaken.** If a newcomer's dial succeeds while an earlier waiter is
still parked, the waiter gets the new connection rather than the old error. That is allowed,
and it is better for the caller.

**The generation is captured under the slot lock.** A waiter reads the counter while holding
the lock that guards the state, then parks until it differs. Subscribing after releasing the
lock and waiting for a *change* would miss a transition that landed in the window, and park
that caller for ever.

### Why the locks are synchronous

Both the pool's lock and each slot's are `std::sync::Mutex`, not `tokio::sync::Mutex`, and
that is a load-bearing choice rather than a micro-optimisation.

A `std::sync::MutexGuard` is not `Send`, so a future that holds one across an `await` does
not compile. The compiler enforces the property, on every future, permanently. This is not
theoretical: two drafts of `Pool::acquire` were rejected by the compiler for exactly this,
which is why the loop now computes a `Decision` under the lock and acts on it after
releasing. Awaiting under the lock is structurally impossible rather than merely against the
rules, and **no test asserts it** — a test would be a weaker instrument than a compile error,
and a redundant one.

The second reason is `Drop`. Both guards in this crate release their state from `Drop`, and
`Drop` cannot await. An acquire that failed to release its count would leave shutdown
waiting for a caller that has gone.

### The trap inside the generation counter

The counter lives in a `tokio::sync::watch`, and the first implementation of the transition
read:

```rust
self.settled.send_replace(self.settled.borrow().wrapping_add(1));
```

`borrow()` returns a guard over the channel's internal lock which lives to the end of the
*statement*, so it was still held when `send_replace` asked for the same lock to write.
Every dial deadlocked on its own completion.

The failure mode is the part worth recording. That lock is a blocking one, so the deadlock
took out the executor thread rather than the task. The timer never advanced, and the
`tokio::time::timeout` that the acceptance harness wraps around every request — put there
precisely so a stall names itself instead of hanging CI — never fired. A suite with bounds
on everything still hung with no output. `send_modify` takes the lock once and increments in
place.

## Keying: the origin, and what normalising it means

The key is scheme, host and port. Since the scheme is always `http`, that is host and port.

Normalisation is where the decisions are:

- **Host case is folded.** DNS is case-insensitive; `EXAMPLE.com` and `example.com` are the
  same origin and must not open two connections.
- **An omitted port becomes 80**, so `http://example.com/` and `http://example.com:80/`
  share a connection.
- **IP literals are canonicalised by parsing them**, so `[::1]`, `[0:0:0:0:0:0:0:1]` and the
  fully expanded form are one origin. Lower-casing alone does not collapse these; only
  parsing does.
- **Brackets are stripped from IPv6 literals**, because `[::1]` is URI syntax and no
  resolver accepts it. That single detail is the difference between IPv6 working and every
  IPv6 origin failing as though the host were unreachable. `Display` puts them back.
- **A trailing dot is preserved.** `example.com.` is fully qualified; `example.com` is
  subject to the resolver's search list. They can name different servers, so collapsing them
  would be wrong exactly when it mattered.
- **An empty host is rejected as a URI error.** `http://:80/` parses, and `Uri::host`
  returns `Some("")` for it. An early draft accepted that and reported it later as a
  *connect* failure, which would have told a caller with a malformed URI to look at the
  network. A test found it.

Names are resolved by `TcpStream::connect`, which resolves and tries the resolved addresses
in turn. There is no happy-eyeballs here, and no address iteration of this crate's own.
Reimplementing what the runtime already does, in order to do it slightly differently and
with no evidence that the difference is wanted, is how a connector becomes the largest part
of a client.

An explicitly supplied `SocketAddr` is not a separate path: written into a URI it is an IP
literal, which the rules above canonicalise, and `connect` resolves it trivially.

## Eviction: when a connection stops being one to use again

A pooled connection is eligible if `SendRequest::is_closed()` and `is_refusing()` are both
false. Those two predicates exist in `ngnet-h2` for exactly this, which is the strongest
evidence available that the seam between the crates is in the right place — the pool needed
no new vocabulary from the layer below.

Eligibility is checked **under the slot lock, at the moment of handing the connection out**,
not at the point of use. Checking it later lets two callers each notice one dead connection
and each replace it.

Replacement is **lazy**: a retired connection is replaced when the next request needs one,
not eagerly when the `GOAWAY` arrives. Eager re-dialling means a client that has finished
with an origin keeps a connection to it open indefinitely, re-establishing it every time the
peer retires it — an idle client generating traffic for ever. Lazily, a client that has
stopped talking to an origin stops talking to it.

A request arriving mid-eviction is not a special case, which is the point of doing the check
under the lock: it finds the slot either still holding the old connection (and evicts it
itself), already `Dialing` (and waits), or already `Ready` with the replacement (and uses
it). All three are ordinary paths.

Dropping the retired handle does not cancel the exchanges still running on it. `ngnet-h2`'s
driver holds the connection open until its stream registry empties, so an in-flight request
on a retired connection runs to completion while new requests go elsewhere.

## Retry safety: reported, never performed

The crate reports whether a failure is retriable. It never retries.

The reason is structural rather than cautious. `SendRequest::send_request` **consumes** the
request and returns only a response future. There is no error path that gives it back. A
retry would therefore need a copy of every request made before it was sent, against the
chance that one was refused — paying for every request to serve a few.

That impossibility *is* the safety property. No request can be silently replayed, because
none can be held. A caller who wants a retry still has the request, knows whether the method
is idempotent, and can decide; this crate has none of those three.

What it does provide is the one signal a caller cannot compute for itself: whether the
request was provably never acted on. `ngnet-h2` reports that as a refusal — a stream above
the peer's last-processed id in its `GOAWAY` — and it is surfaced unchanged.

With one exception, which is genuinely a different event wearing the same clothes.
`ngnet-h2` reports one category for both "the peer refused this stream" and "this end is
shutting down", because from a connection's point of view they are the same thing. They are
not the same for a caller: a peer's refusal is worth retrying elsewhere, while our own
shutdown will refuse identically for ever. The second is reclassified as `Closed`, and not
retriable.

## Queueing: there is none, and that is the design

A request arriving while its origin is being dialled waits for that dial. That is the whole
of the queueing story: there is no request queue, no bound on one, and no configuration for
it.

The obvious feature is a bounded queue that sheds load when full. It is absent because the
bound would be a guess. How many requests may usefully wait for a connection depends on how
long the connect takes and how long the caller is prepared to wait, and this crate knows
neither. A caller that wants a bound has better tools: `tokio::time::timeout` around the
request expresses "I will wait this long", and a tower concurrency layer over the `Service`
impl expresses "at most this many at once". Both are the caller's numbers.

What the crate does guarantee is that waiting is not *silent*: a caller waiting on a dial is
waiting on a dial that is actually happening, because the slot cannot be left in `Dialing`
by a dialer that went away. That is what `DialGuard` is for, and it is the reason a dropped
dialer settles the slot back to `Idle` rather than to `Failed` — nothing was learned about
the origin, so the next caller should try rather than inherit an error nobody observed.

## `tower::Service`: implemented, and not the primary API

`Client` implements `tower_service::Service<http::Request<B>>`, which mirrors how
`ngnet-axum` consumes axum's `Router` on the server side and makes every tower layer —
retry, timeout, concurrency limit, load shed — available over the top of it.

It is not the primary API. The inherent `Client::request` is, because `Service` requires
`&mut self` and a readiness protocol, neither of which this client has any use for, and a
caller should not have to hold a client mutably to send a request through a pool that is
internally shared.

`poll_ready` is always ready, and **that is not backpressure**. Readiness is asked before
the request exists, and everything that could make this client unready — whether a
connection to *that origin* exists, whether it is refusing — is a property of an origin that
arrives with the request. Reporting pending here would block every origin on one.

## Bodies: no adapters, checked rather than assumed

`ngnet-axum` records that no body adapters were needed on the server side, because
`ngnet-h2` already accepts any `http_body::Body<Data = Bytes>` and its `IncomingBody` is
already `http_body::Body<Data = Bytes> + Send + 'static`.

The same holds on the client side, in both directions, and it was checked rather than
assumed: `tests/bodies.rs` sends a body spanning several flow control windows and a
streaming body whose last chunk does not exist when the request is sent, and reads the
response through the `http_body::Body` trait alone. No payload is copied and no conversion
type exists to be maintained.

The one real constraint is that `B` is fixed per client rather than per request, because
`ngnet-h2`'s connection is generic over its request body and a pool of connections inherits
that. A caller needing several body types should box: `BoxBody<Bytes, E>` satisfies the
bound and erases the difference.

## Shutdown

`Client::shutdown` tells every connection to go away, lets exchanges already in flight
finish, and resolves when the last driver task has ended. When it returns, the connections
really are gone — not merely marked.

The ordering is the whole of it, and it is: set the flag, wait for in-flight acquires, take
the map, tell each connection to go away, drop the handles, await the drivers, publish
completion.

Waiting for the acquires **first** is what closes the race. With the flag set no new acquire
can register, so every one in flight resolves and files its driver task, and the drain that
follows sees all of them. Draining first would let a dial already in the air land
afterwards, leaving a live connection nobody is waiting for and a completion signal that
lied.

Dropping the handles is what lets each driver finish: `ngnet-h2`'s driver completes when its
handle count reaches zero *and* its stream registry is empty, so a retained handle would
hold the connection open for ever.

Every caller awaits the same completion, including the one that performed the drain. A
second caller returning early on the strength of the flag already being set would be
reporting a drain it had not observed.

**There is no deadline**, consistently with the decision recorded for the server side in
[`../h2/pending-work.md`](../h2/pending-work.md). A response body the caller never reads
holds its exchange open, which holds this pending. That is a real trap, and it is the
caller's to avoid, because only the caller knows how long is too long. A deadline chosen
here would be a guess that silently truncated somebody's upload. `tokio::time::timeout`
wraps this if a caller wants one.

Dropping the last `Client` is *not* a shutdown. The driver tasks are held in a
`Vec<JoinHandle>` rather than a `tokio::task::JoinSet` for that reason: `JoinSet` aborts its
tasks when dropped, which would cancel a response still arriving the moment the last handle
went away. `JoinHandle` detaches, which is the behaviour required.

## Errors

Four kinds, and the test suite insists each is reachable and produced by the cause that
documents it — a category nothing produces is a lie in the documentation, and two categories
produced by the same cause are one category with two names.

| Kind | What happened | Retriable |
| --- | --- | --- |
| `Uri` | The request URI has no usable origin. No peer was involved. | Never |
| `Connect` | This end could not reach the origin. Nothing was sent. | Never, on this client |
| `Closed` | This client is shutting down or has shut down. | Never |
| `Exchange` | A connection was established and the request failed on it. | When the peer said the stream was never begun |

`Connect` deliberately does **not** cover a peer that accepts the TCP connection and then
says nothing. `handshake_shared_with` is synchronous and fails only if the local session
cannot be built; the settings exchange happens afterwards on the driver. So such a peer
produces a connection that looks good at dial time and fails later as an exchange. Reporting
it as a connect failure would require holding the request until the settings arrived, which
would make every first request on every connection slower in order to serve a case that is
already reported accurately, just under a different name.

## Testing

The suite drives the real client over real loopback TCP against a real HTTP/2 server. The
server is **hyper's**, and that is the mirror image of `ngnet-axum`'s reasoning rather than a
coincidence: there, an independent *client* was needed because the server was under test.
Here the client is under test, so the server must be one this workspace did not write —
otherwise the suite would show `ngnet-h2` agreeing with itself.

### Almost everything is asserted at the peer

Every plausibly-wrong implementation of a connection pool still returns correct responses. A
pool that dials afresh for every request answers each one perfectly. A pool that serialises
every origin behind one lock answers them all, slowly. A pool that never evicts a dead
connection answers until it doesn't. So the assertions are on what the *server* saw — how
many connections it accepted, in what order requests arrived, which frames it received — and
the response is checked only to confirm the request worked at all.

`assert_eq!(server.accepts(), 1)` after three requests is the whole of "the connection was
reused". Without it, that test passes against a client that opens three sockets.

There are three deliberate exceptions, each because the claim has no observable at the peer:

- **The resolution counter.** "A pooled request resolved no name" leaves no trace at a
  server, which saw no new connection either way.
- **`has_eligible_connection`.** "The client has observed the peer's `GOAWAY`" cannot be
  reported by the peer, which learns nothing after sending one. Polling the predicate is a
  wait on the event; sleeping is a wait on a guess about it.
- **The unit tests on the two guards, and on `classify`.** The guards' failure mode is not a
  wrong answer but a hang — a dialer dropped mid-flight leaves its slot in `Dialing` for ever
  and every later caller parks behind a dial that is not happening. An end-to-end test of
  that is a test that hangs when it fails, in a suite whose whole discipline is that a stall
  must name itself. `classify` is separated out for a different reason, below.

### A hand-written peer for two frames

`tests/support/raw.rs` is a peer that speaks almost none of HTTP/2, for the two claims hyper
cannot be made to demonstrate: a `GOAWAY` naming a stream *below* one already accepted, and
the `GOAWAY` the client itself sends, which hyper consumes and reports as an
indistinguishable closing connection.

It reads the preface, sends an empty `SETTINGS`, acknowledges the peer's, and thereafter
looks at nothing but the frame type byte. Responses are one byte of HPACK — `0x88` is the
static table's entry for `:status: 200` — so no encoder, no dynamic table and no flow
control accounting are needed. A test peer complicated enough to have bugs is one whose
failures have to be diagnosed before the crate's can be.

It keeps its socket open after sending `GOAWAY`, which is the point: a peer that also hung
up would let a pool pass the eviction tests by noticing the *disconnection*, a far weaker
property that a much worse implementation also has.

### What being suspicious of green tests found

Three things, and none of them would have been found by reading the code.

**A deadlock that no timeout caught**, described above. Found by writing the first test that
actually opened a connection.

**An empty-host origin accepted as valid.** `http://:80/` parses, and `Uri::host` returns
`Some("")`. A unit test on the normalisation rules found it; the symptom would have been a
malformed URI reported as a network failure.

**A test that could not fail.** The first version of the shutdown reclassification test
raced a request against a shutdown and asserted the result was `Closed`. It passed — and it
would have passed almost however that code behaved, because `Client::request` checks for a
closed client before anything else, so reaching `send_request` at all means the client was
open then, and the branch under test only runs if a shutdown starts in the window between
the two. The test could not reliably win that race. A test that passes without entering the
branch it names is worse than no test, because it reports coverage that does not exist.

The fix was to separate the classification from the timing: `classify` is a free function,
tested directly with both values of the flag, and the timing is left honestly untested
rather than apparently covered.

Two further test-design faults were found the same way and are recorded in the commits: an
eviction test whose replacement connection inherited the retiring behaviour, so nine of ten
racing requests failed for the test's reason rather than the pool's; and a shutdown test
that asserted the peer had *recorded* the `GOAWAY` the instant `shutdown` returned, when
what `shutdown` guarantees is that the bytes were written — reading them is the peer task's
own work, and asserting on when it was scheduled is asserting on the scheduler.

### Inversion

Every behavioural claim was checked by reverting the code that makes it true and confirming
the test fails: twenty-one mutations, twenty-one failures, each one the test that names the
behaviour and not some other test noticing. A green test that has not been seen fail is an
unverified test, and a pool — dense with races, futures rebuilt on a lost `select!` branch,
and detached `JoinHandle`s that make an assertion wait for nothing — is exactly where that
matters. The full matrix is in the pull request.

One mutation survived on the first attempt, and it is the reason the exercise is worth its
cost. Deleting the `is_closed` check at the top of `Client::request` changed no test's
result, because `Pool::acquire` checks the same flag moments later and caught everything.
The branch was covered by no test at all, and its comment justified it with a benefit it did
not deliver. What it actually decides is precedence — whether a closed client with a
malformed URI is told about the closure or about the URI — and only a request that would
fail on both can tell those apart. That test now exists, and the comment now claims only
what is true.

## Dependencies

Nothing new reaches the workspace. `ngnet-util` depends on `ngnet-h2` (with its `tokio`
feature), `bytes`, `http`, `http-body`, `tokio` and `tower-service` — every one already
declared in `[workspace.dependencies]` with its rationale, and every one referenced with
`workspace = true`. `hyper`, `hyper-util` and `http-body-util` are dev-dependencies only.

CI asserts the split, in the same form and for the same reason as the axum integration's
check: `cargo tree -p ngnet-util -e normal` must contain no hyper crate. The claim is about
what a downstream user links, not about what the test binaries link, which is why the check
looks at the normal graph and deliberately ignores dev-dependencies.
