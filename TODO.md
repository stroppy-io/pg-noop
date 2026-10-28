# TODO

Not in 0.3.0 and not half-done in it. Line numbers are as of the commit that added
this file; the reasoning is in
[`docs/review/2026-10-28-design-panel.md`](docs/review/2026-10-28-design-panel.md).

**Target: ≤ ~20 ns of db-noop cost per query** at saturation depth; today it is
**~220–330 ns**. That is the number that decides whether a comparison is about the
clients.

## B — the hot path

**B1. Classify once, at Parse.** `wire.rs:1444` rescans the SQL on every Execute
(`tpcc::number_query`, `tpcc::statement_kind`) and `row_description`
(`wire.rs:1896`) twice more; `mysql/mod.rs:694` builds a `Shape` with
`Vec<String>` column names per query. Store a `u8` class on the plan (MySQL: at
prepare), intern simple-query SQL per shard by hash, and on a miss scan only the
first two words, allocation-free. *Cost: most of the 220–330 ns.*

**B2. Per-shard counters, published once per pass.** `tpcc.rs:124` is one
process-wide `static STATS` of atomics: every counted transaction is a shared
cache-line RMW at 64 shards. Give each shard a `#[repr(align(128))]` struct of
plain `u64`s, `+=` it, and publish once per loop pass before `io_uring_enter`; the
reader sums the snapshots. *Cost: two shared-line atomics per counted statement,
zero after. Belongs in the standalone loop crate, with C1.*

**B3. Copy-on-write catalog, not a lock.** `handler.rs:23` guards the schema with
`RwLock` and allocates a `String` and an `Arc` per column at Execute; the
"shared" catalog is in fact per listener (`lib.rs:594`), contradicting
`protocol.rs:53`. Make it a per-shard snapshot republished on DDL, and make one of
those two statements true. *Cost: a lock acquire and two allocations per column,
on the path a schema-driven client takes constantly.*

**B4. Intern statement and portal names.** `wire.rs:231`/`294` key them by owned
`String`, so every Bind and Execute hashes a name and every Parse allocates one,
for the dozen names a client uses. Small ids per connection, matched by bytes.
*Cost: a hash and a branch per message, every round trip.*

**B5. Prebuild replies, stop copying the receive tail.** `uring.rs:1538` drains
`pending` per read (a memmove whenever bytes remain); `uring.rs:1558` walks
completions through a `Vec`; `uring.rs:780` calls `sq.sync()` per push. Prebuild
constant replies at Parse, carry the tail as a ring offset, iterate completions in
place, sync once per pass, and use `min_wait` where the kernel grants it.

## C — logging, injection, reporting

**C1. Call-site logging into a per-shard ring.** The loop calls `eprintln!`
(`uring.rs:1386`, `1489`, `1672`, `lib.rs:265`, `399`, `409`): formatting and a
write syscall on the shard's own thread. `log!(level, "…")` should expand to a
static call-site record, encode arguments through a `Copy` trait into a per-shard
SPSC byte ring (drop-and-count when full), and let a drain thread write a binary
log with a format table plus an optional live tail; an offline reader decodes it.
Compile-time minimum level, runtime threshold, per-site rate limit. *Cost: a
syscall per event today. Belongs in the loop crate, with B2.*

**C2. Injection that costs nothing when off.** `--inject 40001:1%` (and 40P01,
1213, 1020) from a per-shard xorshift with an optional fixed seed; `--latency-us N`
by holding a finished reply and releasing it from a timer wheel or a linked
`IORING_OP_TIMEOUT`; `--rows N` from prebuilt `DataRow` blocks. *Cost: one branch
when off; there is no way to ask for a simulated failure today.*

**C3. Reporting that can show the server was not the bottleneck.** The stats line
should add db-noop's CPU per query (from `/proc/self/stat` over the interval) and
each shard's busy fraction, next to the counters, and say `synthetic NOPM`.
*Cost: two reads per interval; nothing on the loop.*

## Deferred from the clean-code review

Carried over rather than done, with the reasoning in
[`docs/review/2026-10-28-clean-code.md`](docs/review/2026-10-28-clean-code.md):

- One answer-decision type shared by both codecs (Reviewer C item 7).
- `Run`'s three flag arguments become one enum (item 9).
- The simple-query COPY arm calls `answer` instead of repeating it (item 10).
- The catalog write moves out of `answer` into dispatch (item 10).
- A `field(body, i)` helper for the RowDescription test (item 13).
- The rest of `run()`: the bind-and-spawn loop and the join.
- The remaining hand-built test frames: DESCRIBE and the COPY bodies.
- The macOS build of the library, which needs a macOS host.
