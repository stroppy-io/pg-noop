# TODO: the hot path, and logging

Two pieces of work that are **not** in 0.3.0 and are not half-done in it. Each
item says what is wrong today, where, what it should be, and what it costs — the
figures are the ones the design review measured or estimated on this host
(AMD Ryzen AI MAX+ 395, 32 logical CPUs), and line numbers are as of the commit
that added this file.

The target for the first section is **≤ ~20 ns of db-noop cost per query** at
saturation depth. Today it is **~220–330 ns** — an order of magnitude more than
the loop needs, and enough that a fast client can be waiting on the server it is
supposed to be measuring. That is the one number that decides whether a db-noop
comparison is about the clients.

The reasoning and the evidence behind both sections are in
[`docs/review/2026-10-28-design-panel.md`](docs/review/2026-10-28-design-panel.md),
the 2026-10-28 design review panel's record; each item below names the finding it
answers.

## B — the hot path

**B1. Classify once, at Parse; never re-scan a statement per execution.**
`wire.rs:1444` (`answer`) rescans the SQL on every Execute — `tpcc::is_district_sum`
and `tpcc::statement_kind` walk the statement, and `row_description`
(`wire.rs:1896`) walks it twice more; on the MySQL side `classify` (`mysql/mod.rs:694`)
builds a `Shape` with `Vec<String>` column names per query, for both
`COM_QUERY` and `COM_STMT_EXECUTE`. The plan should carry a `u8` class — a rule
id plus feature bits — decided once at Parse (MySQL: at prepare), with
simple-query clients going through a per-shard intern map keyed by a hash of the
SQL bytes and a miss scanning only the first two words, allocation-free.
*Cost:* the scan is most of the 220–330 ns; the allocations are the rest of the
variance. Removing both is the bulk of the way to 20 ns.

**B2. Counters per shard, published once per pass.**
`tpcc.rs:124` is one process-wide `static STATS` of atomics: every counted
transaction is a relaxed read-modify-write on a cache line every core touches,
which is the worst shape a counter can have at 64 shards. A shard should own a
`#[repr(align(128))]` struct of plain `u64`s, increment it with `+=`, and publish
it with one store-release per loop pass before `io_uring_enter`; a reader (the
stats line, the synthetic answers) sums the published snapshots.
*Cost:* two shared-line atomics per counted transaction today; zero after. This
is the piece that must land in a **standalone loop crate** (with C1), because a
loop crate cannot depend on this server's types.

**B3. The catalog: per-shard copy-on-write, not a process-wide lock.**
`handler.rs:23` guards the schema with `RwLock`, and every metadata SELECT goes
through it, allocating a `String` and an `Arc` per column at Execute. Worse, the
"shared" catalog is not shared: `lib.rs:594` builds one `Codec` per listener, so
each *protocol's* shards share one catalog and the two protocols do not — while
`protocol.rs:53`'s comment says the catalog is common to every shard. It should
be per shard, a copy-on-write snapshot republished when DDL runs, so a metadata
SELECT is a read of a plain field; and one of the two statements about sharing
should be made true.
*Cost:* an uncontended `RwLock` acquire plus two allocations per column per
Execute, on the one path (metadata SELECT) a schema-driven client takes
constantly.

**B4. Statement and portal names: no `HashMap<String>` per Bind/Execute.**
`wire.rs:231`/`294` key statements by an owned `String`, so every Bind and
Execute hashes a name and every Parse allocates one, for the two dozen names a
client actually uses. Names should be interned to small ids per connection (a
fixed array of the last N, matched by bytes), with the `String` kept only for the
SQL text.
*Cost:* a hash of a short string plus a branch per message, on every round trip.

**B5. Replies: prebuild, and stop copying the receive tail.**
`uring.rs:1538` does `slot.pending.drain(..used)` — a memmove per read that still
has bytes pending — and the reply is rebuilt into `out` each time even when it is
constant. `uring.rs:1558` walks completions through a `Vec` rather than in place,
and `uring.rs:780` calls `sq.sync()` on every push instead of once per pass.
Replies that are a function of the statement should be prebuilt at Parse and
written from a static; the carry-over should be a ring offset rather than a
`Vec` drain; completions should be iterated in place; and `min_wait` batching
should be used where the kernel grants it.
*Cost:* the memmove is bounded by one read (32 KiB) but lands once per read under
load; the per-push `sync` is a memory barrier per message.

## C — logging, injection, reporting

The second section is about what an operator can see and cause. Both of its
first two items belong in the same standalone loop crate as B2: they are
properties of the loop, not of this server, and a loop crate must not know what a
PostgreSQL is.

**C1. NanoLog-style logging, off the loop's critical path.**
The loop calls `eprintln!` directly (`uring.rs:1386`, `1489`, `1672`, and
`lib.rs:265`/`399`/`409`): a format, a write syscall and a lock on the shard's own
thread, whenever a client does something worth mentioning. A `log!(level, "fmt
{}", x)` should expand to a static call-site record (id, level, file, line,
format string, argument type tags), with arguments encoded by a `Copy` trait that
never boxes (static strings by id, dynamic ones length-prefixed and truncated)
into a per-shard single-producer byte ring; when the ring is full it drops and
counts, and never blocks. A drain thread writes a binary log with a format table
plus an optional formatted live tail, and an offline reader binary decodes it.
A compile-time minimum level and a runtime threshold, and a per-call-site rate
limit that reports `+k suppressed`. *Keep `eprintln!` only before the loop
starts.*
*Cost:* a write syscall and a formatting pass per event today; a few stores and
a truncation check after. This is the other half of the **standalone loop crate**
with B2.

**C2. Injection that costs nothing when it is off.**
A benchmark needs a rollback path to be exercised — stroppy-rust's NewOrder rolls
back one time in a hundred, and HammerDB's PG arm raises — and today the only way
to make that happen is a real server. `--inject 40001:1%` (and `40P01`, `1213`,
`1020`) should decide per statement from a per-shard xorshift with an optional
fixed seed; `--latency-us N` should hold a finished reply and release it from a
per-shard timer wheel or a linked `IORING_OP_TIMEOUT` — never sleeping in the
shard; `--rows N` should write rows from prebuilt `DataRow` blocks. Off by
default, and a single branch when off.
*Cost:* one branch per statement when off; nothing else. Today there is no way to
ask for a simulated failure at all.

**C3. Reporting that can prove the server was not the bottleneck.**
The stats line (`--stats-every`) reports the TPCC counters but not db-noop's own
cost, so a client A/B run cannot say whether the server was ever the constraint.
It should report db-noop's CPU per query (from `/proc/self/stat` over the
interval) and each shard's busy fraction, next to the counters it already
reports, and say `synthetic NOPM` when the answers are synthetic.
*Cost:* reading two numbers per interval; nothing on the loop.
