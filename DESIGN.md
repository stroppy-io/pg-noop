# Design decisions

What db-noop answers, and why it answers it that way. The reasoning behind these
— including the reviewers' arguments against the server as it was — is in
[`docs/review/2026-10-28-design-panel.md`](docs/review/2026-10-28-design-panel.md).
What is deliberately *not* here yet, and what it would cost, is in
[`TODO.md`](TODO.md).

## One answer shape on both protocols

**A row is only something the server can state for a constant select list.**
`select 1`, `select @@version` and `select @out` answer one row of exactly those
values; anything that names a table, a column or a function answers its columns
and no rows, whether or not DDL has been seen for the table. `INSERT` /
`UPDATE` / `DELETE` answer `INSERT 0 0`, `UPDATE 0`, `DELETE 0`.

The rule is one function (`sql::constant_select`) used by both codecs, because the
failure it fixes was a difference between them: PostgreSQL answered a row of `1`s
for a table the catalog had not seen while MySQL answered none, so the same
plain-SQL client found every item on one protocol and nothing on the other, and
its own code paths — and therefore its cost — depended on which port it was
pointed at. A client that is measured has to be the same client on both.

## Counting: at COMMIT, on both protocols, in the connection

A TPC-C transaction is counted when it **commits**. The kind is noted on the
connection (`tpcc::Pending`) when the statement runs, folded into the shared
counters at COMMIT, and dropped by ROLLBACK; a `COMMIT` of a failed transaction
discards, because its tag already says `ROLLBACK`. Counting when the statement ran
inflated NOPM by every failed transaction.

**Autocommit is one transaction per statement, on both protocols.** A client that
never says `BEGIN` — HammerDB's stored-procedure path does not, and neither does a
PostgreSQL client issuing a bare `insert` — still commits, and the TPM a client
reads from `Com_commit`/`xact_commit` has to move. PostgreSQL used to count only
explicit `COMMIT`s and MySQL counted autocommit, which made one protocol report a
throughput and the other report zero.

## The failures a client injects are errors

A benchmark exercises its rollback path on purpose: stroppy-rust writes a NULL
into a NOT NULL column one NewOrder in a hundred and ends the transaction with
`SIGNAL SQLSTATE '45000'`, and HammerDB's PostgreSQL arm raises. Answering OK
turns every injected rollback into a commit, so the counters move and the client's
own error handling never runs. `tpcc::failure` is the whole rule set — `SIGNAL`, a
NULL written by an INSERT/UPDATE, a `RAISE`, and a `CALL` of a procedure with no
rule — and each codec answers it with its own protocol's numbers and states.

## The synthetic answers are a profile

`--profile tpcc` turns on the statements that read like a database's: the district
sum, the population counts, `pg_stat_database`'s totals, `show global status`'s
values. Off by default, because a server that states numbers it has no data for is
manufacturing a measurement, and a client that did not ask for them may be one
that validates rows. Every value derives from `--warehouses`, the counters run
whether the profile does or not, and the stats line ends with `[synthetic NOPM]`
whenever the profile is on. An unsupported client gets no number rather than a
low one, and every figure is published next to a real-server run.

## The loop: a ring budget, and a fallback that serves

Ring memory — the queues, and the pages a provided-buffer ring pins — is charged
to the **user**, not to the shard that has it, and `RLIMIT_MEMLOCK` is 8 MiB on a
default Linux host. So the number of shards that may have a ring is planned before
any shard starts (`config::ring_plan`), each of those gets a ring sized for what is
left, and the rest serve the same codec over epoll. A shard that cannot get a ring
hands its listener back and serves on epoll rather than ending: a shard that ends
leaves its socket in the `SO_REUSEPORT` group, where the kernel keeps routing
connections to a port nobody accepts on — which is how a container answered `psql`
and refused `mariadb` on a server that was otherwise up. The banner reports how
many shards serve each protocol and on which backend, and how many are not serving
at all.

## SQL `PREPARE name AS body`

HammerDB's PostgreSQL TPC-C prepares its procedures with SQL at login and then
executes them by name; it never sends a Parse. The name is registered against its
body and is an ordinary prepared statement from then on, because a client whose
preparation is ignored is a client whose every execution is 26000 and whose run
measures zero.

## Not decided here

Two sections of `TODO.md` are design work this file does not record, because they
are not built: the hot path (~220–330 ns per query now, ~20 ns wanted, and the
per-shard counters and call-site logging that belong in a standalone loop crate)
and the loop furniture (opt-in injection, a stats line that can show the server
was not the bottleneck).
