# Design decisions

Why db-noop answers what it answers. The review that argued for these is
[`docs/review/2026-10-28-design-panel.md`](docs/review/2026-10-28-design-panel.md);
what is not built yet is in [`TODO.md`](TODO.md).

## One answer shape on both protocols

A row is only something the server can state for a constant select list.
`select 1`, `select @@version` and `select @out` answer one row of exactly those
values; anything naming a table, a column or a function answers its columns and no
rows, whether or not DDL was seen; `INSERT`/`UPDATE`/`DELETE` answer
`INSERT 0 0`/`UPDATE 0`/`DELETE 0`.

PostgreSQL used to answer a row of `1`s for an unseen table while MySQL answered
none, so the same plain-SQL client found every item on one protocol and nothing on
the other - and its cost depended on which port it was pointed at.

## Counted at COMMIT, on both protocols

The transaction kind is noted on the connection, folded into the counters at
COMMIT, and dropped by ROLLBACK; a COMMIT of a failed transaction discards, as its
tag already says. Autocommit is one transaction per statement, so a client that
never says `BEGIN` - HammerDB's stored-procedure path does not - still moves the
counters a TPM check reads. Counting when the statement ran inflated NOPM by every
failed transaction, and counting only explicit COMMITs made PostgreSQL report zero.

## Injected failures are errors

A benchmark exercises its rollback path on purpose: `SIGNAL SQLSTATE '45000'`,
`RAISE`, an unknown `CALL`, and under `--profile tpcc` a NULL written into a NOT
NULL column. Answering OK turns every injected rollback into a commit, so
`tpcc::failure` matches those four and each protocol answers with its own numbers and
states. **The NULL is the profile's alone**: off, a NULL is data, and a client
writing one into a nullable column is not injecting anything -- the same profile
that decides the synthetic answers decides this.

## The synthetic answers are a profile

`--profile tpcc` turns on the district sum, the population counts,
`pg_stat_database` and `show global status`; every value derives from
`--warehouses`. Off by default, because numbers the server has no data for are a
manufactured measurement, and a client that did not ask for them may validate rows.
The counters run regardless, and the stats line says `[synthetic NOPM]`. An
unsupported client gets no number, not a low one.

## A ring budget, and a fallback that serves

Ring memory is charged to the user, not the shard, and `RLIMIT_MEMLOCK` is 8 MiB
by default. The number of shards that may have a ring is planned before any shard
starts; the rest serve the same codec over epoll. A shard that cannot get a ring
hands its listener back rather than ending, because a dead shard leaves its socket
in the `SO_REUSEPORT` group and the kernel keeps routing connections to a port
nobody accepts on. The banner reports the split.

## SQL `PREPARE name AS body`

HammerDB's PostgreSQL TPC-C prepares its procedures with SQL at login and never
sends a Parse, so the name is registered against its body and behaves as an
ordinary prepared statement from then on.
