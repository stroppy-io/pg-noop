# Protocol support

What each protocol is answered with. The rules behind it are in
[`../DESIGN.md`](../DESIGN.md); the design review is in
[`review/2026-10-28-design-panel.md`](review/2026-10-28-design-panel.md).

## PostgreSQL

| Statement | Response |
|---|---|
| `SELECT` / `WITH` / `TABLE` / `VALUES` naming data | Its columns, no rows |
| A constant select list (`select 1`, `select @x`, `select @@version`) | One row of exactly those values |
| `INSERT` | `INSERT 0 0` |
| `UPDATE` | `UPDATE 0` |
| `DELETE` | `DELETE 0` |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Correct transaction tags |
| `COPY … FROM STDIN` | Accepts and discards all data |
| `COPY … TO STDOUT` | Empty stream |
| SQL `PREPARE name AS body` | Registers `name` for Bind/Execute (`42P05` if it exists) |
| Everything else | `OK` |

Both **simple query** (psql) and **extended query** (pgx, psycopg3, JDBC) are
supported, and there is no authentication: every connection is accepted. The
codec is `wire::Conn` - sans-io, so one read batch produces at most one write.

## MySQL / MariaDB

The greeting offers `mysql_native_password` and accepts `caching_sha2_password`
on its fast-auth path; any user and any password are accepted. There is **no
TLS**: `CLIENT_SSL` is not advertised, and an SSL request is answered with an
error packet rather than a handshake that cannot continue.

| Command | Response |
|---|---|
| `COM_QUERY` | A result set for a `SELECT` (0 rows), an OK packet for everything else |
| `COM_QUERY` with several statements | One answer per statement, `SERVER_MORE_RESULTS_EXISTS` on all but the last |
| `COM_STMT_PREPARE` / `EXECUTE` | Statement id, parameter and column counts; the statement's answer, parameters ignored |
| `COM_STMT_CLOSE` | Nothing (the protocol has no reply) |
| `COM_STMT_RESET` / `SEND_LONG_DATA` | OK / nothing |
| `COM_PING`, `COM_INIT_DB`, `COM_SET_OPTION`, `COM_RESET_CONNECTION` | OK (an EOF packet for `SET_OPTION`) |
| `COM_STATISTICS`, `COM_PROCESS_INFO`, `COM_FIELD_LIST` | A string, an empty result set, an EOF |
| `COM_QUIT` | The connection closes |
| Anything else | `ER_UNKNOWN_COM_ERROR` (1047) |

`BEGIN` / `START TRANSACTION` / `COMMIT` / `ROLLBACK` are tracked, and every
answer's status flags say whether a transaction is open
(`SERVER_STATUS_IN_TRANS`).

A **SELECT answers its columns and no rows**; a constant select list answers one
row, and is sent as a **binary row** (a `0x00` header, a NULL bitmap, each value
in its type's binary form) when the client prepared the statement. A command
larger than the protocol's 16 MiB packet is reassembled, a pipelined command's
answer starts its own sequence at one, and a stored routine's `BEGIN … END` body
is one statement however many semicolons it holds.

Variables a client reads at connect answer plausible values rather than an empty
set: `@@version` (`8.0.0-db-noop`), `@@max_allowed_packet` (`16777216`),
`@@autocommit` (`1`), `@@tx_isolation` (`REPEATABLE-READ`), `@@character_set_*`
(`utf8mb4`), the timeouts (`28800`), and `1` for anything else.

## Rules, failures and the profile

A TPC-C transaction (`CALL neword(…)`, `select neword(…)`, an INSERT into
`new_order`) is counted when it **commits**, and a rollback counts nothing;
autocommit is one transaction per statement on both protocols. `SIGNAL SQLSTATE`,
a NULL written into a NOT NULL column, a `RAISE` and a `CALL` of an unknown
procedure answer the error the client injected, so its rollback path runs.

With `--profile tpcc` the server also answers `select sum(d_next_o_id) from
district` (`3001 × districts + committed NewOrders`), the population counts
(`max(w_id)`, `count(*) from orders`), PostgreSQL's
`select sum(xact_commit+xact_rollback) from pg_stat_database` and `show global
status`'s `Com_commit`/`Com_rollback`. Without it those are ordinary selects:
columns, no rows. `--warehouses` sets the schema size (ten districts per
warehouse). The counters are the server's, shared across protocols, and
`--stats-every N` prints them on stderr.

## Configuration

CLI flag, then environment, then JSON file, then default. Every variable answers
to `DBNOOP_*` and the older `PGNOOP_*`; the config file is `dbnoop.json`, or
`pgnoop.json` when that is the only one there. `--help` is the list.

| Parameter | CLI flag | Default |
|---|---|---|
| Bind host | `--host` | `0.0.0.0` |
| PostgreSQL port | `--port` | `5432` |
| MySQL/MariaDB port | `--mysql-port` | off |
| Shards per protocol | `--workers` | logical CPUs |
| Warehouses | `--warehouses` | `10` |
| TPCC counters on stderr | `--stats-every` | `0` (never) |
| I/O backend | `--io` | `uring` (`epoll` also) |
| Config file | `--config` | `./dbnoop.json`, else `./pgnoop.json` |
