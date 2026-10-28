# Changelog

## 0.3.0

The crate is `db-noop`, the binary is `dbnoop`, and `pgnoop` still runs — a
second binary of the same program, so scripts written for the PostgreSQL-only
server keep working.

### MySQL / MariaDB

- **A MySQL wire-protocol listener**, on `--mysql-port` (off unless set), served
  by the same shard-per-core io_uring or epoll loop as PostgreSQL. `HandshakeV10`
  with the flags a current client negotiates; any user and password, with
  `mysql_native_password` or `caching_sha2_password`'s fast-auth path; an SSL
  request refused with an error packet, because there is no TLS.
- `COM_QUERY` (with multi-statements and `MORE_RESULTS_EXISTS`),
  `COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`/`RESET`/`SEND_LONG_DATA`, `COM_PING`,
  `COM_QUIT`, `COM_INIT_DB`, `COM_SET_OPTION`, `COM_RESET_CONNECTION`,
  `COM_STATISTICS`, `COM_PROCESS_INFO` and `COM_FIELD_LIST`; anything else is
  answered with `ER_UNKNOWN_COM_ERROR` rather than silence.
- Transactions tracked in `SERVER_STATUS_IN_TRANS`. Prepared statements answer
  **binary** rows, and their prepare response carries definitions rather than a
  column count.
- A SELECT answers a zero-row result set, except a select list of literals and
  system variables, which answers one row: `select 1` is `1`, and `@@version`,
  `@@max_allowed_packet`, `@@autocommit`, `@@tx_isolation` and the rest answer
  plausible values rather than nothing.
- A stored routine's `BEGIN … END` body is one statement, however many
  semicolons it holds.

### What it answers

- **One answer shape on both protocols.** A literal, `@var` or `@@var` select
  answers one row of exactly those values; anything that names a table, a column
  or a function answers its columns and no rows; `INSERT`/`UPDATE`/`DELETE` answer
  `INSERT 0 0`, `UPDATE 0`, `DELETE 0`. PostgreSQL's row of `1`s for an unseen
  table -- which made the same client find every item on one protocol and nothing
  on the other -- is gone.
- **A transaction is counted when it commits**, not when its statement runs, and a
  rollback counts nothing. Autocommit is one transaction per statement on both
  protocols, so a client that never says `BEGIN` still moves the counters a TPM
  check reads.
- **The failures a client injects are errors**: `SIGNAL SQLSTATE`, a NULL written
  into a NOT NULL column, a `RAISE`, and a `CALL` of a procedure with no rule.
  Answering OK to them turned every injected rollback into a commit.
- **SQL `PREPARE name AS body` registers the name** for Bind/Execute, which is how
  HammerDB's PostgreSQL TPC-C prepares at login.
- **`--profile tpcc`** turns on the synthetic numbers (the district sum, population
  counts, `pg_stat_database`, `show global status`'s values); off by default, and
  the stats line says `synthetic NOPM` when it is on.

### TPC-C

- **A rule set, so a TPC-C client can report a throughput**: transactions are
  counted by procedure name (`CALL …`, and PostgreSQL's `select … neword(…)`),
  `select sum(d_next_o_id) from district` answers `3001 × districts + NewOrders`,
  `show global status` answers `Com_commit` and `Com_rollback`, and `SELECT @…`
  answers one row. `--warehouses` sets the schema size; `--stats-every N` prints
  the counters on stderr, and they are printed once when the server stops. The
  rules are listed in full in `src/tpcc.rs` and in the README, with their limits.

### io_uring

- **Multishot receive is armed again.** A leftover probe had left every receive a
  one-shot request since 0.2.0.
- **Backpressure**: a client that pipelines and never reads its replies has its
  receive cancelled past a water mark, instead of growing the server's buffer for
  as long as it keeps writing.
- A completion that arrives while a pass is working is processed by that pass,
  rather than dropped by the next one.
- The ring is closed before the memory it points at, and a shard that stops — or
  fails — closes every descriptor it still holds.
- A provided-buffer ring the kernel refuses falls back to per-connection buffers
  instead of ending the shard; `DBNOOP_PBUF` is clamped to what the ring's index
  can address; a cancelled receive on a closing slot is reaped.
- **A shard that cannot have a ring serves on epoll instead of dying.** Ring
  memory is charged to the user, so a 64-shard process cannot have 64 rings; the
  shards that could not used to end and leave their listeners in the
  `SO_REUSEPORT` group, where connections kept being routed to a port nobody
  accepted on. The ring budget is planned from `RLIMIT_MEMLOCK` before any shard
  starts, the banner reports how many shards serve on each backend, and
  `DBNOOP_FAIL_RING=<n>` refuses the first n shards a ring for testing.

### Everything else

- Environment variables answer to both prefixes (`DBNOOP_PORT` and the old
  `PGNOOP_PORT`), and so does the config file (`dbnoop.json`, else
  `pgnoop.json`). A number in the environment that is not a number stops the
  server instead of silently becoming the default.
- `saturate` measures the MySQL listener too: `--proto mysql`.
- A Dockerfile: a static musl binary on `scratch`, both ports, both protocols.
- `scripts/release-local.sh` builds the release artifacts and their checksums
  locally, for a host with no CI minutes.

## 0.2.0

The io_uring loop: multishot accept, a shared provided-buffer ring for receives,
and the setup-flag ladder. One shard per core, pinned, with `SO_REUSEPORT`.

## 0.1.0

A no-op PostgreSQL server that saturates benchmark clients: simple and extended
query protocol, COPY, and a result set for every statement.
