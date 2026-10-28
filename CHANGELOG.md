# Changelog

## 0.3.0

The crate is `db-noop`, the binary is `dbnoop`, and `pgnoop` still runs — the same
program under the older name.

**MySQL / MariaDB.** A wire-protocol listener on `--mysql-port` (off by default):
handshake with `mysql_native_password` or `caching_sha2_password`, no TLS, an SSL
request refused with an error; `COM_QUERY` with multi-statements, the
`COM_STMT_*` family with binary rows, the transaction state in the status flags,
and every statement answered as a result set or an OK packet.

**Answers, on both protocols.** A SELECT answers its columns and no rows; a
constant select answers one row; `INSERT 0 0`/`UPDATE 0`/`DELETE 0`; an injected
failure (`SIGNAL`, a NULL into a NOT NULL column, `RAISE`, an unknown `CALL`) is
an error. SQL `PREPARE name AS body` registers the name for Bind/Execute.

**TPC-C.** `--profile tpcc` answers the district sum, the population counts,
`pg_stat_database` and `show global status` from `--warehouses`, and the stats
line says `synthetic NOPM`. A transaction is counted when it commits, on both
protocols, and a rollback counts nothing.

**The loop.** Multishot receive armed again, with backpressure for a client that
never reads; a completion that arrives mid-pass is processed by that pass; the
ring is closed before the memory it points at; ring memory is planned from
`RLIMIT_MEMLOCK`, and a shard that cannot have a ring serves on epoll instead of
dying and leaving its port with one listener fewer.

**Packaging.** A Docker image (static musl on `scratch`, both ports),
`scripts/release-local.sh` for artifacts without CI, `saturate --proto mysql`, and
`DBNOOP_*` environment names with `PGNOOP_*` still read.

## 0.2.0

The io_uring loop: multishot accept, a shared provided-buffer ring, and the
setup-flag ladder. One shard per core, pinned, behind `SO_REUSEPORT`.

## 0.1.0

A no-op PostgreSQL server: simple and extended query protocol, COPY, and a result
set for every statement.
