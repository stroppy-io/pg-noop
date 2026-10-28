# db-noop

A no-op database server: PostgreSQL and MySQL/MariaDB wire protocols, answering
every statement with a valid, empty response. It exists so that a **benchmark
client** can be measured against a server that does no work. Against a real
database, two clients make the server do the same work and the comparison
measures the server; against db-noop it measures them.

## Quickstart

```sh
cargo build --release
./target/release/dbnoop --port 5432 --mysql-port 3306 --workers 16

psql  -h 127.0.0.1 -p 5432 -U any_user
mysql -h 127.0.0.1 -P 3306 -u any_user
```

The PostgreSQL listener is on by default; the **MySQL listener only when
`--mysql-port` is given**. `--profile tpcc` turns on the numbers a TPC-C client
reads its throughput from - off by default.

```sh
docker run --network host --security-opt seccomp=unconfined \
  ghcr.io/stroppy-io/db-noop:0.3.0 --port 5432 --mysql-port 3306 --profile tpcc
```

io_uring needs the seccomp profile opened; without it the server says so and
serves the same codec over epoll. `--network host` keeps the kernel's
`SO_REUSEPORT` balancing in the loop, with no proxy in the measurement.

## Which clients it measures

| Client | Runs | Needs |
|---|---|---|
| HammerDB, PostgreSQL TPROC-C (function mode) | yes | `--profile tpcc` |
| HammerDB, MySQL TPROC-C (stored procedures) | yes | `--profile tpcc` |
| stroppy-rust, both protocols | yes | `--no-verify --profile tpcc` |
| stroppy-go | **no** - it validates the rows it reads, and there are none | |
| HammerDB, MySQL client-side SQL arm | **no** - it needs point lookups, and there is no data | |

An unsupported client gets **no number, not a low one**. Every db-noop figure is
published next to a real-server run: it says how much of a client's cost the
database was not, and it is not a TPC-C result.

## What it answers

A SELECT answers its columns and no rows; `INSERT`/`UPDATE`/`DELETE` answer
`INSERT 0 0`/`UPDATE 0`/`DELETE 0`; a constant select (`select 1`, `select
@@version`) answers one row of exactly those values. A failure a benchmark
injects (`SIGNAL`, `RAISE`, an unknown `CALL`) is answered as the error it is, so
the client's rollback path runs and counts nothing; a NULL written into a NOT NULL
column is one too, under `--profile tpcc` only, where writing one is the injection
rather than the data.

`--profile tpcc` adds the numbers a TPC-C client reads: the district sum, the
population counts, `pg_stat_database`'s transaction totals and `show global
status`'s commit counters, all derived from `--warehouses`. The counters run
whether the profile does or not, and the stats line says `synthetic NOPM` when it
is on.

## Where to read more

- [`docs/protocols.md`](docs/protocols.md) - every command and statement, and what
  it is answered with, on both protocols.
- [`docs/measurements.md`](docs/measurements.md) - throughput, the io_uring ring,
  and how to reproduce it with `saturate`.
- [`DESIGN.md`](DESIGN.md) - why these answers, and the rules behind them.
- [`docs/review/2026-10-28-design-panel.md`](docs/review/2026-10-28-design-panel.md)
  - the design review, verbatim.
- [`TODO.md`](TODO.md) - the hot path and the logging, with their costs.

## Configuration and build

`--help` lists every flag; `DBNOOP_*` (or the older `PGNOOP_*`) sets each from the
environment, and `dbnoop.json` (or `pgnoop.json`) from a file. `dbnoop` and
`pgnoop` are the same program under two names. `scripts/release-local.sh` builds
the static musl artifacts and their checksums; `dist` builds them in CI.
