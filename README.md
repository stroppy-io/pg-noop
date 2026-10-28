# db-noop

A NoOp database server — a black hole for benchmark tools. PostgreSQL and
MySQL/MariaDB wire protocols, one shard-per-core loop, and an answer for every
statement that is valid but does nothing.

The point is to measure a **client**. Against a real database, two clients make
the server do the same work, so a comparison between them measures the server.
Against this one the server does nothing, so it measures them: their parsing,
their connection handling, their protocol round trips and their own bookkeeping.
That is what the second protocol is for — `stroppy-rust` against MariaDB and
`HammerDB` against MariaDB cannot be compared while MariaDB is what they are
waiting on; against db-noop they can.

## Install

**One-line installer** (Linux x86-64 / ARM64, macOS):
```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/stroppy-io/pg-noop/releases/latest/download/db-noop-installer.sh | sh
```

**Direct download** (static Linux binary, no dependencies):
```sh
wget https://github.com/stroppy-io/pg-noop/releases/latest/download/dbnoop-x86_64-unknown-linux-musl.tar.gz
tar xf dbnoop-x86_64-unknown-linux-musl.tar.gz
./dbnoop --help
```

The repository is still named `pg-noop`, and the binary still goes by two names:
`dbnoop` is its own, and `pgnoop` is kept so that every script written for the
PostgreSQL-only server keeps working. They are the same program.

## Build

**Regular build:**
```sh
cargo build --release
# binaries: target/release/dbnoop, target/release/pgnoop, target/release/saturate
```

**Static binary (portable, no glibc dependency):**
```sh
# One-time setup
sudo dnf install musl-gcc musl-libc-static   # Fedora/RHEL
# sudo apt install musl-tools                # Debian/Ubuntu
rustup target add x86_64-unknown-linux-musl

cargo build --release --target x86_64-unknown-linux-musl --bin dbnoop
# binary: target/x86_64-unknown-linux-musl/release/dbnoop
```

Uses [jemalloc](https://github.com/jemalloc/jemalloc) instead of musl's default
allocator to recover the performance lost in a musl build. The static binary can
be `scp`'d to any x86-64 Linux server and run directly.

## Docker

```sh
docker build -t db-noop .
docker run --network host db-noop
# or the published image, one tag per release
docker run --network host ghcr.io/stroppy-io/db-noop:0.3.0 --port 5432 --mysql-port 3306
```

The image is a static musl build of `dbnoop` on `scratch` — nothing but the
binary — and its `CMD` already binds `0.0.0.0` on both ports (5432 and 3306).
Nothing in it needs a shell or a libc.

**io_uring needs the host's seccomp profile opened**, because Docker's default
refuses `io_uring_setup`:

```sh
docker run --network host --security-opt seccomp=unconfined ghcr.io/stroppy-io/db-noop:0.3.0
```

Without it the server says so and serves with epoll, which is correct and slower;
`--io epoll` asks for it. `--network host` is not decoration: one shard per core
behind `SO_REUSEPORT` is the point of the thing, and a NAT'd port with a proxy in
front measures the proxy.

## Configuration

Parameters are resolved in order: **CLI flag → environment variable → JSON
config file → default**. Every environment variable answers to two names, the
new one and the PostgreSQL-only one: `PGNOOP_PORT` keeps working, and `DBNOOP_PORT`
is read first when both are set.

| Parameter | CLI flag | Environment variable | Default |
|---|---|---|---|
| Bind host | `--host` | `DBNOOP_HOST`, `PGNOOP_HOST` | `0.0.0.0` |
| PostgreSQL port | `--port` | `DBNOOP_PORT`, `PGNOOP_PORT` | `5432` |
| MySQL/MariaDB port | `--mysql-port` | `DBNOOP_MYSQL_PORT`, `PGNOOP_MYSQL_PORT` | off |
| Shards (threads) per protocol | `--workers` | `DBNOOP_WORKERS`, `PGNOOP_WORKERS` | number of logical CPUs |
| Districts per schema | `--warehouses` | `DBNOOP_WAREHOUSES`, `PGNOOP_WAREHOUSES` | `10` |
| TPCC counters on stderr | `--stats-every` | `DBNOOP_STATS_EVERY`, `PGNOOP_STATS_EVERY` | `0` (never) |
| I/O backend | `--io` | `DBNOOP_IO`, `PGNOOP_IO` | `uring` (`epoll` also available) |
| Config file path | `--config` | `DBNOOP_CONFIG`, `PGNOOP_CONFIG` | `./dbnoop.json`, or `./pgnoop.json` |
| Startup timeout | — | `DBNOOP_STARTUP_TIMEOUT_MS`, `PGNOOP_STARTUP_TIMEOUT_MS` | `60000` (`0` disables) |

The MySQL listener is **off unless `--mysql-port` is given**: a server that opens
a second port nobody asked for is one an operator has to remember to close.
`--workers` is shards *per protocol*, so a server with both ports open runs twice
as many threads, and pinning is skipped entirely when they do not fit on distinct
CPUs.

A connection that does not complete its startup handshake within the startup
timeout is closed, so a client that connects and never speaks cannot hold an fd
and a receive buffer indefinitely. `DBNOOP_IO=uring` falls back to `epoll` when
the kernel refuses the ring (a container's seccomp profile, or a kernel without
io_uring), and says so on stderr; `--io epoll` asks for epoll directly.

**`dbnoop.json` example:**
```json
{
  "host": "0.0.0.0",
  "port": 5432,
  "mysql_port": 3306,
  "workers": 16,
  "warehouses": 10
}
```

## Usage

```sh
# PostgreSQL only, as before
./dbnoop --port 5432 --workers 16

# Both protocols: one worker set each
./dbnoop --port 5432 --mysql-port 3306 --workers 16

# Via environment (either prefix)
DBNOOP_PORT=15432 DBNOOP_MYSQL_PORT=13306 ./pgnoop

# Connect with psql
psql -h 127.0.0.1 -p 5432 -U any_user

# Connect with mysql
mysql -h 127.0.0.1 -P 3306 -u any_user

# The stores a benchmark reads its throughput from, on stderr
./dbnoop --mysql-port 3306 --warehouses 10 --stats-every 10
```

## Protocol support

### PostgreSQL

| Statement | Response |
|---|---|
| `SELECT` / `WITH` / `TABLE` / `VALUES` | Empty result set (0 rows) |
| `INSERT` | `INSERT 0 0` |
| `UPDATE` | `UPDATE 0` |
| `DELETE` | `DELETE 0` |
| `BEGIN` / `COMMIT` / `ROLLBACK` | Correct transaction tags |
| `COPY … FROM STDIN` | Accepts and discards all data |
| `COPY … TO STDOUT` | Empty stream |
| Everything else | `OK` |

Both **simple query** (psql) and **extended query** (pgx, psycopg3, JDBC, etc.)
protocols are supported. No authentication — all connections are accepted.

PostgreSQL wire-protocol framing, parsing, dispatch, and encoding use the custom
sans-I/O `wire::Conn` codec. `Conn::advance` consumes bytes and appends responses
without touching a socket, so each read batch can produce one write. The serving
path implements the protocol directly rather than delegating it to `pgwire`.

### MySQL / MariaDB

The greeting offers `mysql_native_password` and accepts
`caching_sha2_password` on its fast-auth path; any user and any password are
accepted. There is **no TLS**: `CLIENT_SSL` is not advertised, and an SSL request
is answered with an error packet that says so rather than a handshake that
cannot continue.

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

`BEGIN` / `START TRANSACTION` / `COMMIT` / `ROLLBACK` are tracked, and the status
flags of every answer say whether a transaction is open (`SERVER_STATUS_IN_TRANS`)
— which is how a pool knows the connection it is about to hand out has one.

A **SELECT answers a well-formed result set with zero rows**, named after its
select list: no value of the server's is worth making up, and an empty result is
what every driver accepts. The one exception is a select list of nothing but
literals and system variables (`select 1`, `select @@version`), which answers one
row — see the TPC-C rules below. A command larger than the protocol's 16 MiB
packet is reassembled; a pipelined command's answer starts its own sequence at
one, and a stored routine's `BEGIN … END` body is one statement however many
semicolons it holds.

## TPC-C rules

A TPC-C client reports a throughput it READ FROM THE SERVER: HammerDB takes
`select sum(d_next_o_id) from district` before and after its window and reports
the difference as NOPM, and reads `show global status` for `Com_commit` and
`Com_rollback` to report TPM. Against a server that answers nothing those are
zero, and the run measures nothing — the one thing this server exists to avoid.

So there is a small, explicit rule set, listed in full in `src/tpcc.rs`. It is
not SQL: each rule is the shape of a statement, matched on its own words.

| Rule | Answer |
|---|---|
| `CALL NEWORD(...)` — also `PAYMENT`, `DELIVERY`, `OSTAT`/`order_status`, `SLEV`/`stock_level`, any case, optional schema prefix | OK packet; the transaction is counted |
| A SELECT that CALLS one of them as a function — `select neword(…)`, `SELECT * FROM neword(…)`, PostgreSQL's form of the same, plain or prepared | The ordinary SELECT answer; the transaction is counted |
| `INSERT` that names `new_order` (or `tpcc_new_order`) | OK packet; counted as a NewOrder |
| `select sum(d_next_o_id) from district` (any case and spacing, optional schema prefix, `district` or `tpcc_district`, nothing after the table) | One row: `3001 × districts + NewOrders seen` |
| `show global status` [`like '<pattern>'`] | A result set of `Com_commit` and `Com_rollback` (MySQL's `%`/`_` matching) |
| `select @a, @b` (variables only) | One row, one column per variable, every value `1` |
| A select list of literals and `@@variables` (`select 1`, `select @@version`) | One row of exactly those values |

**A row where a row can be stated.** `select 1` answers one row (`1`), because a
constant select list is a value the server knows without knowing anything else —
PostgreSQL has always answered one here, and an empty answer made `mariadb -e
'select 1'` print nothing at all. A select list that names a table, a column or a
function keeps its empty result set, and a statement executed as a prepared
statement answers its rows in the **binary protocol** (a `0x00` header, a NULL
bitmap, and each value in its type's binary form), which is what a client that
prepared it decodes.

System variables a client reads at connect answer plausible values rather than an
empty set: `@@version` (`8.0.0-db-noop`), `@@max_allowed_packet` (`16777216`),
`@@autocommit` (`1`), `@@tx_isolation`/`@@transaction_isolation`
(`REPEATABLE-READ`), `@@character_set_*` (`utf8mb4`), the timeouts (`28800`), and
`1` for anything this server has not heard of.

`districts` is ten per warehouse, and `--warehouses` says how many there are (ten
by default, which is HammerDB's own default schema). A statement outside a
transaction counts as its own commit on the MySQL side, where a client's
autocommit is implicit; the PostgreSQL side counts explicit `COMMIT`/`ROLLBACK`,
which is what its clients send. Counters are counted where a statement RUNS, so a
prepared statement counts once per execution, and they are process-wide because
they are a fact about the server rather than about a connection.

The counters are also on stderr: `--stats-every N` prints them every N seconds,
and the same line is printed once when the server stops.

```sh
$ ./dbnoop --mysql-port 3306 --stats-every 10
tpcc: new_order=1204 payment=1207 delivery=241 order_status=241 stock_level=241 com_commit=3134 com_rollback=0 district_sum=313404 warehouses=10
```

**What the rules are not.** They are not a TPC-C implementation: no table is
stored, no statement is checked, and the numbers they answer are consistent with
each other and with nothing else. A client that validates its own data — checks a
row it inserted, sums a column it populated — will not find it, exactly as with
the empty result sets everywhere else. They exist so that a client's own
throughput report has something true-shaped to read.

## Throughput

Measurements are of the **server's** ceiling, not of a database: the number a
benchmark plateaus near, in queries per second, with the reply count taken from
the wire.

### Where the numbers come from

Two things changed the io_uring loop after the table below was first published,
and both change what it measures:

* **23210e4** left multishot receive switched off (`let mut multishot = false; //
  PROBE`, a leftover probe). Every receive was a one-shot request, so the
  provided-buffer ring was drained and refilled once per read.
* This branch arms multishot receive again, and adds backpressure: a client that
  pipelines and never reads its replies has its receive cancelled past a water
  mark instead of growing the server's buffer without bound.

The table below is **the one-shot loop's** — it is what `23210e4` measured, kept
because it is the figure the earlier README published. The multishot numbers
follow it.

### The one-shot loop (23210e4, 16-core/32-thread host, 32 connections)

`select 1`, extended protocol, statement prepared once.

| pipeline depth | before | after | ratio |
|---|---|---|---|
| 1 | 783,639 | **3,318,093** | **4.2** |
| 8 | 1,670,803 | 20,724,935 | 12.4 |
| 32 | 1,811,259 | 49,544,744 | 27.4 |

Medians of three runs on an otherwise idle host (99.5% idle before the run);
before-bands within 0.6%. **Measure on a quiet machine**: the pre-change server
uses unpinned work-stealing threads and loses 2.6× under CPU contention
(787,075 → 307,343 q/s with 16 cores busy), while the shard-per-core version is
largely unaffected. A contended host therefore exaggerates the difference. Depth 1 is one query per round
trip. PostgreSQL 18.6, same host and client, answers 548,217 q/s at that depth.

### This branch: multishot receive, measured on a shared 32-core host

Commit `b344b37`, AMD Ryzen AI MAX+ 395 (32 logical CPUs, `7.0.0-30-generic`), **four
shards** and **32 connections**, `DBNOOP_PBUF=8`, `saturate` at 5 s, medians of
three runs each, `23210e4` (one-shot) against this branch (multishot), both
binaries run alternately. Four shards rather than 32 because this box's
`RLIMIT_MEMLOCK` is 8 MiB and a provided-buffer ring is what the kernel pins at
registration: a 32-shard run on this host loses shards to `ENOMEM` and measures a
smaller server instead.

| depth | PostgreSQL, one-shot (q/s) | PostgreSQL, multishot (q/s) | MySQL, multishot (q/s) |
|---|---|---|---|
| 1 | 961,686 | 928,413 | 846,188 |
| 8 | 5,879,400 | 5,665,313 | 5,451,905 |
| 32 | 12,816,730 | 12,192,955 | 11,122,569 |

The PostgreSQL columns are one session's, base and tip alternating; the MySQL
column was taken in a later pass on the same box, and repeats of it moved
between 10.9M and 11.6M at depth 32. Read these as *the box was the
bottleneck*: the host carried another tenant's load average of 4–12 throughout,
and the same binary repeated at one setting moved by 25% between runs. What survives is the shape — depth 1 is round-trip
bound (one answer per round trip), depth 8 and 32 are amortised, and MySQL costs
what PostgreSQL costs, because the codec does the same work and the loop moves
the same bytes. The per-query cost of the codec itself is in *Limit* below, which
was measured on an idle host.

### Limit

| depth | q/s | µs/query |
|---|---|---|
| 1 | 3,318,093 | 8.000 |
| 2 | 5,333,392 | 4.500 |
| 4 | 10,651,482 | 2.000 |
| 8 | 18,012,011 | 1.500 |
| 16 | 31,548,029 | 0.875 |
| 32 | 46,320,724 | 0.594 |
| 64 | 71,358,174 | 0.344 |

Per-query cost falls 23× under amortisation; per-batch median is flat.
Decomposition: ~0.3 µs work, ~7.7 µs kernel TCP loopback. **At depth 1 the
server is round-trip bound, not work bound.** A benchmark plateauing near these
figures is probably not plateauing on db-noop. (Measured on the idle 16-core host
of the one-shot table, PostgreSQL, one worker set.)

### I/O backends

Shard-per-core over the same codec, differing only in byte movement. Selected
with `--io`.

| | syscalls/query | q/s at depth 1 |
|---|---|---|
| `epoll` | 3.00 (`epoll_wait` + `recvfrom` + `sendto`) | 2,886,260 |
| `uring` | 1.51 (`io_uring_enter`) | 2,944,008 |

io_uring halves syscalls for +2%: the codec had already reduced the reply to one
write.

`SINGLE_ISSUER` + `DEFER_TASKRUN` (Linux 6.1+) are default on and worth +19% at
depth 1 over plain io_uring; without them io_uring is slower than epoll.
`DBNOOP_NO_DEFER=1` disables.

`DBNOOP_SQPOLL=<idle_ms>` is off by default and should stay off at one shard per
core: one spinning kernel poller per shard, 174× slower.

### What the ring is granted

`uring` probes every ring feature the kernel has, one setup flag at a time, and
reports the granted set on stderr at startup and in the listen banner. A flag
`io_uring_setup` will not take is dropped *after the errno that refused it*,
never silently; so is a feature that a later call refuses.

On Linux 7.0 (this box):

```
dbnoop: io_uring granted: SINGLE_ISSUER DEFER_TASKRUN COOP_TASKRUN TASKRUN_FLAG
        NO_SQARRAY CLAMP SUBMIT_ALL CQSIZE RING_FD PBUF_RING
        (PBUF_RING_INC granted, unused: a buffer's ownership is not reported
         while a receive holds it)
```

* `CQSIZE` is four times the ring's connection capacity (4096 submission entries
  → 16384 completions), so a burst of completions is drained rather than pushed
  into the kernel's overflow list. If a ring cannot get that memory
  (`ENOMEM` — a process's ring memory is bounded, 8 MiB of `mlock` on this box),
  the shards say so and keep the kernel's own completion queue.
* `RING_FD` (`IORING_REGISTER_RING_FDS`) lets `io_uring_enter` name the ring by
  a registered index instead of the raw descriptor.
* `PBUF_RING` is one registered provided-buffer ring per shard, shared by every
  connection on it: a connection no longer owns a 32 KiB zeroed receive buffer,
  and a shard's receive memory is the ring, not the connection count. A buffer is
  taken per read and given straight back -- one-shot consumption, because with
  incremental consumption the kernel does not report when it gives a buffer up,
  and a buffer's ownership cannot be followed from the completion queue.
* **Multishot receive** is one request per connection that the kernel re-issues
  by itself; a kernel that will not take it says so once and every receive is a
  one-shot instead.
* **Backpressure**: a reply is staged while a write is in flight, and past
  256 KiB of staged reply the receive is cancelled until the write drains. A
  client that pipelines and never reads its answers is then pushed back through
  its own receive window instead of growing the server's memory.

`DBNOOP_PBUF=<n>` sizes that ring (default 256 buffers, 32 KiB each; the mapping
is anonymous and only buffers that have been received into are resident). The
value is clamped to the 32768 a ring's index can address, and said so.

### Reproducing

```sh
cargo build --release
DBNOOP_PBUF=8 ./target/release/dbnoop --host 127.0.0.1 --port 15432 --mysql-port 13306 --workers 4 &
./target/release/saturate --addr 127.0.0.1:15432 --conns 32 --depth 1 --secs 5
./target/release/saturate --proto mysql --addr 127.0.0.1:13306 --conns 32 --depth 1 --secs 5
```

`saturate`: raw sockets, pre-encoded pipelined messages, no allocation in the
loop, reports p50/p90/p99/p99.9/max. It speaks both protocols: `--proto postgres`
(the default) counts `ReadyForQuery`, and `--proto mysql` sends one `COM_QUERY`
packet per query and counts the packets that end an answer. Conventional load
tools saturate first — four instances reached 825,238 q/s with db-noop at 4% of
one core, at which point the figure describes the client.

## What has not been verified here

* **No real MySQL client library was driven against this server** *in the
  sandbox this branch was written in*: it has no network and no MySQL client
  crate in its registry, so the integration test drives the server with a client
  written against the protocol itself (`tests/wire.rs`). The CI job `Real
  clients` does the other half — the official `mariadb` and `psql` binaries
  against the release build — and it is the job to read first if a driver
  refuses the greeting:

  ```sh
  act -j clients -P ubuntu-24.04=catthehacker/ubuntu:act-24.04 --bind \
      --container-options "--security-opt seccomp=unconfined"
  ```

  The seccomp flag is what lets the job use io_uring; without it the server says
  so and serves with epoll, which the job still passes. That checks the framing, the
  sequencing, the status flags and the result-set shapes a driver reads, but not
  a driver's own opinions — `mysql_async`'s handshake parsing, or MariaDB's
  connector, are untested. If one of them refuses the greeting, the first thing
  to check is the auth plugin and the capability bits it was offered.
* **HammerDB and stroppy-rust were not run against it** (no network, no MariaDB,
  no HammerDB). What their TPC-C paths need is implemented from their documented
  statement shapes: `CALL <proc>` per transaction, `select sum(d_next_o_id) from
  district` before and after, and `show global status` for the commit counters.
  The rules are pinned by tests in both protocols, and `tests/wire.rs` drives the
  exact statements a HammerDB run issues.
* The **throughput figures above** were taken on a shared host whose load average
  moved between 4 and 12 during the runs. They are medians with their spread
  visible in the table above, not a quiet-machine measurement; the one-shot table
  and *Limit* were measured on an idle host and say so.
