# pg-noop

A NoOp PostgreSQL server — a black hole for benchmark tools.

Speaks the full PostgreSQL wire protocol (simple query, extended query, COPY), accepts connections from any driver, and discards everything. Returns mechanically valid but empty responses. The goal is to saturate benchmark tools and measure their maximum theoretical throughput without a real database in the way.

## Install

**One-line installer** (Linux x86-64 / ARM64, macOS):
```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/stroppy-io/pg-noop/releases/latest/download/pg-noop-installer.sh | sh
```

**Direct download** (static Linux binary, no dependencies):
```sh
wget https://github.com/stroppy-io/pg-noop/releases/latest/download/pgnoop-x86_64-unknown-linux-musl.tar.gz
tar xf pgnoop-x86_64-unknown-linux-musl.tar.gz
./pgnoop --help
```

## Build

**Regular build:**
```sh
cargo build --release
# binary: target/release/pgnoop
```

**Static binary (portable, no glibc dependency):**
```sh
# One-time setup
sudo dnf install musl-gcc musl-libc-static   # Fedora/RHEL
# sudo apt install musl-tools                # Debian/Ubuntu
rustup target add x86_64-unknown-linux-musl

cargo build --release --target x86_64-unknown-linux-musl
# binary: target/x86_64-unknown-linux-musl/release/pgnoop
```

Uses [jemalloc](https://github.com/jemalloc/jemalloc) instead of musl's default allocator to recover the performance lost in a musl build. The static binary can be `scp`'d to any x86-64 Linux server and run directly.

## Configuration

Parameters are resolved in order: **CLI flag → environment variable → `pgnoop.json` → default**.

| Parameter | CLI flag | Environment variable | Default |
|---|---|---|---|
| Bind host | `--host` | `PGNOOP_HOST` | `0.0.0.0` |
| Port | `--port` | `PGNOOP_PORT` | `5432` |
| Shards (threads) | `--workers` | `PGNOOP_WORKERS` | number of logical CPUs |
| I/O backend | `--io` | `PGNOOP_IO` | `uring` (`epoll` also available) |
| Config file path | `--config` | `PGNOOP_CONFIG` | `./pgnoop.json` |
| Startup timeout | — | `PGNOOP_STARTUP_TIMEOUT_MS` | `60000` (`0` disables) |

A connection that does not complete its startup handshake within the startup
timeout is closed, so a client that connects and never speaks cannot hold an fd
and a receive buffer indefinitely. `PGNOOP_IO=uring` falls back to `epoll` when
the kernel refuses the ring (a container's seccomp profile, or a kernel without
io_uring), and says so on stderr; `--io epoll` asks for epoll directly.

**`pgnoop.json` example:**
```json
{
  "host": "0.0.0.0",
  "port": 5432,
  "workers": 16
}
```

## Usage

```sh
# Default: listen on 0.0.0.0:5432, one thread per CPU
./pgnoop

# Custom port and thread count
./pgnoop --port 15432 --workers 8

# Via environment
PGNOOP_PORT=15432 PGNOOP_WORKERS=8 ./pgnoop

# Connect with psql
psql -h 127.0.0.1 -p 5432 -U any_user
```

## Throughput

16-core / 32-thread x86-64 host, 32 connections, `select 1`, extended protocol,
statement prepared once.

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
figures is probably not plateauing on pg-noop.

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
`PGNOOP_NO_DEFER=1` disables.

`PGNOOP_SQPOLL=<idle_ms>` is off by default and should stay off at one shard per
core: one spinning kernel poller per shard, 174× slower.

### Reproducing

```sh
cargo build --release
./target/release/pgnoop --host 127.0.0.1 &
./target/release/saturate --conns 32 --depth 1 --secs 5
```

`saturate`: raw sockets, pre-encoded pipelined messages, no allocation in the
loop, reports p50/p90/p99/p99.9/max. Conventional load tools saturate first —
four instances reached 825,238 q/s with pg-noop at 4% of one core, at which
point the figure describes the client.

## Protocol support

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

Both **simple query** (psql) and **extended query** (pgx, psycopg3, JDBC, etc.) protocols are supported. No authentication — all connections are accepted.

PostgreSQL wire-protocol framing, parsing, dispatch, and encoding use the custom sans-I/O `wire::Conn` codec. `Conn::advance` consumes bytes and appends responses without touching a socket, so each read batch can produce one write. The serving path implements the protocol directly rather than delegating it to `pgwire`.
