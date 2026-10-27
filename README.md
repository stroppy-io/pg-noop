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

### What the ring is granted

`uring` probes every ring feature the kernel has, one setup flag at a time, and
reports the granted set on stderr at startup and in the listen banner. A flag
`io_uring_setup` will not take is dropped *after the errno that refused it*,
never silently; so is a feature that a later call refuses.

On Linux 7.0 (this box):

```
pgnoop: io_uring granted: SINGLE_ISSUER DEFER_TASKRUN COOP_TASKRUN TASKRUN_FLAG
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
  and a shard's receive memory is the ring, not the connection count.
* `PBUF_RING_INC` (`IOU_PBUF_RING_INC`, incremental consumption) is granted and
  **not used**. A buffer stays with the receive that filled it until it is full,
  and the kernel publishes no completion when it gives one up: a connection whose
  first read is 19 bytes holds that buffer while other connections go without,
  and its later completions refer to a different buffer with the first never
  reported. Measured here; the ownership cannot be followed from the completion
  queue, so the ring is used one-shot -- every completion carries a buffer and it
  goes straight back.
`PGNOOP_PBUF=<n>` sizes that ring (default 256 buffers, 32 KiB each; the mapping
is anonymous and only buffers that have been received into are resident).

`uring` also retries what is not a failure: a signal interrupting the wait *or a
submit* (EINTR), and a completion ring the kernel could not flush (EBUSY: the
completions are drained, then the submit is repeated). Both used to end the
shard.

### Cost of the provided-buffer receive

Base `59065e7` against this change (`27fa72a`), same binary and settings, one
worker pinned to a core, 16 connections, `saturate` at depth 1/8/32, medians of
three runs. **Server CPU per query**, from `/proc/<pid>/stat`, is the figure that
survives this host: it carried another tenant's load average of 18-22 throughout,
which is why wall-clock throughput is reported with its spread rather than as a
ratio.

| depth | base µs/query | tip µs/query | base q/s (median, 4 workers/32 conns) | tip q/s (median) |
|---|---|---|---|---|
| 1 | 3.96 | **3.78** | 679,713 | 393,240 |
| 8 | 0.842 | **0.817** | 4,146,810 | 2,144,244 |
| 32 | 0.359 | 0.379 | 9,466,866 | 5,478,843 |

Per-query server cost is unchanged within noise (±5%, and the sign flips between
depths). The wall-clock columns are not: they were taken alternately, base then
tip, on a box another tenant kept at a load average of 18-22, and there the
client is the bottleneck -- the same binary repeated at one setting moved between
795k and 248k q/s. Syscalls per query at depth 1 (8 connections, `strace -c -f -p`
on the server): 0.272 for the base, 0.252 for this change -- one
`io_uring_enter` per pass serves the whole batch.

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
