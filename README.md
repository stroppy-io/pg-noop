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
| Worker threads | `--workers` | `PGNOOP_WORKERS` | number of logical CPUs |
| Config file path | `--config` | `PGNOOP_CONFIG` | `./pgnoop.json` |

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

Wire protocol handling is provided by [`pgwire`](https://github.com/sunng87/pgwire), a Rust implementation of the PostgreSQL frontend/backend protocol ([crates.io](https://crates.io/crates/pgwire) · [docs.rs](https://docs.rs/pgwire)).
