# Measurements

Throughput of db-noop itself: the number a benchmark plateaus near, with the
reply count taken from the wire. It says how fast a **client** is when the server
is not the constraint.

Every number below is a record of a run, not a reproducible artefact of this tree.
Where a table names the commit it was measured at, that is the tree that produced it.

## Reproducing

```sh
cargo build --release
DBNOOP_PBUF=8 ./target/release/dbnoop --host 127.0.0.1 --port 15432 \
  --mysql-port 13306 --workers 4 --profile tpcc &
./target/release/saturate --addr 127.0.0.1:15432 --conns 32 --depth 1 --secs 5
./target/release/saturate --proto mysql --addr 127.0.0.1:13306 --conns 32 --depth 1 --secs 5
```

`saturate` is raw sockets, pre-encoded pipelined messages, no allocation in the
loop, and reports p50/p90/p99/p99.9/max. `--proto postgres` (the default) counts
`ReadyForQuery`; `--proto mysql` sends one `COM_QUERY` packet per query and counts
the packets that end an answer.

## Depth and limit

`select 1`, extended protocol, statement prepared once, one shard per core on an
idle 16-core / 32-thread host, 32 connections:

**Historical.** The table as printed is from `a61b119` (2026-09-14, the
PostgreSQL-only tree): six rows are unchanged from `a436f4c` earlier that day, and
the depth-1 row was revised there from 2,831,421 to 3,318,093. No run log in this
tree records either run; `saturate` is what would measure it again.

| depth | q/s | µs/query |
|---|---|---|
| 1 | 3,318,093 | 8.000 |
| 2 | 5,333,392 | 4.500 |
| 4 | 10,651,482 | 2.000 |
| 8 | 18,012,011 | 1.500 |
| 16 | 31,548,029 | 0.875 |
| 32 | 46,320,724 | 0.594 |
| 64 | 71,358,174 | 0.344 |

Per-query cost falls 23× under amortisation. Decomposition: ~0.3 µs work, ~7.7 µs
kernel TCP loopback, so **at depth 1 the server is round-trip bound**. Measure on
a quiet machine: a contended host moves the same binary by 25%.

## Both protocols, on a shared 32-core host

AMD Ryzen AI MAX+ 395, four shards, 32 connections, `DBNOOP_PBUF=8`, medians of
three runs. The host carried another tenant's load average of 4–12 throughout, so
read the shape rather than the ratio: depth 1 is one answer per round trip, and
MySQL costs what PostgreSQL costs.

**Historical too**, from `88da3ef` (2026-10-28); again, nothing in the tree
reproduces the run.

| depth | PostgreSQL (q/s) | MySQL (q/s) |
|---|---|---|
| 1 | 928,413 | 846,188 |
| 8 | 5,665,313 | 5,451,905 |
| 32 | 12,192,955 | 11,122,569 |

## I/O backends

Shard-per-core over the same codec, differing only in byte movement, selected
with `--io`: `epoll` costs 3.00 syscalls per query (`epoll_wait` + `recvfrom` +
`sendto`) and `uring` 1.51, for +2% at depth 1. `SINGLE_ISSUER` +
`DEFER_TASKRUN` are worth +19% over a plain ring (`DBNOOP_NO_DEFER=1` disables).
`DBNOOP_SQPOLL` should stay off at one shard per core: one spinning kernel poller
per shard is 174× slower.

## The ring

`uring` probes every ring feature the kernel has, one setup flag at a time, and
reports the granted set on stderr; a flag `io_uring_setup` refuses is dropped
after the errno that refused it, never silently.

- `CQSIZE`: completions are four times the submission entries, so a burst is
  drained rather than pushed into the kernel's overflow list.
- `PBUF_RING`: one provided-buffer ring per shard, shared by its connections, and
  one-shot: every completion carries a buffer and gives it straight back.
- **Multishot receive**: one request per connection that the kernel re-issues
  itself, with a one-shot fallback for a kernel that will not take it.
- **Backpressure**: past 256 KiB of staged reply the receive is cancelled until
  the write drains, so a client that never reads is pushed back through its own
  receive window.
- **A ring budget**: ring memory is charged to the user, so the number of shards
  that may have one is planned from `RLIMIT_MEMLOCK` and the rest serve on epoll.
  The banner says how it was spent: `serving 64/64 (io_uring 12, epoll 52)`.
  `DBNOOP_PBUF` sizes the per-shard buffer pool, clamped to 32768.

```
dbnoop: io_uring granted: SINGLE_ISSUER DEFER_TASKRUN COOP_TASKRUN TASKRUN_FLAG
        NO_SQARRAY CLAMP SUBMIT_ALL CQSIZE RING_FD PBUF_RING
        (PBUF_RING_INC granted, unused: a buffer's ownership is not reported
         while a receive holds it) (SQPOLL not requested)
```

With `DBNOOP_SQPOLL` set and granted the list starts `SQPOLL` instead; a refused
SQPOLL is named under `dropped` with the errno that refused it.
