# Working in this repository

`pg-noop` is a no-op PostgreSQL server: it speaks enough of the wire protocol to answer a
client without doing any work, so a benchmark measures the client and not the server. The
serving loop is `io_uring` on Linux (`src/uring.rs`) with an epoll twin (`src/main.rs`), so
the tests are Linux tests; the MySQL wire lives on `feat/db-noop` and `main-v030`, not here.

## Merge gate

The merge queue runs these in a branch's worktree, after rebasing it onto `main` and
before landing it; the first that fails parks the branch. Read from `main`'s copy of this
file, so a branch cannot change its own gate. They need no database and no network, so the
gate is seconds rather than minutes — but they must run on Linux, because the engine is
`io_uring`/epoll.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
```
