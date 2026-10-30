# Decisions

Each entry: the question, the decision, and why. These are choices taken while the
operator was away, not questions waiting for an answer.

## D1 — The 2026-10-30 review's five items are closed on the db-noop line

**Question:** are the five findings of the read-only review of the PostgreSQL-only
line still open on the db-noop line (`main-v030`, the tree this branch is cut from)?

**Decision:** no. All five were fixed before this pass; this pass verified them
against the db-noop tree and added the one datum the report was still missing (D2).

| # | Finding | State on the db-noop line | Where |
|---|---|---|---|
| 1 | `Slot.closing` written, never read | read: it gates `next_for`, `arm_recv` and the cancel path | `src/uring.rs:193,1594,1649,1766`; the read came in with `24c23c4` |
| 2 | `TCP_NODELAY` result discarded | discarded on purpose, and the reason is written at the call | `src/uring.rs:1566-1571`; epoll twin at `src/lib.rs:353-355` |
| 3 | a malformed config file replaced by defaults | refused, and exits 2 like the env knob; test `a_malformed_config_file_names_itself` | `src/config.rs:181-189,336-342` (`efed707`) |
| 4 | the ring's granted set never reported | printed by `probe()` and again in the banner, through `describe()` | `src/uring.rs:310-345,382-390,393-395`; `src/lib.rs:688-693` (`97d7ed8`) |
| 5 | `postgres-types` and the pgwire handler layer dead | both deleted, with `pgwire`, `async-trait` and `futures` | `489b0da` |

**Why:** the review read the PG-only tree. The db-noop line replaced pgwire's handler
traits with the sans-io codecs in `wire.rs` and `mysql/`, so the layer the review
found unreachable no longer exists to decide about.

## D2 — The report says whether SQPOLL was asked for

**Question:** the banner prints the granted set, but SQPOLL is asked for only by
environment variable, so a reader cannot tell "not asked" from "asked and lost".

**Decision:** `Plan::describe()` appends `(SQPOLL not requested)` when SQPOLL is
neither granted nor in the dropped list. A refused SQPOLL is already named with its
errno in `dropped`.

**Why:** the io_uring review's §5 asks for the granted set in the report, and silence
about the one flag set out of band is the gap left in it.

## D3 — The throughput tables stay, marked historical

**Question:** `docs/measurements.md` carries two tables of q/s with no run log in the
tree. Rewrite, delete, or label?

**Decision:** label. The depth table is marked as from `a61b119` (2026-09-14, the
PostgreSQL-only tree; six rows unchanged from `a436f4c` earlier that day, the
depth-1 row revised there) and the two-protocol table at `88da3ef` (2026-10-28).
The numbers are unchanged.

**Why:** the numbers are a record of what a run once showed, and rewriting them would
replace evidence with invention. A labelled historical table is honest; an unlabelled
one reads as a claim about the current binary.

## D4 — Not fixed here: the crate does not build on non-Linux

**Question:** `src/lib.rs` names `Io::Uring` at 504, 570 and 620 with no `cfg`, and
the variant exists only under `target_os = "linux"`, so `cargo build` fails on macOS.

**Decision:** left alone in this pass, and reported.

**Why:** it is outside the five findings, the fix touches the shard planner rather
than the ring, and it cannot be validated on the host that found it. It needs
`#[cfg(target_os = "linux")]` guards (or an `Io::Uring` that exists everywhere and is
refused by `Io::parse` off Linux) plus a non-Linux CI job. Until then the crate is
Linux-only in fact, whatever the comment on `libc` says.
