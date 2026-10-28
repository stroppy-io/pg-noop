# db-noop clean-code review, 2026-10-28

Three independent reviewers in the persona of the author of "Clean Code", same brief, same snapshot (`90da41d`, after the deslop pass). Copied verbatim. Decisions at the end.

## Reviewer A: Opus

## db-noop 0.3.0, reviewed by the author of "Clean Code"

**1. Verdict**
The protocol knowledge is sound and the uring loop is careful work. The structure has not kept up with it.
There are two SQL lexers, two classifiers, two transaction-counting rules and a dead pgwire layer, and everything is `pub`, so the compiler cannot point any of this out.
Two functions of about 500 lines each hold most of the logic. Most comments are a changelog, and several are cut off mid-sentence.

**2. Findings, ranked**

1. **lib.rs:5-13: every module is `pub`, but the binaries and tests only call `run()`.** This hides dead code from rustc. *Refactor:* make the modules `pub(crate)`; the compiler will then list most of item 2 and `tpcc::is_district_sum` (tpcc.rs:403), which is used only by its own tests. **High.**

2. **handler.rs:1532-1812 is dead: the pgwire server API** (`SimpleQueryHandler`, `ExtendedQueryHandler`, `CopyHandler`, `respond_to_plan`, `classify_simple`/`classify_extended`, `vector_for_copy`, the stub cache). `wire.rs` replaced it. The catalog still stores pgwire `FieldInfo` and converts it back to `(String, u32)` on every call (handler.rs:1397). *Refactor:* delete that block, store `(name, oid)`, and drop the `pgwire`, `async-trait`, `futures` and `postgres-types` dependencies. That removes roughly 450 lines. **High.**

3. **There are two SQL lexers, and they disagree.** handler.rs has `SqlWords`:421, `split_top_level`:1144, `split_top_level_commas`:389, `count_top_level_commas`:628 and `first_keyword`:184. sql.rs has its own versions. `first_keyword` does not skip comments, so on PostgreSQL `/* x */ INSERT …` comes out as `FromText` and gets the tag `SELECT 0`, while MySQL classifies the same statement correctly. *Refactor:* make handler.rs call `sql::first_word`, `sql::Words` and `sql::split_top_level`, and delete its copies. **High.**

4. **wire.rs:809: injected failures are detected only on the PostgreSQL simple Query path.** The Execute path (wire.rs:1112-1190) never calls `tpcc::failure`, so a NULL written through Parse/Bind/Execute is answered OK. That is the commit DESIGN.md says must not happen. The other TPC-C rules are re-scanned outside the plan, in `answer`:1344 and `row_description`:1743. *Refactor:* decide `Failure`, `Number` and `Status` once in `PreparedPlan::build`, as `mysql::classify` already does with `Shape`. This lines up with TODO B1, but the reason here is correctness, not speed. **High.**

5. **"Counted at COMMIT" is implemented twice:** wire.rs:341-382 (`note_transaction`, `note_statement`) and mysql/mod.rs:473 (`apply`). *Refactor:* move it into `tpcc::Pending` as `begin()`, `end(committed)` and `statement(kind)`, and have both codecs call that. One rule, one owner. **High.**

6. **Two functions of about 500 lines.** `wire.rs::dispatch` (737-1217) is one match. `uring.rs::passes` (962-1458) takes 7 parameters and contains `match next {…}` four times over. *Refactor:* in wire.rs, one method per message tag (`on_parse`, `on_bind`, …). In uring.rs, a `Shard` struct that owns slots, buffers and the ring, with `on_accept`, `on_recv`, `on_send`, `expire_startups` and a single `act(idx, Next)`. **High.**

7. **tpcc.rs:345-352: `Box::leak` on every `SIGNAL '<code>'` that has no `SQLSTATE` keyword**, and the result is then `to_string()`'d anyway. tpcc.rs:378-380 is an `if let` that allocates and then does `let _ = first`. *Refactor:* return an owned `String`, and delete the dead `if let`. **Medium-high.**

8. **config.rs:325-328: a malformed `dbnoop.json` is silently ignored** (`.ok()…unwrap_or_default()`). `DBNOOP_PBUF` (config.rs:168) and `FAIL_RING` (uring.rs:731) parse the same way. Meanwhile config.rs:~370 says "a typo must not silently measure the wrong thing." *Refactor:* add `fn or_exit<T>(Result<T,String>) -> T`. Use it for the file and those variables, and also in place of the four copies of parse-or-exit at config.rs:330-360. **Medium-high.**

9. **The uring and epoll loops duplicate each other.** The pending/advance step appears at lib.rs:207-219 and uring.rs:1287-1296. The accept errno set appears at lib.rs:353 and uring.rs:1608. *Refactor:* add `Session::feed(&codec, &mut pending, bytes, out)` in protocol.rs and a shared `accept_should_pause`. **Medium.**

10. **Comments that cut off or mislead.** These end mid-sentence:
    - wire.rs:784, 841
    - mysql/mod.rs:442
    - uring.rs:1056, 1445
    - tpcc.rs:420

    These are wrong:
    - tpcc.rs:319 has two doc comments glued together.
    - handler.rs:173 says "Builds N FieldInfo" above `starts_with_keyword`.
    - Cargo.toml cites `main.rs` and `main::resolve_io`, which no longer exist.

    Comments such as "this was `unwrap_or("")`" or "pgwire allowed 60 s; the rewrite dropped it" are history, and history belongs in git. *Refactor:* finish or delete each of these, and keep comments to 2-3 lines that say why. **Medium.**

11. **Test hooks are spread through production code.** `Tuning` has 5 `#[cfg(test)]` fields, and `passes`/`arm_recv` contain about 12 `#[cfg(test)] if let Some(s) = &…stats` blocks. *Refactor:* one `ring.count(|s| &s.arms)` helper that compiles to nothing outside tests. **Medium.**

12. **PostgreSQL unit tests build frames byte by byte** (wire.rs:3069-3141, 4423-4495), and there are four separate builders for the startup message (protocol.rs:128, wire.rs:1942, uring.rs:2060, plus inline copies). *Refactor:* one `#[cfg(test)] mod pg_frames` with `parse`, `bind`, `execute` and `sync`, as mysql/tests.rs:30-40 already does for MySQL. **Medium.**

13. **wire.rs:1342/1462/1497: `answer` returns a `bool` that is always `true`,** and it is stored in `portal.consumed`. The return value means nothing. *Refactor:* return `()` and set `consumed` at the call site. Also delete `let _ = formats;` (wire.rs:~1457). **Low-medium.**

14. **mysql/result.rs:65, 148, 161: stale `#[allow(dead_code)]`** on `LongLong`, `row` and `Value`, all of which are now used. The attached comments are false. *Refactor:* delete the attributes. **Low.**

15. **lib.rs:411-607: `run()` is about 200 lines.** It does the CPU plan, the ring split, binding, spawning, three filters to build the banner, and joining. *Refactor:* extract `spawn_shards` and `Banner::from(&reports)`. **Low.**

**3. What I would not change**
- **The sans-io codec contract** (`advance(input, out) -> consumed`). It is the best boundary in the code: it makes both loops possible and the codecs testable without sockets.
- **`next_for` and the `Next` enum** (uring.rs:183). This is a small, honest state decision, and the `Shard` refactor in item 6 should keep it.
- **The `Retry` classification** (`Again`, `Drain`, `Stop`). The names say exactly what they mean.
- **`mysql::packet::Reader`** and its `Option`-returning reads. They are clean, and `?` reads well there.
- **Test names** such as `a_client_that_never_reads_cannot_grow_the_shard_memory`. They state behaviour; keep them.
- **The `Codec`/`Session` enum dispatch in protocol.rs.** With only two protocols, a trait would be more machinery than it is worth.

## Reviewer B: leticode, DeepSeek

## 1. Verdict

The sans-io codec and the shard loop are real engineering; the code *around* them was not finished being cleaned. The 0.3.0 tree still carries a complete second PostgreSQL implementation that nothing calls, and 1990 of `wire.rs`'s 4536 lines are gone in all but name. Fix the four big ones and this is a book-worthy codebase.

## 2. Findings, ranked

**1. `src/handler.rs:1531-1812` — a whole second PostgreSQL server, dead in the shipped build.** `pgwire`, `async-trait`, `futures`, `postgres-types` are imported *only* here, and nothing outside `handler.rs` calls `PlanParser`, the `SimpleQueryHandler`/`ExtendedQueryHandler`/`CopyHandler` impls, `classify_simple`/`classify_extended`, `respond_to_plan`, `stub_fields` or `constant_fields` — all unreachable from `run()`. Refactor: delete the pgwire layer with its four dependencies; move `PreparedPlan`/`PlanKind`/`SchemaCatalog`/the parsers into `plan.rs` and `catalog.rs`. Impact: **high** — and it takes `handler.rs:173` ("Builds N FieldInfo entries…" sitting on `starts_with_keyword`) with it.

**2. `src/wire.rs:737-1215` — `dispatch` is 480 lines doing six jobs.** Framing errors, the transaction state machine, the statement/portal registry, Bind arity checks, the COPY transition and the counters; 19 `return Some(())` and 20 `self.refuse(...)`. Refactor: one `fn on_parse/on_bind/on_execute/on_describe` per tag, and make `refuse` return `None` so `dispatch` reads `self.refuse(..)?` instead of "error, then return". Impact: **high**.

**3. `src/uring.rs:1321-1383` and `src/lib.rs:222-231` — the loops have drifted apart.** The `match next { Send/Close/Arm/Park/Idle }` block is copied four times verbatim; the pending-tail step (`advance` + `pending.drain`) is duplicated at `lib.rs:222` and `uring.rs:1293`. Refactor: `impl Ring { fn apply(&mut self, idx, next) }`, and one free `fn feed(session, pending, bytes, out) -> usize` both loops call. Impact: **high** — this is the duplication that lets the backends answer differently.

**4. `src/handler.rs:1144, 1192, 1241, 823` — four hand-rolled SQL scanners.** The same quote/depth state machine appears character-for-character three times, and `placeholders` writes a fourth with its own comment and `$tag$` handling — while `src/sql.rs:124` already exports `split_top_level`, `skip_quoted`, `skip_comment`. Refactor: delete `handler`'s three copies; keep one tokeniser. Impact: **high**.

**5. `src/handler.rs:1616-1714` vs `src/wire.rs:1342-1426` — the keyword→answer table written four times.** `classify_simple`, `classify_extended`, `respond_to_plan` and `answer` must agree and nothing makes them: COPY out lives in two of them. Refactor: one `fn kind_to_reply(&PlanKind) -> Reply`; the simple path wraps it. Impact: **high**.

**6. `src/mysql/mod.rs:701-702` — a branch that cannot be taken.** `number_query` returns `None` when the profile is off (`tpcc.rs:141`), so `!tpcc::profile() && tpcc::number_query(..).is_some()` is always false; the district sum is *not* answered as an empty select with the profile off, which is what that line intended. Refactor: delete it, or move the `profile()` check to the answer. Impact: **medium**.

**7. `src/tpcc.rs:403` — `is_district_sum` is called only by its own tests**, and it is a second, narrower copy of the rule `number_query` (`tpcc.rs:179`) implements. `tpcc.rs:240` (`note`) and `tpcc.rs:234` (`is_empty`) are likewise production-dead. `TODO.md:14` still lists `is_district_sum` as hot-path work. Impact: **medium**.

**8. `src/wire.rs:1342` — `answer() -> bool` is `true` on every path**, and its value becomes `portal.consumed`, so a portal answered with `PortalSuspended` is recorded as "read to its end" (1370-1374). Refactor: return `()`; have the suspend arm say `consumed = false`. Impact: **medium** — a field whose doc comment is false.

**9. `src/uring.rs:962` — `passes` is 500 lines with seven `&mut` parameters**, and `Slot` (101-139) tracks twelve booleans, five of which (`sending`, `recv_armed`, `private`, `parked`, `closing`) are one state machine written as flags. Refactor: `struct Shard { slots, free, spare, bufs }` as the receiver; `enum Rd { Idle, Armed, Parked }`, `enum Wr { Idle, Sending }`. Impact: **medium-high**.

**10. `src/lib.rs:197-205` — a comment that compensates for a silent path.** "the caller has what it needs to tell them apart in the log line below" — there is no such line: timeout logs, closed and error return silently. Refactor: log the error arm. Impact: **medium** — a measurement's log cannot explain a dropped connection.

**11. `src/lib.rs:596` — `t.join().unwrap_or(false)`** makes a panicked shard identical to a shard that returned false; if any other shard served, the process exits 0. Refactor: `match t.join() { Err(p) => eprintln!(…) }`. Impact: **medium**.

**12. `src/wire.rs:1443-1459` — `status_description(formats, …)` takes `formats` and throws it away** (`let _ = formats;`). Drop the parameter. Impact: **low**.

**13. `src/mysql/mod.rs:501` — `answer(answer, out, more: bool, binary: bool)`**: two flags selecting four behaviours. Pass `status: u16` and a `RowFmt` enum. Impact: **low**.

**14. `src/tpcc.rs:319` — two comments merged onto one line**: "The value of a MySQL system variable" now documents `pub enum Failure`. Also stale `#[allow(dead_code)]` at `mysql/result.rs:65,148,161` on three items that *are* used. Impact: **low**, one-line fixes.

## 3. What I would not change

- The sans-io codec boundary: `Conn::advance(&[u8], &mut Vec<u8>) -> usize`, no socket, no async. Every test in `wire.rs` exists because of it.
- `mysql/mod.rs:62-131` — `Shape`/`Answer`/`Tx` decided once, then rendered. This is the design the PostgreSQL side should be pulled toward, not away from.
- `Run<'a>` (`wire.rs:1557`): five arguments bundled into one named struct, and `answer` made a free function to stop cloning the plan — right calls, documented with *why*.
- `BinaryState`/`LineState`: fussy, but an honest state machine over a stream, named after what it tracks.
- The `#[cfg(test)]` knobs in `Tuning`/`Stats`: ugly in production structs, and the only reason EMFILE, backpressure and fd leaks are tested at all.
- The test names. They are behavioural sentences, and the eleven-line `a_denied_ring_falls_back_to_epoll` is worth more than a thousand-line fixture.

## Reviewer C: leticode, GLM

(no report found)