# db-noop design review panel, 2026-10-28

Three Opus reviewers in two rounds on db-noop at `37350e9` (snapshot of `feat/db-noop`).
Copied verbatim from the reviewers' own hand-backs. The orchestrator's synthesis and decisions are at the end.

## Reviewer 1: against the blackhole (skeptic) — round 1

**Reviewer 1 (against db-noop): round 1**

**Thesis.** db-noop measures one thing well: how fast a client can push protocol frames at a peer that answers instantly. Its answers are made up, and they differ by protocol and by client. So per-transaction cost, the gain from batching, and the order in which clients rank all depend on the blackhole as well as on the client. Whether a client runs cleanly against it says nothing about whether the client is representative.

**Evidence**

1. **The README contradicts the code, and the two protocols disagree.**
   - The README says the PostgreSQL side answers `INSERT 0 0`, `UPDATE 0`, `DELETE 0` and 0-row SELECTs (README "Protocol support / PostgreSQL").
   - The code answers `INSERT 0 1`, `UPDATE 1`, `DELETE 1` (wire.rs:1503-1505, handler.rs:1659-1661).
   - On PostgreSQL, a SELECT on a table the server has not seen DDL for gets **one row of `1`s** (wire.rs:1483-1497, data_row at wire.rs:1801; stub_row at handler.rs:1430).
   - MySQL answers the same SELECT with **zero rows** (mysql/mod.rs:760).
   - So the same plain-SQL TPC-C client finds every item on PostgreSQL and none on MySQL. Its code paths, and therefore its cost, differ by protocol and by whether DDL ran first.

2. **The rollback and retry paths never run.**
   - On MySQL, stroppy-rust ends its 1% rollback NewOrders with `SIGNAL SQLSTATE '45000'` (stroppy-rust tpcc_hammer.rs:1188). db-noop has no SIGNAL handling and answers OK (mysql/mod.rs:743-745), so the rollback becomes a commit.
   - HammerDB's plain-SQL path decides a rollback from an empty item SELECT (tpcc_hammer.rs:1106-1110). On MySQL every item SELECT is empty, so that client would roll back **every** NewOrder. On PostgreSQL it would roll back none.
   - db-noop never sends 40001, 40P01, 1213 or 1020.
   - A CancelRequest just closes the connection (wire.rs:730).
   - There is no TLS on either protocol (wire.rs:725; README "MySQL").

3. **The heuristic counters are wrong in ways a real database is not.**
   - A NewOrder is counted when its statement runs (wire.rs:1463, mysql/mod.rs:509), before COMMIT. A rolled-back NewOrder still raises `district_sum`, so NOPM is inflated.
   - On PostgreSQL, autocommit `select neword()` is never counted as a commit (wire.rs:1512-1515 only counts COMMIT). HammerDB on PostgreSQL reads TPM from `sum(xact_commit+xact_rollback) from pg_stat_database` (hammerdb-baseline.md:17), and nothing in src handles that query. It gets a stub `1`, so TPM is 0.
   - The README claims a SELECT-form procedure call is counted on both protocols. The code counts it on PostgreSQL only: on MySQL the SELECT branch returns before `statement_kind` runs (mysql/mod.rs:725-745).
   - `select max(w_id) from warehouse`, which HammerDB reads at login (hammerdb-baseline.md:118), returns `1` on PostgreSQL and nothing on MySQL, whatever `--warehouses` is set to.

4. **Rankings have already flipped.**
   - client-ceiling.md:37-41: stroppy-go "fails every transaction (`district not found`) because it validates the rows it receives". stroppy-rust "counts an empty response as a commit".
   - So against db-noop the client that checks less looks faster, and the two cannot be compared. A client gains by trusting the server blindly.

5. **Batching and runtime gains are inflated.**
   - Against the blackhole, depth 4 is worth 2.9x (client-ceiling.md:16) and DEFER_TASKRUN is worth +27% at depth 4 (client-ceiling.md:120).
   - That same document says such gains are hidden once latency dominates: "at depth 16 the queue hides the latency".
   - Against a server at about 5,000 tx/s (client-ceiling.md:24), where the client uses 0.27% of its ceiling, these gains mostly disappear. Real-server lock waits can also turn deep pipelines into contention, so the ordering can reverse. The depth-16 knee (client-ceiling.md:96-104) is a property of the blackhole.

**To Reviewer 2.** Passing end to end only shows that db-noop's invented numbers are self-consistent. The README says so itself: "consistent with each other and with nothing else". A client that never decodes a row, never retries and never rolls back can pass and still be unrepresentative. Points 2 and 4 are documented cases.

**What would make the numbers trustworthy**
- **Same answer shape on both protocols.** PostgreSQL and MySQL should return the same answer shape, the README should match the code, and a `--rows N` option should return realistic result sizes.
- **Opt-in injected behaviour.** Flags for latency (`--latency-us`), a rollback rate, a 40001/1213 rate and a cancel delay. The ceiling can then be swept against conditions that resemble a real server.
- **Error on unknown rollback triggers.** A SIGNAL, or an unrecognised RAISE, should get an error, not OK.
- **Count NewOrders at COMMIT.** Then rolled-back transactions do not move `district_sum`.
- **Count protocol units by default.** Count protocol units (queries and transactions seen on the wire) by default. Put the TPC-C rules behind `--tpcc`, and print "synthetic NOPM" in the stats line.
- **Label every number.** Every published number should say "client ceiling, not comparable across clients that validate differently" (hammerdb-baseline.md:138 already says "never report faster from a pg-noop number"), and should be paired with a real-server run at the same VU count and depth.

**Concessions**
- As an upper bound on one client's framing and syscall cost, db-noop is sound. It is useful for "how much headroom does the client have" (0.27%), and for A/B tests of one client's changes when the code paths exercised stay the same.
- Its protocol-level work is careful: extended protocol, PortalSuspended, binary rows, and multi-statement sequencing.
- The README already admits that no data is stored and that it was not run against HammerDB.

My objection is to cross-client ranking and to gain ratios carried over to real servers. I do not object to the tool's existence.

## Reviewer 1: against the blackhole (skeptic) — round 2

**Reviewer 1, round 2**

**1. Reviewer 2's profile meets half my objection.** It fixes the inconsistencies I raised: one answer shape on both protocols, no row-of-1s stub, and a district sum and `max(w_id)` that follow `--warehouses`. Declaring HammerDB's MySQL plain-SQL arm unsupported is also right. Their finding that HammerDB measures zero today strengthens my point that passing is not the same as measuring. It still needs four things:
- **Count at COMMIT.** NewOrders should count only on COMMIT, and a rollback should not move the district sum. Otherwise NOPM is inflated.
- **Error on unknown rollback triggers.** SIGNAL, an unknown RAISE or an unknown procedure should get an error, not OK. Otherwise a client's rollback path silently becomes a commit.
- **Labelled output.** The profile is opt-in (`--profile tpcc`), and its output is labelled "synthetic NOPM". Each rule cites the client and line it serves.
- **Opt-in fault knobs.** `--latency-us` and rates for rollback and 40001/40P01/1213 retries. Without them, depth and runtime gains are measured only at zero latency, which is the case I said can reverse.

**2. Reviewer 3's finding makes my thesis stronger.** At 220–330 ns per query against a client that spends about 4 µs per order, db-noop's cost is small at depth 1. But at depth 16–64, where the per-query cost is 0.3–0.6 µs, it is a large share. So gains measured at depth, and the depth-16 knee, are partly db-noop's own cost, including within a single client's A/B test. I concede that A/B results at depth 1 to 4 stay usable.

**3. Non-negotiables:**
1. The README matches the code, and the two protocols return the same answer shapes.
2. TPC-C rules only behind an explicit labelled flag; NewOrders counted at COMMIT; unknown rollback triggers get an error.
3. A server-cost line on every report: db-noop's own CPU per query, measured on the same run.
4. Every published number paired with a real-server run, and never used to rank clients that validate rows differently.
5. Opt-in latency and failure injection, so a gain can be shown to hold above zero latency.

## Reviewer 2: real clients and TPC-C must pass (pragmatist) — round 1

**Round 1, reviewer 3: a blackhole that breaks the run measures nothing**

**Thesis.** db-noop's whole job is to give a ceiling number for a real client. Today neither HammerDB arm can produce that number, and stroppy-rust only gets one on PostgreSQL, partly by luck. The breaks are protocol gaps and one inconsistency between the two protocols. Nothing below asks db-noop to store data.

**Per-client trace (client file:line → db-noop file:line)**

*HammerDB TPROC-C on PostgreSQL (default function mode)*
1. At login HammerDB runs SQL `PREPARE neword (...) AS select neword($1..)` (pgoltp.tcl:3407-3419). Each transaction then calls `pg_exec_prepared neword` (:3253), which sends a Bind to the statement name `neword`.
   - db-noop never turns SQL `PREPARE` into a statement Bind can find. The Bind gets **26000, "prepared statement does not exist"** (src/wire.rs:1030-1035).
   - HammerDB's default `raiseerror=false` prints and ignores errors (hammerdb-baseline.md:36). The loop spins on errors, nothing is counted, and NOPM is 0. **This break decides the run.**
2. TPM comes from `select sum(xact_commit + xact_rollback) from pg_stat_database` (:3140, :3179). There is no rule for it.
   - It falls into the stub path and answers `1` both times, so TPM is 0.
   - Even with a rule, PostgreSQL-side commits are counted only on an explicit COMMIT (src/wire.rs:1507-1519). HammerDB's function calls run in autocommit and send no COMMIT, so the count stays 0.
3. `select max(w_id) from warehouse` and `max(d_id)` (:2925-2932) work only by accident. Any SELECT with a WHERE or a function becomes `SelectStub` and answers one row of `1` (src/handler.rs:1549-1556, src/wire.rs:1483-1498).

*HammerDB TPROC-C on MySQL*
1. The same login `max(w_id)` read gets **0 rows**, because every table SELECT is `Shape::Empty` (src/mysql/mod.rs:727).
   - The warehouse draw then has no upper bound and the virtual user dies.
   - I'm inferring this from the PostgreSQL code path; I couldn't check it against mysqloltp.tcl here.
2. The client-side arm (`mysql_no_stored_procs`) rolls back when its item SELECT comes back empty (mysqloltp.tcl:1453-1456, cited at stroppy tpcc_hammer.rs:1105-1107, 1153). On db-noop that is **every** NewOrder, so NOPM is 0 and Com_rollback inflates.
3. `CALL`, `show global status`, `select @…` and the autocommit commit (src/mysql/mod.rs:497-517) are correct.

*stroppy-rust, conformant and hammer arms*
1. A `--no-load` run reads a baseline whether or not `--no-verify` is set (run.rs:2695-2697). The baseline needs `count(*)` from orders and from new_order back as numbers (run.rs:1365-1385; tpcc.rs:384-386; tpcc_hammer.rs:703-705).
   - PostgreSQL passes, because the stub answers `1`.
   - MySQL gets 0 rows and the run **aborts before its window**.
2. The hammer arm's load on PostgreSQL first runs `foreign_objects_sql` (pg.rs:1807-1818) to check for tables it didn't create. The stub's one row makes it look like there are some, so **the load refuses**. The stub is wrong here.
3. Verify cannot pass, so `--no-verify` is required. That's acceptable.
4. Intended rollbacks (MySQL `SIGNAL 45000`, the PostgreSQL NOT NULL violation) get OK, so the client reports 0 rollbacks and counts those orders as commits. This is a misreport, not a break.
5. Not checked: ring vs threaded at depth > 1. I didn't trace whether either breaks.

**The README disagrees with the code.** It says a PostgreSQL SELECT answers 0 rows, INSERT answers `INSERT 0 0` and DELETE answers `DELETE 0`. The code answers one row and `INSERT 0 1` / `UPDATE 1` / `DELETE 1` (src/wire.rs:1500-1502). MySQL answers 0 rows. The two protocols should not give opposite answers to the same SELECT.

**Minimal, principled rule set: per-client profiles**

Every rule has to name the client line it serves, and anything without a citation goes.

- **Protocol (no profile, not "DB behaviour"):**
  - P1: SQL `PREPARE name AS body` registers `name` for Bind and Execute, as PostgreSQL does.
  - P2: on PostgreSQL, a counted statement outside BEGIN counts as one commit, the same as MySQL already does.
  - P3: a table SELECT answers 0 rows on both protocols, so the PostgreSQL row-of-1s stub goes.
  - P4: one row for literals, `@@vars` and `@vars` only.
- **`tpcc` profile (both protocols)**, with values derived from `--warehouses W`:
  - `sum(d_next_o_id)`, which exists today;
  - `max(w_id)` = W and `max(d_id)` = 10;
  - `count(*)` from orders and from new_order = population + counters;
  - `pg_stat_database` sum = commits + rollbacks;
  - `show global status`, which exists today.
- **`hammerdb-mysql-sql` arm: unsupported, with a reason printed at startup.** It needs rows from point lookups, which is a database.

**Concessions to reviewer 1**
- No generic aggregates, no catalog emulation, and no keyed lookups.
- The PostgreSQL stub row and the catalog-backed column names (src/wire.rs:1883) can both go.
- stroppy-rust should skip its baseline under `--no-verify`. That fixes the client and spares the server a rule.
- Losing the intended-rollback count is acceptable if it's documented.
- I won't trade away P1, P2 or `max(w_id)`: without them HammerDB on PostgreSQL measures zero.

## Reviewer 2: real clients and TPC-C must pass (pragmatist) — round 2

**Round 2, reviewer 3**

**1. Ranking flips and rollbacks.** I agree with reviewer 1 about stroppy-go. A client that validates rows can't run against a server that returns none, so its number is not comparable and must not be ranked. Each profile should list the clients it supports. A client outside that list gets no number, not a low one.

On counting, I concede: count NewOrders at COMMIT, or at autocommit end, not when the statement runs. Under a profile that is cheap: the shard keeps one flag per session and counts it at commit. Without that change, intended rollbacks inflate NOPM.

Opt-in failure injection fits a profile, as long as it uses the client's own trigger. For example, `SIGNAL 45000` or a NOT NULL violation gets the real error, and `--inject 40001:1%` adds serialization failures. Then rollback and retry paths run and get counted, without db-noop making up when they happen. An unknown trigger should get an error, not OK.

I reject "protocol units by default". TPC-C counters cost nothing until a client reads them.

**2. Classify-once at Parse.** It fits, and it strengthens the profiles. Each rule is a pure function of the SQL text. Run it once at Parse, store a small rule-ID enum on the plan, and the hot path pays one branch. The same applies to a MySQL prepared statement, or to text keyed by its hash in a per-shard cache for simple queries. Counters should be per shard, as reviewer 3 says. So the rules don't cost per query, and today's six scans are an implementation bug.

**3. Final minimal list**
- P1: SQL `PREPARE` registers the name for Bind.
- P2: on PostgreSQL, a counted statement in autocommit counts as a commit.
- P3: SELECTs that name a table answer 0 rows on both protocols. Remove the PostgreSQL stub row and use `INSERT 0 0`, as the README says.
- P4: one row for literals and variables.
- Count at COMMIT.
- Classify at Parse, keep counters per shard.
- `tpcc` profile:
  - `sum(d_next_o_id)`;
  - `max(w_id)` and `max(d_id)`;
  - `count(*)` for orders and new_order;
  - the `pg_stat_database` sum;
  - `show global status`.
- Opt-in: answer rollback triggers with their real errors, and `--inject`.
- Out of scope, with a reason given: HammerDB's MySQL client-side arm, and clients that validate rows.

## Reviewer 3: performance critic (NanoLog logging, sharded counters) — round 1

**Thesis.** It doesn't matter whether db-noop behaves like a real database if it isn't close to the hardware ceiling. If it isn't, any client measured against it is partly measuring db-noop. The README's own numbers show it isn't. At depth 64, 71.4M q/s on 16 cores is about 4.5M q/s per core, or about 220 ns per query. On the 4-shard host, 12.2M q/s at depth 32 is about 330 ns per query. A per-query cost of 220–330 ns is roughly 5–10× what one core needs to parse about 60 bytes of Parse/Bind/Execute/Sync and copy about 30 bytes of prebuilt reply, which should be 20–50 ns. The README's "~0.3 µs work" figure is that same overhead: it's db-noop's cost, not the client's. I concede depth 1: about 7.7 µs of it is loopback, so there the client is the bottleneck.

**Violations**
- **Re-scanning SQL on every Execute.** `answer` runs `is_district_sum`, `show_status` and `statement_kind` on `plan.sql` each time (wire.rs:1449, 1456, 1463). `statement_kind` on a SELECT walks every byte (tpcc.rs `called_procedure`), and `show_status` allocates a String. `row_description` scans twice more (wire.rs:1904, 1909). All of this could be decided once at Parse and stored in `PlanKind`.
- **Simple query rebuilds the plan per query.** `PreparedPlan::build` runs per 'Q' (wire.rs:876), and `parse_metadata_select` allocates a `Vec<String>`. Adding the checks above, that is about 6 passes over the SQL per query.
- **Shared lock and allocations on SelectMeta Execute.** `resolve_columns` calls `columns_for` (handler.rs:1460–1471). That takes a process-wide `RwLock` read (handler.rs:52), which is an atomic RMW on one cache line shared by all shards. It then clones each column name into new `FieldInfo`s and an `Arc` (handler.rs:117–128), and again into `Vec<(String,u32)>`. That is 2N+2 allocations and one contended RMW per Execute.
- **Global TPC-C counters.** All seven are `static AtomicU64` in one 56-byte struct (tpcc.rs:115–131): every shard writes the same cache line. MySQL autocommit does 2 `fetch_add` per transaction (mysql/mod.rs:510–515). A line bouncing across cores costs about 100–200 ns, which caps the total at roughly 5–10M increments/s however many shards run.
- **MySQL COM_QUERY.** It builds a `Vec<&str>` even for one statement (mysql/mod.rs:469). `classify` then tries up to five scanners in turn: `show_status`, `is_district_sum`, `select_vars` (returns `Vec<String>`), `constant_select`, and `select_columns` (returns `Vec<String>`, mysql/mod.rs:719–743, 901). It also allocates `vec!["…".to_string()]` (mysql/mod.rs:732, 741) and `text_columns` (mysql/mod.rs:533).
- **PG name lookups.** Statements and portals are `HashMap<String,_>` (wire.rs:294–296), so each Bind and Execute hashes a name, and each Execute does an `Arc::clone`. An unnamed Parse does `to_string` + `Arc::new` + `parameter_types(sql)` every time (wire.rs:1013).
- **Buffers.**
  - The reply is built in a `Vec` and then copied again by the kernel (`Send`, not zero-copy and not a registered buffer).
  - The carry does `extend_from_slice` + `drain(..used)`, a memmove (uring.rs:1380–1385).
  - CQEs are first copied into `done` and then iterated (uring.rs:1558–1566).
  - `sq.sync()` runs on every push (uring.rs:779).
- **Syscalls.** 1.51 `io_uring_enter` per query (README). `submit_and_wait(1)` wakes on the first completion, so there is no batching window (uring.rs:1615).
- **Logging.** Unthrottled `eprintln!` runs inside the loop, on accept failure and on every startup-timeout close (uring.rs:1233, 1519). Each takes std's process-wide stderr lock and makes a `write(2)` call.

**Target design**
1. **Counters.** Each shard has a `#[repr(align(128))]` struct that only it writes, with plain `u64 += 1` and a store-release publish once per pass. A reader adds up all shards. "Consistent enough" means:
   - each shard's value never decreases;
   - a sum can lag by at most one pass per shard (microseconds);
   - NOPM is a difference between two reads taken seconds apart, so the error is below 1e-5;
   - `district_sum` must be built only from published snapshots, so it never goes backwards.
2. **NanoLog logging.**
   - `log!(Level, "fmt {} {}", a, b)` expands to a `static` call-site record (id, level, file, line, format string, argument type tags) in a link section. A `build.rs`/defmt-style table supplies the ids.
   - Arguments go through a `trait Arg: Copy` with `encode(&mut [u8])`, monomorphised for integers, floats, `Errno` and `SocketAddr`, so nothing is boxed.
   - A `&'static str` goes in as its interned id. A dynamic string is copied with a length prefix, truncated to 64 bytes.
   - The record (id u16, timestamp delta varint, arguments) goes into a per-shard SPSC byte ring.
   - A background drain thread compacts records into a binary log with a header holding the format table. It can optionally format a live tail to stderr for operators.
   - An offline `dbnoop-log` reader decodes the binary log.
   - Levels: a level below the compile-time minimum compiles out; a runtime threshold is one relaxed load.
   - Rate limiting: per-call-site token bucket of N per second on the shard clock. Suppressed records are counted and reported as "+k suppressed". When the ring is full the record is dropped and counted; the hot path never blocks.
3. **Classify once.** At Parse, or at the first sight of a SQL text in an interned per-shard map, set a `u8` class (district, status, NewOrder, Payment, …). Simple query does a first-byte/length check before calling any scanner. Column metadata is per-shard: copy-on-write snapshots, epoch-published by DDL.
4. **Loop.** Encode replies straight into registered buffers (`SendZC`/fixed buffers for large replies, a slab for small ones). Carry the tail as a ring offset, not a memmove. Use `min_wait` batching (6.12+).

**Shared loop crate** (stroppy-rust, db-noop, letidb): a pinned shard runtime with one ring per shard, multishot accept and receive, the pbuf ring, a send slab, the CQ iterated in place, the wait policy, the sharded counters, and the NanoLog runtime, macro and reader. **Stays in db-noop:** the PG and MySQL codecs, TPC-C classification, the catalog, and `saturate`.

**Concessions.** The ring stats are `cfg(test)` only (uring.rs:893). The stats thread is off by default (lib.rs:423). MySQL prepared statements already cache `kind` at prepare (mysql/mod.rs:128). `complete` avoids `format!` (wire.rs:1700). The plan is no longer deep-cloned per Execute (wire.rs:1208).

## Reviewer 3: performance critic (NanoLog logging, sharded counters) — round 2

**1. Injection at zero cost when off.**
- Each feature is a bit in the `u8` class that Parse decides once. When off, Execute only tests a byte it has already loaded, a branch the CPU predicts correctly.
- **Latency:** never sleep in the shard. The finished reply is held in the send slab and released by a per-shard timer wheel, or a linked `IORING_OP_TIMEOUT`. Cost when on: one wheel insert.
- **Faults:** a per-shard xorshift draw compared against a threshold, about 2 ns. Fixing the seed also makes the faults repeatable across runs.
- **`--rows N`:** build the DataRow block once per (statement, formats) and keep it in a registered buffer. Each Execute then costs one memcpy, or a `SendZC` above about 4 KiB. No per-row encoding.

**2. Reviewer 2's rules after classify-once.**
- **Per Execute:** one `u8` match plus a plain shard-local `+=`.
- **Counting at COMMIT (reviewer 1):** a per-connection kind mask, folded into the shard counter when the transaction commits. About 3 ns.
- **SQL PREPARE:** registers a name-to-class entry. Its EXECUTE is a single hash lookup.
- **Rare reads** (`pg_stat_database`, `max(w_id)`, district sum): they sum published snapshots, and they run rarely.
- **Simple query** does force work per query: a hash of the SQL bytes into a per-shard intern map, about 15 ns. HammerDB's `CALL` statements carry literals, so they miss the map. For those, scan at most the first two words, with no allocation.

**3. Target for being invisible.** Fast pipelined clients spend about 100–200 ns per query. db-noop should cost 10% of that or less, so 20 ns or less per query, about 50M q/s per core at depth. At depth 1 it already passes. It should also export each shard's busy fraction, so an A/B run can show the server was not the bottleneck.

**4. Minimal list, by impact.**
1. Classify-once, plus an intern map for simple queries.
2. Remove the catalog `RwLock` and the String allocations per Execute (per-shard copy-on-write snapshots).
3. Sharded counters, counted at COMMIT.
4. Prebuilt replies in registered buffers.
5. Carry the tail as a ring offset, and iterate the CQ in place.
6. `min_wait` batching.
7. NanoLog-style logging, rate-limited.
8. Export each shard's busy fraction.

## Synthesis and decisions (orchestrator)

**Agreed by all three:**
1. **One answer shape on both protocols,** and the README matches the code.
   - A SELECT that names a table answers 0 rows; the PostgreSQL row-of-1s stub goes.
   - Literals and variables answer one row; `INSERT 0 0`.
2. **Register SQL `PREPARE` for Bind, and count PostgreSQL autocommit statements as commits.** Without these, HammerDB measures zero today.
3. **Count NewOrders at COMMIT** (or at the end of an autocommit statement), not when the statement runs.
4. **Rollback triggers:** a known trigger gets its real error; an unknown trigger or procedure gets an error, never OK.
5. **Classify once at Parse** into a `u8` class. Simple-query statements go through a per-shard intern map.
6. **Per-shard plain counters,** published once per loop pass, with no read-modify-write atomics.
7. **NanoLog-style logging.**
8. **Opt-in, zero cost when off:** `--inject` faults, latency through a timer wheel, and `--rows N` served from prebuilt replies.
9. **Honest reporting:**
   - db-noop's own CPU per query, and each shard's busy fraction, in every report;
   - every db-noop number paired with a run against a real server;
   - unsupported clients (those that validate rows, and HammerDB's MySQL client-side SQL arm) get no number at all.

**The one disagreement, and the decision.** Reviewer 1 wanted only protocol-level counts by default; reviewer 2 said the TPC-C counters cost nothing. Decision: the counters stay on. The synthetic answers (district sum, `max(w_id)`, `count(*)`, `pg_stat_database`) sit behind an explicit `--profile tpcc` and are labelled "synthetic NOPM".

**Performance target:** at most about 20 ns of db-noop cost per query at depth. Today it is 220–330 ns.

**Order of work, owner 2026-10-28:**
- Phase A (items 1–4 plus the profile and the supported-clients list) is done now.
- Phase B (the hot path) and phase C (logging, injection, reporting) go into `TODO.md` and are not implemented yet.
- B2 and C1 are meant for the future standalone loop crate under `deadtrickster`.
