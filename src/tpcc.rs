//! The TPC-C rules: the small, explicit set of statements this server answers
//! with something other than a no-op.
//!
//! # Why they exist
//!
//! A TPC-C client reports a throughput from what the server tells it. HammerDB
//! reads `select sum(d_next_o_id) from district` before and after its window and
//! calls the difference NOPM, and reads `show global status` for `Com_commit`
//! and `Com_rollback` to report TPM. Against a server that answers nothing, both
//! are zero and the run is unmeasurable -- even though the CLIENT is exactly
//! what such a run is measuring.
//!
//! # What they are, and what they are not
//!
//! These are not SQL: they are a handful of shapes matched on the statement's
//! own words, listed here in full so that a reader can see everything this
//! server will ever answer with a number.
//!
//! * A transaction is counted by the procedure a client calls -- `CALL
//!   NEWORD(...)`, `PAYMENT`, `DELIVERY`, `OSTAT`, `SLEV`, by name in any case,
//!   with or without a schema prefix -- or, for a plain-SQL client, by an
//!   INSERT that names the `new_order` table (or `tpcc_new_order`), which is
//!   the one table only a NewOrder writes.
//! * `select sum(d_next_o_id) from district` (any case and spacing, an
//!   optional schema prefix, `district` or `tpcc_district`) answers
//!   `3001 x districts + NewOrders seen`, which is what the schema holds at the
//!   start plus one per NewOrder committed. Districts is 10 per warehouse, and
//!   `--warehouses` says how many there are.
//! * `show global status`, or with a `LIKE` filter, answers `Com_commit` and
//!   `Com_rollback`: the first for every explicit COMMIT and -- because a
//!   statement outside a transaction is a transaction of its own -- for every
//!   counted TPC-C statement in autocommit mode.
//! * `select @a, @b` answers one row whose values are all `1`: a plausible
//!   number for an OUT parameter, and never NULL.
//!
//! Everything else is answered by the rules of the protocol's codec. A server
//! that answered *only* these would be useless to the clients it exists for;
//! one that answered more would be a database.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::sql;

/// A TPC-C transaction, by the procedure that runs it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    NewOrder,
    Payment,
    Delivery,
    OrderStatus,
    StockLevel,
}

impl Kind {
    /// The procedure names this kind answers to. A TPC-C client spells them
    /// `NEWORD`, `PAYMENT`, `DELIVERY`, `OSTAT`, `SLEV` (HammerDB's stored
    /// procedures) or `neword`, `payment`, ... (its own naming).
    fn from_procedure(word: &str) -> Option<Kind> {
        let word = word.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
        for (names, kind) in [
            (&["neword", "new_order", "neworder"][..], Kind::NewOrder),
            (&["payment"][..], Kind::Payment),
            (&["delivery"][..], Kind::Delivery),
            (
                &["ostat", "order_status", "orderstatus"][..],
                Kind::OrderStatus,
            ),
            (&["slev", "stock_level", "stocklevel"][..], Kind::StockLevel),
        ] {
            if names.iter().any(|n| word.eq_ignore_ascii_case(n)) {
                return Some(kind);
            }
        }

        None
    }

    /// Every kind, in the order the line on stderr reports them.
    pub const ALL: [Kind; 5] = [
        Kind::NewOrder,
        Kind::Payment,
        Kind::Delivery,
        Kind::OrderStatus,
        Kind::StockLevel,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Kind::NewOrder => "new_order",
            Kind::Payment => "payment",
            Kind::Delivery => "delivery",
            Kind::OrderStatus => "order_status",
            Kind::StockLevel => "stock_level",
        }
    }

    fn counter(self) -> &'static AtomicU64 {
        match self {
            Kind::NewOrder => &STATS.new_order,
            Kind::Payment => &STATS.payment,
            Kind::Delivery => &STATS.delivery,
            Kind::OrderStatus => &STATS.order_status,
            Kind::StockLevel => &STATS.stock_level,
        }
    }
}

/// The counters, process-wide.
///
/// They are shared by every shard because they are a fact about the server and
/// not about a connection: a client's NewOrders do not belong to the thread the
/// kernel put it on. Relaxed ordering throughout: nothing here orders anything
/// else, and a count that a reader sees one behind is a count.
struct Counters {
    new_order: AtomicU64,
    payment: AtomicU64,
    delivery: AtomicU64,
    order_status: AtomicU64,
    stock_level: AtomicU64,
    com_commit: AtomicU64,
    com_rollback: AtomicU64,
}

static STATS: Counters = Counters {
    new_order: AtomicU64::new(0),
    payment: AtomicU64::new(0),
    delivery: AtomicU64::new(0),
    order_status: AtomicU64::new(0),
    stock_level: AtomicU64::new(0),
    com_commit: AtomicU64::new(0),
    com_rollback: AtomicU64::new(0),
};

/// How many warehouses the schema is said to have. Read at startup from
/// `--warehouses`, and a `OnceLock` rather than a constant because the district
/// sum depends on it.
static WAREHOUSES: AtomicU64 = AtomicU64::new(DEFAULT_WAREHOUSES);

/// HammerDB's own default schema: ten warehouses, and therefore a hundred
/// districts.
pub const DEFAULT_WAREHOUSES: u64 = 10;

/// The districts in the schema: ten per warehouse, as TPC-C defines them.
pub fn districts() -> u64 {
    10 * WAREHOUSES.load(Ordering::Relaxed)
}

/// `--warehouses N`.
pub fn set_warehouses(n: u64) {
    WAREHOUSES.store(n.max(1), Ordering::Relaxed);
}

/// The sum a client reads before and after its window.
///
/// The schema holds `3001` per district at the start -- `d_next_o_id` begins at
/// 3001 -- and every committed NewOrder increments one district's value, so the
/// total is that plus the NewOrders this process has seen.
pub fn district_sum() -> u64 {
    3001 * districts() + new_orders()
}

/// Count one TPC-C transaction of this kind, as committed.
pub fn note(kind: Kind) {
    kind.counter().fetch_add(1, Ordering::Relaxed);
}

pub fn note_commit() {
    STATS.com_commit.fetch_add(1, Ordering::Relaxed);
}

pub fn note_rollback() {
    STATS.com_rollback.fetch_add(1, Ordering::Relaxed);
}

pub fn com_commit() -> u64 {
    STATS.com_commit.load(Ordering::Relaxed)
}

pub fn com_rollback() -> u64 {
    STATS.com_rollback.load(Ordering::Relaxed)
}

pub fn new_orders() -> u64 {
    STATS.new_order.load(Ordering::Relaxed)
}

/// One line for stderr: what the server has counted, in the names the rules use.
pub fn line() -> String {
    let mut line = String::from("tpcc:");
    for kind in Kind::ALL {
        line.push_str(&format!(
            " {}={}",
            kind.name(),
            kind.counter().load(Ordering::Relaxed)
        ));
    }
    line.push_str(&format!(
        " com_commit={} com_rollback={} district_sum={} warehouses={}",
        com_commit(),
        com_rollback(),
        district_sum(),
        WAREHOUSES.load(Ordering::Relaxed),
    ));

    line
}

/// **The district sum a TPC-C client reads for NOPM.**
///
/// `select sum(d_next_o_id) from district`, in any case and spacing, with an
/// optional database prefix on the table, and for the `tpcc_district` name a
/// TPC-C schema may use instead. The words are read, not the punctuation:
/// `sum ( d_next_o_id )` is the same statement to a scanner that skips
/// parentheses.
pub fn is_district_sum(sql: &str) -> bool {
    let mut words = sql::Words::new(sql);

    let Some(select) = words.next() else {
        return false;
    };
    if !select.eq_ignore_ascii_case("SELECT") {
        return false;
    }
    // `sum(d_next_o_id)`: the aggregate and its argument, as words.
    for expected in ["SUM", "D_NEXT_O_ID", "FROM"] {
        match words.next() {
            Some(word) if word.eq_ignore_ascii_case(expected) => {}
            _ => return false,
        }
    }

    // The table, optionally `schema.district` -- the scanner splits on `.`, so
    // the name is the first or the second word after FROM -- and **nothing
    // follows it**: a sum over a filtered district is a different question from
    // the whole schema's, and this server does not answer questions it cannot
    // read.
    let is_table = |word: &str| {
        word.eq_ignore_ascii_case("district") || word.eq_ignore_ascii_case("tpcc_district")
    };
    let Some(first) = words.next() else {
        return false;
    };
    let named = if is_table(first) {
        true
    } else {
        // A schema prefix, then the table.
        words.next().is_some_and(is_table)
    };

    named && words.next().is_none()
}

/// **The transaction a statement is**, when it is a TPC-C one.
///
/// A `CALL` names the procedure (a stored-procedure client), and an `INSERT`
/// that names the `new_order` table is a NewOrder (a plain-SQL client: it is the
/// one table only that transaction writes).
pub fn statement_kind(sql: &str) -> Option<Kind> {
    let mut words = sql::Words::new(sql);

    match words.next() {
        Some(word) if word.eq_ignore_ascii_case("CALL") => {
            // `CALL neword(...)`, and `CALL tpcc.neword(...)` -- a scanner that
            // splits on `.` gives both, so the first two words after CALL are
            // tried.
            for _ in 0..2 {
                let word = words.next()?;
                if let Some(kind) = Kind::from_procedure(word) {
                    return Some(kind);
                }
            }

            None
        }
        Some(word) if word.eq_ignore_ascii_case("INSERT") => {
            let names_new_order = words.any(|w| {
                w.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .eq_ignore_ascii_case("new_order")
                    || w.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                        .eq_ignore_ascii_case("tpcc_new_order")
            });
            names_new_order.then_some(Kind::NewOrder)
        }
        _ => None,
    }
}

/// **`SHOW GLOBAL STATUS`, and the filter a client gives it.**
///
/// HammerDB reads `Com_commit` and `Com_rollback` from it; a `LIKE` filter is
/// how it asks for one at a time. `None` is not this statement at all, and
/// `Some(None)` is the unfiltered list.
pub fn show_status(sql: &str) -> Option<Option<String>> {
    let mut words = sql::Words::new(sql);
    if !words.next()?.eq_ignore_ascii_case("SHOW") {
        return None;
    }
    let mut next = words.next()?;
    // `SHOW GLOBAL STATUS` and `SHOW SESSION STATUS` are the same list here:
    // there is one server and nothing per session to differ.
    if next.eq_ignore_ascii_case("GLOBAL") || next.eq_ignore_ascii_case("SESSION") {
        next = words.next()?;
    }
    if !next.eq_ignore_ascii_case("STATUS") {
        return None;
    }

    Some(sql::quoted_argument(sql, "LIKE").map(str::to_string))
}

/// The status rows this server reports, in MySQL's own order, with the `LIKE`
/// filter applied when the client gave one.
pub fn status_rows(filter: Option<&str>) -> Vec<(&'static str, u64)> {
    // One test of `group_concat_max_len LIKE 'x'` would let a LIKE pattern that
    // begins with `Com_` be answered literally; matching with the wildcard rules
    // is what MySQL does and what a client's own pattern expects.
    /// A counter: its name and how to read it.
    type Counter = (&'static str, fn() -> u64);

    let rows: [Counter; 2] = [("Com_commit", com_commit), ("Com_rollback", com_rollback)];

    rows.into_iter()
        .filter(|(name, _)| filter.is_none_or(|pattern| like(pattern, name)))
        .map(|(name, value)| (name, value()))
        .collect()
}

/// MySQL's `LIKE`: `%` is any run of characters, `_` is one, and everything else
/// is itself. Case-insensitive, as MySQL's default collation is for these names.
fn like(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.to_lowercase().chars().collect();
    let text: Vec<char> = text.to_lowercase().chars().collect();

    // A greedy wildcard match: the classic linear backtracking algorithm, which
    // is all a two-word pattern needs.
    let (mut p, mut t) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '_' || pattern[p] == text[t]) && pattern[p] != '%' {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == '%' {
            star = Some((p, t));
            p += 1;
        } else if let Some((sp, st)) = star {
            p = sp + 1;
            t = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '%' {
        p += 1;
    }

    p == pattern.len()
}

/// **`SELECT @var, ...`**: the OUT parameters a stored-procedure client reads
/// after its `CALL`.
///
/// Only user variables, and only variables: a select of a column is not this.
pub fn select_vars(sql: &str) -> Option<Vec<String>> {
    let list = sql::select_list(sql)?;

    let mut names = Vec::new();
    for item in sql::split_top_level(list) {
        let name = item.trim().strip_prefix('@')?;
        // `@@version` is a system variable, and this rule does not answer it.
        if name.starts_with('@') {
            return None;
        }
        names.push(name.trim_matches('`').to_string());
    }
    if names.is_empty() {
        return None;
    }

    Some(names)
}

/// The value every `SELECT @...` answers: a number, so a client that parses it
/// as one -- an id, a count, an amount -- is not given a string it cannot use.
pub const VAR_VALUE: &str = "1";

#[cfg(test)]
mod tests {
    use super::*;

    /// **The district sum is recognised as HammerDB spells it**, with the
    /// whitespace and case a client varies and the schema prefix it may add.
    #[test]
    fn the_district_sum_is_recognised() {
        for sql in [
            "select sum(d_next_o_id) from district",
            "SELECT sum(d_next_o_id) FROM district",
            "select sum ( d_next_o_id ) from district",
            "select sum(d_next_o_id) from tpcc_district",
            "select sum(d_next_o_id) from tpcc.district",
            "  select\n  sum(d_next_o_id)\n  from\n  district  ",
            "select sum(d_next_o_id) from district;",
        ] {
            assert!(is_district_sum(sql), "{sql:?} is the district sum");
        }

        for sql in [
            "select sum(d_next_o_id), count(*) from district",
            "select d_next_o_id from district",
            "select sum(d_next_o_id) from warehouse",
            "select sum(d_next_o_id) from district where d_id = 1",
            "update district set d_next_o_id = d_next_o_id + 1",
            "insert into district values (1, 2, 3)",
        ] {
            assert!(!is_district_sum(sql), "{sql:?} is not the district sum");
        }
    }

    /// **A CALL is counted by the procedure it names**, in either case and
    /// under a schema prefix.
    #[test]
    fn a_call_is_counted_by_its_procedure() {
        for (sql, kind) in [
            ("call NEWORD(1, 2, 3, 4)", Kind::NewOrder),
            ("CALL neword(1, 2, 3, 4)", Kind::NewOrder),
            ("call PAYMENT(1, 2)", Kind::Payment),
            ("call DELIVERY(1)", Kind::Delivery),
            ("call OSTAT(1, 2, 3)", Kind::OrderStatus),
            ("call order_status(1, 2, 3)", Kind::OrderStatus),
            ("call SLEV(1, 2, 3, 4)", Kind::StockLevel),
            ("call tpcc.neword(1, 2, 3, 4)", Kind::NewOrder),
        ] {
            assert_eq!(statement_kind(sql), Some(kind), "{sql}");
        }

        assert_eq!(statement_kind("call unknown_proc(1)"), None);
        assert_eq!(statement_kind("select neword(1)"), None);
        assert_eq!(statement_kind("insert into warehouse values (1)"), None);
    }

    /// **A plain-SQL NewOrder is an INSERT into the new_order table**, which is
    /// the one table only that transaction writes.
    #[test]
    fn an_insert_into_new_order_is_a_new_order() {
        for sql in [
            "insert into new_order values (1, 2, 3)",
            "INSERT INTO tpcc_new_order (no_w_id) VALUES (1)",
            "insert into tpcc.new_order (no_w_id) values (1)",
        ] {
            assert_eq!(statement_kind(sql), Some(Kind::NewOrder), "{sql}");
        }

        assert_eq!(statement_kind("insert into orders values (1)"), None);
    }

    /// **`show global status`, and the `LIKE` a client filters it with.**
    #[test]
    fn show_global_status_is_recognised_with_its_filter() {
        assert_eq!(show_status("show global status"), Some(None));
        assert_eq!(show_status("SHOW STATUS"), Some(None));
        assert_eq!(
            show_status("show global status like 'Com_commit'"),
            Some(Some("Com_commit".to_string()))
        );
        assert_eq!(
            show_status("show global status like 'Com%'"),
            Some(Some("Com%".to_string()))
        );
        assert_eq!(show_status("show tables"), None);
        assert_eq!(show_status("show global variables"), None);
    }

    /// **The filter decides which counters are reported**, with MySQL's wildcard
    /// rules.
    #[test]
    fn the_status_rows_follow_the_filter() {
        assert_eq!(status_rows(None).len(), 2, "both counters unfiltered");
        assert_eq!(
            status_rows(Some("Com_commit")),
            vec![("Com_commit", com_commit())]
        );
        let both = status_rows(Some("Com%"));
        assert_eq!(both.len(), 2, "a prefix matches both");
        assert!(status_rows(Some("Innodb_rows_read")).is_empty());

        assert!(like("Com%", "Com_commit"));
        assert!(like("com_commit", "COM_COMMIT"), "case-insensitive");
        assert!(like("Com_commit", "Com_commit"));
        assert!(like("Com_commi_", "Com_commit"), "_ is one character");
        assert!(!like("Com%x", "Com_commit"));
        assert!(like("%commit", "Com_commit"));
        assert!(like("%", "anything"));
        assert!(!like("", "anything"));
    }

    /// **`SELECT @a, @b` answers its variables**, and a select of columns is not
    /// mistaken for it.
    #[test]
    fn select_variables_are_recognised() {
        assert_eq!(
            select_vars("select @neword_out, @d_next_o_id"),
            Some(vec!["neword_out".to_string(), "d_next_o_id".to_string()])
        );
        assert_eq!(select_vars("select @x"), Some(vec!["x".to_string()]));
        assert_eq!(select_vars("select @@version"), None);
        assert_eq!(select_vars("select w_id from warehouse"), None);
        assert_eq!(
            select_vars("select @x, w_id"),
            None,
            "mixed is not this rule"
        );
    }

    /// **The district sum grows by one per NewOrder**, over the districts the
    /// schema is said to have. The absolute value is what a client reads twice;
    /// the difference is what it reports.
    #[test]
    fn the_district_sum_starts_at_3001_a_district_and_grows_by_new_orders() {
        let before_kind = STATS.new_order.load(Ordering::Relaxed);
        set_warehouses(3);

        let at_start = district_sum();
        assert_eq!(at_start, 3001 * 30 + before_kind, "3001 per district");

        note(Kind::NewOrder);
        assert_eq!(district_sum(), at_start + 1, "one NewOrder, one more");

        // Another kind of transaction does not move it: only a NewOrder
        // advances a district's next order id.
        note(Kind::Payment);
        assert_eq!(district_sum(), at_start + 1);

        set_warehouses(DEFAULT_WAREHOUSES);
    }

    /// The counters a client reads are the ones the statements counted, and the
    /// line on stderr says the same.
    #[test]
    fn the_counters_are_reported() {
        let commits = com_commit();
        let payments = STATS.payment.load(Ordering::Relaxed);
        note_commit();
        note_rollback();
        note(Kind::Payment);

        assert_eq!(com_commit(), commits + 1, "a commit is counted");
        assert!(com_rollback() > 0);
        assert_eq!(STATS.payment.load(Ordering::Relaxed), payments + 1);

        let line = line();
        for field in [
            "new_order=",
            "payment=",
            "delivery=",
            "order_status=",
            "stock_level=",
            "com_commit=",
            "com_rollback=",
            "district_sum=",
        ] {
            assert!(line.contains(field), "{field} missing from {line:?}");
        }
    }
}
