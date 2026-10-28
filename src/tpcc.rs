//! The TPC-C rules: the only statements answered with more than a no-op.
//! A TPC-C client reads its throughput from the server, so a handful of statement
//! shapes are matched and answered with numbers derived from `--warehouses`.

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
    /// The procedure names this kind answers to.
    /// A TPC-C client spells them `NEWORD`, `PAYMENT`, `DELIVERY`, `OSTAT`, `SLEV`
    /// (HammerDB's stored procedures) or `neword`, `payment`, ...
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

    /// Its index in `Pending`'s counts.
    fn index(self) -> usize {
        match self {
            Kind::NewOrder => 0,
            Kind::Payment => 1,
            Kind::Delivery => 2,
            Kind::OrderStatus => 3,
            Kind::StockLevel => 4,
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

/// How many warehouses the schema is said to have.
/// Read at startup from `--warehouses`, and a `OnceLock` rather than a constant because
/// the district sum depends on it.
static WAREHOUSES: AtomicU64 = AtomicU64::new(DEFAULT_WAREHOUSES);

/// HammerDB's own default schema: ten warehouses, and therefore a hundred districts.
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
pub fn district_sum() -> u64 {
    3001 * districts() + new_orders()
}

/// Whether the synthetic ANSWERS are on, which `--profile tpcc` says.
static PROFILE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn set_profile(on: bool) {
    PROFILE.store(on, Ordering::Relaxed);
}

pub fn profile() -> bool {
    PROFILE.load(Ordering::Relaxed)
}

/// A one-number answer a TPC-C client reads, when the profile is on.
pub fn number_query(sql: &str) -> Option<(&'static str, u64)> {
    if !profile() {
        return None;
    }

    let trimmed = sql::trim_leading(sql);
    let mut words = sql::Words::new(trimmed).map(|w| w.to_ascii_lowercase());
    let first = words.next()?;
    if first != "select" {
        return None;
    }

    // The aggregate, then the table it is over.
    // The argument is skipped: the scanner drops punctuation, so `count(*)` is the one
    // word `count`.
    let aggregate = words.next()?;
    words.by_ref().find(|w| w == "from")?;
    // The table is the LAST word before a clause: `from tpcc.district` is the words
    // `tpcc` and `district`, and the table is the second.
    let mut table = None;
    for word in words.by_ref() {
        if [
            "where", "group", "order", "limit", "having", "offset", "union", "for",
        ]
        .iter()
        .any(|c| word == *c)
        {
            break;
        }
        table = Some(word);
    }
    let table = table?;
    let table = table.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '_');
    // A TPC-C schema may prefix its tables: `tpcc_district` is a district.
    let table = table.strip_prefix("tpcc_").unwrap_or(table);

    let districts = districts();
    let items = match (aggregate.as_str(), table) {
        // The NOPM read: 3001 per district at the start, plus one per NewOrder.
        (a, "district") if a == "sum" && trimmed.to_lowercase().contains("d_next_o_id") => {
            Some(("sum(d_next_o_id)", district_sum()))
        }
        ("max", "warehouse") => Some(("max(w_id)", WAREHOUSES.load(Ordering::Relaxed))),
        ("max", "district") => Some(("max(d_id)", 10)),
        ("max", "customer") => Some(("max(c_id)", 3000)),
        ("max", "orders") | ("max", "oorder") => Some(("max(o_id)", 3000)),
        // A population count, plus the transactions this process has seen: a client
        // that counts orders before and after its window reads a difference, exactly as
        // it does with the district sum.
        ("count", "orders") | ("count", "oorder") => {
            Some(("count(*)", 3000 * districts + new_orders()))
        }
        ("count", "new_order") => Some(("count(*)", 900 * districts)),
        ("count", "warehouse") => Some(("count(*)", WAREHOUSES.load(Ordering::Relaxed))),
        ("count", "district") => Some(("count(*)", districts)),
        // HammerDB's PostgreSQL TPM: the transactions the server has committed and
        // rolled back.
        ("sum", "pg_stat_database") => Some(("sum", com_commit() + com_rollback())),
        _ => None,
    };

    items
}

/// What one connection has done but not yet committed.
#[derive(Default, Clone, Copy)]
pub struct Pending {
    counts: [u64; 5],
}

impl Pending {
    /// Note one statement of this kind.
    /// Nothing is counted yet.
    pub fn note(&mut self, kind: Kind) {
        self.counts[kind.index()] += 1;
    }

    /// Commit: the noted transactions are counted, and the commit is one more.
    pub fn commit(&mut self) {
        for kind in Kind::ALL {
            let n = std::mem::take(&mut self.counts[kind.index()]);
            if n > 0 {
                kind.counter().fetch_add(n, Ordering::Relaxed);
            }
        }
        note_commit();
    }

    /// Roll back: the noted transactions never happened.
    pub fn discard(&mut self) {
        self.counts = [0; 5];
    }
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
    if profile() {
        // A figure taken with the synthetic answers on is not a measurement of a
        // database, and the line must say so wherever it is read.
        line.push_str(" [synthetic NOPM]");
    }

    line
}

/// The profile switched on (or off) for the life of a test, with the switch held: it is
/// process-wide, and two tests that set it differently race.
#[cfg(test)]
pub fn profile_guard(on: bool) -> ProfileGuard {
    state_guard(on, WAREHOUSES.load(Ordering::Relaxed))
}

/// Hold the process-wide TPC-C state -- the profile and the schema size -- for the
/// life of a test: both are globals, and two tests that set them differently race.
#[cfg(test)]
pub fn state_guard(on: bool, warehouses: u64) -> ProfileGuard {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    let held = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let profile_before = profile();
    let warehouses_before = WAREHOUSES.load(Ordering::Relaxed);
    set_profile(on);
    set_warehouses(warehouses);

    ProfileGuard {
        _held: held,
        profile: profile_before,
        warehouses: warehouses_before,
    }
}

#[cfg(test)]
pub struct ProfileGuard {
    _held: std::sync::MutexGuard<'static, ()>,
    profile: bool,
    warehouses: u64,
}

#[cfg(test)]
impl Drop for ProfileGuard {
    fn drop(&mut self) {
        set_profile(self.profile);
        set_warehouses(self.warehouses);
    }
}

/// The value of a MySQL system variable/// A failure the client asked for, and the path
/// it exercises.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Failure {
    /// `SIGNAL SQLSTATE '<code>'`: the client names the error it wants.
    Signal(String),
    /// A NULL written where a NOT NULL column would refuse it.
    Null,
    /// `RAISE`, the PostgreSQL way of failing on purpose.
    Raise,
    /// A `CALL` of a procedure this server has no rule for.
    /// A client that calls something we do not know is a client whose expectations we
    /// cannot meet, and answering OK hides that.
    UnknownProcedure,
}

/// The failure a statement asks for, if it asks for one.
/// Deliberately mechanical: the shapes are listed here and nowhere else.
pub fn failure(sql: &str) -> Option<Failure> {
    let trimmed = sql::trim_leading(sql);
    let mut words = sql::Words::new(trimmed);

    // `SIGNAL [SQLSTATE] '<code>'`
    if words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("SIGNAL"))
    {
        // `SIGNAL '<code>'` without the keyword, else the generic one.
        let code = sql::quoted_argument(trimmed, "SQLSTATE")
            .map(str::to_string)
            .or_else(|| {
                let rest = sql::trim_leading(&trimmed["SIGNAL".len()..]);

                rest.strip_prefix('\'')
                    .and_then(|r| r.split('\'').next())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "45000".to_string());

        return Some(Failure::Signal(code));
    }

    // `RAISE`, as a word anywhere: `select raise(...)`, `raise_exception(...)`,
    // `raise_sqlstate(...)`.
    // `raise_notice` is a warning and is not one.
    if sql::Words::new(trimmed).any(|w| {
        let lower = w.to_ascii_lowercase();

        (lower == "raise" || lower.starts_with("raise_")) && lower != "raise_notice"
    }) {
        return Some(Failure::Raise);
    }

    // A NULL in the statement's values.
    if writes_null(trimmed) {
        return Some(Failure::Null);
    }

    // `CALL name(...)`: known procedures are transactions, the rest are not.
    if let Some(first) = sql::first_word(trimmed).to_uppercase().as_str().into() {
        let _ = first;
    }
    if sql::Words::new(trimmed)
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("CALL"))
        && statement_kind(trimmed).is_none()
    {
        return Some(Failure::UnknownProcedure);
    }

    None
}

/// Whether an INSERT or UPDATE writes the SQL `NULL` literal into a column.
fn writes_null(sql: &str) -> bool {
    let head = sql::first_word(sql);
    if !head.eq_ignore_ascii_case("INSERT") && !head.eq_ignore_ascii_case("UPDATE") {
        return false;
    }

    sql::Words::new(sql).any(|w| w.eq_ignore_ascii_case("NULL"))
}

/// The transaction a statement is, when it is a TPC-C one.
pub fn statement_kind(sql: &str) -> Option<Kind> {
    let mut words = sql::Words::new(sql);

    match words.next() {
        Some(word) if word.eq_ignore_ascii_case("CALL") => {
            // `CALL neword(...)`, and `CALL tpcc.neword(...)` -- a scanner that splits
            // on `.` gives both, so the first two words after CALL are tried.
            for _ in 0..2 {
                let word = words.next()?;
                if let Some(kind) = Kind::from_procedure(word) {
                    return Some(kind);
                }
            }

            None
        }
        // A PostgreSQL TPC-C client calls a procedure as a function, in a SELECT:
        // `select neword(...)`, `select * from neword(...)`.
        Some(word) if word.eq_ignore_ascii_case("SELECT") || word.eq_ignore_ascii_case("WITH") => {
            called_procedure(sql)
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

/// The TPC-C procedure a statement CALLS, as `name(` outside quotes and comments.
fn called_procedure(sql: &str) -> Option<Kind> {
    let bytes = sql.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => i = sql::skip_quoted(bytes, i),
            b'-' | b'#' | b'/' => match sql::skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            c if sql::is_word_byte(c) => {
                let end = sql::word_end(bytes, i)?;
                let word = &sql[i..end];
                let after = sql::skip_space(bytes, end);
                if after < bytes.len() && bytes[after] == b'(' {
                    if let Some(kind) = Kind::from_procedure(word) {
                        return Some(kind);
                    }
                }
                i = end;
            }
            _ => i += 1,
        }
    }

    None
}

/// `SHOW GLOBAL STATUS`, and the filter a client gives it.
/// HammerDB reads `Com_commit` and `Com_rollback` from it; a `LIKE` filter is how it
/// asks for one at a time.
pub fn show_status(sql: &str) -> Option<Option<String>> {
    let mut words = sql::Words::new(sql);
    if !words.next()?.eq_ignore_ascii_case("SHOW") {
        return None;
    }
    let mut next = words.next()?;
    // `SHOW GLOBAL STATUS` and `SHOW SESSION STATUS` are the same list here: there is
    // one server and nothing per session to differ.
    if next.eq_ignore_ascii_case("GLOBAL") || next.eq_ignore_ascii_case("SESSION") {
        next = words.next()?;
    }
    if !next.eq_ignore_ascii_case("STATUS") {
        return None;
    }

    Some(sql::quoted_argument(sql, "LIKE").map(str::to_string))
}

/// The status rows this server reports, in MySQL's own order, with the `LIKE` filter
/// applied when the client gave one.
pub fn status_rows(filter: Option<&str>) -> Vec<(&'static str, u64)> {
    // Wildcards are matched as MySQL matches them; a literal compare would answer
    // `Com%` with nothing.
    /// A counter: its name and how to read it.
    type Counter = (&'static str, fn() -> u64);

    let rows: [Counter; 2] = [("Com_commit", com_commit), ("Com_rollback", com_rollback)];

    rows.into_iter()
        .filter(|(name, _)| filter.is_none_or(|pattern| like(pattern, name)))
        .map(|(name, value)| (name, value()))
        .collect()
}

/// MySQL's `LIKE`: `%` is any run of characters, `_` is one, and everything else is
/// itself.
/// Case-insensitive, as MySQL's default collation is for these names.
fn like(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.to_lowercase().chars().collect();
    let text: Vec<char> = text.to_lowercase().chars().collect();

    // A greedy wildcard match: the classic linear backtracking algorithm, which is all
    // a two-word pattern needs.
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

/// `SELECT @var, ...`: the OUT parameters a stored-procedure client reads after its
/// `CALL`.
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

/// The value every `SELECT @...` answers: a number, so a client that parses it as one
/// -- an id, a count, an amount -- is not given a string it cannot use.
pub const VAR_VALUE: &str = "1";

#[cfg(test)]
mod tests {
    use super::*;

    /// The district sum is recognised as HammerDB spells it, with the whitespace and
    /// case a client varies and the schema prefix it may add.
    /// A CALL is counted by the procedure it names, in either case and under a schema
    /// prefix.
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
        assert_eq!(statement_kind("insert into warehouse values (1)"), None);
    }

    /// A PostgreSQL TPC-C client calls its procedures as functions, in a SELECT:
    /// `select neword(...)`, `select * from neword(...)`, and the same statements
    /// prepared.
    #[test]
    fn a_called_function_is_the_same_transaction() {
        for (sql, kind) in [
            ("select neword(1, 2, 3, 4)", Kind::NewOrder),
            ("SELECT neword(1, 2, 3, 4)", Kind::NewOrder),
            ("select * from neword(1, 2, 3, 4)", Kind::NewOrder),
            ("select neword (1, 2, 3, 4)", Kind::NewOrder),
            ("select payment(1, 2, 3, 4)", Kind::Payment),
            ("select * from delivery(1)", Kind::Delivery),
            ("select * from ostat(1, 2)", Kind::OrderStatus),
            ("select * from slev(1, 2, 3)", Kind::StockLevel),
            ("select tpcc.neword(1, 2, 3, 4)", Kind::NewOrder),
            // Prepared, the statement keeps its text: the count happens when it runs,
            // not when it is parsed.
            ("select * from neword($1, $2, $3, $4)", Kind::NewOrder),
        ] {
            assert_eq!(statement_kind(sql), Some(kind), "{sql}");
        }

        // A name that is not called is not a transaction: a column called `payment`, or
        // a table someone named after the procedure.
        assert_eq!(statement_kind("select payment from orders"), None);
        assert_eq!(
            statement_kind("select * from orders where payment = 1"),
            None
        );
        assert_eq!(statement_kind("select count(*) from new_order"), None);
    }

    /// A plain-SQL NewOrder is an INSERT into the new_order table, which is the one
    /// table only that transaction writes.
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

    /// `show global status`, and the `LIKE` a client filters it with.
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

    /// The filter decides which counters are reported, with MySQL's wildcard rules.
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

    /// The failures a client asks for are recognised, and nothing else is.
    #[test]
    fn an_injected_failure_is_recognised() {
        assert_eq!(
            failure("SIGNAL SQLSTATE '45000'"),
            Some(Failure::Signal("45000".to_string()))
        );
        assert_eq!(
            failure("signal '40001'"),
            Some(Failure::Signal("40001".to_string()))
        );
        assert_eq!(
            failure("insert into new_order (no_w_id) values (NULL)"),
            Some(Failure::Null)
        );
        assert_eq!(
            failure("update district set d_next_o_id = NULL"),
            Some(Failure::Null)
        );
        assert_eq!(
            failure("select raise_exception('no stock')"),
            Some(Failure::Raise)
        );
        assert_eq!(
            failure("call nosuch_procedure(1, 2)"),
            Some(Failure::UnknownProcedure)
        );

        // A known procedure is a transaction, not a failure; a NULL in a SELECT is a
        // value the server answers.
        assert_eq!(failure("call NEWORD(1, 2, 3, 4)"), None);
        assert_eq!(failure("select NULL"), None);
        assert_eq!(failure("insert into new_order values (1, 2, 3)"), None);
    }

    /// `SELECT @a, @b` answers its variables, and a select of columns is not mistaken
    /// for it.
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

    /// The district sum grows by one per NewOrder, over the districts the schema is
    /// said to have.
    #[test]
    fn the_district_sum_starts_at_3001_a_district_and_grows_by_new_orders() {
        let _state = state_guard(false, 3);
        let before_kind = STATS.new_order.load(Ordering::Relaxed);

        let at_start = district_sum();
        assert_eq!(at_start, 3001 * 30 + before_kind, "3001 per district");

        let mut pending = Pending::default();
        pending.note(Kind::NewOrder);
        pending.commit();
        assert_eq!(district_sum(), at_start + 1, "one NewOrder, one more");

        // Another kind of transaction does not move it: only a NewOrder advances a
        // district's next order id.
        let mut pending = Pending::default();
        pending.note(Kind::Payment);
        pending.commit();
        assert_eq!(district_sum(), at_start + 1);

        set_warehouses(DEFAULT_WAREHOUSES);
    }

    /// The one-number answers are the profile's, and they come from the schema size.
    /// Off, none of them is answered at all.
    #[test]
    fn the_population_answers_need_the_profile() {
        let _state = state_guard(false, 10);
        for sql in [
            "select sum(d_next_o_id) from district",
            "select max(w_id) from warehouse",
            "select count(*) from orders",
        ] {
            assert_eq!(number_query(sql), None, "{sql} without the profile");
        }

        set_profile(true);
        assert_eq!(
            number_query("select sum(d_next_o_id) from district"),
            Some(("sum(d_next_o_id)", 3001 * 100 + new_orders()))
        );
        assert_eq!(
            number_query("select max(w_id) from warehouse"),
            Some(("max(w_id)", 10))
        );
        assert_eq!(
            number_query("select max(d_id) from district"),
            Some(("max(d_id)", 10))
        );
        assert_eq!(
            number_query("select count(*) from orders"),
            Some(("count(*)", 3000 * 100 + new_orders()))
        );
        assert_eq!(
            number_query("select count(*) from new_order"),
            Some(("count(*)", 900 * 100))
        );
        assert_eq!(
            number_query("select sum(xact_commit+xact_rollback) from pg_stat_database"),
            Some(("sum", com_commit() + com_rollback()))
        );
        assert_eq!(number_query("select 1"), None, "a literal is not a count");
        assert_eq!(number_query("select a from t"), None);

        // The line says which kind of number it is reporting.
        assert!(line().contains("[synthetic NOPM]"));
        set_profile(false);
        assert!(!line().contains("[synthetic NOPM]"));
    }

    /// A transaction is counted at COMMIT, and a rollback counts nothing.
    /// The note lives with the connection until then.
    #[test]
    fn a_transaction_is_counted_at_commit_not_when_it_runs() {
        let before = new_orders();
        let mut pending = Pending::default();

        pending.note(Kind::NewOrder);
        assert_eq!(new_orders(), before, "nothing is counted until it commits");

        pending.discard();
        pending.commit();
        assert_eq!(
            new_orders(),
            before,
            "and commits what is left, which is nothing",
        );

        let mut pending = Pending::default();
        pending.note(Kind::NewOrder);
        pending.note(Kind::NewOrder);
        pending.commit();
        assert_eq!(new_orders(), before + 2, "two NewOrders, one commit");
    }

    /// The counters a client reads are the ones the statements counted, and the line on
    /// stderr says the same.
    #[test]
    fn the_counters_are_reported() {
        let commits = com_commit();
        let payments = STATS.payment.load(Ordering::Relaxed);
        note_commit();
        note_rollback();
        let mut pending = Pending::default();
        pending.note(Kind::Payment);
        pending.commit();

        assert!(com_commit() > commits, "a commit is counted");
        assert!(com_rollback() > 0);
        assert!(STATS.payment.load(Ordering::Relaxed) > payments);

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
