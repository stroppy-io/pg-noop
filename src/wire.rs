//! A sans-io PostgreSQL wire codec.
//!
//! # Why sans-io
//!
//! The protocol here does NO I/O. It is `(&[u8] in, &mut Vec<u8> out)` over a
//! state machine, so who reads the bytes and who writes them is somebody else's
//! decision -- epoll today, io_uring next, a unit test in between.
//!
//! That is not architectural taste, it is the fix for a measured defect. While
//! pgwire owned the socket it flushed each backend message as it produced them,
//! so pg-noop answered every query with THREE `sendto` calls -- BindComplete(5
//! bytes), DataRow+CommandComplete(26), ReadyForQuery(6) -- where PostgreSQL
//! writes the same 37 bytes once. Three syscalls and three wakeups per query is
//! why a server that executes nothing was losing to a database that executes
//! everything. A codec that only appends to a buffer cannot flush early: the
//! caller decides, and it decides once per read.
//!
//! # What it is not
//!
//! Not a PostgreSQL. It answers structurally: a SELECT it cannot resolve gets
//! one stub row of `1`s, DML gets a tag claiming one row. The one thing it does
//! track is the schema, because a client that creates a table and then selects
//! from it must see its own column names back.

use std::collections::HashMap;

use crate::handler::{CatalogView, PlanKind, PreparedPlan};

/// Frontend messages the blackhole understands. Anything else is consumed and
/// answered as though it succeeded, which is the whole contract.
mod tag {
    pub const QUERY: u8 = b'Q';
    pub const PARSE: u8 = b'P';
    pub const BIND: u8 = b'B';
    pub const DESCRIBE: u8 = b'D';
    pub const EXECUTE: u8 = b'E';
    pub const SYNC: u8 = b'S';
    pub const FLUSH: u8 = b'H';
    pub const CLOSE: u8 = b'C';
    pub const TERMINATE: u8 = b'X';
}

/// Where a connection is in its life. The startup exchange is unframed and has
/// to be recognised by shape, not by a tag byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Startup,
    Query,
    Closed,
}

pub struct Conn {
    phase: Phase,
    /// Prepared statements by name. "" is the unnamed statement, which clients
    /// reuse constantly, so it is a normal entry rather than a special case.
    statements: HashMap<String, PreparedPlan>,
    /// Portal name -> statement name.
    portals: HashMap<String, String>,
}

impl Default for Conn {
    fn default() -> Self {
        Self::new()
    }
}

impl Conn {
    pub fn new() -> Self {
        Conn {
            phase: Phase::Startup,
            statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// Consume every COMPLETE message in `input`, appending replies to `out`.
    ///
    /// Returns how many bytes were consumed. A partial message at the end is
    /// left alone -- the caller keeps it and calls again with more. Nothing is
    /// written to a socket here, which is the point.
    pub fn advance(&mut self, input: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> usize {
        let mut pos = 0usize;

        loop {
            if self.phase == Phase::Closed {
                return pos;
            }

            let rest = &input[pos..];

            let consumed = match self.phase {
                Phase::Startup => self.startup(rest, out),
                Phase::Query => self.message(rest, out, catalog),
                Phase::Closed => return pos,
            };

            match consumed {
                Some(n) if n > 0 => pos += n,
                // Incomplete or nothing to do: keep the tail for next time.
                _ => return pos,
            }
        }
    }

    /// The unframed startup exchange: `[len:i32][code:i32][payload]`.
    fn startup(&mut self, input: &[u8], out: &mut Vec<u8>) -> Option<usize> {
        if input.len() < 8 {
            return None;
        }

        let len = i32::from_be_bytes(input[0..4].try_into().ok()?) as usize;
        if len < 8 || input.len() < len {
            return None;
        }

        let code = i32::from_be_bytes(input[4..8].try_into().ok()?);

        match code {
            // SSLRequest / GSSENCRequest: decline, stay in Startup for the real
            // StartupMessage that follows on the same connection.
            80877103 | 80877104 => {
                out.push(b'N');
                Some(len)
            }
            // CancelRequest: there is nothing to cancel.
            80877102 => {
                self.phase = Phase::Closed;
                Some(len)
            }
            _ => {
                // AuthenticationOk
                msg(out, b'R', |b| b.extend_from_slice(&0i32.to_be_bytes()));
                // The few ParameterStatus values a driver reads before it will
                // talk. pgx wants server_version and client_encoding at minimum.
                for (k, v) in [
                    ("server_version", "17.0 (pg-noop)"),
                    ("client_encoding", "UTF8"),
                    ("DateStyle", "ISO, MDY"),
                    ("integer_datetimes", "on"),
                    ("standard_conforming_strings", "on"),
                    ("TimeZone", "UTC"),
                ] {
                    msg(out, b'S', |b| {
                        cstr(b, k);
                        cstr(b, v);
                    });
                }
                // BackendKeyData: pid and secret, both arbitrary.
                msg(out, b'K', |b| {
                    b.extend_from_slice(&1i32.to_be_bytes());
                    b.extend_from_slice(&1i32.to_be_bytes());
                });
                ready(out);
                self.phase = Phase::Query;
                Some(len)
            }
        }
    }

    /// One tagged message: `[tag:u8][len:i32][payload]`, len covering itself.
    fn message(&mut self, input: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> Option<usize> {
        if input.len() < 5 {
            return None;
        }

        let t = input[0];
        let len = i32::from_be_bytes(input[1..5].try_into().ok()?) as usize;
        if len < 4 || input.len() < 1 + len {
            return None;
        }

        let body = &input[5..1 + len];
        let total = 1 + len;

        match t {
            tag::TERMINATE => {
                self.phase = Phase::Closed;
            }

            tag::QUERY => {
                let sql = cstr_read(body).unwrap_or("");
                let plan = PreparedPlan::build(sql);
                self.answer(&plan, out, catalog, true);
                ready(out);
            }

            tag::PARSE => {
                // [stmt_name][query][n_params:i16][types...]
                let (name, after) = cstr_at(body, 0)?;
                let (sql, _) = cstr_at(body, after)?;
                self.statements
                    .insert(name.to_string(), PreparedPlan::build(sql));
                msg(out, b'1', |_| {});
            }

            tag::BIND => {
                // [portal][stmt_name] then parameters we never look at.
                let (portal, after) = cstr_at(body, 0)?;
                let (stmt, _) = cstr_at(body, after)?;
                self.portals.insert(portal.to_string(), stmt.to_string());
                msg(out, b'2', |_| {});
            }

            tag::DESCRIBE => {
                // ['S'|'P'][name]
                let kind = body.first().copied().unwrap_or(b'P');
                let (name, _) = cstr_at(body, 1)?;
                let plan = self.plan_for(kind, name);

                // A described STATEMENT also gets its parameter types, and a
                // blackhole derives none -- an empty list is the honest answer.
                if kind == b'S' {
                    msg(out, b't', |b| b.extend_from_slice(&0i16.to_be_bytes()));
                }

                match plan.and_then(|p| row_description(p, catalog)) {
                    Some(desc) => out.extend_from_slice(&desc),
                    None => msg(out, b'n', |_| {}),
                }
            }

            tag::EXECUTE => {
                let (portal, _) = cstr_at(body, 0)?;
                let plan = self
                    .portals
                    .get(portal)
                    .and_then(|s| self.statements.get(s))
                    .cloned();

                match plan {
                    // Execute does NOT re-send RowDescription; Describe did.
                    Some(p) => self.answer(&p, out, catalog, false),
                    None => msg(out, b'C', |b| cstr(b, "SELECT 0")),
                }
            }

            tag::SYNC => ready(out),

            // Flush means "send what you have". The caller writes `out` after
            // this call returns, so there is nothing to do but not swallow it.
            tag::FLUSH => {}

            tag::CLOSE => {
                let kind = body.first().copied().unwrap_or(b'P');
                if let Some((name, _)) = cstr_at(body, 1) {
                    if kind == b'S' {
                        self.statements.remove(name);
                    } else {
                        self.portals.remove(name);
                    }
                }
                msg(out, b'3', |_| {});
            }

            // Unknown: consumed and ignored, which is what a blackhole is for.
            _ => {}
        }

        Some(total)
    }

    fn plan_for(&self, kind: u8, name: &str) -> Option<&PreparedPlan> {
        if kind == b'S' {
            self.statements.get(name)
        } else {
            self.portals
                .get(name)
                .and_then(|s| self.statements.get(s))
        }
    }

    /// The rows and the completion tag for one plan.
    fn answer(&self, plan: &PreparedPlan, out: &mut Vec<u8>, catalog: &CatalogView, describe: bool) {
        match &plan.kind {
            PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. } => {
                let cols = plan_columns(plan, catalog);
                if describe {
                    if let Some(desc) = row_description(plan, catalog) {
                        out.extend_from_slice(&desc);
                    }
                }
                // A metadata select answers empty; a stub select answers one row.
                let rows = if matches!(plan.kind, PlanKind::SelectMeta { .. }) && cols.resolved {
                    0
                } else {
                    data_row(out, cols.count);
                    1
                };
                msg(out, b'C', |b| cstr(b, &format!("SELECT {rows}")));
            }
            PlanKind::Insert => msg(out, b'C', |b| cstr(b, "INSERT 0 1")),
            PlanKind::Update => msg(out, b'C', |b| cstr(b, "UPDATE 1")),
            PlanKind::Delete => msg(out, b'C', |b| cstr(b, "DELETE 1")),
            PlanKind::Begin => msg(out, b'C', |b| cstr(b, "BEGIN")),
            PlanKind::Commit => msg(out, b'C', |b| cstr(b, "COMMIT")),
            PlanKind::Rollback => msg(out, b'C', |b| cstr(b, "ROLLBACK")),
            PlanKind::Ddl => {
                catalog.apply(&plan.sql);
                msg(out, b'C', |b| cstr(b, ddl_tag(&plan.sql)));
            }
            PlanKind::FromText => msg(out, b'C', |b| cstr(b, "SELECT 0")),
        }
    }
}

// ------------------------------------------------------------------ encoding

/// Append `[tag][len:i32][body]`, with len covering itself.
fn msg(out: &mut Vec<u8>, tag: u8, body: impl FnOnce(&mut Vec<u8>)) {
    out.push(tag);
    let at = out.len();
    out.extend_from_slice(&0i32.to_be_bytes());
    body(out);
    let len = (out.len() - at) as i32;
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

fn cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn ready(out: &mut Vec<u8>) {
    msg(out, b'Z', |b| b.push(b'I'));
}

/// One DataRow of `n` int8 columns, every value the text "1".
fn data_row(out: &mut Vec<u8>, n: usize) {
    msg(out, b'D', |b| {
        b.extend_from_slice(&(n as i16).to_be_bytes());
        for _ in 0..n {
            b.extend_from_slice(&1i32.to_be_bytes());
            b.push(b'1');
        }
    });
}

struct Columns {
    count: usize,
    resolved: bool,
}

fn plan_columns(plan: &PreparedPlan, catalog: &CatalogView) -> Columns {
    match &plan.kind {
        PlanKind::SelectMeta { table, columns } => match catalog.columns_for(table, columns) {
            Some(names) => Columns {
                count: names.len(),
                resolved: true,
            },
            None => Columns {
                count: columns.len().max(1),
                resolved: false,
            },
        },
        PlanKind::SelectStub { columns } => Columns {
            count: *columns,
            resolved: false,
        },
        _ => Columns {
            count: 0,
            resolved: false,
        },
    }
}

/// RowDescription, using the client's own column names when the catalog knows
/// the table -- a driver that created a table and selects from it gets its names
/// back, which is the one piece of real behaviour this server has.
fn row_description(plan: &PreparedPlan, catalog: &CatalogView) -> Option<Vec<u8>> {
    let (names, oids) = match &plan.kind {
        PlanKind::SelectMeta { table, columns } => match catalog.columns_for(table, columns) {
            Some(cols) => {
                let (n, o): (Vec<String>, Vec<u32>) = cols.into_iter().unzip();
                (n, o)
            }
            None => stub_names(columns.len().max(1)),
        },
        PlanKind::SelectStub { columns } => stub_names(*columns),
        _ => return None,
    };

    let mut buf = Vec::with_capacity(16 + names.len() * 24);
    msg(&mut buf, b'T', |b| {
        b.extend_from_slice(&(names.len() as i16).to_be_bytes());
        for (name, oid) in names.iter().zip(oids.iter()) {
            cstr(b, name);
            b.extend_from_slice(&0i32.to_be_bytes()); // table oid
            b.extend_from_slice(&0i16.to_be_bytes()); // column no
            b.extend_from_slice(&oid.to_be_bytes()); // type oid
            b.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            b.extend_from_slice(&0i16.to_be_bytes()); // text format
        }
    });

    Some(buf)
}

fn stub_names(n: usize) -> (Vec<String>, Vec<u32>) {
    ((0..n).map(|_| "?column?".to_string()).collect(), vec![20; n])
}

fn ddl_tag(sql: &str) -> &'static str {
    let head = sql.trim_start();
    if head.len() >= 6 && head.as_bytes()[..6].eq_ignore_ascii_case(b"CREATE") {
        "CREATE TABLE"
    } else {
        "DROP TABLE"
    }
}

// ------------------------------------------------------------------ decoding

fn cstr_read(body: &[u8]) -> Option<&str> {
    cstr_at(body, 0).map(|(s, _)| s)
}

/// The NUL-terminated string starting at `from`, and the index after its NUL.
fn cstr_at(body: &[u8], from: usize) -> Option<(&str, usize)> {
    let rest = body.get(from..)?;
    let end = rest.iter().position(|&b| b == 0)?;

    Some((std::str::from_utf8(&rest[..end]).ok()?, from + end + 1))
}

// ---------------------------------------------------------------------- tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::NoopHandler;
    use std::sync::Arc;

    fn view() -> CatalogView {
        CatalogView(Arc::new(NoopHandler::new()))
    }

    /// A startup packet: [len][code][payload].
    fn startup_msg() -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&0i32.to_be_bytes());
        b.extend_from_slice(&196608i32.to_be_bytes());
        cstr(&mut b, "user");
        cstr(&mut b, "bench");
        b.push(0);
        let len = b.len() as i32;
        b[0..4].copy_from_slice(&len.to_be_bytes());
        b
    }

    fn tagged(tag: u8, body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut b = Vec::new();
        msg(&mut b, tag, body);
        b
    }

    /// Every message tag present in `out`, in order. The property most of these
    /// tests assert is about the SEQUENCE the client sees.
    fn tags(out: &[u8]) -> Vec<u8> {
        let mut seen = Vec::new();
        let mut i = 0;
        while i + 5 <= out.len() {
            let len = i32::from_be_bytes(out[i + 1..i + 5].try_into().unwrap()) as usize;
            seen.push(out[i]);
            i += 1 + len;
        }
        seen
    }

    fn connected() -> (Conn, CatalogView) {
        let (mut c, v) = (Conn::new(), view());
        let mut out = Vec::new();
        let used = c.advance(&startup_msg(), &mut out, &v);
        assert_eq!(used, startup_msg().len());
        (c, v)
    }

    #[test]
    fn ssl_request_is_declined_without_leaving_startup() {
        let (mut c, v) = (Conn::new(), view());
        let mut req = Vec::new();
        req.extend_from_slice(&8i32.to_be_bytes());
        req.extend_from_slice(&80877103i32.to_be_bytes());

        let mut out = Vec::new();
        assert_eq!(c.advance(&req, &mut out, &v), 8);
        assert_eq!(out, b"N", "a single 'N' is the whole reply");

        // Still in startup: the real StartupMessage follows on this connection.
        let mut out2 = Vec::new();
        c.advance(&startup_msg(), &mut out2, &v);
        assert!(tags(&out2).contains(&b'R'), "authentication must follow");
    }

    #[test]
    fn startup_answers_auth_params_key_and_ready() {
        let (mut c, v) = (Conn::new(), view());
        let mut out = Vec::new();
        c.advance(&startup_msg(), &mut out, &v);

        let t = tags(&out);
        assert_eq!(t.first(), Some(&b'R'), "AuthenticationOk first");
        assert!(t.contains(&b'S'), "ParameterStatus present");
        assert!(t.contains(&b'K'), "BackendKeyData present");
        assert_eq!(t.last(), Some(&b'Z'), "ReadyForQuery last");
        assert!(!c.is_closed());
    }

    /// THE PROPERTY THIS FILE EXISTS FOR. A pipelined Bind/Execute/Sync must
    /// produce ONE contiguous reply, because the caller writes `out` once. The
    /// old pgwire path flushed BindComplete, then DataRow+CommandComplete, then
    /// ReadyForQuery -- three syscalls for these same 37 bytes.
    #[test]
    fn bind_execute_sync_produces_one_contiguous_reply() {
        let (mut c, v) = connected();

        let mut input = tagged(b'P', |b| {
            cstr(b, "s1");
            cstr(b, "select 1");
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "s1");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
        input.extend(tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        let used = c.advance(&input, &mut out, &v);

        assert_eq!(used, input.len(), "the whole batch is consumed");
        assert_eq!(
            tags(&out),
            vec![b'1', b'2', b'D', b'C', b'Z'],
            "ParseComplete, BindComplete, DataRow, CommandComplete, ReadyForQuery"
        );
    }

    /// A message split across reads must not be consumed or answered until it
    /// is whole -- otherwise a slow client desynchronises the stream.
    #[test]
    fn a_partial_message_is_left_for_the_next_read() {
        let (mut c, v) = connected();
        let whole = tagged(b'S', |_| {});

        for cut in 1..whole.len() {
            let mut c2 = Conn::new();
            let mut warm = Vec::new();
            c2.advance(&startup_msg(), &mut warm, &v);

            let mut out = Vec::new();
            assert_eq!(
                c2.advance(&whole[..cut], &mut out, &v),
                0,
                "nothing consumed from a partial message cut at {cut}"
            );
            assert!(out.is_empty(), "and nothing answered");
        }

        // Whole, it is answered.
        let mut out = Vec::new();
        assert_eq!(c.advance(&whole, &mut out, &v), whole.len());
        assert_eq!(tags(&out), vec![b'Z']);
    }

    #[test]
    fn terminate_closes_and_stops_consuming() {
        let (mut c, v) = connected();
        let mut input = tagged(b'X', |_| {});
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        let used = c.advance(&input, &mut out, &v);

        assert!(c.is_closed());
        assert_eq!(used, 5, "consumed Terminate and stopped");
        assert!(out.is_empty(), "a closed connection answers nothing");
    }

    #[test]
    fn a_simple_query_answers_and_is_ready_again() {
        let (mut c, v) = connected();
        let input = tagged(b'Q', |b| cstr(b, "select 1"));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);

        let t = tags(&out);
        assert!(t.contains(&b'T'), "simple query describes its rows");
        assert!(t.contains(&b'D'), "and returns one");
        assert_eq!(t.last(), Some(&b'Z'));
    }

    /// The one piece of real behaviour: a client that creates a table and then
    /// selects from it gets ITS OWN column names back, not stubs.
    #[test]
    fn a_created_tables_columns_come_back_by_name() {
        let (mut c, v) = connected();

        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| {
                cstr(b, "CREATE TABLE demo (id INT PRIMARY KEY, label TEXT)")
            }),
            &mut out,
            &v,
        );

        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "select id, label from demo")),
            &mut out,
            &v,
        );

        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("id"), "the client's own column name: {text:?}");
        assert!(text.contains("label"), "and the second: {text:?}");
        assert!(
            !text.contains("?column?"),
            "a known table must not answer with stubs: {text:?}"
        );
    }

    #[test]
    fn an_unknown_message_is_consumed_rather_than_desynchronising() {
        let (mut c, v) = connected();
        let mut input = tagged(b'!', |b| b.extend_from_slice(b"whatever"));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        assert_eq!(c.advance(&input, &mut out, &v), input.len());
        assert_eq!(tags(&out), vec![b'Z'], "the Sync after it still answers");
    }
}
