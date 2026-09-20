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

use crate::handler::{CatalogView, CopyDirection, PlanKind, PreparedPlan};

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
    /// Frontend COPY messages. Only reachable once the backend has sent
    /// `CopyInResponse`, which is what puts the connection into `Phase::CopyIn`.
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_FAIL: u8 = b'f';
}

/// Largest message this server will frame. PostgreSQL's own limit is 1 GB; a
/// blackhole has no reason to buffer anything near it, and an unbounded value is
/// how a malformed length becomes an allocation.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// The longest StartupMessage accepted, matching PostgreSQL's own
/// `PQ_MAX_STARTUP_PACKET_LENGTH`. A startup packet carries parameters, not a
/// query, so it needs nothing like `MAX_MESSAGE`.
const MAX_STARTUP: usize = 10000;

/// Where a connection is in its life. The startup exchange is unframed and has
/// to be recognised by shape, not by a tag byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Startup,
    Query,
    /// Between `CopyInResponse` and `CopyDone`/`CopyFail`. The client is
    /// streaming rows and the only messages that mean anything are `d`, `c` and
    /// `f`; a query arriving here is a client that ignored the protocol.
    ///
    /// The row count travels because `CommandComplete` must report it: PostgreSQL
    /// answers `COPY n`, and a client that loads 10000 rows and is told `COPY 0`
    /// has been lied to in a way it can see.
    ///
    /// `binary` travels because the count depends on it. In text format a row
    /// ends at a newline, so newlines are the rows; in binary format there are no
    /// newlines at all -- `pgx.CopyFrom` encodes field lengths and payload bytes,
    /// and `0x0a` appears wherever a value happens to contain it.
    CopyIn { rows: u64, binary: bool },
    Closed,
}

pub struct Conn {
    phase: Phase,
    /// Prepared statements by name. "" is the unnamed statement, which clients
    /// reuse constantly, so it is a normal entry rather than a special case.
    statements: HashMap<String, PreparedPlan>,
    /// Portal name -> statement name.
    portals: HashMap<String, String>,
    /// Bytes of a binary COPY stream that did not yet form a whole tuple.
    ///
    /// A `CopyData` message is a slice of the stream, not a row: pgx fills a
    /// 64 KiB buffer and sends whatever fits, so a tuple can straddle two
    /// messages and a message can end mid-field. Counting therefore has to carry
    /// state, which is what this is.
    copy_carry: Vec<u8>,
    /// Whether the 19-byte binary COPY header has been consumed.
    copy_header_seen: bool,
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
            copy_carry: Vec::new(),
            copy_header_seen: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// True until a startup handshake has completed.
    ///
    /// The phase a client is in while it may legitimately have sent nothing at
    /// all: a TCP connection that exists is not a client yet, and the server
    /// cannot tell a slow driver from an idle socket that will never speak.
    /// Callers use this to bound how long that state may last.
    pub fn awaiting_startup(&self) -> bool {
        self.phase == Phase::Startup
    }

    /// Answer a `COPY ... FROM STDIN` and wait for rows.
    fn enter_copy_in(&mut self, columns: usize, binary: bool, out: &mut Vec<u8>) {
        copy_in_response(out, columns, binary);
        self.copy_carry.clear();
        self.copy_header_seen = false;
        self.phase = Phase::CopyIn { rows: 0, binary };
    }

    /// Back to ordinary statements, with the framing state dropped.
    fn leave_copy_in(&mut self) {
        self.copy_carry.clear();
        self.copy_header_seen = false;
        self.phase = Phase::Query;
    }

    /// Count complete tuples in a binary COPY stream, carrying the tail.
    ///
    /// The framing is PostgreSQL's: an 11-byte signature, two int32s, then per
    /// tuple an int16 field count followed by each field's int32 length (-1 for
    /// NULL) and its bytes, ending with an int16 -1 trailer. A field's length is
    /// what makes this countable at all -- newline bytes inside a payload are
    /// just bytes, which is exactly why counting them reported 2 for a 3-row
    /// load.
    fn count_binary_copy_rows(&mut self, body: &[u8]) {
        /// The signature, its flags and the header-extension length.
        const HEADER: usize = 19;

        /// How much unparsed stream to hold before calling it a violation. A
        /// whole tuple has to fit for any progress to be possible; past this the
        /// client is not sending tuples and the buffer would grow without bound.
        const MAX_CARRY: usize = 1 << 20;

        self.copy_carry.extend_from_slice(body);

        let mut buf = std::mem::take(&mut self.copy_carry);
        let n = buf.len();
        let mut pos = 0usize;
        let mut rows = 0u64;

        if !self.copy_header_seen {
            if n < HEADER {
                self.copy_carry = buf;

                return;
            }

            pos = HEADER;
            self.copy_header_seen = true;
        }

        loop {
            if n - pos < 2 {
                break;
            }

            let fields = i16::from_be_bytes([buf[pos], buf[pos + 1]]);

            // The trailer: an int16 -1 ends the stream. Not a row.
            if fields < 0 {
                pos += 2;

                break;
            }

            let mut p = pos + 2;
            let mut whole = true;

            for _ in 0..fields {
                if n - p < 4 {
                    whole = false;

                    break;
                }

                let len = i32::from_be_bytes([buf[p], buf[p + 1], buf[p + 2], buf[p + 3]]);

                p += 4;

                // -1 is NULL: a length with no bytes after it.
                if len >= 0 {
                    if (n - p) < len as usize {
                        whole = false;

                        break;
                    }

                    p += len as usize;
                }
            }

            if !whole {
                break;
            }

            pos = p;
            rows += 1;
        }

        if let Phase::CopyIn { rows: counted, .. } = &mut self.phase {
            *counted += rows;
        }

        if pos < n {
            buf.drain(..pos);

            if buf.len() > MAX_CARRY {
                // A client that has sent a megabyte without completing a tuple
                // is not sending tuples. Answering an error and closing is the
                // only ending that does not end in the shard's own allocation.
                self.phase = Phase::Closed;
                buf.clear();
            }

            self.copy_carry = buf;
        } else {
            buf.clear();
            self.copy_carry = buf;
        }
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
                // A COPY in flight is framed the same way a query is; which
                // messages MEAN anything is `dispatch`'s business, not the
                // framer's.
                Phase::Query | Phase::CopyIn { .. } => self.message(rest, out, catalog),
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

        // The same guard the framed path carries at `dispatch`, and it was missing
        // here. A negative i32 cast straight to usize becomes enormous, so
        // `input.len() < len` is always true, this returns "incomplete", and the
        // caller keeps every later byte in its pending buffer waiting for a frame
        // that can never arrive -- unbounded memory from one malformed startup
        // packet, before any authentication.
        //
        // `MAX_STARTUP` rather than `MAX_MESSAGE`: a StartupMessage carries a
        // parameter list, not a query, and PostgreSQL itself refuses one over
        // 10000 bytes (`PQ_MAX_STARTUP_PACKET_LENGTH` in backend/libpq/pqcomm.c).
        // Matching that is a tighter bound than 16 MiB and is the documented one.
        let declared = i32::from_be_bytes(input[0..4].try_into().ok()?);
        if !(8..=MAX_STARTUP as i32).contains(&declared) {
            self.phase = Phase::Closed;

            return Some(input.len());
        }

        let len = declared as usize;
        if input.len() < len {
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
        let declared = i32::from_be_bytes(input[1..5].try_into().ok()?);

        // The length is client-controlled. A negative i32 cast straight to usize
        // becomes enormous, `input.len() < 1 + len` is then always true, and the
        // connection stalls forever while the caller appends every later byte to
        // its pending buffer -- an unbounded allocation from one malformed
        // frame. Reject the frame instead.
        if !(4..=MAX_MESSAGE as i32).contains(&declared) {
            self.phase = Phase::Closed;

            return Some(input.len());
        }

        let len = declared as usize;
        if input.len() < 1 + len {
            return None;
        }

        let body = &input[5..1 + len];
        let total = 1 + len;

        // Past this point the message is FRAMED: its bytes are all here, so the
        // only correct outcomes are "handled" and "handled badly". Returning
        // None would mean "incomplete, keep the bytes", which for a complete
        // frame is a permanent stall. `handled` runs the body and its `?` are
        // confined to it.
        let handled = self.dispatch(t, body, out, catalog);
        if handled.is_none() {
            // Malformed body inside a well-framed message: answer an error and
            // consume it, so the stream stays synchronised.
            error_response(out, "08P01", "malformed message body");
        }

        return Some(total);
    }

    /// The body of one framed message. Every `?` here means "this body is
    /// malformed", never "wait for more bytes".
    fn dispatch(
        &mut self,
        t: u8,
        body: &[u8],
        out: &mut Vec<u8>,
        catalog: &CatalogView,
    ) -> Option<()> {

        match t {
            tag::TERMINATE => {
                self.phase = Phase::Closed;
            }

            tag::QUERY => {
                let sql = cstr_read(body).unwrap_or("");
                let plan = PreparedPlan::build(sql);

                // **COPY is decided by its direction, once, in the plan.**
                //
                // `COPY ... FROM STDIN` is a conversation, not an answer: the
                // client sends the statement and then WAITS for `CopyInResponse`
                // before streaming rows. This path used to fall into
                // `PlanKind::FromText` and reply `CommandComplete("SELECT 0")` --
                // so the client believed the statement had finished and then
                // wrote `CopyData` into a connection that had moved on. The
                // stream desynchronises and every later reply is read against the
                // wrong message.
                //
                // `COPY ... TO STDOUT` and `COPY ... FROM '/path'` do not talk
                // back, but they are COPY statements and the tag a client checks
                // is `COPY n`. `SELECT 0` for a COPY is a reply no client can
                // accept: pgx's `CopyTo` reads the tag and reports it, and
                // stroppy's load path counts rows from `CopyFrom`'s.
                //
                // The pgwire handler this codec replaced implemented the
                // FROM STDIN half (`handler.rs`, `Response::CopyIn` /
                // `on_copy_data` / `on_copy_done`), so that was a regression
                // introduced by the rewrite rather than a gap that was always
                // there. Found in review.
                if let PlanKind::Copy {
                    direction,
                    columns,
                    binary,
                } = plan.kind
                {
                    match direction {
                        CopyDirection::FromStdin => {
                            self.enter_copy_in(columns, binary, out);

                            return Some(());
                        }
                        CopyDirection::ToStdout => {
                            // The server would stream rows here. A blackhole has
                            // none, so it says so in the protocol's own words:
                            // `CopyOutResponse`, then `CopyDone` with no DataRow
                            // in between, then `COPY 0`.
                            copy_out_response(out);
                            copy_done(out);
                            complete(out, "COPY", Some(0));
                        }
                        CopyDirection::FromFile => {
                            // No conversation: the server reads a path. It read
                            // nothing, so it copied nothing.
                            complete(out, "COPY", Some(0));
                        }
                    }

                    ready(out);

                    return Some(());
                }

                answer(&plan, out, catalog, true);
                ready(out);
            }

            // In flight: count rows, and end on done or fail.
            tag::COPY_DATA if matches!(self.phase, Phase::CopyIn { .. }) => {
                let binary = matches!(self.phase, Phase::CopyIn { binary: true, .. });

                if binary {
                    self.count_binary_copy_rows(body);

                    if self.is_closed() {
                        return Some(());
                    }
                } else if let Phase::CopyIn { rows, .. } = &mut self.phase {
                    // One CopyData message is one or more rows of text, newline
                    // separated. Counting newlines is what PostgreSQL reports and
                    // is right even when a driver batches many rows per message
                    // and even when a row straddles two: every newline the client
                    // sends is counted exactly once, in whichever message carried
                    // it.
                    *rows += body.iter().filter(|&&b| b == b'\n').count() as u64;
                }
            }

            tag::COPY_DONE if matches!(self.phase, Phase::CopyIn { .. }) => {
                let rows = match &self.phase {
                    Phase::CopyIn { rows, .. } => *rows,
                    _ => 0,
                };
                self.leave_copy_in();
                complete_raw(out, &format!("COPY {rows}"));
                ready(out);
            }

            tag::COPY_FAIL if matches!(self.phase, Phase::CopyIn { .. }) => {
                // The client is abandoning its own COPY. That is an error by the
                // protocol's own definition, and answering it as success would
                // tell a loader its data landed.
                self.leave_copy_in();
                error_response(out, "57014", "COPY from stdin failed");
                ready(out);
            }

            tag::COPY_DATA | tag::COPY_DONE | tag::COPY_FAIL => {
                // A COPY message outside a COPY is the client desynchronised, not
                // something to absorb quietly: absorbing it is what lets the two
                // sides disagree for the rest of the connection.
                error_response(out, "08P01", "COPY message outside a COPY operation");
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
                // Two String allocations per query if done unconditionally, and
                // a driver binds the SAME portal to the SAME statement forever.
                match self.portals.get(portal) {
                    Some(cur) if cur == stmt => {}
                    _ => {
                        self.portals.insert(portal.to_string(), stmt.to_string());
                    }
                }
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

                let described = match plan {
                    Some(p) => row_description(p, catalog, out),
                    None => false,
                };
                if !described {
                    msg(out, b'n', |_| {});
                }
            }

            tag::EXECUTE => {
                let (portal, _) = cstr_at(body, 0)?;
                // No clone: `answer` is a free function precisely so the plan can
                // stay borrowed out of `self`. It used to be a method, which made
                // the borrow checker demand `.cloned()` -- a DEEP copy of the sql
                // String and the parsed table/column Vecs, on every Execute.
                match self
                    .portals
                    .get(portal)
                    .and_then(|s| self.statements.get(s))
                {
                    // Execute does NOT re-send RowDescription; Describe did.
                    Some(p) => {
                        // A COPY that streams from the client needs the
                        // connection to change phase, which is not something a
                        // free function writing into `out` can do. Read what it
                        // is first, answer, then enter the phase.
                        let streams_rows = match &p.kind {
                            PlanKind::Copy {
                                direction: CopyDirection::FromStdin,
                                columns,
                                binary,
                            } => Some((*columns, *binary)),
                            _ => None,
                        };

                        answer(p, out, catalog, false);

                        if let Some((columns, binary)) = streams_rows {
                            let _ = columns;
                            self.phase = Phase::CopyIn { rows: 0, binary };
                            self.copy_carry.clear();
                            self.copy_header_seen = false;
                        }
                    }
                    None => complete(out, "SELECT", Some(0)),
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

        Some(())
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

}

/// The rows and the completion tag for one plan. A FREE function, not a method:
/// it never needed `self`, and being a method is what forced a deep clone of the
/// plan at every Execute.
fn answer(plan: &PreparedPlan, out: &mut Vec<u8>, catalog: &CatalogView, describe: bool) {
    match &plan.kind {
        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. } => {
            let cols = plan_columns(plan, catalog);
            if describe {
                row_description(plan, catalog, out);
            }
            // A metadata select answers empty; a stub select answers one row.
            let rows = if matches!(plan.kind, PlanKind::SelectMeta { .. }) && cols.resolved {
                0
            } else {
                data_row(out, cols.count);
                1
            };
            complete(out, "SELECT", Some(rows));
        }
        PlanKind::Insert => complete_raw(out, "INSERT 0 1"),
        PlanKind::Update => complete_raw(out, "UPDATE 1"),
        PlanKind::Delete => complete_raw(out, "DELETE 1"),
        PlanKind::Begin => complete_raw(out, "BEGIN"),
        PlanKind::Commit => complete_raw(out, "COMMIT"),
        PlanKind::Rollback => complete_raw(out, "ROLLBACK"),
        PlanKind::Ddl => {
            catalog.apply(&plan.sql);
            complete_raw(out, ddl_tag(&plan.sql));
        }
        // Reached by the extended protocol's Execute. The caller sets
        // `Phase::CopyIn` for the FROM STDIN half, because the phase belongs to
        // the connection and not to the reply.
        PlanKind::Copy {
            direction,
            columns,
            binary,
        } => match direction {
            CopyDirection::FromStdin => copy_in_response(out, *columns, *binary),
            CopyDirection::ToStdout => {
                copy_out_response(out);
                copy_done(out);
                complete(out, "COPY", Some(0));
            }
            CopyDirection::FromFile => complete(out, "COPY", Some(0)),
        },
        PlanKind::FromText => complete(out, "SELECT", Some(0)),
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

/// CommandComplete with a trailing count, written WITHOUT formatting into a
/// temporary String. `format!("SELECT {n}")` was one heap allocation per query.
fn complete(out: &mut Vec<u8>, verb: &str, n: Option<u64>) {
    msg(out, b'C', |b| {
        b.extend_from_slice(verb.as_bytes());
        if let Some(n) = n {
            b.push(b' ');
            push_u64(b, n);
        }
        b.push(0);
    });
}

fn complete_raw(out: &mut Vec<u8>, tag: &str) {
    msg(out, b'C', |b| cstr(b, tag));
}

fn push_u64(out: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        out.push(b'0');
        return;
    }
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out.extend_from_slice(&buf[i..]);
}

fn cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

/// `COPY ... FROM STDIN` puts the connection into a conversation: the client
/// sends `CopyData` and ends it with `CopyDone` or `CopyFail`.
///
/// `CopyInResponse`: the overall format, then one format code per column.
fn copy_in_response(out: &mut Vec<u8>, columns: usize, binary: bool) {
    // PostgreSQL's format codes: 0 text, 1 binary.
    let code: i16 = i16::from(binary);

    msg(out, b'G', |b| {
        b.push(if binary { 1 } else { 0 }); // overall format
        b.extend_from_slice(&(columns as i16).to_be_bytes());
        for _ in 0..columns {
            b.extend_from_slice(&code.to_be_bytes());
        }
    });
}

/// `CopyOutResponse` for `COPY ... TO STDOUT`.
///
/// Text format, zero columns: an empty stream has none, and a driver that reads
/// rows until `CopyDone` does not need a header per column to read none of them.
fn copy_out_response(out: &mut Vec<u8>) {
    msg(out, b'H', |b| {
        b.push(0); // overall format: text
        b.extend_from_slice(&0i16.to_be_bytes()); // no columns
    });
}

/// `CopyDone` as the SERVER sends it: the stream it is writing is over.
fn copy_done(out: &mut Vec<u8>) {
    msg(out, b'c', |_| {});
}

/// A minimal ErrorResponse: severity, SQLSTATE, message, terminator.
fn error_response(out: &mut Vec<u8>, code: &str, message: &str) {
    msg(out, b'E', |b| {
        b.push(b'S');
        cstr(b, "ERROR");
        b.push(b'C');
        cstr(b, code);
        b.push(b'M');
        cstr(b, message);
        b.push(0);
    });
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
fn row_description(plan: &PreparedPlan, catalog: &CatalogView, out: &mut Vec<u8>) -> bool {
    // Resolved names only when the catalog knows the table; otherwise n stub
    // columns, named from a &'static str rather than n freshly allocated
    // Strings.
    let resolved = match &plan.kind {
        PlanKind::SelectMeta { table, columns } => catalog
            .columns_for(table, columns)
            .or_else(|| Some(stub_cols(columns.len().max(1)))),
        PlanKind::SelectStub { columns } => Some(stub_cols(*columns)),
        _ => return false,
    };

    let Some(cols) = resolved else { return false };

    msg(out, b'T', |b| {
        b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for (name, oid) in cols.iter() {
            cstr(b, name);
            b.extend_from_slice(&0i32.to_be_bytes()); // table oid
            b.extend_from_slice(&0i16.to_be_bytes()); // column no
            b.extend_from_slice(&oid.to_be_bytes()); // type oid
            b.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            b.extend_from_slice(&0i16.to_be_bytes()); // text format
        }
    });

    true
}

const STUB_COL: &str = "?column?";

fn stub_cols(n: usize) -> Vec<(String, u32)> {
    (0..n).map(|_| (STUB_COL.to_string(), 20u32)).collect()
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

    /// A client-controlled negative length cast to usize becomes enormous, so
    /// `input.len() < 1 + len` is permanently true: the codec consumes nothing,
    /// the caller appends every later byte to its pending buffer, and one frame
    /// becomes an unbounded allocation. Found in review, not by these tests --
    /// which covered a message SPLIT across reads but never a malformed one.
    #[test]
    fn a_negative_length_is_rejected_rather_than_stalling() {
        for bad in [-1i32, i32::MIN, 3, (MAX_MESSAGE as i32) + 1] {
            let (mut c, v) = connected();
            let mut input = vec![b'P'];
            input.extend_from_slice(&bad.to_be_bytes());
            input.extend_from_slice(b"junk");

            let mut out = Vec::new();
            let used = c.advance(&input, &mut out, &v);

            assert!(used > 0, "len {bad} consumed nothing: the stall");
            assert!(c.is_closed(), "len {bad} left the connection open");
        }
    }

    /// **COPY FROM STDIN is a conversation**, and answering it with
    /// `CommandComplete` desynchronises the stream: the client believes the
    /// statement finished and then writes rows into a connection that moved on.
    ///
    /// The pgwire handler this codec replaced implemented it; the rewrite dropped
    /// it and answered `SELECT 0`. A reviewer found that, not these tests.
    #[test]
    fn copy_from_stdin_gets_its_response_and_row_count() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY warehouse (w_id, w_name) FROM STDIN")),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'G', "a COPY must be answered with CopyInResponse");
        // Text format overall, then one text format code per column.
        assert_eq!(out[5], 0, "overall format must be text");
        assert_eq!(i16::from_be_bytes([out[6], out[7]]), 2, "two columns");
        assert!(
            !out.windows(1).any(|w| w == b"Z"),
            "ReadyForQuery must NOT follow: the client speaks next"
        );

        // Three rows across two messages: the count must follow the ROWS, not the
        // client's batching.
        out.clear();
        c.advance(&tagged(b'd', |b| b.extend_from_slice(b"1\tone\n2\ttwo\n")), &mut out, &v);
        c.advance(&tagged(b'd', |b| b.extend_from_slice(b"3\tthree\n")), &mut out, &v);
        assert!(out.is_empty(), "CopyData is absorbed silently, as the protocol says");

        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 3"), "row count must be reported: {text}");
        assert!(out.ends_with(&[b'I']), "and ReadyForQuery(idle) must close it");
    }

    /// A client that abandons its own COPY is told it failed. Answering success
    /// would tell a loader its data landed.
    #[test]
    fn copy_fail_is_an_error_not_a_success() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN")), &mut out, &v);

        out.clear();
        c.advance(&tagged(b'f', |b| cstr(b, "client gave up")), &mut out, &v);
        assert_eq!(out[0], b'E', "CopyFail must produce ErrorResponse");
        assert!(!c.is_closed(), "and the connection survives it");
    }

    /// COPY TO STDOUT and COPY FROM a file are not conversations, so they must
    /// not enter the copy phase -- but they are COPY statements, and the tag a
    /// client checks is `COPY n`.
    ///
    /// This test used to assert only "not CopyInResponse" and "answered and
    /// ready", which a `SELECT 0` satisfied: it accepted the wrong answer for
    /// `COPY t TO STDOUT` because it never looked at what came back. pgx's
    /// `CopyTo` reads the tag, and stroppy's load path reads the row count.
    #[test]
    fn only_copy_from_stdin_enters_the_copy_phase() {
        for sql in ["COPY t TO STDOUT", "COPY t FROM '/tmp/x.csv'", "SELECT 1"] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);
            assert_ne!(out[0], b'G', "{sql} must not get CopyInResponse");
            assert!(out.ends_with(&[b'I']), "{sql} must be answered and ready");
        }
    }

    /// `COPY t TO STDOUT` is a `CopyOutResponse`, an empty stream and `COPY 0`.
    /// It used to fall through to `CommandComplete("SELECT 0")`.
    #[test]
    fn copy_to_stdout_answers_copy_out_and_a_copy_tag() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&tagged(b'Q', |b| cstr(b, "COPY warehouse TO STDOUT")), &mut out, &v);

        assert_eq!(out[0], b'H', "CopyOutResponse");
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 0"), "the tag must be COPY 0: {text}");
        assert!(!text.contains("SELECT"), "not a SELECT: {text}");
        // CopyOutResponse is `H` + length + (format, columns) = 8 bytes, then the
        // server's own CopyDone, then the tag, then ReadyForQuery.
        assert_eq!(out[8], b'c', "CopyDone before the tag");
        assert!(out.ends_with(&[b'I']), "and ready");
    }

    /// A file the server reads is the same non-conversation, and the same tag.
    #[test]
    fn copy_from_a_file_answers_a_copy_tag() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY warehouse FROM '/tmp/x.csv'")),
            &mut out,
            &v,
        );

        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 0"), "the tag must be COPY 0: {text}");
    }

    /// **The reviewer's case.** SQL whitespace is not one space: three spaces, a
    /// tab and a newline are all the same statement, and all three have to be
    /// recognised as the conversation FROM STDIN is.
    ///
    /// `COPY t FROM    STDIN` used to answer `SELECT 0` (the two-spelling match
    /// missed it by one space), after which the client's `CopyData` arrived
    /// outside a COPY and every later reply was read against the wrong message.
    #[test]
    fn copy_from_stdin_survives_any_whitespace() {
        for sql in [
            "COPY t FROM STDIN",
            "COPY t FROM  STDIN",
            "COPY t FROM    STDIN",
            "COPY t FROM\tSTDIN",
            "COPY t FROM\nSTDIN",
            "COPY t (a, b) FROM    STDIN",
            "  COPY t FROM\t STDIN  ",
        ] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);

            assert_eq!(out[0], b'G', "{sql:?} must get CopyInResponse");

            // And the conversation completes rather than desynchronising.
            out.clear();
            c.advance(&tagged(b'd', |b| b.extend_from_slice(b"1\n")), &mut out, &v);
            c.advance(&tagged(b'c', |_| {}), &mut out, &v);
            let text = String::from_utf8_lossy(&out);
            assert!(text.contains("COPY 1"), "{sql:?} must count its row: {text}");
        }
    }

    /// The column count announced to the client has to match the client's own
    /// column list, whatever whitespace separates them.
    #[test]
    fn copy_in_announces_the_column_count_under_any_whitespace() {
        for (sql, want) in [
            ("COPY t (a, b) FROM STDIN", 2),
            ("COPY t (a,b,c) FROM\tSTDIN", 3),
            ("\n\tCOPY t ( a , b ) FROM    STDIN", 2),
            ("COPY t FROM STDIN", 1),
        ] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);

            assert_eq!(out[0], b'G', "{sql:?}");
            assert_eq!(
                i16::from_be_bytes([out[6], out[7]]),
                want,
                "{sql:?} announced the wrong column count"
            );
        }
    }

    /// **Binary COPY counts tuples, not newlines.**
    ///
    /// pgx's `CopyFrom` sends PostgreSQL's binary framing, where `0x0a` is just
    /// a byte in a payload. Counting newline bytes reported `COPY 2` for three
    /// rows -- which stroppy then reported as `confirmed_rows=2` of 3 -- and the
    /// count is the only thing a loader can check.
    #[test]
    fn binary_copy_counts_tuples() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        // A schema, so the columns are real and the format is the client's.
        c.advance(
            &tagged(b'Q', |b| cstr(b, "CREATE TABLE t (a int8, b text)")),
            &mut out,
            &v,
        );
        out.clear();

        // pgx spells it exactly like this.
        c.advance(
            &tagged(b'Q', |b| cstr(b, "copy t ( a, b ) from stdin binary;")),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'G', "CopyInResponse");
        assert_eq!(out[5], 1, "binary overall format");
        assert_eq!(i16::from_be_bytes([out[6], out[7]]), 2, "two columns");
        assert_eq!(i16::from_be_bytes([out[8], out[9]]), 1, "binary, per column");
        out.clear();

        // Header, then three tuples. The second field of the first row contains
        // a newline byte, and the third row is split across two messages.
        let mut stream = Vec::new();
        stream.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        stream.extend_from_slice(&0i32.to_be_bytes()); // flags
        stream.extend_from_slice(&0i32.to_be_bytes()); // header extension

        for (n, text) in [(1i64, "one\nwith newline"), (2, "two"), (3, "three")] {
            stream.extend_from_slice(&2i16.to_be_bytes()); // two fields
            stream.extend_from_slice(&8i32.to_be_bytes());
            stream.extend_from_slice(&n.to_be_bytes());
            stream.extend_from_slice(&(text.len() as i32).to_be_bytes());
            stream.extend_from_slice(text.as_bytes());
        }

        stream.extend_from_slice(&(-1i16).to_be_bytes()); // trailer

        // Split it mid-tuple on purpose: a CopyData message is a slice of the
        // stream, not a row.
        let split = stream.len() - 12;

        c.advance(&tagged(b'd', |b| b.extend_from_slice(&stream[..split])), &mut out, &v);
        assert!(out.is_empty(), "CopyData is absorbed silently");
        c.advance(&tagged(b'd', |b| b.extend_from_slice(&stream[split..])), &mut out, &v);

        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 3"), "three tuples: {text}");
    }

    /// A NULL field is a length of -1 with no bytes after it, and it must not
    /// throw the frame walk off by four.
    #[test]
    fn binary_copy_counts_nulls_and_knows_when_the_stream_is_over() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")), &mut out, &v);
        out.clear();

        let mut stream = Vec::new();
        stream.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        stream.extend_from_slice(&0i32.to_be_bytes());
        stream.extend_from_slice(&0i32.to_be_bytes());

        // One row of two NULLs, then the trailer -- which is not a row.
        stream.extend_from_slice(&2i16.to_be_bytes());
        stream.extend_from_slice(&(-1i32).to_be_bytes());
        stream.extend_from_slice(&(-1i32).to_be_bytes());
        stream.extend_from_slice(&(-1i16).to_be_bytes());

        c.advance(&tagged(b'd', |b| b.extend_from_slice(&stream)), &mut out, &v);
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);

        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 1"), "one row, trailer not counted: {text}");
    }

    /// A client that never completes a tuple must not grow the carry buffer
    /// without bound. A megabyte of unterminated stream is a violation, and the
    /// connection is closed rather than held.
    #[test]
    fn binary_copy_closes_a_client_that_never_completes_a_tuple() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")), &mut out, &v);

        // A field header promising far more bytes than will ever arrive, sent
        // until the carry exceeds its ceiling.
        let mut stuck = Vec::new();
        stuck.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        stuck.extend_from_slice(&0i32.to_be_bytes());
        stuck.extend_from_slice(&0i32.to_be_bytes());
        stuck.extend_from_slice(&1i16.to_be_bytes());
        stuck.extend_from_slice(&(1i32 << 30).to_be_bytes());

        // 1 MiB / 25 bytes per message, and a little past it.
        for _ in 0..50_000 {
            c.advance(&tagged(b'd', |b| b.extend_from_slice(&stuck)), &mut out, &v);

            if c.is_closed() {
                break;
            }
        }

        assert!(c.is_closed(), "an unterminated tuple must not be held forever");
    }

    /// The same stall, one packet earlier: the STARTUP length is client-controlled
    /// too, and it is read before any authentication. The framed path was guarded
    /// in the first review round and this one was not, which is what a reviewer
    /// found and these tests did not -- they covered the framed path only.
    #[test]
    fn a_negative_startup_length_is_rejected_rather_than_stalling() {
        for bad in [-1i32, i32::MIN, 7, (MAX_STARTUP as i32) + 1] {
            let mut c = Conn::new();
            let v = view();
            let mut input = bad.to_be_bytes().to_vec();
            input.extend_from_slice(&196608i32.to_be_bytes()); // protocol 3.0
            input.extend_from_slice(b"user\0bench\0\0");

            let mut out = Vec::new();
            let used = c.advance(&input, &mut out, &v);

            assert!(used > 0, "startup len {bad} consumed nothing: the stall");
            assert!(c.is_closed(), "startup len {bad} left the connection open");
        }
    }

    /// And the honest packet still works, so the bound is not simply refusing
    /// everything -- the failure a too-tight guard would produce.
    #[test]
    fn an_ordinary_startup_still_completes() {
        let mut c = Conn::new();
        let v = view();
        let mut body = 196608i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0bench\0database\0bench\0\0");
        let mut input = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        input.extend_from_slice(&body);

        let mut out = Vec::new();
        let used = c.advance(&input, &mut out, &v);

        assert_eq!(used, input.len(), "a valid startup must be consumed whole");
        assert!(!c.is_closed(), "a valid startup must not close the connection");
        assert!(!out.is_empty(), "and must be answered");
    }

    /// Once a message is FRAMED its bytes are all present, so the only outcomes
    /// are handled and handled-badly. Returning None would mean "incomplete,
    /// keep the bytes", which for a complete frame is a permanent stall.
    #[test]
    fn a_malformed_body_is_answered_and_consumed() {
        let (mut c, v) = connected();

        // Parse with no NUL terminator anywhere in the body.
        let mut input = Vec::new();
        msg(&mut input, b'P', |b| b.extend_from_slice(b"no-nul-here"));
        let framed = input.len();
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        let used = c.advance(&input, &mut out, &v);

        assert_eq!(used, input.len(), "a framed message must always be consumed");
        let t = tags(&out);
        assert_eq!(t.first(), Some(&b'E'), "malformed body gets an error: {t:?}");
        assert_eq!(
            t.last(),
            Some(&b'Z'),
            "and the Sync after it is still answered, so the stream is in sync"
        );
        assert!(framed < input.len());
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
