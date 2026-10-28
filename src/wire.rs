//! A sans-io PostgreSQL wire codec.
//! `Conn::advance` consumes bytes and appends responses without touching a socket,
//! so each read batch produces at most one write.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::CatalogView;
use crate::plan::{CopyDirection, CopyFormat, PlanKind, PreparedPlan};

/// Frontend messages the blackhole understands.
/// Anything else is consumed and answered as though it succeeded, which is the whole
/// contract.
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
    /// Frontend COPY messages.
    /// Only reachable once the backend has sent `CopyInResponse`, which is what puts
    /// the connection into `Phase::CopyIn`.
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_FAIL: u8 = b'f';

    /// Whether a message belongs to the extended protocol, which is what decides how an
    /// error in it ends.
    pub fn is_extended(t: u8) -> bool {
        matches!(t, PARSE | BIND | DESCRIBE | EXECUTE | CLOSE | FLUSH)
    }
}

/// Largest message this server will frame.
/// PostgreSQL's own limit is 1 GB; a blackhole has no reason to buffer anything near
/// it, and an unbounded value is how a malformed length becomes an allocation.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// The longest StartupMessage accepted, matching PostgreSQL's own
/// `PQ_MAX_STARTUP_PACKET_LENGTH`.
const MAX_STARTUP: usize = 10000;

/// PostgreSQL's own wording for a statement inside a failed transaction.
const ABORTED: &str =
    "current transaction is aborted, commands ignored until end of transaction block";

/// The statements PostgreSQL still accepts in a failed transaction: the ones that end
/// it, and the empty one, which has nothing in it to refuse.
fn ends_transaction(kind: &PlanKind) -> bool {
    matches!(
        kind,
        PlanKind::Commit | PlanKind::Rollback | PlanKind::Empty
    )
}

/// Where a connection is in its life.
/// The startup exchange is unframed and has to be recognised by shape, not by a tag
/// byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Startup,
    Query,
    /// Between `CopyInResponse` and `CopyDone`/`CopyFail`.
    /// The client is streaming rows and the only messages that mean anything are `d`,
    /// `c` and `f`; a query arriving here is a client that ignored the protocol.
    CopyIn {
        rows: u64,
        format: CopyFormat,
        extended: bool,
    },
    /// After an error in an extended-protocol message.
    AwaitingSync,
    Closed,
}

/// Where a binary COPY stream is, carried across CopyData messages: a position in the
/// framing plus, where a length was declared, how much of it is still to arrive.
/// Never the bytes themselves.
#[derive(Debug, Clone, Copy)]
struct BinaryState {
    stage: BinaryStage,
    /// A header, field count or field length that straddles two messages.
    prefix: [u8; 19],
    prefix_len: usize,
}

impl Default for BinaryState {
    fn default() -> Self {
        BinaryState {
            stage: BinaryStage::Header,
            prefix: [0; 19],
            prefix_len: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryStage {
    /// Gathering the 19-byte signature, flags and extension length.
    Header,
    /// Discarding the header extension.
    Extension { remaining: u32 },
    /// At a tuple boundary: the next int16 is a field count or the trailer.
    FieldCount,
    /// Expecting a field's int32 length.
    FieldLength { fields_left: u16 },
    /// Discarding a field's payload.
    Payload { fields_left: u16, remaining: u32 },
    /// The trailer was seen; nothing more may arrive.
    Done,
}

impl BinaryStage {
    /// The stage after one field of a tuple with `fields_left` to go, counting the
    /// tuple when it was the last.
    fn after_field(fields_left: u16, rows: &mut u64) -> BinaryStage {
        if fields_left <= 1 {
            *rows += 1;
            BinaryStage::FieldCount
        } else {
            BinaryStage::FieldLength {
                fields_left: fields_left - 1,
            }
        }
    }
}

/// Where a text or CSV COPY stream is within its current line, carried across CopyData
/// messages.
#[derive(Debug, Default, Clone, Copy)]
struct LineState {
    /// Bytes have arrived since the last row end: the last row, if the data ends here,
    /// is this unterminated line.
    line_open: bool,
    /// CSV: inside a quoted field, where a newline is data.
    in_quotes: bool,
    /// Text: the previous byte was an unescaped backslash, so this one is data whatever
    /// it is.
    escaped: bool,
    /// A `\.` line was seen: the data is over and what follows is ignored.
    ended: bool,
    /// The current line's length so far, and its first three bytes -- enough to
    /// recognise `\.`, with or without a carriage return before the newline.
    line_len: usize,
    head: [u8; 3],
}

impl LineState {
    fn note(&mut self, b: u8) {
        self.line_open = true;
        if self.line_len < self.head.len() {
            self.head[self.line_len] = b;
        }
        self.line_len = self.line_len.saturating_add(1);
    }

    /// Whether the line ending now is the end-of-data marker.
    fn is_end_marker(&self) -> bool {
        (self.line_len == 2 && &self.head[..2] == b"\\.")
            || (self.line_len == 3 && &self.head == b"\\.\r")
    }
}

/// The transaction state `ReadyForQuery` advertises, one byte on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxStatus {
    /// `I`: no transaction open.
    Idle,
    /// `T`: a transaction block is open.
    InTransaction,
    /// `E`: a transaction block is open and a statement in it failed.
    /// PostgreSQL stays here until `ROLLBACK`, refusing everything else.
    Failed,
}

impl TxStatus {
    fn byte(self) -> u8 {
        match self {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        }
    }
}

/// A prepared statement: its plan and the parameter types Describe reports.
struct Statement {
    plan: PreparedPlan,
    /// One OID per `$n`, declared by Parse or completed from the text.
    params: Vec<u32>,
}

/// The result formats a Bind asked for: none (text), one for every column, or one per
/// column.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Formats {
    All(i16),
    PerColumn(Vec<i16>),
}

impl Formats {
    const TEXT: Formats = Formats::All(0);

    fn from_codes(codes: &[i16]) -> Formats {
        match codes {
            [] => Formats::TEXT,
            [one] => Formats::All(*one),
            [first, rest @ ..] if rest.iter().all(|c| c == first) => Formats::All(*first),
            _ => Formats::PerColumn(codes.to_vec()),
        }
    }

    /// The format of column `i`.
    /// Past the end of a per-column list is text, which is where PostgreSQL's
    /// `pq_getmsgint` would have read zero.
    fn get(&self, i: usize) -> i16 {
        match self {
            Formats::All(code) => *code,
            Formats::PerColumn(codes) => codes.get(i).copied().unwrap_or(0),
        }
    }
}

/// A portal: the statement it was bound to, the result formats the Bind asked for, and
/// how far it has been read.
/// The statement itself, not its name.
struct Portal {
    statement: Arc<Statement>,
    formats: Formats,
    /// Whether the portal has no rows left to send: a suspended single-row answer
    /// has none, its row having gone out with the `PortalSuspended`.
    consumed: bool,
}

pub struct Conn {
    phase: Phase,
    /// Prepared statements by name.
    /// "" is the unnamed statement, which clients reuse constantly, so it is a normal
    /// entry rather than a special case.
    statements: HashMap<String, Arc<Statement>>,
    /// Portals by name.
    /// "" is the unnamed portal, rebound by every query.
    portals: HashMap<String, Portal>,
    /// Where a binary COPY stream is, between CopyData messages.
    copy_binary: BinaryState,
    /// Where a text or CSV COPY stream is within its current line.
    copy_lines: LineState,
    /// What this connection has done in its open transaction, counted when it commits
    /// and dropped if it rolls back.
    pending: crate::tpcc::Pending,
    /// The state `ReadyForQuery` reports, which is not a constant.
    tx_status: TxStatus,
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
            copy_binary: BinaryState::default(),
            copy_lines: LineState::default(),
            pending: crate::tpcc::Pending::default(),
            tx_status: TxStatus::Idle,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// True until a startup handshake has completed.
    pub fn awaiting_startup(&self) -> bool {
        self.phase == Phase::Startup
    }

    /// Answer a `COPY ...
    /// FROM STDIN` and wait for rows.
    fn enter_copy_in(
        &mut self,
        columns: usize,
        format: CopyFormat,
        extended: bool,
        out: &mut Vec<u8>,
    ) {
        copy_in_response(out, columns, format == CopyFormat::Binary);
        self.copy_binary = BinaryState::default();
        self.copy_lines = LineState::default();
        self.phase = Phase::CopyIn {
            rows: 0,
            format,
            extended,
        };
    }

    /// Back to ordinary statements, with the framing state dropped.
    fn leave_copy_in(&mut self) {
        self.copy_binary = BinaryState::default();
        self.copy_lines = LineState::default();
        self.phase = Phase::Query;
    }

    /// `ReadyForQuery`, with the transaction state that is true at this moment.
    fn ready(&self, out: &mut Vec<u8>) {
        msg(out, b'Z', |b| b.push(self.tx_status.byte()));
    }

    /// An ErrorResponse, and the state it leaves behind.
    fn error(&mut self, out: &mut Vec<u8>, code: &str, message: &str) {
        if self.tx_status == TxStatus::InTransaction {
            self.tx_status = TxStatus::Failed;
        }

        error_response(out, code, message);
    }

    /// An error, and the recovery the protocol prescribes for where it happened.
    /// In the simple protocol the reply ends with `ReadyForQuery` and the next message
    /// runs.
    /// Answer the error a message earned, and say the message was answered.
    ///
    /// `Some(())` and not `None`: a refusal is a handled message, and `None` means a
    /// body that could not be read at all.
    fn refuse(
        &mut self,
        extended: bool,
        out: &mut Vec<u8>,
        code: &str,
        message: &str,
    ) -> Option<()> {
        self.error(out, code, message);

        if extended {
            self.phase = Phase::AwaitingSync;
        } else {
            self.ready(out);
        }

        Some(())
    }

    /// What a statement did to the transaction state.
    /// `BEGIN` opens one, and inside an open one it is a warning that changes nothing;
    /// `COMMIT` and `ROLLBACK` close it, failed or not.
    fn note_transaction(&mut self, kind: &PlanKind, failed: bool) {
        match kind {
            PlanKind::Begin if self.tx_status == TxStatus::Idle => {
                self.tx_status = TxStatus::InTransaction;
                self.pending.begin();
            }
            PlanKind::Commit if failed => {
                // Committing a failed transaction rolls it back, and the tag says so:
                // its statements never happened, and the rollback is counted.
                self.tx_status = TxStatus::Idle;
                self.pending.rollback();
            }
            PlanKind::Commit => {
                self.tx_status = TxStatus::Idle;
                self.pending.commit();
            }
            PlanKind::Rollback => {
                self.tx_status = TxStatus::Idle;
                self.pending.rollback();
            }
            _ => {}
        }
    }

    /// Answer the error a client injected, and say whether it did.
    ///
    /// The failure has to be refused on BOTH paths: through Execute it used to be
    /// answered OK, so the client's rollback became a commit.
    fn injected_failure(&mut self, failure: crate::tpcc::Failure, out: &mut Vec<u8>) {
        let (code, message) = match failure {
            crate::tpcc::Failure::Signal(code) => (code, "injected failure".to_string()),
            crate::tpcc::Failure::Null => (
                "23502".to_string(),
                "null value in a column that is not null".to_string(),
            ),
            crate::tpcc::Failure::Raise => ("P0001".to_string(), "raise_exception".to_string()),
            crate::tpcc::Failure::UnknownProcedure => {
                ("42883".to_string(), "function does not exist".to_string())
            }
        };
        self.error(out, &code, &message);
    }

    /// Note what a statement does to the counters.
    fn note_statement(&mut self, sql: &str) {
        let Some(kind) = crate::tpcc::statement_kind(sql) else {
            return;
        };
        // `Pending` owns the rule, including autocommit: a statement outside a
        // transaction commits as it is answered.
        self.pending.statement(kind);
    }

    /// Whether the session is in a failed transaction, where everything but COMMIT and
    /// ROLLBACK is refused.
    fn failed(&self) -> bool {
        self.tx_status == TxStatus::Failed
    }

    /// Count rows in a text or CSV COPY stream, carrying the line state.
    /// A row is a line.
    fn count_lines(&mut self, body: &[u8], csv: bool) {
        let ls = &mut self.copy_lines;
        let mut rows = 0u64;

        for &b in body {
            if ls.ended {
                break;
            }

            if csv && ls.in_quotes {
                if b == b'"' {
                    ls.in_quotes = false;
                }
                ls.note(b);

                continue;
            }

            if !csv && ls.escaped {
                ls.escaped = false;
                ls.note(b);

                continue;
            }

            match b {
                b'\n' => {
                    if ls.is_end_marker() {
                        ls.ended = true;

                        break;
                    }
                    rows += 1;
                    ls.line_open = false;
                    ls.line_len = 0;
                }
                b'"' if csv => {
                    ls.in_quotes = true;
                    ls.note(b);
                }
                b'\\' if !csv => {
                    ls.escaped = true;
                    ls.note(b);
                }
                _ => ls.note(b),
            }
        }

        if let Phase::CopyIn { rows: counted, .. } = &mut self.phase {
            *counted += rows;
        }
    }

    /// Count tuples in a binary COPY stream, without buffering it.
    fn count_binary(&mut self, body: &[u8]) -> Result<(), &'static str> {
        const SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

        let st = &mut self.copy_binary;
        let mut rows = 0u64;
        let mut i = 0usize;

        while i < body.len() {
            match st.stage {
                BinaryStage::Header => {
                    let take = (19 - st.prefix_len).min(body.len() - i);
                    st.prefix[st.prefix_len..st.prefix_len + take]
                        .copy_from_slice(&body[i..i + take]);
                    st.prefix_len += take;
                    i += take;

                    if st.prefix_len < 19 {
                        break;
                    }

                    let h = st.prefix;
                    if &h[..11] != SIGNATURE {
                        return Err("COPY file signature not recognized");
                    }
                    let flags = i32::from_be_bytes([h[11], h[12], h[13], h[14]]);
                    if flags & !0xffff != 0 {
                        return Err("unrecognized critical flags in COPY file header");
                    }
                    let extension = i32::from_be_bytes([h[15], h[16], h[17], h[18]]);
                    if extension < 0 {
                        return Err("invalid COPY file header (wrong length)");
                    }

                    st.prefix_len = 0;
                    st.stage = if extension == 0 {
                        BinaryStage::FieldCount
                    } else {
                        BinaryStage::Extension {
                            remaining: extension as u32,
                        }
                    };
                }

                BinaryStage::Extension { remaining } => {
                    let take = (remaining as usize).min(body.len() - i);
                    i += take;
                    let left = remaining - take as u32;
                    st.stage = if left == 0 {
                        BinaryStage::FieldCount
                    } else {
                        BinaryStage::Extension { remaining: left }
                    };
                }

                BinaryStage::FieldCount => {
                    let take = (2 - st.prefix_len).min(body.len() - i);
                    st.prefix[st.prefix_len..st.prefix_len + take]
                        .copy_from_slice(&body[i..i + take]);
                    st.prefix_len += take;
                    i += take;

                    if st.prefix_len < 2 {
                        break;
                    }

                    let fields = i16::from_be_bytes([st.prefix[0], st.prefix[1]]);
                    st.prefix_len = 0;

                    // The trailer: an int16 -1 ends the stream.
                    // Not a row.
                    // Any other negative count is not a trailer, it is malformed.
                    if fields == -1 {
                        st.stage = BinaryStage::Done;
                        continue;
                    }
                    if fields < -1 {
                        return Err("invalid field count in COPY data");
                    }
                    if fields == 0 {
                        rows += 1;
                        continue;
                    }
                    st.stage = BinaryStage::FieldLength {
                        fields_left: fields as u16,
                    };
                }

                BinaryStage::FieldLength { fields_left } => {
                    let take = (4 - st.prefix_len).min(body.len() - i);
                    st.prefix[st.prefix_len..st.prefix_len + take]
                        .copy_from_slice(&body[i..i + take]);
                    st.prefix_len += take;
                    i += take;

                    if st.prefix_len < 4 {
                        break;
                    }

                    let p = st.prefix;
                    let len = i32::from_be_bytes([p[0], p[1], p[2], p[3]]);
                    st.prefix_len = 0;

                    // -1 is NULL: a length with no bytes after it.
                    // Below that is not a length at all.
                    if len < -1 {
                        return Err("invalid field size in COPY data");
                    }
                    if len <= 0 {
                        st.stage = BinaryStage::after_field(fields_left, &mut rows);
                    } else {
                        st.stage = BinaryStage::Payload {
                            fields_left,
                            remaining: len as u32,
                        };
                    }
                }

                BinaryStage::Payload {
                    fields_left,
                    remaining,
                } => {
                    let take = (remaining as usize).min(body.len() - i);
                    i += take;
                    let left = remaining - take as u32;
                    st.stage = if left == 0 {
                        BinaryStage::after_field(fields_left, &mut rows)
                    } else {
                        BinaryStage::Payload {
                            fields_left,
                            remaining: left,
                        }
                    };
                }

                // The trailer ended the stream; a writer that keeps going is not
                // writing this format.
                BinaryStage::Done => return Err("received copy data after EOF marker"),
            }
        }

        if let Phase::CopyIn { rows: counted, .. } = &mut self.phase {
            *counted += rows;
        }

        Ok(())
    }

    /// Consume every COMPLETE message in `input`, appending replies to `out`.
    /// Returns how many bytes were consumed.
    pub(crate) fn advance(
        &mut self,
        input: &[u8],
        out: &mut Vec<u8>,
        catalog: &CatalogView,
    ) -> usize {
        let mut pos = 0usize;

        loop {
            if self.phase == Phase::Closed {
                return pos;
            }

            let rest = &input[pos..];

            let consumed = match self.phase {
                Phase::Startup => self.startup(rest, out),
                // A COPY in flight is framed the same way a query is; which messages
                // MEAN anything is `dispatch`'s business, not the framer's.
                Phase::Query | Phase::CopyIn { .. } | Phase::AwaitingSync => {
                    self.message(rest, out, catalog)
                }
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
        // here.
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
            // A StartupMessage: its code is the protocol version, major in the high
            // half.
            code if code >> 16 != 3 => {
                let (major, minor) = (code >> 16, code & 0xffff);
                fatal_response(
                    out,
                    "0A000",
                    &format!(
                        "unsupported frontend protocol {major}.{minor}: server supports 3.0 to 3.2"
                    ),
                );
                self.phase = Phase::Closed;
                Some(len)
            }
            _ => {
                // AuthenticationOk
                msg(out, b'R', |b| b.extend_from_slice(&0i32.to_be_bytes()));
                // The few ParameterStatus values a driver reads before it will talk.
                // pgx wants server_version and client_encoding at minimum.
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
                self.ready(out);
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

        // The length is client-controlled.
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

        // Past this point the message is FRAMED: its bytes are all here, so the only
        // correct outcomes are "handled" and "handled badly".
        let handled = self.dispatch(t, body, out, catalog);
        if handled.is_none() {
            // Malformed body inside a well-framed message: answer an error and consume
            // it, so the stream stays synchronised.
            self.refuse(tag::is_extended(t), out, "08P01", "malformed message body");
        }

        Some(total)
    }

    /// The body of one framed message.
    /// Every `?` here means "this body is malformed", never "wait for more bytes".
    fn dispatch(
        &mut self,
        t: u8,
        body: &[u8],
        out: &mut Vec<u8>,
        catalog: &CatalogView,
    ) -> Option<()> {
        // Skipping to Sync: only Sync ends it, and only Terminate is otherwise heard.
        if self.phase == Phase::AwaitingSync {
            match t {
                tag::SYNC if body.is_empty() => {
                    self.phase = Phase::Query;
                    self.ready(out);
                }
                tag::SYNC => {
                    self.phase = Phase::Query;
                    return None;
                }
                tag::TERMINATE => self.phase = Phase::Closed,
                _ => {}
            }

            return Some(());
        }

        // A COPY in flight has its own vocabulary, and everything outside it is a
        // violation rather than a statement to run.
        if let Phase::CopyIn {
            format, extended, ..
        } = self.phase
        {
            return self.copy_message(t, body, format, extended, out);
        }

        match t {
            tag::TERMINATE => self.on_terminate(body, out, catalog),

            tag::QUERY => self.on_query(body, out, catalog),

            tag::COPY_DATA | tag::COPY_DONE | tag::COPY_FAIL => {
                self.on_copy_message(body, out, catalog)
            }

            tag::PARSE => self.on_parse(body, out, catalog),

            tag::BIND => self.on_bind(body, out, catalog),

            tag::DESCRIBE => self.on_describe(body, out, catalog),

            tag::EXECUTE => self.on_execute(body, out, catalog),

            tag::SYNC => self.on_sync(body, out, catalog),

            tag::FLUSH => self.on_flush(body, out, catalog),

            tag::CLOSE => self.on_close(body, out, catalog),

            _ => Some(()),
        }
    }

    /// One `TERMINATE` message.
    fn on_terminate(
        &mut self,
        _body: &[u8],
        _out: &mut Vec<u8>,
        _catalog: &CatalogView,
    ) -> Option<()> {
        self.phase = Phase::Closed;
        Some(())
    }

    /// One `QUERY` message.
    fn on_query(&mut self, body: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> Option<()> {
        // A Query without its NUL is malformed, not empty: this was
        // `unwrap_or("")`, and `select 1` with no terminator answered
        // EmptyQueryResponse where PostgreSQL answers 08P01.
        let (sql, end) = cstr_at(body, 0)?;
        exhausted(body, end)?;
        // `PREPARE name AS body` registers a statement: a SQL-preparing
        // client sends no Parse, and an unregistered name is 26000 on Bind.
        if let Some((name, body)) = crate::sql::sql_prepare(sql) {
            if self.statements.contains_key(name) {
                self.error(
                    out,
                    "42P05",
                    &format!("prepared statement \"{name}\" already exists"),
                );
            } else {
                let plan = PreparedPlan::build(body);
                let params = crate::plan::parameter_types(body, &[]).unwrap_or_default();
                self.statements.insert(
                    name.to_string(),
                    std::sync::Arc::new(Statement { plan, params }),
                );
                complete_raw(out, "PREPARE");
            }
            self.ready(out);

            return Some(());
        }

        if let Some(failure) = crate::tpcc::failure(sql) {
            self.injected_failure(failure, out);
            self.ready(out);

            return Some(());
        }

        let plan = PreparedPlan::build(sql);

        // A failed transaction runs nothing until it is ended.
        if self.failed() && !ends_transaction(&plan.kind) {
            return self.refuse(false, out, "25P02", ABORTED);
        }

        // COPY is decided by its direction, once, in the plan.
        // `COPY ...
        if let PlanKind::Copy {
            direction,
            columns,
            format,
        } = plan.kind
        {
            match direction {
                CopyDirection::FromStdin => {
                    self.enter_copy_in(columns, format, false, out);

                    return Some(());
                }
                CopyDirection::ToStdout => {
                    // The server would stream rows here.
                    copy_out_response(out);
                    copy_done(out);
                    complete(out, "COPY", Some(0));
                }
                CopyDirection::FromFile => {
                    // No conversation: the server reads a path.
                    // It read nothing, so it copied nothing.
                    complete(out, "COPY", Some(0));
                }
            }

            self.ready(out);

            return Some(());
        }

        // A statement that opens, ends or fails a transaction changes what the
        // NEXT `ReadyForQuery` must say, and it is noted before the answer is
        // written so that the byte and the statement cannot disagree.
        let failed = self.failed();
        self.note_transaction(&plan.kind, failed);
        self.note_statement(&plan.sql);

        answer(
            &plan,
            out,
            catalog,
            Run {
                describe: true,
                failed,
                formats: &Formats::TEXT,
                max_rows: 0,
                consumed: false,
            },
        );
        self.ready(out);
        Some(())
    }

    /// One `COPY_DATA`/`COPY_DONE`/`COPY_FAIL` message.
    fn on_copy_message(
        &mut self,
        _body: &[u8],
        out: &mut Vec<u8>,
        _catalog: &CatalogView,
    ) -> Option<()> {
        // A COPY message outside a COPY is the client desynchronised, not
        // something to absorb quietly: absorbing it is what lets the two sides
        // disagree for the rest of the connection.
        self.error(out, "08P01", "COPY message outside a COPY operation");
        self.ready(out);
        Some(())
    }

    /// One `PARSE` message.
    fn on_parse(&mut self, body: &[u8], out: &mut Vec<u8>, _catalog: &CatalogView) -> Option<()> {
        // [stmt_name][query][n_params:i16][types...]
        let (name, after) = cstr_at(body, 0)?;
        let (sql, after) = cstr_at(body, after)?;
        let (declared, end) = oid_list(body, after)?;
        exhausted(body, end)?;
        let plan = PreparedPlan::build(sql);

        if self.failed() && !ends_transaction(&plan.kind) {
            return self.refuse(true, out, "25P02", ABORTED);
        }

        // A named statement is not replaced in place: PostgreSQL answers 42P05,
        // and a driver that wants the name back closes it first.
        if !name.is_empty() && self.statements.contains_key(name) {
            return self.refuse(
                true,
                out,
                "42P05",
                &format!("prepared statement \"{name}\" already exists"),
            );
        }

        let params = match crate::plan::parameter_types(sql, &declared) {
            Ok(params) => params,
            Err(e) => {
                return self.refuse(true, out, e.sqlstate(), &e.message());
            }
        };
        self.statements
            .insert(name.to_string(), Arc::new(Statement { plan, params }));
        msg(out, b'1', |_| {});
        Some(())
    }

    /// One `BIND` message.
    fn on_bind(&mut self, body: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> Option<()> {
        // [portal][stmt][n_pfmt][pfmt..][n_params][(len, bytes)..]
        // [n_rfmt][rfmt..].
        let (portal, after) = cstr_at(body, 0)?;
        let (stmt, after) = cstr_at(body, after)?;
        let bind = bind_tail(body, after)?;

        // A statement that was never prepared -- or was closed -- is not bound
        // to; it is reported.
        let Some(statement) = self.statements.get(stmt) else {
            return self.refuse(
                true,
                out,
                "26000",
                &format!("prepared statement \"{stmt}\" does not exist"),
            );
        };

        let Statement { plan, params } = &**statement;

        if self.failed() && !ends_transaction(&plan.kind) {
            return self.refuse(true, out, "25P02", ABORTED);
        }

        // The counts PostgreSQL checks, with its SQLSTATE and wording.
        if bind.params != params.len() {
            let want = params.len();
            return self.refuse(
                    true,
                    out,
                    "08P01",
                    &format!(
                        "bind message supplies {} parameters, but prepared statement \"{stmt}\" requires {want}",
                        bind.params
                    ),
                );
        }

        if let Some(mismatch) = bind.format_mismatch {
            return self.refuse(
                true,
                out,
                "08P01",
                &format!(
                    "bind message has {mismatch} parameter formats but {} parameters",
                    bind.params
                ),
            );
        }

        // A per-column result-format list has to be one per column.
        // Two codes for a one-column query answered BindComplete; PostgreSQL
        // refuses it.
        if bind.result_formats > 1 {
            let columns = match plan.kind {
                PlanKind::SelectMeta { .. }
                | PlanKind::SelectStub { .. }
                | PlanKind::Constant(_) => resolve_columns(plan, catalog).len(),
                _ => 0,
            };
            if bind.result_formats != columns {
                return self.refuse(
                    true,
                    out,
                    "08P01",
                    &format!(
                        "bind message has {} result formats but query has {columns} columns",
                        bind.result_formats
                    ),
                );
            }
        }

        // A named portal is not rebound in place either: 42P03, as PostgreSQL
        // answers, until it is closed.
        if !portal.is_empty() && self.portals.contains_key(portal) {
            return self.refuse(
                true,
                out,
                "42P03",
                &format!("portal \"{portal}\" already exists"),
            );
        }

        // One String allocation per query if done unconditionally, and a driver
        // binds the unnamed portal to the SAME statement forever.
        let statement = Arc::clone(statement);
        match self.portals.get_mut(portal) {
            Some(cur) => {
                cur.statement = statement;
                cur.formats = bind.formats;
                cur.consumed = false;
            }
            None => {
                self.portals.insert(
                    portal.to_string(),
                    Portal {
                        statement,
                        formats: bind.formats,
                        consumed: false,
                    },
                );
            }
        }
        msg(out, b'2', |_| {});
        Some(())
    }

    /// One `DESCRIBE` message.
    fn on_describe(&mut self, body: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> Option<()> {
        // ['S'|'P'][name].
        let kind = subtype(body)?;
        let (name, end) = cstr_at(body, 1)?;
        exhausted(body, end)?;

        let Some((Statement { plan, params }, formats)) = self.described(kind, name) else {
            let (code, what) = if kind == b'S' {
                ("26000", "prepared statement")
            } else {
                ("34000", "portal")
            };
            return self.refuse(
                true,
                out,
                code,
                &format!("{what} \"{name}\" does not exist"),
            );
        };

        // PostgreSQL refuses to describe a statement that returns rows inside a
        // failed transaction, and only those: a client that blindly describes
        // its ROLLBACK must still be able to send it.
        if self.failed()
            && matches!(
                plan.kind,
                PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. }
            )
        {
            return self.refuse(true, out, "25P02", ABORTED);
        }

        // A described STATEMENT also gets its parameter types.
        if kind == b'S' {
            msg(out, b't', |b| {
                b.extend_from_slice(&(params.len() as i16).to_be_bytes());
                for oid in params {
                    b.extend_from_slice(&oid.to_be_bytes());
                }
            });
        }

        if !row_description(plan, catalog, formats, out) {
            msg(out, b'n', |_| {});
        }
        Some(())
    }

    /// One `EXECUTE` message.
    fn on_execute(&mut self, body: &[u8], out: &mut Vec<u8>, catalog: &CatalogView) -> Option<()> {
        // [portal][max_rows:i32].
        // Zero is no limit.
        let (name, after) = cstr_at(body, 0)?;
        let max_rows = i32::from_be_bytes(body.get(after..after + 4)?.try_into().ok()?);
        exhausted(body, after + 4)?;
        let failed = self.failed();

        // **The injected failure is refused here too**, or a NULL written
        // through the extended protocol answers OK. Detected before the
        // mutable borrow, because the error path needs `self`.
        let injected = self
            .portals
            .get(name)
            .and_then(|p| crate::tpcc::failure(&p.statement.plan.sql));
        if let Some(failure) = injected {
            self.injected_failure(failure, out);
            self.phase = Phase::AwaitingSync;

            return Some(());
        }

        // No clone: `answer` is a free function precisely so the plan can stay
        // borrowed out of `self`.
        // The plan is the portal's own, so one lookup finds both.
        let Some(portal) = self.portals.get_mut(name) else {
            // PostgreSQL reports it, and so does this.
            return self.refuse(
                true,
                out,
                "34000",
                &format!("portal \"{name}\" does not exist"),
            );
        };
        let statement = Arc::clone(&portal.statement);
        let p = &statement.plan;

        if failed && !ends_transaction(&p.kind) {
            return self.refuse(true, out, "25P02", ABORTED);
        }

        // A COPY that streams from the client needs the connection to change
        // phase, which is not something a free function writing into `out` can
        // do.
        let streams_rows = match &p.kind {
            PlanKind::Copy {
                direction: CopyDirection::FromStdin,
                format,
                ..
            } => Some(*format),
            _ => None,
        };

        // Execute does NOT re-send RowDescription; Describe did.
        portal.consumed = answer(
            p,
            out,
            catalog,
            Run {
                describe: false,
                failed,
                formats: &portal.formats,
                max_rows,
                consumed: portal.consumed,
            },
        );

        // The extended protocol's Execute is where a statement actually runs,
        // so it is where the transaction state changes.
        // `SYNC` is what asks for the byte, later.
        self.note_transaction(&p.kind, failed);
        self.note_statement(&p.sql);

        if let Some(format) = streams_rows {
            self.copy_binary = BinaryState::default();
            self.copy_lines = LineState::default();
            self.phase = Phase::CopyIn {
                rows: 0,
                format,
                extended: true,
            };
        }
        Some(())
    }

    /// One `SYNC` message.
    fn on_sync(&mut self, body: &[u8], out: &mut Vec<u8>, _catalog: &CatalogView) -> Option<()> {
        exhausted(body, 0)?;
        self.ready(out);
        Some(())
    }

    /// One `FLUSH` message.
    fn on_flush(&mut self, body: &[u8], _out: &mut Vec<u8>, _catalog: &CatalogView) -> Option<()> {
        exhausted(body, 0)?;

        Some(())
    }

    /// One `CLOSE` message.
    fn on_close(&mut self, body: &[u8], out: &mut Vec<u8>, _catalog: &CatalogView) -> Option<()> {
        // ['S'|'P'][name], strictly: Close `X` answered CloseComplete where
        // PostgreSQL answers 08P01.
        let kind = subtype(body)?;
        let (name, end) = cstr_at(body, 1)?;
        exhausted(body, end)?;
        if kind == b'S' {
            self.statements.remove(name);
        } else {
            self.portals.remove(name);
        }
        msg(out, b'3', |_| {});
        Some(())
    }

    /// One message while a COPY FROM STDIN is open.
    /// `d`, `c` and `f` are the conversation.
    fn copy_message(
        &mut self,
        t: u8,
        body: &[u8],
        format: CopyFormat,
        extended: bool,
        out: &mut Vec<u8>,
    ) -> Option<()> {
        let binary = format == CopyFormat::Binary;

        match t {
            tag::COPY_DATA => {
                if binary {
                    if let Err(violation) = self.count_binary(body) {
                        // A stream that is not PostgreSQL's binary framing is not
                        // counted, it is refused -- with the SQLSTATE the server uses
                        // for a bad COPY file, and the recovery the entry protocol
                        self.leave_copy_in();
                        self.refuse(extended, out, "22P04", violation);
                    }
                } else {
                    self.count_lines(body, format == CopyFormat::Csv);
                }
            }

            tag::COPY_DONE => {
                // The end of a binary stream is checked against its framing; this used
                // to answer `COPY n` whatever state it was in.
                if binary {
                    // PostgreSQL's reader takes a clean EOF at a tuple boundary as the
                    // end (`CopyFromBinaryOneRow`: "end of file, or trailer"), so a
                    // missing trailer is complete; no header is not a binary COPY at
                    let violation = match self.copy_binary.stage {
                        BinaryStage::FieldCount | BinaryStage::Done => None,
                        BinaryStage::Header => Some("COPY file signature not recognized"),
                        BinaryStage::Extension { .. } => {
                            Some("invalid COPY file header (wrong length)")
                        }
                        BinaryStage::FieldLength { .. } | BinaryStage::Payload { .. } => {
                            Some("unexpected EOF in COPY data")
                        }
                    };

                    if let Some(violation) = violation {
                        self.leave_copy_in();
                        return self.refuse(extended, out, "22P04", violation);
                    }
                } else {
                    // A quote still open at the end is PostgreSQL's error for it; a
                    // line still open is the last row, terminated by the end of the
                    // data rather than a newline.
                    if self.copy_lines.in_quotes {
                        self.leave_copy_in();
                        return self.refuse(
                            extended,
                            out,
                            "22P04",
                            "unterminated CSV quoted field",
                        );
                    }

                    if self.copy_lines.line_open && !self.copy_lines.ended {
                        if let Phase::CopyIn { rows, .. } = &mut self.phase {
                            *rows += 1;
                        }
                    }
                }

                let rows = match &self.phase {
                    Phase::CopyIn { rows, .. } => *rows,
                    _ => 0,
                };
                self.leave_copy_in();
                complete_raw(out, &format!("COPY {rows}"));

                // Simple protocol: the statement is over, say so.
                // Extended: the client's Sync behind CopyDone asks for the byte, later.
                if !extended {
                    self.ready(out);
                }
            }

            tag::COPY_FAIL => {
                // The client is abandoning its own COPY.
                // That is an error by the protocol's own definition, and answering it
                // as success would tell a loader its data landed.
                self.leave_copy_in();
                self.refuse(extended, out, "57014", "COPY from stdin failed");
            }

            // Sent behind the Execute by drivers that did not notice the statement was
            // a COPY; PostgreSQL ignores both here.
            tag::SYNC | tag::FLUSH => {}

            tag::TERMINATE => self.phase = Phase::Closed,

            _ => {
                self.leave_copy_in();
                self.refuse(
                    extended,
                    out,
                    "08P01",
                    "unexpected message type during COPY from stdin",
                );
            }
        }

        Some(())
    }

    /// The statement a Describe names, directly or through a portal, and the result
    /// formats to describe it in.
    fn described(&self, kind: u8, name: &str) -> Option<(&Statement, &Formats)> {
        if kind == b'S' {
            self.statements.get(name).map(|s| (&**s, &Formats::TEXT))
        } else {
            self.portals.get(name).map(|p| (&*p.statement, &p.formats))
        }
    }
}

/// The rows and the completion tag for one plan.
/// A FREE function, not a method: it never needed `self`, and being a method is what
/// forced a deep clone of the plan at every Execute.
/// Answer one execution, and say whether the portal has rows left to send.
///
/// A single-row answer suspended by `max_rows` has none: the row went out with the
/// `PortalSuspended`, so the next Execute short of closing the portal answers
/// `SELECT 0`, which is what PostgreSQL does too.
fn answer(plan: &PreparedPlan, out: &mut Vec<u8>, catalog: &CatalogView, run: Run<'_>) -> bool {
    // The statements answered with a number are the TPC-C profile's.
    if let Some((name, value)) = crate::tpcc::number_query(&plan.sql) {
        return number_answer(out, run, name, value);
    }

    if crate::tpcc::profile() {
        if let Some(filter) = crate::tpcc::show_status(&plan.sql) {
            return status_answer(out, run, filter.as_deref());
        }
    }

    match &plan.kind {
        // A constant select answers one row, and nothing else does.
        // The values are the statement's own: no data is involved, so the row is
        // true-shaped on both protocols.
        PlanKind::Constant(items) => {
            if run.describe {
                row_description(plan, catalog, run.formats, out);
            }

            if run.consumed {
                complete(out, "SELECT", Some(0));

                return true;
            }

            constant_row(items, out, run.formats);
            if run.max_rows == 1 {
                portal_suspended(out);

                return true;
            }
            complete(out, "SELECT", Some(1));

            return true;
        }
        // A select that names data answers no rows.
        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. } => {
            if run.describe {
                row_description(plan, catalog, run.formats, out);
            }
            complete(out, "SELECT", Some(0));

            return true;
        }
        // Nothing was written, so nothing is reported written -- the tags the README
        // documents.
        PlanKind::Insert => complete_raw(out, "INSERT 0 0"),
        PlanKind::Update => complete_raw(out, "UPDATE 0"),
        PlanKind::Delete => complete_raw(out, "DELETE 0"),
        PlanKind::Begin => complete_raw(out, "BEGIN"),
        // The counters are moved by the connection, at the moment the transaction ends;
        // the tag is what the client reads.
        PlanKind::Commit if run.failed => complete_raw(out, "ROLLBACK"),
        PlanKind::Commit => complete_raw(out, "COMMIT"),
        PlanKind::Rollback => complete_raw(out, "ROLLBACK"),
        PlanKind::Ddl => {
            catalog.apply(&plan.sql);
            complete_raw(out, ddl_tag(&plan.sql));
        }
        // Reached by the extended protocol's Execute.
        // The caller sets `Phase::CopyIn` for the FROM STDIN half, because the phase
        // belongs to the connection and not to the reply.
        PlanKind::Copy {
            direction,
            columns,
            format,
        } => match direction {
            CopyDirection::FromStdin => {
                copy_in_response(out, *columns, *format == CopyFormat::Binary)
            }
            CopyDirection::ToStdout => {
                copy_out_response(out);
                copy_done(out);
                complete(out, "COPY", Some(0));
            }
            CopyDirection::FromFile => complete(out, "COPY", Some(0)),
        },
        // PostgreSQL's answer to nothing: `I`, in place of CommandComplete.
        PlanKind::Empty => msg(out, b'I', |_| {}),
        PlanKind::FromText => complete(out, "SELECT", Some(0)),
    }

    true
}

/// A one-number answer's column, int8, in the format the portal asked for.
fn number_description(formats: &Formats, out: &mut Vec<u8>, name: &str) {
    msg(out, b'T', |b| {
        b.extend_from_slice(&1i16.to_be_bytes());
        cstr(b, name);
        b.extend_from_slice(&0i32.to_be_bytes()); // table oid: none
        b.extend_from_slice(&0i16.to_be_bytes()); // column number: none
        b.extend_from_slice(&20u32.to_be_bytes()); // int8
        b.extend_from_slice(&8i16.to_be_bytes()); // type size
        b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
        b.extend_from_slice(&formats.get(0).to_be_bytes());
    });
}

/// The two columns of a status answer: `Variable_name` and `Value`, both text.
fn status_description(out: &mut Vec<u8>) {
    msg(out, b'T', |b| {
        b.extend_from_slice(&2i16.to_be_bytes());
        for name in ["Variable_name", "Value"] {
            cstr(b, name);
            b.extend_from_slice(&0i32.to_be_bytes()); // table oid: none
            b.extend_from_slice(&0i16.to_be_bytes()); // column number: none
            b.extend_from_slice(&25u32.to_be_bytes()); // text
            b.extend_from_slice(&(-1i16).to_be_bytes()); // type size: variable
            b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            b.extend_from_slice(&0i16.to_be_bytes()); // text format
        }
    });
}

/// The TPC-C commit counters as a result set of two text columns.
fn status_answer(out: &mut Vec<u8>, run: Run<'_>, filter: Option<&str>) -> bool {
    if run.describe {
        status_description(out);
    }

    if run.consumed {
        complete(out, "SELECT", Some(0));

        return true;
    }

    let rows = crate::tpcc::status_rows(filter);
    let mut written = 0u64;
    for (name, value) in &rows {
        let value = value.to_string();
        msg(out, b'D', |b| {
            b.extend_from_slice(&2i16.to_be_bytes());
            for text in [*name, value.as_str()] {
                b.extend_from_slice(&(text.len() as i32).to_be_bytes());
                b.extend_from_slice(text.as_bytes());
            }
        });
        written += 1;
    }

    if run.max_rows > 0 && written >= run.max_rows as u64 {
        portal_suspended(out);

        return true;
    }
    complete(out, "SELECT", Some(written));

    true
}

/// A TPC-C one-number answer: one row, one int8 column.
fn number_answer(out: &mut Vec<u8>, run: Run<'_>, name: &str, value: u64) -> bool {
    if run.describe {
        number_description(run.formats, out, name);
    }

    // A portal already read to its end has nothing more, as for any other single-row
    // plan.
    if run.consumed {
        complete(out, "SELECT", Some(0));

        return true;
    }

    msg(out, b'D', |b| {
        b.extend_from_slice(&1i16.to_be_bytes());
        if run.formats.get(0) == 1 {
            // The binary form of int8: eight bytes, network order.
            b.extend_from_slice(&8i32.to_be_bytes());
            b.extend_from_slice(&(value as i64).to_be_bytes());
        } else {
            let text = value.to_string();
            b.extend_from_slice(&(text.len() as i32).to_be_bytes());
            b.extend_from_slice(text.as_bytes());
        }
    });

    // The one row is also the last one, but with a limit of one the server cannot know
    // that until it looks for the next -- which is why PostgreSQL answers
    // PortalSuspended here, as it does for any stub row.
    if run.max_rows == 1 {
        portal_suspended(out);

        return true;
    }
    complete(out, "SELECT", Some(1));

    true
}

/// One DataRow of the values a constant select stated, in the format the portal asked
/// for.
fn constant_row(items: &[crate::sql::Constant], out: &mut Vec<u8>, formats: &Formats) {
    msg(out, b'D', |b| {
        b.extend_from_slice(&(items.len() as i16).to_be_bytes());
        for (i, item) in items.iter().enumerate() {
            match (item.kind, formats.get(i) == 1) {
                (crate::sql::ConstantKind::Integer, true) => {
                    let n: i64 = item.value.parse().unwrap_or_default();
                    b.extend_from_slice(&8i32.to_be_bytes());
                    b.extend_from_slice(&n.to_be_bytes());
                }
                _ => {
                    b.extend_from_slice(&(item.value.len() as i32).to_be_bytes());
                    b.extend_from_slice(item.value.as_bytes());
                }
            }
        }
    });
}

/// How one execution of a plan is to be answered.
struct Run<'a> {
    /// Whether to send RowDescription first: the simple protocol does, and Execute does
    /// not, because Describe did.
    describe: bool,
    /// Whether the transaction the statement runs in has failed.
    failed: bool,
    /// What the portal's Bind asked the rows to be encoded in.
    /// The simple protocol has no Bind and is always text.
    formats: &'a Formats,
    /// Execute's row limit; zero is none.
    max_rows: i32,
    /// Whether the portal has no rows left to send.
    consumed: bool,
}

/// `PortalSuspended`: the row limit was reached with the portal still open.
fn portal_suspended(out: &mut Vec<u8>) {
    msg(out, b's', |_| {});
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

/// CommandComplete with a trailing count, written WITHOUT formatting into a temporary
/// String.
/// `format!("SELECT {n}")` was one heap allocation per query.
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

/// `COPY ...
/// FROM STDIN` puts the connection into a conversation: the client sends `CopyData` and
/// ends it with `CopyDone` or `CopyFail`.
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

/// `CopyOutResponse` for `COPY ...
/// TO STDOUT`.
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
    error_with_severity(out, "ERROR", code, message);
}

/// An ErrorResponse that ends the connection: PostgreSQL's severity for a startup the
/// server will not continue.
fn fatal_response(out: &mut Vec<u8>, code: &str, message: &str) {
    error_with_severity(out, "FATAL", code, message);
}

fn error_with_severity(out: &mut Vec<u8>, severity: &str, code: &str, message: &str) {
    msg(out, b'E', |b| {
        b.push(b'S');
        cstr(b, severity);
        b.push(b'V');
        cstr(b, severity);
        b.push(b'C');
        cstr(b, code);
        b.push(b'M');
        cstr(b, message);
        b.push(0);
    });
}

/// The columns a SELECT answers with: the catalog's, when it knows the table, or `n`
/// stubs.
enum Columns {
    /// `?column?` int8, `n` times, from a `&'static str` rather than n freshly
    /// allocated Strings.
    Stub(usize),
    Known(Vec<(String, u32)>),
    /// A constant select's items: the statement's own text and value types.
    Constant(Vec<crate::sql::Constant>),
}

impl Columns {
    fn len(&self) -> usize {
        match self {
            Columns::Stub(n) => *n,
            Columns::Known(cols) => cols.len(),
            Columns::Constant(items) => items.len(),
        }
    }

    fn name(&self, i: usize) -> &str {
        match self {
            Columns::Stub(_) => STUB_COL,
            Columns::Known(cols) => &cols[i].0,
            Columns::Constant(items) => &items[i].name,
        }
    }

    fn oid(&self, i: usize) -> u32 {
        match self {
            Columns::Stub(_) => 20,
            Columns::Known(cols) => cols[i].1,
            Columns::Constant(items) => match items[i].kind {
                crate::sql::ConstantKind::Integer => 20,
                crate::sql::ConstantKind::Text => 25,
            },
        }
    }
}

fn resolve_columns(plan: &PreparedPlan, catalog: &CatalogView) -> Columns {
    match &plan.kind {
        PlanKind::SelectMeta { table, columns } => match catalog.columns_for(table, columns) {
            Some(names) => Columns::Known(names),
            None => Columns::Stub(columns.len().max(1)),
        },
        PlanKind::SelectStub { columns } => Columns::Stub(*columns),
        PlanKind::Constant(items) => Columns::Constant(items.clone()),
        _ => Columns::Stub(0),
    }
}

/// RowDescription for a plan that returns rows, or false for one that does not.
fn row_description(
    plan: &PreparedPlan,
    catalog: &CatalogView,
    formats: &Formats,
    out: &mut Vec<u8>,
) -> bool {
    // Described as it is answered: one int8 column named `sum`, so a driver that
    // decodes the value in binary reads the type it was told.
    if let Some((name, _)) = crate::tpcc::number_query(&plan.sql) {
        number_description(formats, out, name);

        return true;
    }
    if crate::tpcc::profile() && crate::tpcc::show_status(&plan.sql).is_some() {
        status_description(out);

        return true;
    }

    if let PlanKind::Constant(items) = &plan.kind {
        row_description_of(&Columns::Constant(items.clone()), formats, out);

        return true;
    }

    if !matches!(
        plan.kind,
        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. }
    ) {
        return false;
    }

    row_description_of(&resolve_columns(plan, catalog), formats, out);

    true
}

/// RowDescription: each column's name, type, and the format code the portal asked for.
/// The format codes are the client's own request read back, and a driver that asked for
/// binary decodes what follows as binary.
fn row_description_of(cols: &Columns, formats: &Formats, out: &mut Vec<u8>) {
    msg(out, b'T', |b| {
        b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for i in 0..cols.len() {
            cstr(b, cols.name(i));
            b.extend_from_slice(&0i32.to_be_bytes()); // table oid
            b.extend_from_slice(&0i16.to_be_bytes()); // column no
            b.extend_from_slice(&cols.oid(i).to_be_bytes()); // type oid
            b.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            b.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            b.extend_from_slice(&formats.get(i).to_be_bytes());
        }
    });
}

const STUB_COL: &str = "?column?";

fn ddl_tag(sql: &str) -> &'static str {
    let head = sql.trim_start();
    if head.len() >= 6 && head.as_bytes()[..6].eq_ignore_ascii_case(b"CREATE") {
        "CREATE TABLE"
    } else {
        "DROP TABLE"
    }
}

// ------------------------------------------------------------------ decoding

/// The `S` or `P` a Describe or Close begins with.
/// Anything else is a malformed body, not a portal.
fn subtype(body: &[u8]) -> Option<u8> {
    match body.first() {
        Some(&k @ (b'S' | b'P')) => Some(k),
        _ => None,
    }
}

/// An int16 count at `from` followed by that many int32 OIDs, as Parse declares its
/// parameter types, and the index after them.
/// A count that outruns the body is malformed.
fn oid_list(body: &[u8], from: usize) -> Option<(Vec<u32>, usize)> {
    let n = i16::from_be_bytes(body.get(from..from + 2)?.try_into().ok()?);
    let n = usize::try_from(n).ok()?;
    let end = from + 2 + 4 * n;
    let bytes = body.get(from + 2..end)?;

    Some((
        (0..n)
            .map(|i| u32::from_be_bytes(bytes[4 * i..4 * i + 4].try_into().unwrap()))
            .collect(),
        end,
    ))
}

/// A body is its fields and nothing after them.
fn exhausted(body: &[u8], end: usize) -> Option<()> {
    (end == body.len()).then_some(())
}

/// What a Bind says after its two names.
struct BindTail {
    /// How many parameter values it carries.
    params: usize,
    /// The parameter-format count, when it is not 0, 1 or `params`.
    format_mismatch: Option<usize>,
    /// How many result-format codes it carries, which must be 0, 1 or the statement's
    /// column count -- checked by the caller, who has the plan.
    result_formats: usize,
    /// The result formats it asks for.
    formats: Formats,
}

/// Decode the rest of a Bind: parameter formats, parameter values, result formats.
/// `None` is a body that ends before its own counts do, or a format code that is
/// neither text nor binary -- malformed, not "no formats".
fn bind_tail(body: &[u8], from: usize) -> Option<BindTail> {
    fn i16_at(body: &[u8], at: usize) -> Option<i16> {
        Some(i16::from_be_bytes(body.get(at..at + 2)?.try_into().ok()?))
    }

    let mut at = from;

    let n_pformats = usize::try_from(i16_at(body, at)?).ok()?;
    at += 2;
    for _ in 0..n_pformats {
        if !matches!(i16_at(body, at)?, 0 | 1) {
            return None;
        }
        at += 2;
    }

    let params = usize::try_from(i16_at(body, at)?).ok()?;
    at += 2;
    for _ in 0..params {
        let len = i32::from_be_bytes(body.get(at..at + 4)?.try_into().ok()?);
        at += 4;
        // -1 is NULL.
        if len < -1 {
            return None;
        }
        if len >= 0 {
            at += len as usize;
            body.get(..at)?;
        }
    }

    let n_rformats = usize::try_from(i16_at(body, at)?).ok()?;
    at += 2;
    let mut codes = [0i16; 64];
    let mut spilled = Vec::new();
    for i in 0..n_rformats {
        let code = i16_at(body, at)?;
        if !matches!(code, 0 | 1) {
            return None;
        }
        at += 2;
        if i < codes.len() {
            codes[i] = code;
        } else {
            if spilled.is_empty() {
                spilled.extend_from_slice(&codes);
            }
            spilled.push(code);
        }
    }

    if at != body.len() {
        return None;
    }

    let formats = if spilled.is_empty() {
        Formats::from_codes(&codes[..n_rformats])
    } else {
        Formats::from_codes(&spilled)
    };

    let format_mismatch = (n_pformats > 1 && n_pformats != params).then_some(n_pformats);

    Some(BindTail {
        params,
        format_mismatch,
        result_formats: n_rformats,
        formats,
    })
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
    use crate::catalog::NoopHandler;
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

    /// Every message tag present in `out`, in order.
    /// The property most of these tests assert is about the SEQUENCE the client sees.
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

    /// The status byte of the LAST `ReadyForQuery` in a reply.
    fn last_ready_status(out: &[u8]) -> u8 {
        let mut i = 0;
        let mut status = None;

        while i + 5 <= out.len() {
            let tag = out[i];
            let len = i32::from_be_bytes([out[i + 1], out[i + 2], out[i + 3], out[i + 4]]) as usize;

            if len < 4 || i + 1 + len > out.len() {
                break;
            }

            if tag == b'Z' {
                status = Some(out[i + 5]);
            }

            i += 1 + len;
        }

        status.expect("no ReadyForQuery in the reply")
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

    /// THE PROPERTY THIS FILE EXISTS FOR.
    /// A pipelined Bind/Execute/Sync must produce ONE contiguous reply, because the
    /// caller writes `out` once.
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

    /// A message split across reads must not be consumed or answered until it is whole
    /// -- otherwise a slow client desynchronises the stream.
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

    /// The one statement answered with a number.
    #[test]
    fn the_district_sum_is_answered_with_one_row() {
        let _profile = crate::tpcc::profile_guard(true);
        let (mut c, v) = (Conn::new(), view());
        let mut out = Vec::new();
        c.advance(&startup_msg(), &mut out, &v);

        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "select sum(d_next_o_id) from district")),
            &mut out,
            &v,
        );

        let mut at = 0usize;
        let (tag, body) = frame(&out, &mut at);
        assert_eq!(tag, b'T', "a RowDescription");
        assert_eq!(i16::from_be_bytes([body[0], body[1]]), 1, "one column");
        assert!(
            body.windows(3).any(|w| w == b"sum"),
            "the column a client asked for"
        );
        // The field's type follows its name, the table oid and the column number: int8,
        // so a driver decodes the value it was promised.
        let field = &body[2..];
        let name_end = field.iter().position(|&b| b == 0).expect("a name") + 1;
        let oid_at = name_end + 4 + 2;
        assert_eq!(
            u32::from_be_bytes([
                field[oid_at],
                field[oid_at + 1],
                field[oid_at + 2],
                field[oid_at + 3]
            ]),
            20,
            "int8"
        );

        let (tag, body) = frame(&out, &mut at);
        assert_eq!(tag, b'D', "a DataRow");
        assert_eq!(i16::from_be_bytes([body[0], body[1]]), 1, "one value");
        let len = i32::from_be_bytes([body[2], body[3], body[4], body[5]]) as usize;
        let value: u64 = std::str::from_utf8(&body[6..6 + len])
            .expect("utf-8")
            .parse()
            .expect("a number");
        assert!(value >= 3001, "{value} is not a district sum");

        let (tag, body) = frame(&out, &mut at);
        assert_eq!(tag, b'C', "a CommandComplete");
        assert_eq!(body, b"SELECT 1\0");
        assert_eq!(frame(&out, &mut at).0, b'Z', "ready for the next query");
    }

    /// One frame out of a buffer: its tag and its body, and where the next one starts.
    fn frame<'a>(out: &'a [u8], at: &mut usize) -> (u8, &'a [u8]) {
        let tag = out[*at];
        let len =
            i32::from_be_bytes([out[*at + 1], out[*at + 2], out[*at + 3], out[*at + 4]]) as usize;
        let body = &out[*at + 5..*at + 1 + len];
        *at += 1 + len;

        (tag, body)
    }

    /// SQL `PREPARE name AS body` registers `name` for Bind/Execute.
    /// HammerDB's PostgreSQL TPROC-C prepares its stored procedures this way at login
    /// (`PREPARE neword AS select ...`) and then executes them by name.
    #[test]
    fn a_sql_prepare_registers_a_statement_for_bind() {
        let (mut c, v) = (Conn::new(), view());
        let mut out = Vec::new();
        c.advance(&startup_msg(), &mut out, &v);

        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "PREPARE neword AS select 1")),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'C', "a CommandComplete: {out:?}");
        assert!(String::from_utf8_lossy(&out).contains("PREPARE"));

        // The name is an ordinary prepared statement from here: Bind/Execute it.
        let mut out = Vec::new();
        c.advance(
            &tagged(b'B', |b| {
                cstr(b, "");
                cstr(b, "neword");
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'2', "BindComplete, not 26000: {out:?}");

        let mut out = Vec::new();
        c.advance(
            &tagged(b'E', |b| {
                cstr(b, "");
                b.extend_from_slice(&0i32.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        c.advance(&tagged(b'S', |_| {}), &mut out, &v);
        assert!(out.contains(&b'D'), "executing it answers its row: {out:?}");
    }

    /// A `PREPARE` of a name already in use is PostgreSQL's 42P05, and a name that was
    /// never prepared is still 26000.
    #[test]
    fn a_sql_prepare_refuses_a_duplicate_name() {
        let (mut c, v) = (Conn::new(), view());
        let mut out = Vec::new();
        c.advance(&startup_msg(), &mut out, &v);

        let mut twice = tagged(b'Q', |b| cstr(b, "PREPARE s AS select 1"));
        twice.extend(tagged(b'Q', |b| cstr(b, "PREPARE s AS select 2")));
        let mut out = Vec::new();
        c.advance(&twice, &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("PREPARE"), "{text}");
        assert!(text.contains("42P05"), "a second PREPARE of s: {text}");
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

    /// The status byte is not a constant.
    /// `ReadyForQuery` always said `I`.
    #[test]
    fn a_raw_begin_reports_in_transaction() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);

        // `BEGIN` answers `BEGIN`, then `ReadyForQuery`, whose status byte is the byte
        // after the message's own length.
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("BEGIN"), "{text}");
        assert_eq!(last_ready_status(&out), b'T', "a transaction block is open");
    }

    /// `COMMIT` and `ROLLBACK` close it in both directions, including out of a failed
    /// transaction.
    #[test]
    fn commit_and_rollback_report_idle() {
        for end in ["COMMIT", "ROLLBACK"] {
            let (mut c, v) = connected();
            let mut out = Vec::new();

            c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
            assert_eq!(last_ready_status(&out), b'T');

            out.clear();
            c.advance(&tagged(b'Q', |b| cstr(b, end)), &mut out, &v);
            assert_eq!(last_ready_status(&out), b'I', "after {end}");
        }
    }

    /// An error inside a transaction block leaves the session in a FAILED transaction
    /// -- `E` -- until it is rolled back, which is what PostgreSQL does and what tells
    /// a client its statements cannot be retried in place.
    #[test]
    fn an_error_inside_a_transaction_reports_failed() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        // Outside: idle.
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
        out.clear();

        // A COPY message with no COPY open is the desync case, and an error.
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        assert!(out.contains(&b'E'), "an ErrorResponse");
        assert_eq!(last_ready_status(&out), b'E', "failed transaction");

        out.clear();
        c.advance(&tagged(b'Q', |b| cstr(b, "ROLLBACK")), &mut out, &v);
        assert_eq!(last_ready_status(&out), b'I', "ROLLBACK clears it");

        // And an error with no transaction open stays idle.
        out.clear();
        c.advance(&tagged(b'f', |b| cstr(b, "nope")), &mut out, &v);
        assert_eq!(last_ready_status(&out), b'I');
    }

    /// The extended protocol's Execute changes the state, and `Sync` is what reports it
    /// -- so `Parse/Bind/Execute(Sync)` around a `BEGIN` must say `T`.
    #[test]
    fn an_extended_begin_reports_in_transaction() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        let mut input = Vec::new();
        input.extend_from_slice(&tagged(b'P', |b| {
            cstr(b, "");
            cstr(b, "BEGIN");
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
        input.extend_from_slice(&tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "");
            b.extend_from_slice(&0i16.to_be_bytes()); // no formats
            b.extend_from_slice(&0i16.to_be_bytes()); // no params
            b.extend_from_slice(&0i16.to_be_bytes()); // no result formats
        }));
        input.extend_from_slice(&tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend_from_slice(&tagged(b'S', |_| {}));

        c.advance(&input, &mut out, &v);

        assert_eq!(last_ready_status(&out), b'T', "Sync reports the open block");
    }

    /// The one piece of real behaviour: a client that creates a table and then selects
    /// from it gets ITS OWN column names back, not stubs.
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
        assert!(
            text.contains("id"),
            "the client's own column name: {text:?}"
        );
        assert!(text.contains("label"), "and the second: {text:?}");
        assert!(
            !text.contains("?column?"),
            "a known table must not answer with stubs: {text:?}"
        );
    }

    /// A client-controlled negative length cast to usize becomes enormous, so `input
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

    /// COPY FROM STDIN is a conversation, and answering it with `CommandComplete`
    /// desynchronises the stream: the client believes the statement finished and then
    /// writes rows into a connection that moved on.
    #[test]
    fn copy_from_stdin_gets_its_response_and_row_count() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| {
                cstr(b, "COPY warehouse (w_id, w_name) FROM STDIN")
            }),
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
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(b"1\tone\n2\ttwo\n")),
            &mut out,
            &v,
        );
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(b"3\tthree\n")),
            &mut out,
            &v,
        );
        assert!(
            out.is_empty(),
            "CopyData is absorbed silently, as the protocol says"
        );

        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(
            text.contains("COPY 3"),
            "row count must be reported: {text}"
        );
        assert!(out.ends_with(b"I"), "and ReadyForQuery(idle) must close it");
    }

    /// A client that abandons its own COPY is told it failed.
    /// Answering success would tell a loader its data landed.
    #[test]
    fn copy_fail_is_an_error_not_a_success() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN")),
            &mut out,
            &v,
        );

        out.clear();
        c.advance(&tagged(b'f', |b| cstr(b, "client gave up")), &mut out, &v);
        assert_eq!(out[0], b'E', "CopyFail must produce ErrorResponse");
        assert!(!c.is_closed(), "and the connection survives it");
    }

    /// COPY TO STDOUT and COPY FROM a file are not conversations, so they must not
    /// enter the copy phase -- but they are COPY statements, and the tag a client
    /// checks is `COPY n`.
    #[test]
    fn only_copy_from_stdin_enters_the_copy_phase() {
        for sql in ["COPY t TO STDOUT", "COPY t FROM '/tmp/x.csv'", "SELECT 1"] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);
            assert_ne!(out[0], b'G', "{sql} must not get CopyInResponse");
            assert!(out.ends_with(b"I"), "{sql} must be answered and ready");
        }
    }

    /// `COPY t TO STDOUT` is a `CopyOutResponse`, an empty stream and `COPY 0`.
    #[test]
    fn copy_to_stdout_answers_copy_out_and_a_copy_tag() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY warehouse TO STDOUT")),
            &mut out,
            &v,
        );

        assert_eq!(out[0], b'H', "CopyOutResponse");
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 0"), "the tag must be COPY 0: {text}");
        assert!(!text.contains("SELECT"), "not a SELECT: {text}");
        // CopyOutResponse is `H` + length + (format, columns) = 8 bytes, then the
        // server's own CopyDone, then the tag, then ReadyForQuery.
        assert_eq!(out[8], b'c', "CopyDone before the tag");
        assert!(out.ends_with(b"I"), "and ready");
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

    /// SQL whitespace is not one space: three spaces, a tab and a newline are all the
    /// same statement, and all three have to be recognised as the conversation FROM
    /// STDIN is.
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
            assert!(
                text.contains("COPY 1"),
                "{sql:?} must count its row: {text}"
            );
        }
    }

    /// The column count announced to the client has to match the client's own column
    /// list, whatever whitespace separates them.
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

    /// Binary COPY counts tuples, not newlines.
    /// pgx's `CopyFrom` sends PostgreSQL's binary framing, where `0x0a` is just a byte
    /// in a payload.
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
        assert_eq!(
            i16::from_be_bytes([out[8], out[9]]),
            1,
            "binary, per column"
        );
        out.clear();

        // Header, then three tuples.
        // The second field of the first row contains a newline byte, and the third row
        // is split across two messages.
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

        // Split it mid-tuple on purpose: a CopyData message is a slice of the stream,
        // not a row.
        let split = stream.len() - 12;

        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&stream[..split])),
            &mut out,
            &v,
        );
        assert!(out.is_empty(), "CopyData is absorbed silently");
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&stream[split..])),
            &mut out,
            &v,
        );

        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 3"), "three tuples: {text}");
    }

    /// A NULL field is a length of -1 with no bytes after it, and it must not throw the
    /// frame walk off by four.
    #[test]
    fn binary_copy_counts_nulls_and_knows_when_the_stream_is_over() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );
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

        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&stream)),
            &mut out,
            &v,
        );
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);

        let text = String::from_utf8_lossy(&out);
        assert!(
            text.contains("COPY 1"),
            "one row, trailer not counted: {text}"
        );
    }

    /// A field is skipped, not buffered.
    #[test]
    fn binary_copy_skips_a_large_field_without_buffering() {
        let mut stream = binary_header(0, &[]);
        stream.extend_from_slice(&2i16.to_be_bytes());
        stream.extend_from_slice(&8i32.to_be_bytes());
        stream.extend_from_slice(&1i64.to_be_bytes());
        stream.extend_from_slice(&(2 * 1024 * 1024i32).to_be_bytes());
        let payload = vec![0xabu8; 2 * 1024 * 1024];

        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );
        out.clear();
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&stream)),
            &mut out,
            &v,
        );
        for chunk in payload.chunks(64 * 1024) {
            c.advance(&tagged(b'd', |b| b.extend_from_slice(chunk)), &mut out, &v);
            assert!(!c.is_closed());
            assert!(out.is_empty());
        }
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&(-1i16).to_be_bytes())),
            &mut out,
            &v,
        );
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        assert_eq!(copy_tag(&out), "COPY 1");

        // A field declaring a gigabyte that never arrives holds no memory either: it is
        // a count, and CopyDone inside it is unexpected EOF.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );
        out.clear();
        let mut stuck = binary_header(0, &[]);
        stuck.extend_from_slice(&1i16.to_be_bytes());
        stuck.extend_from_slice(&(1i32 << 30).to_be_bytes());
        c.advance(&tagged(b'd', |b| b.extend_from_slice(&stuck)), &mut out, &v);
        for _ in 0..1000 {
            c.advance(
                &tagged(b'd', |b| b.extend_from_slice(&[0u8; 4096])),
                &mut out,
                &v,
            );
        }
        assert!(!c.is_closed());
        assert!(out.is_empty());
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// The same stall, one packet earlier: the STARTUP length is client-controlled too,
    /// and it is read before any authentication.
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
        assert!(
            !c.is_closed(),
            "a valid startup must not close the connection"
        );
        assert!(!out.is_empty(), "and must be answered");
    }

    /// Once a message is FRAMED its bytes are all present, so the only outcomes are
    /// handled and handled-badly.
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

        assert_eq!(
            used,
            input.len(),
            "a framed message must always be consumed"
        );
        let t = tags(&out);
        assert_eq!(
            t.first(),
            Some(&b'E'),
            "malformed body gets an error: {t:?}"
        );
        assert_eq!(
            t.last(),
            Some(&b'Z'),
            "and the Sync after it is still answered, so the stream is in sync"
        );
        assert!(framed < input.len());
    }

    /// **An injected failure is an error through Parse/Bind/Execute too.**
    ///
    /// The check lived on the simple-query path only, so a NULL written through
    /// the extended protocol answered OK and the rollback the client sends next
    /// counted nothing -- the failure its 1% injection is for, answered as
    /// success.
    #[test]
    fn an_injected_failure_is_refused_through_execute() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "ins");
                cstr(b, "insert into new_order values (1, NULL, 3)");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'1', "Parse is answered first: {out:?}");

        let mut out = Vec::new();
        c.advance(
            &tagged(b'B', |b| {
                cstr(b, "");
                cstr(b, "ins");
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        c.advance(
            &tagged(b'E', |b| {
                cstr(b, "");
                b.extend_from_slice(&0i32.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        c.advance(&tagged(b'S', |_| {}), &mut out, &v);

        let text = String::from_utf8_lossy(&out);
        assert!(
            out.contains(&b'E'),
            "Execute must answer the error, not an OK: {text}"
        );
        assert!(text.contains("23502"), "{text}");
    }

    /// An error in an extended-protocol message discards everything until Sync.
    /// PostgreSQL answers `ErrorResponse`, then reads and drops every message until
    /// `Sync`, then sends ONE `ReadyForQuery`.
    #[test]
    fn an_error_in_an_extended_message_discards_everything_until_sync() {
        let (mut c, v) = connected();

        // Malformed Parse (no NUL anywhere), then a Query, then Sync, in one packet --
        // which is how a pipelining driver sends them.
        let mut input = Vec::new();
        msg(&mut input, b'P', |b| b.extend_from_slice(b"no-nul-here"));
        input.extend(tagged(b'Q', |b| cstr(b, "select 1")));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        assert_eq!(c.advance(&input, &mut out, &v), input.len());
        assert_eq!(
            tags(&out),
            vec![b'E', b'Z'],
            "the Query must be discarded, and Sync must answer exactly once"
        );

        // And after Sync the connection is normal again.
        out.clear();
        c.advance(&tagged(b'Q', |b| cstr(b, "select 1")), &mut out, &v);
        assert_eq!(tags(&out), vec![b'T', b'D', b'C', b'Z']);
    }

    /// Extended messages that arrive while skipping are not answered either -- no
    /// BindComplete for a Bind, no ParseComplete for a Parse.
    /// A driver counts replies against what it sent, and a stray `2` desynchronises it.
    #[test]
    fn extended_messages_are_silent_while_awaiting_sync() {
        let (mut c, v) = connected();

        let mut input = Vec::new();
        msg(&mut input, b'P', |b| b.extend_from_slice(b"no-nul-here"));
        input.extend(tagged(b'P', |b| {
            cstr(b, "s1");
            cstr(b, "select 1");
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
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
        input.extend(tagged(b'H', |_| {}));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
    }

    /// A Terminate while skipping still closes: the client is allowed to give up
    /// without sending the Sync it never got an answer for.
    #[test]
    fn terminate_while_awaiting_sync_closes() {
        let (mut c, v) = connected();

        let mut input = Vec::new();
        msg(&mut input, b'P', |b| b.extend_from_slice(b"no-nul-here"));
        input.extend(tagged(b'X', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert!(c.is_closed());
    }

    /// The simple protocol has no Sync, so an error there ends with `ReadyForQuery` at
    /// once and the next message runs.
    #[test]
    fn a_simple_protocol_error_is_ready_at_once() {
        let (mut c, v) = connected();

        let mut input = tagged(b'c', |_| {});
        input.extend(tagged(b'Q', |b| cstr(b, "select 1")));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z', b'T', b'D', b'C', b'Z']);
    }

    /// The SQLSTATE of the first ErrorResponse in a reply, as the five bytes after the
    /// `C` field tag.
    fn first_sqlstate(out: &[u8]) -> String {
        let mut i = 0;
        while i + 5 <= out.len() {
            let len = i32::from_be_bytes(out[i + 1..i + 5].try_into().unwrap()) as usize;
            if out[i] == b'E' {
                let body = &out[i + 5..i + 1 + len];
                let at = body.iter().position(|&b| b == b'C').expect("a C field") + 1;
                return String::from_utf8_lossy(&body[at..at + 5]).into_owned();
            }
            i += 1 + len;
        }
        panic!("no ErrorResponse in the reply")
    }

    /// A name that was never prepared is an error, not an empty answer.
    /// Bind to an unknown statement answered BindComplete, Execute of an unknown portal
    /// fabricated `SELECT 0`, and Describe of either answered as if it existed.
    #[test]
    fn bind_to_an_unknown_statement_is_an_error() {
        let (mut c, v) = connected();

        let mut input = tagged(b'B', |b| {
            cstr(b, "p1");
            cstr(b, "never-prepared");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'E', |b| {
            cstr(b, "p1");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "no BindComplete, no rows");
        assert_eq!(first_sqlstate(&out), "26000");

        // And the portal was not created by the failed Bind.
        out.clear();
        let mut input = tagged(b'E', |b| {
            cstr(b, "p1");
            b.extend_from_slice(&0i32.to_be_bytes());
        });
        input.extend(tagged(b'S', |_| {}));
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "34000");
    }

    #[test]
    fn describe_of_an_unknown_name_is_an_error() {
        for (kind, code) in [(b'S', "26000"), (b'P', "34000")] {
            let (mut c, v) = connected();

            let mut input = tagged(b'D', |b| {
                b.push(kind);
                cstr(b, "nope");
            });
            input.extend(tagged(b'S', |_| {}));

            let mut out = Vec::new();
            c.advance(&input, &mut out, &v);
            assert_eq!(
                tags(&out),
                vec![b'E', b'Z'],
                "Describe {} must not answer ParameterDescription or NoData",
                kind as char
            );
            assert_eq!(first_sqlstate(&out), code);
        }
    }

    /// Close of a name that does not exist is NOT an error: PostgreSQL answers
    /// CloseComplete, and drivers close speculatively.
    #[test]
    fn close_of_an_unknown_name_is_not_an_error() {
        let (mut c, v) = connected();

        let mut input = tagged(b'C', |b| {
            b.push(b'S');
            cstr(b, "nope");
        });
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'3', b'Z']);
    }

    /// Put a connection into a FAILED transaction: BEGIN, then the one simple-protocol
    /// error this codec produces.
    fn failed(c: &mut Conn, v: &CatalogView) {
        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, v);
        c.advance(&tagged(b'c', |_| {}), &mut out, v);
        assert_eq!(last_ready_status(&out), b'E');
    }

    /// A failed transaction refuses statements, not just reports itself.
    /// The state byte said `E` and every statement still ran: a SELECT got its row, and
    /// a BEGIN flipped the byte back to `T` with no ROLLBACK.
    #[test]
    fn a_failed_transaction_refuses_everything_but_its_end() {
        let (mut c, v) = connected();
        failed(&mut c, &v);

        for sql in [
            "select 1",
            "INSERT INTO t VALUES (1)",
            "BEGIN",
            "CREATE TABLE t (a int)",
        ] {
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);
            assert_eq!(tags(&out), vec![b'E', b'Z'], "{sql} must be refused");
            assert_eq!(first_sqlstate(&out), "25P02", "{sql}");
            assert_eq!(last_ready_status(&out), b'E', "{sql} must stay failed");
        }

        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "ROLLBACK")), &mut out, &v);
        assert_eq!(tags(&out), vec![b'C', b'Z']);
        assert_eq!(last_ready_status(&out), b'I');
    }

    /// COMMIT of a failed transaction rolls it back, and says so: the tag is
    /// `ROLLBACK`, which is how a client learns its COMMIT committed nothing.
    #[test]
    fn commit_of_a_failed_transaction_says_rollback() {
        let (mut c, v) = connected();
        failed(&mut c, &v);

        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "COMMIT")), &mut out, &v);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("ROLLBACK"), "{text}");
        assert!(!text.contains("COMMIT"), "{text}");
        assert_eq!(last_ready_status(&out), b'I');
    }

    /// BEGIN inside an open transaction is a warning in PostgreSQL, not a new block:
    /// the state stays `T`.
    #[test]
    fn begin_inside_a_transaction_keeps_it_open() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
        assert_eq!(last_ready_status(&out), b'T');
    }

    /// The extended protocol is refused the same way, at Parse, Bind and Execute -- and
    /// skips to Sync, since those are extended messages.
    #[test]
    fn a_failed_transaction_refuses_extended_messages_too() {
        let (mut c, v) = connected();

        // A statement prepared BEFORE the failure, so Bind and Execute can be tried on
        // their own.
        let mut out = Vec::new();
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "s1");
                cstr(b, "select 1");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        failed(&mut c, &v);

        // Parse of a new SELECT.
        let mut input = tagged(b'P', |b| {
            cstr(b, "s2");
            cstr(b, "select 2");
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "Parse");
        assert_eq!(first_sqlstate(&out), "25P02");

        // Bind of the old one.
        let mut input = tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "s1");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "Bind");
        assert_eq!(first_sqlstate(&out), "25P02");
        assert_eq!(last_ready_status(&out), b'E');

        // ROLLBACK through the extended protocol ends it.
        let mut input = tagged(b'P', |b| {
            cstr(b, "");
            cstr(b, "ROLLBACK");
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
        input.extend(tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'1', b'2', b'C', b'Z'], "ROLLBACK runs");
        assert_eq!(last_ready_status(&out), b'I');
    }

    /// Parse/Bind/Execute/Sync for a statement, in one packet, the way a driver sends
    /// them.
    fn extended(sql: &str) -> Vec<u8> {
        let mut input = tagged(b'P', |b| {
            cstr(b, "");
            cstr(b, sql);
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
        input.extend(tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));
        input
    }

    /// A COPY entered through the extended protocol ends at the client's Sync, not at
    /// CopyDone.
    #[test]
    fn an_extended_copy_ends_at_sync() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&extended("COPY t FROM STDIN"), &mut out, &v);
        assert_eq!(
            tags(&out),
            vec![b'1', b'2', b'G'],
            "the Sync sent with Execute is swallowed while COPY is open"
        );

        let mut input = tagged(b'd', |b| b.extend_from_slice(b"1\n2\n"));
        input.extend(tagged(b'c', |_| {}));
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'C', b'Z'], "one ReadyForQuery, from Sync");
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 2"), "{text}");

        // And the next query is answered normally: the stream is in sync.
        out.clear();
        c.advance(&extended("select 1"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'1', b'2', b'D', b'C', b'Z']);
    }

    /// The simple protocol has no Sync, so there CopyDone still ends with ReadyForQuery
    /// -- and a Flush or Sync a confused client sends during either kind is ignored, as
    /// PostgreSQL ignores them.
    #[test]
    fn a_simple_copy_still_ends_at_copy_done() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN")),
            &mut out,
            &v,
        );
        assert_eq!(tags(&out), vec![b'G']);

        let mut input = tagged(b'H', |_| {});
        input.extend(tagged(b'S', |_| {}));
        input.extend(tagged(b'd', |b| b.extend_from_slice(b"1\n")));
        input.extend(tagged(b'c', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'C', b'Z']);
    }

    /// An ordinary message during COPY is a protocol violation, not a query to run.
    /// A Query executed mid-COPY answered rows into a stream the client was reading as
    /// COPY replies.
    #[test]
    fn a_query_during_copy_is_refused_not_run() {
        // Extended: the error skips to Sync.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&extended("COPY t FROM STDIN"), &mut out, &v);

        let mut input = tagged(b'Q', |b| cstr(b, "select 1"));
        input.extend(tagged(b'd', |b| b.extend_from_slice(b"late\n")));
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "no rows, one ready");
        assert_eq!(first_sqlstate(&out), "08P01");

        // Simple: the error is ready at once, and the COPY is over.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN")),
            &mut out,
            &v,
        );
        out.clear();
        c.advance(&tagged(b'Q', |b| cstr(b, "select 1")), &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        out.clear();
        c.advance(&tagged(b'Q', |b| cstr(b, "select 1")), &mut out, &v);
        assert_eq!(
            tags(&out),
            vec![b'T', b'D', b'C', b'Z'],
            "COPY is no longer open"
        );
    }

    /// CopyFail in an extended COPY is an extended error: ErrorResponse, then nothing
    /// until the client's Sync.
    #[test]
    fn copy_fail_in_an_extended_copy_waits_for_sync() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&extended("COPY t FROM STDIN"), &mut out, &v);

        let mut input = tagged(b'f', |b| cstr(b, "gave up"));
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "57014");
    }

    /// A binary COPY header: signature, flags, and an extension of `ext` bytes.
    fn binary_header(flags: i32, ext: &[u8]) -> Vec<u8> {
        let mut h = Vec::new();
        h.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        h.extend_from_slice(&flags.to_be_bytes());
        h.extend_from_slice(&(ext.len() as i32).to_be_bytes());
        h.extend_from_slice(ext);
        h
    }

    /// One binary tuple of the given int8 values.
    fn binary_tuple(values: &[i64]) -> Vec<u8> {
        let mut t = Vec::new();
        t.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for v in values {
            t.extend_from_slice(&8i32.to_be_bytes());
            t.extend_from_slice(&v.to_be_bytes());
        }
        t
    }

    /// Start a binary COPY and feed it `chunks`, then CopyDone; the reply to the whole
    /// exchange after CopyInResponse.
    fn binary_copy(chunks: &[&[u8]]) -> (Conn, Vec<u8>) {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );
        assert_eq!(out[0], b'G');
        out.clear();
        for chunk in chunks {
            c.advance(&tagged(b'd', |b| b.extend_from_slice(chunk)), &mut out, &v);
        }
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        (c, out)
    }

    /// The header is not 19 bytes; it is 19 bytes plus the extension it declares.
    /// Bytes 15..19 carry the extension length, and a reader must skip that many more
    /// before the first tuple.
    #[test]
    fn binary_copy_skips_the_header_extension() {
        let mut stream = binary_header(0, &[0xde, 0xad, 0xbe, 0xef]);
        stream.extend(binary_tuple(&[1]));
        stream.extend_from_slice(&(-1i16).to_be_bytes());

        let (_, out) = binary_copy(&[&stream]);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 1"), "{text}");

        // And an extension split across messages is waited for, not parsed.
        let (_, out) = binary_copy(&[&stream[..17], &stream[17..21], &stream[21..]]);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 1"), "split: {text}");
    }

    /// A stream that does not start with the signature is not a binary COPY, and
    /// PostgreSQL says so (22P04) instead of counting whatever it is.
    #[test]
    fn binary_copy_rejects_a_bad_signature() {
        let mut stream = b"PGCOPY\n\xff\r\n\0".to_vec();
        stream[0] = b'X';
        stream.extend_from_slice(&0i32.to_be_bytes());
        stream.extend_from_slice(&0i32.to_be_bytes());
        stream.extend(binary_tuple(&[1]));

        let (c, out) = binary_copy(&[&stream]);
        assert_eq!(
            tags(&out),
            vec![b'E', b'Z', b'E', b'Z'],
            "the COPY ended at the error; the CopyDone after it is outside a COPY"
        );
        assert_eq!(first_sqlstate(&out), "22P04");
        assert!(!c.is_closed());
    }

    /// The flags word reserves bits 16..31 as critical: a reader that does not
    /// understand one must refuse the file.
    /// None are defined, so any set bit there is a refusal.
    #[test]
    fn binary_copy_rejects_critical_flags() {
        let mut stream = binary_header(1 << 20, &[]);
        stream.extend(binary_tuple(&[1]));

        let (_, out) = binary_copy(&[&stream]);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// Only -1 is special.
    /// A field count of -1 is the trailer and a field length of -1 is NULL; every other
    /// negative value is a malformed stream.
    #[test]
    fn binary_copy_rejects_negative_values_other_than_minus_one() {
        // Field length -2.
        let mut stream = binary_header(0, &[]);
        stream.extend_from_slice(&1i16.to_be_bytes());
        stream.extend_from_slice(&(-2i32).to_be_bytes());
        stream.extend_from_slice(&(-1i16).to_be_bytes());

        let (_, out) = binary_copy(&[&stream]);
        assert_eq!(first_sqlstate(&out), "22P04", "length -2");
        let text = String::from_utf8_lossy(&out);
        assert!(!text.contains("COPY 1"), "{text}");

        // Field count -2.
        let mut stream = binary_header(0, &[]);
        stream.extend_from_slice(&(-2i16).to_be_bytes());

        let (_, out) = binary_copy(&[&stream]);
        assert_eq!(first_sqlstate(&out), "22P04", "count -2");
    }

    /// CopyDone is checked against the framing, not taken on trust.
    #[test]
    fn binary_copy_done_with_a_partial_tuple_is_an_error() {
        let mut stream = binary_header(0, &[]);
        stream.extend(binary_tuple(&[1]));
        let cut = stream.len() - 3;

        let (_, out) = binary_copy(&[&stream[..cut]]);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "no CommandComplete");
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// Bytes after the trailer are `received copy data after EOF marker`, whether they
    /// arrive in the same message or a later one.
    #[test]
    fn binary_copy_data_after_the_trailer_is_an_error() {
        let mut stream = binary_header(0, &[]);
        stream.extend(binary_tuple(&[1]));
        stream.extend_from_slice(&(-1i16).to_be_bytes());
        let trailer_end = stream.len();
        stream.extend(binary_tuple(&[2]));

        let (_, out) = binary_copy(&[&stream]);
        assert_eq!(first_sqlstate(&out), "22P04", "same message");

        let (_, out) = binary_copy(&[&stream[..trailer_end], &stream[trailer_end..]]);
        assert_eq!(first_sqlstate(&out), "22P04", "later message");
    }

    /// A stream with no header at all is not a binary COPY: PostgreSQL's
    /// `ReceiveCopyBinaryHeader` fails to read the signature and says so.
    #[test]
    fn binary_copy_done_with_no_header_is_an_error() {
        let (_, out) = binary_copy(&[]);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// The trailer says the writer is done, and PostgreSQL's reader treats a clean
    /// EOF at a tuple boundary the same way.
    #[test]
    fn binary_copy_without_a_trailer_is_still_complete() {
        let mut stream = binary_header(0, &[]);
        stream.extend(binary_tuple(&[1]));

        let (_, out) = binary_copy(&[&stream]);
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("COPY 1"), "{text}");
    }

    /// The parameter OIDs of the first ParameterDescription in a reply.
    fn parameter_description(out: &[u8]) -> Vec<u32> {
        let mut i = 0;
        while i + 5 <= out.len() {
            let len = i32::from_be_bytes(out[i + 1..i + 5].try_into().unwrap()) as usize;
            if out[i] == b't' {
                let body = &out[i + 5..i + 1 + len];
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                return (0..n)
                    .map(|k| u32::from_be_bytes(body[2 + 4 * k..6 + 4 * k].try_into().unwrap()))
                    .collect();
            }
            i += 1 + len;
        }
        panic!("no ParameterDescription in the reply")
    }

    /// Parse the unnamed statement with declared parameter types, Describe it, Sync.
    /// Unnamed, so one connection can do it repeatedly: a named statement cannot be
    /// re-parsed without a Close.
    fn parse_and_describe(c: &mut Conn, v: &CatalogView, sql: &str, oids: &[u32]) -> Vec<u8> {
        let mut input = tagged(b'P', |b| {
            cstr(b, "");
            cstr(b, sql);
            b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
            for oid in oids {
                b.extend_from_slice(&oid.to_be_bytes());
            }
        });
        input.extend(tagged(b'D', |b| {
            b.push(b'S');
            cstr(b, "");
        }));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, v);
        out
    }

    /// ParameterDescription reports the statement's parameters.
    /// It reported zero, always: Parse read the statement name and the SQL and dropped
    /// the declared types on the floor.
    #[test]
    fn declared_parameter_types_are_reported() {
        let (mut c, v) = connected();
        let out = parse_and_describe(&mut c, &v, "select $1", &[20]);
        assert_eq!(parameter_description(&out), vec![20]);

        let out = parse_and_describe(&mut c, &v, "select $1, $2", &[20, 25]);
        assert_eq!(parameter_description(&out), vec![20, 25]);
    }

    /// An undeclared parameter with a cast takes the cast's type; without one it is
    /// reported as unknown (OID 0).
    #[test]
    fn undeclared_parameters_are_inferred_from_casts_or_reported_unknown() {
        let (mut c, v) = connected();

        let out = parse_and_describe(&mut c, &v, "select $1::bigint", &[]);
        assert_eq!(parameter_description(&out), vec![20]);

        let out = parse_and_describe(&mut c, &v, "UPDATE t SET v = $1 WHERE id = $2", &[]);
        assert_eq!(parameter_description(&out), vec![0, 0]);

        // A declared 0 means "infer", and a declared type beats a cast.
        let out = parse_and_describe(&mut c, &v, "select $1::int4, $2::int4", &[0, 20]);
        assert_eq!(parameter_description(&out), vec![23, 20]);

        // Placeholders are counted by the highest number, not by occurrences, and a
        // `$1` inside a string literal is not a placeholder.
        let out = parse_and_describe(&mut c, &v, "select $2, $2, '$3'", &[]);
        assert_eq!(parameter_description(&out), vec![0, 0]);

        // No parameters is still no parameters.
        let out = parse_and_describe(&mut c, &v, "select 1", &[]);
        assert_eq!(parameter_description(&out), Vec::<u32>::new());
    }

    /// A Parse whose declared count outruns its body is malformed, not "zero
    /// parameters".
    #[test]
    fn a_parse_with_a_truncated_type_list_is_malformed() {
        let (mut c, v) = connected();
        let mut input = tagged(b'P', |b| {
            cstr(b, "s");
            cstr(b, "select $1");
            b.extend_from_slice(&3i16.to_be_bytes());
            b.extend_from_slice(&20u32.to_be_bytes());
        });
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");
    }

    /// The body of the first message tagged `want` in a reply.
    fn first_body(out: &[u8], want: u8) -> Option<&[u8]> {
        let mut i = 0;
        while i + 5 <= out.len() {
            let len = i32::from_be_bytes(out[i + 1..i + 5].try_into().unwrap()) as usize;
            if out[i] == want {
                return Some(&out[i + 5..i + 1 + len]);
            }
            i += 1 + len;
        }
        None
    }

    /// The format code of each column in the first RowDescription.
    fn row_description_formats(out: &[u8]) -> Vec<i16> {
        let body = first_body(out, b'T').expect("a RowDescription");
        let n = i16::from_be_bytes([body[0], body[1]]) as usize;
        let mut at = 2;
        let mut formats = Vec::new();
        for _ in 0..n {
            let end = body[at..].iter().position(|&b| b == 0).unwrap();
            at += end + 1 + 4 + 2 + 4 + 2 + 4;
            formats.push(i16::from_be_bytes([body[at], body[at + 1]]));
            at += 2;
        }
        formats
    }

    /// Each field of the first DataRow, `None` for NULL.
    fn data_row_fields(out: &[u8]) -> Vec<Option<Vec<u8>>> {
        let body = first_body(out, b'D').expect("a DataRow");
        let n = i16::from_be_bytes([body[0], body[1]]) as usize;
        let mut at = 2;
        let mut fields = Vec::new();
        for _ in 0..n {
            let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
            at += 4;
            if len < 0 {
                fields.push(None);
            } else {
                fields.push(Some(body[at..at + len as usize].to_vec()));
                at += len as usize;
            }
        }
        fields
    }

    /// Parse `sql`, Bind with the given result formats, Describe the portal, Execute,
    /// Sync -- the shape pgx and rust-postgres send for a query.
    fn query_with_formats(c: &mut Conn, v: &CatalogView, sql: &str, formats: &[i16]) -> Vec<u8> {
        let mut input = tagged(b'P', |b| {
            cstr(b, "");
            cstr(b, sql);
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "");
            b.extend_from_slice(&0i16.to_be_bytes()); // parameter formats
            b.extend_from_slice(&0i16.to_be_bytes()); // parameters
            b.extend_from_slice(&(formats.len() as i16).to_be_bytes());
            for f in formats {
                b.extend_from_slice(&f.to_be_bytes());
            }
        }));
        input.extend(tagged(b'D', |b| {
            b.push(b'P');
            cstr(b, "");
        }));
        input.extend(tagged(b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        }));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, v);
        assert_eq!(tags(&out).last(), Some(&b'Z'), "{sql}: {:?}", tags(&out));
        out
    }

    /// The result format a client asks for is the one it gets.
    /// Bind read two names and ignored the rest, so a portal bound with result format 1
    /// was described as text and answered with the one ASCII byte `1`.
    #[test]
    fn a_binary_result_format_is_honoured() {
        let (mut c, v) = connected();
        let out = query_with_formats(&mut c, &v, "select 1", &[1]);

        assert_eq!(row_description_formats(&out), vec![1]);
        assert_eq!(
            data_row_fields(&out),
            vec![Some(1i64.to_be_bytes().to_vec())],
            "a binary int8 is eight bytes"
        );

        // Text is still text.
        let out = query_with_formats(&mut c, &v, "select 1", &[0]);
        assert_eq!(row_description_formats(&out), vec![0]);
        assert_eq!(data_row_fields(&out), vec![Some(b"1".to_vec())]);

        // And no formats at all means text, as the protocol says.
        let out = query_with_formats(&mut c, &v, "select 1", &[]);
        assert_eq!(data_row_fields(&out), vec![Some(b"1".to_vec())]);
    }

    /// A single format code applies to every column; a list is per column.
    #[test]
    fn one_result_format_applies_to_every_column() {
        let (mut c, v) = connected();

        let out = query_with_formats(&mut c, &v, "select 1, 2, 3", &[1]);
        assert_eq!(row_description_formats(&out), vec![1, 1, 1]);
        assert!(data_row_fields(&out)
            .iter()
            .all(|f| f.as_ref().unwrap().len() == 8));

        // The values are the statement's, not a stub: `select 1, 2` answers 1 and 2,
        // each in the format its column was asked for.
        let out = query_with_formats(&mut c, &v, "select 1, 2", &[0, 1]);
        assert_eq!(row_description_formats(&out), vec![0, 1]);
        let fields = data_row_fields(&out);
        assert_eq!(fields[0], Some(b"1".to_vec()));
        assert_eq!(fields[1], Some(2i64.to_be_bytes().to_vec()));
    }

    /// A select from a table the catalog knows describes the client's own types, in the
    /// format the client asked for -- and answers no rows, which is the rule for a
    /// metadata select and unchanged here.
    #[test]
    fn known_columns_are_described_with_their_types_and_the_asked_format() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| {
                cstr(b, "CREATE TABLE t (a INT, b TEXT, c BOOLEAN)")
            }),
            &mut out,
            &v,
        );

        let out = query_with_formats(&mut c, &v, "select a, b, c from t", &[1, 0, 1]);
        assert_eq!(row_description_formats(&out), vec![1, 0, 1]);
        assert!(
            first_body(&out, b'D').is_none(),
            "a metadata select answers empty"
        );
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("SELECT 0"), "{text}");
    }

    /// Describe of a STATEMENT has no portal and so no formats: PostgreSQL reports text
    /// there, whatever a later Bind will ask for.
    #[test]
    fn describe_statement_reports_text_formats() {
        let (mut c, v) = connected();
        let out = parse_and_describe(&mut c, &v, "select 1", &[]);
        assert_eq!(row_description_formats(&out), vec![0]);
    }

    /// Bind is parsed to its end and its counts are checked: the parameter count must
    /// match the statement, and there are 0, 1 or n parameter format codes.
    /// Anything else is 08P01, as it is in PostgreSQL.
    #[test]
    fn bind_counts_are_validated() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "s");
                cstr(b, "select $1::int8");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );

        let bind = |pformats: &[i16], params: &[Option<&[u8]>]| {
            let mut input = tagged(b'B', |b| {
                cstr(b, "");
                cstr(b, "s");
                b.extend_from_slice(&(pformats.len() as i16).to_be_bytes());
                for f in pformats {
                    b.extend_from_slice(&f.to_be_bytes());
                }
                b.extend_from_slice(&(params.len() as i16).to_be_bytes());
                for p in params {
                    match p {
                        Some(bytes) => {
                            b.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            b.extend_from_slice(bytes);
                        }
                        None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
                b.extend_from_slice(&0i16.to_be_bytes());
            });
            input.extend(tagged(b'S', |_| {}));
            input
        };

        // Right: one parameter, one value (binary int8 here, a NULL below).
        out.clear();
        c.advance(&bind(&[1], &[Some(&7i64.to_be_bytes())]), &mut out, &v);
        assert_eq!(tags(&out), vec![b'2', b'Z']);
        out.clear();
        c.advance(&bind(&[], &[None]), &mut out, &v);
        assert_eq!(tags(&out), vec![b'2', b'Z']);

        // Wrong: no parameters for a statement with one.
        out.clear();
        c.advance(&bind(&[], &[]), &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");

        // Wrong: two parameter formats for one parameter.
        out.clear();
        c.advance(&bind(&[0, 0], &[Some(b"7")]), &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");
    }

    /// Execute a named portal with a row limit, then Sync.
    fn execute(c: &mut Conn, v: &CatalogView, portal: &str, max_rows: i32) -> Vec<u8> {
        let mut input = tagged(b'E', |b| {
            cstr(b, portal);
            b.extend_from_slice(&max_rows.to_be_bytes());
        });
        input.extend(tagged(b'S', |_| {}));
        let mut out = Vec::new();
        c.advance(&input, &mut out, v);
        out
    }

    /// Parse `sql` and bind it to portal `p1`, text formats.
    fn bind_p1(c: &mut Conn, v: &CatalogView, sql: &str) {
        let mut input = tagged(b'P', |b| {
            cstr(b, "s1");
            cstr(b, sql);
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'B', |b| {
            cstr(b, "p1");
            cstr(b, "s1");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        }));
        let mut out = Vec::new();
        c.advance(&input, &mut out, v);
        assert_eq!(tags(&out), vec![b'1', b'2']);
    }

    /// A portal is a cursor, and Execute reads `max_rows`.
    /// Execute parsed only the portal name, so a limit was ignored and a second Execute
    /// of the same portal replayed the row.
    #[test]
    fn a_portal_suspends_at_max_rows_and_does_not_replay() {
        let (mut c, v) = connected();
        bind_p1(&mut c, &v, "select 1");

        let out = execute(&mut c, &v, "p1", 1);
        assert_eq!(
            tags(&out),
            vec![b'D', b's', b'Z'],
            "the row, then PortalSuspended"
        );

        let out = execute(&mut c, &v, "p1", 1);
        assert_eq!(tags(&out), vec![b'C', b'Z'], "no row the second time");
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("SELECT 0"), "{text}");
    }

    /// No limit reads the portal to its end in one go, and a later Execute of it finds
    /// nothing more.
    #[test]
    fn an_unlimited_execute_completes_the_portal() {
        let (mut c, v) = connected();
        bind_p1(&mut c, &v, "select 1");

        let out = execute(&mut c, &v, "p1", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z']);
        assert!(String::from_utf8_lossy(&out).contains("SELECT 1"));

        let out = execute(&mut c, &v, "p1", 0);
        assert_eq!(tags(&out), vec![b'C', b'Z']);
        assert!(String::from_utf8_lossy(&out).contains("SELECT 0"));

        // Closed and bound again, it is a new portal, with its row.
        // (A named portal cannot be rebound in place: that is 42P03.)
        let mut out = Vec::new();
        c.advance(
            &tagged(b'C', |b| {
                b.push(b'P');
                cstr(b, "p1");
            }),
            &mut out,
            &v,
        );
        c.advance(
            &tagged(b'B', |b| {
                cstr(b, "p1");
                cstr(b, "s1");
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        assert_eq!(tags(&out), vec![b'3', b'2']);
        let out = execute(&mut c, &v, "p1", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z']);
    }

    /// A named object is not silently replaced.
    /// Parsing named statement `s` twice answered ParseComplete twice; PostgreSQL
    /// answers 42P05 the second time.
    #[test]
    fn named_statements_and_portals_are_not_overwritten() {
        let (mut c, v) = connected();
        let parse = |name: &str, sql: &str| {
            let mut input = tagged(b'P', |b| {
                cstr(b, name);
                cstr(b, sql);
                b.extend_from_slice(&0i16.to_be_bytes());
            });
            input.extend(tagged(b'S', |_| {}));
            input
        };
        let bind = |portal: &str, stmt: &str| {
            let mut input = tagged(b'B', |b| {
                cstr(b, portal);
                cstr(b, stmt);
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
            });
            input.extend(tagged(b'S', |_| {}));
            input
        };
        let close = |kind: u8, name: &str| {
            let mut input = tagged(b'C', |b| {
                b.push(kind);
                cstr(b, name);
            });
            input.extend(tagged(b'S', |_| {}));
            input
        };

        let mut out = Vec::new();
        c.advance(&parse("s", "select 1"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'1', b'Z']);
        out.clear();
        c.advance(&parse("s", "select 2"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "a second Parse of s");
        assert_eq!(first_sqlstate(&out), "42P05");

        out.clear();
        c.advance(&bind("p", "s"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'2', b'Z']);
        out.clear();
        c.advance(&bind("p", "s"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "a second Bind of p");
        assert_eq!(first_sqlstate(&out), "42P03");

        // Closed, the names are free again.
        out.clear();
        c.advance(&close(b'S', "s"), &mut out, &v);
        c.advance(&parse("s", "select 3"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'3', b'Z', b'1', b'Z']);
        out.clear();
        c.advance(&close(b'P', "p"), &mut out, &v);
        c.advance(&bind("p", "s"), &mut out, &v);
        assert_eq!(tags(&out), vec![b'3', b'Z', b'2', b'Z']);

        // The unnamed statement and portal are replaced freely: every driver reuses
        // them for every query.
        out.clear();
        c.advance(&parse("", "select 1"), &mut out, &v);
        c.advance(&parse("", "select 2"), &mut out, &v);
        c.advance(&bind("", ""), &mut out, &v);
        c.advance(&bind("", ""), &mut out, &v);
        assert_eq!(
            tags(&out),
            vec![b'1', b'Z', b'1', b'Z', b'2', b'Z', b'2', b'Z']
        );
    }

    /// An Execute without its `max_rows` is malformed, not "no limit".
    #[test]
    fn an_execute_without_max_rows_is_malformed() {
        let (mut c, v) = connected();
        bind_p1(&mut c, &v, "select 1");

        let mut input = tagged(b'E', |b| cstr(b, "p1"));
        input.extend(tagged(b'S', |_| {}));
        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");
    }

    /// An empty query is EmptyQueryResponse, not `SELECT 0`.
    #[test]
    fn an_empty_query_is_an_empty_query_response() {
        for sql in ["", ";", " ; ", "\n;;\t"] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);
            assert_eq!(tags(&out), vec![b'I', b'Z'], "{sql:?}");
        }
    }

    /// The same through the extended protocol: Describe says NoData, and Execute
    /// answers EmptyQueryResponse in place of CommandComplete.
    #[test]
    fn an_empty_prepared_statement_executes_to_an_empty_query_response() {
        let (mut c, v) = connected();
        let out = parse_and_describe(&mut c, &v, "", &[]);
        assert_eq!(tags(&out), vec![b'1', b't', b'n', b'Z']);

        let out = query_with_formats(&mut c, &v, ";", &[]);
        assert_eq!(tags(&out), vec![b'1', b'2', b'n', b'I', b'Z']);
    }

    /// An empty query is allowed in a failed transaction -- there is nothing in it to
    /// refuse -- and leaves the state as it found it.
    #[test]
    fn an_empty_query_is_allowed_in_a_failed_transaction() {
        let (mut c, v) = connected();
        failed(&mut c, &v);

        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "")), &mut out, &v);
        assert_eq!(tags(&out), vec![b'I', b'Z']);
        assert_eq!(last_ready_status(&out), b'E');
    }

    /// A placeholder PostgreSQL would refuse is refused at Parse, with its SQLSTATE,
    /// and skips to Sync like any other Parse error.
    #[test]
    fn an_invalid_placeholder_is_a_parse_error() {
        for (sql, code) in [("select $0", "42P02"), ("select $5000000", "54000")] {
            let (mut c, v) = connected();
            let mut input = tagged(b'P', |b| {
                cstr(b, "");
                cstr(b, sql);
                b.extend_from_slice(&0i16.to_be_bytes());
            });
            input.extend(tagged(b'S', |_| {}));

            let mut out = Vec::new();
            c.advance(&input, &mut out, &v);
            assert_eq!(tags(&out), vec![b'E', b'Z'], "{sql}");
            assert_eq!(first_sqlstate(&out), code, "{sql}");
        }
    }

    /// A header extension is skipped like a field, not waited for.
    #[test]
    fn a_binary_copy_header_extension_is_skipped_not_buffered() {
        let ext = vec![0xabu8; 100 * 1024];
        let mut stream = binary_header(0, &ext);
        stream.extend(binary_tuple(&[1]));
        let (_, out) = binary_copy(&[&stream[..50_000], &stream[50_000..]]);
        assert_eq!(copy_tag(&out), "COPY 1");

        // A 2 GB extension is a count too.
        // Nothing arrives after the header here, so CopyDone inside it is unexpected
        // EOF, and the connection was never asked to hold anything.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );
        out.clear();
        let mut header = b"PGCOPY\n\xff\r\n\0".to_vec();
        header.extend_from_slice(&0i32.to_be_bytes());
        header.extend_from_slice(&i32::MAX.to_be_bytes());
        c.advance(
            &tagged(b'd', |b| b.extend_from_slice(&header)),
            &mut out,
            &v,
        );
        for _ in 0..1000 {
            c.advance(
                &tagged(b'd', |b| b.extend_from_slice(&[0u8; 4096])),
                &mut out,
                &v,
            );
        }
        assert!(out.is_empty());
        assert!(!c.is_closed());
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// Start a COPY with `options` after STDIN, feed `chunks`, CopyDone; the reply
    /// after CopyInResponse.
    fn text_copy(options: &str, chunks: &[&[u8]]) -> (Conn, Vec<u8>) {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        let sql = format!("COPY t FROM STDIN {options}");
        c.advance(&tagged(b'Q', |b| cstr(b, &sql)), &mut out, &v);
        assert_eq!(out[0], b'G', "{sql}");
        out.clear();
        for chunk in chunks {
            c.advance(&tagged(b'd', |b| b.extend_from_slice(chunk)), &mut out, &v);
        }
        c.advance(&tagged(b'c', |_| {}), &mut out, &v);
        (c, out)
    }

    fn copy_tag(out: &[u8]) -> String {
        let body = first_body(out, b'C').expect("a CommandComplete");
        String::from_utf8_lossy(&body[..body.len() - 1]).into_owned()
    }

    /// Text rows are lines, and the last line need not be terminated.
    /// Newlines were counted, so `a\nb\nc` answered `COPY 2` where PostgreSQL reads the
    /// final unterminated line as a row and answers `COPY 3`.
    #[test]
    fn a_text_copy_counts_an_unterminated_final_line() {
        assert_eq!(copy_tag(&text_copy("", &[b"a\nb\nc"]).1), "COPY 3");
        assert_eq!(copy_tag(&text_copy("", &[b"a\nb\n"]).1), "COPY 2");
        assert_eq!(copy_tag(&text_copy("", &[b"a\nb", b"\nc"]).1), "COPY 3");
        assert_eq!(copy_tag(&text_copy("", &[b"a\r\nb\r\n"]).1), "COPY 2");
        assert_eq!(copy_tag(&text_copy("", &[]).1), "COPY 0");
        assert_eq!(copy_tag(&text_copy("", &[b"\n"]).1), "COPY 1");
    }

    /// In text format a backslash escapes the byte after it, including a newline, which
    /// is then data and not a row end; a doubled backslash is one backslash and the
    /// newline after it ends the row.
    #[test]
    fn a_text_copy_honours_escapes_and_the_end_marker() {
        assert_eq!(copy_tag(&text_copy("", &[b"a\\\nb\n"]).1), "COPY 1");
        assert_eq!(copy_tag(&text_copy("", &[b"a\\", b"\nb\n"]).1), "COPY 1");
        assert_eq!(copy_tag(&text_copy("", &[b"a\\\\\nb\n"]).1), "COPY 2");
        assert_eq!(
            copy_tag(&text_copy("", &[b"a\n\\.\nignored\n"]).1),
            "COPY 1"
        );
        assert_eq!(
            copy_tag(&text_copy("", &[b"a\n\\.", b"\nignored\n"]).1),
            "COPY 1"
        );
        assert_eq!(copy_tag(&text_copy("", &[b"a\n\\.x\n"]).1), "COPY 2");
    }

    /// CSV rows are lines outside quotes.
    /// A quoted newline is field data; `1,"hello\nworld"\n2,x\n` answered `COPY 3`
    /// where PostgreSQL answers `COPY 2`.
    #[test]
    fn a_csv_copy_counts_lines_outside_quotes() {
        let csv = b"1,\"hello\nworld\"\n2,x\n";
        assert_eq!(copy_tag(&text_copy("CSV", &[csv]).1), "COPY 2");
        assert_eq!(
            copy_tag(&text_copy("WITH (FORMAT csv)", &[csv]).1),
            "COPY 2"
        );
        assert_eq!(
            copy_tag(&text_copy("CSV", &[&csv[..9], &csv[9..]]).1),
            "COPY 2"
        );
        assert_eq!(
            copy_tag(&text_copy("CSV", &[b"1,\"a\"\"b\nc\"\n"]).1),
            "COPY 1"
        );
        assert_eq!(
            copy_tag(&text_copy("CSV", &[b"1,\"a\"\"\"\n2\n3"]).1),
            "COPY 3"
        );
        assert_eq!(copy_tag(&text_copy("CSV", &[b"a\n\\.\nb\n"]).1), "COPY 1");

        let (_, out) = text_copy("CSV", &[b"1,\"open\n2\n"]);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// Two more Bind shapes PostgreSQL rejects.
    #[test]
    fn bind_result_format_count_must_match_the_columns() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "one");
                cstr(b, "select 1");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "two");
                cstr(b, "select 1, 2");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "none");
                cstr(b, "INSERT INTO t VALUES (1)");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );

        let bind = |stmt: &str, formats: &[i16]| {
            let mut input = tagged(b'B', |b| {
                cstr(b, "");
                cstr(b, stmt);
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&(formats.len() as i16).to_be_bytes());
                for f in formats {
                    b.extend_from_slice(&f.to_be_bytes());
                }
            });
            input.extend(tagged(b'S', |_| {}));
            input
        };

        for (stmt, formats, ok) in [
            ("one", &[][..], true),
            ("one", &[1][..], true),
            ("one", &[0, 1][..], false),
            ("two", &[0, 1][..], true),
            ("two", &[0, 1, 0][..], false),
            ("none", &[1][..], true),
            ("none", &[0, 0][..], false),
        ] {
            out.clear();
            c.advance(&bind(stmt, formats), &mut out, &v);
            if ok {
                assert_eq!(tags(&out), vec![b'2', b'Z'], "{stmt} {formats:?}");
            } else {
                assert_eq!(tags(&out), vec![b'E', b'Z'], "{stmt} {formats:?}");
                assert_eq!(first_sqlstate(&out), "08P01", "{stmt} {formats:?}");
            }
        }
    }

    #[test]
    fn bind_parameter_length_below_minus_one_is_malformed() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'P', |b| {
                cstr(b, "s");
                cstr(b, "select $1::int8");
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            &v,
        );

        let mut input = tagged(b'B', |b| {
            cstr(b, "");
            cstr(b, "s");
            b.extend_from_slice(&0i16.to_be_bytes());
            b.extend_from_slice(&1i16.to_be_bytes());
            b.extend_from_slice(&(-2i32).to_be_bytes());
            b.extend_from_slice(&0i16.to_be_bytes());
        });
        input.extend(tagged(b'S', |_| {}));
        out.clear();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");
    }

    /// A comment glued to the direction must still enter COPY, or the client's rows
    /// land outside one.
    #[test]
    fn a_comment_glued_to_a_token_still_enters_copy() {
        for sql in [
            "COPY t/*x*/FROM STDIN",
            "COPY t FROM/*x*/STDIN",
            "COPY/*x*/t FROM STDIN",
        ] {
            let (mut c, v) = connected();
            let mut out = Vec::new();
            c.advance(&tagged(b'Q', |b| cstr(b, sql)), &mut out, &v);
            assert_eq!(tags(&out), vec![b'G'], "{sql:?}");

            out.clear();
            c.advance(&tagged(b'd', |b| b.extend_from_slice(b"1\n")), &mut out, &v);
            c.advance(&tagged(b'c', |_| {}), &mut out, &v);
            assert_eq!(copy_tag(&out), "COPY 1", "{sql:?}");
        }
    }

    /// A portal keeps the plan it was bound to.
    #[test]
    fn a_portal_keeps_its_plan_when_the_statement_name_is_reused() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        let parse = |name: &str, sql: &str| {
            tagged(b'P', |b| {
                cstr(b, name);
                cstr(b, sql);
                b.extend_from_slice(&0i16.to_be_bytes());
            })
        };
        let close_s = |name: &str| {
            tagged(b'C', |b| {
                b.push(b'S');
                cstr(b, name);
            })
        };

        c.advance(&parse("s", "select 1"), &mut out, &v);
        bind_only(&mut c, &v, "p", "s");
        c.advance(&close_s("s"), &mut out, &v);
        c.advance(&parse("s", "BEGIN"), &mut out, &v);

        let out = execute(&mut c, &v, "p", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z'], "the row, not BEGIN");
        assert_eq!(copy_tag(&out), "SELECT 1");
        assert_eq!(last_ready_status(&out), b'I');

        // And a portal survives the Close of its statement on its own: the plan is the
        // portal's, and the name was only how it was found.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&parse("s", "select 1"), &mut out, &v);
        bind_only(&mut c, &v, "p", "s");
        c.advance(&close_s("s"), &mut out, &v);
        let out = execute(&mut c, &v, "p", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z']);

        // Reusing the name for a COPY does not turn the old portal into one.
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&parse("s", "select 1"), &mut out, &v);
        bind_only(&mut c, &v, "p", "s");
        c.advance(&close_s("s"), &mut out, &v);
        c.advance(&parse("s", "COPY t FROM STDIN"), &mut out, &v);
        let out = execute(&mut c, &v, "p", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z']);
    }

    /// Bind `portal` to `stmt`, text formats, no Sync.
    fn bind_only(c: &mut Conn, v: &CatalogView, portal: &str, stmt: &str) {
        let mut out = Vec::new();
        c.advance(
            &tagged(b'B', |b| {
                cstr(b, portal);
                cstr(b, stmt);
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
                b.extend_from_slice(&0i16.to_be_bytes());
            }),
            &mut out,
            v,
        );
        assert_eq!(tags(&out), vec![b'2'], "{portal} -> {stmt}");
    }

    /// A startup packet with the given protocol code and the usual params.
    fn startup_with_code(code: i32) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&0i32.to_be_bytes());
        b.extend_from_slice(&code.to_be_bytes());
        cstr(&mut b, "user");
        cstr(&mut b, "bench");
        b.push(0);
        let len = b.len() as i32;
        b[0..4].copy_from_slice(&len.to_be_bytes());
        b
    }

    /// The protocol version is checked before anyone is authenticated.
    /// Every startup code other than SSL, GSS and Cancel fell through to
    /// AuthenticationOk: protocol 4.0, 1.0 and an arbitrary 123 were all welcomed.
    #[test]
    fn an_unsupported_protocol_version_is_refused_at_startup() {
        for code in [262144, 65536, 123, 0, -1] {
            let (mut c, v) = (Conn::new(), view());
            let mut out = Vec::new();
            let input = startup_with_code(code);
            let used = c.advance(&input, &mut out, &v);
            assert_eq!(used, input.len(), "code {code} consumed");
            assert_eq!(
                tags(&out),
                vec![b'E'],
                "code {code}: an error and nothing else"
            );
            assert_eq!(first_sqlstate(&out), "0A000", "code {code}");
            assert!(c.is_closed(), "code {code} must close");
            let text = String::from_utf8_lossy(&out);
            assert!(text.contains("FATAL"), "{text}");
        }
    }

    /// Any 3.x is spoken: 3.0 is what every driver sends, and a newer minor is what
    /// PostgreSQL 18 negotiates down rather than refuses.
    #[test]
    fn protocol_three_is_accepted_whatever_the_minor() {
        for code in [196608, 196609, 196610] {
            let (mut c, v) = (Conn::new(), view());
            let mut out = Vec::new();
            c.advance(&startup_with_code(code), &mut out, &v);
            assert_eq!(tags(&out).first(), Some(&b'R'), "code {code}");
            assert!(!c.is_closed(), "code {code}");
        }
    }

    /// A `)` inside a quoted column name must not keep the statement out of COPY.
    #[test]
    fn a_quoted_paren_in_the_column_list_still_enters_copy() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t (\"a)\", b) FROM STDIN")),
            &mut out,
            &v,
        );
        assert_eq!(tags(&out), vec![b'G']);
        assert_eq!(i16::from_be_bytes([out[6], out[7]]), 2, "two columns");
    }

    /// A Query without its terminating NUL is malformed, not empty.
    /// The body was read with `unwrap_or("")`, so `select 1` with no NUL became the
    /// empty query and answered EmptyQueryResponse.
    #[test]
    fn a_query_without_a_nul_is_malformed() {
        let (mut c, v) = connected();
        let mut input = Vec::new();
        msg(&mut input, b'Q', |b| b.extend_from_slice(b"select 1"));
        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");
    }

    /// Describe and Close take `S` or `P` and nothing else.
    /// Any other subtype was treated as a portal: Close `X` answered CloseComplete
    /// where PostgreSQL answers 08P01.
    #[test]
    fn describe_and_close_subtypes_are_strict() {
        for (tag, ok_tag) in [(b'D', b'n'), (b'C', b'3')] {
            for sub in [b'X', b'p', b's', 0u8] {
                let (mut c, v) = connected();
                let mut input = tagged(tag, |b| {
                    b.push(sub);
                    cstr(b, "");
                });
                input.extend(tagged(b'S', |_| {}));
                let mut out = Vec::new();
                c.advance(&input, &mut out, &v);
                assert_eq!(
                    tags(&out),
                    vec![b'E', b'Z'],
                    "{} subtype {sub}",
                    tag as char
                );
                assert_eq!(first_sqlstate(&out), "08P01");
            }

            // And the two valid ones still work on the unnamed portal, which is never
            // absent for Close and is refused as absent by Describe.
            let (mut c, v) = connected();
            let mut input = tagged(tag, |b| {
                b.push(b'P');
                cstr(b, "");
            });
            input.extend(tagged(b'S', |_| {}));
            let mut out = Vec::new();
            c.advance(&input, &mut out, &v);
            let t = tags(&out);
            assert!(t == vec![ok_tag, b'Z'] || t == vec![b'E', b'Z'], "{:?}", t);
        }
    }

    /// A body is its fields and nothing after them.
    /// Query, Parse, Describe, Close and Execute accepted a valid prefix plus trailing
    /// bytes; PostgreSQL answers 08P01 for each.
    #[test]
    fn trailing_bytes_after_a_body_are_malformed() {
        let cases: Vec<(&str, Vec<u8>, bool)> = vec![
            (
                "Query",
                tagged(b'Q', |b| {
                    cstr(b, "select 1");
                    b.extend_from_slice(b"junk");
                }),
                false,
            ),
            (
                "Parse",
                tagged(b'P', |b| {
                    cstr(b, "");
                    cstr(b, "select 1");
                    b.extend_from_slice(&0i16.to_be_bytes());
                    b.push(7);
                }),
                true,
            ),
            (
                "Describe",
                tagged(b'D', |b| {
                    b.push(b'S');
                    cstr(b, "");
                    b.push(7);
                }),
                true,
            ),
            (
                "Close",
                tagged(b'C', |b| {
                    b.push(b'S');
                    cstr(b, "");
                    b.push(7);
                }),
                true,
            ),
            (
                "Execute",
                tagged(b'E', |b| {
                    cstr(b, "");
                    b.extend_from_slice(&0i32.to_be_bytes());
                    b.push(7);
                }),
                true,
            ),
        ];

        for (name, message, is_extended) in cases {
            let (mut c, v) = connected();
            // Something to describe, close and execute, so the refusal is for the
            // trailing bytes and not for a missing object.
            let mut out = Vec::new();
            c.advance(&extended("select 1"), &mut out, &v);

            let mut input = message;
            input.extend(tagged(b'S', |_| {}));
            out.clear();
            c.advance(&input, &mut out, &v);
            if is_extended {
                assert_eq!(tags(&out), vec![b'E', b'Z'], "{name}");
            } else {
                assert_eq!(
                    tags(&out),
                    vec![b'E', b'Z', b'Z'],
                    "{name}: simple, then the Sync"
                );
            }
            assert_eq!(first_sqlstate(&out), "08P01", "{name}");
        }
    }

    #[test]
    fn sync_and_flush_require_empty_bodies() {
        for (tag, want) in [(b'S', vec![b'E', b'Z', b'Z']), (b'H', vec![b'E', b'Z'])] {
            let (mut c, v) = connected();
            let mut input = tagged(tag, |b| b.extend_from_slice(b"junk"));
            input.extend(tagged(b'S', |_| {}));

            let mut out = Vec::new();
            c.advance(&input, &mut out, &v);
            assert_eq!(tags(&out), want, "tag {}", tag as char);
            assert_eq!(first_sqlstate(&out), "08P01");
        }
    }

    #[test]
    fn malformed_sync_ends_awaiting_sync_with_an_error() {
        let (mut c, v) = connected();
        let mut input = tagged(b'P', |b| b.extend_from_slice(b"malformed"));
        input.extend(tagged(b'S', |b| b.extend_from_slice(b"junk")));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'E', b'Z']);
        assert_eq!(first_sqlstate(&out), "08P01");

        out.clear();
        c.advance(&tagged(b'Q', |b| cstr(b, "select 1")), &mut out, &v);
        assert_eq!(tags(&out), vec![b'T', b'D', b'C', b'Z']);
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
