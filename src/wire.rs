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

    /// Whether a message belongs to the extended protocol, which is what
    /// decides how an error in it ends. PostgreSQL's `SocketBackend` marks
    /// exactly these as `doing_extended_query_message`.
    pub fn is_extended(t: u8) -> bool {
        matches!(t, PARSE | BIND | DESCRIBE | EXECUTE | CLOSE | FLUSH)
    }
}

/// Largest message this server will frame. PostgreSQL's own limit is 1 GB; a
/// blackhole has no reason to buffer anything near it, and an unbounded value is
/// how a malformed length becomes an allocation.
const MAX_MESSAGE: usize = 16 * 1024 * 1024;

/// The longest StartupMessage accepted, matching PostgreSQL's own
/// `PQ_MAX_STARTUP_PACKET_LENGTH`. A startup packet carries parameters, not a
/// query, so it needs nothing like `MAX_MESSAGE`.
const MAX_STARTUP: usize = 10000;

/// PostgreSQL's own wording for a statement inside a failed transaction.
const ABORTED: &str =
    "current transaction is aborted, commands ignored until end of transaction block";

/// The statements PostgreSQL still accepts in a failed transaction: the ones
/// that end it.
fn ends_transaction(kind: &PlanKind) -> bool {
    matches!(kind, PlanKind::Commit | PlanKind::Rollback)
}

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
    ///
    /// `extended` travels because it decides how the COPY ENDS. Entered from a
    /// simple Query, CopyDone is answered with the tag and `ReadyForQuery`.
    /// Entered from Execute, the client has already sent a Sync behind the
    /// Execute and will send another behind CopyDone; PostgreSQL ignores the
    /// first and answers the second, so CopyDone gets the tag alone.
    CopyIn {
        rows: u64,
        binary: bool,
        extended: bool,
    },
    /// After an error in an extended-protocol message. PostgreSQL discards
    /// every message until `Sync`, then sends ONE `ReadyForQuery`; a driver
    /// that pipelined Parse/Bind/Execute/Sync counts on that, because it has
    /// already sent the rest of the batch and must not get answers for it.
    ///
    /// This codec used to answer the error and carry on, so a Query behind a
    /// malformed Parse executed and both it and the Sync reported ready.
    AwaitingSync,
    Closed,
}

/// The transaction state `ReadyForQuery` advertises, one byte on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxStatus {
    /// `I`: no transaction open.
    Idle,
    /// `T`: a transaction block is open.
    InTransaction,
    /// `E`: a transaction block is open and a statement in it failed. PostgreSQL
    /// stays here until `ROLLBACK`, refusing everything else.
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
    /// One OID per `$n`, declared by Parse or completed from the text. This
    /// used to be dropped at Parse and reported as zero, so a driver refused
    /// to bind any parametrised statement: pgx `expected 0 arguments, got 1`,
    /// rust-postgres `Parameters(1, 0)`.
    params: Vec<u32>,
}

/// The result formats a Bind asked for: none (text), one for every column,
/// or one per column. Collapsed at Bind so that the two shapes every driver
/// sends -- nothing, or the same code for every column -- allocate nothing.
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

    /// The format of column `i`. Past the end of a per-column list is text,
    /// which is where PostgreSQL's `pq_getmsgint` would have read zero.
    fn get(&self, i: usize) -> i16 {
        match self {
            Formats::All(code) => *code,
            Formats::PerColumn(codes) => codes.get(i).copied().unwrap_or(0),
        }
    }
}

/// A portal: a bound statement, the result formats the Bind asked for, and
/// how far it has been read.
struct Portal {
    statement: String,
    formats: Formats,
    /// Whether the portal has been read to its end. A plan answers at most
    /// one row, so this is the whole cursor: Execute of a consumed portal
    /// answers no row and `SELECT 0`, as PostgreSQL does for a portal already
    /// fetched to its end. Execute used to replay the row every time.
    consumed: bool,
}

pub struct Conn {
    phase: Phase,
    /// Prepared statements by name. "" is the unnamed statement, which clients
    /// reuse constantly, so it is a normal entry rather than a special case.
    statements: HashMap<String, Statement>,
    /// Portals by name. "" is the unnamed portal, rebound by every query.
    portals: HashMap<String, Portal>,
    /// Bytes of a binary COPY stream that did not yet form a whole tuple.
    ///
    /// A `CopyData` message is a slice of the stream, not a row: pgx fills a
    /// 64 KiB buffer and sends whatever fits, so a tuple can straddle two
    /// messages and a message can end mid-field. Counting therefore has to carry
    /// state, which is what this is.
    copy_carry: Vec<u8>,
    /// Whether the binary COPY header has been consumed.
    copy_header_seen: bool,
    /// Whether the binary COPY trailer has been seen. After it, more data is
    /// a violation, and at CopyDone it is what makes a partial tuple an error
    /// rather than a tail still to come.
    copy_trailer_seen: bool,
    /// The state `ReadyForQuery` reports, which is not a constant.
    ///
    /// pgxpool reads it: a connection that reports idle while a transaction is
    /// open is one the pool will hand to another query with the transaction
    /// still running underneath it, and PostgreSQL's own clients use the byte to
    /// decide whether a failed statement can be retried in place. This server
    /// answers `I` always, which is where that behaviour came from.
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
            copy_carry: Vec::new(),
            copy_header_seen: false,
            copy_trailer_seen: false,
            tx_status: TxStatus::Idle,
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
    fn enter_copy_in(&mut self, columns: usize, binary: bool, extended: bool, out: &mut Vec<u8>) {
        copy_in_response(out, columns, binary);
        self.copy_carry.clear();
        self.copy_header_seen = false;
        self.copy_trailer_seen = false;
        self.phase = Phase::CopyIn {
            rows: 0,
            binary,
            extended,
        };
    }

    /// Back to ordinary statements, with the framing state dropped.
    fn leave_copy_in(&mut self) {
        self.copy_carry.clear();
        self.copy_header_seen = false;
        self.copy_trailer_seen = false;
        self.phase = Phase::Query;
    }

    /// `ReadyForQuery`, with the transaction state that is true at this moment.
    fn ready(&self, out: &mut Vec<u8>) {
        msg(out, b'Z', |b| b.push(self.tx_status.byte()));
    }

    /// An ErrorResponse, and the state it leaves behind.
    ///
    /// A failed statement inside a transaction block puts the SESSION in a
    /// failed transaction -- `E` until `ROLLBACK` -- because that is what
    /// PostgreSQL does and what a poolering client reads the byte to find out.
    /// An error outside a transaction leaves it idle.
    fn error(&mut self, out: &mut Vec<u8>, code: &str, message: &str) {
        if self.tx_status == TxStatus::InTransaction {
            self.tx_status = TxStatus::Failed;
        }

        error_response(out, code, message);
    }

    /// An error, and the recovery the protocol prescribes for where it
    /// happened. In the simple protocol the reply ends with `ReadyForQuery`
    /// and the next message runs. In the extended protocol nothing more is
    /// said until the client's `Sync`, and nothing in between is executed.
    fn refuse(&mut self, extended: bool, out: &mut Vec<u8>, code: &str, message: &str) {
        self.error(out, code, message);

        if extended {
            self.phase = Phase::AwaitingSync;
        } else {
            self.ready(out);
        }
    }

    /// What a statement did to the transaction state.
    ///
    /// `BEGIN` opens one, and inside an open one it is a warning that changes
    /// nothing; `COMMIT` and `ROLLBACK` close it, failed or not. A statement
    /// inside a failed transaction never reaches here: it is refused first,
    /// with 25P02, which is what a client reading `E` expects.
    fn note_transaction(&mut self, kind: &PlanKind) {
        match kind {
            PlanKind::Begin if self.tx_status == TxStatus::Idle => {
                self.tx_status = TxStatus::InTransaction
            }
            PlanKind::Commit | PlanKind::Rollback => self.tx_status = TxStatus::Idle,
            _ => {}
        }
    }

    /// Whether the session is in a failed transaction, where everything but
    /// COMMIT and ROLLBACK is refused.
    fn failed(&self) -> bool {
        self.tx_status == TxStatus::Failed
    }

    /// Count complete tuples in a binary COPY stream, carrying the tail.
    ///
    /// The framing is PostgreSQL's: an 11-byte signature, an int32 of flags,
    /// an int32 extension length and that many bytes of extension, then per
    /// tuple an int16 field count followed by each field's int32 length (-1
    /// for NULL) and its bytes, ending with an int16 -1 trailer. A field's
    /// length is what makes this countable at all -- newline bytes inside a
    /// payload are just bytes, which is exactly why counting them reported 2
    /// for a 3-row load.
    ///
    /// The header is checked, not skipped. The signature is what says this
    /// is a binary COPY at all; bits 16..31 of the flags are critical, and a
    /// reader that does not know one must refuse; and the extension length
    /// decides where the first tuple begins -- a fixed 19 was reading a
    /// four-byte extension as a tuple and answering `COPY 3` for one row.
    ///
    /// `Err` is a framing violation: the COPY is over and the caller says so
    /// with PostgreSQL's SQLSTATE for a bad COPY file.
    fn count_binary_copy_rows(&mut self, body: &[u8]) -> Result<(), &'static str> {
        /// The signature, its flags and the header-extension length.
        const HEADER: usize = 19;

        const SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

        /// How much unparsed stream to hold before calling it a violation. A
        /// whole tuple has to fit for any progress to be possible; past this the
        /// client is not sending tuples and the buffer would grow without bound.
        const MAX_CARRY: usize = 1 << 20;

        // The trailer ended the stream; a writer that keeps going is not
        // writing this format.
        if self.copy_trailer_seen && !body.is_empty() {
            return Err("received copy data after EOF marker");
        }

        self.copy_carry.extend_from_slice(body);

        let mut buf = std::mem::take(&mut self.copy_carry);
        let n = buf.len();
        let mut pos = 0usize;
        let mut rows = 0u64;

        if !self.copy_header_seen {
            if n < HEADER {
                self.copy_carry = buf;

                return Ok(());
            }

            if &buf[..11] != SIGNATURE {
                return Err("COPY file signature not recognized");
            }

            let flags = i32::from_be_bytes([buf[11], buf[12], buf[13], buf[14]]);
            if flags & !0xffff != 0 {
                return Err("unrecognized critical flags in COPY file header");
            }

            let extension = i32::from_be_bytes([buf[15], buf[16], buf[17], buf[18]]);
            if extension < 0 {
                return Err("invalid COPY file header (wrong length)");
            }

            // The extension is skipped whole, so it has to be here whole.
            let first_tuple = HEADER + extension as usize;
            if n < first_tuple {
                self.copy_carry = buf;

                return Ok(());
            }

            pos = first_tuple;
            self.copy_header_seen = true;
        }

        loop {
            if n - pos < 2 {
                break;
            }

            let fields = i16::from_be_bytes([buf[pos], buf[pos + 1]]);

            // The trailer: an int16 -1 ends the stream. Not a row. Any other
            // negative count is not a trailer, it is a malformed stream.
            if fields == -1 {
                pos += 2;
                self.copy_trailer_seen = true;

                if pos < n {
                    return Err("received copy data after EOF marker");
                }

                break;
            }

            if fields < -1 {
                return Err("invalid field count in COPY data");
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

                // -1 is NULL: a length with no bytes after it. Below that is
                // not a length at all.
                if len < -1 {
                    return Err("invalid field size in COPY data");
                }

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

        Ok(())
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
            self.refuse(tag::is_extended(t), out, "08P01", "malformed message body");
        }

        Some(total)
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
        // Skipping to Sync: only Sync ends it, and only Terminate is otherwise
        // heard. Everything else is consumed unanswered -- no BindComplete for
        // a Bind, no ParseComplete for a Parse -- because the driver has
        // already stopped counting on them.
        if self.phase == Phase::AwaitingSync {
            match t {
                tag::SYNC => {
                    self.phase = Phase::Query;
                    self.ready(out);
                }
                tag::TERMINATE => self.phase = Phase::Closed,
                _ => {}
            }

            return Some(());
        }

        // A COPY in flight has its own vocabulary, and everything outside it is
        // a violation rather than a statement to run.
        if let Phase::CopyIn {
            binary, extended, ..
        } = self.phase
        {
            return self.copy_message(t, body, binary, extended, out);
        }

        match t {
            tag::TERMINATE => {
                self.phase = Phase::Closed;
            }

            tag::QUERY => {
                let sql = cstr_read(body).unwrap_or("");
                let plan = PreparedPlan::build(sql);

                // A failed transaction runs nothing until it is ended. This
                // used to answer the statement anyway, so a SELECT after an
                // error got its row and a BEGIN flipped the state to `T`
                // without any ROLLBACK.
                if self.failed() && !ends_transaction(&plan.kind) {
                    self.refuse(false, out, "25P02", ABORTED);

                    return Some(());
                }

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
                            self.enter_copy_in(columns, binary, false, out);

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

                    self.ready(out);

                    return Some(());
                }

                // A statement that opens, ends or fails a transaction changes
                // what the NEXT `ReadyForQuery` must say, and it is noted before
                // the answer is written so that the byte and the statement
                // cannot disagree.
                let failed = self.failed();
                self.note_transaction(&plan.kind);

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
            }

            tag::COPY_DATA | tag::COPY_DONE | tag::COPY_FAIL => {
                // A COPY message outside a COPY is the client desynchronised, not
                // something to absorb quietly: absorbing it is what lets the two
                // sides disagree for the rest of the connection.
                self.error(out, "08P01", "COPY message outside a COPY operation");
                self.ready(out);
            }

            tag::PARSE => {
                // [stmt_name][query][n_params:i16][types...]
                let (name, after) = cstr_at(body, 0)?;
                let (sql, after) = cstr_at(body, after)?;
                let declared = oid_list(body, after)?;
                let plan = PreparedPlan::build(sql);

                if self.failed() && !ends_transaction(&plan.kind) {
                    self.refuse(true, out, "25P02", ABORTED);

                    return Some(());
                }

                let params = crate::handler::parameter_types(sql, &declared);
                self.statements
                    .insert(name.to_string(), Statement { plan, params });
                msg(out, b'1', |_| {});
            }

            tag::BIND => {
                // [portal][stmt][n_pfmt][pfmt..][n_params][(len, bytes)..]
                // [n_rfmt][rfmt..]. The values are never looked at; the
                // counts and the result formats are.
                let (portal, after) = cstr_at(body, 0)?;
                let (stmt, after) = cstr_at(body, after)?;
                let bind = bind_tail(body, after)?;

                // A statement that was never prepared -- or was closed -- is
                // not bound to; it is reported. Answering BindComplete here
                // let a driver with a stale statement cache run against a
                // statement the server did not have.
                let Some(Statement { plan, params }) = self.statements.get(stmt) else {
                    self.refuse(
                        true,
                        out,
                        "26000",
                        &format!("prepared statement \"{stmt}\" does not exist"),
                    );

                    return Some(());
                };

                if self.failed() && !ends_transaction(&plan.kind) {
                    self.refuse(true, out, "25P02", ABORTED);

                    return Some(());
                }

                // The counts PostgreSQL checks, with its SQLSTATE and wording.
                if bind.params != params.len() {
                    let want = params.len();
                    self.refuse(
                        true,
                        out,
                        "08P01",
                        &format!(
                            "bind message supplies {} parameters, but prepared statement \"{stmt}\" requires {want}",
                            bind.params
                        ),
                    );

                    return Some(());
                }

                if let Some(mismatch) = bind.format_mismatch {
                    self.refuse(
                        true,
                        out,
                        "08P01",
                        &format!(
                            "bind message has {mismatch} parameter formats but {} parameters",
                            bind.params
                        ),
                    );

                    return Some(());
                }

                // Two String allocations per query if done unconditionally, and
                // a driver binds the SAME portal to the SAME statement forever.
                // The formats are replaced in place: for every driver's usual
                // shapes that is a copy of an enum, not an allocation.
                match self.portals.get_mut(portal) {
                    Some(cur) => {
                        if cur.statement != stmt {
                            cur.statement = stmt.to_string();
                        }
                        cur.formats = bind.formats;
                        cur.consumed = false;
                    }
                    None => {
                        self.portals.insert(
                            portal.to_string(),
                            Portal {
                                statement: stmt.to_string(),
                                formats: bind.formats,
                                consumed: false,
                            },
                        );
                    }
                }
                msg(out, b'2', |_| {});
            }

            tag::DESCRIBE => {
                // ['S'|'P'][name]
                let kind = body.first().copied().unwrap_or(b'P');
                let (name, _) = cstr_at(body, 1)?;

                let Some((Statement { plan, params }, formats)) = self.described(kind, name) else {
                    let (code, what) = if kind == b'S' {
                        ("26000", "prepared statement")
                    } else {
                        ("34000", "portal")
                    };
                    self.refuse(
                        true,
                        out,
                        code,
                        &format!("{what} \"{name}\" does not exist"),
                    );

                    return Some(());
                };

                // PostgreSQL refuses to describe a statement that returns rows
                // inside a failed transaction, and only those: a client that
                // blindly describes its ROLLBACK must still be able to send it.
                if self.failed()
                    && matches!(
                        plan.kind,
                        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. }
                    )
                {
                    self.refuse(true, out, "25P02", ABORTED);

                    return Some(());
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
            }

            tag::EXECUTE => {
                // [portal][max_rows:i32]. Zero is no limit.
                let (name, after) = cstr_at(body, 0)?;
                let max_rows = i32::from_be_bytes(body.get(after..after + 4)?.try_into().ok()?);
                let failed = self.failed();

                // No clone: `answer` is a free function precisely so the plan can
                // stay borrowed out of `self`. It used to be a method, which made
                // the borrow checker demand `.cloned()` -- a DEEP copy of the sql
                // String and the parsed table/column Vecs, on every Execute.
                // The portal is borrowed mutably and the statement immutably,
                // from two different fields, which the borrow checker allows.
                let statement = self
                    .portals
                    .get_mut(name)
                    .and_then(|portal| self.statements.get(&portal.statement).map(|s| (portal, s)));

                // Executing a portal that was never bound used to fabricate
                // `SELECT 0`. PostgreSQL reports it, and so does this. A portal
                // whose statement was closed is closed with it.
                let Some((portal, Statement { plan: p, .. })) = statement else {
                    self.refuse(
                        true,
                        out,
                        "34000",
                        &format!("portal \"{name}\" does not exist"),
                    );

                    return Some(());
                };

                if failed && !ends_transaction(&p.kind) {
                    self.refuse(true, out, "25P02", ABORTED);

                    return Some(());
                }

                // A COPY that streams from the client needs the connection to
                // change phase, which is not something a free function writing
                // into `out` can do. Read what it is first, answer, then enter
                // the phase.
                let streams_rows = match &p.kind {
                    PlanKind::Copy {
                        direction: CopyDirection::FromStdin,
                        binary,
                        ..
                    } => Some(*binary),
                    _ => None,
                };

                // The extended protocol's Execute is where a statement actually
                // runs, so it is where the transaction state changes. `SYNC` is
                // what asks for the byte, later. Copied out, because
                // `note_transaction` needs `self` mutably and `p` is borrowed
                // from it.
                let touched_tx = matches!(
                    p.kind,
                    PlanKind::Begin | PlanKind::Commit | PlanKind::Rollback
                );

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

                if touched_tx {
                    let kind = p.kind.clone();
                    self.note_transaction(&kind);
                }

                if let Some(binary) = streams_rows {
                    self.copy_carry.clear();
                    self.copy_header_seen = false;
                    self.copy_trailer_seen = false;
                    self.phase = Phase::CopyIn {
                        rows: 0,
                        binary,
                        extended: true,
                    };
                }
            }

            tag::SYNC => self.ready(out),

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

    /// One message while a COPY FROM STDIN is open.
    ///
    /// `d`, `c` and `f` are the conversation. `S` and `H` are ignored, as
    /// PostgreSQL's `CopyGetData` ignores them, because libpq and rust-postgres
    /// send a Sync behind the Execute that started the COPY without noticing
    /// that it was one. Anything else is the client talking out of turn, and
    /// PostgreSQL ends the COPY with a protocol violation rather than running
    /// it -- a Query executed mid-COPY used to answer rows into a stream the
    /// client was reading as COPY replies.
    ///
    /// How the COPY ends depends on how it began: see `Phase::CopyIn`.
    fn copy_message(
        &mut self,
        t: u8,
        body: &[u8],
        binary: bool,
        extended: bool,
        out: &mut Vec<u8>,
    ) -> Option<()> {
        match t {
            tag::COPY_DATA => {
                if binary {
                    if let Err(violation) = self.count_binary_copy_rows(body) {
                        // A stream that is not PostgreSQL's binary framing is
                        // not counted, it is refused -- with the SQLSTATE the
                        // server uses for a bad COPY file, and the recovery
                        // the entry protocol prescribes.
                        self.leave_copy_in();
                        self.refuse(extended, out, "22P04", violation);
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

            tag::COPY_DONE => {
                // The end of a binary stream is checked against its framing.
                // PostgreSQL's reader takes a clean EOF at a tuple boundary
                // as the end (`CopyFromBinaryOneRow`: "end of file, or
                // trailer"), so a missing trailer is complete; a stream with
                // no header is not a binary COPY at all, and one that ends
                // mid-tuple is `unexpected EOF`. This used to answer `COPY n`
                // for all three.
                if binary {
                    let violation = if !self.copy_header_seen {
                        Some("COPY file signature not recognized")
                    } else if !self.copy_carry.is_empty() {
                        Some("unexpected EOF in COPY data")
                    } else {
                        None
                    };

                    if let Some(violation) = violation {
                        self.leave_copy_in();
                        self.refuse(extended, out, "22P04", violation);

                        return Some(());
                    }
                }

                let rows = match &self.phase {
                    Phase::CopyIn { rows, .. } => *rows,
                    _ => 0,
                };
                self.leave_copy_in();
                complete_raw(out, &format!("COPY {rows}"));

                // Simple protocol: the statement is over, say so. Extended: the
                // client's Sync behind CopyDone asks for the byte, later.
                if !extended {
                    self.ready(out);
                }
            }

            tag::COPY_FAIL => {
                // The client is abandoning its own COPY. That is an error by the
                // protocol's own definition, and answering it as success would
                // tell a loader its data landed.
                self.leave_copy_in();
                self.refuse(extended, out, "57014", "COPY from stdin failed");
            }

            // Sent behind the Execute by drivers that did not notice the
            // statement was a COPY; PostgreSQL ignores both here.
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

    /// The statement a Describe names, directly or through a portal, and the
    /// result formats to describe it in. A statement has no portal and so no
    /// formats: PostgreSQL reports text there, whatever a later Bind asks.
    fn described(&self, kind: u8, name: &str) -> Option<(&Statement, &Formats)> {
        if kind == b'S' {
            self.statements.get(name).map(|s| (s, &Formats::TEXT))
        } else {
            self.portals
                .get(name)
                .and_then(|p| self.statements.get(&p.statement).map(|s| (s, &p.formats)))
        }
    }
}

/// The rows and the completion tag for one plan. A FREE function, not a method:
/// it never needed `self`, and being a method is what forced a deep clone of the
/// plan at every Execute.
///
/// Returns whether the portal has now been read to its end. Only a plan that
/// returns rows has an end to reach; the rest are done when they answer.
fn answer(plan: &PreparedPlan, out: &mut Vec<u8>, catalog: &CatalogView, run: Run<'_>) -> bool {
    match &plan.kind {
        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. } => {
            let cols = resolve_columns(plan, catalog);
            if run.describe {
                row_description_of(&cols, run.formats, out);
            }

            // A portal already read to its end has nothing more: PostgreSQL
            // answers `SELECT 0` with no row, and so does this.
            if run.consumed {
                complete(out, "SELECT", Some(0));

                return true;
            }

            // A metadata select answers empty; a stub select answers one row.
            if matches!(plan.kind, PlanKind::SelectMeta { .. }) && cols.resolved() {
                complete(out, "SELECT", Some(0));

                return true;
            }

            data_row(out, &cols, run.formats);

            // The one row is also the last one, but with a limit of one the
            // server cannot know that until it looks for the next -- which is
            // why PostgreSQL answers PortalSuspended here, and the next
            // Execute finds the end.
            if run.max_rows == 1 {
                portal_suspended(out);
            } else {
                complete(out, "SELECT", Some(1));
            }

            return true;
        }
        PlanKind::Insert => complete_raw(out, "INSERT 0 1"),
        PlanKind::Update => complete_raw(out, "UPDATE 1"),
        PlanKind::Delete => complete_raw(out, "DELETE 1"),
        PlanKind::Begin => complete_raw(out, "BEGIN"),
        PlanKind::Commit if run.failed => complete_raw(out, "ROLLBACK"),
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

    true
}

/// How one execution of a plan is to be answered.
struct Run<'a> {
    /// Whether to send RowDescription first: the simple protocol does, and
    /// Execute does not, because Describe did.
    describe: bool,
    /// Whether the transaction the statement runs in has failed. Only COMMIT
    /// reads it: committing a failed transaction rolls it back, and
    /// PostgreSQL's tag says `ROLLBACK` so the client can see that its COMMIT
    /// committed nothing.
    failed: bool,
    /// What the portal's Bind asked the rows to be encoded in. The simple
    /// protocol has no Bind and is always text.
    formats: &'a Formats,
    /// Execute's row limit; zero is none.
    max_rows: i32,
    /// Whether the portal was already read to its end.
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

/// One DataRow, every value a `1`, encoded per column in the format the
/// portal asked for.
///
/// Text is the one byte `1` whatever the type. Binary is the type's own wire
/// form -- eight bytes for an int8, four for an int4, one for a bool -- which
/// is what a driver that read the RowDescription will decode. The row used to
/// be text regardless of what Bind asked: pgx, which asks for binary int8 by
/// default, then failed with `invalid length for int8: 1`.
fn data_row(out: &mut Vec<u8>, cols: &Columns, formats: &Formats) {
    msg(out, b'D', |b| {
        b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
        for i in 0..cols.len() {
            if formats.get(i) == 1 {
                binary_one(b, cols.oid(i));
            } else {
                b.extend_from_slice(&1i32.to_be_bytes());
                b.push(b'1');
            }
        }
    });
}

/// The value 1 in the binary send format of the type `oid`, with its length.
fn binary_one(b: &mut Vec<u8>, oid: u32) {
    fn field(b: &mut Vec<u8>, bytes: &[u8]) {
        b.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        b.extend_from_slice(bytes);
    }

    match oid {
        // bool
        16 => field(b, &[1]),
        // int2, int4, int8
        21 => field(b, &1i16.to_be_bytes()),
        23 => field(b, &1i32.to_be_bytes()),
        20 => field(b, &1i64.to_be_bytes()),
        // float4, float8
        700 => field(b, &1f32.to_be_bytes()),
        701 => field(b, &1f64.to_be_bytes()),
        // numeric: one base-10000 digit, weight 0, positive, scale 0, digit 1.
        1700 => field(b, &[0, 1, 0, 0, 0, 0, 0, 0, 0, 1]),
        // date: days since 2000-01-01. timestamp, timestamptz: microseconds
        // since then. Zero is a valid value of each; "1" would not be.
        1082 => field(b, &0i32.to_be_bytes()),
        1114 | 1184 => field(b, &0i64.to_be_bytes()),
        // text, varchar, bpchar, bytea, json and anything else whose binary
        // form is its bytes.
        _ => field(b, b"1"),
    }
}

/// The columns a SELECT answers with: the catalog's, when it knows the table,
/// or `n` stubs. Resolved once per answer and used for both the
/// RowDescription and the DataRow, so the two cannot disagree.
enum Columns {
    /// `?column?` int8, `n` times, from a `&'static str` rather than n freshly
    /// allocated Strings.
    Stub(usize),
    Known(Vec<(String, u32)>),
}

impl Columns {
    fn len(&self) -> usize {
        match self {
            Columns::Stub(n) => *n,
            Columns::Known(cols) => cols.len(),
        }
    }

    fn resolved(&self) -> bool {
        matches!(self, Columns::Known(_))
    }

    fn name(&self, i: usize) -> &str {
        match self {
            Columns::Stub(_) => STUB_COL,
            Columns::Known(cols) => &cols[i].0,
        }
    }

    fn oid(&self, i: usize) -> u32 {
        match self {
            Columns::Stub(_) => 20,
            Columns::Known(cols) => cols[i].1,
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
        _ => Columns::Stub(0),
    }
}

/// RowDescription for a plan that returns rows, or false for one that does
/// not. Uses the client's own column names when the catalog knows the table --
/// a driver that created a table and selects from it gets its names back,
/// which is the one piece of real behaviour this server has.
fn row_description(
    plan: &PreparedPlan,
    catalog: &CatalogView,
    formats: &Formats,
    out: &mut Vec<u8>,
) -> bool {
    if !matches!(
        plan.kind,
        PlanKind::SelectMeta { .. } | PlanKind::SelectStub { .. }
    ) {
        return false;
    }

    row_description_of(&resolve_columns(plan, catalog), formats, out);

    true
}

/// RowDescription: each column's name, type, and the format code the portal
/// asked for. The format codes are the client's own request read back, and a
/// driver that asked for binary decodes what follows as binary.
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

fn cstr_read(body: &[u8]) -> Option<&str> {
    cstr_at(body, 0).map(|(s, _)| s)
}

/// An int16 count at `from` followed by that many int32 OIDs, as Parse
/// declares its parameter types. A count that outruns the body is malformed.
fn oid_list(body: &[u8], from: usize) -> Option<Vec<u32>> {
    let n = i16::from_be_bytes(body.get(from..from + 2)?.try_into().ok()?);
    let n = usize::try_from(n).ok()?;
    let bytes = body.get(from + 2..from + 2 + 4 * n)?;

    Some(
        (0..n)
            .map(|i| u32::from_be_bytes(bytes[4 * i..4 * i + 4].try_into().unwrap()))
            .collect(),
    )
}

/// What a Bind says after its two names.
struct BindTail {
    /// How many parameter values it carries.
    params: usize,
    /// The parameter-format count, when it is not 0, 1 or `params`.
    format_mismatch: Option<usize>,
    /// The result formats it asks for.
    formats: Formats,
}

/// Decode the rest of a Bind: parameter formats, parameter values, result
/// formats. `None` is a body that ends before its own counts do, or a format
/// code that is neither text nor binary -- malformed, not "no formats".
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

    /// The status byte of the LAST `ReadyForQuery` in a reply.
    ///
    /// `Z` messages are `[b'Z'][len:i32][status:u8]`, so the byte the client
    /// reads sits at the end of the last one -- which is always where
    /// `ReadyForQuery` is in a well-formed reply.
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

    /// **The status byte is not a constant.**
    ///
    /// `ReadyForQuery` always said `I`. pgxpool reads it to decide whether a
    /// connection is free, so after a raw `BEGIN` it put the connection back in
    /// the pool with the transaction still open underneath the next user of it.
    /// PostgreSQL reports `T` there and so must this.
    #[test]
    fn a_raw_begin_reports_in_transaction() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);

        // `BEGIN` answers `BEGIN`, then `ReadyForQuery`, whose status byte is
        // the byte after the message's own length.
        let text = String::from_utf8_lossy(&out);
        assert!(text.contains("BEGIN"), "{text}");
        assert_eq!(last_ready_status(&out), b'T', "a transaction block is open");
    }

    /// `COMMIT` and `ROLLBACK` close it in both directions, including out of a
    /// failed transaction.
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

    /// An error inside a transaction block leaves the session in a FAILED
    /// transaction -- `E` -- until it is rolled back, which is what PostgreSQL
    /// does and what tells a client its statements cannot be retried in place.
    /// Outside a transaction, an error leaves the session idle.
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

    /// The extended protocol's Execute changes the state, and `Sync` is what
    /// reports it -- so `Parse/Bind/Execute(Sync)` around a `BEGIN` must say `T`.
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

    /// A client that abandons its own COPY is told it failed. Answering success
    /// would tell a loader its data landed.
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
            assert!(out.ends_with(b"I"), "{sql} must be answered and ready");
        }
    }

    /// `COPY t TO STDOUT` is a `CopyOutResponse`, an empty stream and `COPY 0`.
    /// It used to fall through to `CommandComplete("SELECT 0")`.
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
            assert!(
                text.contains("COPY 1"),
                "{sql:?} must count its row: {text}"
            );
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
        assert_eq!(
            i16::from_be_bytes([out[8], out[9]]),
            1,
            "binary, per column"
        );
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

    /// A NULL field is a length of -1 with no bytes after it, and it must not
    /// throw the frame walk off by four.
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

    /// A client that never completes a tuple must not grow the carry buffer
    /// without bound. A megabyte of unterminated stream is a violation, and the
    /// connection is closed rather than held.
    #[test]
    fn binary_copy_closes_a_client_that_never_completes_a_tuple() {
        let (mut c, v) = connected();
        let mut out = Vec::new();

        c.advance(
            &tagged(b'Q', |b| cstr(b, "COPY t FROM STDIN BINARY")),
            &mut out,
            &v,
        );

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

        assert!(
            c.is_closed(),
            "an unterminated tuple must not be held forever"
        );
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
        assert!(
            !c.is_closed(),
            "a valid startup must not close the connection"
        );
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

    /// **An error in an extended-protocol message discards everything until
    /// Sync.** PostgreSQL answers `ErrorResponse`, then reads and drops every
    /// message until `Sync`, then sends ONE `ReadyForQuery`. This codec answered
    /// the error and kept going: the Query after a malformed Parse executed,
    /// and both it and the Sync reported ready -- `E,T,D,C,Z,Z` where the
    /// client's driver expects `E,Z`.
    #[test]
    fn an_error_in_an_extended_message_discards_everything_until_sync() {
        let (mut c, v) = connected();

        // Malformed Parse (no NUL anywhere), then a Query, then Sync, in one
        // packet -- which is how a pipelining driver sends them.
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

    /// Extended messages that arrive while skipping are not answered either --
    /// no BindComplete for a Bind, no ParseComplete for a Parse. A driver
    /// counts replies against what it sent, and a stray `2` desynchronises it.
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

    /// A Terminate while skipping still closes: the client is allowed to give
    /// up without sending the Sync it never got an answer for.
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

    /// The simple protocol has no Sync, so an error there ends with
    /// `ReadyForQuery` at once and the next message runs. A COPY message with
    /// no COPY open is the one simple-protocol error this codec produces.
    #[test]
    fn a_simple_protocol_error_is_ready_at_once() {
        let (mut c, v) = connected();

        let mut input = tagged(b'c', |_| {});
        input.extend(tagged(b'Q', |b| cstr(b, "select 1")));

        let mut out = Vec::new();
        c.advance(&input, &mut out, &v);
        assert_eq!(tags(&out), vec![b'E', b'Z', b'T', b'D', b'C', b'Z']);
    }

    /// The SQLSTATE of the first ErrorResponse in a reply, as the five bytes
    /// after the `C` field tag.
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

    /// **A name that was never prepared is an error, not an empty answer.**
    /// Bind to an unknown statement answered BindComplete, Execute of an
    /// unknown portal fabricated `SELECT 0`, and Describe of either answered as
    /// if it existed. A driver whose statement cache was invalidated -- or
    /// that closed the statement itself -- then reads answers for a statement
    /// the server does not have, and never learns it. PostgreSQL says 26000 for
    /// a statement and 34000 for a portal, and skips to Sync.
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

    /// Close of a name that does not exist is NOT an error: PostgreSQL
    /// answers CloseComplete, and drivers close speculatively.
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

    /// Put a connection into a FAILED transaction: BEGIN, then the one
    /// simple-protocol error this codec produces.
    fn failed(c: &mut Conn, v: &CatalogView) {
        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, v);
        c.advance(&tagged(b'c', |_| {}), &mut out, v);
        assert_eq!(last_ready_status(&out), b'E');
    }

    /// **A failed transaction refuses statements, not just reports itself.**
    /// The state byte said `E` and every statement still ran: a SELECT got its
    /// row, and a BEGIN flipped the byte back to `T` with no ROLLBACK. A
    /// client reading `E` expects PostgreSQL's 25P02 for anything but the
    /// transaction's end, and a pool that retries in place relies on it.
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

    /// BEGIN inside an open transaction is a warning in PostgreSQL, not a new
    /// block: the state stays `T`.
    #[test]
    fn begin_inside_a_transaction_keeps_it_open() {
        let (mut c, v) = connected();
        let mut out = Vec::new();
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
        c.advance(&tagged(b'Q', |b| cstr(b, "BEGIN")), &mut out, &v);
        assert_eq!(last_ready_status(&out), b'T');
    }

    /// The extended protocol is refused the same way, at Parse, Bind and
    /// Execute -- and skips to Sync, since those are extended messages.
    #[test]
    fn a_failed_transaction_refuses_extended_messages_too() {
        let (mut c, v) = connected();

        // A statement prepared BEFORE the failure, so Bind and Execute can be
        // tried on their own.
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

    /// Parse/Bind/Execute/Sync for a statement, in one packet, the way a
    /// driver sends them.
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

    /// **A COPY entered through the extended protocol ends at the client's
    /// Sync, not at CopyDone.** rust-postgres and libpq send Execute+Sync,
    /// wait for CopyInResponse, stream, then send CopyDone+Sync and expect
    /// `C, Z`. This codec answered the first Sync mid-COPY and let CopyDone
    /// send its own ReadyForQuery: `G, Z, C, Z, Z`, and the driver read
    /// `UnexpectedMessage` twice.
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

    /// The simple protocol has no Sync, so there CopyDone still ends with
    /// ReadyForQuery -- and a Flush or Sync a confused client sends during
    /// either kind is ignored, as PostgreSQL ignores them.
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

    /// An ordinary message during COPY is a protocol violation, not a query
    /// to run. A Query executed mid-COPY answered rows into a stream the
    /// client was reading as COPY replies.
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

    /// CopyFail in an extended COPY is an extended error: ErrorResponse, then
    /// nothing until the client's Sync.
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

    /// Start a binary COPY and feed it `chunks`, then CopyDone; the reply to
    /// the whole exchange after CopyInResponse.
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

    /// **The header is not 19 bytes; it is 19 bytes plus the extension it
    /// declares.** Bytes 15..19 carry the extension length, and a reader
    /// must skip that many more before the first tuple. This one started
    /// tuple parsing at byte 19 regardless, so a four-byte extension was read
    /// as a field count and a length, and one tuple came back as `COPY 3`.
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

    /// A stream that does not start with the signature is not a binary COPY,
    /// and PostgreSQL says so (22P04) instead of counting whatever it is.
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

    /// The flags word reserves bits 16..31 as critical: a reader that does
    /// not understand one must refuse the file. None are defined, so any set
    /// bit there is a refusal.
    #[test]
    fn binary_copy_rejects_critical_flags() {
        let mut stream = binary_header(1 << 20, &[]);
        stream.extend(binary_tuple(&[1]));

        let (_, out) = binary_copy(&[&stream]);
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// **Only -1 is special.** A field count of -1 is the trailer and a field
    /// length of -1 is NULL; every other negative value is a malformed stream.
    /// This treated every negative count as the trailer and every negative
    /// length as NULL, so a length of -2 was counted as a valid row.
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

    /// **CopyDone is checked against the framing, not taken on trust.** A
    /// stream that ends mid-tuple is `unexpected EOF in COPY data` in
    /// PostgreSQL; this answered `COPY n` for whatever had been counted and
    /// dropped the partial tuple on the floor.
    #[test]
    fn binary_copy_done_with_a_partial_tuple_is_an_error() {
        let mut stream = binary_header(0, &[]);
        stream.extend(binary_tuple(&[1]));
        let cut = stream.len() - 3;

        let (_, out) = binary_copy(&[&stream[..cut]]);
        assert_eq!(tags(&out), vec![b'E', b'Z'], "no CommandComplete");
        assert_eq!(first_sqlstate(&out), "22P04");
    }

    /// Bytes after the trailer are `received copy data after EOF marker`,
    /// whether they arrive in the same message or a later one.
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

    /// The trailer is how a writer says it is done, but PostgreSQL's reader
    /// (`CopyFromBinaryOneRow`) treats a clean EOF at a tuple boundary the
    /// same way -- "EOF detected (end of file, or trailer)" -- so a stream that
    /// omits it is complete, not malformed, and this server agrees with the
    /// server it stands in for.
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

    /// Parse with declared parameter types, Describe the statement, Sync.
    fn parse_and_describe(c: &mut Conn, v: &CatalogView, sql: &str, oids: &[u32]) -> Vec<u8> {
        let mut input = tagged(b'P', |b| {
            cstr(b, "s");
            cstr(b, sql);
            b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
            for oid in oids {
                b.extend_from_slice(&oid.to_be_bytes());
            }
        });
        input.extend(tagged(b'D', |b| {
            b.push(b'S');
            cstr(b, "s");
        }));
        input.extend(tagged(b'S', |_| {}));

        let mut out = Vec::new();
        c.advance(&input, &mut out, v);
        out
    }

    /// **ParameterDescription reports the statement's parameters.** It
    /// reported zero, always: Parse read the statement name and the SQL and
    /// dropped the declared types on the floor. A driver checks the count
    /// against its arguments before it binds -- pgx: `expected 0 arguments,
    /// got 1`; rust-postgres: `Parameters(1, 0)` -- so no parametrised
    /// statement could be executed at all.
    #[test]
    fn declared_parameter_types_are_reported() {
        let (mut c, v) = connected();
        let out = parse_and_describe(&mut c, &v, "select $1", &[20]);
        assert_eq!(parameter_description(&out), vec![20]);

        let out = parse_and_describe(&mut c, &v, "select $1, $2", &[20, 25]);
        assert_eq!(parameter_description(&out), vec![20, 25]);
    }

    /// An undeclared parameter with a cast takes the cast's type; without
    /// one it is reported as unknown (OID 0). Text was tried first and
    /// measured to break pgx, which refuses to encode a Go integer into a
    /// text parameter; given an unknown OID it encodes from the value's own
    /// type, so `UPDATE t SET v = $1 WHERE id = $2` with two int64 arguments
    /// -- the case in issue #1 -- goes through.
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

        // Placeholders are counted by the highest number, not by occurrences,
        // and a `$1` inside a string literal is not a placeholder.
        let out = parse_and_describe(&mut c, &v, "select $2, $2, '$3'", &[]);
        assert_eq!(parameter_description(&out), vec![0, 0]);

        // No parameters is still no parameters.
        let out = parse_and_describe(&mut c, &v, "select 1", &[]);
        assert_eq!(parameter_description(&out), Vec::<u32>::new());
    }

    /// A Parse whose declared count outruns its body is malformed, not
    /// "zero parameters".
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

    /// Parse `sql`, Bind with the given result formats, Describe the portal,
    /// Execute, Sync -- the shape pgx and rust-postgres send for a query.
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

    /// **The result format a client asks for is the one it gets.** Bind read
    /// two names and ignored the rest, so a portal bound with result format 1
    /// was described as text and answered with the one ASCII byte `1`.
    /// rust-postgres decoded that as a binary INT8 and failed with
    /// `UnexpectedEof`; pgx, which asks for binary int8 by default, failed
    /// with `invalid length for int8: 1`. This is the second half of issue #1.
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

        let out = query_with_formats(&mut c, &v, "select 1, 2", &[0, 1]);
        assert_eq!(row_description_formats(&out), vec![0, 1]);
        let fields = data_row_fields(&out);
        assert_eq!(fields[0], Some(b"1".to_vec()));
        assert_eq!(fields[1], Some(1i64.to_be_bytes().to_vec()));
    }

    /// A select from a table the catalog knows describes the client's own
    /// types, in the format the client asked for -- and answers no rows, which
    /// is the rule for a metadata select and unchanged here.
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

    /// The binary form of `1` per type the catalog can announce. Only int8
    /// reaches a DataRow today (stub columns are int8, and a known table
    /// answers no rows), but the RowDescription can promise any of these and
    /// the encoder has to keep that promise if a row is ever sent.
    #[test]
    fn binary_one_is_each_types_own_wire_form() {
        let one = |oid: u32| {
            let mut b = Vec::new();
            binary_one(&mut b, oid);
            let len = i32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
            assert_eq!(b.len(), 4 + len, "oid {oid}: length prefix matches");
            b[4..].to_vec()
        };

        assert_eq!(one(16), vec![1], "bool");
        assert_eq!(one(21), 1i16.to_be_bytes(), "int2");
        assert_eq!(one(23), 1i32.to_be_bytes(), "int4");
        assert_eq!(one(20), 1i64.to_be_bytes(), "int8");
        assert_eq!(one(700), 1f32.to_be_bytes(), "float4");
        assert_eq!(one(701), 1f64.to_be_bytes(), "float8");
        assert_eq!(one(1700), vec![0, 1, 0, 0, 0, 0, 0, 0, 0, 1], "numeric 1");
        assert_eq!(one(1082), 0i32.to_be_bytes(), "date: 2000-01-01");
        assert_eq!(one(1114), 0i64.to_be_bytes(), "timestamp");
        assert_eq!(one(25), b"1", "text");
        assert_eq!(one(1043), b"1", "varchar");
    }

    /// Describe of a STATEMENT has no portal and so no formats: PostgreSQL
    /// reports text there, whatever a later Bind will ask for.
    #[test]
    fn describe_statement_reports_text_formats() {
        let (mut c, v) = connected();
        let out = parse_and_describe(&mut c, &v, "select 1", &[]);
        assert_eq!(row_description_formats(&out), vec![0]);
    }

    /// Bind is parsed to its end and its counts are checked: the parameter
    /// count must match the statement, and there are 0, 1 or n parameter
    /// format codes. Anything else is 08P01, as it is in PostgreSQL.
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

    /// **A portal is a cursor, and Execute reads `max_rows`.** Execute parsed
    /// only the portal name, so a limit was ignored and a second Execute of the
    /// same portal replayed the row. PostgreSQL returns the row and
    /// PortalSuspended when the limit is hit, and `SELECT 0` with no row for
    /// a portal already read to its end.
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

    /// No limit reads the portal to its end in one go, and a later Execute of
    /// it finds nothing more.
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

        // Binding again is a new portal, with its row.
        bind_p1(&mut c, &v, "select 1");
        let out = execute(&mut c, &v, "p1", 0);
        assert_eq!(tags(&out), vec![b'D', b'C', b'Z']);
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
