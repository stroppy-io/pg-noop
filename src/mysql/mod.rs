//! The MySQL/MariaDB wire protocol, as a sans-io codec.
//!
//! `Conn::advance` takes bytes off a socket and appends packets to a buffer;
//! nothing here touches a socket, so the same codec runs under the io_uring loop
//! and under the tokio one. `handshake` is the greeting and the one exchange
//! before commands; `result` writes the packet shapes; `packet` is the framing
//! and the length encodings.
//!
//! # What this server answers
//!
//! A no-op cannot know what a query means, so the answer is decided by the
//! statement's SHAPE and never by its data: a SELECT answers a well-formed
//! result set with **zero rows** (the same choice the PostgreSQL codec makes --
//! an empty result is what any driver can accept), and everything else answers
//! an OK packet with zero affected rows. The exceptions are the TPC-C rules in
//! `crate::tpcc`, which are explicit and documented there.
//!
//! Nothing is interpreted beyond what the reply's shape needs: the first word of
//! a statement, where its statements end, how many placeholders it has, and, for
//! a SELECT, its column count and names.

mod handshake;
mod packet;
mod result;

use std::collections::HashMap;

use packet::Reader;
use result::{Column, Kind};

pub use handshake::greeting;

// Server status flags, by their names in `include/mysql_com.h`. A client reads
// them: `IN_TRANS` is how a pool knows a transaction is open, and
// `MORE_RESULTS_EXISTS` is how it knows another result follows.
pub const IN_TRANS: u16 = 0x0001;
pub const AUTOCOMMIT: u16 = 0x0002;
pub const MORE_RESULTS_EXISTS: u16 = 0x0008;

/// The commands this server answers, by their names in `include/mysql_com.h`.
mod cmd {
    pub const QUIT: u8 = 0x01;
    pub const INIT_DB: u8 = 0x02;
    pub const QUERY: u8 = 0x03;
    pub const FIELD_LIST: u8 = 0x04;
    pub const CREATE_DB: u8 = 0x05;
    pub const DROP_DB: u8 = 0x06;
    pub const REFRESH: u8 = 0x07;
    pub const SHUTDOWN: u8 = 0x08;
    pub const STATISTICS: u8 = 0x09;
    pub const PROCESS_INFO: u8 = 0x0a;
    pub const PROCESS_KILL: u8 = 0x0c;
    pub const DEBUG: u8 = 0x0d;
    pub const PING: u8 = 0x0e;
    pub const CHANGE_USER: u8 = 0x11;
    pub const STMT_PREPARE: u8 = 0x16;
    pub const STMT_EXECUTE: u8 = 0x17;
    pub const STMT_SEND_LONG_DATA: u8 = 0x18;
    pub const STMT_CLOSE: u8 = 0x19;
    pub const STMT_RESET: u8 = 0x1a;
    pub const SET_OPTION: u8 = 0x1b;
    pub const STMT_FETCH: u8 = 0x1c;
    pub const RESET_CONNECTION: u8 = 0x1f;
}

/// The most a command may accumulate across the protocol's 16 MiB packets.
///
/// One packet carries nearly 16 MiB, and a command larger than that is split
/// across several; the pieces are buffered here, so the total is bounded the way
/// PostgreSQL's `MAX_MESSAGE` is. MySQL's own `max_allowed_packet` defaults to
/// 64 MiB, and a statement larger than this is one no benchmark sends.
const MAX_COMMAND: usize = 32 * 1024 * 1024;

/// The longest a column name taken from the SQL text may be. A select-list item
/// is echoed as its column's name, and a client that reads the name should be
/// able to use it: a 200-byte expression is not a name.
const MAX_COLUMN_NAME: usize = 64;

/// What a statement answers.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Shape {
    /// An OK packet, with zero affected rows.
    Ok,
    /// A result set with no rows and these columns.
    Empty(Vec<String>),
}

/// What a statement does to the session, decided from its text and applied when
/// it runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tx {
    None,
    Begin,
    Commit,
    Rollback,
}

/// Everything a statement's answer needs, decided once -- at prepare for a
/// prepared statement, and at the command for everything else.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Answer {
    shape: Shape,
    tx: Tx,
}

impl Answer {
    /// The columns a result set will have, for the prepare response.
    fn columns(&self) -> &[String] {
        match &self.shape {
            Shape::Ok => &[],
            Shape::Empty(columns) => columns,
        }
    }
}

/// A prepared statement. Nothing about it depends on a parameter's value: this
/// server never looks at one, which is why preparing one is the same work as
/// running one.
struct Prepared {
    answer: Answer,
}

enum Phase {
    /// The greeting has been written; the handshake response is what arrives.
    Handshake,
    /// Commands, one packet each.
    Commands,
    Closed,
}

/// One connection's MySQL state.
pub struct Conn {
    phase: Phase,
    /// The capabilities the client negotiated. Zero until it has answered.
    caps: u32,
    /// The sequence number of the next packet this connection writes. Every
    /// command restarts the count from its own sequence, which is what lets a
    /// client pipeline commands.
    seq: u8,
    /// A command split across packets of 16 MiB, while its pieces arrive.
    partial: Vec<u8>,
    stmts: HashMap<u32, Prepared>,
    /// The next statement id to hand out. Ids are not reused: a client that
    /// closed one and executes it again gets "unknown statement" rather than
    /// another statement's answer.
    next_stmt: u32,
    /// `SERVER_STATUS_IN_TRANS`: a transaction is open.
    in_trans: bool,
}

impl Default for Conn {
    fn default() -> Self {
        Self::new()
    }
}

impl Conn {
    pub fn new() -> Self {
        Conn {
            phase: Phase::Handshake,
            caps: 0,
            seq: 0,
            partial: Vec::new(),
            stmts: HashMap::new(),
            next_stmt: 1,
            in_trans: false,
        }
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.phase, Phase::Closed)
    }

    /// True until the handshake is over: the one state in which a client may
    /// legitimately have sent nothing at all.
    pub fn awaiting_startup(&self) -> bool {
        matches!(self.phase, Phase::Handshake)
    }

    /// The status flags every answer carries.
    fn status(&self, more: bool) -> u16 {
        let mut status = AUTOCOMMIT;
        if self.in_trans {
            status |= IN_TRANS;
        }
        if more {
            status |= MORE_RESULTS_EXISTS;
        }

        status
    }

    fn deprecate_eof(&self) -> bool {
        self.caps & handshake::CLIENT_DEPRECATE_EOF != 0
    }

    fn multi_statements(&self) -> bool {
        self.caps & handshake::CLIENT_MULTI_STATEMENTS != 0
    }

    /// Consume every COMPLETE packet in `input`, appending replies to `out`.
    ///
    /// Returns how many bytes were consumed; a partial packet at the end is left
    /// alone for the caller to keep, exactly as the PostgreSQL codec does.
    pub fn advance(&mut self, input: &[u8], out: &mut Vec<u8>) -> usize {
        let mut pos = 0usize;

        loop {
            if self.is_closed() {
                return pos;
            }
            let rest = &input[pos..];
            if rest.len() < packet::HEADER {
                return pos;
            }

            let (len, seq) = packet::header(rest);
            let total = packet::HEADER + len;
            if rest.len() < total {
                return pos;
            }
            let payload = &rest[packet::HEADER..total];
            pos += total;

            // A payload of exactly the maximum length means the command
            // continues in the next packet: the protocol's way of carrying
            // something larger than its own length field.
            if len == packet::MAX_PAYLOAD {
                if self.partial.len() + len > MAX_COMMAND {
                    self.phase = Phase::Closed;

                    return pos;
                }
                self.partial.extend_from_slice(payload);

                continue;
            }

            if self.partial.is_empty() {
                self.frame(seq, payload, out);
            } else {
                self.partial.extend_from_slice(payload);
                let whole = std::mem::take(&mut self.partial);
                self.frame(seq, &whole, out);
            }
        }
    }

    /// One framed packet: the handshake response, or one command.
    fn frame(&mut self, request_seq: u8, payload: &[u8], out: &mut Vec<u8>) {
        // The reply's first packet follows the request's own sequence number,
        // which is what a pipelining client counts on: every command starts at
        // zero again, and its answer starts at one.
        self.seq = request_seq.wrapping_add(1);

        match self.phase {
            Phase::Handshake => self.hello(payload, out),
            Phase::Commands => {
                let Some((&command, body)) = payload.split_first() else {
                    // An empty command packet is a protocol error, not an idle
                    // one: a client that sends it has lost the framing.
                    self.error(out, 1047, "42000", "empty command packet");

                    return;
                };
                self.dispatch(command, body, out);
            }
            Phase::Closed => {}
        }
    }

    /// The handshake response, answered with an OK packet or a refusal.
    fn hello(&mut self, payload: &[u8], out: &mut Vec<u8>) {
        match handshake::parse_response(payload) {
            Some(handshake::Response::Ssl) => {
                // **No TLS, and the client is told so.** It asked for a TLS
                // handshake this server cannot complete; an error packet is what
                // a client can report, and closing afterwards stops it waiting
                // for a greeting that will not come.
                result::err(
                    out,
                    &mut self.seq,
                    1043,
                    "08S01",
                    "TLS is not supported by db-noop: connect without TLS",
                );
                self.phase = Phase::Closed;
            }
            Some(handshake::Response::Login { caps, plugin }) => {
                self.caps = caps;
                if plugin.as_deref() == Some(handshake::CACHING_SHA2) {
                    // **The fast-auth success path.** `caching_sha2_password`
                    // normally has the server check a hash; this server accepts
                    // any credentials, so the answer is the "proceed" that ends
                    // the exchange -- a full authentication would need an RSA
                    // exchange, which has nothing to protect here and does not
                    // work without TLS.
                    packet::write(out, &mut self.seq, &[0x01, 0x03]);
                }
                let status = self.status(false);
                result::ok(out, &mut self.seq, 0, 0, status);
                self.phase = Phase::Commands;
            }
            None => {
                result::err(
                    out,
                    &mut self.seq,
                    1043,
                    "08S01",
                    "malformed handshake response",
                );
                self.phase = Phase::Closed;
            }
        }
    }

    fn error(&mut self, out: &mut Vec<u8>, code: u16, state: &str, message: &str) {
        result::err(out, &mut self.seq, code, state, message);
    }

    fn dispatch(&mut self, command: u8, body: &[u8], out: &mut Vec<u8>) {
        let status = self.status(false);

        match command {
            cmd::QUIT => self.phase = Phase::Closed,

            cmd::PING
            | cmd::INIT_DB
            | cmd::CREATE_DB
            | cmd::DROP_DB
            | cmd::REFRESH
            | cmd::PROCESS_KILL
            | cmd::DEBUG
            | cmd::SHUTDOWN => result::ok(out, &mut self.seq, 0, 0, status),

            // A connection that changes user or is reset starts over: no open
            // transaction, no prepared statements. Both are answered with a
            // plain OK, which a client accepts as "authenticated, and clean".
            cmd::CHANGE_USER | cmd::RESET_CONNECTION => {
                self.in_trans = false;
                self.stmts.clear();
                let status = self.status(false);
                result::ok(out, &mut self.seq, 0, 0, status);
            }

            cmd::QUERY => self.query(body, out),

            // The old table-description command: this server has no fields to
            // describe, and an EOF is how the protocol says so.
            cmd::FIELD_LIST => result::eof(out, &mut self.seq, status),

            // One human-readable packet, not an OK: a client that asks for
            // statistics prints the string.
            cmd::STATISTICS => packet::write(
                out,
                &mut self.seq,
                b"Uptime: 0  Threads: 1  Questions: 0  Slow queries: 0  Opens: 0  \
                  Flush tables: 0  Open tables: 0",
            ),

            cmd::PROCESS_INFO => {
                let columns: Vec<Column<'_>> = [
                    "Id", "User", "Host", "db", "Command", "Time", "State", "Info",
                ]
                .into_iter()
                .map(|name| Column {
                    name,
                    kind: Kind::Text,
                })
                .collect();
                let deprecate = self.deprecate_eof();
                result::header(out, &mut self.seq, &columns, status, deprecate);
                result::finish(out, &mut self.seq, status, deprecate);
            }

            cmd::SET_OPTION => {
                // `[option:u16]`: 0 is MULTI_STATEMENTS on, 1 off, and there is
                // no other option. The reply is an EOF packet, which is what the
                // protocol specifies for this command even where EOF is
                // deprecated for result sets.
                if let Some(option) = Reader::new(body).u16() {
                    match option {
                        0 => self.caps |= handshake::CLIENT_MULTI_STATEMENTS,
                        1 => self.caps &= !handshake::CLIENT_MULTI_STATEMENTS,
                        _ => {}
                    }
                }
                result::eof(out, &mut self.seq, status);
            }

            cmd::STMT_PREPARE => self.prepare(body, out),
            cmd::STMT_EXECUTE => self.execute(body, out),
            cmd::STMT_CLOSE => {
                if let Some(id) = Reader::new(body).u32() {
                    // No reply at all: the client has already moved on, and a
                    // packet it is not waiting for desynchronises the stream.
                    self.stmts.remove(&id);
                }
            }
            cmd::STMT_SEND_LONG_DATA => {
                // A parameter being streamed in. This server never looks at a
                // parameter, and the command has no reply.
            }
            cmd::STMT_RESET => match Reader::new(body).u32() {
                Some(id) if self.stmts.contains_key(&id) => {
                    result::ok(out, &mut self.seq, 0, 0, status)
                }
                _ => self.error(out, 1243, "HY000", "unknown prepared statement handler"),
            },

            // Cursors: this server answers every row at execute time and has
            // none left to fetch.
            cmd::STMT_FETCH => self.error(out, 1047, "42000", "cursors are not supported"),

            other => {
                let message = format!("unsupported command 0x{other:02x}");
                self.error(out, 1047, "42000", &message);
            }
        }
    }

    /// `COM_QUERY`: one statement, or several when the client negotiated it.
    fn query(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let Ok(sql) = std::str::from_utf8(body) else {
            self.error(out, 1064, "42000", "statement is not valid UTF-8");

            return;
        };

        // An empty segment -- `a;;b`, or a trailing `;` -- is not a statement:
        // answering one would be a result the client is not counting on, and
        // leaving `MORE_RESULTS_EXISTS` on it would make the client wait for a
        // result that never comes.
        let statements: Vec<&str> = if self.multi_statements() {
            split_statements(sql)
        } else {
            vec![sql]
        };
        let statements: Vec<&str> = statements
            .into_iter()
            .filter(|s| !s.trim().is_empty())
            .collect();

        if statements.is_empty() {
            // A query of nothing but whitespace, or a lone `;`: MySQL answers an
            // OK packet, and so does this.
            let status = self.status(false);
            result::ok(out, &mut self.seq, 0, 0, status);

            return;
        }

        let total = statements.len();
        for (i, statement) in statements.iter().enumerate() {
            let answer = classify(statement);
            self.apply(&answer);
            self.answer(&answer, out, i + 1 < total);
        }
    }

    /// Note what a statement does to the session, before it is answered so that
    /// the packet cannot disagree with the state.
    fn apply(&mut self, answer: &Answer) {
        match answer.tx {
            Tx::None => {}
            Tx::Begin => self.in_trans = true,
            Tx::Commit | Tx::Rollback => self.in_trans = false,
        }
    }

    /// Write one statement's answer, with `more` set when another follows.
    fn answer(&mut self, answer: &Answer, out: &mut Vec<u8>, more: bool) {
        let status = self.status(more);
        let deprecate = self.deprecate_eof();

        match &answer.shape {
            Shape::Ok => result::ok(out, &mut self.seq, 0, 0, status),
            Shape::Empty(columns) => {
                let columns: Vec<Column<'_>> = columns
                    .iter()
                    .map(|name| Column {
                        name,
                        kind: Kind::Text,
                    })
                    .collect();
                result::header(out, &mut self.seq, &columns, status, deprecate);
                result::finish(out, &mut self.seq, status, deprecate);
            }
        }
    }

    /// `COM_STMT_PREPARE`: the statement id, its parameter and column counts,
    /// and the definitions themselves.
    fn prepare(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let Ok(sql) = std::str::from_utf8(body) else {
            self.error(out, 1064, "42000", "statement is not valid UTF-8");

            return;
        };

        let answer = classify(sql);
        let params = count_placeholders(sql);
        let id = self.next_stmt;
        self.next_stmt += 1;

        let deprecate = self.deprecate_eof();
        let status = self.status(false);
        let columns: Vec<Column<'_>> = answer
            .columns()
            .iter()
            .map(|name| Column {
                name,
                kind: Kind::Text,
            })
            .collect();

        let at = packet::begin(out, &mut self.seq);
        out.push(0x00); // the OK header of a prepare response
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
        out.extend_from_slice(&params.to_le_bytes());
        out.push(0x00); // reserved
        out.extend_from_slice(&0u16.to_le_bytes()); // warnings
        packet::end(out, at);

        for _ in 0..params {
            // A parameter has no name and no type until it is bound.
            result::definition(
                out,
                &mut self.seq,
                &Column {
                    name: "?",
                    kind: Kind::Text,
                },
            );
        }
        if !deprecate && params > 0 {
            result::eof(out, &mut self.seq, status);
        }
        if !columns.is_empty() {
            result::header(out, &mut self.seq, &columns, status, deprecate);
        }

        self.stmts.insert(id, Prepared { answer });
    }

    /// `COM_STMT_EXECUTE`: the statement's answer, with its parameters ignored.
    ///
    /// The body is `[id:u32][flags:u8][iteration:u32]` and then the bound
    /// parameters in whichever types the client chose. This server never reads a
    /// parameter, so the packet is framed and dispatched without being decoded
    /// at all.
    fn execute(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let Some(id) = Reader::new(body).u32() else {
            self.error(out, 1064, "42000", "malformed execute packet");

            return;
        };
        let answer = match self.stmts.get(&id) {
            Some(prepared) => prepared.answer.clone(),
            None => {
                self.error(out, 1243, "HY000", "unknown prepared statement handler");

                return;
            }
        };

        self.apply(&answer);
        self.answer(&answer, out, false);
    }
}

/// What a statement answers, from its text alone.
fn classify(sql: &str) -> Answer {
    let trimmed = trim_leading(sql);
    let head = first_word(trimmed);

    let tx = if head.eq_ignore_ascii_case("BEGIN") || starts_transaction(trimmed) {
        Tx::Begin
    } else if head.eq_ignore_ascii_case("COMMIT") {
        Tx::Commit
    } else if head.eq_ignore_ascii_case("ROLLBACK") {
        // `ROLLBACK TO SAVEPOINT x` keeps the transaction open; only a plain
        // rollback ends one.
        if mentions(trimmed, "SAVEPOINT") {
            Tx::None
        } else {
            Tx::Rollback
        }
    } else {
        Tx::None
    };

    let shape = if head.eq_ignore_ascii_case("SELECT") {
        Shape::Empty(select_columns(trimmed))
    } else if head.eq_ignore_ascii_case("SHOW") {
        // Every SHOW this server answers has the two-column shape of
        // `SHOW STATUS`, by far the one a benchmark client asks for.
        Shape::Empty(vec!["Variable_name".to_string(), "Value".to_string()])
    } else if ["DESCRIBE", "DESC", "EXPLAIN", "WITH", "TABLE", "VALUES"]
        .iter()
        .any(|kw| head.eq_ignore_ascii_case(kw))
    {
        // Statements that may return rows but have no `SELECT` to read the shape
        // from. `CALL` is deliberately NOT here: a stored procedure call answers
        // an OK packet, which is what a client that then reads its OUT
        // parameters from `SELECT @...` expects (see `crate::tpcc`).
        Shape::Empty(vec!["result".to_string()])
    } else {
        Shape::Ok
    };

    Answer { shape, tx }
}

/// `START TRANSACTION`, with any of its modifiers.
fn starts_transaction(sql: &str) -> bool {
    let mut words = Words::new(sql);
    if !words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("START"))
    {
        return false;
    }

    words
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("TRANSACTION"))
}

/// Whether the statement mentions a word anywhere, outside quotes and comments.
fn mentions(sql: &str, word: &str) -> bool {
    Words::new(sql).any(|w| w.eq_ignore_ascii_case(word))
}

/// The columns of a `SELECT`, named after the select-list items.
///
/// A name is the item's own text, trimmed and cut short: a client that reads a
/// column name gets something it recognises from the statement it wrote, and
/// nothing here tries to evaluate the expression.
fn select_columns(sql: &str) -> Vec<String> {
    let Some(list) = select_list(sql) else {
        return vec!["result".to_string()];
    };
    let columns: Vec<String> = split_top_level(list)
        .into_iter()
        .map(|item| {
            let name = item.trim();
            if name.is_empty() {
                return "result".to_string();
            }
            if name.len() <= MAX_COLUMN_NAME {
                return name.to_string();
            }
            // Cut on a character boundary: a multi-byte name is still a name.
            let mut end = MAX_COLUMN_NAME;
            while !name.is_char_boundary(end) {
                end -= 1;
            }

            name[..end].to_string()
        })
        .collect();

    if columns.is_empty() {
        vec!["result".to_string()]
    } else {
        columns
    }
}

/// The select list: the text between `SELECT` (and `DISTINCT`) and the first
/// top-level `FROM`, or the end of the statement.
fn select_list(sql: &str) -> Option<&str> {
    let bytes = sql.as_bytes();
    let mut i = skip_space(bytes, 0);
    // `SELECT`
    let end = word_end(bytes, i)?;
    i = skip_space(bytes, end);
    // `DISTINCT` / `ALL` / `DISTINCTROW`
    if let Some(end) = word_end(bytes, i) {
        if ["DISTINCT", "ALL", "DISTINCTROW"]
            .iter()
            .any(|kw| sql[i..end].eq_ignore_ascii_case(kw))
        {
            i = skip_space(bytes, end);
        }
    }

    let from = find_keyword(sql, i, "FROM");
    let stop = from.unwrap_or(bytes.len());
    let list = &sql[i..stop];
    if list.trim().is_empty() {
        return None;
    }

    Some(list)
}

/// The offset of the first top-level `keyword` at or after `from`.
///
/// Top level is outside parentheses, quotes and comments: `select coalesce(a,
/// 'from')` names no table, and `select (select 1 from t)` does.
fn find_keyword(sql: &str, from: usize, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = from;
    let mut depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            c if depth == 0 && is_word_byte(c) => {
                let end = word_end(bytes, i)?;
                if sql[i..end].eq_ignore_ascii_case(keyword) {
                    return Some(i);
                }
                i = end;
            }
            _ => i += 1,
        }
    }

    None
}

/// Split a multi-statement query on top-level `;`, keeping empty segments: which
/// of them answer is the caller's rule.
fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            b';' => {
                out.push(&sql[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    if start < bytes.len() {
        out.push(&sql[start..]);
    }

    out
}

/// Split on top-level commas.
fn split_top_level(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut depth = 0usize;

    while i < bytes.len() {
        match bytes[i] {
            b'(' => {
                depth += 1;
                i += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
            b',' if depth == 0 => {
                out.push(&text[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    out.push(&text[start..]);

    out
}

/// How many placeholders a statement has: `?` outside quotes, comments and
/// identifiers.
fn count_placeholders(sql: &str) -> u16 {
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut count = 0u16;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => i = skip_quoted(bytes, i),
            b'-' | b'#' | b'/' => match skip_comment(bytes, i) {
                Some(next) => i = next,
                None => i += 1,
            },
            b'?' => {
                count = count.saturating_add(1);
                i += 1;
            }
            _ => i += 1,
        }
    }

    count
}

/// Skip a quoted run, `i` at its opening quote. A doubled quote inside is an
/// escaped one, which is how SQL spells a quote in a literal.
fn skip_quoted(bytes: &[u8], i: usize) -> usize {
    let quote = bytes[i];
    let mut i = i + 1;
    // A backtick is an identifier: it has no backslash escape.
    let escapes = quote != b'`';
    while i < bytes.len() {
        if escapes && bytes[i] == b'\\' {
            i += 2;

            continue;
        }
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;

                continue;
            }

            return i + 1;
        }
        i += 1;
    }

    bytes.len()
}

/// Skip a comment, `i` at its first byte, or `None` when what is there only
/// looks like one (`-` that is not `--`, `/` that is not `/*`).
fn skip_comment(bytes: &[u8], i: usize) -> Option<usize> {
    match *bytes.get(i)? {
        b'#' => Some(line_end(bytes, i + 1)),
        b'-' if bytes.get(i + 1) == Some(&b'-') => Some(line_end(bytes, i + 2)),
        b'/' if bytes.get(i + 1) == Some(&b'*') => {
            let mut j = i + 2;
            while j + 1 < bytes.len() {
                if bytes[j] == b'*' && bytes[j + 1] == b'/' {
                    return Some(j + 2);
                }
                j += 1;
            }

            Some(bytes.len())
        }
        _ => None,
    }
}

fn line_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }

    i
}

fn skip_space(bytes: &[u8], mut i: usize) -> usize {
    loop {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        match skip_comment(bytes, i) {
            // A comment is whitespace: `select /* x */ 1` is `select 1`.
            Some(next) if next > i => i = next,
            _ => return i,
        }
    }
}

/// The statement with leading whitespace and comments removed.
fn trim_leading(sql: &str) -> &str {
    let start = skip_space(sql.as_bytes(), 0);

    &sql[start..]
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// The end of the word starting at `i`, or `None` when there is none there.
fn word_end(bytes: &[u8], i: usize) -> Option<usize> {
    if i >= bytes.len() || !is_word_byte(bytes[i]) {
        return None;
    }
    let mut j = i;
    while j < bytes.len() && is_word_byte(bytes[j]) {
        j += 1;
    }

    Some(j)
}

/// The first word of a statement.
fn first_word(sql: &str) -> &str {
    let bytes = sql.as_bytes();
    let start = skip_space(bytes, 0);
    match word_end(bytes, start) {
        Some(end) => &sql[start..end],
        None => "",
    }
}

/// The words of a statement, outside quotes and comments -- all this codec reads
/// of a statement it does not interpret.
struct Words<'a> {
    sql: &'a str,
    pos: usize,
}

impl<'a> Words<'a> {
    fn new(sql: &'a str) -> Self {
        Words { sql, pos: 0 }
    }
}

impl<'a> Iterator for Words<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        // Copied out so that a word's borrow is the statement's, not the scan's.
        let sql: &'a str = self.sql;
        let bytes = sql.as_bytes();

        loop {
            self.pos = skip_space(bytes, self.pos);
            if self.pos >= bytes.len() {
                return None;
            }
            match bytes[self.pos] {
                // A literal or a quoted identifier is not a word.
                b'\'' | b'"' | b'`' => self.pos = skip_quoted(bytes, self.pos),
                c if is_word_byte(c) => {
                    let end = word_end(bytes, self.pos).unwrap_or(bytes.len());
                    let word = &sql[self.pos..end];
                    self.pos = end;

                    return Some(word);
                }
                _ => self.pos += 1,
            }
        }
    }
}

#[cfg(test)]
mod tests;
