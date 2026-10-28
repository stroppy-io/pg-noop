//! The MySQL/MariaDB wire protocol, as a sans-io codec.
//! An answer is decided by the statement's shape and never by its data, so a value
//! that arrives is never interpreted.

mod handshake;
mod packet;
mod result;

use std::collections::HashMap;

use packet::Reader;
use result::{Column, Kind};

use crate::sql;
use crate::tpcc;

pub use handshake::{greeting, SERVER_VERSION};

// Server status flags, by their names in `include/mysql_com.h`.
// A client reads them: `IN_TRANS` is how a pool knows a transaction is open, and
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
const MAX_COMMAND: usize = 32 * 1024 * 1024;

/// The longest a column name taken from the SQL text may be.
/// A select-list item is echoed as its column's name, and a client that reads the name
/// should be able to use it: a 200-byte expression is not a name.
const MAX_COLUMN_NAME: usize = 64;

/// What a statement answers.
#[derive(Clone, PartialEq, Eq, Debug)]
enum Shape {
    /// An OK packet, with zero affected rows.
    Ok,
    /// A result set with no rows and these columns.
    Empty(Vec<String>),
    /// `SHOW GLOBAL STATUS`, with the `LIKE` filter the client gave it: the
    /// `Com_commit` and `Com_rollback` counters.
    Status(Option<String>),
    /// `SELECT @...`: one row of plausible values, one column per variable.
    Vars(Vec<String>),
    /// A failure the client asked for, answered with the error it names.
    Failure(tpcc::Failure),
    /// A one-number answer the TPC-C profile states: its column name and value.
    Number(&'static str, u64),
    /// A SELECT whose list is literals and variables: one row of exactly those values,
    /// which is a row the server states rather than invents.
    /// The rule is `crate::sql::constant_select`, the same one PostgreSQL uses.
    Constant(Vec<sql::Constant>),
}

/// The MySQL column type a constant's value is sent as.
fn constant_kind(kind: sql::ConstantKind) -> Kind {
    match kind {
        sql::ConstantKind::Integer => Kind::LongLong,
        sql::ConstantKind::Text => Kind::Text,
    }
}

/// What a statement does to the session, decided from its text and applied when it
/// runs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Tx {
    None,
    Begin,
    Commit,
    Rollback,
}

/// Everything a statement's answer needs, decided once -- at prepare for a prepared
/// statement, and at the command for everything else.
#[derive(Clone, PartialEq, Eq, Debug)]
struct Answer {
    shape: Shape,
    tx: Tx,
    /// The TPC-C transaction this statement is, when it is one: counted at EXECUTION,
    /// so a prepared statement counts once per execution.
    kind: Option<tpcc::Kind>,
}

impl Answer {
    /// The columns a result set will have, and the type of each, for the prepare
    /// response -- a client decodes a binary row by the type it was told here.
    fn columns(&self) -> Vec<(&str, Kind)> {
        match &self.shape {
            Shape::Ok => Vec::new(),
            Shape::Empty(columns) | Shape::Vars(columns) => columns
                .iter()
                .map(|name| (name.as_str(), Kind::Text))
                .collect(),

            Shape::Status(_) => vec![("Variable_name", Kind::Text), ("Value", Kind::Text)],
            Shape::Failure(_) => Vec::new(),
            Shape::Number(name, _) => vec![(*name, Kind::LongLong)],
            Shape::Constant(items) => items
                .iter()
                .map(|c| (c.name.as_str(), constant_kind(c.kind)))
                .collect(),
        }
    }
}

/// A prepared statement.
/// Nothing about it depends on a parameter's value: this server never looks at one,
/// which is why preparing one is the same work as running one.
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
    /// The capabilities the client negotiated.
    /// Zero until it has answered.
    caps: u32,
    /// The sequence number of the next packet this connection writes.
    /// Every command restarts the count from its own sequence, which is what lets a
    /// client pipeline commands.
    seq: u8,
    /// A command split across packets of 16 MiB, while its pieces arrive.
    partial: Vec<u8>,
    stmts: HashMap<u32, Prepared>,
    /// The next statement id to hand out.
    /// Ids are not reused: a client that closed one and executes it again gets "unknown
    /// statement" rather than another statement's answer.
    next_stmt: u32,
    /// `SERVER_STATUS_IN_TRANS`: a transaction is open.
    in_trans: bool,
    /// What this connection has done in the open transaction, counted when it commits
    /// and dropped if it rolls back.
    pending: tpcc::Pending,
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
            pending: tpcc::Pending::default(),
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
    /// Returns how many bytes were consumed; a partial packet at the end is left alone
    /// for the caller to keep, exactly as the PostgreSQL codec does.
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

            // A payload of exactly the maximum length means the command continues in
            // the next packet: the protocol's way of carrying something larger than its
            // own length field.
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
        // The reply's first packet follows the request's own sequence number, which is
        // what a pipelining client counts on: every command starts at zero again, and
        // its answer starts at one.
        self.seq = request_seq.wrapping_add(1);

        match self.phase {
            Phase::Handshake => self.hello(payload, out),
            Phase::Commands => {
                let Some((&command, body)) = payload.split_first() else {
                    // An empty command packet is a protocol error, not an idle one: a
                    // client that sends it has lost the framing.
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
                // No TLS, and the client is told so.
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
                    // The fast-auth success path.
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
            // transaction, no prepared statements.
            cmd::CHANGE_USER | cmd::RESET_CONNECTION => {
                self.in_trans = false;
                self.pending.discard();
                self.stmts.clear();
                let status = self.status(false);
                result::ok(out, &mut self.seq, 0, 0, status);
            }

            cmd::QUERY => self.query(body, out),

            // The old table-description command: this server has no fields to describe,
            // and an EOF is how the protocol says so.
            cmd::FIELD_LIST => result::eof(out, &mut self.seq, status),

            // One human-readable packet, not an OK: a client that asks for statistics
            // prints the string.
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
                // `[option:u16]`: 0 is MULTI_STATEMENTS on, 1 off, and there is no
                // other option.
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
                    // No reply at all: the client has already moved on, and a packet it
                    // is not waiting for desynchronises the stream.
                    self.stmts.remove(&id);
                }
            }
            cmd::STMT_SEND_LONG_DATA => {
                // A parameter being streamed in.
                // This server never looks at a parameter, and the command has no reply.
            }
            cmd::STMT_RESET => match Reader::new(body).u32() {
                Some(id) if self.stmts.contains_key(&id) => {
                    result::ok(out, &mut self.seq, 0, 0, status)
                }
                _ => self.error(out, 1243, "HY000", "unknown prepared statement handler"),
            },

            // Cursors: this server answers every row at execute time and has none left
            // to fetch.
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
        // answering one would be a result the client is not counting on, and leaving
        // `MORE_RESULTS_EXISTS` on it would make the client wait for a result that
        let statements: Vec<&str> = if self.multi_statements() {
            sql::split_statements(sql)
        } else {
            vec![sql]
        };
        let statements: Vec<&str> = statements
            .into_iter()
            .filter(|s| !s.trim().is_empty())
            .collect();

        if statements.is_empty() {
            // A query of nothing but whitespace, or a lone `;`: MySQL answers an OK
            // packet, and so does this.
            let status = self.status(false);
            result::ok(out, &mut self.seq, 0, 0, status);

            return;
        }

        let total = statements.len();
        for (i, statement) in statements.iter().enumerate() {
            let answer = classify(statement);
            self.apply(&answer);
            self.answer(&answer, out, i + 1 < total, false);
        }
    }

    /// Note what a statement does to the session, and to the counters, before it is
    /// answered so that the packet cannot disagree with the state.
    fn apply(&mut self, answer: &Answer) {
        match answer.tx {
            Tx::None => {}
            Tx::Begin => self.in_trans = true,
            Tx::Commit => {
                self.in_trans = false;
                // Counted here, not when the statement ran: a NewOrder that is rolled
                // back raises no district's next order id, and counting it when it ran
                // inflated NOPM by every failed transaction.
                self.pending.commit();
            }
            Tx::Rollback => {
                self.in_trans = false;
                self.pending.discard();
                tpcc::note_rollback();
            }
        }

        if let Some(kind) = answer.kind {
            self.pending.note(kind);
            // Autocommit is a transaction per statement.
            if !self.in_trans {
                self.pending.commit();
            }
        }
    }

    /// Write one statement's answer.
    fn answer(&mut self, answer: &Answer, out: &mut Vec<u8>, more: bool, binary: bool) {
        let status = self.status(more);
        let deprecate = self.deprecate_eof();

        match &answer.shape {
            Shape::Ok => result::ok(out, &mut self.seq, 0, 0, status),
            Shape::Failure(failure) => {
                // MySQL's numbers and states for the same three failures its clients
                // inject: a signal carries its own state, a NULL into a NOT NULL column
                // is 1048, and a CALL of something unknown is 1305.
                let (code, state, message) = match failure {
                    tpcc::Failure::Signal(code) => (1644, code.as_str(), "injected failure"),
                    tpcc::Failure::Null => (1048, "23000", "column cannot be null"),
                    tpcc::Failure::Raise => (1644, "P0001", "raised"),
                    tpcc::Failure::UnknownProcedure => (1305, "42000", "PROCEDURE does not exist"),
                };
                self.error(out, code, state, message);
            }
            Shape::Empty(columns) => {
                let columns = text_columns(columns.iter().map(String::as_str));
                result::header(out, &mut self.seq, &columns, status, deprecate);
                result::finish(out, &mut self.seq, status, deprecate);
            }
            Shape::Number(name, value) => {
                let columns = [Column {
                    name,
                    kind: Kind::LongLong,
                }];
                result::header(out, &mut self.seq, &columns, status, deprecate);
                if binary {
                    result::binary_row(
                        out,
                        &mut self.seq,
                        &[result::Value::LongLong(*value as i64)],
                    );
                } else {
                    let text = value.to_string();
                    result::row(out, &mut self.seq, &[Some(&text)]);
                }
                result::finish(out, &mut self.seq, status, deprecate);
            }
            Shape::Status(filter) => {
                let columns = text_columns(["Variable_name", "Value"]);
                result::header(out, &mut self.seq, &columns, status, deprecate);
                for (name, value) in tpcc::status_rows(filter.as_deref()) {
                    let value = value.to_string();
                    if binary {
                        result::binary_row(
                            out,
                            &mut self.seq,
                            &[result::Value::Text(name), result::Value::Text(&value)],
                        );
                    } else {
                        result::row(out, &mut self.seq, &[Some(name), Some(&value)]);
                    }
                }
                result::finish(out, &mut self.seq, status, deprecate);
            }
            Shape::Vars(names) => {
                let columns = text_columns(names.iter().map(String::as_str));
                result::header(out, &mut self.seq, &columns, status, deprecate);
                if binary {
                    let values: Vec<result::Value<'_>> = names
                        .iter()
                        .map(|_| result::Value::Text(tpcc::VAR_VALUE))
                        .collect();
                    result::binary_row(out, &mut self.seq, &values);
                } else {
                    let values: Vec<Option<&str>> =
                        names.iter().map(|_| Some(tpcc::VAR_VALUE)).collect();
                    result::row(out, &mut self.seq, &values);
                }
                result::finish(out, &mut self.seq, status, deprecate);
            }
            Shape::Constant(items) => {
                let columns: Vec<Column<'_>> = items
                    .iter()
                    .map(|c| Column {
                        name: &c.name,
                        kind: constant_kind(c.kind),
                    })
                    .collect();
                result::header(out, &mut self.seq, &columns, status, deprecate);
                if binary {
                    let values: Vec<result::Value<'_>> = items
                        .iter()
                        .map(|c| match constant_kind(c.kind) {
                            Kind::LongLong => {
                                result::Value::LongLong(c.value.parse::<i64>().unwrap_or_default())
                            }
                            Kind::Text => result::Value::Text(&c.value),
                        })
                        .collect();
                    result::binary_row(out, &mut self.seq, &values);
                } else {
                    let values: Vec<Option<&str>> =
                        items.iter().map(|c| Some(c.value.as_str())).collect();
                    result::row(out, &mut self.seq, &values);
                }
                result::finish(out, &mut self.seq, status, deprecate);
            }
        }
    }

    /// `COM_STMT_PREPARE`: the statement id, its parameter and column counts, and the
    /// definitions themselves.
    fn prepare(&mut self, body: &[u8], out: &mut Vec<u8>) {
        let Ok(sql) = std::str::from_utf8(body) else {
            self.error(out, 1064, "42000", "statement is not valid UTF-8");

            return;
        };

        let answer = classify(sql);
        let params = sql::count_placeholders(sql);
        let id = self.next_stmt;
        self.next_stmt += 1;

        let deprecate = self.deprecate_eof();
        let status = self.status(false);
        let columns: Vec<Column<'_>> = answer
            .columns()
            .into_iter()
            .map(|(name, kind)| Column { name, kind })
            .collect();

        let at = packet::begin(out, &mut self.seq);
        out.push(0x00); // the OK header of a prepare response
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
        out.extend_from_slice(&params.to_le_bytes());
        out.push(0x00); // reserved
        out.extend_from_slice(&0u16.to_le_bytes()); // warnings
        packet::end(out, at);

        // Definitions, not a result set.
        let parameters: Vec<Column<'_>> = (0..params)
            .map(|_| Column {
                // A parameter has no name and no type until it is bound.
                name: "?",
                kind: Kind::Text,
            })
            .collect();
        result::definitions(out, &mut self.seq, &parameters, status, deprecate);
        result::definitions(out, &mut self.seq, &columns, status, deprecate);

        self.stmts.insert(id, Prepared { answer });
    }

    /// `COM_STMT_EXECUTE`: the statement's answer, with its parameters ignored.
    /// The body is `[id:u32][flags:u8][iteration:u32]` and then the bound parameters in
    /// whichever types the client chose.
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
        self.answer(&answer, out, false, true);
    }
}

/// What a statement answers, from its text alone.
fn classify(sql: &str) -> Answer {
    let trimmed = sql::trim_leading(sql);
    let head = sql::first_word(trimmed);

    let tx = if head.eq_ignore_ascii_case("BEGIN") || starts_transaction(trimmed) {
        Tx::Begin
    } else if head.eq_ignore_ascii_case("COMMIT") {
        Tx::Commit
    } else if head.eq_ignore_ascii_case("ROLLBACK") {
        // `ROLLBACK TO SAVEPOINT x` keeps the transaction open; only a plain rollback
        // ends one.
        if mentions(trimmed, "SAVEPOINT") {
            Tx::None
        } else {
            Tx::Rollback
        }
    } else {
        Tx::None
    };

    // The TPC-C rules come first.
    let mut kind = None;
    let shape = if let Some(failure) = tpcc::failure(trimmed) {
        // The rollback path a client injects: an error, so its own error handling runs
        // and its rollback is counted -- an OK here turns the rollback into a commit.
        Shape::Failure(failure)
    // The synthetic answers are the profile's.
    } else if !tpcc::profile() && tpcc::number_query(trimmed).is_some() {
        Shape::Empty(select_columns(trimmed))
    } else if let Some((name, value)) = tpcc::number_query(trimmed) {
        Shape::Number(name, value)
    } else if tpcc::profile() && tpcc::show_status(trimmed).is_some() {
        Shape::Status(tpcc::show_status(trimmed).unwrap_or(None))
    } else if let Some(names) = tpcc::select_vars(trimmed) {
        Shape::Vars(names)
    } else if let Some(items) = sql::constant_select(trimmed) {
        Shape::Constant(items)
    } else if head.eq_ignore_ascii_case("SELECT") {
        Shape::Empty(select_columns(trimmed))
    } else if head.eq_ignore_ascii_case("SHOW") {
        // Every SHOW this server answers has the two-column shape of `SHOW STATUS`, by
        // far the one a benchmark client asks for.
        Shape::Empty(vec!["Variable_name".to_string(), "Value".to_string()])
    } else if ["DESCRIBE", "DESC", "EXPLAIN", "WITH", "TABLE", "VALUES"]
        .iter()
        .any(|kw| head.eq_ignore_ascii_case(kw))
    {
        // Statements that may return rows but have no `SELECT` to read the shape from.
        Shape::Empty(vec!["result".to_string()])
    } else {
        kind = tpcc::statement_kind(trimmed);
        Shape::Ok
    };

    Answer { shape, tx, kind }
}

/// One text column per name: everything this server sends as a result set is either a
/// string or the district sum.
fn text_columns<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<Column<'a>> {
    names
        .into_iter()
        .map(|name| Column {
            name,
            kind: Kind::Text,
        })
        .collect()
}

/// `START TRANSACTION`, with any of its modifiers.
fn starts_transaction(sql: &str) -> bool {
    let mut words = sql::Words::new(sql);
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
    sql::Words::new(sql).any(|w| w.eq_ignore_ascii_case(word))
}

/// The columns of a `SELECT`, named after the select-list items.
fn select_columns(sql: &str) -> Vec<String> {
    let Some(list) = sql::select_list(sql) else {
        return vec!["result".to_string()];
    };
    let columns: Vec<String> = sql::split_top_level(list)
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

#[cfg(test)]
mod tests;
