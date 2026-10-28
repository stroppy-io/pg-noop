//! Drive the server over a real socket with a MySQL client.
//!
//! There is no MySQL client crate in this workspace's registry, and a sandbox
//! without a network cannot fetch one, so the client here is written against the
//! same protocol the server speaks -- which has an advantage for this job: a
//! driver would hide exactly the packets being tested (a terminator, a sequence
//! number, a status flag).
//!
//! What it drives is the REAL server: the binary this workspace builds, on a
//! port the kernel picks, through the same accept, receive and send path
//! `saturate` measures.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The server, killed when the test ends.
struct Server {
    child: Child,
    postgres: u16,
    mysql: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A server on both protocols with the kernel's own ports, discovered from its
/// banner.
fn server() -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pgnoop"))
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "0",
            "--mysql-port",
            "0",
            "--workers",
            "1",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the server binary starts");

    let stderr = child.stderr.take().expect("stderr is piped");
    // **The pipe has to stay drained for the life of the child.** A child whose
    // stderr nobody reads blocks when the pipe fills, and one that writes to a
    // pipe whose reader has gone dies -- which is why the banner lines are read
    // by a thread that keeps reading afterwards rather than by a reader the
    // function drops.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    // Ignored: after the test ends nobody is listening, and the
                    // pipe still needs emptying.
                    let _ = tx.send(line.clone());
                }
            }
        }
    });

    let mut postgres = None;
    let mut mysql = None;
    let deadline = Instant::now() + Duration::from_secs(20);

    while postgres.is_none() || mysql.is_none() {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = rx
            .recv_timeout(left)
            .unwrap_or_else(|_| panic!("the server did not announce both listeners"));
        for (protocol, slot) in [
            ("postgres listening on ", &mut postgres),
            ("mysql listening on ", &mut mysql),
        ] {
            if let Some(at) = line.find(protocol) {
                // `... listening on 127.0.0.1:PORT (N shards, ...)`.
                let addr = line[at + protocol.len()..]
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .trim();
                let port = addr
                    .rsplit(':')
                    .next()
                    .and_then(|p| p.trim().parse().ok())
                    .unwrap_or_else(|| panic!("a port in {line:?}"));
                *slot = Some(port);
            }
        }
    }

    Server {
        child,
        postgres: postgres.expect("a postgres port"),
        mysql: mysql.expect("a mysql port"),
    }
}

// Capability bits, by their names in `include/mysql_com.h`.
const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
const CLIENT_PS_MULTI_RESULTS: u32 = 0x0004_0000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

const CAPS: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PS_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_DEPRECATE_EOF;

// Commands.
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_PING: u8 = 0x0e;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_CLOSE: u8 = 0x19;
const COM_RESET_CONNECTION: u8 = 0x1f;

// Server status flags.
const IN_TRANS: u16 = 0x0001;
const AUTOCOMMIT: u16 = 0x0002;
const MORE_RESULTS_EXISTS: u16 = 0x0008;

/// A hand-written MySQL client: framing and the few packets the tests read.
struct Client {
    stream: TcpStream,
}

/// One packet: its sequence number and its body.
#[derive(Debug)]
struct Packet {
    seq: u8,
    body: Vec<u8>,
}

impl Packet {
    /// A length-encoded integer at `at`, and where it ended.
    fn lenenc(&self, at: usize) -> (u64, usize) {
        match self.body[at] {
            n @ 0..=0xfa => (n as u64, at + 1),
            0xfc => (
                u16::from_le_bytes([self.body[at + 1], self.body[at + 2]]) as u64,
                at + 3,
            ),
            0xfd => (
                u32::from_le_bytes([self.body[at + 1], self.body[at + 2], self.body[at + 3], 0])
                    as u64,
                at + 4,
            ),
            0xfe => {
                let mut n = [0u8; 8];
                n.copy_from_slice(&self.body[at + 1..at + 9]);
                (u64::from_le_bytes(n), at + 9)
            }
            other => panic!("not a length-encoded integer: {other:#x}"),
        }
    }

    /// An OK packet's status flags.
    fn status(&self) -> u16 {
        let (_, at) = self.lenenc(1);
        let (_, at) = self.lenenc(at);
        u16::from_le_bytes([self.body[at], self.body[at + 1]])
    }

    /// An ERR packet's code and message.
    fn error(&self) -> (u16, String) {
        let code = u16::from_le_bytes([self.body[1], self.body[2]]);
        let message = String::from_utf8_lossy(&self.body[9..])
            .trim_end_matches('\0')
            .to_string();

        (code, message)
    }
}

impl Client {
    /// Connect, answer the greeting, and check the server accepted it.
    fn connect(port: u16) -> Client {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream.set_nodelay(true).expect("nodelay");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .expect("write timeout");
        let mut client = Client { stream };

        // The server greets first, and a client waits for it.
        let greeting = client.read();
        assert_eq!(greeting.seq, 0);
        assert_eq!(greeting.body[0], 10, "HandshakeV10");
        let version_end = greeting.body[1..]
            .iter()
            .position(|&b| b == 0)
            .expect("a server version");
        let version = String::from_utf8_lossy(&greeting.body[1..1 + version_end]).to_string();
        assert!(version.contains("db-noop"), "{version}");

        client.login();
        client
    }

    fn write_packet(&mut self, seq: u8, body: &[u8]) {
        let mut packet = Vec::with_capacity(body.len() + 4);
        packet.push((body.len() & 0xff) as u8);
        packet.push(((body.len() >> 8) & 0xff) as u8);
        packet.push(((body.len() >> 16) & 0xff) as u8);
        packet.push(seq);
        packet.extend_from_slice(body);
        self.stream.write_all(&packet).expect("write");
    }

    fn read(&mut self) -> Packet {
        let mut head = [0u8; 4];
        self.stream.read_exact(&mut head).expect("a packet header");
        let len = head[0] as usize | (head[1] as usize) << 8 | (head[2] as usize) << 16;
        let mut body = vec![0u8; len];
        self.stream.read_exact(&mut body).expect("a packet body");

        Packet { seq: head[3], body }
    }

    /// `HandshakeResponse41`, then the OK packet that answers it.
    fn login(&mut self) {
        let mut body = Vec::new();
        body.extend_from_slice(&CAPS.to_le_bytes());
        body.extend_from_slice(&16_777_216u32.to_le_bytes());
        body.push(45); // utf8mb4_general_ci
        body.extend_from_slice(&[0u8; 23]);
        body.extend_from_slice(b"bench\0");
        body.push(20); // the auth response's length
        body.extend_from_slice(&[0x41; 20]);
        body.extend_from_slice(b"tpcc\0");
        body.extend_from_slice(b"mysql_native_password\0");
        self.write_packet(1, &body);

        let ok = self.read();
        assert_eq!(ok.seq, 2);
        assert_eq!(ok.body[0], 0x00, "an OK packet: {:?}", ok.body);
        assert_eq!(ok.status() & AUTOCOMMIT, AUTOCOMMIT);
    }

    /// One command, and the bytes of its answer.
    fn command(&mut self, command: u8, body: &[u8]) -> Vec<u8> {
        let mut payload = Vec::with_capacity(body.len() + 1);
        payload.push(command);
        payload.extend_from_slice(body);
        self.write_packet(0, &payload);

        // A command's answer may be several packets; the caller drains them.
        Vec::new()
    }

    /// Run `sql`, returning its answer's packets, in order.
    fn query(&mut self, sql: &str) -> Vec<Packet> {
        self.command(COM_QUERY, sql.as_bytes());
        self.answer()
    }

    /// Read one answer: a result set up to its terminator, or a single OK or ERR
    /// packet -- and, while the status says another answer follows, the next one
    /// too, so a multi-statement query's answers come back in order.
    fn answer(&mut self) -> Vec<Packet> {
        let mut all = Vec::new();
        loop {
            let first = self.read();
            let more = match first.body[0] {
                0x00 => {
                    let more = first.status() & MORE_RESULTS_EXISTS != 0;
                    all.push(first);

                    more
                }
                0xff => {
                    all.push(first);

                    return all;
                }
                _ => {
                    // A result set: the column count, a definition per column,
                    // then rows until a terminator -- which is fewer than nine
                    // bytes, and that is what tells it from a row.
                    let (columns, _) = first.lenenc(0);
                    all.push(first);
                    for _ in 0..columns {
                        let packet = self.read();
                        all.push(packet);
                    }
                    let mut more = false;
                    loop {
                        let packet = self.read();
                        let last = packet.body[0] == 0xfe && packet.body.len() < 9;
                        if last {
                            more = packet.status() & MORE_RESULTS_EXISTS != 0;
                        }
                        all.push(packet);
                        if last {
                            break;
                        }
                    }

                    more
                }
            };
            if !more {
                return all;
            }
        }
    }
}

/// **A connection, a query and a reply, over a real socket.**
#[test]
fn a_client_connects_queries_and_quits() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    // A statement that changes nothing answers an OK packet with no rows.
    let answer = client.query("create table if not exists warehouse (id int)");
    assert_eq!(answer.len(), 1);
    assert_eq!(answer[0].body[0], 0x00, "an OK packet");
    assert_eq!(answer[0].status() & AUTOCOMMIT, AUTOCOMMIT);

    // A SELECT answers a well-formed result set with no rows: the column count,
    // the definitions, and the terminator -- which the server sends as an OK
    // packet with a `0xfe` header, because this client gave up EOF.
    let answer = client.query("select w_id, w_name from warehouse");
    assert_eq!(answer.len(), 4, "{answer:?}");
    assert_eq!(answer[0].lenenc(0).0, 2, "two columns");
    assert_eq!(answer[3].body[0], 0xfe, "the terminator");

    let ping = client.command(COM_PING, &[]);
    let _ = ping;
    let answer = client.answer();
    assert_eq!(answer[0].body[0], 0x00);

    let init = client.command(COM_INIT_DB, b"tpcc");
    let _ = init;
    assert_eq!(client.answer()[0].body[0], 0x00);

    // And a quit is answered with a closed connection, not a packet.
    client.command(COM_QUIT, &[]);
    let mut byte = [0u8; 1];
    let read = client.stream.read(&mut byte);
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "the server closes on COM_QUIT: {read:?}"
    );
}

/// **A multi-statement query is answered one statement at a time**, with
/// `MORE_RESULTS_EXISTS` on every answer but the last -- which is what a client
/// loops on.
#[test]
fn a_multi_statement_query_is_answered_in_order() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    let answer = client.query("set @x = 1; select 1; set @y = 2");
    // OK, then the result set's column count, its definition and its
    // terminator, then the last OK.
    assert_eq!(answer.len(), 5, "{answer:?}");

    // First: an OK packet with more to come.
    assert_eq!(answer[0].body[0], 0x00);
    assert_ne!(answer[0].status() & MORE_RESULTS_EXISTS, 0);

    // Second: the result set, whose terminator also says more.
    assert_eq!(answer[3].body[0], 0xfe);
    assert_ne!(answer[3].status() & MORE_RESULTS_EXISTS, 0);

    // Last: an OK packet with the sequence complete.
    assert_eq!(answer[4].body[0], 0x00);
    assert_eq!(answer[4].status() & MORE_RESULTS_EXISTS, 0);
}

/// **A prepared statement is prepared, executed twice and closed**, and a closed
/// one is refused: the client's statement cache is what this exists for.
#[test]
fn a_prepared_statement_survives_being_executed_twice() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    client.command(
        COM_STMT_PREPARE,
        b"select d_next_o_id from district where d_id = ?",
    );
    let prepare = client.read();
    assert_eq!(prepare.body[0], 0x00, "a prepare response");
    let id = u32::from_le_bytes([
        prepare.body[1],
        prepare.body[2],
        prepare.body[3],
        prepare.body[4],
    ]);
    let columns = u16::from_le_bytes([prepare.body[5], prepare.body[6]]);
    let params = u16::from_le_bytes([prepare.body[7], prepare.body[8]]);
    assert_eq!((columns, params), (1, 1));
    // The parameter definition and the column definition follow.
    let _ = client.read();
    let _ = client.read();
    let _ = client.read();

    for _ in 0..2 {
        let mut body = Vec::new();
        body.extend_from_slice(&id.to_le_bytes());
        body.push(0x00); // no cursor
        body.extend_from_slice(&1u32.to_le_bytes()); // iteration count
        body.push(0x00); // null bitmap: nothing NULL
        body.push(0x01); // parameter types follow
        body.push(0x03); // MYSQL_TYPE_LONG
        body.push(0x00);
        body.extend_from_slice(&1i32.to_le_bytes());
        client.command(COM_STMT_EXECUTE, &body);

        let answer = client.answer();
        assert_eq!(answer[0].lenenc(0).0, 1, "one column");
        assert_eq!(answer[2].body[0], 0xfe, "a terminator, so no rows remain");
    }

    client.command(COM_STMT_CLOSE, &id.to_le_bytes());
    // No reply to a close: the next answer must belong to the next command.
    client.command(COM_PING, &[]);
    assert_eq!(client.answer()[0].body[0], 0x00, "still in step");

    // The statement is gone, and executing it says so.
    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    client.command(COM_STMT_EXECUTE, &body);
    let answer = client.answer();
    assert_eq!(answer[0].body[0], 0xff, "an ERR packet");
    assert_eq!(answer[0].error().0, 1243);
}

/// **A transaction's state is visible in the status flags**, which is how a pool
/// knows the connection it is about to hand out has one open.
#[test]
fn a_transaction_shows_in_the_status_of_every_answer() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    let answer = client.query("begin");
    assert_ne!(answer[0].status() & IN_TRANS, 0, "a transaction is open");

    let answer = client.query("select 1");
    let terminator = answer.last().expect("an answer");
    assert_ne!(terminator.status() & IN_TRANS, 0, "and it stays open");

    let answer = client.query("commit");
    assert_eq!(answer[0].status() & IN_TRANS, 0, "committed");

    let answer = client.query("begin");
    assert_ne!(answer[0].status() & IN_TRANS, 0);
    let answer = client.query("rollback");
    assert_eq!(answer[0].status() & IN_TRANS, 0, "rolled back");

    // A reset connection starts with no transaction.
    client.query("begin");
    client.command(COM_RESET_CONNECTION, &[]);
    let answer = client.answer();
    assert_eq!(answer[0].status() & IN_TRANS, 0);
}

/// **Pipelined commands are answered in order, one answer per command.** A
/// client that writes three commands before reading any gets three answers, in
/// the order it asked, each numbered from one.
#[test]
fn pipelined_commands_are_answered_in_order() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    for sql in ["select 1", "select 1, 1", "select 1, 1, 1"] {
        client.command(COM_QUERY, sql.as_bytes());
    }

    for columns in [1u64, 2, 3] {
        let answer = client.answer();
        assert_eq!(
            answer[0].lenenc(0).0,
            columns,
            "the answer is not the next command's"
        );
        assert_eq!(answer[0].seq, 1, "every answer starts at sequence one");
    }
}

/// **A MySQL connection is greeted before it says anything**, and a client that
/// connects to the PostgreSQL port is not greeted at all: the two protocols are
/// separate listeners with separate codecs.
#[test]
fn each_port_speaks_its_own_protocol() {
    let server = server();

    let mut mysql = TcpStream::connect(("127.0.0.1", server.mysql)).expect("connect");
    mysql
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut head = [0u8; 4];
    mysql.read_exact(&mut head).expect("the greeting");
    let len = head[0] as usize;
    let mut body = vec![0u8; len];
    mysql.read_exact(&mut body).expect("the greeting body");
    assert_eq!(body[0], 10, "HandshakeV10 on the MySQL port");

    // The PostgreSQL port says nothing until it is spoken to.
    let mut pg = TcpStream::connect(("127.0.0.1", server.postgres)).expect("connect");
    pg.set_read_timeout(Some(Duration::from_millis(300)))
        .expect("read timeout");
    let mut byte = [0u8; 1];
    assert!(
        pg.read(&mut byte).is_err(),
        "the PostgreSQL port must wait for a startup message"
    );

    // And it answers one.
    let mut startup = 196_608i32.to_be_bytes().to_vec();
    startup.extend_from_slice(b"user\0bench\0\0");
    let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
    framed.extend_from_slice(&startup);
    pg.write_all(&framed).expect("startup");
    pg.set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut frame = [0u8; 5];
    pg.read_exact(&mut frame).expect("a reply frame");
    assert_eq!(frame[0], b'R', "AuthenticationOk");
}
