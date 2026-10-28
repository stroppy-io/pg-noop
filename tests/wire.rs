//! Drive the server over a real socket, in both protocols.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The server, killed when the test ends.
struct Server {
    child: Child,
    postgres: u16,
    mysql: u16,
    /// Every line the server has written to stderr, so a test can read what the banner
    /// claimed as well as what the ports answer.
    log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A server on both protocols with the kernel's own ports, discovered from its banner.
fn server() -> Server {
    server_with(&[])
}

/// The banner of a spawned server: the two ports the kernel gave it, read from its
/// stderr.
fn banner(mut child: Child) -> Server {
    let stderr = child.stderr.take().expect("stderr is piped");
    // The pipe has to stay drained for the life of the child.
    let (tx, rx) = std::sync::mpsc::channel();
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = std::sync::Arc::clone(&log);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {
                    if let Ok(mut log) = recorded.lock() {
                        log.push(line.clone());
                    }
                    // Ignored: after the test ends nobody is listening, and the pipe
                    // still needs emptying.
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
                // `...
                // listening on 127.0.0.1:PORT (N shards, ...)`.
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
        log,
    }
}

// Capability bits, by their names in `include/mysql_com.h`.
/// What the MySQL handshake calls itself, which `select @@version` must answer.
const SERVER_VERSION: &str = "8.0.0-db-noop";

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

    /// A length-encoded string at `at`, and where it ended.
    fn lenenc_bytes(&self, at: usize) -> (Vec<u8>, usize) {
        let (len, at) = self.lenenc(at);
        let len = len as usize;
        (self.body[at..at + len].to_vec(), at + len)
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

    /// Read one answer: a result set up to its terminator, or a single OK or ERR packet
    /// -- and, while the status says another answer follows, the next one too, so a
    /// multi-statement query's answers come back in order.
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
                    // A result set: the column count, a definition per column, then
                    // rows until a terminator -- which is fewer than nine bytes, and
                    // that is what tells it from a row.
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

/// A connection, a query and a reply, over a real socket.
#[test]
fn a_client_connects_queries_and_quits() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    // A statement that changes nothing answers an OK packet with no rows.
    let answer = client.query("create table if not exists warehouse (id int)");
    assert_eq!(answer.len(), 1);
    assert_eq!(answer[0].body[0], 0x00, "an OK packet");
    assert_eq!(answer[0].status() & AUTOCOMMIT, AUTOCOMMIT);

    // A SELECT answers a well-formed result set with no rows: the column count, the
    // definitions, and the terminator -- which the server sends as an OK packet with a
    // `0xfe` header, because this client gave up EOF.
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

/// A multi-statement query is answered one statement at a time, with
/// `MORE_RESULTS_EXISTS` on every answer but the last -- which is what a client loops
/// on.
#[test]
fn a_multi_statement_query_is_answered_in_order() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    let answer = client.query("set @x = 1; select 1; set @y = 2");
    // OK, then the result set's column count, its definition, its one row and its
    // terminator, then the last OK.
    assert_eq!(answer.len(), 6, "{answer:?}");

    // First: an OK packet with more to come.
    assert_eq!(answer[0].body[0], 0x00);
    assert_ne!(answer[0].status() & MORE_RESULTS_EXISTS, 0);

    // Second: the result set, whose terminator also says more.
    assert_eq!(answer[2].body[0], 3, "a column definition");
    assert_eq!(answer[3].body, b"\x01\x31", "one row: the constant 1");
    assert_eq!(answer[4].body[0], 0xfe);
    assert_ne!(answer[4].status() & MORE_RESULTS_EXISTS, 0);

    // Last: an OK packet with the sequence complete.
    assert_eq!(answer[5].body[0], 0x00);
    assert_eq!(answer[5].status() & MORE_RESULTS_EXISTS, 0);
}

/// A prepared statement is prepared, executed twice and closed, and a closed one is
/// refused: the client's statement cache is what this exists for.
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
    // One definition per parameter, then one per column -- and no count packet in front
    // of them.
    for expected in ["?", "d_next_o_id"] {
        let definition = client.read();
        assert_eq!(definition.body[0], 3, "a definition: {:?}", definition.body);
        let text = String::from_utf8_lossy(&definition.body).to_string();
        assert!(text.contains(expected), "{expected} missing from {text:?}");
    }

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

/// A transaction's state is visible in the status flags, which is how a pool knows the
/// connection it is about to hand out has one open.
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

/// Pipelined commands are answered in order, one answer per command.
/// A client that writes three commands before reading any gets three answers, in the
/// order it asked, each numbered from one.
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

/// A MySQL connection is greeted before it says anything, and a client that connects to
/// the PostgreSQL port is not greeted at all: the two protocols are separate listeners
/// with separate codecs.
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

/// The `--warehouses` the rules start from, when a test says otherwise.
fn server_with(args: &[&str]) -> Server {
    server_with_env(&[], args)
}

/// A server with environment variables set, for the knobs a run needs.
fn server_with_env(env: &[(&str, &str)], args: &[&str]) -> Server {
    let mut all: Vec<&str> = vec!["--host", "127.0.0.1", "--port", "0", "--mysql-port", "0"];
    // One shard per protocol unless the test says otherwise: clap refuses a flag given
    // twice, so the default cannot simply be appended.
    if !args.contains(&"--workers") {
        all.extend_from_slice(&["--workers", "1"]);
    }
    // The TPC-C profile, unless a test says otherwise: the numbers this server states
    // are what a TPC-C client reads, and a test that checks them is checking the
    // profile.
    if !args.contains(&"--profile") {
        all.extend_from_slice(&["--profile", "tpcc"]);
    }
    all.extend_from_slice(args);

    let mut command = Command::new(env!("CARGO_BIN_EXE_dbnoop"));
    // A small provided-buffer ring on purpose.
    command.env("DBNOOP_PBUF", "4");
    for (name, value) in env {
        command.env(name, value);
    }

    let child = command
        .args(&all)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the server binary starts");

    banner(child)
}

// ---------------------------------------------------------------- TPC-C rules

/// The district sum is answered with the schema's own number, and grows by one per
/// NewOrder.
#[test]
fn the_district_sum_is_what_a_tpcc_client_reads_for_nopm() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    // Ten warehouses by default: a hundred districts, 3001 per district.
    let answer = client.query("select sum(d_next_o_id) from district");
    assert_eq!(answer.len(), 4, "{answer:?}");
    assert_eq!(answer[0].lenenc(0).0, 1, "one column");
    assert_eq!(row(&answer[2]).len(), 1, "one row");
    assert_eq!(
        row(&answer[2])[0],
        "300100",
        "3001 per district, 100 districts"
    );

    for expected in 300101..=300103u64 {
        client.command(COM_QUERY, b"call NEWORD(1, 2, 3, 4)");
        assert_eq!(client.answer()[0].body[0], 0x00, "a CALL answers an OK");

        let answer = client.query("select sum(d_next_o_id) from district");
        assert_eq!(
            row(&answer[2])[0],
            expected.to_string(),
            "a NewOrder advances one district"
        );
    }

    // The same query in another case, with the schema prefix a client may use, is the
    // same query.
    let answer = client.query("SELECT SUM(D_NEXT_O_ID) FROM tpcc.district");
    assert_eq!(row(&answer[2])[0], "300103");

    // A plain-SQL NewOrder is the INSERT into new_order.
    client.command(
        COM_QUERY,
        b"insert into tpcc_new_order (no_w_id, no_d_id, no_o_id) values (1, 1, 3001)",
    );
    assert_eq!(client.answer()[0].body[0], 0x00);
    let answer = client.query("select sum(d_next_o_id) from district");
    assert_eq!(row(&answer[2])[0], "300104", "the insert counts too");
}

/// `--warehouses` sets how many districts the schema has: the sum a client reads at the
/// start is 3001 per district.
#[test]
fn the_district_sum_follows_the_warehouse_count() {
    let server = server_with(&["--warehouses", "5"]);
    let mut client = Client::connect(server.mysql);

    let answer = client.query("select sum(d_next_o_id) from district");
    assert_eq!(
        row(&answer[2])[0],
        (3001 * 50).to_string(),
        "five warehouses, ten districts each"
    );
}

/// `show global status` answers the commit counters a TPC-C client reads for TPM,
/// unfiltered and with the `LIKE` a client asks one at a time with.
#[test]
fn the_commit_counters_are_what_a_tpcc_client_reads_for_tpm() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    // An explicit transaction: the COMMIT is the one commit.
    client.query("begin");
    client.query("call PAYMENT(1, 2, 3)");
    assert_eq!(counter(&mut client, "Com_commit"), 0, "still open");
    client.query("commit");
    assert_eq!(counter(&mut client, "Com_commit"), 1);

    // Autocommit: each stored-procedure call is its own transaction.
    client.query("call DELIVERY(1)");
    client.query("call OSTAT(1, 2, 3)");
    assert_eq!(counter(&mut client, "Com_commit"), 3);

    // A rollback is a rollback.
    client.query("begin");
    client.query("call SLEV(1, 2, 3)");
    client.query("rollback");
    assert_eq!(counter(&mut client, "Com_commit"), 3);
    assert_eq!(counter(&mut client, "Com_rollback"), 1);

    // The unfiltered list holds both counters.
    let answer = client.query("show global status");
    let rows: Vec<Vec<String>> = answer[3..answer.len() - 1].iter().map(row).collect();
    assert_eq!(rows.len(), 2, "{answer:?}");
    assert_eq!(rows[0][0], "Com_commit");
    assert_eq!(rows[1][0], "Com_rollback");
}

/// One `show global status like '<name>'` counter.
fn counter(client: &mut Client, name: &str) -> u64 {
    let sql = format!("show global status like '{name}'");
    let answer = client.query(&sql);
    let rows: Vec<Vec<String>> = answer[3..answer.len() - 1].iter().map(row).collect();
    assert_eq!(rows.len(), 1, "{name}: {answer:?}");
    assert_eq!(rows[0][0], name);

    rows[0][1].parse().expect("a number")
}

/// A PostgreSQL DataRow's values: an i16 count, then an i32 length and that many bytes
/// for each.
fn pg_values(body: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes([body[0], body[1]]) as usize;
    let mut at = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let len = i32::from_be_bytes([body[at], body[at + 1], body[at + 2], body[at + 3]]) as usize;
        values.push(String::from_utf8_lossy(&body[at + 4..at + 4 + len]).to_string());
        at += 4 + len;
    }

    values
}

/// A row packet's values.
fn row(packet: &Packet) -> Vec<String> {
    let mut values = Vec::new();
    let mut at = 0usize;
    while at < packet.body.len() {
        let (value, next) = packet.lenenc_bytes(at);
        values.push(String::from_utf8(value).expect("utf-8"));
        at = next;
    }

    values
}

/// A stored procedure's OUT parameters read back as one row of numbers, which is what a
/// client does straight after its `CALL`.
#[test]
fn out_parameters_answer_one_plausible_row() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    let answer = client.query("call NEWORD(1, 2, 3, 4)");
    assert_eq!(answer[0].body[0], 0x00, "a CALL is an OK packet");

    let answer = client.query("select @neword_out, @d_next_o_id");
    assert_eq!(answer[0].lenenc(0).0, 2, "one column per variable");
    let values = row(&answer[3]);
    assert_eq!(values, vec!["1", "1"], "numbers, and never NULL");
}

/// The same rules answer on the PostgreSQL port, where a TPC-C client's NOPM reads the
/// same district query.
#[test]
fn the_district_sum_is_answered_on_the_postgres_port() {
    let server = server();
    let mut pg = PgClient::connect(server.postgres);

    let answer = pg.query("select sum(d_next_o_id) from district");
    assert_eq!(answer.len(), 3, "RowDescription, one row, CommandComplete");
    assert_eq!(answer[0].0, b'T');
    assert_eq!(answer[1].0, b'D');
    // One field, and its length: six bytes of text.
    assert_eq!(
        &answer[1].1[..6],
        b"\x00\x01\x00\x00\x00\x06",
        "one field, six bytes long: {:?}",
        answer[1].1
    );
    // The value itself: 3001 per district, 100 districts.
    assert_eq!(&answer[1].1[6..], b"300100");

    // A NewOrder counted on the PostgreSQL side advances it too.
    pg.query("insert into new_order values (1, 1, 3001)");
    let answer = pg.query("select sum(d_next_o_id) from district");
    assert!(answer[1].1.ends_with(b"300101"), "{:?}", answer[1].1);

    // And the commit counters a TPC-C client reads are there.
    pg.query("begin");
    pg.query("commit");
    let answer = pg.query("show global status like 'Com_commit'");
    assert_eq!(answer[0].0, b'T', "a result set");
    let values = pg_values(&answer[1].1);
    assert_eq!(values[0], "Com_commit", "the counter's name");
    assert!(values[1].parse::<u64>().is_ok(), "and a number: {values:?}");
}

/// A minimal PostgreSQL client: enough to send a simple query and read its frames.
struct PgClient {
    stream: TcpStream,
}

impl PgClient {
    fn connect(port: u16) -> PgClient {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        let mut client = PgClient { stream };

        let mut startup = 196_608i32.to_be_bytes().to_vec();
        startup.extend_from_slice(b"user\0bench\0\0");
        let mut framed = ((startup.len() + 4) as i32).to_be_bytes().to_vec();
        framed.extend_from_slice(&startup);
        client.stream.write_all(&framed).expect("startup");
        // AuthenticationOk and the ParameterStatus messages, then ReadyForQuery.
        client.until_ready();

        client
    }

    /// Frames up to and including ReadyForQuery.
    fn until_ready(&mut self) -> Vec<(u8, Vec<u8>)> {
        let mut frames = Vec::new();
        loop {
            let frame = self.frame();
            let ready = frame.0 == b'Z';
            frames.push(frame);
            if ready {
                return frames;
            }
        }
    }

    fn frame(&mut self) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        self.stream.read_exact(&mut head).expect("a frame header");
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        self.stream.read_exact(&mut body).expect("a frame body");

        (head[0], body)
    }

    /// A simple query, and the frames of its answer without ReadyForQuery.
    fn query(&mut self, sql: &str) -> Vec<(u8, Vec<u8>)> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut framed = vec![b'Q'];
        framed.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        framed.extend_from_slice(&body);
        self.stream.write_all(&framed).expect("query");

        let mut frames = self.until_ready();
        frames.pop();

        frames
    }
}

/// Both binary names serve.
/// `dbnoop` is the name; `pgnoop` is what the scripts written for the PostgreSQL-only
/// server call, and an upgrade that dropped it would break them at their next run.
#[test]
fn both_binary_names_serve() {
    for binary in [env!("CARGO_BIN_EXE_dbnoop"), env!("CARGO_BIN_EXE_pgnoop")] {
        let child = Command::new(binary)
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
            .env("DBNOOP_PBUF", "4")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the binary starts");
        let server = banner(child);

        let mut pg = PgClient::connect(server.postgres);
        let answer: Vec<(u8, String)> = pg
            .query("select 1")
            .into_iter()
            .map(|(tag, body)| (tag, String::from_utf8_lossy(&body).to_string()))
            .collect();
        assert!(
            answer.iter().any(|(tag, _)| *tag == b'T'),
            "{binary} did not answer a query: {answer:?}"
        );
    }
}

/// A prepared statement's row is in the binary protocol, over a real socket: the client
/// here sends the parameter types and reads the row bytes the way a driver does.
#[test]
fn a_prepared_select_answers_a_binary_row() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    client.command(COM_STMT_PREPARE, b"select 1");
    let prepare = client.read();
    assert_eq!(prepare.body[0], 0x00, "a prepare response");
    let id = u32::from_le_bytes([
        prepare.body[1],
        prepare.body[2],
        prepare.body[3],
        prepare.body[4],
    ]);
    let columns = u16::from_le_bytes([prepare.body[5], prepare.body[6]]);
    assert_eq!(columns, 1);
    let definition = client.read();
    assert_eq!(definition.body[0], 3, "the column's definition");

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00); // no cursor
    body.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    body.push(0x00); // null bitmap
    client.command(COM_STMT_EXECUTE, &body);

    // The column count, the definition, the row, the terminator.
    let count = client.read();
    assert_eq!(count.body, vec![1]);
    let _definition = client.read();
    let row = client.read();
    assert_eq!(row.body[0], 0x00, "the binary row header: {:?}", row.body);
    assert_eq!(row.body[1], 0x00, "a NULL bitmap with nothing in it");
    assert_eq!(
        &row.body[2..10],
        &1i64.to_le_bytes(),
        "the value 1, as int8"
    );
    let terminator = client.read();
    assert_eq!(terminator.body[0], 0xfe);
}

/// A stored routine's body does not leave the session in a transaction.
#[test]
fn a_routine_body_leaves_no_transaction_behind() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    let create = "create procedure neword(in w int) begin \
                  insert into new_order values (w); \
                  if w > 0 then select 1; end if; \
                  end";
    let answer = client.query(&format!("{create}; select 1"));
    assert_eq!(
        answer.len(),
        1 + 4,
        "the definition, then the select: {answer:?}"
    );
    assert_eq!(
        answer[0].status() & IN_TRANS,
        0,
        "the body's BEGIN is not a transaction"
    );

    // The next statement is still autocommit, and an explicit transaction still works.
    client.query("call PAYMENT(1, 2, 3)");
    assert_eq!(counter(&mut client, "Com_commit"), 1, "the CALL committed");

    client.query("begin");
    let answer = client.query("select 1");
    let terminator = answer.last().expect("an answer");
    assert_ne!(
        terminator.status() & IN_TRANS,
        0,
        "a real BEGIN does open one"
    );
    client.query("commit");
    assert_eq!(counter(&mut client, "Com_commit"), 2);
}

/// A PostgreSQL TPC-C client's function call counts, so its NOPM is not zero.
/// HammerDB's PG TPROC-C calls `neword(...)` as a function inside a SELECT -- plain and
/// prepared -- rather than as a `CALL`.
#[test]
fn a_postgres_function_call_is_a_tpcc_transaction() {
    let server = server();
    let mut pg = PgClient::connect(server.postgres);

    let answer = pg.query("select sum(d_next_o_id) from district");
    let before = String::from_utf8_lossy(&answer[1].1).to_string();
    assert!(before.ends_with("300100"), "{before:?}");

    for sql in [
        "select neword(1, 2, 3, 4)",
        "SELECT * FROM neword(1, 2, 3, 4)",
    ] {
        let answer = pg.query(sql);
        assert!(
            answer.iter().any(|(tag, _)| *tag == b'C'),
            "{sql} was not answered"
        );
    }

    // Prepared, executed twice: two transactions, counted where they run.
    let parse = |pg: &mut PgClient, sql: &str| {
        let mut body = Vec::new();
        body.extend_from_slice(b"s1\0");
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        body.extend_from_slice(&0i16.to_be_bytes());
        let mut framed = vec![b'P'];
        framed.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        framed.extend_from_slice(&body);
        pg.stream.write_all(&framed).expect("parse");
        let mut sync = vec![b'S'];
        sync.extend_from_slice(&4i32.to_be_bytes());
        pg.stream.write_all(&sync).expect("sync");
        pg.until_ready();
    };
    parse(&mut pg, "select * from payment($1, $2, $3)");
    for _ in 0..2 {
        let mut body = Vec::new();
        body.extend_from_slice(b"\0"); // portal
        body.extend_from_slice(b"s1\0"); // statement
        body.extend_from_slice(&0i16.to_be_bytes()); // format codes
        body.extend_from_slice(&0i16.to_be_bytes()); // parameters
        body.extend_from_slice(&0i16.to_be_bytes()); // result formats
        let mut framed = vec![b'B'];
        framed.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        framed.extend_from_slice(&body);
        let mut exec = vec![b'E'];
        exec.extend_from_slice(&9i32.to_be_bytes());
        exec.extend_from_slice(b"\0");
        exec.extend_from_slice(&0i32.to_be_bytes());
        let mut sync = vec![b'S'];
        sync.extend_from_slice(&4i32.to_be_bytes());
        framed.extend_from_slice(&exec);
        framed.extend_from_slice(&sync);
        pg.stream.write_all(&framed).expect("bind and execute");
        pg.until_ready();
    }

    // One district sum read after three NewOrders -- the first two statements -- and
    // two Payments, which move no district.
    let answer = pg.query("select sum(d_next_o_id) from district");
    let after = String::from_utf8_lossy(&answer[1].1).to_string();
    assert!(
        after.ends_with("300102"),
        "the NewOrders moved it: {after:?}"
    );
}

/// A shard that cannot have a ring serves on epoll; it does not die.
#[test]
fn a_shard_without_a_ring_serves_on_epoll() {
    for (case, fail) in [("every shard", "64"), ("half of them", "2")] {
        let server = server_with_env(
            &[("DBNOOP_FAIL_RING", fail)],
            &["--workers", "2", "--io", "uring"],
        );

        // What the banner claims: every shard of both protocols reports itself serving,
        // and the backends add up.
        let log = server.log.lock().map(|l| l.join("\n")).unwrap_or_default();
        for protocol in ["postgres", "mysql"] {
            let line = log
                .lines()
                .find(|l| l.contains(&format!("{protocol} listening on")))
                .unwrap_or_else(|| panic!("{case}: no {protocol} banner in\n{log}"));
            assert!(
                line.contains("serving 2/2"),
                "{case}: {protocol} is not serving all its shards: {line}"
            );
        }

        // And what the ports do: a query on each, answered.
        let mut client = Client::connect(server.mysql);
        let answer = client.query("select 1");
        assert_eq!(row(&answer[2]), vec!["1"], "{case}: MySQL is not answering");

        let mut pg = PgClient::connect(server.postgres);
        let answer = pg.query("select 1");
        assert!(
            answer.iter().any(|(tag, _)| *tag == b'T'),
            "{case}: PostgreSQL is not answering"
        );
    }
}

/// The ring budget is what the process may lock, and the banner says how it was spent.
#[test]
fn the_banner_says_which_backend_each_shard_got() {
    let server = server_with_env(&[], &["--workers", "1", "--io", "uring"]);
    let log = server.log.lock().map(|l| l.join("\n")).unwrap_or_default();

    for protocol in ["postgres", "mysql"] {
        let line = log
            .lines()
            .find(|l| l.contains(&format!("{protocol} listening on")))
            .unwrap_or_else(|| panic!("no {protocol} banner in\n{log}"));
        // `serving 1/1, io_uring <n>, epoll <m>, pinned=1/1`, and the two add up.
        // `io_uring 3, epoll 1)` -- the number after the label, up to whatever follows
        // it.
        let count = |label: &str| -> usize {
            line.split(label)
                .nth(1)
                .map(|rest| {
                    rest.trim_start()
                        .chars()
                        .take_while(char::is_ascii_digit)
                        .collect::<String>()
                })
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("no {label} count in {line}"))
        };
        let (ring, epoll) = (count("io_uring "), count("epoll "));
        assert_eq!(ring + epoll, 1, "{line}");
    }
}

/// One answer shape on both protocols.
/// A row is something the server can state only for a constant select list; everything
/// that names data answers none.
#[test]
fn a_select_answers_the_same_shape_on_both_protocols() {
    let server = server();

    let mut mysql = Client::connect(server.mysql);
    let answer = mysql.query("select w_id, w_name from warehouse");
    assert_eq!(
        answer.len(),
        4,
        "a MySQL select on a table: count, two definitions, terminator: {answer:?}"
    );
    let answer = mysql.query("select 1");
    assert_eq!(answer[2].body, b"\x01\x31", "a MySQL literal: one row, 1");

    let mut pg = PgClient::connect(server.postgres);
    let answer = pg.query("select w_id, w_name from warehouse");
    assert_eq!(
        answer.len(),
        2,
        "RowDescription, CommandComplete: {answer:?}"
    );
    assert_eq!(answer[0].0, b'T');
    assert_eq!(answer[1].0, b'C');
    assert_eq!(
        &answer[1].1, b"SELECT 0\0",
        "a PostgreSQL select on a table answers no rows"
    );

    let answer = pg.query("select 1");
    assert_eq!(answer.len(), 3, "RowDescription, one row, CommandComplete");
    assert_eq!(&answer[1].1[6..], b"1", "a PostgreSQL literal: one row, 1");

    // `@var` and `@@var` are values, on both protocols.
    let answer = pg.query("select @x");
    assert_eq!(answer.len(), 3, "{answer:?}");
    assert_eq!(&answer[1].1[6..], b"1");
    let answer = mysql.query("select @@version");
    assert_eq!(answer[2].body[1..], *SERVER_VERSION.as_bytes());

    // And a DML tag says what a no-op did: nothing.
    let answer = pg.query("insert into t values (1)");
    assert_eq!(&answer[0].1, b"INSERT 0 0\0");
    let answer = pg.query("update t set a = 1");
    assert_eq!(&answer[0].1, b"UPDATE 0\0");
    let answer = pg.query("delete from t");
    assert_eq!(&answer[0].1, b"DELETE 0\0");
}

/// A transaction is counted when it commits; a rollback counts nothing.
#[test]
fn a_transaction_is_counted_at_commit_on_both_protocols() {
    let server = server();

    // MySQL: inside a transaction, nothing moves until the COMMIT.
    let mut mysql = Client::connect(server.mysql);
    let sum = |client: &mut Client| -> u64 {
        let answer = client.query("select sum(d_next_o_id) from district");
        row(&answer[2])[0].parse().expect("a number")
    };
    let before = sum(&mut mysql);

    mysql.query("begin");
    mysql.query("call NEWORD(1, 2, 3, 4)");
    assert_eq!(
        sum(&mut mysql),
        before,
        "an uncommitted NewOrder is not counted"
    );
    mysql.query("rollback");
    assert_eq!(sum(&mut mysql), before, "and a rolled-back one never is");

    mysql.query("begin");
    mysql.query("call NEWORD(1, 2, 3, 4)");
    mysql.query("commit");
    assert_eq!(sum(&mut mysql), before + 1, "a committed one is");

    // Autocommit: the statement is its own transaction, and commits.
    mysql.query("call NEWORD(1, 2, 3, 4)");
    assert_eq!(sum(&mut mysql), before + 2);

    // PostgreSQL: the same rules.
    let mut pg = PgClient::connect(server.postgres);
    let sum = |pg: &mut PgClient| -> u64 {
        let answer = pg.query("select sum(d_next_o_id) from district");
        pg_values(&answer[1].1)[0].parse().expect("a number")
    };
    let before = sum(&mut pg);

    pg.query("begin");
    pg.query("insert into new_order values (1, 1, 3001)");
    assert_eq!(
        sum(&mut pg),
        before,
        "an uncommitted NewOrder is not counted"
    );
    pg.query("rollback");
    assert_eq!(sum(&mut pg), before);

    pg.query("begin");
    pg.query("insert into new_order values (1, 1, 3001)");
    pg.query("commit");
    assert_eq!(sum(&mut pg), before + 1, "a committed one is");

    // Autocommit on PostgreSQL counts a commit too, which is what a TPM read off
    // `xact_commit` needs.
    let commits = |pg: &mut PgClient| -> u64 {
        let answer = pg.query("show global status like 'Com_commit'");
        pg_values(&answer[1].1)[1].parse().expect("a number")
    };
    let before_commits = commits(&mut pg);
    pg.query("insert into new_order values (1, 1, 3001)");
    assert_eq!(
        sum(&mut pg),
        before + 2,
        "an autocommit NewOrder is counted"
    );
    assert_eq!(
        commits(&mut pg),
        before_commits + 1,
        "and it is a commit of its own"
    );
}

/// The failures a client injects are errors, on both protocols.
#[test]
fn an_injected_failure_is_an_error_on_both_protocols() {
    let server = server();

    let mut mysql = Client::connect(server.mysql);
    for (sql, code, state) in [
        ("signal sqlstate '45000'", 1644u16, "45000"),
        (
            "insert into new_order (no_w_id) values (NULL)",
            1048,
            "23000",
        ),
        ("call nosuch_procedure(1, 2)", 1305, "42000"),
    ] {
        let answer = mysql.query(sql);
        assert_eq!(answer.len(), 1, "{sql}: {answer:?}");
        assert_eq!(answer[0].body[0], 0xff, "{sql} must be an error");
        let (got, message) = answer[0].error();
        assert_eq!(got, code, "{sql}: {message}");
        assert_eq!(&answer[0].body[4..9], state.as_bytes(), "{sql}: {message}");
    }

    let mut pg = PgClient::connect(server.postgres);
    for (sql, state) in [
        ("signal sqlstate '45000'", "45000"),
        ("insert into new_order values (1, NULL, 3)", "23502"),
        ("select raise_exception('no stock')", "P0001"),
    ] {
        let answer = pg.query(sql);
        assert_eq!(answer[0].0, b'E', "{sql} must be an error: {answer:?}");
        let text = String::from_utf8_lossy(&answer[0].1).to_string();
        assert!(text.contains(state), "{sql}: expected {state} in {text:?}");
    }

    // And a failure inside a transaction aborts it, so the rollback the client sends
    // next is answered -- and counts nothing.
    let mut mysql = Client::connect(server.mysql);
    let sum = |client: &mut Client| -> u64 {
        let answer = client.query("select sum(d_next_o_id) from district");
        row(&answer[2])[0].parse().expect("a number")
    };
    let before = sum(&mut mysql);
    mysql.query("begin");
    mysql.query("insert into new_order (no_w_id) values (NULL)");
    mysql.query("rollback");
    assert_eq!(
        sum(&mut mysql),
        before,
        "the failed transaction counted nothing"
    );
}

/// The synthetic answers need `--profile tpcc`, and say so when they are on.
#[test]
fn the_synthetic_answers_need_the_profile() {
    let plain = server_with_env(&[], &["--profile", "none"]);

    let mut mysql = Client::connect(plain.mysql);
    let answer = mysql.query("select sum(d_next_o_id) from district");
    assert_eq!(answer[0].body, vec![1], "a zero-row result set");
    assert_eq!(answer.len(), 3, "count, definition, terminator: {answer:?}");
    let answer = mysql.query("show global status");
    assert_eq!(answer[0].body, vec![2], "two columns, no rows");

    let mut pg = PgClient::connect(plain.postgres);
    let answer = pg.query("select sum(d_next_o_id) from district");
    assert_eq!(
        answer.len(),
        2,
        "RowDescription and CommandComplete {answer:?}"
    );
    assert!(String::from_utf8_lossy(&answer[1].1).contains("SELECT 0"));
    let answer = pg.query("select sum(xact_commit+xact_rollback) from pg_stat_database");
    assert!(String::from_utf8_lossy(&answer[1].1).contains("SELECT 0"));

    // With the profile, the numbers are the schema's, and the line says they are
    // synthetic.
    let profiled = server_with_env(&[], &["--warehouses", "4"]);
    let mut pg = PgClient::connect(profiled.postgres);
    let answer = pg.query("select sum(d_next_o_id) from district");
    assert_eq!(pg_values(&answer[1].1)[0], (3001 * 40).to_string());
    let answer = pg.query("select max(w_id) from warehouse");
    assert_eq!(pg_values(&answer[1].1)[0], "4");
    let answer = pg.query("select sum(xact_commit+xact_rollback) from pg_stat_database");
    assert!(
        pg_values(&answer[1].1)[0].parse::<u64>().is_ok(),
        "TPM is a number: {answer:?}"
    );

    let mut mysql = Client::connect(profiled.mysql);
    let answer = mysql.query("select count(*) from orders");
    assert_eq!(row(&answer[2])[0], (3000 * 40).to_string());
    let answer = mysql.query("show global status like 'Com_commit'");
    assert_eq!(row(&answer[3])[0], "Com_commit");
}

/// **A reset connection is not a rollback.** MySQL's COM_RESET_CONNECTION ends the
/// session's work; counting it as a rollback inflated Com_rollback by one per reused
/// pooled connection. This test owns a server, so the counters are exact.
#[test]
fn a_reset_connection_counts_no_rollback() {
    let server = server();
    let mut client = Client::connect(server.mysql);

    client.query("begin");
    client.query("call NEWORD(1, 2, 3, 4)");
    let before = counter(&mut client, "Com_rollback");

    client.command(COM_RESET_CONNECTION, &[]);
    let answer = client.read();
    assert_eq!(answer.body[0], 0x00, "an OK packet");

    assert_eq!(
        counter(&mut client, "Com_rollback"),
        before,
        "a reset is not a rollback"
    );
}

/// **The NULL rule is the profile's, on both protocols.** Without `--profile tpcc` a
/// NULL is data: a benchmark writing one into a nullable column is answered as the
/// no-op it is, whether the NULL arrives as a literal or as a bound value.
#[test]
fn a_null_is_data_without_the_profile_on_both_ports() {
    let plain = server_with_env(&[], &["--profile", "none"]);

    let mut mysql = Client::connect(plain.mysql);
    let answer = mysql.query("insert into t values (1, NULL, 3)");
    assert_eq!(answer[0].body[0], 0x00, "a MySQL literal NULL: {answer:?}");

    mysql.command(COM_STMT_PREPARE, b"insert into t values (?)");
    let prepare = mysql.read();
    let id = u32::from_le_bytes([
        prepare.body[1],
        prepare.body[2],
        prepare.body[3],
        prepare.body[4],
    ]);
    // The parameter's definition follows the header; an INSERT has no columns.
    let definition = mysql.read();
    assert_eq!(definition.body[0], 3, "the parameter's definition");
    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(0b0000_0001);
    body.push(0x00);
    mysql.command(COM_STMT_EXECUTE, &body);
    let answer = mysql.read();
    assert_eq!(answer.body[0], 0x00, "a MySQL bound NULL: {answer:?}");

    let mut pg = PgClient::connect(plain.postgres);
    let answer = pg.query("insert into t values (1, NULL, 3)");
    assert!(
        String::from_utf8_lossy(&answer[0].1).contains("INSERT 0 0"),
        "a PostgreSQL literal NULL: {answer:?}"
    );

    // And the profile turns it back into an error.
    let profiled = server();
    let mut pg = PgClient::connect(profiled.postgres);
    let answer = pg.query("insert into t values (1, NULL, 3)");
    assert_eq!(answer[0].0, b'E', "with the profile: {answer:?}");
    assert!(String::from_utf8_lossy(&answer[0].1).contains("23502"));
}
