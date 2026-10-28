//! Scripted-packet tests: one per command, plus the framing decisions.
//! Every test writes the bytes a client would write and reads the packets back out, so
//! what is pinned is the protocol a client parses rather than an internal shape.

use super::handshake::{
    CACHING_SHA2, CLIENT_CONNECT_WITH_DB, CLIENT_DEPRECATE_EOF, CLIENT_MULTI_STATEMENTS,
    CLIENT_PLUGIN_AUTH, CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA, CLIENT_PROTOCOL_41,
    CLIENT_SECURE_CONNECTION, CLIENT_SSL, NATIVE,
};
use super::*;

/// The capabilities a test connection negotiates unless it says otherwise: what a
/// current client asks for, EOF deprecated included.
const CAPS: u32 = CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PLUGIN_AUTH
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_DEPRECATE_EOF;

/// One packet as a client sends it: length, sequence, body.
fn client_packet(seq: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut seq = seq;
    packet::write(&mut out, &mut seq, body);
    out
}

/// A command packet: one command byte and its body, sequence zero.
fn command(command: u8, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(body.len() + 1);
    payload.push(command);
    payload.extend_from_slice(body);
    client_packet(0, &payload)
}

/// A `COM_QUERY`.
fn query(sql: &str) -> Vec<u8> {
    command(cmd::QUERY, sql.as_bytes())
}

/// `HandshakeResponse41`, with the auth response in whichever encoding the capabilities
/// name.
fn handshake_response(caps: u32, plugin: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&caps.to_le_bytes());
    body.extend_from_slice(&16_777_216u32.to_le_bytes());
    body.push(45);
    body.extend_from_slice(&[0u8; 23]);
    packet::put_cstr(&mut body, "bench");
    if caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        packet::put_lenenc_bytes(&mut body, b"\x11\x22\x33");
    } else if caps & CLIENT_SECURE_CONNECTION != 0 {
        body.push(3);
        body.extend_from_slice(b"\x11\x22\x33");
    } else {
        packet::put_cstr(&mut body, "auth");
    }
    if caps & CLIENT_CONNECT_WITH_DB != 0 {
        packet::put_cstr(&mut body, "tpcc");
    }
    if caps & CLIENT_PLUGIN_AUTH != 0 {
        packet::put_cstr(&mut body, plugin);
    }

    client_packet(1, &body)
}

/// A connection that has completed its handshake with `caps`.
fn connected(caps: u32) -> Conn {
    let mut c = Conn::new();
    let mut out = Vec::new();
    c.advance(&handshake_response(caps, NATIVE), &mut out);
    assert!(
        out.starts_with(&[7, 0, 0, 2, 0x00]),
        "the handshake is answered: {out:?}"
    );
    assert!(!c.awaiting_startup());

    c
}

/// The packets in `out`: each as a body, with its sequence number asserted to count up
/// from `first` -- the sequence a pipelining client checks.
fn packets_from(out: &[u8], first: u8) -> Vec<Vec<u8>> {
    let mut rest = out;
    let mut packets = Vec::new();
    let mut expected_seq = first;

    while !rest.is_empty() {
        assert!(rest.len() >= packet::HEADER, "a packet header is complete");
        let (len, seq) = packet::header(rest);
        assert_eq!(
            seq, expected_seq,
            "sequence numbers count up from the reply"
        );
        expected_seq = expected_seq.wrapping_add(1);
        assert!(
            rest.len() >= packet::HEADER + len,
            "a packet body is complete: {len} declared, {} left",
            rest.len() - packet::HEADER
        );
        packets.push(rest[packet::HEADER..packet::HEADER + len].to_vec());
        rest = &rest[packet::HEADER + len..];
    }

    packets
}

/// The packets of a command's answer, whose first sequence number is one.
fn packets(out: &[u8]) -> Vec<Vec<u8>> {
    packets_from(out, 1)
}

/// Run one command and return its packets.
fn round_trip(c: &mut Conn, input: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    assert_eq!(
        c.advance(input, &mut out),
        input.len(),
        "the command is consumed"
    );
    packets(&out)
}

/// An OK packet's status flags.
fn ok_status(packet: &[u8]) -> u16 {
    assert_eq!(packet[0], 0x00, "an OK packet: {packet:?}");
    let mut r = Reader::new(&packet[1..]);
    r.lenenc().expect("affected rows");
    r.lenenc().expect("last insert id");
    r.u16().expect("status flags")
}

/// The number of rows in a result set's packets, and the terminator's status.
/// A result set is the column count, one definition per column, then rows, then the
/// terminator -- which, with `DEPRECATE_EOF`, is an OK packet whose header is `0xfe`.
fn result_set(packets: &[Vec<u8>]) -> (usize, usize, u16) {
    let mut r = Reader::new(&packets[0]);
    let columns = r.lenenc().expect("a column count") as usize;
    let mut at = 1 + columns;
    let mut rows = 0usize;
    while at < packets.len() {
        let packet = &packets[at];
        if packet[0] == 0xfe && packet.len() < 9 {
            let mut r = Reader::new(&packet[1..]);
            let _affected = r.lenenc().expect("affected rows");
            let _last = r.lenenc().expect("last insert id");
            let status = r.u16().expect("status flags");

            return (columns, rows, status);
        }
        rows += 1;
        at += 1;
    }

    panic!("the result set has no terminator: {packets:?}");
}

/// The handshake is answered, and only then are commands read.
#[test]
fn a_handshake_is_answered_with_an_ok_packet() {
    let mut c = Conn::new();
    let mut out = Vec::new();
    let used = c.advance(&handshake_response(CAPS, NATIVE), &mut out);

    assert_eq!(used, handshake_response(CAPS, NATIVE).len());
    assert!(
        !c.awaiting_startup(),
        "the connection may take commands now"
    );
    let packets = packets_from(&out, 2);
    assert_eq!(packets.len(), 1);
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);

    // And a command after it is read as a command.
    let packets = round_trip(&mut c, &query("select 1"));
    assert_eq!(result_set(&packets).0, 1);
}

/// `caching_sha2_password` gets the fast-auth success path: the "proceed" byte, then
/// the OK packet.
#[test]
fn caching_sha2_is_answered_with_the_fast_auth_success() {
    let caps = CAPS | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    let mut c = Conn::new();
    let mut out = Vec::new();
    c.advance(&handshake_response(caps, CACHING_SHA2), &mut out);

    let packets = packets_from(&out, 2);
    assert_eq!(packets.len(), 2, "AuthMoreData, then OK: {packets:?}");
    assert_eq!(packets[0], vec![0x01, 0x03], "fast auth success");
    assert_eq!(ok_status(&packets[1]), AUTOCOMMIT);
    assert!(!c.awaiting_startup());
}

/// An SSL request is refused with an error packet, and the connection closes.
/// There is no TLS to fall back to, and a client waiting for a greeting that will not
/// come is worse than one told why.
#[test]
fn an_ssl_request_is_refused_with_an_error() {
    let mut c = Conn::new();
    let mut body = Vec::new();
    body.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SSL).to_le_bytes());
    body.extend_from_slice(&16_777_216u32.to_le_bytes());
    body.push(45);
    body.extend_from_slice(&[0u8; 23]);

    let mut out = Vec::new();
    c.advance(&client_packet(1, &body), &mut out);

    let packets = packets_from(&out, 2);
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0][0], 0xff, "an ERR packet");
    let mut r = Reader::new(&packets[0][1..]);
    assert_eq!(r.u16(), Some(1043), "ER_HANDSHAKE_ERROR");
    assert!(c.is_closed(), "and nothing else is expected of it");
}

/// A SELECT answers a well-formed result set with no rows: the column count, a
/// definition per column, and the terminator, with the status a client reads.
#[test]
fn a_select_answers_a_zero_row_result_set() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &query("select id, name from items where id = 1"));

    let (columns, rows, status) = result_set(&packets);
    assert_eq!(columns, 2, "the select list's two items");
    assert_eq!(rows, 0, "a no-op returns no rows");
    assert_eq!(status, AUTOCOMMIT);

    // The names are the items a client wrote.
    let mut r = Reader::new(&packets[1]);
    for expected in [b"def".as_slice(), b"", b"", b"", b"id", b""] {
        assert_eq!(r.lenenc_bytes(), Some(expected));
    }
}

/// A SELECT's column names come from its select list, outside strings and nested calls
/// -- a comma inside a function call is not a column boundary.
#[test]
fn a_select_list_is_split_at_its_own_commas() {
    let mut c = connected(CAPS);
    let packets = round_trip(
        &mut c,
        &query("select coalesce(a, 'x,y'), b as bee, (select 1 from t where k = 2) from t"),
    );

    let (columns, _, _) = result_set(&packets);
    assert_eq!(columns, 3, "{packets:?}");
    let names: Vec<String> = (1..=3)
        .map(|i| {
            let mut r = Reader::new(&packets[i]);
            for _ in 0..4 {
                r.lenenc_bytes().expect("a definition field");
            }
            String::from_utf8(r.lenenc_bytes().expect("the name").to_vec()).expect("utf-8")
        })
        .collect();
    assert_eq!(
        names,
        vec![
            "coalesce(a, 'x,y')",
            "b as bee",
            "(select 1 from t where k = 2)"
        ]
    );
}

/// Everything that is not a SELECT answers an OK packet with zero rows.
#[test]
fn dml_ddl_and_session_statements_answer_ok() {
    for sql in [
        "insert into new_order values (1, 2, 3)",
        "update warehouse set w_ytd = w_ytd + 1",
        "delete from district where d_id = 1",
        "create table t (id int primary key)",
        "drop table if exists t",
        "set names utf8mb4",
        "set autocommit = 1",
        "use tpcc",
        "call NEWORD(1, 2, 3)",
        "truncate table orders",
        "analyze table t",
        "lock tables t write",
    ] {
        let mut c = connected(CAPS);
        let packets = round_trip(&mut c, &query(sql));
        assert_eq!(packets.len(), 1, "{sql}: {}", packets.len());
        assert_eq!(ok_status(&packets[0]), AUTOCOMMIT, "{sql}");
    }
}

/// A transaction is tracked, and the status flags say so.
/// A pool that reads `IN_TRANS` clear while a transaction is open hands the connection
/// to another query with the transaction still running underneath it.
#[test]
fn a_transaction_shows_in_the_status_flags() {
    for begin in [
        "begin",
        "BEGIN",
        "start transaction",
        "start transaction read only",
    ] {
        let mut c = connected(CAPS);
        let packets = round_trip(&mut c, &query(begin));
        assert_eq!(
            ok_status(&packets[0]),
            AUTOCOMMIT | IN_TRANS,
            "{begin} must open a transaction"
        );

        // A statement inside it keeps the flag.
        let packets = round_trip(&mut c, &query("select 1"));
        assert_eq!(result_set(&packets).2, AUTOCOMMIT | IN_TRANS);

        for end in ["commit", "rollback"] {
            let mut c = connected(CAPS);
            round_trip(&mut c, &query(begin));
            let packets = round_trip(&mut c, &query(end));
            assert_eq!(
                ok_status(&packets[0]),
                AUTOCOMMIT,
                "{end} must close the transaction"
            );
            let packets = round_trip(&mut c, &query("select 1"));
            assert_eq!(result_set(&packets).2, AUTOCOMMIT);
        }
    }
}

/// `ROLLBACK TO SAVEPOINT` is not the end of a transaction.
#[test]
fn a_rollback_to_a_savepoint_keeps_the_transaction_open() {
    let mut c = connected(CAPS);
    round_trip(&mut c, &query("begin"));
    round_trip(&mut c, &query("savepoint s1"));
    let packets = round_trip(&mut c, &query("rollback to savepoint s1"));
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT | IN_TRANS);
}

/// Multiple statements are answered one result at a time, in order, and only the last
/// one clears `MORE_RESULTS_EXISTS`.
#[test]
fn multi_statement_query_answers_each_statement_with_more_results_except_the_last() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &query("set @x = 1; select 1; call NEWORD(1, 2)"));

    // The first answer is an OK with MORE_RESULTS_EXISTS.
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT | MORE_RESULTS_EXISTS);
    // The second is a result set, also with more to come: its column count is one, its
    // definition follows, then the terminator.
    let packets = round_trip(&mut c, &query("select 1; select 2; select 3"));
    let (columns, rows, status) = result_set(&packets[..4]);
    assert_eq!((columns, rows), (1, 1), "a constant select answers one row");
    assert_eq!(
        status,
        AUTOCOMMIT | MORE_RESULTS_EXISTS,
        "one result follows"
    );
}

/// Every statement of a multi-statement query is answered, and exactly the last one is
/// clear of the flag.
#[test]
fn every_statement_of_a_multi_statement_query_is_answered() {
    let mut c = connected(CAPS);
    let mut out = Vec::new();
    c.advance(&query("select 1; set @x = 2; select 3"), &mut out);

    let packets = packets(&out);
    // select -> count + definition + one row + terminator = 4 packets, set -> 1, select
    // -> 4.
    assert_eq!(packets.len(), 9, "{packets:?}");
    assert_eq!(
        result_set(&packets[..4]).2,
        AUTOCOMMIT | MORE_RESULTS_EXISTS
    );
    assert_eq!(ok_status(&packets[4]), AUTOCOMMIT | MORE_RESULTS_EXISTS);
    assert_eq!(result_set(&packets[5..]).2, AUTOCOMMIT);
}

/// Empty statements between semicolons answer nothing at all: an answer the client is
/// not counting on is a desynchronised stream.
#[test]
fn empty_statements_between_semicolons_answer_nothing() {
    let mut c = connected(CAPS);
    let mut out = Vec::new();
    c.advance(&query("select 1;;  ;"), &mut out);

    let packets = packets(&out);
    assert_eq!(
        result_set(&packets).2,
        AUTOCOMMIT,
        "one answer, and no more"
    );
}

/// Without `CLIENT_MULTI_STATEMENTS` a query is one statement, semicolons and all:
/// nothing was negotiated to make it two.
#[test]
fn a_query_without_multi_statements_is_one_statement() {
    let mut c = connected(CAPS & !CLIENT_MULTI_STATEMENTS);
    let packets = round_trip(&mut c, &query("set @a = 1; set @b = 2"));

    assert_eq!(packets.len(), 1);
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);
}

/// `COM_SET_OPTION` turns multi-statement parsing on and off, and its reply is the EOF
/// packet the protocol specifies.
#[test]
fn com_set_option_toggles_multi_statements() {
    let mut c = connected(CAPS & !CLIENT_MULTI_STATEMENTS);

    let packets = round_trip(&mut c, &command(cmd::SET_OPTION, &0u16.to_le_bytes()));
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0][0], 0xfe, "an EOF packet, not an OK");
    assert_eq!(packets[0].len(), 5);

    // Now two statements are two answers.
    let packets = round_trip(&mut c, &query("set @a = 1; set @b = 2"));
    assert_eq!(packets.len(), 2);

    // And off again.
    round_trip(&mut c, &command(cmd::SET_OPTION, &1u16.to_le_bytes()));
    let packets = round_trip(&mut c, &query("set @a = 1; set @b = 2"));
    assert_eq!(packets.len(), 1);
}

/// A prepared statement's response carries its parameter and column counts, and
/// executing it answers the statement's result set.
#[test]
fn a_prepared_statement_is_prepared_and_executed() {
    let mut c = connected(CAPS);

    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select ? , ?"));
    let mut r = Reader::new(&packets[0]);
    assert_eq!(r.u8(), Some(0x00), "the prepare response");
    let id = r.u32().expect("statement id");
    let columns = r.u16().expect("column count");
    let params = r.u16().expect("parameter count");
    assert_eq!(columns, 2);
    assert_eq!(params, 2);
    assert_eq!(r.u8(), Some(0x00), "reserved");
    assert_eq!(r.u16(), Some(0), "warnings");

    // The counts are in that header and nowhere else.
    assert_eq!(packets.len(), 1 + 2 + 2, "{packets:?}");
    assert!(
        packets[1..].iter().all(|p| p[0] == 3),
        "every packet after the header is a column definition: {packets:?}"
    );

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00); // no cursor
    body.extend_from_slice(&1u32.to_le_bytes()); // iteration count
    body.push(0x00); // null bitmap
    body.push(0x00); // no parameter types follow: the values are not read
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));
    let (columns, rows, status) = result_set(&packets);
    assert_eq!((columns, rows), (2, 0));
    assert_eq!(status, AUTOCOMMIT);

    // Executing it again is answered again; a prepared statement is not consumed.
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));
    assert_eq!(result_set(&packets).0, 2);
}

/// The same response when EOF is not deprecated: a definition per parameter, an EOF, a
/// definition per column, an EOF -- and still no column count anywhere.
#[test]
fn a_prepare_response_without_deprecated_eof_has_two_eofs() {
    let mut c = connected(CAPS & !CLIENT_DEPRECATE_EOF);
    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select ? , ?"));

    assert_eq!(packets.len(), 1 + 2 + 1 + 2 + 1, "{packets:?}");
    assert_eq!(packets[3][0], 0xfe, "an EOF after the parameters");
    assert_eq!(packets[6][0], 0xfe, "an EOF after the columns");
    assert!(
        packets[1..3].iter().all(|p| p[0] == 3) && packets[4..6].iter().all(|p| p[0] == 3),
        "definitions on either side of the first EOF: {packets:?}"
    );
}

/// A statement that answers with an OK packet is prepared with no columns, which is
/// what tells a client there is no result set to read.
#[test]
fn a_prepared_dml_statement_has_no_columns() {
    let mut c = connected(CAPS);
    let packets = round_trip(
        &mut c,
        &command(cmd::STMT_PREPARE, b"insert into t values (?)"),
    );

    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");
    assert_eq!(r.u16(), Some(0), "no columns");
    assert_eq!(r.u16(), Some(1), "one parameter");

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));
    assert_eq!(packets.len(), 1);
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);
}

/// `COM_STMT_CLOSE` has no reply, and a closed statement is gone.
/// Answering a close desynchronises a client that is not reading anything for it.
#[test]
fn a_closed_prepared_statement_has_no_reply_and_is_unknown() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select 1"));
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");

    let mut out = Vec::new();
    let close = command(cmd::STMT_CLOSE, &id.to_le_bytes());
    assert_eq!(c.advance(&close, &mut out), close.len());
    assert!(out.is_empty(), "a close is answered by silence");

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));
    assert_eq!(
        packets[0][0], 0xff,
        "an ERR packet for an unknown statement"
    );
    let mut r = Reader::new(&packets[0][1..]);
    assert_eq!(r.u16(), Some(1243));
}

/// `COM_STMT_RESET` is answered with an OK for a statement that exists, and an error
/// for one that does not.
#[test]
fn com_stmt_reset_answers_ok_or_an_error() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select 1"));
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");

    let packets = round_trip(&mut c, &command(cmd::STMT_RESET, &id.to_le_bytes()));
    assert_eq!(packets[0][0], 0x00);

    let packets = round_trip(&mut c, &command(cmd::STMT_RESET, &999u32.to_le_bytes()));
    assert_eq!(packets[0][0], 0xff);
}

/// `COM_STMT_SEND_LONG_DATA` has no reply: the data is a fragment of a parameter this
/// server never reads.
#[test]
fn long_data_is_read_without_an_answer() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select ?"));
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.extend_from_slice(b"a fragment of a parameter");
    let mut out = Vec::new();
    let data = command(cmd::STMT_SEND_LONG_DATA, &body);
    assert_eq!(c.advance(&data, &mut out), data.len());
    assert!(out.is_empty(), "no reply at all");
    assert!(!c.is_closed(), "and the connection is still open");
}

/// `COM_PING` is an OK packet, which is how a pool checks a connection is alive.
#[test]
fn com_ping_answers_ok() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::PING, &[]));
    assert_eq!(packets.len(), 1);
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);
}

/// `COM_INIT_DB` is an OK packet: there are no schemas to select between.
#[test]
fn com_init_db_answers_ok() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::INIT_DB, b"tpcc"));
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);
}

/// `COM_QUIT` closes the connection and answers nothing.
#[test]
fn com_quit_closes_without_a_reply() {
    let mut c = connected(CAPS);
    let mut out = Vec::new();
    let quit = command(cmd::QUIT, &[]);
    assert_eq!(c.advance(&quit, &mut out), quit.len());
    assert!(out.is_empty());
    assert!(c.is_closed());
}

/// `COM_RESET_CONNECTION` starts the session over: no transaction, no prepared
/// statements, an OK packet in reply.
#[test]
fn com_reset_connection_clears_the_session() {
    let mut c = connected(CAPS);
    round_trip(&mut c, &query("begin"));
    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select 1"));
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");

    let packets = round_trip(&mut c, &command(cmd::RESET_CONNECTION, &[]));
    assert_eq!(
        ok_status(&packets[0]),
        AUTOCOMMIT,
        "no transaction any more"
    );

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));
    assert_eq!(packets[0][0], 0xff, "the statement is gone too");
}

/// `COM_STATISTICS` answers one plain-text packet, which a client prints.
#[test]
fn com_statistics_answers_a_string() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::STATISTICS, &[]));
    assert_eq!(packets.len(), 1);
    let text = String::from_utf8(packets[0].clone()).expect("utf-8");
    assert!(text.starts_with("Uptime: "), "{text}");
}

/// `COM_FIELD_LIST`, `COM_PROCESS_INFO`, `COM_DEBUG` and the rest of the administrative
/// set are answered in their own shapes rather than by silence.
#[test]
fn the_administrative_commands_are_answered() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::FIELD_LIST, b"t\0"));
    assert_eq!(packets[0][0], 0xfe, "an empty field list is an EOF");

    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::PROCESS_INFO, &[]));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (8, 0));

    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::DEBUG, &[]));
    assert_eq!(ok_status(&packets[0]), AUTOCOMMIT);
}

/// A command this server does not know is answered with an error, not with silence: a
/// client waiting for an answer it will not get hangs.
#[test]
fn an_unknown_command_is_refused() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(0x42, &[]));
    assert_eq!(packets[0][0], 0xff);
    let mut r = Reader::new(&packets[0][1..]);
    assert_eq!(r.u16(), Some(1047), "ER_UNKNOWN_COM_ERROR");
    assert!(!c.is_closed(), "and the connection survives it");
}

/// Pipelined commands are answered in order, each reply numbered from one.
#[test]
fn pipelined_commands_are_answered_in_order() {
    let mut c = connected(CAPS);

    let mut input = Vec::new();
    input.extend_from_slice(&query("select 1"));
    input.extend_from_slice(&command(cmd::PING, &[]));
    input.extend_from_slice(&query("select 1, 1, 1"));

    let mut out = Vec::new();
    assert_eq!(c.advance(&input, &mut out), input.len());

    // Three answers: a 4-packet result set (count, definition, row, terminator), a
    // 1-packet OK, a 6-packet result set (three columns).
    // Every answer restarts its sequence at one.
    let mut rest = &out[..];
    let mut sizes = vec![0usize];
    let mut expected = 1u8;
    while !rest.is_empty() {
        let (len, seq) = packet::header(rest);
        if seq == 1 {
            sizes.push(0);
            expected = 1;
        }
        assert_eq!(seq, expected, "each answer's packets count up from one");
        expected = expected.wrapping_add(1);
        *sizes.last_mut().expect("an answer") += 1;
        rest = &rest[packet::HEADER + len..];
    }
    assert_eq!(sizes, vec![0, 4, 1, 6], "4 + 1 + 6 packets");
}

/// A command split across the protocol's 16 MiB packets is one command.
#[test]
fn a_command_split_across_packets_is_one_command() {
    let mut c = connected(CAPS);

    // A query padded to exactly the largest payload, so it has to continue in a second
    // packet: `select 1` inside a comment that runs to the end.
    let mut payload = Vec::with_capacity(packet::MAX_PAYLOAD);
    payload.push(cmd::QUERY);
    payload.extend_from_slice(b"select 1 -- ");
    payload.resize(packet::MAX_PAYLOAD, b' ');
    assert_eq!(payload.len(), packet::MAX_PAYLOAD);

    let mut input = Vec::new();
    let mut seq = 0u8;
    packet::write(&mut input, &mut seq, &payload);
    // The rest of the same command, in a packet of its own: the newline that ends the
    // comment.
    packet::write(&mut input, &mut seq, b"\n");

    let mut out = Vec::new();
    assert_eq!(c.advance(&input, &mut out), input.len());
    // The answer follows the command's LAST packet, which is the second one.
    let packets = packets_from(&out, 2);
    let (columns, rows, status) = result_set(&packets);
    // `select 1` inside a comment is still the literal: one row, and the one piece it
    // was split across did not become two statements.
    assert_eq!((columns, rows, status), (1, 1, AUTOCOMMIT));
    assert_eq!(row_value(&packets[2]), "1");
}

/// A packet that is still incomplete is not consumed: the caller keeps it and calls
/// again, which is what the read loops rely on.
#[test]
fn an_incomplete_packet_is_left_for_the_next_read() {
    let mut c = connected(CAPS);
    let full = query("select 1");

    for cut in 0..full.len() {
        let mut out = Vec::new();
        let used = c.advance(&full[..cut], &mut out);
        assert_eq!(
            used,
            0,
            "a packet cut at {cut} of {} is incomplete",
            full.len()
        );
        assert!(out.is_empty());
    }

    let mut out = Vec::new();
    assert_eq!(c.advance(&full, &mut out), full.len());
    assert!(!out.is_empty());
}

/// A malformed handshake response is refused rather than guessed at.
#[test]
fn a_truncated_handshake_response_closes_the_connection() {
    let mut c = Conn::new();
    // A complete packet whose body ends in the middle of the field it declared: the
    // framing is fine, the handshake is not.
    let full = handshake_response(CAPS, NATIVE);
    let body = &full[packet::HEADER..full.len() - 4];
    let mut out = Vec::new();
    c.advance(&client_packet(1, body), &mut out);

    let packets = packets_from(&out, 2);
    assert_eq!(packets[0][0], 0xff);
    assert!(c.is_closed());
}

/// A statement that is not UTF-8 is answered with an error and the connection stays
/// usable: SQL is text, and a client that sends bytes has a bug of its own, not a dead
/// connection.
#[test]
fn a_statement_that_is_not_utf8_is_refused_without_closing() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &command(cmd::QUERY, &[0x73, 0xff, 0xfe]));
    assert_eq!(packets[0][0], 0xff);
    assert!(!c.is_closed());

    let packets = round_trip(&mut c, &query("select 1"));
    assert_eq!(result_set(&packets).0, 1);
}

/// Every statement shape decides the same answer from the same text whether it arrives
/// as a query or as a prepared statement: a client that prepares what another runs
/// directly cannot see two different servers.
#[test]
fn a_statement_is_classified_the_same_way_either_way() {
    for sql in [
        "select 1",
        "select a, b from t",
        "insert into t values (1)",
        "begin",
        "commit",
        "show tables",
        "describe t",
    ] {
        let mut direct = connected(CAPS);
        let packets = round_trip(&mut direct, &query(sql));

        let mut prepared = connected(CAPS);
        let prepare = round_trip(&mut prepared, &command(cmd::STMT_PREPARE, sql.as_bytes()));
        let mut r = Reader::new(&prepare[0]);
        r.u8().expect("header");
        let id = r.u32().expect("statement id");
        let columns = r.u16().expect("column count");
        let mut body = Vec::new();
        body.extend_from_slice(&id.to_le_bytes());
        body.push(0x00);
        body.extend_from_slice(&1u32.to_le_bytes());
        let execute = round_trip(&mut prepared, &command(cmd::STMT_EXECUTE, &body));

        if packets[0][0] == 0x00 {
            assert_eq!(columns, 0, "{sql}: a statement with no result set");
            assert_eq!(execute[0][0], 0x00, "{sql}");
        } else {
            assert_eq!(columns as usize, result_set(&packets).0, "{sql}");
            assert_eq!(result_set(&execute).0, result_set(&packets).0, "{sql}");
        }
    }
}

/// A column definition's name: the fifth length-encoded field of the packet.
fn definition_name(packet: &[u8]) -> String {
    let mut r = Reader::new(packet);
    for _ in 0..4 {
        r.lenenc_bytes().expect("a definition field");
    }

    String::from_utf8(r.lenenc_bytes().expect("the name").to_vec()).expect("utf-8")
}

/// Every value of a row packet.
fn row_values(packet: &[u8]) -> Vec<String> {
    let mut r = Reader::new(packet);
    let mut values = Vec::new();
    while !r.rest().is_empty() {
        values.push(String::from_utf8(r.lenenc_bytes().expect("a value").to_vec()).expect("utf-8"));
    }

    values
}

/// The value of a one-column row packet: its body, without the length prefixes this
/// server never has more than one of in a row.
fn row_value(packet: &[u8]) -> String {
    let mut r = Reader::new(packet);
    let value = r.lenenc_bytes().expect("a value");

    String::from_utf8(value.to_vec()).expect("utf-8")
}

/// The TPC-C shapes are answered as the rules say, and the counters behind them move
/// when a transaction runs.
#[test]
fn the_tpcc_rules_answer_with_numbers() {
    let _profile = tpcc::profile_guard(true);
    let mut c = connected(CAPS);

    // `select sum(d_next_o_id) from district`: one column, one row, a number.
    let packets = round_trip(&mut c, &query("select sum(d_next_o_id) from district"));
    let (columns, rows, status) = result_set(&packets);
    assert_eq!((columns, rows, status), (1, 1, AUTOCOMMIT));
    let before: u64 = row_value(&packets[2]).parse().expect("a number");

    // A NewOrder moves it by one.
    round_trip(&mut c, &query("call NEWORD(1, 2, 3, 4)"));
    let packets = round_trip(&mut c, &query("SELECT SUM(D_NEXT_O_ID) FROM TPCC_DISTRICT"));
    let after: u64 = row_value(&packets[2]).parse().expect("a number");
    assert!(after > before, "{after} does not follow {before}");

    // `show global status` answers the two counters, and a LIKE filter selects.
    let packets = round_trip(&mut c, &query("show global status"));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (2, 2), "{packets:?}");

    let packets = round_trip(&mut c, &query("show global status like 'Com_commit'"));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (2, 1), "only the filtered counter");
    let name = definition_name(&packets[1]);
    assert_eq!(name, "Variable_name");

    // A stored procedure's OUT parameters read back as numbers.
    let packets = round_trip(&mut c, &query("select @neword_out, @d_next_o_id"));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (2, 1));
    assert_eq!(row_value(&packets[3]), "1", "the first variable");
}

/// **With the profile off, a TPC-C query is an ordinary select**: its columns,
/// no rows. The profile is the only thing that makes it answer a number.
#[test]
fn a_tpcc_query_without_the_profile_is_an_empty_select() {
    let _profile = tpcc::profile_guard(false);
    let mut c = connected(CAPS);

    let packets = round_trip(&mut c, &query("select sum(d_next_o_id) from district"));
    let (columns, rows, status) = result_set(&packets);
    assert_eq!((columns, rows), (1, 0), "{packets:?}");
    assert_eq!(status, AUTOCOMMIT);

    let packets = round_trip(&mut c, &query("show global status"));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (2, 0), "{packets:?}");
}

/// A constant SELECT answers one row, the way PostgreSQL answers one.
#[test]
fn a_constant_select_answers_one_row() {
    let mut c = connected(CAPS);

    let packets = round_trip(&mut c, &query("select 1"));
    let (columns, rows, status) = result_set(&packets);
    assert_eq!((columns, rows, status), (1, 1, AUTOCOMMIT));
    assert_eq!(row_value(&packets[2]), "1");

    // Several literals, and a string keeps its own text.
    let packets = round_trip(&mut c, &query("select 2, 'abc', 42"));
    let (columns, rows, _) = result_set(&packets);
    assert_eq!((columns, rows), (3, 1));
    let values = row_values(&packets[4]);
    assert_eq!(values, vec!["2", "abc", "42"]);

    // A select that names a table still answers no rows: the server has none.
    let packets = round_trip(&mut c, &query("select 1 from warehouse"));
    assert_eq!(result_set(&packets).1, 0, "{packets:?}");

    // And a select of a column is not a constant.
    let packets = round_trip(&mut c, &query("select w_id from warehouse"));
    assert_eq!(result_set(&packets).1, 0);
}

/// The variables a client reads at connect answer plausible values, rather than an
/// empty result set a driver cannot parse.
#[test]
fn the_connect_variables_answer_plausible_values() {
    let mut c = connected(CAPS);

    for (sql, expected) in [
        ("select @@version", "8.0.0-db-noop"),
        ("select @@max_allowed_packet", "16777216"),
        ("select @@autocommit", "1"),
        ("select @@tx_isolation", "REPEATABLE-READ"),
        ("select @@transaction_isolation", "REPEATABLE-READ"),
        ("select @@session.auto_increment_increment", "1"),
        ("select @@session.tx_isolation", "REPEATABLE-READ"),
        ("select @@sql_mode", ""),
        ("select @@character_set_client", "utf8mb4"),
    ] {
        let packets = round_trip(&mut c, &query(sql));
        let (columns, rows, _) = result_set(&packets);
        assert_eq!((columns, rows), (1, 1), "{sql}: {packets:?}");
        assert_eq!(row_value(&packets[2]), expected, "{sql}");
    }

    // A variable nobody asked us about has a plausible number rather than nothing: a
    // client that reads it at connect must not get zero rows.
    let packets = round_trip(&mut c, &query("select @@lower_case_table_names"));
    assert_eq!(result_set(&packets).1, 1);
}

/// `COM_STMT_EXECUTE` answers BINARY rows.
#[test]
fn a_prepared_statement_answers_binary_rows() {
    let mut c = connected(CAPS);

    let packets = round_trip(&mut c, &command(cmd::STMT_PREPARE, b"select 1"));
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");
    assert_eq!(r.u16(), Some(1), "one column");

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));

    // count, definition, one binary row, terminator.
    assert_eq!(packets.len(), 4, "{packets:?}");
    let row = &packets[2];
    assert_eq!(row[0], 0x00, "the binary row header");
    assert_eq!(row[1], 0x00, "a NULL bitmap: nothing is NULL");
    assert_eq!(
        &row[2..10],
        &1i64.to_le_bytes(),
        "a LONGLONG is eight bytes, little-endian"
    );
}

/// The district sum is binary too when it is executed as a prepared statement, and its
/// column is the LONGLONG a client decodes it as.
#[test]
fn a_prepared_district_sum_answers_a_binary_longe_long() {
    // The synthetic answers are the profile's; this is what a profile run reads.
    let _profile = tpcc::profile_guard(true);
    let mut c = connected(CAPS);

    let packets = round_trip(
        &mut c,
        &command(cmd::STMT_PREPARE, b"select sum(d_next_o_id) from district"),
    );
    let mut r = Reader::new(&packets[0]);
    r.u8().expect("header");
    let id = r.u32().expect("statement id");
    let columns = r.u16().expect("column count");
    assert_eq!(columns, 1);
    // The definition's type: a client decodes the row by it.
    let definition = &packets[1];
    assert!(
        definition.windows(2).any(|w| w == [0x08, 0x00]),
        "MYSQL_TYPE_LONGLONG and no flags: {definition:?}"
    );

    let mut body = Vec::new();
    body.extend_from_slice(&id.to_le_bytes());
    body.push(0x00);
    body.extend_from_slice(&1u32.to_le_bytes());
    let packets = round_trip(&mut c, &command(cmd::STMT_EXECUTE, &body));

    let row = &packets[2];
    assert_eq!(row[0], 0x00, "the binary row header");
    let mut value = [0u8; 8];
    value.copy_from_slice(&row[2..10]);
    let sum = i64::from_le_bytes(value);
    assert!(
        (300100..=300100 + 1_000_000).contains(&sum),
        "{sum} is not a district sum"
    );
}

/// A routine definition is one statement, whatever semicolons its body holds.
#[test]
fn a_routine_body_is_one_statement() {
    let mut c = connected(CAPS);

    // Before: nothing is open.
    let packets = round_trip(&mut c, &query("select 1"));
    assert_eq!(result_set(&packets).2, AUTOCOMMIT, "no transaction yet");

    let create = "create procedure neword(in w int) begin \
                  insert into new_order values (w); \
                  if w > 0 then select 1; end if; \
                  begin select 2; end; \
                  end";
    let packets = round_trip(&mut c, &query(&format!("{create}; select 3")));

    // Two statements, two answers: the definition's OK and the select's row.
    assert_eq!(packets.len(), 1 + 4, "{packets:?}");
    let status = ok_status(&packets[0]);
    assert_eq!(
        status & MORE_RESULTS_EXISTS,
        MORE_RESULTS_EXISTS,
        "one answer follows"
    );
    assert_eq!(
        status & IN_TRANS,
        0,
        "**no transaction**: the body's BEGIN is not a statement"
    );
    assert_eq!(result_set(&packets[1..]).1, 1, "the select's row");

    // And the session is not stuck in a transaction: a CALL answers with no transaction
    // open, which is what makes it a commit of its own.
    let packets = round_trip(&mut c, &query("call PAYMENT(1, 2, 3)"));
    assert_eq!(
        ok_status(&packets[0]) & IN_TRANS,
        0,
        "an autocommit CALL is not inside a transaction"
    );
}

/// A statement that is only a routine definition answers one OK packet, and a trigger
/// and a function are the same shape.
#[test]
fn every_kind_of_routine_is_one_statement() {
    for sql in [
        "create function f() returns int begin return 1; end",
        "create trigger t before insert on x for each row begin set @a = 1; end",
        "create event e on schedule every 1 day do begin select 1; end",
        "create definer = root function g() returns int begin return 2; end",
    ] {
        let mut c = connected(CAPS);
        let packets = round_trip(&mut c, &query(sql));
        assert_eq!(packets.len(), 1, "{sql}: {packets:?}");
        assert_eq!(ok_status(&packets[0]), AUTOCOMMIT, "{sql}");
    }
}

/// A semicolon that is not in a routine body still splits, and a CASE expression's
/// `END` does not close a body it never opened.
#[test]
fn a_plain_query_still_splits_and_a_case_expression_closes_nothing() {
    let mut c = connected(CAPS);
    let packets = round_trip(&mut c, &query("select 1; select 2"));
    assert_eq!(packets.len(), 8, "two one-row result sets: {packets:?}");

    let mut c = connected(CAPS);
    let packets = round_trip(
        &mut c,
        &query("create procedure p() begin set @x = case when 1 then 2 end; end; select 4"),
    );
    assert_eq!(packets.len(), 5, "the definition and one row: {packets:?}");
}

/// A trailing `LIMIT` is a clause, not part of the select list.
#[test]
fn a_trailing_limit_still_leaves_a_constant() {
    let mut c = connected(CAPS);

    for (sql, columns, expected) in [
        ("select 1 limit 1", 1, "1"),
        (
            "select @@version_comment limit 1",
            1,
            "db-noop: a no-op server for measuring clients",
        ),
        ("select 7, 'x' limit 1", 2, "7"),
    ] {
        let packets = round_trip(&mut c, &query(sql));
        let (got_columns, rows, _) = result_set(&packets);
        assert_eq!((got_columns, rows), (columns, 1), "{sql}: {packets:?}");
        assert_eq!(row_value(&packets[columns + 1]), expected, "{sql}");
    }

    // The column is named after the list, not after the clause.
    let packets = round_trip(&mut c, &query("select 1 limit 1"));
    assert_eq!(definition_name(&packets[1]), "1");
}
