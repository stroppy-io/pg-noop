//! The packets a MySQL command is answered with: OK, ERR, EOF, a column
//! definition and a result set.
//!
//! Nothing here decides what an answer should be; it writes the shape the
//! protocol requires, which is the half a client is strict about.

use super::packet;

/// OK, with the affected-row count and the last insert id.
///
/// This server changes nothing, so both are zero -- except where a rule says
/// otherwise (see `crate::tpcc`). The status flags are what a client reads to
/// know whether a transaction is open and whether more results follow, and they
/// are not decoration: a pool that sees `IN_TRANS` clear commits an open
/// transaction it thinks is closed.
pub fn ok(out: &mut Vec<u8>, seq: &mut u8, affected: u64, last_insert_id: u64, status: u16) {
    let at = packet::begin(out, seq);
    out.push(0x00);
    packet::put_lenenc(out, affected);
    packet::put_lenenc(out, last_insert_id);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // warnings
    packet::end(out, at);
}

/// The end of a result set when the client negotiated `CLIENT_DEPRECATE_EOF`:
/// an OK packet whose header is `0xfe`, so it cannot be read as a row.
pub fn ok_eof(out: &mut Vec<u8>, seq: &mut u8, status: u16) {
    let at = packet::begin(out, seq);
    out.push(0xfe);
    packet::put_lenenc(out, 0); // affected rows
    packet::put_lenenc(out, 0); // last insert id
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    packet::end(out, at);
}

/// The classic end of a result set: `0xfe`, zero warnings, the status flags.
/// Five bytes, which is what keeps it from being read as a length-encoded
/// integer: those need at least nine.
pub fn eof(out: &mut Vec<u8>, seq: &mut u8, status: u16) {
    let at = packet::begin(out, seq);
    out.push(0xfe);
    out.extend_from_slice(&0u16.to_le_bytes()); // warnings
    out.extend_from_slice(&status.to_le_bytes());
    packet::end(out, at);
}

/// An ERR packet. The SQL state is five bytes and is shown to the client, so it
/// is chosen for what it says rather than invented: `42000` for a statement this
/// server will not run, `08S01` for a connection-level refusal.
pub fn err(out: &mut Vec<u8>, seq: &mut u8, code: u16, sqlstate: &str, message: &str) {
    let at = packet::begin(out, seq);
    out.push(0xff);
    out.extend_from_slice(&code.to_le_bytes());
    out.push(b'#');
    let state = sqlstate.as_bytes();
    for i in 0..5 {
        out.push(*state.get(i).unwrap_or(&b'0'));
    }
    packet::put_cstr(out, message);
    packet::end(out, at);
}

/// The type and width of one column, decided by what the answer holds rather
/// than by any parsing of the statement: everything this server sends is either
/// a decimal integer or a string.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `MYSQL_TYPE_VAR_STRING`, which any client decodes as text.
    Text,
    /// `MYSQL_TYPE_LONGLONG`, for the one number this server really produces:
    /// the TPC-C district sum in `crate::tpcc`.
    #[allow(dead_code)]
    LongLong,
}

impl Kind {
    fn wire(self) -> (u8, u16, u32, u16, u8) {
        // (type, charset, display length, flags, decimals). A numeric column is
        // in the binary charset, which is what a client uses to decide it may
        // parse the text as a number.
        match self {
            Kind::Text => (253, 45, 1024, 0, 0),
            Kind::LongLong => (8, 63, 20, 0, 0),
        }
    }
}

/// One column of a result set.
pub struct Column<'a> {
    pub name: &'a str,
    pub kind: Kind,
}

impl Column<'_> {
    fn write(&self, out: &mut Vec<u8>, seq: &mut u8) {
        let (kind, charset, length, flags, decimals) = self.kind.wire();

        let at = packet::begin(out, seq);
        packet::put_lenenc_bytes(out, b"def"); // catalog
        packet::put_lenenc_bytes(out, b""); // schema
        packet::put_lenenc_bytes(out, b""); // table
        packet::put_lenenc_bytes(out, b""); // org_table
        packet::put_lenenc_bytes(out, self.name.as_bytes());
        packet::put_lenenc_bytes(out, b""); // org_name
        out.push(0x0c); // length of the fixed fields that follow
        out.extend_from_slice(&charset.to_le_bytes());
        out.extend_from_slice(&length.to_le_bytes());
        out.push(kind);
        out.extend_from_slice(&flags.to_le_bytes());
        out.push(decimals);
        out.extend_from_slice(&[0, 0]); // filler
        packet::end(out, at);
    }
}

/// One column-definition packet on its own: what a prepare response puts in
/// front of its parameters and its result columns, with no count in front of it.
pub fn definition(out: &mut Vec<u8>, seq: &mut u8, column: &Column<'_>) {
    column.write(out, seq);
}

/// The column count and the column definitions: everything before the rows.
///
/// The EOF that follows the definitions is sent only when the client did not ask
/// to deprecate it. Sending one to a client that did is the mistake that makes a
/// driver read the EOF as the first row.
pub fn header(
    out: &mut Vec<u8>,
    seq: &mut u8,
    columns: &[Column<'_>],
    status: u16,
    deprecate: bool,
) {
    let at = packet::begin(out, seq);
    packet::put_lenenc(out, columns.len() as u64);
    packet::end(out, at);

    for c in columns {
        c.write(out, seq);
    }
    if !deprecate {
        eof(out, seq, status);
    }
}

/// One row: every value length-encoded, and `None` as the NULL marker.
///
/// Only the TPC-C answers (`crate::tpcc`) have a row to send: every other result
/// set this server produces is empty.
#[allow(dead_code)]
pub fn row(out: &mut Vec<u8>, seq: &mut u8, values: &[Option<&str>]) {
    let at = packet::begin(out, seq);
    for value in values {
        match value {
            Some(v) => packet::put_lenenc_bytes(out, v.as_bytes()),
            None => out.push(0xfb),
        }
    }
    packet::end(out, at);
}

/// The end of a result set, in whichever shape the client negotiated.
pub fn finish(out: &mut Vec<u8>, seq: &mut u8, status: u16, deprecate: bool) {
    if deprecate {
        ok_eof(out, seq, status);
    } else {
        eof(out, seq, status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::packet::Reader;

    const AUTOCOMMIT: u16 = 0x0002;
    const MORE_RESULTS: u16 = 0x0008;

    /// An OK packet is `0x00`, the two length-encoded counts, and the flags a
    /// client reads the transaction state from.
    #[test]
    fn an_ok_packet_carries_its_counts_and_flags() {
        let mut out = Vec::new();
        let mut seq = 1u8;
        ok(&mut out, &mut seq, 0, 0, AUTOCOMMIT);

        assert_eq!(packet::header(&out[..4]), (7, 1));
        assert_eq!(seq, 2);
        let mut r = Reader::new(&out[4..]);
        assert_eq!(r.u8(), Some(0x00));
        assert_eq!(r.lenenc(), Some(0), "affected rows");
        assert_eq!(r.lenenc(), Some(0), "last insert id");
        assert_eq!(r.u16(), Some(AUTOCOMMIT));
        assert_eq!(r.u16(), Some(0), "warnings");
        assert!(r.rest().is_empty());
    }

    /// With `DEPRECATE_EOF` the terminator is an OK packet whose header is
    /// `0xfe`; without it, the five-byte EOF packet. Both carry the status.
    #[test]
    fn the_terminator_follows_what_the_client_negotiated() {
        let mut out = Vec::new();
        let mut seq = 3u8;
        finish(&mut out, &mut seq, MORE_RESULTS, true);
        let mut r = Reader::new(&out[4..]);
        assert_eq!(r.u8(), Some(0xfe), "an OK packet, marked as a terminator");
        assert_eq!(r.lenenc(), Some(0));
        assert_eq!(r.lenenc(), Some(0));
        assert_eq!(r.u16(), Some(MORE_RESULTS));

        let mut out = Vec::new();
        let mut seq = 3u8;
        finish(&mut out, &mut seq, MORE_RESULTS, false);
        assert_eq!(packet::header(&out[..4]).0, 5, "the classic EOF is 5 bytes");
        let mut r = Reader::new(&out[4..]);
        assert_eq!(r.u8(), Some(0xfe));
        assert_eq!(r.u16(), Some(0), "warnings");
        assert_eq!(r.u16(), Some(MORE_RESULTS));
    }

    /// An ERR packet names its code and SQL state, and the message is a C
    /// string: a client shows it to a person.
    #[test]
    fn an_err_packet_carries_a_code_and_a_state() {
        let mut out = Vec::new();
        let mut seq = 2u8;
        err(&mut out, &mut seq, 1043, "08S01", "TLS is not supported");

        let mut r = Reader::new(&out[4..]);
        assert_eq!(r.u8(), Some(0xff));
        assert_eq!(r.u16(), Some(1043));
        assert_eq!(r.u8(), Some(b'#'));
        assert_eq!(r.bytes(5), Some(&b"08S01"[..]));
        assert_eq!(r.cstr(), Some("TLS is not supported"));

        // A state shorter than five bytes is padded rather than truncating the
        // packet length.
        let mut out = Vec::new();
        let mut seq = 0u8;
        err(&mut out, &mut seq, 1, "HY", "short state");
        let mut r = Reader::new(&out[4..]);
        r.skip(4)
            .expect("the error header, the code and the marker");
        assert_eq!(r.bytes(5), Some(&b"HY000"[..]));
    }

    /// A result set is the column count, a definition per column, then rows,
    /// then the terminator -- and with `DEPRECATE_EOF` there is no EOF between
    /// the definitions and the rows.
    #[test]
    fn a_result_set_is_framed_in_the_order_a_client_reads_it() {
        let columns = [
            Column {
                name: "sum(d_next_o_id)",
                kind: Kind::LongLong,
            },
            Column {
                name: "note",
                kind: Kind::Text,
            },
        ];

        for deprecate in [false, true] {
            let mut out = Vec::new();
            let mut seq = 1u8;
            header(&mut out, &mut seq, &columns, AUTOCOMMIT, deprecate);
            row(&mut out, &mut seq, &[Some("30010"), None]);
            finish(&mut out, &mut seq, AUTOCOMMIT, deprecate);

            // Walk the packets back out: count, columns, [EOF], row, terminator.
            let mut at = 0usize;
            let mut packets = Vec::new();
            while at < out.len() {
                let (len, _seq) = packet::header(&out[at..at + 4]);
                packets.push(&out[at + 4..at + 4 + len]);
                at += 4 + len;
            }
            let mut expected = 1 + columns.len() + 1 + 1;
            if !deprecate {
                expected += 1;
            }
            assert_eq!(packets.len(), expected, "deprecate_eof={deprecate}");

            let mut r = Reader::new(packets[0]);
            assert_eq!(r.lenenc(), Some(2), "column count");
            let mut r = Reader::new(packets[1]);
            assert_eq!(r.lenenc_bytes(), Some(&b"def"[..]));
            assert_eq!(r.lenenc_bytes(), Some(&b""[..]));
            assert_eq!(r.lenenc_bytes(), Some(&b""[..]));
            assert_eq!(r.lenenc_bytes(), Some(&b""[..]));
            assert_eq!(r.lenenc_bytes(), Some(&b"sum(d_next_o_id)"[..]));
            assert_eq!(r.lenenc_bytes(), Some(&b""[..]));
            assert_eq!(r.u8(), Some(0x0c));
            assert_eq!(r.u16(), Some(63), "a numeric column is in binary");
            assert_eq!(r.u32(), Some(20));
            assert_eq!(r.u8(), Some(8), "MYSQL_TYPE_LONGLONG");
            assert_eq!(r.u16(), Some(0), "flags");
            assert_eq!(r.u8(), Some(0), "decimals");

            let row_at = if deprecate { 3 } else { 4 };
            let mut r = Reader::new(packets[row_at]);
            assert_eq!(r.lenenc_bytes(), Some(&b"30010"[..]));
            assert_eq!(r.lenenc(), None, "NULL is the 0xfb marker, not a number");
        }
    }
}
