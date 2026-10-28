//! The MySQL packet layer: `[len:u24 LE][seq:u8][payload]`, and the
//! length-encoded integers and strings the payloads are made of.
//!
//! Nothing here knows what a command is. The framing is the same in the
//! handshake, in a command and in a reply, so the codec has one reader and one
//! writer for all three.

/// Three length bytes and one sequence byte in front of every payload.
pub const HEADER: usize = 4;

/// The largest payload one packet can carry. A payload of exactly this size
/// means the command continues in the next packet -- the protocol's way of
/// carrying something larger than its own length field.
pub const MAX_PAYLOAD: usize = 0xFF_FFFF;

/// Start a packet: four placeholder header bytes, then the body the caller
/// appends, then `end`. The sequence number advances here, so a reply of
/// several packets numbers them 1, 2, 3 without the caller counting.
pub fn begin(out: &mut Vec<u8>, seq: &mut u8) -> usize {
    let at = out.len();
    out.extend_from_slice(&[0, 0, 0, *seq]);
    *seq = seq.wrapping_add(1);

    at
}

/// Fill in the header of a packet started at `at`.
///
/// A reply here is a handful of columns of plausible values; nothing this
/// server builds comes near 16 MiB, and a body that did would be a bug rather
/// than something to split. An assertion rather than a silent truncation,
/// which is what writing the length into three bytes by hand would give.
pub fn end(out: &mut [u8], at: usize) {
    let len = out.len() - at - HEADER;
    assert!(
        len <= MAX_PAYLOAD,
        "reply packet of {len} bytes does not fit"
    );

    out[at] = (len & 0xff) as u8;
    out[at + 1] = ((len >> 8) & 0xff) as u8;
    out[at + 2] = ((len >> 16) & 0xff) as u8;
}

/// A whole packet in one call, for the replies whose body is already built.
pub fn write(out: &mut Vec<u8>, seq: &mut u8, body: &[u8]) {
    let at = begin(out, seq);
    out.extend_from_slice(body);
    end(out, at);
}

/// The payload length of a packet whose header is `header`, and its sequence.
///
/// The length is three little-endian bytes, so the reader cannot be handed one
/// that is negative or too large: unlike PostgreSQL's i32 length it has no
/// sign bit to misread.
pub fn header(header: &[u8]) -> (usize, u8) {
    let len = header[0] as usize | (header[1] as usize) << 8 | (header[2] as usize) << 16;

    (len, header[3])
}

// ------------------------------------------------------------ length encoding

/// A length-encoded integer. `0xfb` is NULL in the places where that is
/// meaningful, and is never written here: this server has no NULLs to send.
pub fn put_lenenc(out: &mut Vec<u8>, n: u64) {
    if n < 0xfb {
        out.push(n as u8);
    } else if n <= 0xffff {
        out.push(0xfc);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= 0xff_ffff {
        out.push(0xfd);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xfe);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

/// A length-encoded string: the length, then the bytes.
pub fn put_lenenc_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_lenenc(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// A NUL-terminated string.
pub fn put_cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

/// Reading a payload: every getter is `Option`, so a truncated or malformed
/// body is `None` at the point it runs out rather than a panic or a wrong
/// answer read from the next field's bytes.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn u8(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;

        Some(b)
    }

    pub fn u16(&mut self) -> Option<u16> {
        let b = self.bytes(2)?;

        Some(u16::from_le_bytes([b[0], b[1]]))
    }

    pub fn u32(&mut self) -> Option<u32> {
        let b = self.bytes(4)?;

        Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Skip `n` bytes, which is how the fields this server does not read are
    /// stepped over -- a fixed-size reservation has no other structure.
    pub fn skip(&mut self, n: usize) -> Option<()> {
        self.bytes(n).map(|_| ())
    }

    pub fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let b = self.buf.get(self.pos..end)?;
        self.pos = end;

        Some(b)
    }

    /// A length-encoded integer. `0xfb` is NULL and is not a number, so it is
    /// `None` -- the one place where running out of bytes and a NULL are
    /// deliberately the same answer, because both mean "no count here".
    pub fn lenenc(&mut self) -> Option<u64> {
        match self.u8()? {
            n @ 0x00..=0xfa => Some(n as u64),
            0xfc => Some(u16::from_le_bytes([self.u8()?, self.u8()?]) as u64),
            0xfd => {
                let b = self.bytes(3)?;

                Some(b[0] as u64 | (b[1] as u64) << 8 | (b[2] as u64) << 16)
            }
            0xfe => {
                let b = self.bytes(8)?;
                let mut n = [0u8; 8];
                n.copy_from_slice(b);

                Some(u64::from_le_bytes(n))
            }
            _ => None,
        }
    }

    pub fn lenenc_bytes(&mut self) -> Option<&'a [u8]> {
        let n = self.lenenc()? as usize;

        self.bytes(n)
    }

    /// A NUL-terminated string, without its terminator. Invalid UTF-8 is
    /// refused rather than replaced: every field read here is a name, and a
    /// name this server cannot read is one it should not echo into a reply.
    pub fn cstr(&mut self) -> Option<&'a str> {
        let rest = &self.buf[self.pos..];
        let n = rest.iter().position(|&b| b == 0)?;
        self.pos += n + 1;

        std::str::from_utf8(&rest[..n]).ok()
    }

    /// Everything not yet read. The handshake response's connection
    /// attributes, when a client sends them, are exactly this.
    #[cfg(test)]
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A packet carries its own length and sequence, little-endian in the
    /// length. Round-tripped through a reader that only has the header.
    #[test]
    fn a_packet_carries_its_length_and_sequence() {
        let mut out = Vec::new();
        let mut seq = 7u8;
        let at = begin(&mut out, &mut seq);
        out.extend_from_slice(b"hello");
        end(&mut out, at);

        assert_eq!(out, b"\x05\x00\x00\x07hello");
        assert_eq!(header(&out[..4]), (5, 7));
        assert_eq!(seq, 8, "the sequence advanced past the packet written");
    }

    /// A three-byte length holds 16 MiB - 1: the largest payload one packet
    /// can carry, which is what makes that size mean "continues".
    #[test]
    fn the_largest_payload_is_the_continuation_marker() {
        let mut out = Vec::new();
        let mut seq = 0u8;
        let at = begin(&mut out, &mut seq);
        out.resize(at + HEADER + MAX_PAYLOAD, b'x');
        end(&mut out, at);

        assert_eq!(header(&out[..4]), (MAX_PAYLOAD, 0));
        assert_eq!(&out[at..at + 3], &[0xff, 0xff, 0xff], "a three-byte length");
    }

    /// The sequence wraps at 256 rather than panicking or pinning, which is
    /// what the protocol says it does.
    #[test]
    fn the_sequence_wraps() {
        let mut out = Vec::new();
        let mut seq = 255u8;
        write(&mut out, &mut seq, b"x");

        assert_eq!(seq, 0);
        assert_eq!(out[3], 255);
    }

    /// Every length-encoded integer is written in the shortest form that holds
    /// it, and read back as itself.
    #[test]
    fn length_encoded_integers_round_trip() {
        for n in [
            0u64,
            1,
            250,
            251,
            252,
            0xffff,
            0x10000,
            0xff_ffff,
            0x100_0000,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            put_lenenc(&mut out, n);

            let mut r = Reader::new(&out);
            assert_eq!(r.lenenc(), Some(n), "encoding of {n} was {out:?}");
            assert!(r.rest().is_empty());
        }

        // 250 still fits in the one-byte form; 251 needs 0xfc.
        let mut out = Vec::new();
        put_lenenc(&mut out, 250);
        assert_eq!(out, vec![250]);
        out.clear();
        put_lenenc(&mut out, 251);
        assert_eq!(out[0], 0xfc);
    }

    /// A length-encoded string is its length and its bytes, so an empty string
    /// is a zero and not a terminator.
    #[test]
    fn length_encoded_strings_round_trip() {
        let mut out = Vec::new();
        put_lenenc_bytes(&mut out, b"");
        put_lenenc_bytes(&mut out, b"def");

        let mut r = Reader::new(&out);
        assert_eq!(r.lenenc_bytes(), Some(&b""[..]));
        assert_eq!(r.lenenc_bytes(), Some(&b"def"[..]));
        assert_eq!(r.lenenc_bytes(), None, "and running out is None");
    }

    /// A NUL-terminated string is read without its terminator, and a body
    /// without one is refused rather than read to the end and believed.
    #[test]
    fn cstrings_need_their_terminator() {
        let mut r = Reader::new(b"root\0talk\0");
        assert_eq!(r.cstr(), Some("root"));
        assert_eq!(r.cstr(), Some("talk"));
        assert_eq!(r.cstr(), None);

        let mut r = Reader::new(b"root");
        assert_eq!(r.cstr(), None, "no terminator is not a name");
    }

    /// Every getter stops at the end of the body instead of reading past it.
    #[test]
    fn a_short_body_reads_as_none() {
        let mut r = Reader::new(&[1, 2]);
        assert_eq!(r.u32(), None);
        assert_eq!(r.u16(), Some(0x0201));
        assert_eq!(r.u8(), None);
        assert_eq!(r.skip(1), None);
        assert_eq!(r.lenenc_bytes(), None);
    }
}
