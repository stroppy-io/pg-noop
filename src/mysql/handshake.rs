//! The MySQL handshake: what the server says first, and what it accepts back.
//! One exchange, once per connection: `HandshakeV10` out, `HandshakeResponse41` in, an
//! OK packet after it.

use std::sync::OnceLock;

use super::packet::{self, Reader};
use super::AUTOCOMMIT;

/// What the handshake claims to be.
/// A client parses this, and some drivers compare it: a MariaDB connector seeing
/// `8.0.0` stays on the MySQL dialect.
pub const SERVER_VERSION: &str = "8.0.0-db-noop";

/// The two authentication plugins this server accepts.
/// It advertises the first -- every MySQL and MariaDB client speaks it -- and answers
/// the second when a client insists on it.
pub const NATIVE: &str = "mysql_native_password";
pub const CACHING_SHA2: &str = "caching_sha2_password";

/// The scramble offered in the greeting.
/// Any user and any password are accepted, so this is never checked; it must not
/// contain a NUL byte, because the fields it is split across are NUL-sensitive.
const SCRAMBLE: [u8; 20] = *b"dbnoop-scramble-0001";

/// `utf8mb4_general_ci`, which every client from 5.5 on knows.
const CHARSET_UTF8MB4: u8 = 45;

// The capability bits this server reads, by their names in `include/mysql_com.h`.
// Those the server advertises are OR'd into `CAPABILITIES`; the rest are here because a
// client may set them.
pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_SSL: u32 = 0x0000_0800;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
pub const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x0004_0000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

/// What this server says it can do.
pub const CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_PROTOCOL_41
    | CLIENT_TRANSACTIONS
    | CLIENT_SECURE_CONNECTION
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS
    | CLIENT_PS_MULTI_RESULTS
    | CLIENT_PLUGIN_AUTH
    | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | CLIENT_DEPRECATE_EOF;

/// `HandshakeV10`, built once.
pub fn greeting() -> &'static [u8] {
    static GREETING: OnceLock<Vec<u8>> = OnceLock::new();

    GREETING.get_or_init(|| {
        let mut out = Vec::with_capacity(80);
        let mut seq = 0u8;
        let at = packet::begin(&mut out, &mut seq);

        out.push(10); // protocol version
        packet::put_cstr(&mut out, SERVER_VERSION);
        out.extend_from_slice(&1u32.to_le_bytes()); // connection id
        out.extend_from_slice(&SCRAMBLE[..8]); // auth-plugin-data, part 1
        out.push(0); // filler
        out.extend_from_slice(&(CAPABILITIES as u16).to_le_bytes());
        out.push(CHARSET_UTF8MB4);
        out.extend_from_slice(&AUTOCOMMIT.to_le_bytes());
        out.extend_from_slice(&((CAPABILITIES >> 16) as u16).to_le_bytes());
        out.push(21); // auth-plugin-data length: 20 bytes and a terminator
        out.extend_from_slice(&[0u8; 10]); // reserved
        out.extend_from_slice(&SCRAMBLE[8..]); // part 2, 12 bytes
        out.push(0);
        packet::put_cstr(&mut out, NATIVE);

        packet::end(&mut out, at);

        out
    })
}

/// What a client answered the greeting with.
pub enum Response {
    /// `CLIENT_SSL`: the client wants TLS before it will authenticate.
    /// This server has none and says so with an error packet, which is the answer a
    /// client expects and can report.
    Ssl,
    /// A `HandshakeResponse41`, accepted whatever it named as user and password.
    /// `plugin` is the authentication plugin the client chose, which decides how the
    /// server answers (see `Conn::hello`).
    Login { caps: u32, plugin: Option<String> },
}

/// `HandshakeResponse41`, or the 32-byte `SSLRequest` that replaces it.
pub fn parse_response(payload: &[u8]) -> Option<Response> {
    let mut r = Reader::new(payload);

    let caps = r.u32()?;
    // Checked before the rest of the body is parsed: an SSLRequest is exactly the
    // 32-byte prefix, and a client that asks for TLS gets an error rather than a
    // handshake that cannot continue.
    if caps & CLIENT_SSL != 0 {
        return Some(Response::Ssl);
    }

    r.u32()?; // max packet size
    r.u8()?; // character set
    r.skip(23)?; // reserved

    r.cstr()?; // user name
               // The authentication response, in whichever of the three encodings the
               // client's own capabilities name.
    if caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        r.lenenc_bytes()?;
    } else if caps & CLIENT_SECURE_CONNECTION != 0 {
        let len = r.u8()? as usize;
        r.bytes(len)?;
    } else {
        r.cstr()?;
    }
    if caps & CLIENT_CONNECT_WITH_DB != 0 {
        r.cstr()?;
    }
    let plugin = if caps & CLIENT_PLUGIN_AUTH != 0 {
        r.cstr()?
    } else {
        // No plugin named: the client took the one the greeting offered.
        NATIVE
    };

    Some(Response::Login {
        caps,
        plugin: Some(plugin.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::packet::Reader;

    /// A handshake response with the fields this server reads, in the layout the
    /// protocol puts them.
    fn response(caps: u32, plugin: Option<&str>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&caps.to_le_bytes());
        out.extend_from_slice(&16_777_216u32.to_le_bytes()); // max packet
        out.push(CHARSET_UTF8MB4);
        out.extend_from_slice(&[0u8; 23]);
        packet::put_cstr(&mut out, "anyuser");
        if caps & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
            packet::put_lenenc_bytes(&mut out, b"\x01\x02\x03");
        } else if caps & CLIENT_SECURE_CONNECTION != 0 {
            out.push(3);
            out.extend_from_slice(b"\x01\x02\x03");
        } else {
            packet::put_cstr(&mut out, "auth");
        }
        if caps & CLIENT_CONNECT_WITH_DB != 0 {
            packet::put_cstr(&mut out, "tpcc");
        }
        if caps & CLIENT_PLUGIN_AUTH != 0 {
            packet::put_cstr(&mut out, plugin.unwrap_or(NATIVE));
        }

        out
    }

    /// The greeting is a well-formed `HandshakeV10` and says exactly what the server
    /// can do: the flags a client decides how to talk from, the charset, and no TLS.
    #[test]
    fn the_greeting_offers_what_this_server_speaks() {
        let g = greeting();
        let (len, seq) = packet::header(&g[..4]);
        assert_eq!(seq, 0, "the greeting is the first packet of the connection");
        assert_eq!(len, g.len() - 4);

        let mut r = Reader::new(&g[4..]);
        assert_eq!(r.u8(), Some(10), "protocol version 10");
        assert_eq!(r.cstr(), Some(SERVER_VERSION));
        r.u32().expect("connection id");
        r.skip(8).expect("scramble, part 1");
        assert_eq!(r.u8(), Some(0), "filler");
        let lower = r.u16().expect("capabilities, low half") as u32;
        assert_eq!(r.u8(), Some(CHARSET_UTF8MB4));
        r.u16().expect("status flags");
        let upper = r.u16().expect("capabilities, high half") as u32;
        let caps = lower | upper << 16;

        for (name, bit) in [
            ("PROTOCOL_41", CLIENT_PROTOCOL_41),
            ("SECURE_CONNECTION", CLIENT_SECURE_CONNECTION),
            ("PLUGIN_AUTH", CLIENT_PLUGIN_AUTH),
            ("MULTI_STATEMENTS", CLIENT_MULTI_STATEMENTS),
            ("MULTI_RESULTS", CLIENT_MULTI_RESULTS),
            ("PS_MULTI_RESULTS", CLIENT_PS_MULTI_RESULTS),
            ("DEPRECATE_EOF", CLIENT_DEPRECATE_EOF),
            ("CONNECT_WITH_DB", CLIENT_CONNECT_WITH_DB),
            ("TRANSACTIONS", CLIENT_TRANSACTIONS),
        ] {
            assert_ne!(caps & bit, 0, "{name} must be advertised");
        }
        assert_eq!(caps & CLIENT_SSL, 0, "there is no TLS to advertise");
        assert_eq!(caps, CAPABILITIES);

        assert_eq!(r.u8(), Some(21), "auth plugin data length");
        r.skip(10).expect("reserved");
        r.skip(12).expect("scramble, part 2");
        assert_eq!(r.u8(), Some(0), "the scramble's terminator");
        assert_eq!(r.cstr(), Some(NATIVE));
        assert!(r.rest().is_empty(), "nothing follows the plugin name");
    }

    /// Every encoding of the authentication response a client may use is read, along
    /// with the optional database and the plugin name.
    #[test]
    fn a_handshake_response_is_read_in_each_encoding() {
        for caps in [
            CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH,
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH,
            CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_PLUGIN_AUTH,
        ] {
            match parse_response(&response(caps, Some(CACHING_SHA2))) {
                Some(Response::Login { caps: got, plugin }) => {
                    assert_eq!(got, caps);
                    assert_eq!(plugin.as_deref(), Some(CACHING_SHA2));
                }
                other => panic!("caps {caps:#x} were not read: {:?}", other.is_some()),
            }
        }
    }

    /// A client that does not name a plugin took the one the greeting offered.
    #[test]
    fn a_client_without_a_plugin_token_is_on_the_native_plugin() {
        match parse_response(&response(
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION,
            None,
        )) {
            Some(Response::Login { plugin, .. }) => assert_eq!(plugin.as_deref(), Some(NATIVE)),
            _ => panic!("a bare handshake response must be read"),
        }
    }

    /// An SSL request is refused, not answered.
    #[test]
    fn an_ssl_request_is_refused() {
        let mut body = Vec::new();
        body.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SSL).to_le_bytes());
        body.extend_from_slice(&16_777_216u32.to_le_bytes());
        body.push(CHARSET_UTF8MB4);
        body.extend_from_slice(&[0u8; 23]);
        assert_eq!(body.len(), 32, "an SSLRequest is exactly 32 bytes");

        assert!(matches!(parse_response(&body), Some(Response::Ssl)));
    }

    /// A body that runs out before a field it declares is `None`, not a wrong answer
    /// read from the next field's bytes.
    #[test]
    fn a_truncated_handshake_response_is_refused() {
        let full = response(
            CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH,
            None,
        );
        for cut in 0..full.len() {
            assert!(
                parse_response(&full[..cut]).is_none(),
                "a response cut at {cut} of {} bytes was accepted",
                full.len()
            );
        }
        assert!(parse_response(&full).is_some());
    }
}
