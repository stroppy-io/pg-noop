//! Which wire protocol a listener speaks.
//! The shard loop moves bytes and knows nothing else, so the protocol lives in the
//! codec a connection is given.

use std::sync::Arc;

use crate::config::Io;
use crate::handler::{CatalogView, NoopHandler};
use crate::mysql;
use crate::wire;

/// One protocol, decided per listener and fixed for the shard that serves it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Protocol {
    Postgres,
    Mysql,
}

impl Protocol {
    pub fn name(self) -> &'static str {
        match self {
            Protocol::Postgres => "postgres",
            Protocol::Mysql => "mysql",
        }
    }
}

/// What a shard needs to serve a connection: the protocol, and the one piece of state
/// the PostgreSQL codec shares between connections.
#[derive(Clone)]
pub struct Codec {
    protocol: Protocol,
    catalog: CatalogView,
}

impl Codec {
    pub fn new(protocol: Protocol) -> Self {
        Codec {
            protocol,
            catalog: CatalogView(Arc::new(NoopHandler::new())),
        }
    }

    /// Which protocol this codec speaks.
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    /// A new connection's state, at its first byte.
    pub fn session(&self) -> Session {
        match self.protocol {
            Protocol::Postgres => Session::Postgres(wire::Conn::new()),
            Protocol::Mysql => Session::Mysql(mysql::Conn::new()),
        }
    }

    /// The bytes this protocol's server speaks before the client has said anything.
    /// PostgreSQL has none -- it waits for the startup message -- and MySQL has its
    /// greeting, which a client waits for before it writes.
    pub fn prelude(&self) -> Option<&'static [u8]> {
        match self.protocol {
            Protocol::Postgres => None,
            Protocol::Mysql => Some(mysql::greeting()),
        }
    }

    /// Run the codec over the bytes that arrived, appending the reply.
    pub fn advance(&self, session: &mut Session, input: &[u8], out: &mut Vec<u8>) -> usize {
        match session {
            Session::Postgres(conn) => conn.advance(input, out, &self.catalog),
            Session::Mysql(conn) => conn.advance(input, out),
        }
    }
}

/// What a shard tells the process about itself, for the startup banner.
pub enum Report {
    Pinned { protocol: Protocol, pinned: bool },
    Serving { protocol: Protocol, backend: Io },
}

/// One connection's protocol state.
pub enum Session {
    Postgres(wire::Conn),
    Mysql(mysql::Conn),
}

impl Session {
    pub fn is_closed(&self) -> bool {
        match self {
            Session::Postgres(conn) => conn.is_closed(),
            Session::Mysql(conn) => conn.is_closed(),
        }
    }

    /// True until the handshake is over: the one state in which a client may
    /// legitimately have sent nothing, and so the one a startup deadline is about.
    pub fn awaiting_startup(&self) -> bool {
        match self {
            Session::Postgres(conn) => conn.awaiting_startup(),
            Session::Mysql(conn) => conn.awaiting_startup(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only MySQL speaks first.
    #[test]
    fn only_mysql_has_a_prelude() {
        assert!(Codec::new(Protocol::Postgres).prelude().is_none());
        let greeting = Codec::new(Protocol::Mysql)
            .prelude()
            .expect("mysql greets first");
        assert_eq!(&greeting[4..5], &[10], "HandshakeV10");
    }

    /// Each protocol gets its own codec, and the same byte reading as a query is not
    /// the same in both: a PostgreSQL `Q` frame is a MySQL command byte.
    #[test]
    fn each_protocol_is_driven_by_its_own_codec() {
        let pg = Codec::new(Protocol::Postgres);
        let mut session = pg.session();
        let mut out = Vec::new();
        // A PostgreSQL startup message: length, protocol 3.0, then parameters.
        let mut body = 196_608i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0test\0\0");
        let mut startup = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        startup.extend_from_slice(&body);
        assert_eq!(pg.advance(&mut session, &startup, &mut out), startup.len());
        assert!(
            !session.awaiting_startup(),
            "PostgreSQL needs no greeting from the server"
        );
        assert!(matches!(session, Session::Postgres(_)));

        let my = Codec::new(Protocol::Mysql);
        let session = my.session();
        assert!(session.awaiting_startup());
        assert!(matches!(session, Session::Mysql(_)));
    }
}
