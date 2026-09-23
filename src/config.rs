use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "pgnoop",
    about = "NoOp PostgreSQL blackhole server for benchmarking"
)]
struct CliArgs {
    /// Path to JSON config file
    #[arg(long, env = "PGNOOP_CONFIG", default_value = "./pgnoop.json")]
    config: PathBuf,

    /// Bind host
    #[arg(long, env = "PGNOOP_HOST")]
    host: Option<String>,

    /// Listen port
    #[arg(long, env = "PGNOOP_PORT")]
    port: Option<u16>,

    /// Number of shards, one thread each (0 = number of logical CPUs)
    #[arg(long, env = "PGNOOP_WORKERS")]
    workers: Option<usize>,

    /// I/O backend: `uring` or `epoll`. Both are shard-per-core and share the
    /// sans-io codec; they differ only in how bytes are moved. Kept selectable
    /// so the two can be measured against each other on one binary. `uring` is
    /// Linux-only.
    #[arg(long, env = "PGNOOP_IO")]
    io: Option<String>,
}

#[derive(Deserialize, Default)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    workers: Option<usize>,
    io: Option<String>,
}

pub struct Config {
    pub host: String,
    pub port: u16,
    pub workers: usize,
    pub io: Io,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Io {
    /// io_uring: one `io_uring_enter` per batch instead of a syscall per byte
    /// movement. Linux only, and not merely a configuration difference -- the
    /// ring, `MAP_POPULATE`, `MADV_DONTFORK` and the `SYS_io_uring_*` syscalls
    /// are all absent on Darwin, so on an Apple target the variant does not
    /// exist and neither does the code behind it.
    #[cfg(target_os = "linux")]
    Uring,
    /// epoll via tokio's current-thread runtime. Carries every platform.
    Epoll,
}

impl Default for Io {
    /// Prefer the ring where it exists. Whether it *works* is a runtime
    /// question and is settled by probing, not here (see `main::resolve_io`):
    /// under Docker's default seccomp profile a ring this process is allowed to
    /// name is still one it may not create.
    fn default() -> Self {
        #[cfg(target_os = "linux")]
        {
            Io::Uring
        }
        #[cfg(not(target_os = "linux"))]
        {
            Io::Epoll
        }
    }
}

impl Io {
    /// Unknown values are rejected, not defaulted. Mapping everything unknown to
    /// one backend means `--io epol` silently selects the other one and the
    /// operator measures something they did not choose.
    ///
    /// Naming a backend the platform has is also an error rather than a silent
    /// substitution: `--io uring` on macOS would otherwise measure epoll while
    /// the operator believes they measured the ring.
    fn parse(s: &str) -> Io {
        match s.trim().to_ascii_lowercase().as_str() {
            "epoll" | "tokio" => Io::Epoll,
            #[cfg(target_os = "linux")]
            "uring" | "io_uring" | "io-uring" => Io::Uring,
            #[cfg(not(target_os = "linux"))]
            "uring" | "io_uring" | "io-uring" => {
                eprintln!("pgnoop: io_uring is Linux-only; this build has epoll");
                std::process::exit(2);
            }
            other => {
                eprintln!("pgnoop: unknown --io value {other:?}; expected `uring` or `epoll`");
                std::process::exit(2);
            }
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            #[cfg(target_os = "linux")]
            Io::Uring => "io_uring",
            Io::Epoll => "epoll",
        }
    }
}

/// How long a paused accept stays paused after `EMFILE`/`ENFILE`/`ENOMEM`/
/// `ENOBUFS`. Long enough for a closed connection to release its fd, short
/// enough that a transient shortage costs a fraction of a second.
///
/// Shared by both backends: "do not accept for 100 ms" has to mean the same
/// thing whichever one is moving the bytes.
pub const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

/// How long a connection may stay in startup before it is closed.
///
/// pgwire allowed 60 s; the rewrite dropped it, and a client that connects and
/// sends nothing then holds an fd, a receive buffer and a slot for as long as it
/// likes. The default bind is 0.0.0.0, so "a client" includes whatever the
/// network can reach.
///
/// The same 60 s, because the timeout is a property of the protocol's users
/// rather than of this server: a driver that has not spoken in a minute is not a
/// driver that is about to. `PGNOOP_STARTUP_TIMEOUT_MS` overrides it, and `0`
/// disables it for anyone who wants the old behaviour on purpose.
const STARTUP_TIMEOUT_DEFAULT_MS: u64 = 60_000;

/// The startup timeout in force, decided once. Read by both backends, so that
/// "a client that never speaks" means the same thing whichever one is serving.
static STARTUP_TIMEOUT: std::sync::OnceLock<Option<std::time::Duration>> =
    std::sync::OnceLock::new();

/// The startup timeout in force. `Config::load` validates and records it; a
/// caller that arrives first (a test) gets the environment parsed leniently,
/// since exiting is `load`'s decision, not a connection's.
pub fn startup_timeout() -> Option<std::time::Duration> {
    *STARTUP_TIMEOUT.get_or_init(|| {
        parse_startup_timeout(std::env::var("PGNOOP_STARTUP_TIMEOUT_MS").ok().as_deref()).unwrap_or(
            Some(std::time::Duration::from_millis(STARTUP_TIMEOUT_DEFAULT_MS)),
        )
    })
}

/// `PGNOOP_STARTUP_TIMEOUT_MS` as the timeout it means. Unset is the default,
/// an exact `0` is disabled, a positive number is itself, and anything else is
/// an error.
///
/// It used to be `parse().ok()`, which made a value that did not parse into
/// `None` -- the same `None` that means disabled. `60000x`, a typo, silently
/// removed the guard the variable exists to tune. A value the operator wrote
/// and the server cannot read is refused, the way `--io epol` is.
fn parse_startup_timeout(value: Option<&str>) -> Result<Option<std::time::Duration>, String> {
    let Some(value) = value else {
        return Ok(Some(std::time::Duration::from_millis(
            STARTUP_TIMEOUT_DEFAULT_MS,
        )));
    };

    match value.trim().parse::<u64>() {
        Ok(0) => Ok(None),
        Ok(ms) => Ok(Some(std::time::Duration::from_millis(ms))),
        Err(_) => Err(format!(
            "PGNOOP_STARTUP_TIMEOUT_MS={value:?} is not a number of milliseconds; \
             expected a positive integer, or 0 to disable"
        )),
    }
}

impl Config {
    pub fn load() -> Self {
        let cli = CliArgs::parse();

        // Decided here, once, so a bad value stops the server before it binds
        // rather than being discovered -- or silently ignored -- per connection.
        let timeout =
            match parse_startup_timeout(std::env::var("PGNOOP_STARTUP_TIMEOUT_MS").ok().as_deref())
            {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("pgnoop: {e}");
                    std::process::exit(2);
                }
            };
        let _ = STARTUP_TIMEOUT.set(timeout);

        let file: FileConfig = std::fs::read_to_string(&cli.config)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let default_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Config {
            host: cli
                .host
                .or(file.host)
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            port: cli.port.or(file.port).unwrap_or(5432),
            workers: cli.workers.or(file.workers).unwrap_or(default_workers),
            io: cli
                .io
                .or(file.io)
                .map(|s| Io::parse(&s))
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A value that does not parse is an error, not "disabled".** The
    /// parse failure became `None`, and `None` is also the explicit
    /// disabled state, so `PGNOOP_STARTUP_TIMEOUT_MS=60000x` -- a typo --
    /// silently removed the guard the variable exists to tune.
    #[test]
    fn a_malformed_timeout_is_refused() {
        for bad in ["60000x", "", " ", "-1", "1.5", "none"] {
            assert!(
                parse_startup_timeout(Some(bad)).is_err(),
                "{bad:?} must be refused, not treated as disabled"
            );
        }
    }

    /// Only an exact `0` disables; unset is the default; a number is itself.
    #[test]
    fn zero_disables_and_unset_is_the_default() {
        assert_eq!(parse_startup_timeout(Some("0")), Ok(None));
        assert_eq!(
            parse_startup_timeout(None),
            Ok(Some(std::time::Duration::from_millis(
                STARTUP_TIMEOUT_DEFAULT_MS
            )))
        );
        assert_eq!(
            parse_startup_timeout(Some("2500")),
            Ok(Some(std::time::Duration::from_millis(2500)))
        );
    }
}
