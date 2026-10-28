use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// An environment variable, under either name this server has had.
///
/// `DBNOOP_` is the name, and `PGNOOP_` is what every script and unit file
/// written before the rename says. Both keep working: an upgrade that silently
/// changed a port because a variable was spelled differently is exactly the kind
/// of breakage the second binary name exists to avoid.
pub fn env(name: &str) -> Option<String> {
    for prefix in ["DBNOOP_", "PGNOOP_"] {
        if let Ok(value) = std::env::var(format!("{prefix}{name}")) {
            return Some(value);
        }
    }

    None
}

#[derive(Parser, Debug)]
#[command(
    name = "dbnoop",
    about = "NoOp PostgreSQL and MySQL/MariaDB server for measuring benchmark clients"
)]
struct CliArgs {
    /// Path to JSON config file (default ./dbnoop.json, or ./pgnoop.json)
    #[arg(long)]
    config: Option<PathBuf>,

    /// Bind host
    #[arg(long)]
    host: Option<String>,

    /// Listen port
    #[arg(long)]
    port: Option<u16>,

    /// MySQL/MariaDB listen port (the protocol is served only when this is set)
    #[arg(long)]
    mysql_port: Option<u16>,

    /// Number of shards, one thread each (0 = number of logical CPUs)
    #[arg(long)]
    workers: Option<usize>,

    /// Answer the synthetic TPC-C values (only `tpcc` is a profile)
    #[arg(long)]
    profile: Option<String>,

    /// Warehouses the TPCC schema is said to have (districts = 10 x this)
    #[arg(long)]
    warehouses: Option<u64>,

    /// Print the TPCC counters on stderr every N seconds (0 = never)
    #[arg(long)]
    stats_every: Option<u64>,

    /// I/O backend: `uring` or `epoll`. Both are shard-per-core and share the
    /// sans-io codec; they differ only in how bytes are moved. Kept selectable
    /// so the two can be measured against each other on one binary. `uring` is
    /// Linux-only.
    #[arg(long)]
    io: Option<String>,
}

#[derive(Deserialize, Default)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    mysql_port: Option<u16>,
    workers: Option<usize>,
    io: Option<String>,
    warehouses: Option<u64>,
    stats_every: Option<u64>,
    profile: Option<String>,
}

/// A number from the environment.
///
/// **A typo is not a default.** `DBNOOP_PORT=5432x` fell through to `None`, and
/// `None` means the default port: a server the operator aimed at one port bound
/// another, silently -- the same failure `parse_startup_timeout` exists to
/// prevent, and the same answer to it.
fn parse_env<T: std::str::FromStr>(name: &str, value: Option<String>) -> Result<Option<T>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.trim().parse::<T>() {
        Ok(v) => Ok(Some(v)),
        Err(_) => Err(format!("{name}={value:?} is not a number")),
    }
}

/// The bytes of memory this process may lock, from `RLIMIT_MEMLOCK`.
///
/// **A ring is charged to the USER, not to the process.** io_uring's rings and
/// the pages a provided-buffer ring pins are accounted against `locked_vm` for
/// the uid, so this is one budget shared by every shard of every protocol -- and
/// by anything else that uid runs. `None` means "no limit" (or no such limit on
/// this platform), which is taken to mean "do not ration".
#[cfg(target_os = "linux")]
pub fn memlock_bytes() -> Option<usize> {
    // SAFETY: a plain kernel-filled struct.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) } != 0 {
        return None;
    }
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }

    Some(limit.rlim_cur as usize)
}

#[cfg(not(target_os = "linux"))]
pub fn memlock_bytes() -> Option<usize> {
    None
}

/// What a shard may have of the ring budget.
///
/// Two numbers, because the budget can be spent two ways: fewer shards on
/// io_uring with a usable ring each, or every shard on io_uring with a ring too
/// small to be worth having. The first is what the default `--workers 0` on a
/// 32-CPU box needs -- 64 shards cannot each pin a ring out of 8 MiB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingPlan {
    /// How many shards may try io_uring. The rest serve on epoll.
    pub uring_shards: usize,
    /// Provided buffers per io_uring shard: 32 KiB each, pinned at registration.
    pub buffers: u16,
}

/// What a ring costs besides its buffers: the submission and completion queues,
/// with `CQSIZE` (4096 entries in, four times that out) and some slack.
const RING_OVERHEAD: usize = 384 * 1024;

/// One provided buffer: the page the kernel pins when the ring is registered.
const BUFFER_BYTES: usize = 32 * 1024;

/// The fewest provided buffers worth having a ring for. Below this the shard
/// spends its life out of buffers and reads into the connection's own, which is
/// what epoll does with less machinery.
const MIN_BUFFERS: u16 = 8;

/// The most provided buffers a shard may have: the production default, and the
/// point past which a ring stops paying for what it pins.
pub const MAX_BUFFERS: u16 = 256;

/// **Plan from the budget, not from an assumption.** `shards` shards of every
/// protocol share `budget` bytes; the plan says how many of them may have a ring
/// and how large each ring's buffer pool may be. It is a pure function of the
/// two so that the arithmetic is testable on a box with any budget, including
/// one that is not this one's.
pub fn ring_plan(shards: usize, budget: Option<usize>, max_buffers: u16) -> RingPlan {
    let max_buffers = max_buffers.clamp(MIN_BUFFERS, MAX_BUFFERS);
    let shards = shards.max(1);

    let Some(budget) = budget else {
        // No limit to respect: every shard may have a ring, with the pool the
        // operator asked for.
        return RingPlan {
            uring_shards: shards,
            buffers: max_buffers,
        };
    };

    let one_shard = RING_OVERHEAD + MIN_BUFFERS as usize * BUFFER_BYTES;
    // How many shards could have even the smallest useful ring.
    let affordable = (budget / one_shard).max(1).min(shards);
    // Give each of them as much of what is left as it may have.
    let per_shard = budget / affordable;
    let buffers = (per_shard.saturating_sub(RING_OVERHEAD) / BUFFER_BYTES)
        .max(MIN_BUFFERS as usize)
        .min(max_buffers as usize) as u16;

    RingPlan {
        uring_shards: affordable,
        buffers,
    }
}

/// `DBNOOP_PBUF` (`PGNOOP_PBUF` is the old name), when the operator set it.
///
/// The plan's number is a ceiling and this is a wish: `ring_plan` takes it as
/// `max_buffers`, so asking for more than the budget allows is answered by the
/// budget, and asking for less is honoured.
pub fn pbuf_override() -> Option<u16> {
    env("PBUF").and_then(|v| v.trim().parse::<u16>().ok())
}

/// The config file to read when `--config` does not name one.
///
/// `./dbnoop.json` is the name; `./pgnoop.json` is read when it is the only one
/// there, so an existing installation keeps its settings.
fn config_path() -> PathBuf {
    let named = PathBuf::from("./dbnoop.json");
    if named.exists() {
        return named;
    }
    let old = PathBuf::from("./pgnoop.json");
    if old.exists() {
        return old;
    }

    named
}

pub struct Config {
    pub host: String,
    pub port: u16,
    /// Whether the synthetic TPC-C answers are on. Off by default: a server that
    /// answers a district sum without being asked is asserting numbers it has no
    /// data for, and a client that has not asked for the profile may be one that
    /// validates rows.
    pub tpcc_profile: bool,
    /// Warehouses the TPCC schema is said to have: `sum(d_next_o_id)` is 3001
    /// per district and ten districts per warehouse.
    pub warehouses: u64,
    /// How often to print the TPCC counters on stderr, in seconds. Zero is off:
    /// a benchmark's stderr is read by whoever runs it, not by the server.
    pub stats_every: u64,
    /// The MySQL port, when one is served. Off by default: a server that opens a
    /// second port nobody asked for is one an operator has to remember to close.
    pub mysql_port: Option<u16>,
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
                eprintln!("dbnoop: io_uring is Linux-only; this build has epoll");
                std::process::exit(2);
            }
            other => {
                eprintln!("dbnoop: unknown --io value {other:?}; expected `uring` or `epoll`");
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
/// driver that is about to. `DBNOOP_STARTUP_TIMEOUT_MS` overrides it, and `0`
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
        parse_startup_timeout(env("STARTUP_TIMEOUT_MS").as_deref()).unwrap_or(Some(
            std::time::Duration::from_millis(STARTUP_TIMEOUT_DEFAULT_MS),
        ))
    })
}

/// `DBNOOP_STARTUP_TIMEOUT_MS` as the timeout it means. Unset is the default,
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
            "STARTUP_TIMEOUT_MS={value:?} is not a number of milliseconds; \
             expected a positive integer, or 0 to disable"
        )),
    }
}

impl Config {
    pub fn load() -> Self {
        let cli = CliArgs::parse();

        // Decided here, once, so a bad value stops the server before it binds
        // rather than being discovered -- or silently ignored -- per connection.
        let timeout = match parse_startup_timeout(env("STARTUP_TIMEOUT_MS").as_deref()) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("dbnoop: {e}");
                std::process::exit(2);
            }
        };
        let _ = STARTUP_TIMEOUT.set(timeout);

        let path = match cli.config {
            Some(path) => path,
            None => env("CONFIG").map(PathBuf::from).unwrap_or_else(config_path),
        };
        let file: FileConfig = std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let default_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        // **CLI, then the environment, then the file.** The environment is read
        // here rather than by clap so that both prefixes can be honoured (see
        // `env`), and a value that is not a number stops the server before it
        // binds.
        let number = |name: &str| -> Option<u16> {
            match parse_env::<u16>(name, env(name)) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("dbnoop: {e}");
                    std::process::exit(2);
                }
            }
        };
        let given = |cli: Option<u16>, name: &str| -> Option<u16> { cli.or_else(|| number(name)) };
        let workers = match parse_env::<usize>("WORKERS", env("WORKERS")) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("dbnoop: {e}");
                std::process::exit(2);
            }
        };
        let warehouses = match parse_env::<u64>("WAREHOUSES", env("WAREHOUSES")) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("dbnoop: {e}");
                std::process::exit(2);
            }
        };
        let stats_every = match parse_env::<u64>("STATS_EVERY", env("STATS_EVERY")) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("dbnoop: {e}");
                std::process::exit(2);
            }
        };

        // Only a profile this server has is accepted, and the answer says which
        // ones those are: a typo must not silently measure the wrong thing.
        let profile = cli.profile.or_else(|| env("PROFILE")).or(file.profile);
        let tpcc_profile = match profile.as_deref().map(str::trim) {
            None | Some("") | Some("none") => false,
            Some("tpcc") => true,
            Some(other) => {
                eprintln!("dbnoop: unknown --profile {other:?}; expected `tpcc`");
                std::process::exit(2);
            }
        };

        Config {
            host: cli
                .host
                .or_else(|| env("HOST"))
                .or(file.host)
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            port: given(cli.port, "PORT").or(file.port).unwrap_or(5432),
            mysql_port: given(cli.mysql_port, "MYSQL_PORT").or(file.mysql_port),
            tpcc_profile,
            warehouses: cli
                .warehouses
                .or(warehouses)
                .or(file.warehouses)
                .unwrap_or(crate::tpcc::DEFAULT_WAREHOUSES),
            stats_every: cli
                .stats_every
                .or(stats_every)
                .or(file.stats_every)
                .unwrap_or(0),
            workers: cli
                .workers
                .or(workers)
                .or(file.workers)
                .unwrap_or(default_workers),
            io: cli
                .io
                .or_else(|| env("IO"))
                .or(file.io)
                .map(|s| Io::parse(&s))
                .unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **A number from the environment that is not a number is an error.**
    /// `DBNOOP_PORT=5432x` was dropped, and a dropped port is the default port:
    /// the server bound somewhere the operator did not ask for and nothing said
    /// so.
    #[test]
    fn a_malformed_environment_number_is_refused() {
        for bad in ["5432x", "", " ", "-1", "1.5", "0x10"] {
            assert!(
                parse_env::<u16>("PORT", Some(bad.to_string())).is_err(),
                "{bad:?} must be refused, not dropped"
            );
        }
        for bad in ["4x", "many"] {
            assert!(parse_env::<usize>("WORKERS", Some(bad.to_string())).is_err());
        }

        assert_eq!(
            parse_env::<u16>("PORT", None),
            Ok(None),
            "unset is the default"
        );
        assert_eq!(
            parse_env::<u16>("PORT", Some(" 5433 ".to_string())),
            Ok(Some(5433))
        );
    }

    /// **The plan is arithmetic on a budget, and the budget is the user's.**
    /// 64 shards sharing 8 MiB cannot each have a ring: a ring's queues are
    /// ~384 KiB and each provided buffer pins 32 KiB, so giving every shard a
    /// ring is how shards came to die of `ENOMEM` at startup and left their
    /// listeners behind in a `SO_REUSEPORT` group.
    #[test]
    fn the_ring_plan_spends_the_budget_it_has() {
        // No limit: every shard on the ring, with the pool it asked for.
        let plan = ring_plan(64, None, MAX_BUFFERS);
        assert_eq!(
            plan,
            RingPlan {
                uring_shards: 64,
                buffers: MAX_BUFFERS
            }
        );

        // 8 MiB and 64 shards: fewer shards with a usable ring, the rest on
        // epoll, and never more memory than the budget.
        let plan = ring_plan(64, Some(8 * 1024 * 1024), MAX_BUFFERS);
        assert!(
            (1..64).contains(&plan.uring_shards),
            "{} of 64",
            plan.uring_shards
        );
        assert!(plan.buffers >= MIN_BUFFERS, "{} buffers", plan.buffers);
        let spent = plan.uring_shards * (RING_OVERHEAD + plan.buffers as usize * BUFFER_BYTES);
        assert!(
            spent <= 8 * 1024 * 1024,
            "{spent} bytes planned out of 8 MiB"
        );

        // Four shards and the same budget: all of them, with a pool in
        // proportion.
        let plan = ring_plan(4, Some(8 * 1024 * 1024), MAX_BUFFERS);
        assert_eq!(plan.uring_shards, 4);
        assert!(plan.buffers > MIN_BUFFERS, "{} buffers", plan.buffers);

        // A budget too small for even one useful ring still leaves one shard
        // trying (it falls back to epoll if the kernel refuses), and never a
        // zero or a panic.
        let plan = ring_plan(64, Some(64 * 1024), MAX_BUFFERS);
        assert_eq!(plan.uring_shards, 1);
        assert_eq!(plan.buffers, MIN_BUFFERS);

        // An operator asking for a small pool is honoured; asking for a large
        // one is bounded by the budget.
        assert_eq!(ring_plan(64, None, MIN_BUFFERS).buffers, MIN_BUFFERS);
        assert_eq!(ring_plan(64, None, 9999).buffers, MAX_BUFFERS);
    }

    /// **This box's budget is read, not assumed.** `RLIMIT_MEMLOCK` is what the
    /// kernel accounts a ring against on Linux, and the plan is only useful if
    /// it is the real number.
    #[test]
    fn the_memlock_limit_is_read_from_the_kernel() {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: a plain kernel-filled struct.
            let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
            let rc = unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) };
            assert_eq!(rc, 0);
            if limit.rlim_cur == libc::RLIM_INFINITY {
                assert_eq!(memlock_bytes(), None, "unlimited is no limit");
            } else {
                assert_eq!(memlock_bytes(), Some(limit.rlim_cur as usize));
            }
        }
        #[cfg(not(target_os = "linux"))]
        assert_eq!(memlock_bytes(), None);
    }

    /// **A value that does not parse is an error, not "disabled".** The
    /// parse failure became `None`, and `None` is also the explicit
    /// disabled state, so `DBNOOP_STARTUP_TIMEOUT_MS=60000x` -- a typo --
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
