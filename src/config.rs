use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(name = "pgnoop", about = "NoOp PostgreSQL blackhole server for benchmarking")]
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

impl Config {
    pub fn load() -> Self {
        let cli = CliArgs::parse();

        let file: FileConfig = std::fs::read_to_string(&cli.config)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let default_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Config {
            host: cli.host.or(file.host).unwrap_or_else(|| "0.0.0.0".to_string()),
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
