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
    /// so the two can be measured against each other on one binary.
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
    Uring,
    Epoll,
}

impl Io {
    fn parse(s: &str) -> Io {
        match s.trim().to_ascii_lowercase().as_str() {
            "epoll" | "tokio" => Io::Epoll,
            _ => Io::Uring,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
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
                .unwrap_or(Io::Uring),
        }
    }
}
