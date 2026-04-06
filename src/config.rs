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

    /// Number of tokio worker threads (0 = number of logical CPUs)
    #[arg(long, env = "PGNOOP_WORKERS")]
    workers: Option<usize>,
}

#[derive(Deserialize, Default)]
struct FileConfig {
    host: Option<String>,
    port: Option<u16>,
    workers: Option<usize>,
}

pub struct Config {
    pub host: String,
    pub port: u16,
    pub workers: usize,
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
        }
    }
}
