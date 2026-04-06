mod config;
mod handler;

use std::sync::Arc;

use pgwire::api::auth::StartupHandler;
use pgwire::api::copy::CopyHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::PgWireServerHandlers;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use config::Config;
use handler::NoopHandler;

struct NoopFactory(Arc<NoopHandler>);

impl NoopFactory {
    fn new() -> Self {
        NoopFactory(Arc::new(NoopHandler))
    }
}

impl PgWireServerHandlers for NoopFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.0.clone()
    }

    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.0.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.0.clone()
    }

    fn copy_handler(&self) -> Arc<impl CopyHandler> {
        self.0.clone()
    }
}

fn main() {
    let config = Config::load();

    eprintln!(
        "pgnoop listening on {}:{} ({} workers)",
        config.host, config.port, config.workers
    );

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let factory = Arc::new(NoopFactory::new());
            let addr = format!("{}:{}", config.host, config.port);
            let listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let factory = factory.clone();
                tokio::spawn(async move {
                    if let Err(e) = process_socket(socket, None, factory).await {
                        eprintln!("connection error: {e}");
                    }
                });
            }
        });
}
