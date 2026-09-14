mod config;
mod handler;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
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
        NoopFactory(Arc::new(NoopHandler::new()))
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

/// Pin the calling thread to one CPU, so a shard stays where its data is.
///
/// Best effort: a failure here costs locality, not correctness, and a container
/// with a restricted cpuset is a normal reason to fail.
fn pin_to_cpu(cpu: usize) -> bool {
    // SAFETY: cpu_set_t is a plain bitmask; we zero it, set one bit within
    // range, and hand it to sched_setaffinity with its own size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);

        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
    }
}

/// One listener per shard on the SAME port, via SO_REUSEPORT.
///
/// This is the half that makes share-nothing possible. With a single shared
/// listener the accepting thread must hand the connection to whichever worker
/// picks it up; with a listener per shard the KERNEL picks the shard at accept
/// time and the connection never moves again.
fn shard_listener(addr: SocketAddr, backlog: i32) -> std::io::Result<std::net::TcpListener> {
    let domain = socket2::Domain::for_address(addr);
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;

    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;

    Ok(socket.into())
}

/// One shard: a pinned thread, its own single-threaded reactor, its own
/// listener, and every connection it accepts served to completion on it.
fn run_shard(id: usize, addr: SocketAddr, pin: bool, factory: Arc<NoopFactory>) {
    if pin && !pin_to_cpu(id) {
        eprintln!("pgnoop: shard {id} could not pin to cpu {id}; continuing unpinned");
    }

    let listener = match shard_listener(addr, 1024) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("pgnoop: shard {id} could not bind {addr}: {e}");
            return;
        }
    };

    // current_thread, not multi_thread: a shard IS one thread. There is no work
    // stealing because there is nothing to steal from -- which is the entire
    // point. The measured cost of the alternative on lab2x1 was 1.14 futex
    // calls per query, a cross-core handoff per request.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("shard runtime");

    rt.block_on(async move {
        let listener = TcpListener::from_std(listener).expect("listener into tokio");

        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    // Nagle off: these replies are tens of bytes and a benchmark
                    // measures their latency.
                    let _ = socket.set_nodelay(true);

                    let factory = factory.clone();
                    // spawn, not spawn_blocking: stays on THIS runtime, hence
                    // this thread, hence this core.
                    tokio::spawn(async move {
                        if let Err(e) = process_socket(socket, None, factory).await {
                            eprintln!("connection error: {e}");
                        }
                    });
                }
                Err(e) => {
                    eprintln!("pgnoop: shard {id} accept failed: {e}");
                    return;
                }
            }
        }
    });
}

fn main() {
    let config = Config::load();

    let shards = if config.workers == 0 {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    } else {
        config.workers
    };

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("host:port");

    // Pinning only makes sense while shards fit on distinct CPUs.
    let pin = shards <= std::thread::available_parallelism().map_or(1, |n| n.get());

    eprintln!(
        "pgnoop listening on {}:{} ({} shards, SO_REUSEPORT, pinned={})",
        config.host, config.port, shards, pin
    );

    // One handler, shared by every shard. This is NOT full share-nothing and the
    // reason is correctness: DDL arriving on a connection the kernel put on
    // shard 3 must be visible to a metadata SELECT on shard 17, so the schema
    // catalog has to be common. It is off the hot path -- an ordinary SELECT
    // returns from parse_metadata_select before the lock is reached -- so what
    // remains shared per query is the Arc refcount, not the catalog.
    let factory = Arc::new(NoopFactory::new());

    let mut threads = Vec::with_capacity(shards);
    for id in 0..shards {
        let factory = factory.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("pgnoop-shard-{id}"))
                .spawn(move || run_shard(id, addr, pin, factory))
                .expect("spawn shard"),
        );
    }

    for t in threads {
        let _ = t.join();
    }
}
