mod config;
mod handler;
mod uring;
mod wire;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use config::{Config, Io};
use handler::{CatalogView, NoopHandler};
use wire::Conn;

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

/// One connection, read-process-write, with ONE write per read batch.
///
/// This is the half that sans-io buys. The codec only appends to `out`, so it
/// cannot flush early; this loop decides when bytes leave, and it decides once
/// per read no matter how many protocol messages arrived in it. A client that
/// pipelines Bind/Execute/Sync -- pgx does, and so does `saturate --depth N` --
/// gets one reply write instead of the three pgwire was making.
async fn serve(mut socket: TcpStream, catalog: CatalogView) {
    let mut conn = Conn::new();
    let mut pending: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];

    loop {
        let n = match socket.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        out.clear();

        // The common case is a whole batch in one read with nothing left over,
        // so the copy into `pending` is avoided when there is nothing pending.
        if pending.is_empty() {
            let used = conn.advance(&chunk[..n], &mut out, &catalog);
            if used < n {
                pending.extend_from_slice(&chunk[used..n]);
            }
        } else {
            pending.extend_from_slice(&chunk[..n]);
            let used = conn.advance(&pending, &mut out, &catalog);
            pending.drain(..used);
        }

        if !out.is_empty() && socket.write_all(&out).await.is_err() {
            return;
        }

        if conn.is_closed() {
            return;
        }
    }
}

/// One shard: a pinned thread, its own single-threaded reactor, its own
/// listener, and every connection it accepts served to completion on it.
fn run_shard(id: usize, addr: SocketAddr, pin: bool, io: Io, catalog: CatalogView) {
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

    // io_uring needs no runtime at all: the codec is sans-io, so a shard is a
    // loop that submits reads, runs the codec, and submits writes. epoll keeps
    // tokio because that is what tokio is for.
    if io == Io::Uring {
        if let Err(e) = crate::uring::run(listener, catalog, 4096) {
            eprintln!("pgnoop: shard {id} io_uring loop ended: {e}");
        }
        return;
    }

    // current_thread, not multi_thread: a shard IS one thread. There is no work
    // stealing because there is nothing to steal from -- which is the entire
    // point. The measured cost of the alternative was 1.14 futex
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

                    let catalog = catalog.clone();
                    // spawn, not spawn_blocking: stays on THIS runtime, hence
                    // this thread, hence this core.
                    tokio::spawn(async move {
                        serve(socket, catalog).await;
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
        "pgnoop listening on {}:{} ({} shards, {}, SO_REUSEPORT, pinned={})",
        config.host, config.port, shards, config.io.as_str(), pin
    );

    // One handler, shared by every shard. This is NOT full share-nothing and the
    // reason is correctness: DDL arriving on a connection the kernel put on
    // shard 3 must be visible to a metadata SELECT on shard 17, so the schema
    // catalog has to be common. It is off the hot path -- an ordinary SELECT
    // returns from parse_metadata_select before the lock is reached -- so what
    // remains shared per query is the Arc refcount, not the catalog.
    let catalog = CatalogView(Arc::new(NoopHandler::new()));

    let io = config.io;
    let mut threads = Vec::with_capacity(shards);
    for id in 0..shards {
        let catalog = catalog.clone();
        threads.push(
            std::thread::Builder::new()
                .name(format!("pgnoop-shard-{id}"))
                .spawn(move || run_shard(id, addr, pin, io, catalog))
                .expect("spawn shard"),
        );
    }

    for t in threads {
        let _ = t.join();
    }
}
