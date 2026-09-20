mod config;
mod handler;
#[cfg(target_os = "linux")]
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
///
/// Linux only. `sched_setaffinity` and `cpu_set_t` do not exist on Apple
/// platforms, and there is no equivalent that can be dropped in -- Darwin
/// threads are placed by the scheduler's own affinity hints, not by a mask the
/// process sets. Pinning is therefore compiled out rather than faked.
#[cfg(target_os = "linux")]
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

/// Resolve `host:port` into a socket address to bind.
///
/// **Names, not just literals.** This parsed `SocketAddr` directly, which
/// accepts `127.0.0.1:5432` and rejects `localhost:5432` -- a panic, not an
/// error, because the parse was `.expect`ed. `TcpListener::bind` had resolved
/// names all along, so the regression was introduced by resolving earlier and
/// doing less. The README documents `--host` as a bind address; a name is a bind
/// address.
///
/// The first resolved address is bound, which is what `bind` does too. A name
/// that resolves to nothing is an error the caller reports, not a panic.
fn resolve_bind_addr(host: &str, port: u16) -> std::io::Result<SocketAddr> {
    use std::net::ToSocketAddrs;

    (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{host}:{port} resolved to no addresses"),
            )
        })
}

/// Whether this platform can pin at all. Asked once, so a platform without
/// pinning never prints a per-shard "could not pin" line for something it never
/// attempted.
const PINNING_SUPPORTED: bool = cfg!(target_os = "linux");

/// The backend to actually serve with, decided once before any shard starts.
///
/// A backend being *available* is not the same as it being *usable*: under
/// Docker's default seccomp profile `io_uring_setup` returns `EPERM`, so every
/// shard fails to build its ring, every shard returns false, and the process
/// exits nonzero having served nothing -- with a bind that succeeded and a port
/// that looks free to the client. Falling back to epoll turns that into a
/// slower server instead of no server.
///
/// Only the ring is probed. epoll is a plain syscall every platform carries,
/// and if it cannot be had the failure belongs to whoever supervises the
/// process, not to a silent substitution.
#[cfg(target_os = "linux")]
fn resolve_io(requested: Io) -> Io {
    io_to_serve(requested, crate::uring::probe())
}

/// The decision itself, separate from the probe so both branches are testable
/// on a box where the ring works and one where it does not.
#[cfg(target_os = "linux")]
fn io_to_serve(requested: Io, probe: std::io::Result<()>) -> Io {
    match (requested, probe) {
        (Io::Uring, Ok(())) => Io::Uring,
        (Io::Uring, Err(e)) => {
            eprintln!(
                "pgnoop: io_uring unavailable ({e}); serving with epoll. \
                 Under Docker this is the default seccomp profile denying \
                 io_uring_setup; --io epoll makes the choice explicit."
            );
            Io::Epoll
        }
        (Io::Epoll, _) => Io::Epoll,
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

    // The deadline is on the *connection*, not on each read: it is the startup
    // handshake that has to finish, and a client that dribbles its startup
    // packet out one byte at a time would otherwise extend it indefinitely.
    let startup_deadline =
        config::startup_timeout().map(|t| tokio::time::Instant::now() + t);

    loop {
        // A client that has not completed startup may legitimately have sent
        // nothing yet, and `read` would wait for it forever: the socket is open,
        // the shard has a slot, and neither is ever given back. Bounding the
        // wait is the whole mechanism -- once startup is done, the same read is
        // the unbounded one it always was.
        let n = if conn.awaiting_startup() {
            match startup_deadline {
                Some(deadline) => match tokio::time::timeout_at(deadline, socket.read(&mut chunk)).await {
                    Ok(Ok(n)) if n > 0 => n,
                    // Timed out, closed, or failed: all three end the connection,
                    // and the caller has what it needs to tell them apart in the
                    // log line below.
                    Ok(Ok(_)) | Ok(Err(_)) => return,
                    Err(_elapsed) => {
                        eprintln!(
                            "pgnoop: connection did not complete startup in time; closing"
                        );
                        return;
                    }
                },
                None => match socket.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                },
            }
        } else {
            match socket.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            }
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
fn run_shard(id: usize, addr: SocketAddr, pin: bool, io: Io, catalog: CatalogView) -> bool {
    // Only ever true on Linux, where the call and the constant both exist.
    #[cfg(target_os = "linux")]
    if pin && !pin_to_cpu(id) {
        eprintln!("pgnoop: shard {id} could not pin to cpu {id}; continuing unpinned");
    }

    // Unused where the enum has a single variant; the loop below is the same.
    #[cfg(not(target_os = "linux"))]
    let _ = io;

    let listener = match shard_listener(addr, 1024) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("pgnoop: shard {id} could not bind {addr}: {e}");
            return false;
        }
    };

    // io_uring needs no runtime at all: the codec is sans-io, so a shard is a
    // loop that submits reads, runs the codec, and submits writes. epoll keeps
    // tokio because that is what tokio is for.
    #[cfg(target_os = "linux")]
    if io == Io::Uring {
        if let Err(e) = crate::uring::run(listener, catalog, 4096) {
            eprintln!("pgnoop: shard {id} io_uring loop ended: {e}");
            return false;
        }
        return true;
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
                    // **A transient fd shortage must not stop the shard.** This
                    // used to return false, which ends the shard thread and, with
                    // every shard hitting it, the process: a burst of connections
                    // against a lowered fd limit turned into `no shard served` and
                    // a refused port, while the connections already accepted were
                    // dropped with it. `EMFILE`/`ENFILE`/`ENOMEM`/`ENOBUFS` are
                    // the shortage to wait out; anything else is the listener
                    // failing and belongs to the caller.
                    //
                    // `sleep` here does not block the shard the way it did in the
                    // io_uring loop: this accepts from a task on a current-thread
                    // runtime, so awaiting the timer parks the ACCEPT and lets the
                    // connection tasks keep running. That is the same property the
                    // ring path gets by recording a deadline instead of sleeping.
                    let err = e.raw_os_error().unwrap_or(0);

                    if matches!(err, libc::EMFILE | libc::ENFILE | libc::ENOMEM | libc::ENOBUFS) {
                        eprintln!(
                            "pgnoop: shard {id} accept failed: {e}; \
                             not accepting for {}ms (open connections continue)",
                            config::ACCEPT_BACKOFF.as_millis()
                        );
                        tokio::time::sleep(config::ACCEPT_BACKOFF).await;

                        continue;
                    }

                    eprintln!("pgnoop: shard {id} accept failed: {e}");
                    return false;
                }
            }
        }
    })
}

fn main() {
    let config = Config::load();

    // Before any shard exists: the ring is either usable in this process or it
    // is not, and the answer does not change per shard.
    #[cfg(target_os = "linux")]
    let io = resolve_io(config.io);
    #[cfg(not(target_os = "linux"))]
    let io = config.io;

    let shards = if config.workers == 0 {
        std::thread::available_parallelism().map_or(1, |n| n.get())
    } else {
        config.workers
    };

    let addr = match resolve_bind_addr(&config.host, config.port) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("pgnoop: cannot bind {}:{}: {e}", config.host, config.port);
            std::process::exit(2);
        }
    };

    // Pinning only makes sense while shards fit on distinct CPUs -- and only
    // where the platform can pin at all; elsewhere this stays false and no shard
    // tries.
    let pin = PINNING_SUPPORTED && shards <= std::thread::available_parallelism().map_or(1, |n| n.get());

    eprintln!(
        "pgnoop listening on {}:{} ({} shards, {}, SO_REUSEPORT, pinned={})",
        config.host, config.port, shards, io.as_str(), pin
    );

    // One handler, shared by every shard. This is NOT full share-nothing and the
    // reason is correctness: DDL arriving on a connection the kernel put on
    // shard 3 must be visible to a metadata SELECT on shard 17, so the schema
    // catalog has to be common. It is off the hot path -- an ordinary SELECT
    // returns from parse_metadata_select before the lock is reached -- so what
    // remains shared per query is the Arc refcount, not the catalog.
    let catalog = CatalogView(Arc::new(NoopHandler::new()));

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

    // A server whose every shard failed to bind must not exit 0. Discarding the
    // join results means a fully dead process reports success to whatever
    // supervises it.
    let mut served = 0usize;
    for t in threads {
        if t.join().unwrap_or(false) {
            served += 1;
        }
    }

    if served == 0 {
        eprintln!("pgnoop: no shard served; exiting nonzero");
        std::process::exit(1);
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    /// The reason this function exists: a container that denies the ring must
    /// still get a server. `EPERM` is what Docker's default seccomp profile
    /// returns from `io_uring_setup`, and it is the case Cianidos reproduced.
    #[test]
    fn a_denied_ring_falls_back_to_epoll() {
        let eperm = std::io::Error::from_raw_os_error(libc::EPERM);

        assert_eq!(io_to_serve(Io::Uring, Err(eperm)), Io::Epoll);
    }

    /// Any ring failure falls back, not only the one that was reported. A
    /// kernel without io_uring answers `ENOSYS`, and an older one answers
    /// `EINVAL` on the flags this server sets; both mean "no ring here".
    #[test]
    fn other_ring_failures_fall_back_too() {
        for errno in [libc::ENOSYS, libc::EINVAL, libc::EACCES] {
            let e = std::io::Error::from_raw_os_error(errno);

            assert_eq!(io_to_serve(Io::Uring, Err(e)), Io::Epoll, "errno {errno}");
        }
    }

    /// A working ring is kept, which is the path this box takes -- and the one
    /// the measurements in the README were taken on.
    #[test]
    fn a_working_ring_is_kept() {
        assert_eq!(io_to_serve(Io::Uring, Ok(())), Io::Uring);
    }

    /// Asking for epoll is never overridden by what the probe found. The
    /// operator may want epoll precisely because it is the fallback they are
    /// about to measure against the ring.
    #[test]
    fn an_explicit_epoll_is_never_replaced_by_the_ring() {
        assert_eq!(io_to_serve(Io::Epoll, Ok(())), Io::Epoll);
        assert_eq!(
            io_to_serve(Io::Epoll, Err(std::io::Error::from_raw_os_error(libc::EPERM))),
            Io::Epoll
        );
    }

    /// The probe's verdict on this box, recorded so the fallback path is not
    /// mistaken for dead code: on a host with io_uring it is `Ok`, and the CI
    /// container that denies it is the case the tests above pin down.
    #[test]
    fn probe_reports_what_this_box_can_do() {
        match crate::uring::probe() {
            Ok(()) => assert_eq!(resolve_io(Io::Uring), Io::Uring),
            Err(e) => {
                eprintln!("this box cannot create a ring: {e}");

                assert_eq!(resolve_io(Io::Uring), Io::Epoll);
            }
        }
    }

    /// **A hostname is a bind address.** `--host localhost` used to panic with
    /// `AddrParseError` because the address was parsed as a literal; `bind`
    /// resolved names before, so this was a regression in name support rather
    /// than a decision.
    #[test]
    fn a_hostname_resolves() {
        let addr = resolve_bind_addr("localhost", 5432).expect("localhost resolves");

        assert_eq!(addr.port(), 5432);
        assert!(
            addr.ip().is_loopback(),
            "localhost must resolve to a loopback address, got {addr}"
        );
    }

    /// A literal is still a literal, and IPv6 in brackets still works -- this is
    /// the address form the release builds are tested with.
    #[test]
    fn a_literal_resolves_to_itself() {
        assert_eq!(
            resolve_bind_addr("127.0.0.1", 15432).unwrap(),
            "127.0.0.1:15432".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            resolve_bind_addr("0.0.0.0", 5432).unwrap(),
            "0.0.0.0:5432".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            resolve_bind_addr("::1", 5432).unwrap(),
            "[::1]:5432".parse::<SocketAddr>().unwrap()
        );
    }

    /// A name that does not resolve is an ERROR, not a panic: the caller prints
    /// it and exits 2. This is the half of the old behaviour that was worse than
    /// the missing feature.
    #[test]
    fn a_name_that_does_not_resolve_is_an_error() {
        let err = match resolve_bind_addr("no-such-host.invalid", 5432) {
            Ok(a) => panic!("an invalid host resolved to {a}"),
            Err(e) => e,
        };

        // Reported, not unwound. The message comes from the resolver, so what is
        // asserted is that there IS one and that it is not a panic.
        assert!(!err.to_string().is_empty(), "the error must say something: {err}");
    }
}
