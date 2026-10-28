//! db-noop: a no-op database server for benchmark clients.
//!
//! Two wire protocols -- PostgreSQL and MySQL/MariaDB -- over one shard-per-core
//! io_uring (or epoll) loop, and an answer for every statement that is valid but
//! does nothing. The point is to measure a CLIENT: against a real database both
//! clients make the server do the same work, so a comparison measures the
//! server; against this one the server does nothing, so it measures them.
//!
//! The TPC-C rules in `tpcc` are the one exception, and they are explicit: a
//! handful of statement shapes answered with the numbers a TPC-C client reads
//! its throughput from, without which such a run reports zero.

pub mod config;
pub mod handler;
pub mod mysql;
pub mod protocol;
pub mod sql;
pub mod tpcc;
#[cfg(target_os = "linux")]
pub mod uring;
pub mod wire;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use config::{Config, Io};
use protocol::{Codec, Protocol, Report};

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

/// Resolve `host:port` into the socket addresses it names, in order.
///
/// **Names, not just literals.** This parsed `SocketAddr` directly, which
/// accepts `127.0.0.1:5432` and rejects `localhost:5432` -- a panic, not an
/// error, because the parse was `.expect`ed. `TcpListener::bind` had resolved
/// names all along, so the regression was introduced by resolving earlier and
/// doing less. The README documents `--host` as a bind address; a name is a bind
/// address.
///
/// **All of them, not the first.** `bind` tries each address a name resolves
/// to until one binds; keeping only the first meant `localhost` with its first
/// address occupied exited with no shard served while the base bound the
/// second. A name that resolves to nothing is an error the caller reports.
fn resolve_bind_addrs(host: &str, port: u16) -> std::io::Result<Vec<SocketAddr>> {
    use std::net::ToSocketAddrs;

    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();

    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{host}:{port} resolved to no addresses"),
        ));
    }

    Ok(addrs)
}

/// One listener per shard, on the first candidate address where every shard
/// can have one.
///
/// Bound here, before any shard exists, for two reasons. A shard cannot try
/// the next address on its own -- every shard has to be on the SAME one, or
/// the kernel is not balancing one port -- so the choice belongs to the
/// process. And a bind that fails is then a startup error with a message,
/// rather than a thread that exits and a process that reports "no shard
/// served" after the fact.
///
/// The first shard's listener decides the address: for a port of 0 that is
/// the kernel's pick, and the rest bind to it rather than each getting a
/// port of its own. The error returned is the last candidate's.
fn bind_shards(
    candidates: &[SocketAddr],
    shards: usize,
    backlog: i32,
) -> std::io::Result<(SocketAddr, Vec<std::net::TcpListener>)> {
    let mut last = std::io::Error::new(std::io::ErrorKind::InvalidInput, "no addresses to bind");

    for &candidate in candidates {
        match bind_shards_to(candidate, shards, backlog) {
            Ok(bound) => return Ok(bound),
            Err(e) => last = e,
        }
    }

    Err(last)
}

fn bind_shards_to(
    candidate: SocketAddr,
    shards: usize,
    backlog: i32,
) -> std::io::Result<(SocketAddr, Vec<std::net::TcpListener>)> {
    let first = shard_listener(candidate, backlog)?;
    let addr = first.local_addr()?;
    let mut listeners = Vec::with_capacity(shards);
    listeners.push(first);

    for _ in 1..shards {
        listeners.push(shard_listener(addr, backlog)?);
    }

    Ok((addr, listeners))
}

/// The CPUs this process may run on, in order, from its affinity mask.
///
/// Shard ids used to be used as CPU ids. Under a restricted cpuset --
/// `docker --cpuset-cpus=2` -- the allowed CPUs are not `0..n`, so shard 0
/// asked for CPU 0, was refused, and ran unpinned after the banner had
/// already said `pinned=true`. The mask is what the kernel will actually
/// honour; shards are laid out on it.
#[cfg(target_os = "linux")]
fn allowed_cpus() -> Vec<usize> {
    // SAFETY: cpu_set_t is a plain bitmask that the kernel fills; it is zeroed
    // first and read only through CPU_ISSET within its own size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }

        (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| libc::CPU_ISSET(cpu, &set))
            .collect()
    }
}

/// No mask to read, so nothing to pin to: the scheduler places the shards.
#[cfg(not(target_os = "linux"))]
fn allowed_cpus() -> Vec<usize> {
    Vec::new()
}

/// The CPU shard `id` is pinned to, or none when the shards outnumber the
/// CPUs -- two shards pinned to one CPU is worse than two the scheduler may
/// move, so then nothing is pinned.
fn cpu_for_shard(allowed: &[usize], id: usize) -> Option<usize> {
    allowed.get(id).copied()
}

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
                "dbnoop: io_uring unavailable ({e}); serving with epoll. \
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
async fn serve(mut socket: TcpStream, codec: Codec) {
    let mut conn = codec.session();
    let mut pending: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut out: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];

    // **MySQL speaks first.** Its client waits for the greeting before writing
    // anything, so a shard that read first would wait for a client that is
    // waiting for it. PostgreSQL has no such packet and writes nothing here.
    if let Some(prelude) = codec.prelude() {
        if socket.write_all(prelude).await.is_err() {
            return;
        }
    }

    // The deadline is on the *connection*, not on each read: it is the startup
    // handshake that has to finish, and a client that dribbles its startup
    // packet out one byte at a time would otherwise extend it indefinitely.
    let startup_deadline = config::startup_timeout().map(|t| tokio::time::Instant::now() + t);

    loop {
        // A client that has not completed startup may legitimately have sent
        // nothing yet, and `read` would wait for it forever: the socket is open,
        // the shard has a slot, and neither is ever given back. Bounding the
        // wait is the whole mechanism -- once startup is done, the same read is
        // the unbounded one it always was.
        let n = if conn.awaiting_startup() {
            match startup_deadline {
                Some(deadline) => match tokio::time::timeout_at(deadline, socket.read(&mut chunk))
                    .await
                {
                    Ok(Ok(n)) if n > 0 => n,
                    // Timed out, closed, or failed: all three end the connection,
                    // and the caller has what it needs to tell them apart in the
                    // log line below.
                    Ok(Ok(_)) | Ok(Err(_)) => return,
                    Err(_elapsed) => {
                        eprintln!("dbnoop: connection did not complete startup in time; closing");
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
            let used = codec.advance(&mut conn, &chunk[..n], &mut out);
            if used < n {
                pending.extend_from_slice(&chunk[used..n]);
            }
        } else {
            pending.extend_from_slice(&chunk[..n]);
            let used = codec.advance(&mut conn, &pending, &mut out);
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

/// What one shard is asked to serve, and how.
struct ShardSpec {
    protocol: Protocol,
    /// Where to pin, if anywhere.
    cpu: Option<usize>,
    /// The backend it was *planned* for: a shard planned for the ring that
    /// cannot have one serves on epoll and says so, rather than ending and
    /// leaving its port with one listener fewer.
    backend: Io,
    /// Provided buffers for its ring, from the process's ring plan.
    buffers: u16,
    codec: Codec,
}

/// One shard: a pinned thread, its own single-threaded reactor, its own
/// listener, and every connection it accepts served to completion on it.
fn run_shard(
    id: usize,
    listener: std::net::TcpListener,
    report: std::sync::mpsc::Sender<Report>,
    spec: ShardSpec,
) -> bool {
    let ShardSpec {
        protocol,
        cpu,
        backend,
        buffers,
        codec,
    } = spec;
    let did_pin = match cpu {
        #[cfg(target_os = "linux")]
        Some(cpu) => {
            let ok = pin_to_cpu(cpu);
            if !ok {
                eprintln!("dbnoop: shard {id} could not pin to cpu {cpu}; continuing unpinned");
            }
            ok
        }
        #[cfg(not(target_os = "linux"))]
        Some(_) => false,
        None => false,
    };
    let _ = report.send(Report::Pinned {
        protocol,
        pinned: did_pin,
    });

    // io_uring needs no runtime at all: the codec is sans-io, so a shard is a
    // loop that submits reads, runs the codec, and submits writes. epoll keeps
    // tokio because that is what tokio is for.
    #[cfg(target_os = "linux")]
    if backend == Io::Uring {
        match crate::uring::try_run(listener, codec.clone(), 4096, buffers, Some(report.clone())) {
            Ok(crate::uring::Attempt::Served) => return true,
            Ok(crate::uring::Attempt::NoRing(listener)) => {
                // **The listener comes back, and this shard serves it on epoll.**
                // Dying here instead leaves the socket in the `SO_REUSEPORT`
                // group, where the kernel keeps routing connections to a port
                // nobody accepts on.
                return serve_epoll(id, listener, report, protocol, codec);
            }
            Err(e) => {
                eprintln!("dbnoop: shard {id} io_uring loop ended: {e}");

                return false;
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = backend;

    serve_epoll(id, listener, report, protocol, codec)
}

/// Serve one shard's listener on epoll, through a single-threaded tokio runtime.
fn serve_epoll(
    id: usize,
    listener: std::net::TcpListener,
    report: std::sync::mpsc::Sender<Report>,
    protocol: Protocol,
    codec: Codec,
) -> bool {
    // Said before the loop, and the loop never returns in normal use: a shard is
    // serving when it says so, and the banner counts it from here.
    let _ = report.send(Report::Serving {
        protocol,
        backend: Io::Epoll,
    });

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

                    let codec = codec.clone();
                    // spawn, not spawn_blocking: stays on THIS runtime, hence
                    // this thread, hence this core.
                    tokio::spawn(async move {
                        serve(socket, codec).await;
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

                    if matches!(
                        err,
                        libc::EMFILE | libc::ENFILE | libc::ENOMEM | libc::ENOBUFS
                    ) {
                        eprintln!(
                            "dbnoop: shard {id} accept failed: {e}; \
                             not accepting for {}ms (open connections continue)",
                            config::ACCEPT_BACKOFF.as_millis()
                        );
                        tokio::time::sleep(config::ACCEPT_BACKOFF).await;

                        continue;
                    }

                    eprintln!("dbnoop: shard {id} accept failed: {e}");
                    return false;
                }
            }
        }
    })
}

/// How many shards are serving on the ring, from their own reports.
#[cfg(target_os = "linux")]
fn ring_serving(reports: &[Report]) -> usize {
    reports
        .iter()
        .filter(|r| {
            matches!(
                r,
                Report::Serving {
                    backend: Io::Uring,
                    ..
                }
            )
        })
        .count()
}

/// The TPCC counters, on stderr every `interval` and once when the server stops.
///
/// A benchmark that reads its throughput from the server reads it from the
/// district sum and the commit counters; this is the same information for
/// whoever is watching the log. Off unless asked for: a server that writes to
/// stderr during a measurement is a server in the measurement.
fn spawn_stats(interval: std::time::Duration) {
    std::thread::Builder::new()
        .name("dbnoop-stats".to_string())
        .spawn(move || loop {
            std::thread::sleep(interval);
            eprintln!("{}", crate::tpcc::line());
        })
        .expect("spawn the stats thread");
}

/// Serve until the process is stopped.
///
/// What both binary names run: `dbnoop` is the name, and `pgnoop` is the same
/// program under the name the scripts written for the PostgreSQL-only server
/// already use.
pub fn run() {
    let config = Config::load();
    crate::tpcc::set_warehouses(config.warehouses);
    crate::tpcc::set_profile(config.tpcc_profile);
    if config.stats_every > 0 {
        spawn_stats(std::time::Duration::from_secs(config.stats_every));
    }

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

    // **One worker set per listener.** `--workers` is shards per protocol, so a
    // server with both ports open has twice as many threads; pinning lays them
    // out on the affinity mask in order and is skipped entirely when they do not
    // fit on distinct CPUs, because two shards on one core is worse than two the
    // scheduler may move.
    let mut sockets = vec![(Protocol::Postgres, config.port)];
    if let Some(port) = config.mysql_port {
        sockets.push((Protocol::Mysql, port));
    }
    let allowed = allowed_cpus();
    let total = shards * sockets.len();
    let cpus: Vec<Option<usize>> = if total <= allowed.len() {
        (0..total).map(|id| cpu_for_shard(&allowed, id)).collect()
    } else {
        vec![None; total]
    };

    // **How much ring this process may have, decided before any shard runs.**
    //
    // A ring's memory -- its queues, and the pages a provided-buffer ring pins --
    // is charged to the USER, not to the shard that has it, and `RLIMIT_MEMLOCK`
    // is 8 MiB on a default Linux host. The default `--workers 0` on a 32-CPU box
    // is 64 shards, which cannot all have a ring: the shards that cannot used to
    // die, and a dead shard's listener stayed in the `SO_REUSEPORT` group
    // routing connections to a port nobody accepted on. The plan says how many
    // shards may have one, and each of those gets a ring sized for what is left;
    // the rest serve on epoll, which is the same codec over a different syscall.
    let plan = config::ring_plan(
        total,
        config::memlock_bytes(),
        config::pbuf_override().unwrap_or(config::MAX_BUFFERS),
    );
    let uring_budget = if io == Io::Uring {
        plan.uring_shards
    } else {
        0
    };

    // Resolve, then bind every shard's listener, before any shard runs: a name's
    // addresses are tried in order until one takes all of them, and a failure is
    // a startup error rather than a thread that quietly exits.
    let (report_tx, report_rx) = std::sync::mpsc::channel();
    let mut threads = Vec::with_capacity(total);
    let mut banners = Vec::with_capacity(sockets.len());
    let mut next_cpu = 0usize;
    // The ring budget is split between the protocols rather than spent on the
    // one that happens to be spawned first: half a server on the ring and half
    // on epoll is worse than every shard on the same backend.
    let protocols = sockets.len();
    let mut ring_shares = (0..protocols)
        .map(|k| uring_budget / protocols + usize::from(k < uring_budget % protocols));

    for (protocol, port) in sockets {
        let candidates = match resolve_bind_addrs(&config.host, port) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("dbnoop: cannot bind {}:{port}: {e}", config.host);
                std::process::exit(2);
            }
        };
        let (addr, listeners) = match bind_shards(&candidates, shards, 1024) {
            Ok(bound) => bound,
            Err(e) => {
                eprintln!("dbnoop: cannot bind {}:{port}: {e}", config.host);
                std::process::exit(2);
            }
        };
        banners.push((protocol, addr));

        // This protocol's share of the ring budget, and no more.
        let mut rings_left = ring_shares.next().unwrap_or(0);

        for (id, listener) in listeners.into_iter().enumerate() {
            // One codec per listener, cloned into its shards: the PostgreSQL
            // catalog is shared by them all, and the MySQL codec holds nothing
            // to share.
            let codec = Codec::new(protocol);
            let cpu = cpus[next_cpu];
            next_cpu += 1;
            let report = report_tx.clone();
            let backend = if rings_left > 0 {
                rings_left -= 1;

                Io::Uring
            } else {
                Io::Epoll
            };
            let spec = ShardSpec {
                protocol,
                cpu,
                backend,
                buffers: plan.buffers,
                codec,
            };
            threads.push(
                std::thread::Builder::new()
                    .name(format!("dbnoop-{}-{id}", protocol.name()))
                    .spawn(move || run_shard(id, listener, report, spec))
                    .expect("spawn shard"),
            );
        }
    }
    drop(report_tx);

    // **Every shard reports before it serves**, so the banner states what is
    // true: how many shards are serving, on which backend, and how many are
    // pinned. Each shard sends two messages -- its pin, then its backend -- and a
    // shard that died before sending is a shard the banner does not count. The
    // timeout is what turns that into a line rather than a hang.
    let mut reports: Vec<Report> = Vec::with_capacity(total * 2);
    for _ in 0..total * 2 {
        match report_rx.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(report) => reports.push(report),
            Err(_) => break,
        }
    }

    for (protocol, addr) in &banners {
        let pinned = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Report::Pinned { protocol: p, pinned: true } if *p == *protocol
                )
            })
            .count();
        let serving = reports
            .iter()
            .filter(|r| matches!(r, Report::Serving { protocol: p, .. } if *p == *protocol))
            .count();
        let ring = reports
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Report::Serving { protocol: p, backend: Io::Uring } if *p == *protocol
                )
            })
            .count();

        eprintln!(
            "dbnoop {} listening on {addr} ({shards} shards, SO_REUSEPORT, \
             serving {serving}/{shards} (io_uring {ring}, epoll {}), pinned={pinned}/{shards})",
            protocol.name(),
            serving - ring,
        );
    }

    let serving = reports
        .iter()
        .filter(|r| matches!(r, Report::Serving { .. }))
        .count();
    if serving < total {
        eprintln!(
            "dbnoop: **{} of {total} shards are not serving**; the rest are. A shard \
             that cannot start leaves its port with fewer listeners, and with none at \
             all if every shard of a protocol failed.",
            total - serving
        );
    }

    // What the ring actually granted, and how it was paid for. Reported here as
    // well as at startup because this is the line a run's log keeps, and a
    // measurement is only reproducible if it says which ring produced it.
    #[cfg(target_os = "linux")]
    if ring_serving(&reports) > 0 {
        eprintln!(
            "dbnoop: io_uring {}; {} shard(s) on the ring with {} buffer(s) each, \
             the rest on epoll",
            crate::uring::report(),
            ring_serving(&reports),
            plan.buffers,
        );
    }

    // A server whose every shard failed must not exit 0. Discarding the join
    // results means a fully dead process reports success to whatever
    // supervises it.
    let mut served = 0usize;
    for t in threads {
        if t.join().unwrap_or(false) {
            served += 1;
        }
    }

    if served == 0 {
        eprintln!("dbnoop: no shard served; exiting nonzero");
        std::process::exit(1);
    }

    // The last word on what was counted, whichever way the shards ended.
    eprintln!("{}", crate::tpcc::line());
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
            io_to_serve(
                Io::Epoll,
                Err(std::io::Error::from_raw_os_error(libc::EPERM))
            ),
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

    /// **Shards are pinned to the CPUs the process may use, not to their own
    /// ids.** Under a restricted cpuset (`docker --cpuset-cpus=2`) the
    /// allowed CPUs are not `0..n`, and shard 0 pinning to CPU 0 fails --
    /// after the banner has already claimed `pinned=true`. The affinity mask
    /// is the fact; the shard id is not.
    #[test]
    fn shards_are_mapped_onto_the_affinity_mask() {
        let all = allowed_cpus();
        assert!(!all.is_empty(), "the process runs somewhere");

        // Restrict THIS thread to the last allowed CPU, which is not CPU 0 on
        // any box with more than one, and read the mask back.
        let last = *all.last().unwrap();
        assert!(pin_to_cpu(last), "pinning to an allowed CPU succeeds");

        let allowed = allowed_cpus();
        assert_eq!(allowed, vec![last], "the mask is what was set");

        // Shard 0 maps onto the first ALLOWED CPU, whatever its number.
        assert_eq!(cpu_for_shard(&allowed, 0), Some(last));
        // And with more shards than CPUs nothing is pinned, since two shards
        // on one CPU is worse than two shards the scheduler may move.
        assert_eq!(cpu_for_shard(&allowed, 1), None);
        assert!(pin_to_cpu(cpu_for_shard(&allowed, 0).unwrap()));
    }

    /// **A hostname is a bind address.** `--host localhost` used to panic with
    /// `AddrParseError` because the address was parsed as a literal; `bind`
    /// resolved names before, so this was a regression in name support rather
    /// than a decision.
    ///
    /// A sandbox with neither a resolver nor an `/etc/hosts` cannot answer, and
    /// that is the environment rather than this code: `a_literal_resolves_to_itself`
    /// below pins the same path with addresses that need no resolver, and the
    /// resolution of a name is the branch this test adds.
    #[test]
    fn a_hostname_resolves() {
        let addrs = match resolve_bind_addrs("localhost", 5432) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("skipping: this host does not resolve `localhost`: {e}");

                return;
            }
        };

        assert!(!addrs.is_empty());
        for addr in &addrs {
            assert_eq!(addr.port(), 5432);
            assert!(
                addr.ip().is_loopback(),
                "localhost must resolve to loopback addresses, got {addr}"
            );
        }
    }

    /// **Every resolved address is a candidate, not just the first.**
    /// `TcpListener::bind` tries each address a name resolves to until one
    /// binds; keeping only the first meant that with `localhost`'s first
    /// address occupied and its second free, the server exited with no shard
    /// served while the base bound the second and ran.
    #[test]
    fn bind_tries_each_address_until_one_binds() {
        // Occupy a port with an ordinary listener (no SO_REUSEPORT), so a
        // shard listener on it fails the way a foreign process makes it fail.
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied = taken.local_addr().unwrap();
        let free: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let (bound, listeners) = bind_shards(&[occupied, free], 3, 16).expect("the second binds");

        assert_ne!(
            bound.port(),
            occupied.port(),
            "the occupied address was skipped"
        );
        assert_ne!(
            bound.port(),
            0,
            "the kernel's pick is reported, not the wildcard"
        );
        assert_eq!(listeners.len(), 3);
        for l in &listeners {
            assert_eq!(
                l.local_addr().unwrap(),
                bound,
                "every shard is on the same address"
            );
        }
    }

    /// When no candidate binds, the error is the last one's, and it is an
    /// error the caller reports rather than a panic.
    #[test]
    fn bind_reports_the_failure_when_nothing_binds() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let occupied = taken.local_addr().unwrap();

        let err = bind_shards(&[occupied, occupied], 2, 16).expect_err("nothing binds");
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse, "{err}");
    }

    /// A literal is still a literal, and IPv6 in brackets still works -- this is
    /// the address form the release builds are tested with.
    #[test]
    fn a_literal_resolves_to_itself() {
        assert_eq!(
            resolve_bind_addrs("127.0.0.1", 15432).unwrap(),
            vec!["127.0.0.1:15432".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            resolve_bind_addrs("0.0.0.0", 5432).unwrap(),
            vec!["0.0.0.0:5432".parse::<SocketAddr>().unwrap()]
        );
        assert_eq!(
            resolve_bind_addrs("::1", 5432).unwrap(),
            vec!["[::1]:5432".parse::<SocketAddr>().unwrap()]
        );
    }

    /// A name that does not resolve is an ERROR, not a panic: the caller prints
    /// it and exits 2. This is the half of the old behaviour that was worse than
    /// the missing feature.
    #[test]
    fn a_name_that_does_not_resolve_is_an_error() {
        let err = match resolve_bind_addrs("no-such-host.invalid", 5432) {
            Ok(a) => panic!("an invalid host resolved to {a:?}"),
            Err(e) => e,
        };

        // Reported, not unwound. The message comes from the resolver, so what is
        // asserted is that there IS one and that it is not a panic.
        assert!(
            !err.to_string().is_empty(),
            "the error must say something: {err}"
        );
    }
}
