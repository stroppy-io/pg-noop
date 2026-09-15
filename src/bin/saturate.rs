//! A load generator that can actually saturate pg-noop.
//!
//! # Why this exists
//!
//! Measured on a 16-core / 32-thread x86-64 host: driving pg-noop with four stroppy processes
//! at 256 connections reached 825,238 q/s with **pgnoop at 4% of one core and
//! the clients at 2259%** -- 22.6 of 32 cores. At that point the number is a
//! measurement of the load generator, and every further improvement to the
//! server is invisible. DESIGN-perf-pfn.md makes exactly this point about
//! knowing the tool's own ceiling; this is that argument turned on the tool.
//!
//! # What it measures, and what it does not
//!
//! It PIPELINES: each connection keeps `--depth` Bind/Execute/Sync exchanges in
//! flight rather than waiting for each reply. That is the honest way to ask
//! "how fast can this server answer", because one-query-per-round-trip measures
//! LATENCY and the network, not the server. Both are useful and they are not
//! the same question -- run with `--depth 1` for the round-trip figure.
//!
//! It is deliberately dumb: pre-encoded bytes, no SQL, no parsing of replies
//! beyond counting ReadyForQuery ('Z') bytes, no allocation in the loop.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Latency samples retained per worker. Unbounded, a 5s run at 3M q/s keeps
/// 15M samples per worker and the final clone doubles it.
const MAX_SAMPLES: usize = 1 << 17;

fn arg(name: &str, default: usize) -> usize {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name {
            return it.next().and_then(|v| v.parse().ok()).unwrap_or(default);
        }
    }
    default
}

fn arg_str(name: &str, default: &str) -> String {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == name {
            return it.next().unwrap_or_else(|| default.to_string());
        }
    }
    default.to_string()
}

/// Count ReadyForQuery messages by walking the frame headers.
///
/// Scanning for the byte b'Z' counts payload bytes too -- a ParameterStatus
/// value or a column name containing 'Z' would end a batch early and corrupt
/// the measurement. `carry` holds a header split across reads.
struct Frames {
    carry: Vec<u8>,
    remaining: usize,
    /// Set when a frame header is not well formed. The stream cannot be trusted
    /// after that point, so the worker stops rather than measuring garbage.
    bad: bool,
}

/// Largest frame the generator will accept from the server under test.
const MAX_FRAME: i32 = 16 * 1024 * 1024;

impl Frames {
    fn new() -> Self {
        Frames {
            carry: Vec::with_capacity(8),
            remaining: 0,
            bad: false,
        }
    }

    fn count_ready(&mut self, mut buf: &[u8]) -> usize {
        let mut ready = 0;

        loop {
            if self.remaining > 0 {
                let skip = self.remaining.min(buf.len());
                self.remaining -= skip;
                buf = &buf[skip..];
            }
            if buf.is_empty() {
                return ready;
            }

            while self.carry.len() < 5 && !buf.is_empty() {
                self.carry.push(buf[0]);
                buf = &buf[1..];
            }
            if self.carry.len() < 5 {
                return ready;
            }

            let tag = self.carry[0];
            let len = i32::from_be_bytes(self.carry[1..5].try_into().unwrap());

            // Validate BEFORE updating state. A negative len cast to usize makes
            // `remaining` enormous and the reader then discards real input and
            // blocks; a Z frame with len < 4 would also be counted before it was
            // known to be well formed. This is the same defect that existed in
            // the server's own framing, written again in the code added to
            // avoid it.
            if !(4..=MAX_FRAME).contains(&len) {
                self.bad = true;

                return ready;
            }

            if tag == b'Z' {
                ready += 1;
            }
            self.remaining = (len as usize) - 4;
            self.carry.clear();
        }
    }
}

fn cstr(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn msg(out: &mut Vec<u8>, tag: u8, body: impl FnOnce(&mut Vec<u8>)) {
    out.push(tag);
    let at = out.len();
    out.extend_from_slice(&0i32.to_be_bytes());
    body(out);
    let len = (out.len() - at) as i32;
    out[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

/// Startup, then Parse one statement and keep it for the whole run -- which is
/// what a real driver does, and what makes the server's per-execute path the
/// thing under test.
fn handshake(sock: &mut TcpStream, sql: &str) -> std::io::Result<Frames> {
    let mut buf = Vec::new();
    let at = buf.len();
    buf.extend_from_slice(&0i32.to_be_bytes());
    buf.extend_from_slice(&196608i32.to_be_bytes());
    cstr(&mut buf, "user");
    cstr(&mut buf, "bench");
    cstr(&mut buf, "database");
    cstr(&mut buf, "bench");
    buf.push(0);
    let len = (buf.len() - at) as i32;
    buf[at..at + 4].copy_from_slice(&len.to_be_bytes());

    msg(&mut buf, b'P', |b| {
        cstr(b, "s1");
        cstr(b, sql);
        b.extend_from_slice(&0i16.to_be_bytes());
    });
    msg(&mut buf, b'S', |_| {});
    sock.write_all(&buf)?;

    // Drain until the second ReadyForQuery (startup's, then Parse's), framed
    // rather than scanned.
    let mut frames = Frames::new();
    let mut seen = 0;
    let mut chunk = [0u8; 4096];
    while seen < 2 {
        let n = sock.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        seen += frames.count_ready(&chunk[..n]);
        if frames.bad {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "malformed frame during handshake",
            ));
        }
    }

    // Returning Ok here would let a worker treat a truncated handshake as
    // success and count a connection that never became usable.
    if seen < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "handshake ended before ReadyForQuery",
        ));
    }

    Ok(frames)
}

fn main() {
    let addr = arg_str("--addr", "127.0.0.1:5432");
    let conns = arg("--conns", 64);
    let depth = arg("--depth", 32);
    let secs = arg("--secs", 8) as u64;
    let sql = arg_str("--sql", "select 1");

    // depth 0 leaves `batch` empty and makes `got < depth` false at once, so the
    // worker spins pushing latency samples until memory runs out.
    if depth == 0 {
        eprintln!("saturate: --depth must be >= 1");
        std::process::exit(2);
    }

    // One pipelined batch, encoded once and reused forever.
    let mut batch = Vec::new();
    for _ in 0..depth {
        msg(&mut batch, b'B', |b| {
            cstr(b, "");
            cstr(b, "s1");
            b.extend_from_slice(&0i16.to_be_bytes()); // format codes
            b.extend_from_slice(&0i16.to_be_bytes()); // params
            b.extend_from_slice(&0i16.to_be_bytes()); // result formats
        });
        msg(&mut batch, b'E', |b| {
            cstr(b, "");
            b.extend_from_slice(&0i32.to_be_bytes());
        });
        msg(&mut batch, b'S', |_| {});
    }
    let batch = Arc::new(batch);

    let done = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    // Workers that finish their handshake early would otherwise run before the
    // clock starts: their queries land in the numerator while part of their
    // execution is outside the denominator.
    let ready_workers = Arc::new(AtomicUsize::new(0));
    // Separate from ready_workers: a failed worker still has to arrive at the
    // barrier, but it must not be reported as a connection that generated load.
    let live_workers = Arc::new(AtomicUsize::new(0));
    let go = Arc::new(AtomicBool::new(false));
    // Per-batch round-trip times, nanoseconds. At --depth 1 a batch IS a query,
    // so these are query latencies; above that they are the time to answer a
    // pipelined batch and the label says so.
    //
    // Why this is measured at all: throughput medians hide the tail, and the
    // tail is where a scheduler shows itself. Two backends can agree on median
    // q/s and disagree completely about p99.
    let lat: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let mut threads = Vec::new();

    for _ in 0..conns {
        let (batch, done, total) = (batch.clone(), done.clone(), total.clone());
        let lat = lat.clone();
        let (ready_workers, go) = (ready_workers.clone(), go.clone());
        let live_workers = live_workers.clone();
        let addr = addr.clone();
        let sql = sql.clone();

        threads.push(std::thread::spawn(move || {
            let Ok(mut sock) = TcpStream::connect(&addr) else {
                ready_workers.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let _ = sock.set_nodelay(true);
            let Ok(mut frames) = handshake(&mut sock, &sql) else {
                ready_workers.fetch_add(1, Ordering::Relaxed);
                return;
            };

            // **Finite deadlines for the measurement phase.**
            //
            // `write_all` and `read` are blocking, and `done` is only checked
            // BETWEEN batches. A server that stalls mid-batch parks the worker
            // inside a syscall where no flag can reach it, `join` never returns,
            // and the harness hangs rather than reporting a bad run. Found in
            // review.
            //
            // Derived from the run rather than picked: a batch that has not been
            // answered in the time the whole run was meant to take is not slow,
            // it is a stall, and the `Err(_) => break 'work` arm already keeps
            // whatever the worker completed before it. The `.max(5)` is for very
            // short runs, where a per-batch bound below a few seconds would start
            // reporting scheduler noise as a failure.
            let stall = Duration::from_secs(secs.max(5));
            if sock.set_read_timeout(Some(stall)).is_err()
                || sock.set_write_timeout(Some(stall)).is_err()
            {
                // A worker that cannot be bounded is not one to run: it is the
                // exact thread that would hang the join.
                ready_workers.fetch_add(1, Ordering::Relaxed);
                return;
            }

            live_workers.fetch_add(1, Ordering::Relaxed);
            ready_workers.fetch_add(1, Ordering::Relaxed);
            while !go.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }

            let mut chunk = vec![0u8; 64 * 1024];
            let mut count: u64 = 0;
            let mut mine: Vec<u64> = Vec::with_capacity(1 << 16);

            'work: while !done.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                if sock.write_all(&batch).is_err() {
                    break;
                }
                // One reply per Sync, counted by frame header.
                let mut got = 0usize;
                while got < depth {
                    match sock.read(&mut chunk) {
                        // `break` and not `return`: a connection that dies
                        // mid-run has still completed everything before this
                        // batch, and discarding it biases the result low.
                        Ok(0) | Err(_) => break 'work,
                        Ok(n) => {
                            got += frames.count_ready(&chunk[..n]);
                            if frames.bad {
                                break 'work;
                            }
                        }
                    }
                }
                if mine.len() < MAX_SAMPLES {
                    mine.push(t0.elapsed().as_nanos() as u64);
                }
                count += depth as u64;
            }

            total.fetch_add(count, Ordering::Relaxed);
            if let Ok(mut all) = lat.lock() {
                all.extend_from_slice(&mine);
            }
        }));
    }

    // A peer can accept a connection and never finish the handshake, which
    // would hold this loop forever.
    let setup_deadline = Instant::now() + Duration::from_secs(30);
    while ready_workers.load(Ordering::Relaxed) < conns {
        if Instant::now() > setup_deadline {
            eprintln!(
                "saturate: only {}/{} workers reached the barrier in 30s",
                ready_workers.load(Ordering::Relaxed),
                conns
            );
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    let live = live_workers.load(Ordering::Relaxed);
    if live == 0 {
        eprintln!("saturate: no worker connected");
        std::process::exit(1);
    }

    // The clock starts BEFORE the release, not after. Reversed, a worker can
    // complete batches between `go.store` and `Instant::now()`: those queries
    // land in the numerator while their time is outside the denominator, and
    // q/s is overstated. This ordering errs the other way -- the window
    // strictly contains every worker's execution.
    let start = Instant::now();
    go.store(true, Ordering::Release);

    std::thread::sleep(Duration::from_secs(secs));
    done.store(true, Ordering::Relaxed);
    for t in threads {
        let _ = t.join();
    }
    let elapsed = start.elapsed().as_secs_f64();

    let q = total.load(Ordering::Relaxed);
    let mut all = lat.lock().map(|g| g.clone()).unwrap_or_default();
    all.sort_unstable();

    let pct = |p: f64| -> f64 {
        if all.is_empty() {
            return 0.0;
        }
        let i = ((all.len() - 1) as f64 * p).round() as usize;
        all[i] as f64 / 1e6
    };

    let unit = if depth == 1 { "query" } else { "batch" };
    println!(
        "{} conns, depth {}, {:.1}s: {} queries, {:.0} q/s | {} ms p50 {:.3} p90 {:.3} p99 {:.3} p999 {:.3} max {:.3} (n={})",
        live,
        depth,
        elapsed,
        q,
        q as f64 / elapsed,
        unit,
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        pct(1.0),
        all.len(),
    );
}
