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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
fn handshake(sock: &mut TcpStream, sql: &str) -> std::io::Result<()> {
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

    // Drain until the second ReadyForQuery (startup's, then Parse's).
    let mut seen = 0;
    let mut chunk = [0u8; 4096];
    while seen < 2 {
        let n = sock.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        seen += chunk[..n].iter().filter(|&&b| b == b'Z').count();
    }

    Ok(())
}

fn main() {
    let addr = arg_str("--addr", "127.0.0.1:5432");
    let conns = arg("--conns", 64);
    let depth = arg("--depth", 32);
    let secs = arg("--secs", 8) as u64;
    let sql = arg_str("--sql", "select 1");

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
        let addr = addr.clone();
        let sql = sql.clone();

        threads.push(std::thread::spawn(move || {
            let Ok(mut sock) = TcpStream::connect(&addr) else {
                return;
            };
            let _ = sock.set_nodelay(true);
            if handshake(&mut sock, &sql).is_err() {
                return;
            }

            let mut chunk = vec![0u8; 64 * 1024];
            let mut count: u64 = 0;
            let mut mine: Vec<u64> = Vec::with_capacity(1 << 16);

            while !done.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                if sock.write_all(&batch).is_err() {
                    break;
                }
                // One reply per Sync; count 'Z' until the batch is answered.
                let mut got = 0usize;
                while got < depth {
                    match sock.read(&mut chunk) {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got += chunk[..n].iter().filter(|&&b| b == b'Z').count(),
                    }
                }
                mine.push(t0.elapsed().as_nanos() as u64);
                count += depth as u64;
            }

            total.fetch_add(count, Ordering::Relaxed);
            if let Ok(mut all) = lat.lock() {
                all.extend_from_slice(&mine);
            }
        }));
    }

    let start = Instant::now();
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
        "{} conns, depth {}, {:.1}s: {} queries, {:.0} q/s | {} ms p50 {:.3} p90 {:.3} p99 {:.3} p999 {:.3} max {:.3}",
        conns,
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
    );
}
