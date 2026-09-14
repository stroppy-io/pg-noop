//! A load generator that can actually saturate pg-noop.
//!
//! # Why this exists
//!
//! Measured on lab2x1 2026-09-14: driving pg-noop with four stroppy processes
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
use std::sync::Arc;
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
    let mut threads = Vec::new();

    for _ in 0..conns {
        let (batch, done, total) = (batch.clone(), done.clone(), total.clone());
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

            while !done.load(Ordering::Relaxed) {
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
                count += depth as u64;
            }

            total.fetch_add(count, Ordering::Relaxed);
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
    println!(
        "{} conns, depth {}, {:.1}s: {} queries, {:.0} q/s",
        conns,
        depth,
        elapsed,
        q,
        q as f64 / elapsed
    );
}
