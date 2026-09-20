//! A shard's event loop on io_uring, with no async runtime at all.
//!
//! # Why this can exist
//!
//! Because the protocol is sans-io. `wire::Conn` is `(&[u8] in, &mut Vec<u8>
//! out)`, so the thing driving it does not have to be a future, a reactor or a
//! runtime -- it can be a loop that submits reads, runs the codec on what comes
//! back, and submits writes. That is the Seastar shape: one thread, one queue,
//! no scheduler.
//!
//! # What it is trying to remove
//!
//! epoll costs a syscall to learn a socket is ready and then another to read it.
//! Measured under the tokio shard: 1.00 epoll_wait, 1.01 recvfrom and
//! 1.00 sendto per query. io_uring collapses the readiness step -- the kernel
//! performs the read and hands back the bytes -- and lets many completions be
//! collected per enter. The submission and completion queues are shared memory,
//! so a batch of ready connections costs ONE `io_uring_enter`, not one syscall
//! each.
//!
//! # Safety
//!
//! Every operation in flight points at a buffer owned by `Slab`. The buffers are
//! `Vec<u8>` INSIDE the slab entries, so their heap allocations are stable even
//! when the slab's own Vec reallocates -- moving a `Vec<u8>` moves its header,
//! never its bytes. An entry is only freed after its last completion is seen,
//! which is what `in_flight` counts.

use std::io;
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};

use io_uring::{opcode, squeue, types, IoUring};

use crate::handler::CatalogView;
use crate::wire::Conn;

const RECV_BUF: usize = 32 * 1024;

/// Operation kind, packed into the low bits of `user_data`.
const OP_ACCEPT: u64 = 0;
const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;

/// How long a paused accept stays paused after `EMFILE`/`ENFILE`/`ENOMEM`/
/// `ENOBUFS`. Long enough for a closed connection to release its fd, short
/// enough that a transient shortage costs a fraction of a second.
const ACCEPT_BACKOFF: std::time::Duration = crate::config::ACCEPT_BACKOFF;

fn tag(idx: usize, op: u64) -> u64 {
    ((idx as u64) << 2) | op
}

fn untag(data: u64) -> (usize, u64) {
    ((data >> 2) as usize, data & 0b11)
}

struct Slot {
    fd: RawFd,
    conn: Conn,
    /// Bytes read but not yet forming a complete message.
    pending: Vec<u8>,
    recv: Vec<u8>,
    send: Vec<u8>,
    /// How much of `send` the kernel has taken.
    sent: usize,
    in_flight: u32,
    closing: bool,
    /// When this connection stops being allowed to sit in startup. `None` once
    /// startup has completed or a timer has already retired it.
    startup_deadline: Option<std::time::Instant>,
}

impl Slot {
    fn new(fd: RawFd, startup_deadline: Option<std::time::Instant>) -> Self {
        Slot {
            fd,
            conn: Conn::new(),
            pending: Vec::with_capacity(4096),
            recv: vec![0u8; RECV_BUF],
            send: Vec::with_capacity(8 * 1024),
            sent: 0,
            in_flight: 0,
            closing: false,
            startup_deadline,
        }
    }
}

/// Can this process create an io_uring ring at all?
///
/// Asked once, before any shard starts, because the answer is a property of the
/// process's environment and not of a shard: the ring is created by the
/// `io_uring_setup` syscall, and under Docker's default seccomp profile that
/// syscall returns `EPERM`. Without this probe each shard discovers the same
/// fact independently, fails, and the process reports `no shard served` -- a
/// container denies the ring and the server has no other way to move bytes.
///
/// The probe builds a ring and drops it. It is the smallest thing that can
/// fail for the same reason the real setup would, and it does not carry the
/// per-shard tuning (`SINGLE_ISSUER`, `DEFER_TASKRUN`, SQPOLL): those have
/// their own fallbacks inside `run`, and a ring that cannot be created is the
/// only failure that is not worth retrying.
pub fn probe() -> io::Result<()> {
    IoUring::new(8).map(|ring| drop(ring))
}

/// Run one shard's loop until the listener fails. Never returns in normal use.
pub fn run(listener: TcpListener, catalog: CatalogView, entries: u32) -> io::Result<()> {
    // SQPOLL: a kernel thread drains the submission queue, so a submit becomes a
    // memory write and `io_uring_enter` leaves the hot path. Unprivileged since
    // Linux 5.11. It costs a kernel thread per ring that spins for `idle` ms
    // before sleeping -- which is why it is opt-in: 32 shards means 32 spinning
    // threads, and on a box that is also serving a model that is rent, not free.
    let sqpoll = std::env::var("PGNOOP_SQPOLL")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());

    // SINGLE_ISSUER + DEFER_TASKRUN, the pair built for exactly this shape.
    //
    // SINGLE_ISSUER (6.0) promises the kernel that one task submits to this
    // ring, which is true by construction here -- a shard IS a thread -- and
    // lets it drop synchronisation it would otherwise need.
    //
    // DEFER_TASKRUN (6.1) stops the kernel running completion work at arbitrary
    // interrupt points and defers it to the moment this thread asks for
    // completions. For a thread-per-core loop that is strictly better: the work
    // happens where it was going to be consumed anyway, instead of interrupting
    // the codec mid-parse. It REQUIRES SINGLE_ISSUER.
    //
    // Unlike SQPOLL these cost nothing: no kernel poller thread per ring.
    // Measured: SQPOLL at 32 shards was 174x SLOWER (15,310 q/s vs
    // 2,659,244) because 32 spinning pollers starve 32 workers.
    let defer = std::env::var("PGNOOP_NO_DEFER").is_err();

    let mut ring = match (sqpoll, defer) {
        (Some(idle_ms), _) => IoUring::builder()
            .setup_sqpoll(idle_ms)
            .build(entries)
            .or_else(|e| {
                eprintln!("pgnoop: SQPOLL unavailable ({e}); falling back to plain io_uring");
                IoUring::new(entries)
            })?,
        (None, true) => IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(entries)
            .or_else(|e| {
                eprintln!(
                    "pgnoop: SINGLE_ISSUER/DEFER_TASKRUN unavailable ({e}); plain io_uring"
                );
                IoUring::new(entries)
            })?,
        (None, false) => IoUring::new(entries)?,
    };
    let listen_fd = listener.as_raw_fd();

    // Slot 0 is reserved for the accept operation so an index is never ambiguous.
    let mut slots: Vec<Option<Slot>> = vec![None];
    let mut free: Vec<usize> = Vec::new();

    // SAFETY: all-zero is a valid sockaddr_storage, and only the kernel writes
    // it. Constructed this way because the padding field names are not stable
    // across libc versions.
    let mut accept_addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut accept_len: libc::socklen_t = std::mem::size_of::<libc::sockaddr_storage>() as _;

    let mut push_accept = |ring: &mut IoUring| -> io::Result<()> {
        let e = opcode::Accept::new(
            types::Fd(listen_fd),
            &mut accept_addr as *mut _ as *mut libc::sockaddr,
            &mut accept_len,
        )
        .build()
        .user_data(tag(0, OP_ACCEPT));
        // SAFETY: the address scratch outlives the loop and only the kernel
        // writes it.
        unsafe { push_or_submit(ring, &e) }
    };

    // When accept may be re-armed again. `None` means now. Set on the errnos
    // that do not clear by retrying, cleared when the wait has expired.
    let mut accept_paused_until: Option<std::time::Instant> = None;

    // The earliest startup deadline among the connections that have not
    // completed one, or `None` when every connection on this shard has. Kept as
    // a minimum rather than recomputed each pass: with either deadline pending
    // the loop does no more than compare an Option, and the scan that refreshes
    // this runs only when a deadline actually comes due.
    let startup_timeout = crate::config::startup_timeout();
    let mut earliest_startup: Option<std::time::Instant> = None;

    push_accept(&mut ring)?;
    ring.submit()?;

    loop {
        // **Wait for a completion, or for the earliest deadline.**
        //
        // Two obligations of this shard are invisible to a completion: a paused
        // accept, and a connection that has not completed startup. Both are
        // reached only by the code below, and `submit_and_wait` never returns to
        // it -- so a 100 ms accept backoff became a shard that never accepted
        // again, and an idle client held its fd for as long as it liked. Bounding
        // the wait is the whole mechanism: when a deadline passes, `io_uring_enter`
        // returns ETIME and this loop keeps running.
        //
        // With neither deadline pending -- the steady state under load -- this is
        // the plain blocking wait it always was, and nothing is added to the hot
        // path but the comparison of two `None`s.
        let deadline = match (accept_paused_until, earliest_startup) {
            (None, None) => None,
            (a, b) => Some(a.into_iter().chain(b).min().expect("one is Some")),
        };

        let expired = match deadline {
            None => {
                ring.submit_and_wait(1)?;

                false
            }
            Some(at) => {
                let wait = at.saturating_duration_since(std::time::Instant::now());
                let ts = types::Timespec::new()
                    .sec(wait.as_secs())
                    .nsec(wait.subsec_nanos());

                // ETIME is the deadline arriving, which is the point; anything
                // else is the ring failing and belongs to the caller.
                match ring
                    .submitter()
                    .submit_with_args(1, &types::SubmitArgs::new().timespec(&ts))
                {
                    Ok(_) => false,
                    Err(e) if e.raw_os_error() == Some(libc::ETIME) => true,
                    Err(e) => return Err(e),
                }
            }
        };

        let mut completions: Vec<(u64, i32)> = Vec::new();
        {
            let mut cq = ring.completion();
            cq.sync();
            for cqe in &mut cq {
                completions.push((cqe.user_data(), cqe.result()));
            }
        }

        for (data, res) in completions {
            let (idx, op) = untag(data);

            match op {
                OP_ACCEPT => {
                    // EMFILE/ENFILE do not clear by retrying immediately;
                    // re-arming at once turns a fd exhaustion into a spin that
                    // burns the shard's core and never recovers.
                    if res < 0 {
                        let err = -res;
                        if err == libc::EMFILE
                            || err == libc::ENFILE
                            || err == libc::ENOMEM
                            || err == libc::ENOBUFS
                        {
                            // **Back off the ACCEPT, not the shard.**
                            //
                            // This used to `thread::sleep(100ms)` right here, in
                            // the middle of draining completions. Every receive
                            // and send that had ALREADY completed waited those
                            // 100 ms with it, and a run of accept failures --
                            // which is what fd exhaustion is -- stopped the
                            // shard doing the work it still had. The connections
                            // already open are exactly the ones that should keep
                            // running while new ones cannot be taken.
                            //
                            // So: record a deadline, skip re-arming accept until
                            // it passes, and carry on with this batch. The wait at
                            // the top of the loop is what makes the deadline mean
                            // anything -- read only here, it could never be
                            // reached again, because no accept is in flight to
                            // complete.
                            accept_paused_until =
                                Some(std::time::Instant::now() + ACCEPT_BACKOFF);
                            eprintln!(
                                "pgnoop: accept failed with errno {err}; \
                                 not re-arming accept for {}ms (other work continues)",
                                ACCEPT_BACKOFF.as_millis()
                            );
                        } else if err == libc::EBADF || err == libc::EINVAL {
                            return Err(io::Error::from_raw_os_error(err));
                        }
                    }

                    if res >= 0 {
                        let fd = res as RawFd;
                        // Nagle off: replies are tens of bytes and latency is
                        // what a benchmark reads.
                        unsafe {
                            let one: libc::c_int = 1;
                            libc::setsockopt(
                                fd,
                                libc::IPPROTO_TCP,
                                libc::TCP_NODELAY,
                                &one as *const _ as *const libc::c_void,
                                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                            );
                        }

                        let admitted = startup_timeout
                            .map(|t| std::time::Instant::now() + t);

                        let i = match free.pop() {
                            Some(i) => {
                                slots[i] = Some(Slot::new(fd, admitted));
                                i
                            }
                            None => {
                                slots.push(Some(Slot::new(fd, admitted)));
                                slots.len() - 1
                            }
                        };
                        submit_recv(&mut ring, &mut slots, i)?;

                        if let Some(at) = admitted {
                            earliest_startup =
                                Some(earliest_startup.map_or(at, |e| e.min(at)));
                        }
                    }
                    // Only re-arm when the pause has expired. While it has
                    // not, no accept is in flight and the loop keeps serving
                    // whatever is already connected.
                    match accept_paused_until {
                        Some(t) if std::time::Instant::now() < t => {}
                        _ => {
                            accept_paused_until = None;
                            push_accept(&mut ring)?;
                        }
                    }
                }

                OP_RECV => {
                    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
                        continue;
                    };
                    slot.in_flight -= 1;

                    if res <= 0 {
                        slot.closing = true;
                        reap(&mut slots, &mut free, idx);
                        continue;
                    }

                    let n = res as usize;
                    slot.send.clear();
                    slot.sent = 0;

                    // Destructured, so `recv` and `pending` and `send` are three
                    // DISTINCT borrows. Written as `slot.x` throughout, the borrow
                    // checker sees one borrow of `slot` and refuses -- which is
                    // what pushed the first version into `.to_vec()`, allocating
                    // and copying the whole read just to append it.
                    let Slot {
                        conn,
                        pending,
                        recv,
                        send,
                        ..
                    } = slot;

                    if pending.is_empty() {
                        let used = conn.advance(&recv[..n], send, &catalog);
                        if used < n {
                            pending.extend_from_slice(&recv[used..n]);
                        }
                    } else {
                        pending.extend_from_slice(&recv[..n]);
                        let used = conn.advance(pending, send, &catalog);
                        pending.drain(..used);
                    }

                    if slot.send.is_empty() {
                        // Startup completed: this connection is no longer the
                        // one the startup deadline is about. Clearing the slot's
                        // deadline here is what keeps `earliest_startup` from
                        // waking the shard for a client that has long since
                        // spoken.
                        if !slot.conn.awaiting_startup() {
                            slot.startup_deadline = None;
                        }

                        if slot.conn.is_closed() {
                            slot.closing = true;
                            reap(&mut slots, &mut free, idx);
                        } else {
                            submit_recv(&mut ring, &mut slots, idx)?;
                        }
                    } else {
                        submit_send(&mut ring, &mut slots, idx)?;
                    }
                }

                OP_SEND => {
                    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
                        continue;
                    };
                    slot.in_flight -= 1;

                    if res <= 0 {
                        slot.closing = true;
                        reap(&mut slots, &mut free, idx);
                        continue;
                    }

                    slot.sent += res as usize;

                    if slot.sent < slot.send.len() {
                        // A short write is normal under pressure; finish it.
                        submit_send(&mut ring, &mut slots, idx)?;
                    } else if slot.conn.is_closed() {
                        slot.closing = true;
                        reap(&mut slots, &mut free, idx);
                    } else {
                        submit_recv(&mut ring, &mut slots, idx)?;
                    }
                }

                _ => {}
            }
        }

        // **The deadlines, once the wait has come back.** Only reached when at
        // least one was pending; with none, `expired` is false and this is a
        // branch on a bool.
        if expired {
            // A paused accept whose backoff has passed resumes here. Checked
            // against the clock rather than assumed: the wait can also have been
            // bounded by the *other* deadline and returned early.
            if let Some(t) = accept_paused_until {
                if std::time::Instant::now() >= t {
                    accept_paused_until = None;
                    push_accept(&mut ring)?;
                }
            }

            // Connections still in startup whose deadline has passed.
            //
            // `shutdown`, not `close`: a slot with a receive in flight may not
            // be freed, and shutting the socket down is what makes that receive
            // return 0, so the ordinary path reaps it. Closing the fd here
            // instead would leave the kernel reading into a slot the shard has
            // already forgotten.
            let now = std::time::Instant::now();
            let mut next: Option<std::time::Instant> = None;

            for slot in slots.iter_mut().flatten() {
                let Some(at) = slot.startup_deadline else {
                    continue;
                };

                if at <= now {
                    if slot.conn.awaiting_startup() {
                        eprintln!(
                            "pgnoop: connection did not complete startup within {}ms; closing",
                            startup_timeout.map_or(0, |t| t.as_millis())
                        );
                        // SAFETY: the fd is owned by the slot and is closed only
                        // once its receive completes.
                        unsafe { libc::shutdown(slot.fd, libc::SHUT_RDWR) };
                    }

                    slot.startup_deadline = None;

                    continue;
                }

                next = Some(next.map_or(at, |n| n.min(at)));
            }

            earliest_startup = next;
        }
    }
}

fn submit_recv(ring: &mut IoUring, slots: &mut [Option<Slot>], idx: usize) -> io::Result<()> {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return Ok(());
    };
    let e = opcode::Recv::new(
        types::Fd(slot.fd),
        slot.recv.as_mut_ptr(),
        slot.recv.len() as u32,
    )
    .build()
    .user_data(tag(idx, OP_RECV));

    slot.in_flight += 1;
    // SAFETY: `slot.recv`'s allocation outlives the operation; the slot is not
    // freed while in_flight > 0.
    unsafe { push_or_submit(ring, &e) }
}

/// Push one entry, making room by submitting when the queue is full.
///
/// **The error is returned, not discarded.** All three call sites used to spin
/// `while push().is_err() { let _ = ring.submit(); }`, which is correct only while
/// `submit` can still succeed: a persistent failure -- a closed ring, EBADF on the
/// ring fd, ENOMEM that does not clear -- leaves the shard spinning on a full queue
/// with no way out and no message. A shard that cannot submit is finished, and
/// saying so is the difference between one dead shard and a core at 100% forever.
///
/// # Safety
///
/// The caller must keep every buffer the entry points at alive until its
/// completion is reaped.
unsafe fn push_or_submit(ring: &mut IoUring, e: &squeue::Entry) -> io::Result<()> {
    loop {
        // SAFETY: the caller's contract, forwarded.
        if unsafe { ring.submission().push(e) }.is_ok() {
            return Ok(());
        }
        // Full: make room. If THAT fails there is nothing left to try.
        ring.submit()?;
    }
}

fn submit_send(ring: &mut IoUring, slots: &mut [Option<Slot>], idx: usize) -> io::Result<()> {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return Ok(());
    };
    let e = opcode::Send::new(
        types::Fd(slot.fd),
        unsafe { slot.send.as_ptr().add(slot.sent) },
        (slot.send.len() - slot.sent) as u32,
    )
    .build()
    .user_data(tag(idx, OP_SEND));

    slot.in_flight += 1;
    // SAFETY: as above; `send` is not touched again until this completes.
    unsafe { push_or_submit(ring, &e) }
}

/// Close and free a slot, but only once nothing is still pointing at its
/// buffers. A slot freed with an operation in flight is a use-after-free the
/// kernel performs for you.
fn reap(slots: &mut [Option<Slot>], free: &mut Vec<usize>, idx: usize) {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return;
    };
    if slot.in_flight > 0 {
        return;
    }
    unsafe { libc::close(slot.fd) };
    slots[idx] = None;
    free.push(idx);
}
