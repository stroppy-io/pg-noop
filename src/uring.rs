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

use io_uring::{opcode, types, IoUring};

use crate::handler::CatalogView;
use crate::wire::Conn;

const RECV_BUF: usize = 32 * 1024;

/// Operation kind, packed into the low bits of `user_data`.
const OP_ACCEPT: u64 = 0;
const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;

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
}

impl Slot {
    fn new(fd: RawFd) -> Self {
        Slot {
            fd,
            conn: Conn::new(),
            pending: Vec::with_capacity(4096),
            recv: vec![0u8; RECV_BUF],
            send: Vec::with_capacity(8 * 1024),
            sent: 0,
            in_flight: 0,
            closing: false,
        }
    }
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

    // SAFETY: the address scratch outlives the loop and only the kernel writes it.
    let mut push_accept = |ring: &mut IoUring| unsafe {
        let e = opcode::Accept::new(
            types::Fd(listen_fd),
            &mut accept_addr as *mut _ as *mut libc::sockaddr,
            &mut accept_len,
        )
        .build()
        .user_data(tag(0, OP_ACCEPT));
        while ring.submission().push(&e).is_err() {
            let _ = ring.submit();
        }
    };

    push_accept(&mut ring);
    ring.submit()?;

    loop {
        // One enter, however many completions are ready. This is the property
        // epoll cannot give: a batch of ready connections is one syscall.
        ring.submit_and_wait(1)?;

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
                            eprintln!(
                                "pgnoop: accept failed with errno {err}; backing off 100ms"
                            );
                            std::thread::sleep(std::time::Duration::from_millis(100));
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

                        let i = match free.pop() {
                            Some(i) => {
                                slots[i] = Some(Slot::new(fd));
                                i
                            }
                            None => {
                                slots.push(Some(Slot::new(fd)));
                                slots.len() - 1
                            }
                        };
                        submit_recv(&mut ring, &mut slots, i);
                    }
                    push_accept(&mut ring);
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
                        if slot.conn.is_closed() {
                            slot.closing = true;
                            reap(&mut slots, &mut free, idx);
                        } else {
                            submit_recv(&mut ring, &mut slots, idx);
                        }
                    } else {
                        submit_send(&mut ring, &mut slots, idx);
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
                        submit_send(&mut ring, &mut slots, idx);
                    } else if slot.conn.is_closed() {
                        slot.closing = true;
                        reap(&mut slots, &mut free, idx);
                    } else {
                        submit_recv(&mut ring, &mut slots, idx);
                    }
                }

                _ => {}
            }
        }
    }
}

fn submit_recv(ring: &mut IoUring, slots: &mut [Option<Slot>], idx: usize) {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return;
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
    unsafe {
        while ring.submission().push(&e).is_err() {
            let _ = ring.submit();
        }
    }
}

fn submit_send(ring: &mut IoUring, slots: &mut [Option<Slot>], idx: usize) {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return;
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
    unsafe {
        while ring.submission().push(&e).is_err() {
            let _ = ring.submit();
        }
    }
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
