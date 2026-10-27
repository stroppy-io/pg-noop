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
//! # What it asks the kernel for
//!
//! Every ring feature this kernel offers, probed once per process and reported
//! on stderr: the setup flags (one at a time, so a refusal is attributable),
//! the ring's own fd registered so an enter does not look it up, a registered
//! provided-buffer ring for receives, multishot accept and multishot receive.
//! Incremental buffer consumption is probed as well, reported when granted, and
//! not used: see `BufRing::register` for what the kernel does not tell us about
//! a buffer it is still holding. Each fallback says so -- a silently different
//! ring is a measurement nobody can reproduce.
//!
//! # Safety
//!
//! Every operation in flight points at memory this module owns and outlives the
//! ring: a slot's `send`/`pending`/`staged` vectors, or a buffer inside the
//! provided-buffer ring. A slot is only freed once nothing of its own is in
//! flight (`recv_armed`, `sending`), and a provided buffer is only put back into
//! the ring once the kernel has given it up -- which is what `buf_more` and the
//! end of a receive request say.

use std::fmt::Write as _;
use std::io;
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use io_uring::{cqueue, opcode, squeue, types, IoUring};

use crate::handler::CatalogView;
use crate::wire::Conn;

/// One provided buffer's size. The same as the per-connection receive buffer
/// this replaced: a pipelined batch of queries fits in one read.
const RECV_BUF: usize = 32 * 1024;

/// Provided buffers in a shard's ring. Every connection on the shard draws from
/// this one ring instead of owning a receive buffer, so this -- not the number
/// of connections -- is what bounds a shard's receive memory. It is also what
/// bounds the reads one shard can have in flight at once, which is why it is
/// generous.
///
/// The mapping is anonymous and only the buffers that have been received into
/// have been touched, so 256 * 32 KiB is 8 MiB of address space per shard and as
/// little as nothing of its memory until it is used.
#[cfg_attr(test, allow(dead_code))] // test shards use a smaller ring
const PBUF_COUNT: u16 = 256;

/// The buffer group every receive on the shard draws from. One group is enough:
/// the ring is shared by the connections, not owned by them.
const PBUF_GROUP: u16 = 0;

/// Operation kind, packed into the low bits of `user_data`.
const OP_ACCEPT: u64 = 0;
const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;

/// How long a paused accept stays paused after `EMFILE`/`ENFILE`/`ENOMEM`/
/// `ENOBUFS`. Long enough for a closed connection to release its fd, short
/// enough that a transient shortage costs a fraction of a second.
const ACCEPT_BACKOFF: Duration = crate::config::ACCEPT_BACKOFF;

fn tag(idx: usize, op: u64) -> u64 {
    ((idx as u64) << 2) | op
}

fn untag(data: u64) -> (usize, u64) {
    ((data >> 2) as usize, data & 0b11)
}

/// A completion, kept in the shard's own buffer rather than in a `Vec` built
/// per wait pass. `flags` carries what the receive path needs: which provided
/// buffer was used, whether the multishot request survives, and whether the
/// kernel still owns that buffer.
#[derive(Clone, Copy)]
struct Done {
    data: u64,
    res: i32,
    flags: u32,
}

/// The per-connection output and carry-over buffers, recycled across accepts.
///
/// A fresh 4 KiB `pending` and 8 KiB `send` per accepted connection is two
/// allocations -- and, since they were zeroed, two page faults' worth of memory
/// traffic -- on the path a connection-per-query benchmark lives on.
#[derive(Default)]
struct Spare {
    /// A connection's own receive buffer, for the connections that cannot have
    /// one of the shard's. Recycled like the rest: a buffer per connection is
    /// what the shared ring exists to avoid, so the ones that do have to exist
    /// are not allocated twice.
    recv: Vec<u8>,
    pending: Vec<u8>,
    send: Vec<u8>,
    staged: Vec<u8>,
}

struct Slot {
    fd: RawFd,
    conn: Conn,
    /// The connection's own receive buffer, used when the shard's ring has no
    /// buffer to lend it. Allocated on the first receive that needs it, so a
    /// connection the ring can serve never pays for one.
    recv: Vec<u8>,
    /// Bytes read but not yet forming a complete message.
    pending: Vec<u8>,
    /// The reply being written, or the next one to write.
    send: Vec<u8>,
    /// The reply built while `send` was in flight. A multishot receive keeps
    /// delivering while a send is outstanding, and the bytes cannot go into a
    /// buffer the kernel is reading.
    staged: Vec<u8>,
    /// How much of `send` the kernel has taken.
    sent: usize,
    /// A send is in flight.
    sending: bool,
    /// A receive is in flight. At most one, ever: a second one would race the
    /// first for the same connection's bytes.
    recv_armed: bool,
    /// The previous receive found no buffer in the ring, so the next one reads
    /// into this connection's own buffer.
    private: bool,
    /// The slot is on its way out and must not arm anything new.
    closing: bool,
    /// When this connection stops being allowed to sit in startup. `None` once
    /// startup has completed or a timer has already retired it.
    startup_deadline: Option<Instant>,
}

impl Slot {
    fn new(fd: RawFd, startup_deadline: Option<Instant>, spare: Spare) -> Self {
        Slot {
            fd,
            conn: Conn::new(),
            recv: spare.recv,
            pending: spare.pending,
            send: spare.send,
            staged: spare.staged,
            sent: 0,
            sending: false,
            recv_armed: false,
            private: false,
            closing: false,
            startup_deadline,
        }
    }
}

/// What a shard is allowed to ask the kernel for, and what it was refused.
///
/// Learned once per process by trying each flag on top of what has already been
/// granted, and reported so a measurement can say which ring produced it.
#[derive(Clone, Copy, Default, Debug)]
struct Setup {
    sqpoll: Option<u32>,
    single_issuer: bool,
    defer_taskrun: bool,
    coop_taskrun: bool,
    taskrun_flag: bool,
    no_sqarray: bool,
    clamp: bool,
    submit_all: bool,
    /// `IORING_SETUP_CQSIZE`. The size itself is derived from the ring's own
    /// submission entries (`build`), so every shard's completion queue is four
    /// times the connection capacity it was built for rather than a constant
    /// that would be wrong for a smaller ring.
    cqsize: bool,
}

/// One setup flag, named the way `include/uapi/linux/io_uring.h` names it so
/// the report reads like `io_uring_setup(2)`.
#[derive(Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // `TaskrunFlag` is the flag's kernel name
enum Flag {
    SingleIssuer,
    DeferTaskrun,
    CoopTaskrun,
    TaskrunFlag,
    NoSqarray,
    Clamp,
    SubmitAll,
    CqSize,
}

impl Flag {
    const ORDER: [Flag; 8] = [
        Flag::SingleIssuer,
        Flag::DeferTaskrun,
        Flag::CoopTaskrun,
        Flag::TaskrunFlag,
        Flag::NoSqarray,
        Flag::Clamp,
        Flag::SubmitAll,
        Flag::CqSize,
    ];

    fn name(self) -> &'static str {
        match self {
            Flag::SingleIssuer => "SINGLE_ISSUER",
            Flag::DeferTaskrun => "DEFER_TASKRUN",
            Flag::CoopTaskrun => "COOP_TASKRUN",
            Flag::TaskrunFlag => "TASKRUN_FLAG",
            Flag::NoSqarray => "NO_SQARRAY",
            Flag::Clamp => "CLAMP",
            Flag::SubmitAll => "SUBMIT_ALL",
            Flag::CqSize => "CQSIZE",
        }
    }

    fn apply(self, s: &mut Setup, cq_entries: u32) {
        match self {
            Flag::SingleIssuer => s.single_issuer = true,
            Flag::DeferTaskrun => s.defer_taskrun = true,
            Flag::CoopTaskrun => s.coop_taskrun = true,
            Flag::TaskrunFlag => s.taskrun_flag = true,
            Flag::NoSqarray => s.no_sqarray = true,
            Flag::Clamp => s.clamp = true,
            Flag::SubmitAll => s.submit_all = true,
            Flag::CqSize => s.cqsize = cq_entries > 0,
        }
    }
}

/// The ring this process can build, and the honest account of how.
#[derive(Clone)]
struct Plan {
    setup: Setup,
    granted: Vec<&'static str>,
    /// Every flag the kernel would not take, with the errno it answered. An
    /// EINVAL here is "dropped after EINVAL": the flag is not *refused by
    /// name*, because `io_uring_setup` does not say which flag it disliked.
    dropped: Vec<(&'static str, i32)>,
    /// The ring's own fd is registered, so an enter does not look it up.
    ring_fd: bool,
    /// The provided-buffer ring registered, and whether it consumes buffers
    /// incrementally.
    pbuf: Option<bool>,
    /// Why the provided-buffer ring is absent, when it is.
    pbuf_errno: Option<i32>,
    /// Set when this process cannot create a ring at all, with the errno that
    /// says why. `probe` turns it back into the error `main` falls back on.
    absent: Option<i32>,
}

impl Plan {
    fn describe(&self) -> String {
        let mut s = String::from("granted: ");
        if self.granted.is_empty() {
            s.push_str("(plain ring)");
        } else {
            s.push_str(&self.granted.join(" "));
        }
        if self.ring_fd {
            s.push_str(" RING_FD");
        }
        match self.pbuf {
            Some(true) => s.push_str(
                " PBUF_RING (PBUF_RING_INC granted, unused: a buffer's ownership \
                 is not reported while a receive holds it)",
            ),
            Some(false) => s.push_str(" PBUF_RING"),
            None => {}
        }
        if !self.dropped.is_empty() {
            s.push_str("; dropped: ");
            for (i, (name, errno)) in self.dropped.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                let _ = write!(s, "{name} after {}", errno_name(*errno));
            }
        }
        if let Some(errno) = self.pbuf_errno {
            let _ = write!(
                s,
                "; provided buffers absent after {} (per-connection receive buffer)",
                errno_name(errno)
            );
        }
        s
    }
}

/// The errno as the name a reader recognises, for the report only.
fn errno_name(errno: i32) -> String {
    for (n, name) in [
        (libc::EINVAL, "EINVAL"),
        (libc::ENOSYS, "ENOSYS"),
        (libc::EPERM, "EPERM"),
        (libc::EOPNOTSUPP, "EOPNOTSUPP"),
        (libc::EACCES, "EACCES"),
        (libc::ENOMEM, "ENOMEM"),
        (libc::EBUSY, "EBUSY"),
        (libc::EEXIST, "EEXIST"),
    ] {
        if errno == n {
            return name.to_string();
        }
    }
    format!("errno {}", errno)
}

/// The process's plan, computed on first use.
fn plan() -> &'static Plan {
    static PLAN: OnceLock<Plan> = OnceLock::new();
    PLAN.get_or_init(compute_plan)
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
/// The probe is also where the ring is asked what it will grant: the setup
/// flags, the provided-buffer ring, the registered ring fd. It builds rings and
/// drops them, and the answer is cached for every shard, because a shard's ring
/// differs only in being kept.
pub fn probe() -> io::Result<()> {
    let p = plan();
    if let Some(errno) = p.absent {
        // No ring at all: the answer `main` needs to serve with epoll instead.
        return Err(io::Error::from_raw_os_error(errno));
    }
    eprintln!("pgnoop: io_uring {}", p.describe());
    Ok(())
}

/// What this process's rings can do, for the banner and the log.
pub fn report() -> String {
    plan().describe()
}

/// Build a ring with exactly the flags in `s`.
fn build(s: &Setup, entries: u32) -> io::Result<IoUring> {
    let mut b = IoUring::builder();
    if let Some(idle) = s.sqpoll {
        b.setup_sqpoll(idle);
    }
    if s.single_issuer {
        b.setup_single_issuer();
    }
    if s.defer_taskrun {
        b.setup_defer_taskrun();
    }
    if s.coop_taskrun {
        b.setup_coop_taskrun();
    }
    if s.taskrun_flag {
        b.setup_taskrun_flag();
    }
    if s.no_sqarray {
        b.setup_no_sqarray();
    }
    if s.clamp {
        b.setup_clamp();
    }
    if s.submit_all {
        b.setup_submit_all();
    }
    if s.cqsize {
        b.setup_cqsize((entries.saturating_mul(4)).next_power_of_two());
    }
    b.build(entries)
}

/// Add one flag at a time, keeping what the kernel accepts.
///
/// One at a time, because `io_uring_setup` answers EINVAL for the *set*: a ring
/// that fails with six flags on it says nothing about which of the six this
/// kernel does not know. The build is a parameter so the ladder's decisions can
/// be tested without a kernel that refuses anything.
fn climb<F>(defer: bool, cq_entries: u32, mut build: F) -> io::Result<Plan>
where
    F: FnMut(&Setup) -> io::Result<()>,
{
    // The plain ring is the one failure that is fatal: no ring, no shard.
    let mut setup = Setup::default();
    build(&setup)?;

    let mut granted = Vec::new();
    let mut dropped = Vec::new();
    for flag in Flag::ORDER {
        // DEFER_TASKRUN needs SINGLE_ISSUER, which the ladder has already tried
        // by the time it gets here; PGNOOP_NO_DEFER is the operator asking for
        // the ring without it.
        if flag == Flag::DeferTaskrun && !defer {
            continue;
        }
        // TASKRUN_FLAG is the task-work notification bit, and the kernel
        // rejects it unless task work is actually deferred to this thread --
        // which only COOP_TASKRUN or DEFER_TASKRUN do.
        if flag == Flag::TaskrunFlag && !(setup.coop_taskrun || setup.defer_taskrun) {
            continue;
        }

        let before = setup;
        flag.apply(&mut setup, cq_entries);
        match build(&setup) {
            Ok(()) => granted.push(flag.name()),
            Err(e) => {
                setup = before;
                match e.raw_os_error() {
                    Some(errno) => dropped.push((flag.name(), errno)),
                    // Not an errno: nothing the ladder can attribute.
                    None => return Err(e),
                }
            }
        }
    }

    Ok(Plan {
        setup,
        granted,
        dropped,
        ring_fd: false,
        pbuf: None,
        pbuf_errno: None,
        absent: None,
    })
}

/// Work out what this process's rings can do. Never fails: a process with no
/// ring at all records why, so `probe` can report it and `main` can serve with
/// epoll.
fn compute_plan() -> Plan {
    let defer = std::env::var("PGNOOP_NO_DEFER").is_err();
    let sqpoll = std::env::var("PGNOOP_SQPOLL")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());

    // The completion queue is sized for four times the connection capacity --
    // `entries` submission slots is the capacity a shard is built for -- so a
    // burst of completions is drained rather than pushed into the kernel's
    // overflow list, which is the state that makes an enter answer EBUSY. The
    // size is derived per ring in `build`; this only says the flag is wanted.
    let mut plan = match climb(defer, 1, |s| build(s, ENTRIES).map(drop)) {
        Ok(p) => p,
        Err(e) => {
            return Plan {
                setup: Setup::default(),
                granted: Vec::new(),
                dropped: Vec::new(),
                ring_fd: false,
                pbuf: None,
                pbuf_errno: None,
                absent: Some(e.raw_os_error().unwrap_or(libc::ENOSYS)),
            }
        }
    };

    // SQPOLL: a kernel thread drains the submission queue, so a submit becomes a
    // memory write and `io_uring_enter` leaves the hot path. Unprivileged since
    // Linux 5.11. It costs a kernel thread per ring that spins for `idle` ms
    // before sleeping -- which is why it is opt-in: 32 shards means 32 spinning
    // threads, and on a box that is also serving a model that is rent, not free.
    // Measured: SQPOLL at 32 shards was 174x SLOWER (15,310 q/s vs 2,659,244)
    // because 32 spinning pollers starve 32 workers.
    if let Some(idle) = sqpoll {
        let mut sq = Setup {
            sqpoll: Some(idle),
            ..Setup::default()
        };
        match build(&sq, ENTRIES) {
            Ok(ring) => {
                drop(ring);
                // SQPOLL is a kernel thread doing the submission; the
                // single-issuer and taskrun flags are about the submitting
                // *task*, so they do not apply to it.
                sq.cqsize = plan.setup.cqsize;
                sq.clamp = plan.setup.clamp;
                sq.no_sqarray = plan.setup.no_sqarray;
                sq.submit_all = plan.setup.submit_all;
                plan.setup = sq;
                plan.granted = vec!["SQPOLL"];
            }
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(0);
                plan.dropped.push(("SQPOLL", errno));
                eprintln!(
                    "pgnoop: SQPOLL unavailable ({}); falling back to the plain ring",
                    errno_name(errno)
                );
            }
        }
    }

    // The features that are not setup flags are probed on a ring of their own,
    // because they are registration calls rather than `io_uring_setup` bits.
    let ring = match build(&plan.setup, ENTRIES) {
        Ok(r) => r,
        Err(e) => {
            // The ladder said this set was buildable; if it is not now, say so
            // rather than quietly serving on a different ring.
            eprintln!(
                "pgnoop: io_uring refused the probed setup ({}); using a plain ring",
                errno_name(e.raw_os_error().unwrap_or(0))
            );
            plan.setup = Setup::default();
            plan.granted.clear();
            match build(&Setup::default(), ENTRIES) {
                Ok(r) => r,
                Err(e) => {
                    plan.absent = Some(e.raw_os_error().unwrap_or(libc::ENOSYS));
                    return plan;
                }
            }
        }
    };

    {
        let mut submit = ring.submitter();
        plan.ring_fd = match submit.register_ring_fd() {
            Ok(()) => true,
            Err(e) => {
                // Linux 5.18. Without it every enter passes the raw fd, which
                // the kernel then has to look up in the caller's file table.
                let errno = e.raw_os_error().unwrap_or(0);
                plan.dropped.push(("RING_FD", errno));
                false
            }
        };
        match BufRing::probe(&submit) {
            Ok(incremental) => plan.pbuf = Some(incremental),
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(0);
                plan.pbuf_errno = Some(errno);
                eprintln!(
                    "pgnoop: provided buffers unavailable ({}); each connection \
                     keeps its own receive buffer",
                    errno_name(errno)
                );
            }
        }
        let _ = submit.unregister_ring_fd();
    }

    plan
}

/// Whether this process can still afford `IORING_SETUP_CQSIZE`. Set when a
/// shard's ring ran out of memory with it, so the shards that start later do not
/// each discover the same thing.
static CQSIZE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// The submission slots a shard's ring is built for. Sizing the completion
/// queue from it is what `CQSIZE` below is about.
const ENTRIES: u32 = 4096;

/// A shard's ring, with the submission and completion queues taken apart once.
///
/// The three quarters of the ring are held for the life of the loop rather than
/// re-borrowed per call: the ring fd is registered on the `Submitter`, and a
/// `Submitter` rebuilt per call would not know that.
struct Ring<'a> {
    submit: io_uring::Submitter<'a>,
    sq: squeue::SubmissionQueue<'a>,
    cq: cqueue::CompletionQueue<'a>,
    /// This pass's completions, reused. A `Vec` of completions allocated per
    /// wait pass is one allocation per batch of work the shard does.
    done: Vec<Done>,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
}

impl Ring<'_> {
    /// Push one entry, making room by submitting when the queue is full.
    ///
    /// **A transient failure of that submit is not the shard's death.** A
    /// signal delivered to this thread reaches `io_uring_enter` as EINTR, and a
    /// completion ring full of entries the kernel could not flush reaches it as
    /// EBUSY; the first is retried, the second is answered by draining the
    /// completions the kernel is holding -- which is what the retry then finds.
    ///
    /// # Safety
    ///
    /// The caller must keep every buffer the entry points at alive until its
    /// completion is reaped.
    unsafe fn push(&mut self, e: &squeue::Entry) -> io::Result<()> {
        loop {
            // SAFETY: the caller's contract, forwarded.
            if unsafe { self.sq.push(e) }.is_ok() {
                self.sq.sync();
                return Ok(());
            }
            // Full: make room. If THAT fails there is nothing left to try.
            self.submit()?;
        }
    }

    /// Submit what is queued, retrying what the ring says is transient.
    fn submit(&mut self) -> io::Result<()> {
        self.sq.sync();
        // Disjoint borrows: the submit needs the ring, the drain needs the
        // completion queue and the buffer the completions go into.
        let this = &mut *self;
        retry_with(
            || this.submit.submit().map(drop),
            || drain_completions(&mut this.cq, &mut this.done),
        )
    }

    /// Move everything the kernel has completed into `done`.
    fn drain(&mut self) {
        drain_completions(&mut self.cq, &mut self.done);
    }

    /// Wait for one completion, or until `deadline`.
    fn wait(&mut self, deadline: Option<Instant>) -> io::Result<()> {
        self.sq.sync();
        wait_for_work(&self.submit, deadline)
    }
}

/// Run one shard's loop until the listener fails. Never returns in normal use.
pub fn run(listener: TcpListener, catalog: CatalogView, entries: u32) -> io::Result<()> {
    run_until(
        listener,
        catalog,
        Tuning {
            entries,
            startup_timeout: crate::config::startup_timeout(),
            ..Tuning::default()
        },
    )
}

/// What a shard is run with. A struct because two of these are knobs a test
/// turns (a small buffer ring, a short startup timeout) and one is a knob the
/// process turns (the ring size); passing five arguments would not say which is
/// which.
#[derive(Clone)]
struct Tuning {
    entries: u32,
    /// `None` disables the startup timeout -- right for a server whose operator
    /// asked for that, and for a test that is not about it.
    startup_timeout: Option<Duration>,
    pbuf_count: u16,
    /// A test's way of asking the shard to stop, so the test can give its ring
    /// back: a test binary runs a dozen shards, and a ring is kernel memory with
    /// a process-wide budget.
    #[cfg(test)]
    stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            // A test process runs a dozen shards at once; the production ring is
            // 4096 submission entries and a completion queue four times that,
            // which is a third of a megabyte of kernel memory per shard.
            #[cfg(not(test))]
            entries: ENTRIES,
            #[cfg(test)]
            entries: 256,
            startup_timeout: None,
            // A dozen test shards in one process; the production ring is 256
            // buffers, 8 MiB of mapping each.
            #[cfg(not(test))]
            pbuf_count: PBUF_COUNT,
            #[cfg(test)]
            pbuf_count: 16,
            #[cfg(test)]
            stop: None,
            #[cfg(test)]
            stats: None,
        }
    }
}

/// Counters a test reads to see what the ring did. Compiled only into test
/// builds: a shard in production pays nothing for them.
#[cfg(test)]
#[derive(Default)]
struct Stats {
    /// Buffers currently in the ring, and buffers the kernel is holding.
    free: std::sync::atomic::AtomicU16,
    held: std::sync::atomic::AtomicU16,
    /// Receives that found no buffer, and so read into the connection's own.
    enobufs: std::sync::atomic::AtomicUsize,
    private_arms: std::sync::atomic::AtomicUsize,
    /// Receives actually armed, and arm attempts skipped because the connection
    /// already had one in flight. The second is the guard doing its job: two
    /// receives on one socket would race for the same bytes.
    arms: std::sync::atomic::AtomicUsize,
    elided: std::sync::atomic::AtomicUsize,
}

/// `run`, with the shard's knobs supplied rather than read from the process.
///
/// The timeout in force is a process-wide `OnceLock` (`config::startup_timeout`),
/// which is right for a server and unusable for a test: a test that had to set
/// an environment variable would be sharing one decision with every other test
/// in the binary. The loop itself takes it as an argument, and `run` is what
/// reads the configured one.
fn run_until(listener: TcpListener, catalog: CatalogView, tuning: Tuning) -> io::Result<()> {
    let plan = plan();
    let setup = Setup {
        cqsize: plan.setup.cqsize && CQSIZE.load(Ordering::Relaxed),
        ..plan.setup
    };
    let mut ioring = match build(&setup, tuning.entries) {
        Ok(r) => r,
        // **A ring's memory is not free.** `io_uring_setup` charges it to the
        // process, and a large completion queue is the biggest part of it: on a
        // box with the default 8 MiB of mlockable memory, a shard per core runs
        // out. The queue is what is dropped, once, and the shard says so -- the
        // alternative is a shard that does not start.
        Err(e) if setup.cqsize && e.raw_os_error() == Some(libc::ENOMEM) => {
            CQSIZE.store(false, Ordering::Relaxed);
            let smaller = Setup {
                cqsize: false,
                ..setup
            };
            eprintln!(
                "pgnoop: io_uring ring ran out of memory with CQSIZE ({}); \
                 shards keep the kernel's own completion queue",
                errno_name(libc::ENOMEM)
            );
            match build(&smaller, tuning.entries) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "pgnoop: io_uring refused the probed setup ({}); serving on a plain ring",
                        errno_name(e.raw_os_error().unwrap_or(0))
                    );
                    build(&Setup::default(), tuning.entries)?
                }
            }
        }
        Err(e) => {
            eprintln!(
                "pgnoop: io_uring refused the probed setup ({}); serving on a plain ring",
                errno_name(e.raw_os_error().unwrap_or(0))
            );
            build(&Setup::default(), tuning.entries)?
        }
    };

    let (mut submit, sq, cq) = ioring.split();

    // Registration before the loop, on the very `Submitter` that will enter:
    // the registered index is remembered by the `Submitter`, so registering on
    // any other one would register nothing at all.
    if plan.ring_fd {
        if let Err(e) = submit.register_ring_fd() {
            eprintln!(
                "pgnoop: ring fd registration failed ({}); enters keep passing the raw fd",
                errno_name(e.raw_os_error().unwrap_or(0))
            );
        }
    }

    let mut bufs = match plan.pbuf {
        Some(_granted_incremental) => Some(BufRing::register(
            &submit,
            tuning.pbuf_count,
            #[cfg(test)]
            tuning.stats.clone(),
        )?),
        None => None,
    };

    let mut ring = Ring {
        submit,
        sq,
        cq,
        done: Vec::with_capacity(1024),
        #[cfg(test)]
        stats: tuning.stats.clone(),
    };

    let listen_fd = listener.as_raw_fd();

    // Slot 0 is reserved for the accept operation so an index is never ambiguous.
    let mut slots: Vec<Option<Slot>> = vec![None];
    let mut free_slots: Vec<usize> = Vec::new();
    let mut spare: Vec<Spare> = Vec::new();

    // Accept: one multishot request, re-armed only when the kernel says it is
    // no longer holding it (`F_MORE` clear), and not at all while paused.
    let mut accept_armed = false;
    let mut accept_paused_until: Option<Instant> = None;

    // **The connections in startup, as a list rather than a scan.**
    //
    // The deadline pass used to walk every slot on the shard, which is a scan
    // of the whole connection table for the handful of connections that have
    // not finished their handshake. Entries can go stale (a slot is reused);
    // the pass drops those it finds, so the list cannot outgrow the connections
    // that have ever been admitted into it between two expiries.
    let mut startup: Vec<usize> = Vec::new();
    let mut earliest_startup: Option<Instant> = None;

    // Slow connections, and the ring itself, come back to in a moment.

    // Multishot receive is discovered from the kernel's answer, not from a
    // version number: a kernel that does not know `IORING_RECV_MULTISHOT`
    // answers the first receive with EINVAL, and every receive after that is
    // a one-shot into a provided buffer instead.
    let mut multishot = bufs.is_some();
    let mut scratch: Vec<usize> = Vec::new();

    arm_accept(&mut ring, listen_fd, &mut accept_armed)?;

    loop {
        // A test asks the shard to stop so that its ring goes back to the
        // process; nothing in the server sets this.
        #[cfg(test)]
        if let Some(stop) = &tuning.stop {
            if stop.load(Ordering::Relaxed) {
                return Ok(());
            }
        }

        // **Wait for a completion, or for the earliest deadline.**
        //
        // Two obligations of this shard are invisible to a completion: a paused
        // accept, and a connection that has not completed startup. Bounding the
        // wait is what keeps the loop coming back to them.
        //
        // With neither pending -- the steady state under load -- this is the
        // plain blocking wait it always was, and nothing is added to the hot
        // path but the comparison of two `None`s.
        #[cfg(test)]
        let tick = tuning
            .stop
            .as_ref()
            .map(|_| Instant::now() + Duration::from_millis(20));
        #[cfg(not(test))]
        let tick: Option<Instant> = None;

        let deadline = match (accept_paused_until, earliest_startup, tick) {
            (None, None, None) => None,
            (a, b, c) => Some(a.into_iter().chain(b).chain(c).min().expect("one is Some")),
        };

        ring.wait(deadline)?;

        // **A deadline's arrival is read from the clock, never from the wait.**
        //
        // This used to be `true` exactly when the wait returned ETIME, which is
        // true only of an idle shard. Under load at least one completion is
        // always ready, `io_uring_enter` returns the moment it has one, and ETIME
        // never comes -- so a 100 ms accept backoff lasted until the shard next
        // went idle, and a connection that never spoke kept its fd and its
        // receive buffer for as long as traffic kept the loop away from the
        // branch below. A deadline is a time, so the clock is what decides it,
        // however the wait returned.
        let expired = deadline.is_some_and(|at| Instant::now() >= at);

        ring.done.clear();
        ring.drain();

        // By index, not by iterator: pushing an entry can make the kernel's
        // completer answer at once (EBUSY, drained into `done`), and those
        // completions belong to this pass.
        let mut i = 0;
        while i < ring.done.len() {
            let Done { data, res, flags } = ring.done[i];
            i += 1;
            let (idx, op) = untag(data);

            match op {
                OP_ACCEPT => {
                    accept_armed = cqueue::more(flags);

                    if res >= 0 {
                        let fd = res as RawFd;
                        // Nagle off: replies are tens of bytes and latency is
                        // what a benchmark reads.
                        set_nodelay(fd);

                        let admitted = tuning.startup_timeout.map(|t| Instant::now() + t);
                        let recycled = spare.pop().unwrap_or_default();
                        let idx = match free_slots.pop() {
                            Some(i) => {
                                slots[i] = Some(Slot::new(fd, admitted, recycled));
                                i
                            }
                            None => {
                                slots.push(Some(Slot::new(fd, admitted, recycled)));
                                slots.len() - 1
                            }
                        };
                        // Only a connection with a deadline belongs in the list
                        // the expiry pass walks; without one it is an entry the
                        // pass would drop for nothing.
                        if let Some(at) = admitted {
                            startup.push(idx);
                            earliest_startup = Some(earliest_startup.map_or(at, |e| e.min(at)));
                        }
                        arm_recv(&mut ring, &mut slots, bufs.as_ref(), idx, multishot)?;
                    } else {
                        let err = -res;
                        if accept_should_pause(err) {
                            // **Back off the ACCEPT, not the shard.** A run of
                            // accept failures is what fd exhaustion is; parking
                            // the accept for a moment keeps the connections
                            // already open served, which is exactly the work
                            // that should continue while new ones cannot be
                            // taken.
                            accept_paused_until = Some(Instant::now() + ACCEPT_BACKOFF);
                            eprintln!(
                                "pgnoop: accept failed with errno {err}; \
                                 not re-arming accept for {}ms (other work continues)",
                                ACCEPT_BACKOFF.as_millis()
                            );
                        } else if err == libc::EBADF || err == libc::EINVAL {
                            return Err(io::Error::from_raw_os_error(err));
                        }
                    }

                    // Only re-arm when the kernel is not still holding the
                    // request and the pause has expired. While paused, no
                    // accept is in flight and the loop keeps serving whatever
                    // is already connected.
                    let paused = accept_paused_until.is_some_and(|t| Instant::now() < t);
                    if !accept_armed && !paused {
                        accept_paused_until = None;
                        arm_accept(&mut ring, listen_fd, &mut accept_armed)?;
                    }
                }

                OP_RECV => {
                    let more = cqueue::more(flags);
                    let bid = cqueue::buffer_select(flags);

                    // **The buffer is the shard's, not the connection's.** If
                    // the slot is already gone the completion is still ours to
                    // clean up: a buffer that is not given back shrinks the ring
                    // for every other connection on the shard.
                    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
                        if let (Some(bid), Some(b)) = (bid, bufs.as_mut()) {
                            b.put(bid);
                        }
                        continue;
                    };
                    slot.recv_armed = more;
                    if res > 0 {
                        // The connection is reading again, whatever it had to
                        // do last time.
                        slot.private = false;
                    }

                    // Where the bytes are, and which buffer must go back.
                    let mut release: Option<u16> = None;
                    let mut src: *const u8 = std::ptr::null();
                    let mut n = 0usize;

                    if let (Some(bid), Some(b)) = (bid, bufs.as_mut()) {
                        // A one-shot descriptor is the whole of one read: the
                        // bytes start at the buffer and the buffer goes back as
                        // soon as the codec has seen them.
                        b.take(bid);
                        src = b.ptr(bid);
                        n = res.max(0) as usize;
                        release = Some(bid);
                    } else if bid.is_none() && res > 0 {
                        // No provided-buffer ring, or none left to give: the
                        // receive went into this connection's own buffer.
                        src = slot.recv.as_ptr();
                        n = res as usize;
                    }

                    if res <= 0 {
                        // An error can still carry a buffer.
                        if let (Some(bid), Some(b)) = (release, bufs.as_mut()) {
                            b.put(bid);
                        }

                        let next = match res {
                            // ENOBUFS: every buffer in the ring was taken when
                            // the data arrived. The bytes are still in the
                            // socket, so this connection is not dead -- it reads
                            // the next one into a buffer of its own.
                            r if r == -libc::ENOBUFS => {
                                #[cfg(test)]
                                if let Some(s) = &tuning.stats {
                                    s.enobufs.fetch_add(1, Ordering::Relaxed);
                                }
                                slot.private = true;
                                Next::Arm
                            }
                            // Cancelled: only a shutdown of ours can still do
                            // this, and the slot is then closing.
                            r if r == -libc::ECANCELED => {
                                if slot.closing {
                                    Next::Idle
                                } else {
                                    Next::Arm
                                }
                            }
                            // A kernel that does not know multishot receive (or
                            // a provided buffer selected by the kernel) says so
                            // here, once. Every receive after this one is a
                            // one-shot instead, and the shard says so rather
                            // than silently running a different loop.
                            r if (r == -libc::EINVAL || r == -libc::EOPNOTSUPP) && multishot => {
                                multishot = false;
                                slot.private = true;
                                eprintln!(
                                    "pgnoop: multishot receive unavailable ({}); \
                                     one receive per read instead",
                                    errno_name(-r)
                                );
                                Next::Arm
                            }
                            // Zero is the peer closing; any other error is the
                            // connection ending. Both close it.
                            _ => Next::Close,
                        };

                        match next {
                            Next::Arm => {
                                arm_recv(&mut ring, &mut slots, bufs.as_ref(), idx, multishot)?
                            }
                            Next::Close => begin_close(
                                &mut slots,
                                &mut free_slots,
                                &mut spare,
                                bufs.as_mut(),
                                idx,
                            ),
                            _ => {}
                        }
                        continue;
                    }

                    // The reply goes into `send` unless the kernel is still
                    // reading `send`, in which case it goes into `staged`: a
                    // multishot receive delivers while a send is outstanding.
                    if slot.send.capacity() == 0 {
                        slot.send.reserve(8 * 1024);
                    }
                    let out = if slot.sending {
                        &mut slot.staged
                    } else {
                        &mut slot.send
                    };

                    // SAFETY: `src` points into a provided buffer this thread
                    // just took out of the ring, or into the slot's own receive
                    // buffer, and `n` is the byte count the kernel wrote there.
                    let bytes = unsafe { std::slice::from_raw_parts(src, n) };

                    if slot.pending.is_empty() {
                        let used = slot.conn.advance(bytes, out, &catalog);
                        if used < n {
                            slot.pending.extend_from_slice(&bytes[used..n]);
                        }
                    } else {
                        slot.pending.extend_from_slice(bytes);
                        let used = slot.conn.advance(&slot.pending, out, &catalog);
                        slot.pending.drain(..used);
                    }

                    // Only now: the codec has read what the buffer held.
                    if let (Some(bid), Some(b)) = (release, bufs.as_mut()) {
                        b.put(bid);
                    }

                    // Startup completed: this connection is no longer the one
                    // the startup deadline is about.
                    if !slot.conn.awaiting_startup() {
                        slot.startup_deadline = None;
                    }

                    let next = if !slot.send.is_empty() && !slot.sending {
                        Next::Send
                    } else if slot.closing || slot.conn.is_closed() {
                        Next::Close
                    } else if !slot.recv_armed {
                        Next::Arm
                    } else {
                        Next::Idle
                    };
                    match next {
                        Next::Send => submit_send(&mut ring, &mut slots, idx)?,
                        Next::Close => {
                            begin_close(&mut slots, &mut free_slots, &mut spare, bufs.as_mut(), idx)
                        }
                        Next::Arm => {
                            arm_recv(&mut ring, &mut slots, bufs.as_ref(), idx, multishot)?
                        }
                        Next::Idle => {}
                    }
                }

                OP_SEND => {
                    let next = {
                        let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
                            continue;
                        };
                        slot.sending = false;

                        if res <= 0 {
                            Next::Close
                        } else {
                            slot.sent += res as usize;

                            if slot.sent < slot.send.len() {
                                // A short write is normal under pressure;
                                // finish it.
                                Next::Send
                            } else {
                                slot.send.clear();
                                slot.sent = 0;

                                if slot.closing || slot.conn.is_closed() {
                                    Next::Close
                                } else if !slot.staged.is_empty() {
                                    // The reply that arrived while this one was
                                    // in flight.
                                    std::mem::swap(&mut slot.send, &mut slot.staged);
                                    slot.staged.clear();
                                    Next::Send
                                } else if !slot.recv_armed {
                                    Next::Arm
                                } else {
                                    Next::Idle
                                }
                            }
                        }
                    };
                    match next {
                        Next::Send => submit_send(&mut ring, &mut slots, idx)?,
                        Next::Close => {
                            begin_close(&mut slots, &mut free_slots, &mut spare, bufs.as_mut(), idx)
                        }
                        Next::Arm => {
                            arm_recv(&mut ring, &mut slots, bufs.as_ref(), idx, multishot)?
                        }
                        Next::Idle => {}
                    }
                }

                _ => {
                    // Nothing else is ever submitted with a tag of its own.
                }
            }
        }

        // **The deadlines, once the wait has come back.** Only reached when at
        // least one was pending; with none, `expired` is false and this is a
        // branch on a bool.
        if expired {
            // A paused accept whose backoff has passed resumes here. Checked
            // against the clock rather than assumed: the wait can also have been
            // bounded by another deadline and returned early.
            if let Some(t) = accept_paused_until {
                if Instant::now() >= t {
                    accept_paused_until = None;
                    if !accept_armed {
                        arm_accept(&mut ring, listen_fd, &mut accept_armed)?;
                    }
                }
            }

            // Connections still in startup whose deadline has passed, walked as
            // the list of connections that are in startup rather than as every
            // slot on the shard.
            let now = Instant::now();
            let mut next: Option<Instant> = None;
            scratch.clear();
            for idx in startup.drain(..) {
                let deadline = slots
                    .get_mut(idx)
                    .and_then(|s| s.as_mut())
                    .and_then(|slot| slot.startup_deadline);
                let Some(at) = deadline else {
                    // The slot is gone, or its connection finished startup:
                    // either way this entry has no deadline to speak for.
                    continue;
                };
                if at > now {
                    next = Some(next.map_or(at, |n| n.min(at)));
                    scratch.push(idx);
                    continue;
                }

                if let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) {
                    if slot.conn.awaiting_startup() {
                        eprintln!(
                            "pgnoop: connection did not complete startup within {}ms; closing",
                            tuning.startup_timeout.map_or(0, |t| t.as_millis())
                        );
                    }
                    slot.startup_deadline = None;
                }

                // `shutdown`, not `close`: a slot with a receive in flight may
                // not be freed, and shutting the socket down is what makes that
                // receive return 0, so the ordinary path reaps it -- and gives
                // its buffer back on the way.
                begin_close(&mut slots, &mut free_slots, &mut spare, bufs.as_mut(), idx);
            }
            std::mem::swap(&mut startup, &mut scratch);
            earliest_startup = next;
        }
    }
}

/// Move every completion the kernel has posted into `done`.
fn drain_completions(cq: &mut cqueue::CompletionQueue<'_>, done: &mut Vec<Done>) {
    cq.sync();
    for cqe in &mut *cq {
        done.push(Done {
            data: cqe.user_data(),
            res: cqe.result(),
            flags: cqe.flags(),
        });
    }
    // Publishing the head is what gives the space back to the kernel.
    cq.sync();
}

/// Retry a ring operation while the kernel says its failure is transient.
///
/// The same three answers a wait gets: EINTR means nothing happened and the
/// operation is repeated; EBUSY means the completions it was for are already in
/// the ring, which `drain` empties before the retry; anything else is the ring
/// failing and ends the shard.
fn retry_with(mut op: impl FnMut() -> io::Result<()>, mut drain: impl FnMut()) -> io::Result<()> {
    loop {
        match op() {
            Ok(()) => return Ok(()),
            Err(e) => match retry_after(&e) {
                Retry::Again => continue,
                Retry::Drain => {
                    drain();
                    continue;
                }
                Retry::Stop => return Err(e),
            },
        }
    }
}

/// Wait for one completion, or until `deadline`, without ever ending the shard
/// over something that is not a failure.
///
/// **EINTR is not a shard's death.** `io_uring_enter` is an ordinary
/// interruptible wait: any signal delivered to this thread -- a profiler's
/// sample, a `SIGCHLD`, a supervisor's nudge -- brings it back as EINTR. That
/// used to be `?`'d straight out of the loop, so the shard stopped serving,
/// silently, on a signal that means nothing to it. An interrupted wait leaves
/// the submission queue untouched, so the retry resubmits exactly the work that
/// was there.
///
/// **EBUSY means the completions are already here.** The kernel answers it when
/// the completion ring is full of entries it could not flush from its overflow
/// list: the wait cannot wait, and the loop's next step -- draining the
/// completion queue -- is the retry.
///
/// ETIME is the deadline arriving, which is one of the things this wait is for.
/// Whether a deadline has actually passed is read from the clock by the caller,
/// never from this function's return.
fn wait_for_work(submit: &io_uring::Submitter<'_>, deadline: Option<Instant>) -> io::Result<()> {
    loop {
        let waited = match deadline {
            None => submit.submit_and_wait(1).map(drop),
            Some(at) => {
                let wait = at.saturating_duration_since(Instant::now());
                let ts = types::Timespec::new()
                    .sec(wait.as_secs())
                    .nsec(wait.subsec_nanos());

                match submit.submit_with_args(1, &types::SubmitArgs::new().timespec(&ts)) {
                    Ok(_) => Ok(()),
                    Err(e) if e.raw_os_error() == Some(libc::ETIME) => Ok(()),
                    Err(e) => Err(e),
                }
            }
        };

        match waited {
            Ok(()) => return Ok(()),
            Err(e) => match retry_after(&e) {
                Retry::Again => continue,
                Retry::Drain => return Ok(()),
                Retry::Stop => return Err(e),
            },
        }
    }
}

/// What a failed ring operation means for the shard.
///
/// The same classification decides a failed `io_uring_enter` whether it was
/// waiting (see `wait_for_work`) or only submitting (see `Ring::submit`): a
/// signal interrupts either, and a full completion ring refuses either.
#[derive(Debug, PartialEq, Eq)]
enum Retry {
    /// The kernel was interrupted before it could do anything. Nothing about
    /// the ring changed, so the same operation is submitted again.
    Again,
    /// The completions the operation was for are already here: return to the
    /// loop and drain them.
    Drain,
    /// The ring itself is failing, and the shard cannot go on.
    Stop,
}

fn retry_after(e: &io::Error) -> Retry {
    match e.raw_os_error() {
        // Some kernels report the interruption as a bare errno rather than
        // letting it reach `ErrorKind::Interrupted`.
        Some(libc::EINTR) => Retry::Again,
        // The completion ring is full with overflow entries the kernel could
        // not flush: there is nothing to wait for, only something to reap.
        Some(libc::EBUSY) => Retry::Drain,
        _ if e.kind() == io::ErrorKind::Interrupted => Retry::Again,
        _ => Retry::Stop,
    }
}

/// Nagle off: replies are tens of bytes and latency is what a benchmark reads.
fn set_nodelay(fd: RawFd) {
    let one: libc::c_int = 1;
    // SAFETY: a plain setsockopt on a socket this shard just accepted. A
    // failure leaves the socket as it was, which is why it is not checked.
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// What a connection needs next, decided while its slot is borrowed and acted
/// on after the borrow ends -- arming a receive needs the slot table again.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Next {
    Send,
    Close,
    Arm,
    Idle,
}

/// Is this accept errno one that clears with time rather than one that ends the
/// shard?
///
/// `EMFILE`/`ENFILE` mean the process is out of descriptors, `ENOMEM`/`ENOBUFS`
/// that the kernel is; none of them clears by retrying immediately, and retrying
/// immediately turns a shortage into a spin that burns the shard's core and
/// never recovers.
fn accept_should_pause(errno: i32) -> bool {
    errno == libc::EMFILE
        || errno == libc::ENFILE
        || errno == libc::ENOMEM
        || errno == libc::ENOBUFS
}

/// One multishot accept: the SQE is re-armed only when the kernel says it is no
/// longer holding it, which is what makes this one request per shard instead of
/// one per connection.
fn arm_accept(ring: &mut Ring, listen_fd: RawFd, armed: &mut bool) -> io::Result<()> {
    let e = opcode::AcceptMulti::new(types::Fd(listen_fd))
        .build()
        .user_data(tag(0, OP_ACCEPT));
    // SAFETY: a multishot accept points at no user buffer at all -- the accepted
    // fd is the completion's result.
    unsafe { ring.push(&e) }?;
    *armed = true;
    Ok(())
}

/// Arm this connection's receive.
///
/// At most one receive per connection: `recv_armed` is the guard, and a send
/// completing never re-arms a receive that is still armed -- that would put two
/// reads on one socket and lose whichever bytes the loser saw.
///
/// **Where the bytes land is the shard's buffer ring, when there is one.** The
/// ring has a fixed number of buffers, so a shard with more reads ready at once
/// than buffers is answered `ENOBUFS`; that connection then reads into a buffer
/// of its own (`private`), allocated the first time it is needed and recycled
/// through `Spare`. A connection the ring can serve never pays for one, and a
/// connection the ring cannot serve never waits for one.
fn arm_recv(
    ring: &mut Ring,
    slots: &mut [Option<Slot>],
    bufs: Option<&BufRing>,
    idx: usize,
    multishot: bool,
) -> io::Result<()> {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return Ok(());
    };
    if slot.recv_armed || slot.closing {
        // The guard, counting itself: a connection that already has a receive
        // in flight does not get a second one.
        #[cfg(test)]
        if let Some(s) = &ring.stats {
            s.elided.fetch_add(1, Ordering::Relaxed);
        }
        return Ok(());
    }

    let from_ring = !slot.private && bufs.is_some();
    #[cfg(test)]
    if !from_ring {
        if let Some(s) = &ring.stats {
            s.private_arms.fetch_add(1, Ordering::Relaxed);
        }
    }
    let e = if from_ring {
        if multishot {
            opcode::RecvMulti::new(types::Fd(slot.fd), PBUF_GROUP).build()
        } else {
            opcode::Recv::new(types::Fd(slot.fd), std::ptr::null_mut(), 0)
                .buf_group(PBUF_GROUP)
                .build()
                .flags(squeue::Flags::BUFFER_SELECT)
        }
    } else {
        if slot.recv.is_empty() {
            slot.recv = vec![0u8; RECV_BUF];
        }
        opcode::Recv::new(
            types::Fd(slot.fd),
            slot.recv.as_mut_ptr(),
            slot.recv.len() as u32,
        )
        .build()
    }
    .user_data(tag(idx, OP_RECV));

    // SAFETY: a provided-buffer receive points at no user buffer (the ring
    // supplies one), and one of the connection's own points at `slot.recv`,
    // whose allocation is stable and outlives the request.
    unsafe { ring.push(&e) }?;
    slot.recv_armed = true;
    #[cfg(test)]
    if let Some(s) = &ring.stats {
        s.arms.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn submit_send(ring: &mut Ring, slots: &mut [Option<Slot>], idx: usize) -> io::Result<()> {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return Ok(());
    };
    if slot.sending || slot.sent >= slot.send.len() {
        return Ok(());
    }
    let e = opcode::Send::new(
        types::Fd(slot.fd),
        // SAFETY: `sent` only advances when a send completes, so the pointer is
        // into the part of `send` the kernel has not taken.
        unsafe { slot.send.as_ptr().add(slot.sent) },
        (slot.send.len() - slot.sent) as u32,
    )
    .build()
    .user_data(tag(idx, OP_SEND));

    // SAFETY: `send` is not touched again until this completes.
    unsafe { ring.push(&e) }?;
    slot.sending = true;
    Ok(())
}

/// Close a connection, but only once nothing of it is in flight.
///
/// **A receive in flight cannot be closed out from under the kernel.** The
/// request holds its own reference to the socket, so `close` would leave the
/// kernel writing into a slot the shard has forgotten -- with a provided buffer
/// attached, which the ring then never gets back. `shutdown` is what ends the
/// receive, and its completion is what frees the slot.
fn begin_close(
    slots: &mut [Option<Slot>],
    free_slots: &mut Vec<usize>,
    spare: &mut Vec<Spare>,
    bufs: Option<&mut BufRing>,
    idx: usize,
) {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return;
    };
    slot.closing = true;

    if slot.recv_armed {
        // SAFETY: the fd is owned by the slot and is closed only once its
        // receive has completed.
        unsafe { libc::shutdown(slot.fd, libc::SHUT_RDWR) };
        return;
    }
    if slot.sending {
        return;
    }
    reap(slots, free_slots, spare, bufs, idx);
}

/// Close and free a slot, but only once nothing is still pointing at its
/// buffers. A slot freed with an operation in flight is a use-after-free the
/// kernel performs for you.
fn reap(
    slots: &mut [Option<Slot>],
    free_slots: &mut Vec<usize>,
    spare: &mut Vec<Spare>,
    bufs: Option<&mut BufRing>,
    idx: usize,
) {
    let fd = match slots.get_mut(idx).and_then(|s| s.as_mut()) {
        Some(slot) if !slot.recv_armed && !slot.sending => {
            // Every buffer this connection was given has been handed back by
            // the completion that carried it, which is what one-shot provided
            // buffers mean: a slot in flight owns nothing.
            let _ = &bufs;
            slot.fd
        }
        _ => return,
    };

    // SAFETY: nothing in flight names this fd any more.
    unsafe { libc::close(fd) };

    if let Some(mut slot) = slots[idx].take() {
        #[cfg(not(test))]
        let _ = idx;
        spare.push(Spare {
            recv: std::mem::take(&mut slot.recv),
            pending: std::mem::take(&mut slot.pending),
            send: std::mem::take(&mut slot.send),
            staged: std::mem::take(&mut slot.staged),
        });
    }
    free_slots.push(idx);
}

/// The shard's provided-buffer ring: one registered array of descriptors and one
/// block of buffers, shared by every connection on the shard.
///
/// `IORING_REGISTER_PBUF_RING` (Linux 5.19) registers a userspace ring of
/// buffer descriptors; a receive with `IOSQE_BUFFER_SELECT` takes one from it
/// instead of pointing at a buffer of its own. `IOU_PBUF_RING_INC` (6.12) makes
/// the kernel keep consuming the same descriptor while there is room, so a
/// stream of small reads does not cost one descriptor round trip per read.
///
/// Both are probed: a kernel that does not know the flag answers EINVAL on the
/// registration, and the ring is registered without it.
struct BufRing {
    /// The descriptor ring, page aligned as the interface requires, and the
    /// buffers it describes.
    desc: *mut types::BufRingEntry,
    desc_len: usize,
    data: *mut u8,
    data_len: usize,
    count: u16,
    /// Our copy of the ring's tail. Userspace owns this field; the kernel only
    /// reads it.
    tail: u16,
    /// A descriptor the kernel has taken and not yet given back.
    in_use: Vec<bool>,
    /// Descriptors we believe are in the ring.
    free: u16,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
}

impl BufRing {
    /// Does this kernel register a provided-buffer ring, and does it consume
    /// buffers incrementally? The answer is a property of the kernel, asked on a
    /// ring that is thrown away -- and reported, even though only the one-shot
    /// form is used (`BufRing::register` says why).
    fn probe(submit: &io_uring::Submitter<'_>) -> io::Result<bool> {
        let one = BufRing::alloc(1)?;
        one.publish();

        // SAFETY: the descriptor ring is page aligned and lives until the
        // registration is undone below.
        let inc = match unsafe {
            submit.register_buf_ring_with_flags(
                one.desc as u64,
                one.count,
                PBUF_GROUP,
                types::IOU_PBUF_RING_INC as u16,
            )
        } {
            Ok(()) => {
                let _ = submit.unregister_buf_ring(PBUF_GROUP);
                true
            }
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => false,
            Err(e) => return Err(e),
        };

        if !inc {
            // SAFETY: as above.
            unsafe {
                submit.register_buf_ring_with_flags(one.desc as u64, one.count, PBUF_GROUP, 0)
            }?;
            let _ = submit.unregister_buf_ring(PBUF_GROUP);
        }
        Ok(inc)
    }

    /// Register the shard's ring. **One-shot, never incremental.**
    ///
    /// `IOU_PBUF_RING_INC` is offered by this kernel and refused here on
    /// evidence: an incremental ring keeps a buffer with the receive that filled
    /// it until the buffer is full, so one connection holds one buffer for as
    /// long as it is idle -- and a shard with more connections than buffers then
    /// has no buffer to give them. The kernel publishes no completion when it
    /// releases such a buffer (measured: a connection that read 19 bytes held
    /// its buffer, and its next completion arrived against a different buffer
    /// with the first never reported), so a buffer's ownership cannot be
    /// followed from the completion queue at all. One-shot consumption is exact:
    /// every completion carries a buffer and gives it straight back.
    fn register(
        submit: &io_uring::Submitter<'_>,
        count: u16,
        #[cfg(test)] stats: Option<std::sync::Arc<Stats>>,
    ) -> io::Result<BufRing> {
        let b = BufRing::alloc(count)?;
        #[cfg(test)]
        let b = {
            let mut b = b;
            b.stats = stats;
            b
        };
        b.publish();

        // SAFETY: the descriptor ring outlives the registration -- it is unmapped
        // in `Drop`, after the ring itself is gone.
        unsafe { submit.register_buf_ring_with_flags(b.desc as u64, b.count, PBUF_GROUP, 0) }?;
        Ok(b)
    }

    fn alloc(count: u16) -> io::Result<BufRing> {
        // The ring's index is a mask, so the count is a power of two.
        let count = count.max(1).next_power_of_two();
        let desc_len =
            (count as usize * std::mem::size_of::<types::BufRingEntry>()).next_multiple_of(4096);
        let data_len = (count as usize * RECV_BUF).next_multiple_of(4096);

        // SAFETY: fresh anonymous mappings, owned by this ring alone.
        let desc = unsafe { mmap_anon(desc_len)? } as *mut types::BufRingEntry;
        // SAFETY: as above. A second mapping that fails must not leak the first.
        let data = match unsafe { mmap_anon(data_len) } {
            Ok(p) => p,
            Err(e) => {
                // SAFETY: `desc` was mapped by us and is not registered.
                unsafe { libc::munmap(desc as *mut libc::c_void, desc_len) };
                return Err(e);
            }
        };

        let mut b = BufRing {
            desc,
            desc_len,
            data,
            data_len,
            count,
            tail: 0,
            in_use: vec![false; count as usize],
            free: 0,
            #[cfg(test)]
            stats: None,
        };
        for bid in 0..count {
            b.add(bid);
        }
        Ok(b)
    }

    /// Publish the tail. Release, because the descriptors it publishes must be
    /// visible to the kernel before it reads the tail.
    fn publish(&self) {
        // SAFETY: `disc` is our mapping, page aligned, and its first entry's
        // reserved field is the ring's tail.
        unsafe {
            let tail = types::BufRingEntry::tail(self.desc) as *const AtomicU16;
            (*tail).store(self.tail, Ordering::Release);
        }
    }

    /// Put one descriptor into the ring.
    fn add(&mut self, bid: u16) {
        // SAFETY: `tail & (count - 1)` is inside the mapping, and this thread is
        // the only one that writes descriptors.
        unsafe {
            let e = &mut *self.desc.add((self.tail & (self.count - 1)) as usize);
            e.set_addr(self.data.add(bid as usize * RECV_BUF) as u64);
            e.set_len(RECV_BUF as u32);
            e.set_bid(bid);
        }
        self.tail = self.tail.wrapping_add(1);
        self.free += 1;
        self.publish();
        #[cfg(test)]
        self.record();
    }

    /// Give a descriptor back to the ring.
    fn put(&mut self, bid: u16) {
        debug_assert!(self.in_use[bid as usize], "buffer {bid} returned twice");
        self.in_use[bid as usize] = false;
        self.add(bid);
    }

    /// Note that the kernel has taken a descriptor.
    fn take(&mut self, bid: u16) {
        self.in_use[bid as usize] = true;
        self.free -= 1;
        #[cfg(test)]
        self.record();
    }

    fn ptr(&self, bid: u16) -> *const u8 {
        // SAFETY: `bid` is a descriptor the ring handed out, so it names one of
        // this ring's RECV_BUF-sized buffers.
        unsafe { self.data.add(bid as usize * RECV_BUF) }
    }

    #[cfg(test)]
    fn record(&self) {
        if let Some(s) = &self.stats {
            s.free.store(self.free, Ordering::Relaxed);
            s.held.store(
                self.in_use.iter().filter(|b| **b).count() as u16,
                Ordering::Relaxed,
            );
        }
    }
}

impl Drop for BufRing {
    fn drop(&mut self) {
        // SAFETY: both mappings were made by `alloc` and are unmapped once.
        unsafe {
            libc::munmap(self.desc as *mut libc::c_void, self.desc_len);
            libc::munmap(self.data as *mut libc::c_void, self.data_len);
        }
    }
}

/// A page-aligned anonymous mapping. The provided-buffer interface requires the
/// descriptor ring to be page aligned, and the buffers it describes are not
/// allowed to move.
unsafe fn mmap_anon(len: usize) -> io::Result<*mut u8> {
    // SAFETY: the caller's contract; MAP_ANONYMOUS means no fd is involved.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p as *mut u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::sync::atomic::{AtomicBool, AtomicI32};
    use std::sync::Arc;

    /// How many clients keep the shard busy in the deadline tests, and how many
    /// pipelined queries each writes per pass. Together they are what saturates
    /// one shard from a test process: see `spawn_busy_client`.
    const BUSY_CLIENTS: usize = 32;
    const BUSY_BATCH: usize = 2048;

    /// Set in the child process of `an_accept_paused_by_emfile_resumes_...`,
    /// which is this same test binary re-entered with one test selected.
    const EMFILE_CHILD: &str = "PGNOOP_URING_EMFILE_CHILD";

    fn startup_message() -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
        body.extend_from_slice(b"user\0test\0\0");

        let mut msg = ((body.len() + 4) as u32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        msg
    }

    /// One simple-protocol query, framed.
    fn query_message(sql: &str) -> Vec<u8> {
        let mut body = sql.as_bytes().to_vec();
        body.push(0);
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        msg.extend_from_slice(&body);
        msg
    }

    fn select_1() -> Vec<u8> {
        query_message("select 1")
    }

    /// One wire frame: its type byte and its body, length prefix removed.
    fn read_frame(s: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
        let mut head = [0u8; 5];
        s.read_exact(&mut head)?;
        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
        if len < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame length {len}"),
            ));
        }
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body)?;
        Ok((head[0], body))
    }

    /// Frames up to and including ReadyForQuery.
    fn read_until_ready(s: &mut TcpStream) -> std::io::Result<Vec<(u8, Vec<u8>)>> {
        let mut frames = Vec::new();
        loop {
            let f = read_frame(s)?;
            let ready = f.0 == b'Z';
            frames.push(f);
            if ready {
                return Ok(frames);
            }
        }
    }

    /// The column count a RowDescription or DataRow body starts with.
    fn columns(body: &[u8]) -> usize {
        u16::from_be_bytes([body[0], body[1]]) as usize
    }

    /// The frames of a query's answer.
    fn ask(s: &mut TcpStream, sql: &str) -> std::io::Result<Vec<(u8, Vec<u8>)>> {
        s.write_all(&query_message(sql))?;
        read_until_ready(s)
    }

    /// A shard on a fresh loopback port, and the tid of the thread running it.
    ///
    /// `None` when this box cannot create a ring at all -- the condition `probe`
    /// reports and the server falls back to epoll for. A test that cannot serve
    /// here is not a test that failed.
    /// A shard that stops, and gives its ring back, when the test is done with
    /// it. A test binary runs a dozen of these, and a ring is kernel memory with
    /// a process-wide budget.
    struct Shard {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
        _slot: Option<ShardSlot>,
    }

    impl Drop for Shard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            // The shard's wait is bounded in test builds, so it notices the flag
            // within a pass and gives its ring back here.
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    /// How many shards a test process may have alive at once. A ring is kernel
    /// memory with a process-wide budget that this box's default `mlock` limit
    /// bounds at about a dozen; a test binary running twenty at once is refused
    /// one, and that is the environment rather than the test.
    const MAX_LIVE_SHARDS: usize = 6;

    static LIVE_SHARDS: std::sync::Mutex<usize> = std::sync::Mutex::new(0);
    static SHARD_FREED: std::sync::Condvar = std::sync::Condvar::new();

    /// One shard's worth of the process's ring budget.
    struct ShardSlot;

    impl Drop for ShardSlot {
        fn drop(&mut self) {
            let mut live = LIVE_SHARDS.lock().expect("live shards");
            *live -= 1;
            SHARD_FREED.notify_one();
        }
    }

    fn shard_slot() -> ShardSlot {
        let mut live = LIVE_SHARDS.lock().expect("live shards");
        while *live >= MAX_LIVE_SHARDS {
            live = SHARD_FREED.wait(live).expect("live shards");
        }
        *live += 1;
        ShardSlot
    }

    fn spawn_shard(mut tuning: Tuning) -> Option<(SocketAddr, Arc<AtomicI32>, Shard)> {
        if plan().absent.is_some() {
            return None;
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let tid = Arc::new(AtomicI32::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let _slot = shard_slot();
        tuning.stop = Some(Arc::clone(&stop));

        let published = Arc::clone(&tid);
        let thread = std::thread::spawn(move || {
            // SAFETY: gettid has no arguments and no failure.
            published.store(
                unsafe { libc::syscall(libc::SYS_gettid) } as i32,
                Ordering::SeqCst,
            );

            let catalog = CatalogView(Arc::new(crate::handler::NoopHandler::new()));
            if let Err(e) = run_until(listener, catalog, tuning) {
                eprintln!("test shard ended: {e}");
            }
        });

        for _ in 0..1000 {
            if tid.load(Ordering::SeqCst) != 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        Some((
            addr,
            tid,
            Shard {
                stop,
                thread: Some(thread),
                _slot: Some(_slot),
            },
        ))
    }

    /// A client that keeps its shard saturated until `stop` is set.
    ///
    /// **Saturated, not merely busy**, because that is the state the deadlines
    /// were invisible in: the loop only ever asked the wait whether a deadline
    /// had passed, and a shard with work always ready answers "a completion"
    /// instead. A batch of pipelined queries per write is what makes the shard
    /// the bottleneck rather than the clients.
    fn spawn_busy_client(addr: SocketAddr, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let Ok(mut stream) = TcpStream::connect(addr) else {
                return;
            };
            let _ = stream.set_nodelay(true);

            // The handshake goes out ONCE: a startup message sent again after
            // the first is a malformed query frame, and the codec would close
            // the connection the shard is supposed to be busy with.
            if stream.write_all(&startup_message()).is_err() {
                return;
            }

            let mut batch = Vec::with_capacity(BUSY_BATCH * 14);
            for _ in 0..BUSY_BATCH {
                batch.extend_from_slice(&select_1());
            }

            let Ok(mut replies) = stream.try_clone() else {
                return;
            };
            std::thread::spawn(move || {
                let mut scratch = [0u8; 16 * 1024];
                while let Ok(n) = replies.read(&mut scratch) {
                    if n == 0 {
                        return;
                    }
                }
            });

            while !stop.load(Ordering::SeqCst) {
                if stream.write_all(&batch).is_err() {
                    return;
                }
            }

            let _ = stream.shutdown(std::net::Shutdown::Both);
        })
    }

    /// Startup, one query, and however much of the reply arrives.
    fn round_trip(addr: SocketAddr) -> std::io::Result<usize> {
        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.write_all(&startup_message())?;
        stream.write_all(&select_1())?;

        let mut buf = [0u8; 256];
        stream.read(&mut buf)
    }

    /// **B1: an expired deadline is decided by the clock, not by the wait's
    /// return code.**
    #[test]
    fn a_startup_deadline_expires_while_completions_keep_arriving() {
        let tuning = Tuning {
            startup_timeout: Some(Duration::from_millis(300)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };

        let stop = Arc::new(AtomicBool::new(false));
        let busy: Vec<_> = (0..BUSY_CLIENTS)
            .map(|_| spawn_busy_client(addr, Arc::clone(&stop)))
            .collect();

        std::thread::sleep(Duration::from_millis(400));

        let mut idle = TcpStream::connect(addr).expect("connect");
        // Long enough for a debug build of the shard to work through the
        // saturated clients' backlog; the property under test is that the
        // deadline is decided by the clock at all, not that it is decided within
        // a millisecond of 300.
        idle.set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");

        let started = Instant::now();
        let mut byte = [0u8; 1];
        let read = idle.read(&mut byte);
        let waited = started.elapsed();

        stop.store(true, Ordering::SeqCst);
        for client in busy {
            let _ = client.join();
        }

        assert_eq!(
            read.ok(),
            Some(0),
            "a connection silent past its 300ms startup deadline must be closed \
             even while the shard is busy serving others (read returned after {waited:?})"
        );
        assert!(
            waited < Duration::from_secs(8),
            "the silent connection was closed only after {waited:?}"
        );
    }

    /// **B1, the accept half: a paused accept resumes when fds come back.**
    #[test]
    fn an_accept_paused_by_emfile_resumes_while_the_shard_is_busy() {
        if std::env::var_os(EMFILE_CHILD).is_some() {
            emfile_scenario();
            return;
        }

        let exe = std::env::current_exe().expect("the test binary's path");
        let child = std::process::Command::new(exe)
            .args([
                "--exact",
                "uring::tests::an_accept_paused_by_emfile_resumes_while_the_shard_is_busy",
                "--nocapture",
            ])
            .env(EMFILE_CHILD, "1")
            .output()
            .expect("run the child test process");

        let stderr = String::from_utf8_lossy(&child.stderr);

        assert!(
            child.status.success(),
            "the fd-exhaustion scenario failed in its own process\n--- stdout ---\n{}\n--- stderr ---\n{stderr}",
            String::from_utf8_lossy(&child.stdout),
        );

        // **The kernel may absorb the shortage itself.** A multishot accept that
        // cannot get a descriptor is retried inside the kernel rather than
        // reported, so the shard's backoff is the fallback for a kernel that
        // reports it rather than the path every fd shortage takes. What the
        // scenario always checks is that the connection made during the shortage
        // is served once the descriptors come back; `only_a_shortage_pauses_the_accept`
        // pins the classification.
        if !stderr.contains(&format!("accept failed with errno {}", libc::EMFILE)) {
            eprintln!(
                "note: this kernel's multishot accept absorbed the fd shortage \
                 (no EMFILE completion); the connection was served once fds returned"
            );
        }
    }

    /// The body of `an_accept_paused_by_emfile_resumes_while_the_shard_is_busy`,
    /// run in the child process where the fd limit can be lowered.
    fn emfile_scenario() {
        let tuning = Tuning {
            startup_timeout: Some(Duration::from_secs(60)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };

        let stop = Arc::new(AtomicBool::new(false));
        let busy: Vec<_> = (0..3)
            .map(|_| spawn_busy_client(addr, Arc::clone(&stop)))
            .collect();

        std::thread::sleep(Duration::from_millis(400));

        let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
        // SAFETY: a plain kernel-filled struct.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
            0,
            "getrlimit: {}",
            io::Error::last_os_error()
        );
        limit.rlim_cur = limit.rlim_cur.min(48);
        // SAFETY: lowering the soft limit is always allowed.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) },
            0,
            "setrlimit: {}",
            io::Error::last_os_error()
        );

        let mut held = Vec::new();
        loop {
            match std::fs::File::open("/dev/null") {
                Ok(f) => held.push(f),
                Err(e) => {
                    assert_eq!(e.raw_os_error(), Some(libc::EMFILE), "exhausting fds: {e}");
                    break;
                }
            }
        }
        assert!(!held.is_empty(), "the limit left room to exhaust");

        held.truncate(held.len() - 1);
        let _late = TcpStream::connect(addr).expect("connect with the last free fd");
        std::thread::sleep(Duration::from_millis(200));

        held.clear();

        let reply = round_trip(addr);
        stop.store(true, Ordering::SeqCst);
        for client in busy {
            let _ = client.join();
        }

        assert!(
            matches!(reply, Ok(n) if n > 0),
            "an accept paused by EMFILE must resume once fds are free, even while \
             the shard is busy; a new connection got {reply:?}"
        );
    }

    extern "C" fn ignore_signal(_: libc::c_int) {}

    /// A no-op handler, installed WITHOUT `SA_RESTART`, so the syscall it
    /// interrupts returns EINTR instead of being restarted invisibly.
    fn install_noop_handler(signal: libc::c_int) {
        // SAFETY: a zeroed sigaction is a valid empty disposition; the handler
        // is a plain `extern "C" fn`, and handles one `int`.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = ignore_signal as *const () as libc::sighandler_t;
            action.sa_flags = 0;
            libc::sigemptyset(&mut action.sa_mask);

            assert_eq!(
                libc::sigaction(signal, &action, std::ptr::null_mut()),
                0,
                "sigaction: {}",
                io::Error::last_os_error()
            );
        }
    }

    /// **B2: a signal is not a shard's death.**
    #[test]
    fn a_signal_interrupting_the_wait_does_not_end_the_shard() {
        install_noop_handler(libc::SIGUSR1);

        let Some((addr, tid, _shard)) = spawn_shard(Tuning::default()) else {
            return;
        };

        std::thread::sleep(Duration::from_millis(100));

        let pid = unsafe { libc::getpid() };
        for _ in 0..20 {
            let shard = tid.load(Ordering::SeqCst);
            assert!(shard > 0, "the shard thread published its tid");

            // SAFETY: `tgkill` with a tid this process published for itself,
            // with a handler installed for the signal.
            let sent = unsafe { libc::syscall(libc::SYS_tgkill, pid, shard, libc::SIGUSR1) };
            assert_eq!(sent, 0, "tgkill: {}", io::Error::last_os_error());

            std::thread::sleep(Duration::from_millis(5));
        }

        let reply = round_trip(addr);

        assert!(
            matches!(reply, Ok(n) if n > 0),
            "a shard interrupted by a signal must keep serving; a client got {reply:?}"
        );
    }

    /// **B2: a failed ring operation is classified, not fatal by default.**
    #[test]
    fn a_transient_wait_failure_is_not_a_dead_shard() {
        assert_eq!(
            retry_after(&io::Error::from(io::ErrorKind::Interrupted)),
            Retry::Again
        );
        assert_eq!(
            retry_after(&io::Error::from_raw_os_error(libc::EINTR)),
            Retry::Again
        );
        assert_eq!(
            retry_after(&io::Error::from_raw_os_error(libc::EBUSY)),
            Retry::Drain
        );

        for fatal in [libc::EBADF, libc::EINVAL, libc::ENOMEM, libc::ECANCELED] {
            assert_eq!(
                retry_after(&io::Error::from_raw_os_error(fatal)),
                Retry::Stop,
                "errno {fatal}"
            );
        }
    }

    /// **A submit's transient failure is not the shard's death either.**
    ///
    /// `push_or_submit` submits when the submission queue is full, and so does
    /// the loop before it first waits. Both used to `?` the errno straight out
    /// of the loop -- the same mistake the wait made, one line further down. A
    /// full completion ring answers such a submit with EBUSY, and a signal
    /// answers it with EINTR.
    ///
    /// EINTR is retried and EBUSY drains and retries; that loop is what this
    /// pins, because the two errnos cannot both be produced on demand from a
    /// real ring: a submit that has nothing to wait for does not block, so a
    /// signal cannot interrupt one, and `io_uring_enter` flushes the kernel's
    /// overflow list before it answers EBUSY.
    #[test]
    fn a_transient_submit_failure_is_retried() {
        let mut attempts = 0;
        retry_with(
            || {
                attempts += 1;
                if attempts < 3 {
                    Err(io::Error::from_raw_os_error(libc::EINTR))
                } else {
                    Ok(())
                }
            },
            || panic!("an interrupted submit has nothing to drain"),
        )
        .expect("an interrupted submit is retried, not fatal");
        assert_eq!(attempts, 3, "the submit was repeated, not abandoned");

        let mut drained = 0;
        let mut busy = true;
        retry_with(
            || {
                if std::mem::take(&mut busy) {
                    Err(io::Error::from_raw_os_error(libc::EBUSY))
                } else {
                    Ok(())
                }
            },
            || drained += 1,
        )
        .expect("a full completion ring is drained and the submit retried");
        assert_eq!(drained, 1, "EBUSY must empty the completion ring");

        let mut drained = 0;
        let e = retry_with(
            || Err::<(), _>(io::Error::from_raw_os_error(libc::EBADF)),
            || drained += 1,
        )
        .expect_err("a failing ring must not be retried forever");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF));
        assert_eq!(drained, 0, "a fatal errno must not be treated as EBUSY");
    }

    /// An accept that runs out of descriptors pauses; one the ring itself
    /// refused does not.
    #[test]
    fn only_a_shortage_pauses_the_accept() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOMEM, libc::ENOBUFS] {
            assert!(accept_should_pause(errno), "errno {errno}");
        }
        for errno in [libc::EBADF, libc::EINVAL, libc::ECONNABORTED, libc::EAGAIN] {
            assert!(!accept_should_pause(errno), "errno {errno}");
        }
    }

    /// **The setup ladder attributes its refusals.**
    #[test]
    fn the_ladder_drops_only_the_flag_the_kernel_refuses() {
        let plan = climb(true, 16384, |s| {
            // A kernel older than 6.6, which does not know SUBMIT_ALL.
            if s.submit_all {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else {
                Ok(())
            }
        })
        .expect("a plain ring builds");

        assert!(!plan.setup.submit_all, "the refused flag is not kept");
        assert_eq!(
            plan.dropped,
            vec![("SUBMIT_ALL", libc::EINVAL)],
            "a flag is dropped after the errno it was refused with, and nothing else is"
        );
        for name in [
            "SINGLE_ISSUER",
            "DEFER_TASKRUN",
            "COOP_TASKRUN",
            "TASKRUN_FLAG",
            "NO_SQARRAY",
            "CLAMP",
            "CQSIZE",
        ] {
            assert!(plan.granted.contains(&name), "{name} must still be granted");
        }
        assert!(plan.setup.cqsize);
    }

    /// `TASKRUN_FLAG` is a notification bit the kernel refuses unless task work
    /// is deferred to this thread, so it is never asked for on its own.
    #[test]
    fn taskrun_flag_is_never_asked_for_without_coop_or_defer() {
        let asked_flag: Arc<std::sync::Mutex<Vec<bool>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&asked_flag);

        let plan = climb(false, 1024, move |s| {
            seen.lock().expect("lock").push(s.taskrun_flag);
            if s.coop_taskrun {
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            } else {
                Ok(())
            }
        })
        .expect("a plain ring builds");

        assert!(
            asked_flag.lock().expect("lock").iter().all(|f| !*f),
            "TASKRUN_FLAG was asked for with neither COOP_TASKRUN nor DEFER_TASKRUN"
        );
        assert!(!plan.setup.taskrun_flag);
        assert!(plan.dropped.iter().any(|(name, _)| *name == "COOP_TASKRUN"));
        assert!(
            !plan.granted.contains(&"DEFER_TASKRUN"),
            "PGNOOP_NO_DEFER was set"
        );
    }

    /// **A query split across two reads is answered once.**
    ///
    /// The bytes of a half message that did not arrive this time are carried in
    /// the connection's own buffer, never in the provided buffer they were read
    /// from: that one is back in the ring -- and in another connection's receive
    /// -- before the rest of the frame arrives.
    #[test]
    fn a_query_split_across_reads_is_answered_once() {
        let Some((addr, _tid, _shard)) = spawn_shard(Tuning::default()) else {
            return;
        };

        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_nodelay(true).expect("nodelay");
        s.set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        s.write_all(&startup_message()).expect("startup");
        read_until_ready(&mut s).expect("handshake reply");

        let q = query_message("select 1, 1, 1");
        s.write_all(&q[..q.len() - 3]).expect("first half");
        std::thread::sleep(Duration::from_millis(30));
        s.write_all(&q[q.len() - 3..]).expect("second half");

        let frames = read_until_ready(&mut s).expect("reply to a split query");
        assert_eq!(frames.len(), 4, "one answer, not two: {frames:?}");
        assert_eq!(frames[0].0, b'T');
        assert_eq!(columns(&frames[0].1), 3);
        assert_eq!(frames[1].0, b'D');
        assert_eq!(columns(&frames[1].1), 3);

        // And the connection is still whole afterwards.
        let frames = ask(&mut s, "select 1").expect("reply");
        assert_eq!(frames.len(), 4);
        assert_eq!(columns(&frames[0].1), 1);
    }

    /// **One buffer, two connections, no starvation.**
    ///
    /// A one-shot descriptor goes back into the ring as soon as its completion
    /// has been read, so a shard whose ring holds a single buffer can still serve
    /// any number of connections -- one read at a time -- and a connection that
    /// arrives while the buffer is out is answered ENOBUFS and reads into a
    /// buffer of its own rather than waiting.
    #[test]
    fn one_buffer_serves_every_connection() {
        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            entries: 128,
            pbuf_count: 1,
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };

        let mut clients = Vec::new();
        for conn in 0..4 {
            clients.push(std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).expect("connect");
                s.set_nodelay(true).expect("nodelay");
                s.set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                s.write_all(&startup_message()).expect("startup");
                read_until_ready(&mut s).expect("handshake reply");
                for seq in 0..8 {
                    let cols = 1 + (conn + seq) % 3;
                    let sql = format!("select {}", vec!["1"; cols].join(", "));
                    let frames = ask(&mut s, &sql).expect("reply");
                    assert_eq!(frames.len(), 4, "conn {conn} seq {seq}");
                    assert_eq!(columns(&frames[0].1), cols, "conn {conn} seq {seq}");
                }
            }));
        }
        for c in clients {
            c.join().expect("a client must not panic");
        }

        // One buffer in the ring, and nothing left holding it.
        assert_eq!(
            stats.free.load(Ordering::Relaxed),
            1,
            "the shard's only buffer must be back in the ring"
        );
        assert_eq!(stats.held.load(Ordering::Relaxed), 0);
    }

    /// **Many connections, one deliberately small ring.**
    ///
    /// Four buffers for sixteen connections forces ENOBUFS, buffer reuse across
    /// connections, and the take-back, while every reply is checked against the
    /// connection and the sequence number that asked for it. Half the connections
    /// then leave mid-frame with a receive armed and a partly filled buffer: that
    /// buffer has to come back too, or the ring is smaller for every connection
    /// that follows.
    #[test]
    fn many_connections_share_one_small_buffer_ring_without_losing_a_byte() {
        const BUFFERS: u16 = 4;
        const CONNS: usize = 16;
        const QUERIES: usize = 40;

        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            entries: 256,
            pbuf_count: BUFFERS,
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };

        let mut clients = Vec::new();
        for conn in 0..CONNS {
            clients.push(std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).expect("connect");
                s.set_nodelay(true).expect("nodelay");
                s.set_read_timeout(Some(Duration::from_secs(30)))
                    .expect("read timeout");
                s.set_write_timeout(Some(Duration::from_secs(30)))
                    .expect("write timeout");
                s.write_all(&startup_message()).expect("startup");
                read_until_ready(&mut s).expect("handshake reply");

                for seq in 0..QUERIES {
                    // The answer's shape depends on the connection AND on the
                    // request, so an answer that belongs to another request --
                    // or another connection -- is one that does not match.
                    let cols = 1 + (conn * 7 + seq) % 5;
                    let sql = format!("select {}", vec!["1"; cols].join(", "));
                    let frames = ask(&mut s, &sql).expect("reply");
                    assert_eq!(frames.len(), 4, "conn {conn} seq {seq}: {frames:?}");
                    assert_eq!(frames[0].0, b'T', "conn {conn} seq {seq}");
                    assert_eq!(
                        columns(&frames[0].1),
                        cols,
                        "conn {conn} seq {seq}: the answer is not this request's"
                    );
                    assert_eq!(frames[1].0, b'D');
                    assert_eq!(columns(&frames[1].1), cols, "conn {conn} seq {seq}");
                    assert_eq!(frames[2].0, b'C');
                    assert_eq!(frames[3].0, b'Z');
                }

                if conn % 2 == 0 {
                    // Leave mid-frame, with a receive armed and a buffer partly
                    // filled: the shard must give that buffer back.
                    let partial = query_message("select 1, 1");
                    s.write_all(&partial[..partial.len() / 2])
                        .expect("half a query");
                    std::thread::sleep(Duration::from_millis(20));
                }
                drop(s);
            }));
        }

        for client in clients {
            client.join().expect("a client must not panic");
        }

        // Every buffer back, once nothing is connected.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && stats.free.load(Ordering::Relaxed) != BUFFERS {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            stats.free.load(Ordering::Relaxed),
            BUFFERS,
            "every provided buffer must be back in the ring"
        );
        assert_eq!(
            stats.held.load(Ordering::Relaxed),
            0,
            "the kernel must not still be holding a buffer"
        );
        assert!(
            stats.arms.load(Ordering::Relaxed) > 0,
            "the connections must have been served"
        );
    }
}
