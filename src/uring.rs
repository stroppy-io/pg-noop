//! A shard's event loop on io_uring, with no async runtime at all.
//! The codec is sans-io, so a shard is a loop that submits reads, runs the codec
//! and submits writes: one thread, one queue, no scheduler.

use std::fmt::Write as _;
use std::io;
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use io_uring::{cqueue, opcode, squeue, types, IoUring};

use crate::config::Io;
use crate::protocol::{Codec, Session};

/// One provided buffer's size.
/// The same as the per-connection receive buffer this replaced: a pipelined batch of
/// queries fits in one read.
const RECV_BUF: usize = 32 * 1024;

/// Provided buffers in a shard's ring.
#[cfg_attr(test, allow(dead_code))] // test shards use a smaller ring
const PBUF_COUNT: u16 = 256;

/// The most buffers a ring can describe.
const PBUF_MAX: u16 = 1 << 15;

/// `DBNOOP_PBUF` -- or `PGNOOP_PBUF`, which is what it was called before the rename --
/// or the default.
fn pbuf_count_from_env() -> u16 {
    let Some(value) = crate::config::env("PBUF") else {
        return PBUF_COUNT;
    };
    let Some(asked) = value.trim().parse::<u32>().ok() else {
        eprintln!("dbnoop: PBUF={value:?} is not a number of buffers; using {PBUF_COUNT}");
        return PBUF_COUNT;
    };
    if asked > PBUF_MAX as u32 {
        eprintln!(
            "dbnoop: PBUF={asked} is more than the {PBUF_MAX} buffers a ring \
             can address; using {PBUF_MAX}"
        );
        return PBUF_MAX;
    }

    // Zero buffers is no ring at all, which is what the shard's own buffer is for.
    (asked as u16).max(1)
}

/// The buffer group every receive on the shard draws from.
/// One group is enough: the ring is shared by the connections, not owned by them.
const PBUF_GROUP: u16 = 0;

/// Operation kind, packed into the low bits of `user_data`.
/// The cancel is a request of its own -- it names the receive it cancels by THAT
/// request's `user_data`, so it cannot use the same one.
const OP_ACCEPT: u64 = 0;
const OP_RECV: u64 = 1;
const OP_SEND: u64 = 2;
const OP_CANCEL: u64 = 3;

/// How much reply a connection may hold while a write is in flight before the shard
/// stops reading from it.
const STAGED_HIGH_WATER: usize = 256 * 1024;

/// How long a paused accept stays paused after `EMFILE`/`ENFILE`/`ENOMEM`/ `ENOBUFS`.
/// Long enough for a closed connection to release its fd, short enough that a transient
/// shortage costs a fraction of a second.
const ACCEPT_BACKOFF: Duration = crate::config::ACCEPT_BACKOFF;

fn tag(idx: usize, op: u64) -> u64 {
    ((idx as u64) << 2) | op
}

fn untag(data: u64) -> (usize, u64) {
    ((data >> 2) as usize, data & 0b11)
}

/// A completion, kept in the shard's own buffer rather than in a `Vec` built per wait
/// pass.
#[derive(Clone, Copy)]
struct Done {
    data: u64,
    res: i32,
    flags: u32,
}

/// The per-connection output and carry-over buffers, recycled across accepts.
#[derive(Default)]
struct Spare {
    /// A connection's own receive buffer, for the connections that cannot have one of
    /// the shard's.
    recv: Vec<u8>,
    pending: Vec<u8>,
    send: Vec<u8>,
    staged: Vec<u8>,
}

struct Slot {
    fd: RawFd,
    session: Session,
    /// The connection's own receive buffer, used when the shard's ring has no buffer to
    /// lend it.
    recv: Vec<u8>,
    /// Bytes read but not yet forming a complete message.
    pending: Vec<u8>,
    /// The reply being written, or the next one to write.
    send: Vec<u8>,
    /// The reply built while `send` was in flight.
    /// A multishot receive keeps delivering while a send is outstanding, and the bytes
    /// cannot go into a buffer the kernel is reading.
    staged: Vec<u8>,
    /// How much of `send` the kernel has taken.
    sent: usize,
    /// A send is in flight.
    sending: bool,
    /// A receive is in flight.
    /// At most one, ever: a second one would race the first for the same connection's
    /// bytes.
    recv_armed: bool,
    /// The previous receive found no buffer in the ring, so the next one reads into
    /// this connection's own buffer.
    private: bool,
    /// The read side is parked on purpose: the connection is holding more reply than
    /// `STAGED_HIGH_WATER` while a write is in flight, so no receive is armed until
    /// that write has drained.
    parked: bool,
    /// How much reply this connection may hold before the read side is parked:
    /// `STAGED_HIGH_WATER`, except in the test that turns it down to reach the mark in
    /// a read rather than in a run.
    water: usize,
    /// The slot is on its way out and must not arm anything new.
    closing: bool,
    /// When this connection stops being allowed to sit in startup.
    /// `None` once startup has completed or a timer has already retired it.
    startup_deadline: Option<Instant>,
}

impl Slot {
    fn new(
        fd: RawFd,
        startup_deadline: Option<Instant>,
        spare: Spare,
        codec: &Codec,
        water: usize,
    ) -> Self {
        let mut slot = Slot {
            fd,
            session: codec.session(),
            recv: spare.recv,
            pending: spare.pending,
            send: spare.send,
            staged: spare.staged,
            sent: 0,
            sending: false,
            recv_armed: false,
            private: false,
            parked: false,
            water,
            closing: false,
            startup_deadline,
        };

        // A recycled connection starts with nothing in it.
        slot.pending.clear();
        slot.send.clear();
        slot.staged.clear();
        slot.sent = 0;

        slot
    }

    /// The reply this connection is holding: the part of `send` the kernel has not
    /// taken, plus everything staged behind it.
    fn queued(&self) -> usize {
        (self.send.len() - self.sent) + self.staged.len()
    }
}

/// What a connection needs next, given what is in flight.
fn next_for(slot: &mut Slot) -> Next {
    if !slot.sending && slot.send.is_empty() && !slot.staged.is_empty() {
        std::mem::swap(&mut slot.send, &mut slot.staged);
        slot.staged.clear();
        slot.sent = 0;
    }

    if !slot.send.is_empty() && !slot.sending {
        return Next::Send;
    }
    if slot.closing || slot.session.is_closed() {
        return Next::Close;
    }
    if slot.parked {
        // The read side stays off until the reply it outran has gone out -- including
        // the one still staged, which the two checks above have just taken into `send`.
        if slot.queued() > slot.water {
            return Next::Idle;
        }
        slot.parked = false;
    }
    // Checked before the arm below, and not only while a write is in flight: a read
    // that arrives while the connection is already this far behind is exactly the read
    // that must not happen, whether or not a receive is armed for it.
    if slot.queued() >= slot.water {
        return Next::Park;
    }
    if !slot.recv_armed {
        return Next::Arm;
    }

    Next::Idle
}

/// What a shard is allowed to ask the kernel for, and what it was refused.
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
    /// `IORING_SETUP_CQSIZE`.
    cqsize: bool,
}

/// One setup flag, named the way `include/uapi/linux/io_uring.h` names it so the report
/// reads like `io_uring_setup(2)`.
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
    /// Every flag the kernel would not take, with the errno it answered.
    /// An EINVAL here is "dropped after EINVAL": the flag is not *refused by name*,
    /// because `io_uring_setup` does not say which flag it disliked.
    dropped: Vec<(&'static str, i32)>,
    /// The ring's own fd is registered, so an enter does not look it up.
    ring_fd: bool,
    /// The provided-buffer ring registered, and whether it consumes buffers
    /// incrementally.
    pbuf: Option<bool>,
    /// Why the provided-buffer ring is absent, when it is.
    pbuf_errno: Option<i32>,
    /// Set when this process cannot create a ring at all, with the errno that says why.
    /// `probe` turns it back into the error `main` falls back on.
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
        // SQPOLL is asked for out of band, so silence about it is ambiguous: say it was
        // never asked for, rather than let a reader infer it from a missing name.
        if !self.granted.contains(&"SQPOLL")
            && !self.dropped.iter().any(|(name, _)| *name == "SQPOLL")
        {
            s.push_str(" (SQPOLL not requested)");
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
/// Asked once before any shard starts: the answer is the process's, not a shard's.
pub fn probe() -> io::Result<()> {
    let p = plan();
    if let Some(errno) = p.absent {
        // No ring at all: the answer `main` needs to serve with epoll instead.
        return Err(io::Error::from_raw_os_error(errno));
    }
    eprintln!("dbnoop: io_uring {}", p.describe());
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
/// `io_uring_setup` answers EINVAL for the set, so one at a time is what makes a
/// refusal attributable to a flag.
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
        // DEFER_TASKRUN needs SINGLE_ISSUER, which the ladder has already tried by the
        // time it gets here; DBNOOP_NO_DEFER is the operator asking for the ring
        // without it.
        if flag == Flag::DeferTaskrun && !defer {
            continue;
        }
        // TASKRUN_FLAG is the task-work notification bit, and the kernel rejects it
        // unless task work is actually deferred to this thread -- which only
        // COOP_TASKRUN or DEFER_TASKRUN do.
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

/// Work out what this process's rings can do.
/// Never fails: a process with no ring at all records why, so `probe` can report it and
/// `main` can serve with epoll.
fn compute_plan() -> Plan {
    let defer = crate::config::env("NO_DEFER").is_none();
    let sqpoll = crate::config::env("SQPOLL").and_then(|v| v.parse::<u32>().ok());

    // The completion queue is four times the submission capacity, so a burst is
    // drained rather than pushed into the kernel's overflow list.
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

    // SQPOLL: a kernel thread drains the submission queue, so a submit becomes a memory
    // write and `io_uring_enter` leaves the hot path.
    // Unprivileged since Linux 5.11.
    if let Some(idle) = sqpoll {
        let mut sq = Setup {
            sqpoll: Some(idle),
            ..Setup::default()
        };
        match build(&sq, ENTRIES) {
            Ok(ring) => {
                drop(ring);
                // SQPOLL is a kernel thread doing the submission; the single-issuer and
                // taskrun flags are about the submitting *task*, so they do not apply
                // to it.
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
                    "dbnoop: SQPOLL unavailable ({}); falling back to the plain ring",
                    errno_name(errno)
                );
            }
        }
    }

    // The features that are not setup flags are probed on a ring of their own, because
    // they are registration calls rather than `io_uring_setup` bits.
    let ring = match build(&plan.setup, ENTRIES) {
        Ok(r) => r,
        Err(e) => {
            // The ladder said this set was buildable; if it is not now, say so rather
            // than quietly serving on a different ring.
            eprintln!(
                "dbnoop: io_uring refused the probed setup ({}); using a plain ring",
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
                // Linux 5.18.
                // Without it every enter passes the raw fd, which the kernel then has
                // to look up in the caller's file table.
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
                    "dbnoop: provided buffers unavailable ({}); each connection \
                     keeps its own receive buffer",
                    errno_name(errno)
                );
            }
        }
        let _ = submit.unregister_ring_fd();
    }

    plan
}

/// Whether this process can still afford `IORING_SETUP_CQSIZE`.
/// Set when a shard's ring ran out of memory with it, so the shards that start later do
/// not each discover the same thing.
static CQSIZE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// The submission slots a shard's ring is built for.
/// Sizing the completion queue from it is what `CQSIZE` below is about.
const ENTRIES: u32 = 4096;

/// Everything a shard owns besides its ring: the connections on its slots, the
/// slots that came free, the buffers to hand the next connection, and the
/// provided-buffer ring they come from.
#[derive(Default)]
struct Shard {
    slots: Vec<Option<Slot>>,
    free: Vec<usize>,
    spare: Vec<Spare>,
    bufs: Option<BufRing>,
}

impl Ring<'_> {
    /// Act on what a slot needs next: send, close, arm, park, or nothing.
    ///
    /// One place for the decision's consequences, so a completion and a deadline
    /// cannot act on the same `Next` differently.
    fn apply(
        &mut self,
        idx: usize,
        next: Next,
        shard: &mut Shard,
        multishot: bool,
    ) -> io::Result<()> {
        match next {
            Next::Send => submit_send(self, &mut shard.slots, idx)?,
            Next::Close => begin_close(
                &mut shard.slots,
                &mut shard.free,
                &mut shard.spare,
                shard.bufs.as_mut(),
                idx,
            ),
            Next::Arm => arm_recv(self, &mut shard.slots, shard.bufs.as_ref(), idx, multishot)?,
            Next::Park => park_recv(self, &mut shard.slots, idx)?,
            Next::Idle => {}
        }

        Ok(())
    }
}

/// A shard's ring, with the submission and completion queues taken apart once.
struct Ring<'a> {
    submit: io_uring::Submitter<'a>,
    sq: squeue::SubmissionQueue<'a>,
    cq: cqueue::CompletionQueue<'a>,
    /// This pass's completions, reused.
    /// A `Vec` of completions allocated per wait pass is one allocation per batch of
    /// work the shard does.
    pass: Pass,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
}

/// The completions one pass of the loop is responsible for.
/// Drained at the start of the pass and cleared at the END of it -- never at the start.
#[derive(Default)]
struct Pass {
    done: Vec<Done>,
    /// How many of `done` this pass has handed out.
    next: usize,
}

impl Pass {
    /// Take everything the kernel has completed so far.
    fn drain(&mut self, cq: &mut cqueue::CompletionQueue<'_>) {
        drain_completions(cq, &mut self.done);
    }

    /// The next completion of this pass, including one that arrived while the pass was
    /// working.
    fn next(&mut self) -> Option<Done> {
        let d = self.done.get(self.next).copied()?;
        self.next += 1;

        Some(d)
    }

    /// The pass is over: everything it was given has been handed out.
    fn finish(&mut self) {
        debug_assert_eq!(
            self.next,
            self.done.len(),
            "a pass ended with {} completions unprocessed",
            self.done.len() - self.next
        );
        self.done.clear();
        self.next = 0;
    }
}

impl Ring<'_> {
    /// Push one entry, making room by submitting when the queue is full.
    /// A transient failure of that submit is not the shard's death.
    unsafe fn push(&mut self, e: &squeue::Entry) -> io::Result<()> {
        loop {
            // SAFETY: the caller's contract, forwarded.
            if unsafe { self.sq.push(e) }.is_ok() {
                self.sq.sync();
                return Ok(());
            }
            // Full: make room.
            // If THAT fails there is nothing left to try.
            self.submit()?;
        }
    }

    /// Submit what is queued, retrying what the ring says is transient.
    fn submit(&mut self) -> io::Result<()> {
        self.sq.sync();
        // Disjoint borrows: the submit needs the ring, the drain needs the completion
        // queue and the buffer the completions go into.
        let this = &mut *self;
        retry_with(
            || this.submit.submit().map(drop),
            || this.pass.drain(&mut this.cq),
        )
    }

    /// Move everything the kernel has completed into this pass.
    fn drain(&mut self) {
        self.pass.drain(&mut self.cq);
    }

    /// This pass is over: everything it was given has been processed.
    fn finish(&mut self) {
        self.pass.finish();
    }

    /// Wait for one completion, or until `deadline`.
    fn wait(&mut self, deadline: Option<Instant>) -> io::Result<()> {
        self.sq.sync();
        wait_for_work(&self.submit, deadline)
    }
}

/// What one shard's attempt at the ring came to.
pub enum Attempt {
    /// The shard served on the ring until it was asked to stop (a test) or its listener
    /// failed.
    Served,
    /// This shard could not have a ring.
    NoRing(TcpListener),
}

/// Serve one shard on the ring, or hand its listener back when no ring can be had.
pub fn try_run(
    listener: TcpListener,
    codec: Codec,
    entries: u32,
    buffers: u16,
    report: Option<std::sync::mpsc::Sender<crate::protocol::Report>>,
) -> io::Result<Attempt> {
    run_until(
        listener,
        codec,
        Tuning {
            entries,
            pbuf_count: buffers.min(Tuning::default().pbuf_count),
            startup_timeout: crate::config::startup_timeout(),
            report,
            ..Tuning::default()
        },
    )
}

/// `DBNOOP_FAIL_RING=<n>`: the next `n` shards are refused a ring with the errno a user
/// out of ring memory gets.
#[cfg_attr(test, allow(dead_code))] // a test forces the failure through `Tuning`
fn injected_ring_failure() -> Option<io::Error> {
    /// How many more shards to refuse.
    static LEFT: std::sync::atomic::AtomicIsize = std::sync::atomic::AtomicIsize::new(0);
    static ONCE: OnceLock<Option<i64>> = OnceLock::new();

    // Seeded here, once: the countdown is what the shards share.
    let left = ONCE.get_or_init(|| {
        let left = crate::config::env("FAIL_RING").and_then(|v| v.trim().parse::<i64>().ok())?;
        LEFT.store(left as isize, Ordering::Relaxed);

        Some(left)
    });
    let left = (*left)?;
    if left <= 0 {
        return None;
    }
    // Count down once per shard: the first `left` shards are refused, the rest behave
    // as they would on a box with room.
    if LEFT.fetch_sub(1, Ordering::Relaxed) < 1 {
        return None;
    }

    Some(io::Error::from_raw_os_error(libc::ENOMEM))
}

/// What a shard is run with.
#[derive(Clone)]
struct Tuning {
    entries: u32,
    /// `None` disables the startup timeout -- right for a server whose operator asked
    /// for that, and for a test that is not about it.
    startup_timeout: Option<Duration>,
    pbuf_count: u16,
    /// How much staged reply a connection may hold before the read side is parked:
    /// `STAGED_HIGH_WATER`, or less in the test that reaches the mark in a read.
    water: usize,
    /// A test's way of asking the shard to stop, so the test can give its ring back: a
    /// test binary runs a dozen shards, and a ring is kernel memory with a process-wide
    /// budget.
    #[cfg(test)]
    stop: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
    /// Set once the shard's ring exists: a test waiting for a shard learns whether it
    /// is serving or whether this box refused it a ring.
    #[cfg(test)]
    up: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    /// Refuse this shard a ring, so the fallback to epoll can be proven on a box that
    /// has ring memory to spare.
    #[cfg(test)]
    fail_ring: bool,
    /// Where to say "this shard is serving on the ring", before it serves.
    /// The process's banner is built from these; a shard that dies instead of reporting
    /// is a shard the banner does not count.
    report: Option<std::sync::mpsc::Sender<crate::protocol::Report>>,
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            // A test process runs a dozen shards at once; the production ring is 4096
            // submission entries and a completion queue four times that, which is a
            // third of a megabyte of kernel memory per shard.
            #[cfg(not(test))]
            entries: ENTRIES,
            #[cfg(test)]
            entries: 256,
            startup_timeout: None,
            // A dozen test shards in one process; the production ring is 256 buffers, 8
            // MiB of mapping each.
            #[cfg(not(test))]
            pbuf_count: pbuf_count_from_env(),
            // A test process's rings share one mlock budget.
            #[cfg(test)]
            pbuf_count: 8,
            water: STAGED_HIGH_WATER,
            #[cfg(test)]
            stop: None,
            #[cfg(test)]
            stats: None,
            #[cfg(test)]
            up: None,
            #[cfg(test)]
            fail_ring: false,
            report: None,
        }
    }
}

/// Counters a test reads to see what the ring did.
/// Compiled only into test builds: a shard in production pays nothing for them.
#[cfg(test)]
#[derive(Default)]
struct Stats {
    /// Buffers currently in the ring, and buffers the kernel is holding.
    free: std::sync::atomic::AtomicU16,
    held: std::sync::atomic::AtomicU16,
    /// Receives that found no buffer, and so read into the connection's own.
    enobufs: std::sync::atomic::AtomicUsize,
    private_arms: std::sync::atomic::AtomicUsize,
    /// Receives actually armed, and arm attempts skipped because the connection already
    /// had one in flight.
    arms: std::sync::atomic::AtomicUsize,
    elided: std::sync::atomic::AtomicUsize,
    /// Receives armed as `RecvMulti`: one request the kernel re-issues by itself,
    /// against one request per read for a one-shot receive.
    multi_arms: std::sync::atomic::AtomicUsize,
    /// Receives completed.
    /// A shard whose `reads` run far above its `multi_arms` is one where the kernel is
    /// re-issuing the request, which is the whole point of arming it as multishot.
    reads: std::sync::atomic::AtomicUsize,
    /// Whether the shard registered a provided-buffer ring.
    provided: std::sync::atomic::AtomicBool,
    /// Reads stopped because the connection was holding more reply than
    /// `STAGED_HIGH_WATER`, and the most reply a connection was ever holding.
    parks: std::sync::atomic::AtomicUsize,
    staged_high: std::sync::atomic::AtomicUsize,
}

/// `run`, with the shard's knobs supplied rather than read from the process.
fn run_until(listener: TcpListener, codec: Codec, tuning: Tuning) -> io::Result<Attempt> {
    // Slot 0 is reserved for the accept operation so an index is never ambiguous.
    let mut shard = Shard {
        slots: vec![None],
        ..Shard::default()
    };

    let outcome = shard.serve(listener, codec, tuning);

    match finish(outcome, &mut shard.slots)? {
        Passed::NoRing(listener) => Ok(Attempt::NoRing(listener)),
        Passed::Served => Ok(Attempt::Served),
    }
}

/// What the event loop did.
enum Passed {
    /// The loop ended because a test asked it to; the server's loop never returns.
    #[cfg_attr(not(test), allow(dead_code))]
    Served,
    /// No ring could be had, and the listener was never used: it goes back to the
    /// caller.
    NoRing(TcpListener),
}

/// Say it once, and say what it costs: a shard serving on epoll is slower, and a
/// process that has many of them is a process whose ring budget was exhausted.
fn report_no_ring(e: &io::Error) {
    static SAID: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if SAID.swap(true, Ordering::Relaxed) {
        return;
    }

    eprintln!(
        "dbnoop: no io_uring ring for this shard ({}); shards without one serve on epoll. \
         Ring memory is charged to the user, so fewer shards have a ring than the process \
         has shards: DBNOOP_PBUF, or more memlock, buys more.",
        errno_name(e.raw_os_error().unwrap_or(0))
    );
}

/// Should this shard be refused a ring?
fn ring_refused(tuning: &Tuning) -> bool {
    #[cfg(test)]
    {
        tuning.fail_ring
    }
    #[cfg(not(test))]
    {
        let _ = tuning;

        injected_ring_failure().is_some()
    }
}

/// The loop's outcome, with the shard's descriptors closed whatever it was.
/// Closed on the way out, error or not.
fn finish(outcome: io::Result<Passed>, slots: &mut [Option<Slot>]) -> io::Result<Passed> {
    close_all(slots);

    outcome
}

/// Build this shard's ring, walking down to a plain one when the kernel refuses what
/// was probed.
/// `DBNOOP_FAIL_RING` short-circuits it, so the fallback to epoll can be tested.
fn ring_for_shard(setup: &Setup, tuning_entries: u32, refuse: bool) -> io::Result<IoUring> {
    let setup = *setup;
    if refuse {
        return Err(io::Error::from_raw_os_error(libc::ENOMEM));
    }

    let ioring = match build(&setup, tuning_entries) {
        Ok(r) => r,
        // A ring's memory is not free.
        Err(e) if setup.cqsize && e.raw_os_error() == Some(libc::ENOMEM) => {
            CQSIZE.store(false, Ordering::Relaxed);
            let smaller = Setup {
                cqsize: false,
                ..setup
            };
            eprintln!(
                "dbnoop: io_uring ring ran out of memory with CQSIZE ({}); \
                 shards keep the kernel's own completion queue",
                errno_name(libc::ENOMEM)
            );
            match build(&smaller, tuning_entries) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "dbnoop: io_uring refused the probed setup ({}); serving on a plain ring",
                        errno_name(e.raw_os_error().unwrap_or(0))
                    );
                    build(&Setup::default(), tuning_entries)?
                }
            }
        }
        Err(e) => {
            eprintln!(
                "dbnoop: io_uring refused the probed setup ({}); serving on a plain ring",
                errno_name(e.raw_os_error().unwrap_or(0))
            );
            build(&Setup::default(), tuning_entries)?
        }
    };

    Ok(ioring)
}

/// One shard's event loop, until the listener fails or a test asks it to stop.
impl Shard {
    /// One shard's event loop, until the listener fails or a test asks it to stop.
    fn serve(&mut self, listener: TcpListener, codec: Codec, tuning: Tuning) -> io::Result<Passed> {
        let plan = plan();
        let setup = Setup {
            cqsize: plan.setup.cqsize && CQSIZE.load(Ordering::Relaxed),
            ..plan.setup
        };
        let mut ioring = match ring_for_shard(&setup, tuning.entries, ring_refused(&tuning)) {
            Ok(r) => r,
            // A shard that cannot have a ring must not die.
            // Memory for rings is charged to the user, so a process with more shards than
            // its budget allows has shards that cannot have one.
            Err(e) => {
                report_no_ring(&e);

                return Ok(Passed::NoRing(listener));
            }
        };

        let (mut submit, sq, cq) = ioring.split();

        // Registration before the loop, on the very `Submitter` that will enter: the
        // registered index is remembered by the `Submitter`, so registering on any other
        // one would register nothing at all.
        if plan.ring_fd {
            if let Err(e) = submit.register_ring_fd() {
                eprintln!(
                    "dbnoop: ring fd registration failed ({}); enters keep passing the raw fd",
                    errno_name(e.raw_os_error().unwrap_or(0))
                );
            }
        }

        self.bufs = match plan.pbuf {
            Some(_granted_incremental) => register_buffers(
                &submit,
                tuning.pbuf_count,
                #[cfg(test)]
                tuning.stats.clone(),
            ),
            None => None,
        };

        // Reported after the registration, not after the ring.
        if let Some(report) = &tuning.report {
            let _ = report.send(crate::protocol::Report::Serving {
                protocol: codec.protocol(),
                backend: Io::Uring,
            });
        }

        #[cfg(test)]
        if let Some(s) = &tuning.stats {
            s.provided.store(self.bufs.is_some(), Ordering::Relaxed);
        }
        #[cfg(test)]
        if let Some(up) = &tuning.up {
            up.store(true, Ordering::SeqCst);
        }

        let mut ring = Ring {
            submit,
            sq,
            cq,
            pass: Pass {
                done: Vec::with_capacity(1024),
                next: 0,
            },
            #[cfg(test)]
            stats: tuning.stats.clone(),
        };

        let listen_fd = listener.as_raw_fd();

        // Accept: one multishot request, re-armed only when the kernel says it is no longer
        // holding it (`F_MORE` clear), and not at all while paused.
        let mut accept_armed = false;
        let mut accept_paused_until: Option<Instant> = None;

        // The connections in startup, as a list rather than a scan.
        let mut startup: Vec<usize> = Vec::new();
        let mut earliest_startup: Option<Instant> = None;

        // Slow connections, and the ring itself, come back to in a moment.

        // Multishot receive is discovered from the kernel's answer, not from a version
        // number: a kernel that does not know `IORING_RECV_MULTISHOT` answers the first
        // receive with EINVAL, and every receive after that is a one-shot into a provided
        let mut multishot = self.bufs.is_some();
        let mut scratch: Vec<usize> = Vec::new();

        arm_accept(&mut ring, listen_fd, &mut accept_armed)?;

        loop {
            // A test asks the shard to stop so that its ring goes back to the process;
            // nothing in the server sets this.
            #[cfg(test)]
            if let Some(stop) = &tuning.stop {
                if stop.load(Ordering::Relaxed) {
                    return Ok(Passed::Served);
                }
            }

            // Wait for a completion, or for the earliest deadline.
            // Two obligations of this shard are invisible to a completion: a paused accept,
            // and a connection that has not completed startup.
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

            // A deadline's arrival is read from the clock, never from the wait.
            let expired = deadline.is_some_and(|at| Instant::now() >= at);

            ring.drain();

            // By hand, not by iterator: a submission made while the pass is working can
            // make the kernel answer at once (EBUSY, drained into the pass), and those
            // completions belong to this pass.
            let mut expiry_done = !expired;
            loop {
                while let Some(Done { data, res, flags }) = ring.pass.next() {
                    let (idx, op) = untag(data);

                    match op {
                        OP_ACCEPT => {
                            accept_armed = cqueue::more(flags);

                            if res >= 0 {
                                let fd = res as RawFd;
                                // Nagle off: replies are tens of bytes and latency is what
                                // a benchmark reads.
                                set_nodelay(fd);

                                let admitted = tuning.startup_timeout.map(|t| Instant::now() + t);
                                let recycled = self.spare.pop().unwrap_or_default();
                                let idx = match self.free.pop() {
                                    Some(i) => {
                                        self.slots[i] = Some(Slot::new(
                                            fd,
                                            admitted,
                                            recycled,
                                            &codec,
                                            tuning.water,
                                        ));
                                        i
                                    }
                                    None => {
                                        self.slots.push(Some(Slot::new(
                                            fd,
                                            admitted,
                                            recycled,
                                            &codec,
                                            tuning.water,
                                        )));
                                        self.slots.len() - 1
                                    }
                                };
                                // MySQL speaks first.
                                if let Some(prelude) = codec.prelude() {
                                    if let Some(slot) = self.slots[idx].as_mut() {
                                        slot.send.extend_from_slice(prelude);
                                    }
                                    submit_send(&mut ring, &mut self.slots, idx)?;
                                }
                                // Only a connection with a deadline belongs in the list the
                                // expiry pass walks; without one it is an entry the pass
                                // would drop for nothing.
                                if let Some(at) = admitted {
                                    startup.push(idx);
                                    earliest_startup =
                                        Some(earliest_startup.map_or(at, |e| e.min(at)));
                                }
                                arm_recv(
                                    &mut ring,
                                    &mut self.slots,
                                    self.bufs.as_ref(),
                                    idx,
                                    multishot,
                                )?;
                            } else {
                                let err = -res;
                                if accept_should_pause(err) {
                                    // Back off the ACCEPT, not the shard.
                                    accept_paused_until = Some(Instant::now() + ACCEPT_BACKOFF);
                                    eprintln!(
                                        "dbnoop: accept failed with errno {err}; \
                                 not re-arming accept for {}ms (other work continues)",
                                        ACCEPT_BACKOFF.as_millis()
                                    );
                                } else if err == libc::EBADF || err == libc::EINVAL {
                                    return Err(io::Error::from_raw_os_error(err));
                                }
                            }

                            // Only re-arm when the kernel is not still holding the request
                            // and the pause has expired.
                            let paused = accept_paused_until.is_some_and(|t| Instant::now() < t);
                            if !accept_armed && !paused {
                                accept_paused_until = None;
                                arm_accept(&mut ring, listen_fd, &mut accept_armed)?;
                            }
                        }

                        OP_RECV => {
                            let more = cqueue::more(flags);
                            let bid = cqueue::buffer_select(flags);

                            // The buffer is the shard's, not the connection's.
                            let Some(slot) = self.slots.get_mut(idx).and_then(|s| s.as_mut())
                            else {
                                if let (Some(bid), Some(b)) = (bid, self.bufs.as_mut()) {
                                    b.put(bid);
                                }
                                continue;
                            };
                            slot.recv_armed = more;
                            #[cfg(test)]
                            if res > 0 {
                                if let Some(s) = &tuning.stats {
                                    s.reads.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            if res > 0 {
                                // The connection is reading again, whatever it had to do
                                // last time.
                                slot.private = false;
                            }

                            // Where the bytes are, and which buffer must go back.
                            let mut release: Option<u16> = None;
                            let mut src: *const u8 = std::ptr::null();
                            let mut n = 0usize;

                            if let (Some(bid), Some(b)) = (bid, self.bufs.as_mut()) {
                                // A one-shot descriptor is the whole of one read: the bytes
                                // start at the buffer and the buffer goes back as soon as
                                // the codec has seen them.
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
                                if let (Some(bid), Some(b)) = (release, self.bufs.as_mut()) {
                                    b.put(bid);
                                }

                                let next = match res {
                                    // ENOBUFS: every buffer in the ring was taken when the
                                    // data arrived.
                                    r if r == -libc::ENOBUFS => {
                                        #[cfg(test)]
                                        if let Some(s) = &tuning.stats {
                                            s.enobufs.fetch_add(1, Ordering::Relaxed);
                                        }
                                        slot.private = true;
                                        Next::Arm
                                    }
                                    // Cancelled: by a park of ours, whose own completion is
                                    // what resumes the read side, or by a slot on its way
                                    // out.
                                    r if r == -libc::ECANCELED => {
                                        after_cancel(slot.parked, slot.closing)
                                    }
                                    // A kernel that does not know multishot receive (or a
                                    // provided buffer selected by the kernel) says so here,
                                    // once.
                                    r if (r == -libc::EINVAL || r == -libc::EOPNOTSUPP)
                                        && multishot =>
                                    {
                                        multishot = false;
                                        slot.private = true;
                                        eprintln!(
                                            "dbnoop: multishot receive unavailable ({}); \
                                     one receive per read instead",
                                            errno_name(-r)
                                        );
                                        Next::Arm
                                    }
                                    // Zero is the peer closing; any other error is the
                                    // connection ending.
                                    // Both close it.
                                    _ => Next::Close,
                                };

                                ring.apply(idx, next, self, multishot)?;
                                continue;
                            }

                            // The reply goes into `send` unless the kernel is still reading
                            // `send`, in which case it goes into `staged`: a multishot
                            // receive delivers while a send is outstanding.
                            if slot.send.capacity() == 0 {
                                slot.send.reserve(8 * 1024);
                            }
                            let out = if slot.sending {
                                &mut slot.staged
                            } else {
                                &mut slot.send
                            };

                            // SAFETY: `src` points into a provided buffer this thread just
                            // took out of the ring, or into the slot's own receive buffer,
                            // and `n` is the byte count the kernel wrote there.
                            let bytes = unsafe { std::slice::from_raw_parts(src, n) };

                            crate::protocol::feed(
                                &codec,
                                &mut slot.session,
                                &mut slot.pending,
                                bytes,
                                out,
                            );

                            // Only now: the codec has read what the buffer held.
                            if let (Some(bid), Some(b)) = (release, self.bufs.as_mut()) {
                                b.put(bid);
                            }

                            // Startup completed: this connection is no longer the one the
                            // startup deadline is about.
                            if !slot.session.awaiting_startup() {
                                slot.startup_deadline = None;
                            }

                            #[cfg(test)]
                            if let Some(s) = &tuning.stats {
                                s.staged_high.fetch_max(slot.queued(), Ordering::Relaxed);
                            }

                            let next = next_for(slot);
                            ring.apply(idx, next, self, multishot)?;
                        }

                        OP_SEND => {
                            let next = {
                                let Some(slot) = self.slots.get_mut(idx).and_then(|s| s.as_mut())
                                else {
                                    continue;
                                };
                                slot.sending = false;

                                if res <= 0 {
                                    Next::Close
                                } else {
                                    slot.sent += res as usize;

                                    if slot.sent < slot.send.len() {
                                        // A short write is normal under pressure; finish
                                        // it.
                                        Next::Send
                                    } else {
                                        slot.send.clear();
                                        slot.sent = 0;

                                        next_for(slot)
                                    }
                                }
                            };
                            ring.apply(idx, next, self, multishot)?;
                        }

                        OP_CANCEL => {
                            let next = {
                                let Some(slot) = self.slots.get_mut(idx).and_then(|s| s.as_mut())
                                else {
                                    continue;
                                };
                                after_cancel_request(slot, res)
                            };
                            ring.apply(idx, next, self, multishot)?;
                        }

                        _ => {
                            // Nothing else is ever submitted with a tag of its own.
                        }
                    }
                }

                // The deadlines, once per pass, after the completions it began with.
                // Only reached when at least one was pending; with none, `expired` is false
                // and this pass is over.
                if expiry_done {
                    break;
                }
                expiry_done = true;

                // A paused accept whose backoff has passed resumes here.
                // Checked against the clock rather than assumed: the wait can also have
                // been bounded by another deadline and returned early.
                if let Some(t) = accept_paused_until {
                    if Instant::now() >= t {
                        accept_paused_until = None;
                        if !accept_armed {
                            arm_accept(&mut ring, listen_fd, &mut accept_armed)?;
                        }
                    }
                }

                // Connections still in startup whose deadline has passed, walked as the
                // list of connections that are in startup rather than as every slot on the
                // shard.
                let now = Instant::now();
                let mut next: Option<Instant> = None;
                scratch.clear();
                for idx in startup.drain(..) {
                    let deadline = self
                        .slots
                        .get_mut(idx)
                        .and_then(|s| s.as_mut())
                        .and_then(|slot| slot.startup_deadline);
                    let Some(at) = deadline else {
                        // The slot is gone, or its connection finished startup: either way
                        // this entry has no deadline to speak for.
                        continue;
                    };
                    if at > now {
                        next = Some(next.map_or(at, |n| n.min(at)));
                        scratch.push(idx);
                        continue;
                    }

                    if let Some(slot) = self.slots.get_mut(idx).and_then(|s| s.as_mut()) {
                        if slot.session.awaiting_startup() {
                            eprintln!(
                                "dbnoop: connection did not complete startup within {}ms; closing",
                                tuning.startup_timeout.map_or(0, |t| t.as_millis())
                            );
                        }
                        slot.startup_deadline = None;
                    }

                    // `shutdown`, not `close`: a receive in flight may not be freed, and
                    // shutting down makes it return 0 so the ordinary path reaps it.
                    ring.apply(idx, Next::Close, self, multishot)?;
                }
                std::mem::swap(&mut startup, &mut scratch);
                earliest_startup = next;
            }

            // Every completion this pass was given has been processed -- including the ones
            // that arrived while it was working.
            // Only now may they be dropped.
            ring.finish();
        }
    }
}

/// Close every descriptor a stopped shard still holds.
fn close_all(slots: &mut [Option<Slot>]) {
    for slot in slots.iter_mut().flatten() {
        // SAFETY: the ring that could still have named this fd is closed, and the slot
        // is dropped immediately after.
        unsafe { libc::close(slot.fd) };
    }
}

/// Move every completion the kernel has posted into this pass's buffer.
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

/// Wait for one completion, or until `deadline`, without ever ending the shard over
/// something that is not a failure.
/// EINTR is not a shard's death.
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
#[derive(Debug, PartialEq, Eq)]
enum Retry {
    /// The kernel was interrupted before it could do anything.
    /// Nothing about the ring changed, so the same operation is submitted again.
    Again,
    /// The completions the operation was for are already here: return to the loop and
    /// drain them.
    Drain,
    /// The ring itself is failing, and the shard cannot go on.
    Stop,
}

fn retry_after(e: &io::Error) -> Retry {
    match e.raw_os_error() {
        // Some kernels report the interruption as a bare errno rather than letting it
        // reach `ErrorKind::Interrupted`.
        Some(libc::EINTR) => Retry::Again,
        // The completion ring is full with overflow entries the kernel could not flush:
        // there is nothing to wait for, only something to reap.
        Some(libc::EBUSY) => Retry::Drain,
        _ if e.kind() == io::ErrorKind::Interrupted => Retry::Again,
        _ => Retry::Stop,
    }
}

/// Nagle off: replies are tens of bytes and latency is what a benchmark reads.
fn set_nodelay(fd: RawFd) {
    let one: libc::c_int = 1;
    // SAFETY: a plain setsockopt on a socket this shard just accepted.
    // A failure leaves the socket as it was, which is why it is not checked.
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

/// What the CANCEL REQUEST's own completion does to the slot it cancelled: nothing to
/// `recv_armed`, and then the usual decision.
fn after_cancel_request(slot: &mut Slot, _res: i32) -> Next {
    // Nothing about the receive changes here, whatever the cancel answered: `0` means
    // this cancel took it, and the cancelled request's own completion -- which always
    // comes -- is what says so.
    next_for(slot)
}

/// What a receive that was cancelled does to its connection.
/// A park of ours: nothing, because the cancel's own completion is what resumes the
/// read side.
fn after_cancel(parked: bool, closing: bool) -> Next {
    if parked {
        Next::Idle
    } else if closing {
        Next::Close
    } else {
        Next::Arm
    }
}

/// What a connection needs next, decided while its slot is borrowed and acted on after
/// the borrow ends -- arming a receive needs the slot table again.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Next {
    Send,
    Close,
    Arm,
    /// Cancel the receive and stop reading until the write drains.
    Park,
    Idle,
}

/// Is this accept errno one that clears with time rather than one that ends the shard?
fn accept_should_pause(errno: i32) -> bool {
    errno == libc::EMFILE
        || errno == libc::ENFILE
        || errno == libc::ENOMEM
        || errno == libc::ENOBUFS
}

/// One multishot accept: the SQE is re-armed only when the kernel says it is no longer
/// holding it, which is what makes this one request per shard instead of one per
/// connection.
fn arm_accept(ring: &mut Ring, listen_fd: RawFd, armed: &mut bool) -> io::Result<()> {
    let e = opcode::AcceptMulti::new(types::Fd(listen_fd))
        .build()
        .user_data(tag(0, OP_ACCEPT));
    // SAFETY: a multishot accept points at no user buffer at all -- the accepted fd is
    // the completion's result.
    unsafe { ring.push(&e) }?;
    *armed = true;
    Ok(())
}

/// Arm this connection's receive.
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
        // The guard, counting itself: a connection that already has a receive in flight
        // does not get a second one.
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
            #[cfg(test)]
            if let Some(s) = &ring.stats {
                s.multi_arms.fetch_add(1, Ordering::Relaxed);
            }
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

    // SAFETY: a provided-buffer receive points at no user buffer (the ring supplies
    // one), and one of the connection's own points at `slot.recv`, whose allocation is
    // stable and outlives the request.
    unsafe { ring.push(&e) }?;
    slot.recv_armed = true;
    #[cfg(test)]
    if let Some(s) = &ring.stats {
        s.arms.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

/// Stop reading from a connection until the reply it has outrun has gone out.
/// A multishot receive cannot simply be "not re-armed": the kernel owns the request and
/// keeps handing data over.
fn park_recv(ring: &mut Ring, slots: &mut [Option<Slot>], idx: usize) -> io::Result<()> {
    let Some(slot) = slots.get_mut(idx).and_then(|s| s.as_mut()) else {
        return Ok(());
    };
    slot.parked = true;
    #[cfg(test)]
    if let Some(s) = &ring.stats {
        s.parks.fetch_add(1, Ordering::Relaxed);
    }
    if !slot.recv_armed {
        // Nothing in flight to cancel; the read side is already off and `next_for` will
        // arm it when the connection drains.
        return Ok(());
    }

    // By `user_data`: a cancel names the request it targets, and the tag is what both a
    // one-shot and a multishot receive carry.
    let e = opcode::AsyncCancel::new(tag(idx, OP_RECV))
        .build()
        .user_data(tag(idx, OP_CANCEL));
    // SAFETY: a cancel points at no user buffer.
    unsafe { ring.push(&e) }
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
        // SAFETY: `sent` only advances when a send completes, so the pointer is into
        // the part of `send` the kernel has not taken.
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
/// A receive in flight cannot be closed out from under the kernel.
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
        // SAFETY: the fd is owned by the slot and is closed only once its receive has
        // completed.
        unsafe { libc::shutdown(slot.fd, libc::SHUT_RDWR) };
        return;
    }
    if slot.sending {
        return;
    }
    reap(slots, free_slots, spare, bufs, idx);
}

/// Close and free a slot, but only once nothing is still pointing at its buffers.
/// A slot freed with an operation in flight is a use-after-free the kernel performs for
/// you.
fn reap(
    slots: &mut [Option<Slot>],
    free_slots: &mut Vec<usize>,
    spare: &mut Vec<Spare>,
    bufs: Option<&mut BufRing>,
    idx: usize,
) {
    let fd = match slots.get_mut(idx).and_then(|s| s.as_mut()) {
        Some(slot) if !slot.recv_armed && !slot.sending => {
            // Every buffer this connection was given has been handed back by the
            // completion that carried it, which is what one-shot provided buffers mean:
            // a slot in flight owns nothing.
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

/// The shard's provided-buffer ring: one registered array of descriptors and one block
/// of buffers, shared by every connection on the shard.
struct BufRing {
    /// The descriptor ring, page aligned as the interface requires, and the buffers it
    /// describes.
    desc: *mut types::BufRingEntry,
    desc_len: usize,
    data: *mut u8,
    data_len: usize,
    count: u16,
    /// Our copy of the ring's tail.
    /// Userspace owns this field; the kernel only reads it.
    tail: u16,
    /// A descriptor the kernel has taken and not yet given back.
    in_use: Vec<bool>,
    /// Descriptors we believe are in the ring.
    free: u16,
    #[cfg(test)]
    stats: Option<std::sync::Arc<Stats>>,
}

impl BufRing {
    /// Does this kernel register a provided-buffer ring, and does it consume buffers
    /// incrementally?
    fn probe(submit: &io_uring::Submitter<'_>) -> io::Result<bool> {
        let one = BufRing::alloc(1)?;
        one.publish();

        // SAFETY: the descriptor ring is page aligned and lives until the registration
        // is undone below.
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

    /// Register the shard's ring.
    /// One-shot, never incremental.
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

        // SAFETY: the descriptor ring outlives the registration -- it is unmapped in
        // `Drop`, after the ring itself is gone.
        unsafe { submit.register_buf_ring_with_flags(b.desc as u64, b.count, PBUF_GROUP, 0) }?;
        Ok(b)
    }

    fn alloc(count: u16) -> io::Result<BufRing> {
        // The ring's index is a mask, so the count is a power of two -- and a count
        // above half the index's range has no power of two to round to.
        let count = count.clamp(1, PBUF_MAX).next_power_of_two();
        let desc_len =
            (count as usize * std::mem::size_of::<types::BufRingEntry>()).next_multiple_of(4096);
        let data_len = (count as usize * RECV_BUF).next_multiple_of(4096);

        // SAFETY: fresh anonymous mappings, owned by this ring alone.
        let desc = unsafe { mmap_anon(desc_len)? } as *mut types::BufRingEntry;
        // SAFETY: as above.
        // A second mapping that fails must not leak the first.
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

    /// Publish the tail.
    /// Release, because the descriptors it publishes must be visible to the kernel
    /// before it reads the tail.
    fn publish(&self) {
        // SAFETY: `disc` is our mapping, page aligned, and its first entry's reserved
        // field is the ring's tail.
        unsafe {
            let tail = types::BufRingEntry::tail(self.desc) as *const AtomicU16;
            (*tail).store(self.tail, Ordering::Release);
        }
    }

    /// Put one descriptor into the ring.
    fn add(&mut self, bid: u16) {
        // SAFETY: `tail & (count - 1)` is inside the mapping, and this thread is the
        // only one that writes descriptors.
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
        // SAFETY: `bid` is a descriptor the ring handed out, so it names one of this
        // ring's RECV_BUF-sized buffers.
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

/// Register the shard's provided-buffer ring, or serve without one.
fn register_buffers(
    submit: &io_uring::Submitter<'_>,
    count: u16,
    #[cfg(test)] stats: Option<std::sync::Arc<Stats>>,
) -> Option<BufRing> {
    match BufRing::register(
        submit,
        count,
        #[cfg(test)]
        stats,
    ) {
        Ok(b) => Some(b),
        Err(e) => {
            eprintln!(
                "dbnoop: provided buffers could not be registered ({}); \
                 each connection keeps its own receive buffer",
                errno_name(e.raw_os_error().unwrap_or(0))
            );
            None
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

/// A page-aligned anonymous mapping.
/// The provided-buffer interface requires the descriptor ring to be page aligned, and
/// the buffers it describes are not allowed to move.
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
    /// pipelined queries each writes per pass.
    const BUSY_CLIENTS: usize = 32;
    const BUSY_BATCH: usize = 2048;

    /// Set in the child process of `an_accept_paused_by_emfile_resumes_...`, which is
    /// this same test binary re-entered with one test selected.
    const EMFILE_CHILD: &str = "PGNOOP_URING_EMFILE_CHILD";

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
    /// `None` when this box cannot create a ring at all -- the condition `probe`
    /// reports and the server falls back to epoll for.
    struct Shard {
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
        _slot: Option<ShardSlot>,
    }

    impl Drop for Shard {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            // The shard's wait is bounded in test builds, so it notices the flag within
            // a pass and gives its ring back here.
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }

    /// How many shards a test process may have alive at once.
    const MAX_LIVE_SHARDS: usize = 3;

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

    fn spawn_shard(tuning: Tuning) -> Option<(SocketAddr, Arc<AtomicI32>, Shard)> {
        spawn_shard_with(crate::protocol::Protocol::Postgres, tuning)
    }

    /// A shard on a fresh loopback port speaking `protocol`.
    fn spawn_shard_with(
        protocol: crate::protocol::Protocol,
        mut tuning: Tuning,
    ) -> Option<(SocketAddr, Arc<AtomicI32>, Shard)> {
        if plan().absent.is_some() {
            return None;
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
        let addr = listener.local_addr().ok()?;
        let tid = Arc::new(AtomicI32::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let up = Arc::new(AtomicBool::new(false));
        let _slot = shard_slot();
        tuning.stop = Some(Arc::clone(&stop));
        tuning.up = Some(Arc::clone(&up));

        let published = Arc::clone(&tid);
        let thread = std::thread::spawn(move || {
            // SAFETY: gettid has no arguments and no failure.
            published.store(
                unsafe { libc::syscall(libc::SYS_gettid) } as i32,
                Ordering::SeqCst,
            );

            let codec = Codec::new(protocol);
            if let Err(e) = run_until(listener, codec, tuning) {
                eprintln!("test shard ended: {e}");
            }
        });

        // A box that refuses a ring right now is not a failing test.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !up.load(Ordering::SeqCst) {
            if thread.is_finished() {
                eprintln!("skipping: this box would not give the shard a ring");

                return None;
            }
            if Instant::now() > deadline {
                panic!("the shard neither served nor ended within 30s");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
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
    fn spawn_busy_client(addr: SocketAddr, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let Ok(mut stream) = TcpStream::connect(addr) else {
                return;
            };
            let _ = stream.set_nodelay(true);

            // The handshake goes out ONCE: a startup message sent again after the first
            // is a malformed query frame, and the codec would close the connection the
            // shard is supposed to be busy with.
            if stream
                .write_all(&crate::protocol::startup_message())
                .is_err()
            {
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
        stream.write_all(&crate::protocol::startup_message())?;
        stream.write_all(&select_1())?;

        let mut buf = [0u8; 256];
        stream.read(&mut buf)
    }

    /// B1: an expired deadline is decided by the clock, not by the wait's return code.
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
        // Long enough for a debug build of the shard to work through the saturated
        // clients' backlog; the property under test is that the deadline is decided by
        // the clock at all, not that it is decided within a millisecond of 300.
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

    /// B1, the accept half: a paused accept resumes when fds come back.
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

        // The kernel may absorb the shortage itself.
        if !stderr.contains(&format!("accept failed with errno {}", libc::EMFILE)) {
            eprintln!(
                "note: this kernel's multishot accept absorbed the fd shortage \
                 (no EMFILE completion); the connection was served once fds returned"
            );
        }
    }

    /// The body of `an_accept_paused_by_emfile_resumes_while_the_shard_is_busy`, run in
    /// the child process where the fd limit can be lowered.
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

    /// A no-op handler, installed WITHOUT `SA_RESTART`, so the syscall it interrupts
    /// returns EINTR instead of being restarted invisibly.
    fn install_noop_handler(signal: libc::c_int) {
        // SAFETY: a zeroed sigaction is a valid empty disposition; the handler is a
        // plain `extern "C" fn`, and handles one `int`.
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

    /// B2: a signal is not a shard's death.
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

            // SAFETY: `tgkill` with a tid this process published for itself, with a
            // handler installed for the signal.
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

    /// B2: a failed ring operation is classified, not fatal by default.
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

    /// A submit's transient failure is not the shard's death either.
    /// `push_or_submit` submits when the submission queue is full, and so does the loop
    /// before it first waits.
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

    /// An accept that runs out of descriptors pauses; one the ring itself refused does
    /// not.
    #[test]
    fn only_a_shortage_pauses_the_accept() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOMEM, libc::ENOBUFS] {
            assert!(accept_should_pause(errno), "errno {errno}");
        }
        for errno in [libc::EBADF, libc::EINVAL, libc::ECONNABORTED, libc::EAGAIN] {
            assert!(!accept_should_pause(errno), "errno {errno}");
        }
    }

    /// The setup ladder attributes its refusals.
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

    /// The report names the granted set, the refusals, and the SQPOLL question: a run
    /// is only reproducible if its log says which ring produced it.
    #[test]
    fn the_report_names_what_was_granted_refused_and_asked_for() {
        let plan = Plan {
            setup: Setup::default(),
            granted: vec!["SINGLE_ISSUER", "COOP_TASKRUN", "DEFER_TASKRUN"],
            dropped: vec![("SUBMIT_ALL", libc::EINVAL)],
            ring_fd: true,
            pbuf: Some(false),
            pbuf_errno: None,
            absent: None,
        };
        let line = plan.describe();
        for name in [
            "SINGLE_ISSUER",
            "COOP_TASKRUN",
            "DEFER_TASKRUN",
            "RING_FD",
            "PBUF_RING",
        ] {
            assert!(line.contains(name), "{name} missing from {line:?}");
        }
        assert!(line.contains("SUBMIT_ALL after EINVAL"), "{line}");
        assert!(line.contains("SQPOLL not requested"), "{line}");

        // Asked for and refused: the refusal is named, and the line does not also claim
        // the flag was never asked for.
        let refused = Plan {
            granted: Vec::new(),
            dropped: vec![("SQPOLL", libc::EPERM)],
            ..plan
        };
        let line = refused.describe();
        assert!(line.contains("dropped: SQPOLL after EPERM"), "{line}");
        assert!(!line.contains("SQPOLL not requested"), "{line}");
    }

    /// `TASKRUN_FLAG` is a notification bit the kernel refuses unless task work is
    /// deferred to this thread, so it is never asked for on its own.
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
            "DBNOOP_NO_DEFER was set"
        );
    }

    /// A query split across two reads is answered once.
    #[test]
    fn a_query_split_across_reads_is_answered_once() {
        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };
        // Without a provided-buffer ring there is no run-ahead to bound.
        // Each receive is then one-shot and armed only after the previous write
        // completed, so the reply cannot pile up and there is no park to see.
        if !has_provided_buffers(&stats) {
            return;
        }

        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_nodelay(true).expect("nodelay");
        s.set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        s.write_all(&crate::protocol::startup_message())
            .expect("startup");
        read_until_ready(&mut s).expect("handshake reply");

        // The halves must arrive in separate receives, and that is what is waited for
        // -- a sleep says the bytes were probably apart, the shard's own read count
        // says they were.
        let before = stats.reads.load(Ordering::Relaxed);
        let q = query_message("select 1, 1, 1");
        s.write_all(&q[..q.len() - 3]).expect("first half");
        let deadline = Instant::now() + Duration::from_secs(5);
        while stats.reads.load(Ordering::Relaxed) == before {
            assert!(
                Instant::now() < deadline,
                "the shard never read the first half of the query"
            );
            std::hint::spin_loop();
        }
        // Asserted before the second half is written: after it, the shard may already
        // have read it, and the count would say two for a reason the test is not about.
        assert!(
            stats.reads.load(Ordering::Relaxed) == before + 1,
            "the first half alone must be one receive"
        );
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

    /// Set in the child process of `a_stopped_shard_closes_its_descriptors`, which is
    /// this same test binary re-entered with one test selected.
    const FD_CHILD: &str = "PGNOOP_URING_FD_CHILD";

    /// Does this shard have a provided-buffer ring?
    /// A box that refuses one serves with per-connection buffers, and a test about the
    /// ring has nothing to test there.
    fn has_provided_buffers(stats: &Stats) -> bool {
        let provided = stats.provided.load(Ordering::Relaxed);
        if !provided {
            eprintln!("skipping: this box would not give the shard a buffer ring");
        }

        provided
    }

    /// This process's open descriptors, as `/proc` counts them.
    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd")
            .map(|d| d.count())
            .unwrap_or(0)
    }

    /// A buffer ring the kernel refuses is not a dead shard.
    #[test]
    fn a_refused_buffer_ring_falls_back_to_per_connection_buffers() {
        let ring = build(&Setup::default(), 64).expect("a plain ring builds");
        let submit = ring.submitter();

        // Occupy the group, so the next registration on it is refused the way a kernel
        // refuses one, and asks the fallback for a ring.
        let first = BufRing::register(&submit, 4, None).expect("the first ring registers");
        assert!(
            register_buffers(&submit, 4, None).is_none(),
            "a refused registration must fall back, not end the shard"
        );
        drop(first);
    }

    /// What `DBNOOP_PBUF` means, including the values an operator types by accident.
    #[test]
    fn the_buffer_ring_size_from_the_environment_is_bounded() {
        std::env::set_var("DBNOOP_PBUF", "40000");
        assert_eq!(pbuf_count_from_env(), PBUF_MAX);
        std::env::set_var("DBNOOP_PBUF", " 16 ");
        assert_eq!(pbuf_count_from_env(), 16);
        std::env::set_var("DBNOOP_PBUF", "0");
        assert_eq!(pbuf_count_from_env(), 1);
        std::env::set_var("DBNOOP_PBUF", "nonsense");
        assert_eq!(pbuf_count_from_env(), PBUF_COUNT);
        std::env::remove_var("DBNOOP_PBUF");
        assert_eq!(pbuf_count_from_env(), PBUF_COUNT);
    }

    /// The ring's index cannot describe every u16 the operator can type.
    #[test]
    fn a_buffer_ring_count_is_clamped_to_what_its_index_can_describe() {
        for (asked, expected) in [
            (0u16, 1u16),
            (1, 1),
            (1000, 1024),
            (PBUF_MAX, PBUF_MAX),
            (PBUF_MAX + 1, PBUF_MAX),
            (u16::MAX, PBUF_MAX),
        ] {
            let ring = BufRing::alloc(asked).expect("a ring is allocated");
            assert_eq!(ring.count, expected, "DBNOOP_PBUF={asked}");
        }
    }

    /// A cancelled receive on a closing slot ends the slot.
    /// `Idle` here left the descriptor open until the process exited: the receive the
    /// slot was waiting for is over, so there is nothing left to wait for.
    #[test]
    fn a_cancelled_receive_is_answered_by_what_the_slot_needs() {
        assert_eq!(
            after_cancel(true, false),
            Next::Idle,
            "a park of ours resumes from the cancel's own completion"
        );
        assert_eq!(
            after_cancel(false, true),
            Next::Close,
            "a closing slot must be reaped, not left holding its descriptor"
        );
        assert_eq!(after_cancel(false, false), Next::Arm);
    }

    /// A stopped shard gives its descriptors back.
    /// Two things were wrong when a shard stopped with connections still open.
    #[test]
    fn a_stopped_shard_closes_its_descriptors() {
        if std::env::var_os(FD_CHILD).is_some() {
            fd_scenario();
            return;
        }

        let exe = std::env::current_exe().expect("the test binary's path");
        let child = std::process::Command::new(exe)
            .args([
                "--exact",
                "uring::tests::a_stopped_shard_closes_its_descriptors",
                "--nocapture",
            ])
            .env(FD_CHILD, "1")
            .output()
            .expect("run the child test process");

        assert!(
            child.status.success(),
            "the teardown scenario failed in its own process\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
    }

    /// The body of `a_stopped_shard_closes_its_descriptors`, in a process where the
    /// descriptor count means something.
    fn fd_scenario() {
        let baseline = open_fds();
        let Some((addr, _tid, shard)) = spawn_shard(Tuning::default()) else {
            return;
        };

        // Four connections that are mid-frame when the shard stops: a receive is in
        // flight, so neither the socket nor the buffer may be freed before the ring is
        // closed.
        let mut clients = Vec::new();
        for _ in 0..4 {
            let mut s = TcpStream::connect(addr).expect("connect");
            s.set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            s.write_all(&crate::protocol::startup_message())
                .expect("startup");
            read_until_ready(&mut s).expect("handshake reply");
            let q = query_message("select 1");
            s.write_all(&q[..3]).expect("half a query");
            clients.push(s);
        }

        let running = open_fds();
        // The listener, the ring, and one socket per connection.
        assert!(
            running >= baseline + 2 + clients.len(),
            "the shard's own descriptors must be open while it runs: {running} \
             against a baseline of {baseline}"
        );

        drop(shard);

        let stopped = open_fds();
        assert!(
            stopped <= baseline + clients.len(),
            "a stopped shard left {} descriptor(s) open: {stopped} against a \
             baseline of {baseline} and {} client socket(s)",
            stopped.saturating_sub(baseline + clients.len()),
            clients.len()
        );
    }

    /// The MySQL greeting, and a login that answers it.
    fn mysql_login(s: &mut TcpStream) {
        let mut head = [0u8; 4];
        s.read_exact(&mut head).expect("a greeting header");
        let len = head[0] as usize | (head[1] as usize) << 8 | (head[2] as usize) << 16;
        let mut greeting = vec![0u8; len];
        s.read_exact(&mut greeting).expect("a greeting");
        assert_eq!(greeting[0], 10, "HandshakeV10");

        const CAPS: u32 = 0x0000_0001
            | 0x0000_0004
            | 0x0000_0008
            | 0x0000_0200
            | 0x0000_2000
            | 0x0000_8000
            | 0x0001_0000
            | 0x0002_0000
            | 0x0004_0000
            | 0x0008_0000
            | 0x0100_0000;
        let mut body = Vec::new();
        body.extend_from_slice(&CAPS.to_le_bytes());
        body.extend_from_slice(&16_777_216u32.to_le_bytes());
        body.push(45);
        body.extend_from_slice(&[0u8; 23]);
        body.extend_from_slice(b"bench\0");
        body.push(20);
        body.extend_from_slice(&[0x41; 20]);
        body.extend_from_slice(b"tpcc\0");
        body.extend_from_slice(b"mysql_native_password\0");

        let mut login = vec![
            (body.len() & 0xff) as u8,
            ((body.len() >> 8) & 0xff) as u8,
            ((body.len() >> 16) & 0xff) as u8,
            1,
        ];
        login.extend_from_slice(&body);
        s.write_all(&login).expect("a handshake response");

        let mut head = [0u8; 4];
        s.read_exact(&mut head).expect("an answer to the login");
        let len = head[0] as usize | (head[1] as usize) << 8 | (head[2] as usize) << 16;
        let mut ok = vec![0u8; len];
        s.read_exact(&mut ok).expect("an answer body");
        assert_eq!(ok[0], 0x00, "an OK packet: {ok:?}");
    }

    /// A recycled connection starts with nothing in it.
    #[test]
    fn a_recycled_connection_starts_with_nothing_in_it() {
        let Some((addr, _tid, _shard)) =
            spawn_shard_with(crate::protocol::Protocol::Mysql, Tuning::default())
        else {
            return;
        };

        // A greeting, then half a handshake response, then the end of the stream: the
        // shard reaps the slot with those six bytes still in it.
        let mut first = TcpStream::connect(addr).expect("connect");
        first
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        let mut head = [0u8; 4];
        first.read_exact(&mut head).expect("the greeting header");
        let len = head[0] as usize | (head[1] as usize) << 8 | (head[2] as usize) << 16;
        let mut greeting = vec![0u8; len];
        first.read_exact(&mut greeting).expect("the greeting");
        // A header that claims 32 bytes, with two of them sent.
        first
            .write_all(&[0x20, 0x00, 0x00, 0x01, 0x00, 0x00])
            .expect("half a response");
        first
            .shutdown(std::net::Shutdown::Write)
            .expect("the end of the stream");
        // The server closes when it sees the end; reading to 0 is how the client knows
        // the slot is free for the next connection.
        let mut byte = [0u8; 1];
        let _ = first.read(&mut byte);
        drop(first);

        // The next connection, on the slot that just came free.
        let mut second = TcpStream::connect(addr).expect("connect");
        second
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        second.set_nodelay(true).expect("nodelay");
        mysql_login(&mut second);

        // And it can run a query, which is what a stale half packet would have made
        // impossible.
        second
            .write_all(&[
                0x09, 0x00, 0x00, 0x00, 0x03, b's', b'e', b'l', b'e', b'c', b't', b' ', b'1',
            ])
            .expect("a query");
        let mut head = [0u8; 4];
        second
            .read_exact(&mut head)
            .expect("an answer to the query");
        let len = head[0] as usize | (head[1] as usize) << 8 | (head[2] as usize) << 16;
        let mut body = vec![0u8; len];
        second.read_exact(&mut body).expect("an answer body");
        assert_eq!(body, vec![1], "a one-column result set");
    }

    /// A shard that ends with an error still closes its sockets.
    #[test]
    fn a_shard_that_ends_with_an_error_closes_its_descriptors() {
        let codec = Codec::new(crate::protocol::Protocol::Postgres);

        // A pipe, whose read end the slot owns the way it owns an accepted socket.
        let mut fds = [0i32; 2];
        // SAFETY: a two-element array the kernel fills.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            // A box out of descriptors is the environment, not this code: the tests
            // that hold shards and connections take a lot of them.
            eprintln!("skipping: this process is out of descriptors");

            return;
        }
        let (read_end, write_end) = (fds[0], fds[1]);

        let mut slots: Vec<Option<Slot>> = vec![
            None,
            Some(Slot::new(
                read_end,
                None,
                Spare::default(),
                &codec,
                STAGED_HIGH_WATER,
            )),
        ];

        let outcome = finish(Err(io::Error::from_raw_os_error(libc::EBADF)), &mut slots);

        assert!(outcome.is_err(), "the error is the caller's to report");
        // SAFETY: a one-byte write to a descriptor this test opened.
        // Rust ignores SIGPIPE, so a closed reader is EPIPE here rather than a signal.
        let wrote = unsafe { libc::write(write_end, b"x".as_ptr().cast(), 1) };
        let err = io::Error::last_os_error();
        // SAFETY: the write end is this test's to close.
        unsafe { libc::close(write_end) };

        assert_eq!(
            wrote, -1,
            "the slot's descriptor must be closed even though the shard ended in an error"
        );
        assert_eq!(err.raw_os_error(), Some(libc::EPIPE), "{err}");
    }

    /// A shard that cannot have a ring hands its listener back.
    /// Ring memory is charged to the user, so a process with more shards than its
    /// budget allows has shards that cannot have one.
    #[test]
    fn a_shard_without_a_ring_hands_its_listener_back() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener");
        let addr = listener.local_addr().expect("an address");
        let tuning = Tuning {
            fail_ring: true,
            ..Tuning::default()
        };

        let outcome = run_until(
            listener,
            Codec::new(crate::protocol::Protocol::Postgres),
            tuning,
        );

        match outcome {
            Ok(Attempt::NoRing(listener)) => {
                assert_eq!(
                    listener.local_addr().expect("the listener is still open"),
                    addr,
                    "the listener that comes back is the one that went in"
                );
            }
            Ok(Attempt::Served) => panic!("a refused ring must not be reported as served"),
            Err(e) => panic!("a refused ring must hand the listener back, not error: {e}"),
        }
    }

    /// A parked receive is armed again once, not twice.
    #[test]
    fn a_parked_receive_is_armed_again_once() {
        let codec = Codec::new(crate::protocol::Protocol::Postgres);
        let mut slot = Slot::new(-1, None, Spare::default(), &codec, STAGED_HIGH_WATER);

        // Parked, with more reply held than the water mark and a write in flight, and
        // the receive still in the kernel's hands.
        slot.recv_armed = true;
        slot.parked = true;
        slot.sending = true;
        slot.send = vec![0u8; STAGED_HIGH_WATER + 1];

        // The cancel request's completion: the receive is unchanged.
        assert_eq!(after_cancel_request(&mut slot, 0), Next::Idle);
        assert!(
            slot.recv_armed,
            "the cancel request's completion must not say the receive is gone"
        );

        // The receive's own, late, terminal completion: now it is gone, and the read
        // side may be armed again.
        slot.recv_armed = false;
        // The write has drained, so the connection is no longer parked.
        slot.sending = false;
        slot.send.clear();
        assert_eq!(next_for(&mut slot), Next::Arm, "exactly one arm");

        // Armed is armed: a second decision does not arm another.
        slot.recv_armed = true;
        assert_eq!(next_for(&mut slot), Next::Idle);
    }

    /// A pass processes every completion it was given, including the ones that arrive
    /// while it is working.
    #[test]
    fn a_pass_processes_the_completions_that_arrive_while_it_works() {
        let mut pass = Pass::default();
        pass.done.push(Done {
            data: 1,
            res: 0,
            flags: 0,
        });
        pass.done.push(Done {
            data: 2,
            res: 0,
            flags: 0,
        });

        assert_eq!(pass.next().map(|d| d.data), Some(1));
        // The kernel answers a submission made while the pass was working.
        pass.done.push(Done {
            data: 3,
            res: 0,
            flags: 0,
        });
        assert_eq!(pass.next().map(|d| d.data), Some(2));
        assert_eq!(
            pass.next().map(|d| d.data),
            Some(3),
            "a completion that arrived during the pass belongs to it"
        );
        assert_eq!(pass.next().map(|d| d.data), None);

        // And only now is the buffer emptied, for the next pass.
        pass.finish();
        assert_eq!(pass.next().map(|d| d.data), None);
    }

    /// One buffer, two connections, no starvation.
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
        if !has_provided_buffers(&stats) {
            return;
        }

        // The clients pipeline, and start together.
        const CONNS: usize = 4;
        const BATCH: usize = 8;
        let ready = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut clients = Vec::new();
        for conn in 0..CONNS {
            let ready = Arc::clone(&ready);
            clients.push(std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).expect("connect");
                s.set_nodelay(true).expect("nodelay");
                s.set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                s.write_all(&crate::protocol::startup_message())
                    .expect("startup");
                read_until_ready(&mut s).expect("handshake reply");

                let mut batch = Vec::new();
                for seq in 0..BATCH {
                    let cols = 1 + (conn + seq) % 3;
                    let sql = format!("select {}", vec!["1"; cols].join(", "));
                    batch.extend_from_slice(&query_message(&sql));
                }

                ready.fetch_add(1, Ordering::SeqCst);
                while ready.load(Ordering::SeqCst) < CONNS {
                    std::hint::spin_loop();
                }
                s.write_all(&batch).expect("one pipelined write");

                for seq in 0..BATCH {
                    let cols = 1 + (conn + seq) % 3;
                    let frames = read_until_ready(&mut s).expect("reply");
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
        // And the fallback was really taken.
        assert!(
            stats.enobufs.load(Ordering::Relaxed) > 0,
            "one buffer served four connections without ever running out -- the \
             private-buffer path was never exercised"
        );
    }

    /// Several messages in one receive are all answered, in order.
    #[test]
    fn several_messages_in_one_receive_are_all_answered_in_order() {
        const BATCH: usize = 16;

        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            pbuf_count: 32,
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };
        // Without a provided-buffer ring there is no run-ahead to bound.
        // Each receive is then one-shot and armed only after the previous write
        // completed, so the reply cannot pile up and there is no park to see.
        if !has_provided_buffers(&stats) {
            return;
        }

        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_nodelay(true).expect("nodelay");
        s.set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        s.write_all(&crate::protocol::startup_message())
            .expect("startup");
        read_until_ready(&mut s).expect("handshake reply");

        // The answer's width identifies the query, so answers that arrive in the wrong
        // order -- or not at all -- are caught one by one.
        let mut batch = Vec::new();
        for i in 0..BATCH {
            let cols = 1 + i % 3;
            let sql = format!("select {}", vec!["1"; cols].join(", "));
            batch.extend_from_slice(&query_message(&sql));
        }

        let before = stats.reads.load(Ordering::Relaxed);
        s.write_all(&batch).expect("one write of many queries");

        for i in 0..BATCH {
            let cols = 1 + i % 3;
            let frames = read_until_ready(&mut s).expect("reply");
            assert_eq!(frames.len(), 4, "answer {i}");
            assert_eq!(frames[0].0, b'T');
            assert_eq!(
                columns(&frames[0].1),
                cols,
                "answer {i} is not the {i}th query's"
            );
        }

        // They were not sixteen reads: a batch of this size fits one receive.
        let reads = stats.reads.load(Ordering::Relaxed) - before;
        assert!(
            reads <= 2,
            "{BATCH} pipelined queries took {reads} receives; the codec is not \
             consuming a whole buffer"
        );
    }

    /// A message larger than the receive buffer is one message.
    #[test]
    fn a_message_larger_than_the_receive_buffer_is_answered_once() {
        let Some((addr, _tid, _shard)) = spawn_shard(Tuning::default()) else {
            return;
        };

        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_nodelay(true).expect("nodelay");
        s.set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        s.write_all(&crate::protocol::startup_message())
            .expect("startup");
        read_until_ready(&mut s).expect("handshake reply");

        // A comment four buffers long: the statement is still `select 1`, and the
        // message is far too large for one receive.
        let sql = format!("select 1 /* {} */", "x".repeat(4 * RECV_BUF));
        let q = query_message(&sql);
        assert!(q.len() > RECV_BUF, "the message must not fit one buffer");
        s.write_all(&q).expect("one large query");

        let frames = read_until_ready(&mut s).expect("reply to a large query");
        assert_eq!(frames.len(), 4, "one answer, not one per piece: {frames:?}");
        assert_eq!(frames[0].0, b'T');
        assert_eq!(columns(&frames[0].1), 1);
        assert_eq!(frames[1].0, b'D');

        // And the connection is still whole afterwards: nothing of the large message
        // was left in the carry-over buffer to be framed as a query.
        let frames = ask(&mut s, "select 1, 1").expect("reply");
        assert_eq!(frames.len(), 4);
        assert_eq!(columns(&frames[0].1), 2);
    }

    /// The kernel is asked to re-issue the receive by itself.
    /// A multishot receive is one request that serves every read on a connection;
    /// arming it is what `RecvMulti` and its EINVAL fallback exist for.
    #[test]
    fn multishot_receive_serves_many_reads_from_one_arm() {
        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            entries: 128,
            pbuf_count: 32,
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };
        if !has_provided_buffers(&stats) {
            return;
        }

        let mut clients = Vec::new();
        for conn in 0..4 {
            clients.push(std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).expect("connect");
                s.set_nodelay(true).expect("nodelay");
                s.set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                s.write_all(&crate::protocol::startup_message())
                    .expect("startup");
                read_until_ready(&mut s).expect("handshake reply");
                for _ in 0..8 {
                    let frames = ask(&mut s, "select 1").expect("reply");
                    assert_eq!(frames.len(), 4, "conn {conn}");
                }
            }));
        }
        for c in clients {
            c.join().expect("a client must not panic");
        }

        assert!(
            stats.multi_arms.load(Ordering::Relaxed) > 0,
            "no receive was armed as multishot: every read cost its own request"
        );
        assert!(
            stats.reads.load(Ordering::Relaxed) > stats.multi_arms.load(Ordering::Relaxed),
            "one multishot arm must serve more than one read (reads {}, arms {})",
            stats.reads.load(Ordering::Relaxed),
            stats.multi_arms.load(Ordering::Relaxed)
        );
    }

    /// A client that writes and never reads cannot grow the server.
    /// A multishot receive runs ahead of the write: the kernel hands over whatever the
    /// client pipelined, and with a write in flight every answer is staged.
    #[test]
    fn a_client_that_never_reads_cannot_grow_the_shard_memory() {
        // The mark is what the test scales down.
        const WATER: usize = 128 * 1024;

        let stats = Arc::new(Stats::default());
        let tuning = Tuning {
            entries: 256,
            pbuf_count: 32,
            water: WATER,
            stats: Some(Arc::clone(&stats)),
            ..Tuning::default()
        };
        let Some((addr, _tid, _shard)) = spawn_shard(tuning) else {
            return;
        };
        // Without a provided-buffer ring there is no run-ahead to bound.
        // Each receive is then one-shot and armed only after the previous write
        // completed, so the reply cannot pile up and there is no park to see.
        if !has_provided_buffers(&stats) {
            return;
        }

        let mut s = TcpStream::connect(addr).expect("connect");
        s.set_nodelay(true).expect("nodelay");
        // The write side is bounded: once the server stops reading, the client's own
        // socket buffer fills and `write_all` would block forever.
        s.set_write_timeout(Some(Duration::from_millis(50)))
            .expect("write timeout");

        // A wide result set: the answer is several times the size of the query, so the
        // reply is what fills the pipeline.
        let wide = format!("select {}", vec!["1"; 200].join(", "));
        let query = query_message(&wide);
        let mut batch = Vec::new();
        for _ in 0..16 {
            batch.extend_from_slice(&query);
        }
        s.write_all(&crate::protocol::startup_message())
            .expect("startup");

        // Write until the shard stops reading from this connection.
        // A write that times out is retried: the pipeline is full for the moment, and
        // the server is still working through what it has.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && stats.parks.load(Ordering::Relaxed) == 0 {
            if s.write_all(&batch).is_err() {
                std::thread::sleep(Duration::from_millis(5));
            }
        }

        let held = stats.staged_high.load(Ordering::Relaxed);
        assert!(
            stats.parks.load(Ordering::Relaxed) > 0,
            "the shard never stopped reading from a connection holding {held} bytes \
             of unanswered reply"
        );
        // What a parked connection still holds is the water mark plus the reads the
        // kernel had ALREADY completed when the park was decided -- bytes in provided
        // buffers that nobody can put back unread.
        assert!(
            held <= WATER + PBUF_COUNT as usize * RECV_BUF * 16,
            "a connection that never reads grew the server's buffer to {held} bytes \
             (water mark {WATER})"
        );

        // Growth STOPS: the client keeps writing and still reads nothing, and once the
        // reads it had already been given are behind it the queue does not move again.
        let settled = |s: &Stats| {
            let before = s.staged_high.load(Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(200));
            before == s.staged_high.load(Ordering::Relaxed)
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && !settled(&stats) {
            let _ = s.write_all(&batch);
        }
        let settled_at = stats.staged_high.load(Ordering::Relaxed);
        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            let _ = s.write_all(&batch);
        }
        assert_eq!(
            stats.staged_high.load(Ordering::Relaxed),
            settled_at,
            "a parked connection grew its queue from {settled_at} bytes while its \
             peer kept writing"
        );

        // Another connection is served normally while the first is parked: the park is
        // per-connection, not per-shard.
        let mut polite = TcpStream::connect(addr).expect("connect");
        polite.set_nodelay(true).expect("nodelay");
        polite
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("read timeout");
        polite
            .write_all(&crate::protocol::startup_message())
            .expect("startup");
        read_until_ready(&mut polite).expect("handshake reply");
        for _ in 0..16 {
            let frames = ask(&mut polite, "select 1").expect("reply");
            assert_eq!(frames.len(), 4);
        }

        // And then read: the read side comes back.
        let reads_before = stats.reads.load(Ordering::Relaxed);
        s.set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");
        let mut ready = 0usize;
        let mut buf = vec![0u8; 64 * 1024];
        let deadline = Instant::now() + Duration::from_secs(20);
        while (stats.reads.load(Ordering::Relaxed) == reads_before || ready < 16)
            && Instant::now() < deadline
        {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => ready += buf[..n].iter().filter(|&&b| b == b'Z').count(),
                // A read that times out is a server with nothing to say yet; the
                // deadline above is what ends the wait.
                Err(_) => continue,
            }
        }
        assert!(
            stats.reads.load(Ordering::Relaxed) > reads_before,
            "a parked connection must be read from again once its peer drains: \
             {ready} answers, and not one read after the park"
        );
        assert!(ready > 0, "and its answers must arrive");
    }

    /// Many connections, one deliberately small ring.
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
        if !has_provided_buffers(&stats) {
            return;
        }

        let mut clients = Vec::new();
        for conn in 0..CONNS {
            clients.push(std::thread::spawn(move || {
                let mut s = TcpStream::connect(addr).expect("connect");
                s.set_nodelay(true).expect("nodelay");
                s.set_read_timeout(Some(Duration::from_secs(30)))
                    .expect("read timeout");
                s.set_write_timeout(Some(Duration::from_secs(30)))
                    .expect("write timeout");
                s.write_all(&crate::protocol::startup_message())
                    .expect("startup");
                read_until_ready(&mut s).expect("handshake reply");

                for seq in 0..QUERIES {
                    // The answer's shape depends on the connection AND on the request,
                    // so an answer that belongs to another request -- or another
                    // connection -- is one that does not match.
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
                    // Leave mid-frame, with a receive armed and a buffer partly filled:
                    // the shard must give that buffer back.
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
