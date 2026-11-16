//! Cooperative program budgets; no root, service or cgroup prerequisite.
//! Bounded algorithms are primary. Sampled monitoring is not a kernel quota.
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::process::{CommandExt, ExitStatusExt},
    },
    path::Path,
    process::Command,
    thread,
    time::Duration,
};

pub const MEMORY_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const STOP_BYTES: u64 = 60 * 1024 * 1024 * 1024;
pub const TASKS: u64 = 32;
pub const CPU_CORES: usize = 32;
pub const IO_THREADS: usize = 8;
const LOCK: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/resource.lock";
const CPU_LIBRARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/libly_cme_cpu_budget.so";
const LAUNCHER: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/ly_cme_run";

#[derive(Debug, Serialize)]
pub struct Limits {
    mode: &'static str,
    kernel_hard_limit: bool,
    supervisor_pid: u32,
    memory_budget_bytes: u64,
    memory_stop_bytes: u64,
    process_and_thread_budget: u64,
    max_workers: usize,
    publication_workers: usize,
    cpu_affinity: Vec<usize>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct Root {
    pid: u32,
    start_ticks: u64,
}

#[derive(Clone, Copy, Debug)]
struct Process {
    pid: u32,
    parent: i32,
    group: i32,
    start_ticks: u64,
}

fn parse_process(text: &[u8]) -> Result<Process> {
    // comm can contain whitespace/parentheses; fields after its final ')'
    // begin at field 3. Identity discovery must not parse any process's RSS:
    // stat RSS is signed, and unrelated counters do not belong to this task.
    // comm is an arbitrary byte string, not necessarily UTF-8.
    let end = text
        .iter()
        .rposition(|b| *b == b')')
        .context("invalid proc stat")?;
    let head = &text[..end];
    let tail = std::str::from_utf8(&text[end + 1..]).context("non-ASCII proc identity fields")?;
    let fields: Vec<_> = tail.split_whitespace().collect();
    ensure!(fields.len() >= 20, "short proc stat identity");
    let pid = std::str::from_utf8(
        head.split(|b| b.is_ascii_whitespace())
            .next()
            .context("pid")?,
    )?
    .parse()
    .context("invalid proc pid")?;
    Ok(Process {
        pid,
        parent: fields[1]
            .parse()
            .with_context(|| format!("pid {pid}: invalid parent PID {:?}", fields[1]))?,
        group: fields[2]
            .parse()
            .with_context(|| format!("pid {pid}: invalid process group {:?}", fields[2]))?,
        start_ticks: fields[19]
            .parse()
            .with_context(|| format!("pid {pid}: invalid start clock {:?}", fields[19]))?,
    })
}

fn process(pid: u32) -> Result<Process> {
    parse_process(&fs::read(format!("/proc/{pid}/stat"))?)
        .with_context(|| format!("reading /proc/{pid}/stat identity"))
}

#[derive(Debug, PartialEq)]
struct Usage {
    bytes: u64,
    threads: u64,
}

fn parse_usage(text: &[u8]) -> Result<Usage> {
    let number = |key: &[u8], required: bool| -> Result<u64> {
        // Use the last occurrence: Name can itself contain a newline.
        let value = text
            .split(|b| *b == b'\n')
            .rev()
            .find_map(|line| line.strip_prefix(key));
        let Some(value) = value else {
            ensure!(!required, "missing {}", String::from_utf8_lossy(key));
            return Ok(0);
        };
        let token = value
            .split(|b| b.is_ascii_whitespace())
            .find(|s| !s.is_empty())
            .context("empty resource field")?;
        let token = std::str::from_utf8(token).context("non-ASCII resource counter")?;
        token.parse().with_context(|| {
            format!(
                "invalid {} counter {:?}",
                String::from_utf8_lossy(key),
                token
            )
        })
    };
    let rss = number(b"VmRSS:", false)?;
    let swap = number(b"VmSwap:", false)?;
    Ok(Usage {
        bytes: rss.saturating_add(swap).saturating_mul(1024),
        threads: number(b"Threads:", true)?,
    })
}

fn usage(p: Process) -> Result<Option<Usage>> {
    let result = fs::read(format!("/proc/{}/status", p.pid))
        .map_err(anyhow::Error::from)
        .and_then(|text| parse_usage(&text));
    // Check identity after reading counters so PID reuse or an exit race can
    // never attribute another task's memory to this task.
    match process(p.pid) {
        Ok(now) if now.start_ticks != p.start_ticks => return Ok(None),
        Err(error) if vanished(&error) => return Ok(None),
        Err(error) => return Err(error),
        _ => {}
    }
    match result {
        Ok(value) => Ok(Some(value)),
        Err(error) if vanished(&error) => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("reading /proc/{}/status resource counters", p.pid))
        }
    }
}

fn vanished(error: &anyhow::Error) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|e| {
        e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(libc::ESRCH)
    })
}

fn is_descendant(root: Root) -> bool {
    let mut pid = std::process::id();
    for _ in 0..1024 {
        let Ok(p) = process(pid) else {
            return false;
        };
        if pid == root.pid {
            return p.start_ticks == root.start_ticks;
        }
        if p.parent <= 0 || p.parent as u32 == pid {
            return false;
        }
        pid = p.parent as u32;
    }
    false
}

fn active_root() -> Option<Root> {
    let mut file = File::open(LOCK).ok()?;
    // Stale metadata or an environment variable cannot establish a supervisor.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } == 0 {
        return None;
    }
    if std::io::Error::last_os_error().kind() != std::io::ErrorKind::WouldBlock {
        return None;
    }
    let mut text = String::new();
    file.read_to_string(&mut text).ok()?;
    let root: Root = serde_json::from_str(&text).ok()?;
    let p = process(root.pid).ok()?;
    (p.start_ticks == root.start_ticks
        && i64::from(p.group) == i64::from(root.pid)
        && is_descendant(root))
    .then_some(root)
}

fn configure() -> Result<Vec<usize>> {
    ensure!(
        Path::new(CPU_LIBRARY).is_file(),
        "run scripts/build.sh first: native auto-pool budget library is missing"
    );
    let existing = std::env::var("LD_PRELOAD").unwrap_or_default();
    if !existing.split([':', ' ']).any(|p| p == CPU_LIBRARY) {
        std::env::set_var(
            "LD_PRELOAD",
            if existing.is_empty() {
                CPU_LIBRARY.to_owned()
            } else {
                format!("{CPU_LIBRARY}:{existing}")
            },
        );
    }
    unsafe {
        let mut allowed: libc::cpu_set_t = std::mem::zeroed();
        ensure!(
            libc::sched_getaffinity(0, std::mem::size_of_val(&allowed), &mut allowed) == 0,
            "sched_getaffinity: {}",
            std::io::Error::last_os_error()
        );
        let selected: Vec<_> = (0..libc::CPU_SETSIZE as usize)
            .filter(|i| libc::CPU_ISSET(*i, &allowed))
            .take(CPU_CORES)
            .collect();
        ensure!(!selected.is_empty(), "empty CPU affinity");
        libc::CPU_ZERO(&mut allowed);
        for &cpu in &selected {
            libc::CPU_SET(cpu, &mut allowed);
        }
        ensure!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&allowed), &allowed) == 0,
            "sched_setaffinity: {}",
            std::io::Error::last_os_error()
        );
        for key in [
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
            "NUMEXPR_MAX_THREADS",
            "RAYON_NUM_THREADS",
            "TOKIO_WORKER_THREADS",
            "UV_THREADPOOL_SIZE",
            "ARROW_NUM_THREADS",
            "OMP_THREAD_LIMIT",
        ] {
            std::env::set_var(key, "1");
        }
        std::env::set_var("LY_CME_DUCKDB_THREADS", "1");
        std::env::set_var("LY_CME_DUCKDB_MEMORY", "2GB");
        std::env::set_var("LY_CME_IO_THREADS", IO_THREADS.to_string());
        std::env::set_var("LY_CME_WORKERS", "2");
        Ok(selected)
    }
}

fn report(root: Root, cpu_affinity: Vec<usize>) -> Limits {
    Limits {
        mode: "program_budget_and_sampled_monitor",
        kernel_hard_limit: false,
        supervisor_pid: root.pid,
        memory_budget_bytes: MEMORY_BYTES,
        memory_stop_bytes: STOP_BYTES,
        process_and_thread_budget: TASKS,
        max_workers: 2,
        publication_workers: IO_THREADS,
        cpu_affinity,
    }
}

/// Native data bins re-exec under the ordinary launcher before opening data.
pub fn enforce() -> Result<Limits> {
    if let Some(root) = active_root() {
        return Ok(report(root, configure()?));
    }
    let error = Command::new(LAUNCHER)
        .arg("--")
        .arg(std::env::current_exe()?)
        .args(std::env::args_os().skip(1))
        .exec();
    Err(error).context("cannot start program resource supervisor")
}

fn members(root: Root) -> Result<Vec<Process>> {
    let mut processes = HashMap::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u32>().ok())
        else {
            continue;
        };
        match process(pid) {
            Ok(p) => {
                processes.insert(pid, p);
            }
            Err(e)
                if vanished(&e)
                    || e.downcast_ref::<std::io::Error>().is_some_and(|e| {
                        matches!(
                            e.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                        )
                    }) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(select_members(root, processes))
}

fn select_members(root: Root, processes: HashMap<u32, Process>) -> Vec<Process> {
    // Linux exposes PPID/PGRP as signed pid_t fields. A disappearing unrelated
    // process may expose -1; retain that identity without interpreting it as
    // our group. Descendants still count even if their group is unavailable.
    let mut selected: HashSet<_> = processes
        .values()
        .filter(|p| i64::from(p.group) == i64::from(root.pid) || p.pid == root.pid)
        .map(|p| p.pid)
        .collect();
    loop {
        let before = selected.len();
        for p in processes.values() {
            if u32::try_from(p.parent).is_ok_and(|pid| selected.contains(&pid)) {
                selected.insert(p.pid);
            }
        }
        if selected.len() == before {
            break;
        }
    }
    processes
        .into_values()
        .filter(|p| selected.contains(&p.pid))
        .collect()
}

fn over_budget(bytes: u64, threads: u64) -> bool {
    bytes >= STOP_BYTES || threads > TASKS
}

extern "C" fn terminate_group(_: libc::c_int) {
    // Only installed after creating our dedicated group. Async-signal-safe.
    unsafe {
        libc::kill(0, libc::SIGKILL);
    }
}

fn stop(root: Root, all: &[Process], reason: &str) -> ! {
    eprintln!("resource budget exceeded or monitor failed: {reason}; stopping this project task");
    // Persist actual failures only, so a later status check does not depend on
    // whether a terminal session still holds stderr or a stale running state.
    let directory = Path::new(LOCK).parent().unwrap().join("failures");
    let record = serde_json::json!({"supervisor_pid":root.pid,"start_ticks":root.start_ticks,
        "unix_seconds":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d|d.as_secs()).unwrap_or(0),
        "reason":reason,"member_pids":all.iter().map(|p|p.pid).collect::<Vec<_>>()});
    if fs::create_dir_all(&directory).is_ok() {
        let target = directory.join(format!("{}_{}.json", root.pid, root.start_ticks));
        let partial = target.with_extension("json.partial");
        if let Ok(bytes) = serde_json::to_vec_pretty(&record) {
            if fs::write(&partial, bytes).is_ok() {
                let _ = fs::rename(partial, target);
            }
        }
    }
    for p in all {
        if p.pid != root.pid && process(p.pid).is_ok_and(|now| now.start_ticks == p.start_ticks) {
            unsafe {
                libc::kill(p.pid as i32, libc::SIGKILL);
            }
        }
    }
    // This is our dedicated task group, never the calling shell's group.
    unsafe {
        libc::kill(-(root.pid as i32), libc::SIGKILL);
    }
    std::process::exit(137)
}

fn monitor(root: Root) -> Result<()> {
    thread::Builder::new()
        .name("resource-watch".into())
        .spawn(move || loop {
            let all = members(root).unwrap_or_else(|e| stop(root, &[], &format!("{e:#}")));
            let mut bytes = 0u64;
            let mut threads = 0u64;
            for p in &all {
                match usage(*p) {
                    Ok(Some(value)) => {
                        bytes = bytes.saturating_add(value.bytes);
                        threads = threads.saturating_add(value.threads);
                    }
                    Ok(None) => {}
                    Err(e) => stop(root, &all, &format!("{e:#}")),
                }
            }
            if over_budget(bytes, threads) {
                stop(
                    root,
                    &all,
                    &format!("RSS+swap={bytes} bytes, process/thread count={threads}"),
                );
            }
            thread::sleep(Duration::from_millis(250));
        })?;
    Ok(())
}

/// Serialize independent top-level tasks; their workers share one budget.
/// This is an ordinary foreground executable, never a service.
pub fn launch(command: &[OsString], check: bool) -> Result<i32> {
    if let Some(root) = active_root() {
        let limits = report(root, configure()?);
        if check {
            println!("{}", serde_json::to_string_pretty(&limits)?);
            return Ok(0);
        }
        return Err(Command::new(&command[0]).args(&command[1..]).exec().into());
    }
    let path = Path::new(LOCK);
    fs::create_dir_all(path.parent().context("runtime directory")?)?;
    let mut lease = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    if unsafe { libc::flock(lease.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        bail!("another independent project task is active; run stages sequentially so they share one 64 GiB / 32-thread budget");
    }
    let previous: Option<Root> = serde_json::from_reader(&lease).ok();
    if let Some(previous) = previous {
        // SIGKILL of a supervisor cannot be handled. Do not let a new task
        // overlap its surviving workers. A reused PID is not that old task.
        let reused = process(previous.pid).is_ok_and(|p| p.start_ticks != previous.start_ticks);
        ensure!(reused || members(previous)?.is_empty(), "previous project task still has worker processes; stop them before starting another task");
    }
    let pid = std::process::id();
    if unsafe { libc::getpgrp() } != pid as i32 {
        ensure!(
            unsafe { libc::setpgid(0, 0) } == 0,
            "cannot create dedicated task process group"
        );
    }
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = terminate_group as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        for signal in [libc::SIGTERM, libc::SIGINT] {
            ensure!(
                libc::sigaction(signal, &action, std::ptr::null_mut()) == 0,
                "cannot install task cancellation handler"
            );
        }
    }
    let root = Root {
        pid,
        start_ticks: process(pid)?.start_ticks,
    };
    lease.set_len(0)?;
    lease.seek(SeekFrom::Start(0))?;
    serde_json::to_writer(&mut lease, &root)?;
    lease.flush()?;
    let limits = report(root, configure()?);
    if check {
        println!("{}", serde_json::to_string_pretty(&limits)?);
        return Ok(0);
    }
    monitor(root)?;
    let mut child = Command::new(&command[0]);
    child.args(&command[1..]);
    unsafe {
        child.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::getppid() != pid as i32 {
                return Err(std::io::Error::other("resource supervisor exited"));
            }
            Ok(())
        });
    }
    let status = child.spawn()?.wait()?;
    for p in members(root)? {
        if p.pid != pid && process(p.pid).is_ok_and(|now| now.start_ticks == p.start_ticks) {
            unsafe {
                libc::kill(p.pid as i32, libc::SIGKILL);
            }
        }
    }
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn process_exit_races_are_not_monitor_failures() {
        for code in [libc::ENOENT, libc::ESRCH] {
            assert!(vanished(&std::io::Error::from_raw_os_error(code).into()));
            let error = anyhow::Error::from(std::io::Error::from_raw_os_error(code))
                .context("reading proc identity");
            assert!(vanished(&error));
        }
        assert!(!vanished(
            &std::io::Error::from_raw_os_error(libc::EACCES).into()
        ));
        assert!(!vanished(&anyhow::anyhow!("malformed proc stat")));
    }
    #[test]
    fn threshold_reserves_memory_headroom() {
        assert!(!over_budget(STOP_BYTES - 1, 32));
        assert!(over_budget(STOP_BYTES, 1));
        assert!(over_budget(1, 33));
        assert_eq!(MEMORY_BYTES - STOP_BYTES, 4 * 1024u64.pow(3));
    }
    #[test]
    fn proc_stat_names_and_large_values() {
        let tail = "S 8 42 42 0 -1 4194304 1 0 0 0 1 0 0 0 20 0 3 0 123456789 0 987654321";
        let p = parse_process(format!("99 (a tricky ) name) {tail}").as_bytes()).unwrap();
        assert_eq!(
            (p.pid, p.parent, p.group, p.start_ticks),
            (99, 8, 42, 123456789)
        );
        assert!(parse_process(b"99 (bad) S").is_err());
        let mut binary = b"99 (non-utf8 \xff name) ".to_vec();
        binary.extend_from_slice(tail.replace("987654321", "-987654321").as_bytes());
        assert_eq!(parse_process(&binary).unwrap().start_ticks, 123456789);
    }
    #[test]
    fn signed_process_ids_do_not_drop_task_descendants() {
        let tail = "S -1 -1 42 0 -1 4194304 1 0 0 0 1 0 0 0 20 0 3 0 123456789 0 -987654321";
        let unrelated = parse_process(format!("99 (retiring process) {tail}").as_bytes()).unwrap();
        assert_eq!((unrelated.parent, unrelated.group), (-1, -1));
        let root = Root {
            pid: 42,
            start_ticks: 100,
        };
        let parent = Process {
            pid: 42,
            parent: 8,
            group: 42,
            start_ticks: 100,
        };
        let child = Process {
            pid: 100,
            parent: 42,
            group: -1,
            start_ticks: 101,
        };
        let grandchild = Process {
            pid: 101,
            parent: 100,
            group: -1,
            start_ticks: 102,
        };
        let members = select_members(
            root,
            [unrelated, parent, child, grandchild]
                .into_iter()
                .map(|p| (p.pid, p))
                .collect(),
        );
        let found: HashSet<_> = members.iter().map(|p| p.pid).collect();
        assert_eq!(found, HashSet::from([42, 100, 101]));
    }
    #[test]
    fn resource_counters_only_use_member_status_fields() {
        let text =
            b"Name:\tanything\xff\nVmRSS: garbage\nVmRSS:\t 123 kB\nVmSwap:\t7 kB\nThreads:\t3\n";
        assert_eq!(
            parse_usage(text).unwrap(),
            Usage {
                bytes: 130 * 1024,
                threads: 3
            }
        );
        assert_eq!(
            parse_usage(b"Name: zombie\nThreads: 1\n").unwrap(),
            Usage {
                bytes: 0,
                threads: 1
            }
        );
        let error = parse_usage(b"VmRSS: invalid kB\nThreads: 1\n").unwrap_err();
        assert!(format!("{error:#}").contains("VmRSS"));
        assert!(parse_usage(b"VmRSS: 1 kB\n").is_err());
    }
}
