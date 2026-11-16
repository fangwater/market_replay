//! Child processes: own process group, stdout+stderr appended to one log
//! file (rotated at `LOG_ROTATE_BYTES`), stopped with SIGTERM to the group
//! then SIGKILL after the grace period.

use crate::config::Command;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Child;

pub const LOG_ROTATE_BYTES: u64 = 64 << 20;

pub fn log_path(state_dir: &Path, kind: &str, name: &str) -> PathBuf {
    state_dir
        .join("logs")
        .join(kind)
        .join(format!("{name}.log"))
}

fn open_log(path: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(path.parent().context("log parent")?)?;
    if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) > LOG_ROTATE_BYTES {
        std::fs::rename(path, path.with_extension("log.1"))?;
    }
    Ok(std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?)
}

/// Appends one marker line (start / exit) to the log.
pub fn mark(path: &Path, text: &str) {
    use std::io::Write;
    if let Ok(mut f) = open_log(path) {
        let _ = writeln!(
            f,
            "==== {} {text}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
    }
}

pub fn spawn(run: &Command, log: &Path) -> Result<Child> {
    let out = open_log(log)?;
    let err = out.try_clone()?;
    let mut cmd = tokio::process::Command::new(&run.command);
    cmd.args(&run.args)
        .current_dir(&run.cwd)
        .envs(&run.env)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .process_group(0)
        .kill_on_drop(false);
    cmd.spawn()
        .with_context(|| format!("spawn {:?} in {:?}", run.command, run.cwd))
}

fn signal_group(pid: u32, sig: libc::c_int) {
    // Negative pid = the child's whole process group.
    unsafe {
        libc::kill(-(pid as i32), sig);
    }
}

/// SIGTERM the child's group, wait up to `grace`, then SIGKILL.
pub async fn stop(child: &mut Child, grace: Duration) -> Option<std::process::ExitStatus> {
    let pid = child.id()?;
    signal_group(pid, libc::SIGTERM);
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(status) => status.ok(),
        Err(_) => {
            signal_group(pid, libc::SIGKILL);
            child.wait().await.ok()
        }
    }
}

pub fn describe(status: Option<std::process::ExitStatus>) -> String {
    use std::os::unix::process::ExitStatusExt;
    match status {
        Some(s) => match (s.code(), s.signal()) {
            (Some(c), _) => format!("exit {c}"),
            (None, Some(sig)) => format!("signal {sig}"),
            _ => "unknown".into(),
        },
        None => "not started".into(),
    }
}

/// Last `max_bytes` of a log, cut at a line boundary.
pub fn tail(path: &Path, max_bytes: u64) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path) else {
        return String::new();
    };
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let start = len.saturating_sub(max_bytes);
    let _ = f.seek(SeekFrom::Start(start));
    let mut buf = Vec::new();
    let _ = f.take(max_bytes).read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf).into_owned();
    if start > 0 {
        text.split_once('\n')
            .map(|(_, t)| t.to_string())
            .unwrap_or(text)
    } else {
        text
    }
}
