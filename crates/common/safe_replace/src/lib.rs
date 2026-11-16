//! Replacement of an already-delivered file (shared by `ib_kline_export`
//! and `lseg_kline`).
//!
//! New files are published with a no-overwrite hard link by the callers. A
//! differing existing file is replaced only like this:
//! 1. the caller has written the new content to a temp file in the target
//!    directory, fsynced it and checked its footer;
//! 2. the old file is copied to `<backup_root>/<path relative to the delivery
//!    root>` (create-new, fsync) and compared byte for byte;
//! 3. the temp file is renamed over the target (NFS has no
//!    `RENAME_NOREPLACE`; nothing is ever removed without the backup).
use anyhow::{ensure, Context, Result};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Byte equality of two files (streamed).
pub fn same_bytes(a: &Path, b: &Path) -> Result<bool> {
    let (mut x, mut y) = (File::open(a)?, File::open(b)?);
    if x.metadata()?.len() != y.metadata()?.len() {
        return Ok(false);
    }
    let (mut bx, mut by) = (vec![0u8; 1 << 20], vec![0u8; 1 << 20]);
    loop {
        let n = read_full(&mut x, &mut bx)?;
        let m = read_full(&mut y, &mut by)?;
        if n != m || bx[..n] != by[..m] {
            return Ok(false);
        }
        if n == 0 {
            return Ok(true);
        }
    }
}

fn read_full(f: &mut File, buf: &mut [u8]) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        let k = f.read(&mut buf[n..])?;
        if k == 0 {
            break;
        }
        n += k;
    }
    Ok(n)
}

/// Directory fsync; best effort on NFS.
pub fn sync_dir(path: &Path) {
    if let Ok(d) = File::open(path) {
        let _ = d.sync_all();
    }
}

/// Backs up `target` and renames `temp` over it; returns the backup path.
pub fn replace_with_backup(
    temp: &Path,
    target: &Path,
    delivery_root: &Path,
    backup_root: &Path,
) -> Result<PathBuf> {
    ensure!(
        temp.parent() == target.parent(),
        "temp file must be in the target directory: {}",
        temp.display()
    );
    let relative = target.strip_prefix(delivery_root).with_context(|| {
        format!(
            "{} is outside delivery root {}",
            target.display(),
            delivery_root.display()
        )
    })?;
    ensure!(
        !backup_root.starts_with(delivery_root),
        "backup root must not be on the delivery disk"
    );
    let backup = backup_root.join(relative);
    fs::create_dir_all(backup.parent().context("backup parent")?)?;
    {
        let mut src = File::open(target)?;
        let mut dst = File::options()
            .write(true)
            .create_new(true)
            .open(&backup)
            .with_context(|| {
                format!(
                    "backup already exists (one replacement per file per run): {}",
                    backup.display()
                )
            })?;
        std::io::copy(&mut src, &mut dst)?;
        dst.sync_all()?;
    }
    sync_dir(backup.parent().unwrap());
    ensure!(
        same_bytes(target, &backup)?,
        "backup differs from original: {}",
        backup.display()
    );
    fs::rename(temp, target)?;
    sync_dir(target.parent().context("target parent")?);
    Ok(backup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_then_rename_over() {
        let t = tempfile::tempdir().unwrap();
        let root = t.path().join("delivery");
        let backups = t.path().join("backups");
        let dir = root.join("cme/2026/X/20261107");
        fs::create_dir_all(&dir).unwrap();
        let target = dir.join("kline.parquet");
        fs::write(&target, b"old").unwrap();
        let temp = dir.join(".kline.parquet.partial");
        fs::write(&temp, b"new!").unwrap();
        let b = replace_with_backup(&temp, &target, &root, &backups).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"new!");
        assert_eq!(fs::read(&b).unwrap(), b"old");
        assert!(!temp.exists());
        // A second replacement in the same backup root is refused.
        fs::write(&temp, b"newer").unwrap();
        assert!(replace_with_backup(&temp, &target, &root, &backups).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"new!");
        // Backups inside the delivery root are refused.
        assert!(replace_with_backup(&temp, &target, &root, &root.join("b")).is_err());
        assert!(same_bytes(&b, &b).unwrap());
        assert!(!same_bytes(&b, &target).unwrap());
    }
}
