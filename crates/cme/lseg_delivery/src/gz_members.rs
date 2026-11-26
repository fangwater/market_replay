//! Whole-member reader for multi-member gzip files on libdeflate.
//!
//! The LSEG TAS packages and daily files are concatenations of independent gzip members
//! (about 10-20 MB compressed, 330-460 MB of text each; checked 2026-11-25). libdeflate inflates
//! a whole member about 5x faster than streaming zlib-ng on this data (TY 2026-11-19: 1.7 s vs
//! about 8 s of CPU for 4.49 GB), and checks each member's CRC32 and size trailer.
//!
//! Memory is bounded: one compressed window (starts at `WINDOW_START`, grows only while a single
//! member does not fit, up to `WINDOW_CAP`) and one output buffer (up to `OUT_CAP`).
use anyhow::{bail, ensure, Context, Result};
use std::{fs::File, io::Read, path::Path, ptr::NonNull};

const WINDOW_START: usize = 128 << 20;
const WINDOW_CAP: usize = 1 << 30;
const OUT_START: usize = 512 << 20;
const OUT_CAP: usize = 4 << 30;

struct Decompressor(NonNull<libdeflate_sys::libdeflate_decompressor>);

// The decompressor is plain heap state without thread affinity.
unsafe impl Send for Decompressor {}

impl Drop for Decompressor {
    fn drop(&mut self) {
        unsafe { libdeflate_sys::libdeflate_free_decompressor(self.0.as_ptr()) }
    }
}

enum Inflate {
    Done { consumed: usize, produced: usize },
    BadData,
    NoSpace,
}

impl Decompressor {
    fn new() -> Result<Self> {
        let p = unsafe { libdeflate_sys::libdeflate_alloc_decompressor() };
        NonNull::new(p)
            .map(Self)
            .context("libdeflate_alloc_decompressor failed")
    }

    fn gzip_member(&mut self, input: &[u8], out: &mut [u8]) -> Inflate {
        let (mut consumed, mut produced) = (0usize, 0usize);
        let ret = unsafe {
            libdeflate_sys::libdeflate_gzip_decompress_ex(
                self.0.as_ptr(),
                input.as_ptr().cast(),
                input.len(),
                out.as_mut_ptr().cast(),
                out.len(),
                &mut consumed,
                &mut produced,
            )
        };
        match ret {
            libdeflate_sys::libdeflate_result_LIBDEFLATE_SUCCESS => {
                Inflate::Done { consumed, produced }
            }
            libdeflate_sys::libdeflate_result_LIBDEFLATE_INSUFFICIENT_SPACE => Inflate::NoSpace,
            _ => Inflate::BadData,
        }
    }
}

pub struct MemberReader {
    input: Box<dyn Read + Send>,
    window: Vec<u8>,
    start: usize,
    end: usize,
    eof: bool,
    cut: bool,
    /// The 10-byte header of the first member; a later occurrence in the window shows that the
    /// current member is complete, so a member is not decoded twice across a refill.
    magic: Option<[u8; 10]>,
    offset: u64,
    members: u64,
    truncated: bool,
    dec: Decompressor,
    out: Vec<u8>,
}

impl MemberReader {
    /// `max_compressed_bytes` cuts the input (measurements); the cut member is then dropped and
    /// `truncated()` reports it.
    pub fn open(path: &Path, max_compressed_bytes: Option<u64>) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let input: Box<dyn Read + Send> = match max_compressed_bytes {
            Some(n) => Box::new(file.take(n)),
            None => Box::new(file),
        };
        Ok(Self {
            input,
            window: vec![0; WINDOW_START],
            start: 0,
            end: 0,
            eof: false,
            cut: max_compressed_bytes.is_some(),
            magic: None,
            offset: 0,
            members: 0,
            truncated: false,
            dec: Decompressor::new()?,
            out: vec![0; OUT_START],
        })
    }

    pub fn members(&self) -> u64 {
        self.members
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Reads more input; grows the window when one member fills it. False at end of input.
    fn fill(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }
        if self.start > 0 {
            self.window.copy_within(self.start..self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }
        if self.end == self.window.len() {
            ensure!(
                self.window.len() < WINDOW_CAP,
                "gzip member at offset {} is larger than {} MiB",
                self.offset,
                WINDOW_CAP >> 20
            );
            self.window
                .resize((self.window.len() * 2).min(WINDOW_CAP), 0);
        }
        while self.end < self.window.len() {
            let n = self.input.read(&mut self.window[self.end..])?;
            if n == 0 {
                self.eof = true;
                break;
            }
            self.end += n;
        }
        Ok(true)
    }

    /// The next member's text, or `None` at the end of input.
    pub fn next_member(&mut self) -> Result<Option<&[u8]>> {
        loop {
            if self.start == self.end && !self.fill()? {
                return Ok(None);
            }
            let avail = &self.window[self.start..self.end];
            if self.magic.is_none() && avail.len() >= 10 {
                ensure!(
                    avail[0] == 0x1f && avail[1] == 0x8b,
                    "no gzip member at offset {}",
                    self.offset
                );
                self.magic = Some(avail[..10].try_into().unwrap());
            }
            let complete = self.eof
                || self.magic.is_some_and(|m| {
                    avail.len() > 10 && memchr::memmem::find(&avail[10..], &m).is_some()
                });
            if !complete && self.fill()? {
                continue;
            }
            let avail = &self.window[self.start..self.end];
            match self.dec.gzip_member(avail, &mut self.out) {
                Inflate::Done { consumed, produced } => {
                    self.start += consumed;
                    self.offset += consumed as u64;
                    self.members += 1;
                    return Ok(Some(&self.out[..produced]));
                }
                Inflate::NoSpace => {
                    ensure!(
                        self.out.len() < OUT_CAP,
                        "gzip member at offset {} inflates to more than {} MiB",
                        self.offset,
                        OUT_CAP >> 20
                    );
                    let len = (self.out.len() * 2).min(OUT_CAP);
                    self.out = vec![0; len];
                }
                Inflate::BadData => {
                    // A false header match inside compressed data, or a member cut by the window.
                    if !self.eof {
                        self.fill()?;
                        continue;
                    }
                    if self.cut {
                        self.truncated = true;
                        return Ok(None);
                    }
                    bail!("corrupt gzip member at offset {}", self.offset);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    fn member(text: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), Compression::fast());
        e.write_all(text).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn reads_members_in_order_and_rejects_corruption() {
        let dir = std::env::temp_dir().join(format!("gz_members_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.gz");
        let mut bytes = member(b"a,1\nb,2\n");
        bytes.extend(member(b"c,3\n"));
        bytes.extend(member(b""));
        std::fs::write(&path, &bytes).unwrap();
        let mut r = MemberReader::open(&path, None).unwrap();
        let mut got = Vec::new();
        while let Some(m) = r.next_member().unwrap() {
            got.push(m.to_vec());
        }
        assert_eq!(got, [b"a,1\nb,2\n".to_vec(), b"c,3\n".to_vec(), Vec::new()]);
        assert_eq!(r.members(), 3);

        let cut = MemberReader::open(&path, Some(bytes.len() as u64 - 3));
        let mut cut = cut.unwrap();
        let mut n = 0;
        while cut.next_member().unwrap().is_some() {
            n += 1;
        }
        assert_eq!(n, 2);
        assert!(cut.truncated());

        let mut bad = bytes.clone();
        let last = bad.len() - 1;
        bad[last] ^= 1; // size trailer of the last member
        std::fs::write(&path, &bad).unwrap();
        let mut r = MemberReader::open(&path, None).unwrap();
        r.next_member().unwrap();
        r.next_member().unwrap();
        assert!(r.next_member().is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
