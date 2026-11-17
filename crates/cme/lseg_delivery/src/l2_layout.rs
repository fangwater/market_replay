//! Compact encodings of the update stream of one RIC (the RocksDB value layer).
//!
//! A *delta* is defined against the previous row of the SAME RIC: the set of
//! (level, field) slots whose value differs (bitwise, empty = NaN) is a 60-bit
//! mask; only those slots are stored. A row identical to its predecessor
//! costs one flag byte (time only): the slices need the time of EVERY update
//! (`last_update_ts_ns`, the silence that starts a session, which grid points
//! had an update at all), so the row cannot disappear. Prices are integers at scale 1e9 stored as zig-zag steps from the
//! last stored price of the slot; sizes and order counts are varints. A price
//! is only stored that way if `mantissa / 1e9` reproduces the exact f64 the CSV
//! parser produced (both operands are exactly representable, IEEE division is
//! correctly rounded, so this equals parsing the decimal text); otherwise the
//! raw f64 bits are stored (escape mask), so the round trip is always bit-exact.
use crate::l2_slice::{Book, BOOK_FIELDS};
use anyhow::{bail, ensure, Context, Result};

pub const PRICE_SCALE: f64 = 1e9;
/// Every `CHECKPOINT_BUCKETS`-th non-empty bucket starts with a self-contained
/// record (reset), so a time range can be decoded without the history before it.
pub const CHECKPOINT_BUCKETS: u32 = 256;
/// Bucket width of the packed layout (nanoseconds).
pub const BUCKET_NS: i64 = 1_000_000_000;
const PRICE_SLOTS: usize = BOOK_FIELDS / 3;
const FLAG_UNCHANGED: u8 = 4;

fn is_price(k: usize) -> bool {
    k.is_multiple_of(3)
}

pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub fn get_varint(b: &[u8], pos: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let byte = *b.get(*pos).context("truncated varint")?;
        *pos += 1;
        v |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return Ok(v);
        }
        shift += 7;
        ensure!(shift < 64, "varint too long");
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Mantissa at scale 1e9 if it reproduces `p` bit for bit.
fn mantissa(p: f64) -> Option<i64> {
    let m = (p * PRICE_SCALE).round();
    if !m.is_finite() || m.abs() >= 9.0e15 {
        return None;
    }
    let mi = m as i64;
    ((mi as f64) / PRICE_SCALE)
        .to_bits()
        .eq(&p.to_bits())
        .then_some(mi)
}

/// Stateful encoder/decoder of the records of ONE RIC.
pub struct Codec2 {
    prev: Book,
    last_m: [i64; PRICE_SLOTS],
}

impl Default for Codec2 {
    fn default() -> Self {
        Self::new()
    }
}

impl Codec2 {
    pub fn new() -> Self {
        Self {
            prev: [f64::NAN; BOOK_FIELDS],
            last_m: [0; PRICE_SLOTS],
        }
    }

    /// Appends one record (flags, masks, values). `reset` makes the record
    /// self-contained (checkpoint / first record of a RIC). A book equal to the
    /// previous one is a single flag byte.
    pub fn encode(&mut self, book: &Book, reset: bool, out: &mut Vec<u8>) {
        if reset {
            self.prev = [f64::NAN; BOOK_FIELDS];
            self.last_m = [0; PRICE_SLOTS];
        }
        let (mut changed, mut present_after, mut raw) = (0u64, 0u64, 0u64);
        #[allow(clippy::needless_range_loop)]
        for k in 0..BOOK_FIELDS {
            if book[k].to_bits() != self.prev[k].to_bits() {
                changed |= 1 << k;
                if !book[k].is_nan() {
                    present_after |= 1 << k;
                    if is_price(k) && mantissa(book[k]).is_none() {
                        raw |= 1 << k;
                    }
                }
            }
        }
        if changed == 0 && !reset {
            out.push(FLAG_UNCHANGED);
            return;
        }
        out.push(reset as u8 | ((raw != 0) as u8) << 1);
        put_varint(out, changed);
        put_varint(out, present_after);
        if raw != 0 {
            put_varint(out, raw);
        }
        #[allow(clippy::needless_range_loop)]
        for k in 0..BOOK_FIELDS {
            if present_after >> k & 1 == 0 {
                continue;
            }
            if is_price(k) {
                if raw >> k & 1 == 1 {
                    out.extend_from_slice(&book[k].to_le_bytes());
                } else {
                    let m = mantissa(book[k]).expect("checked");
                    put_varint(out, zigzag(m - self.last_m[k / 3]));
                    self.last_m[k / 3] = m;
                }
            } else {
                put_varint(out, book[k] as u32 as u64);
            }
        }
        self.prev = *book;
    }

    /// Applies one record at `pos`, advances `pos`, leaves the book in `out`.
    pub fn decode(&mut self, b: &[u8], pos: &mut usize, out: &mut Book) -> Result<()> {
        let flags = *b.get(*pos).context("truncated record")?;
        *pos += 1;
        ensure!(flags <= FLAG_UNCHANGED, "bad record flags {flags}");
        if flags == FLAG_UNCHANGED {
            *out = self.prev;
            return Ok(());
        }
        if flags & 1 == 1 {
            self.prev = [f64::NAN; BOOK_FIELDS];
            self.last_m = [0; PRICE_SLOTS];
        }
        let changed = get_varint(b, pos)?;
        let present_after = get_varint(b, pos)?;
        let raw = if flags & 2 != 0 {
            get_varint(b, pos)?
        } else {
            0
        };
        for k in 0..BOOK_FIELDS {
            if changed >> k & 1 == 0 {
                continue;
            }
            if present_after >> k & 1 == 0 {
                self.prev[k] = f64::NAN;
            } else if is_price(k) {
                if raw >> k & 1 == 1 {
                    let bytes: [u8; 8] = b
                        .get(*pos..*pos + 8)
                        .context("truncated raw price")?
                        .try_into()
                        .expect("8");
                    *pos += 8;
                    self.prev[k] = f64::from_le_bytes(bytes);
                } else {
                    let m = self.last_m[k / 3] + unzigzag(get_varint(b, pos)?);
                    self.last_m[k / 3] = m;
                    self.prev[k] = (m as f64) / PRICE_SCALE;
                }
            } else {
                self.prev[k] = get_varint(b, pos)? as u32 as f64;
            }
        }
        *out = self.prev;
        Ok(())
    }
}

pub fn bucket_of(t: i64) -> u32 {
    (t.div_euclid(BUCKET_NS)) as u32
}

/// Packs the updates of one RIC into per-second buckets:
/// value = varint(n) then n x (varint(dt ns), record), `dt` of the first update
/// is the offset inside the second, later ones are steps from the previous
/// update. Every update is stored (an unchanged book as a one byte record).
pub struct BucketWriter {
    codec: Codec2,
    bucket: Option<u32>,
    buf: Vec<u8>,
    n: u64,
    prev_t: i64,
    nonempty_buckets: u32,
    started: bool,
}

impl Default for BucketWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketWriter {
    pub fn new() -> Self {
        Self {
            codec: Codec2::new(),
            bucket: None,
            buf: Vec::with_capacity(4096),
            n: 0,
            prev_t: 0,
            nonempty_buckets: 0,
            started: false,
        }
    }

    fn flush(&mut self, emit: &mut dyn FnMut(u32, &[u8]) -> Result<()>) -> Result<()> {
        if let Some(b) = self.bucket.take() {
            if self.n > 0 {
                let mut v = Vec::with_capacity(self.buf.len() + 4);
                put_varint(&mut v, self.n);
                v.extend_from_slice(&self.buf);
                emit(b, &v)?;
            }
        }
        self.buf.clear();
        self.n = 0;
        Ok(())
    }

    /// `emit(bucket, value)` is called for every finished bucket, in order.
    pub fn push(
        &mut self,
        t: i64,
        book: &Book,
        emit: &mut dyn FnMut(u32, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let b = bucket_of(t);
        if self.bucket != Some(b) {
            self.flush(emit)?;
            self.bucket = Some(b);
        }
        // checkpoint: the first update of every CHECKPOINT_BUCKETS-th bucket
        let first_in_bucket = self.n == 0;
        let reset = !self.started
            || (first_in_bucket && self.nonempty_buckets.is_multiple_of(CHECKPOINT_BUCKETS));
        let dt = if first_in_bucket {
            t - b as i64 * BUCKET_NS
        } else {
            t - self.prev_t
        };
        put_varint(&mut self.buf, dt as u64);
        self.codec.encode(book, reset, &mut self.buf);
        if first_in_bucket {
            self.nonempty_buckets += 1;
        }
        self.started = true;
        self.n += 1;
        self.prev_t = t;
        Ok(())
    }

    pub fn finish(&mut self, emit: &mut dyn FnMut(u32, &[u8]) -> Result<()>) -> Result<()> {
        self.flush(emit)
    }
}

/// Decodes one bucket value, calling `f(time_ns, book)` for each stored update.
pub fn decode_bucket(
    codec: &mut Codec2,
    bucket: u32,
    v: &[u8],
    book: &mut Book,
    f: &mut dyn FnMut(i64, &Book) -> Result<()>,
) -> Result<()> {
    let mut pos = 0usize;
    let n = get_varint(v, &mut pos)?;
    let mut t = 0i64;
    for i in 0..n {
        let dt = get_varint(v, &mut pos)? as i64;
        t = if i == 0 {
            bucket as i64 * BUCKET_NS + dt
        } else {
            t + dt
        };
        codec.decode(v, &mut pos, book)?;
        f(t, book)?;
    }
    if pos != v.len() {
        bail!("trailing bytes in bucket {bucket}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(f: impl Fn(usize) -> Option<f64>) -> Book {
        let mut b = [f64::NAN; BOOK_FIELDS];
        for (k, slot) in b.iter_mut().enumerate() {
            if let Some(v) = f(k) {
                *slot = v;
            }
        }
        b
    }

    fn sample_books() -> Vec<Book> {
        let px = |k: usize, base: f64| {
            if is_price(k) {
                base + (k as f64) * 0.25
            } else {
                (k * 3) as f64
            }
        };
        vec![
            book(|_| None),
            book(|k| (k < 6).then(|| px(k, 100.0))),
            book(|k| (k < 12).then(|| px(k, 100.25))),
            book(|k| (k < 12).then(|| px(k, 100.25))), // identical -> no record
            book(|k| {
                (k % 4 == 0).then(|| {
                    if is_price(k) {
                        0.0007346
                    } else {
                        4_000_000_000.0
                    }
                })
            }),
            book(|k| Some(if is_price(k) { 1.0 / 3.0 } else { 7.0 })), // not a 1e-9 decimal -> raw escape
            book(|k| (k < 6).then(|| if is_price(k) { -1.5e-9 } else { 0.0 })),
            book(|k| {
                Some(if is_price(k) {
                    12_345_678.123_456_79
                } else {
                    1.0
                })
            }),
            book(|_| None),
        ]
    }

    #[test]
    fn mantissa_division_equals_the_text_parser() {
        for s in [
            "0.0007346",
            "2534.25",
            "98.5",
            "0.000001",
            "100000.5",
            "12345678.123456789",
            "0.1",
            "1e-9",
        ] {
            let p: f64 = s.parse().unwrap();
            if let Some(m) = mantissa(p) {
                assert_eq!(((m as f64) / PRICE_SCALE).to_bits(), p.to_bits(), "{s}");
            }
        }
        assert!(mantissa(1.0 / 3.0).is_none());
        assert!(mantissa(f64::INFINITY).is_none());
        assert!(mantissa(9.1e15).is_none());
    }

    #[test]
    fn records_round_trip_bit_exactly_and_unchanged_books_cost_one_byte() {
        let mut enc = Codec2::new();
        let mut dec = Codec2::new();
        let mut got = [f64::NAN; BOOK_FIELDS];
        let mut stored = 0;
        for (i, b) in sample_books().iter().cycle().take(40).enumerate() {
            let mut v = Vec::new();
            enc.encode(b, i == 0, &mut v);
            if v == [FLAG_UNCHANGED] {
                stored += 1;
            }
            let mut pos = 0;
            dec.decode(&v, &mut pos, &mut got).unwrap();
            assert_eq!(pos, v.len());
            assert!(
                b.iter().zip(&got).all(|(x, y)| x.to_bits() == y.to_bits()),
                "update {i}"
            );
        }
        assert!(
            stored >= 1,
            "an identical consecutive book is a single flag byte"
        );
    }

    #[test]
    fn buckets_round_trip_with_checkpoints_and_independent_ranges() {
        let books = sample_books();
        let mut w = BucketWriter::new();
        let mut values: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut emit = |b: u32, v: &[u8]| {
            values.push((b, v.to_vec()));
            Ok(())
        };
        // 1000 buckets of 3 updates each; every update comes back, in order
        let mut expect: Vec<(i64, Book)> = Vec::new();
        for s in 0..1000i64 {
            for j in 0..3i64 {
                let t = 1_700_000_000 * BUCKET_NS + s * BUCKET_NS + j * 1_234_567 + 5;
                let b = books[((s * 3 + j) as usize) % books.len()];
                w.push(t, &b, &mut emit).unwrap();
                expect.push((t, b));
            }
        }
        w.finish(&mut emit).unwrap();
        let mut codec = Codec2::new();
        let mut book = [f64::NAN; BOOK_FIELDS];
        let mut got = Vec::new();
        for (b, v) in &values {
            decode_bucket(&mut codec, *b, v, &mut book, &mut |t, bk| {
                got.push((t, *bk));
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(got.len(), expect.len());
        assert!(got.iter().zip(&expect).all(|((t1, b1), (t2, b2))| t1 == t2
            && b1.iter().zip(b2).all(|(x, y)| x.to_bits() == y.to_bits())));
        // a range that starts at a checkpoint decodes without the history before it
        let ckpt = values
            .iter()
            .enumerate()
            .filter(|(_, (_, v))| {
                let mut p = 0;
                let _n = get_varint(v, &mut p).unwrap();
                let _dt = get_varint(v, &mut p).unwrap();
                v[p] & 1 == 1
            })
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        assert!(
            ckpt.len() >= 3,
            "checkpoints every {CHECKPOINT_BUCKETS} buckets: {ckpt:?}"
        );
        let start = ckpt[1];
        let mut fresh = Codec2::new();
        let mut again = Vec::new();
        for (b, v) in &values[start..] {
            decode_bucket(&mut fresh, *b, v, &mut book, &mut |t, bk| {
                again.push((t, *bk));
                Ok(())
            })
            .unwrap();
        }
        let tail = &got[got.len() - again.len()..];
        assert!(again.iter().zip(tail).all(|((t1, b1), (t2, b2))| t1 == t2
            && b1.iter().zip(b2).all(|(x, y)| x.to_bits() == y.to_bits())));
    }

    #[test]
    fn corrupt_records_are_rejected() {
        let mut d = Codec2::new();
        let mut b = [f64::NAN; BOOK_FIELDS];
        let mut pos = 0;
        assert!(d.decode(&[], &mut pos, &mut b).is_err());
        pos = 0;
        assert!(d.decode(&[9], &mut pos, &mut b).is_err());
        let mut v = Vec::new();
        Codec2::new().encode(&book(|k| (k < 3).then_some(1.0)), true, &mut v);
        pos = 0;
        assert!(Codec2::new()
            .decode(&v[..v.len() - 1], &mut pos, &mut b)
            .is_err());
    }
}
