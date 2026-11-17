//! Compares every trade/depth/open-interest record of a (small) candidate
//! RocksDB against the reference (production) RocksDB, both opened read-only.
//! usage: compare_l2_db <candidate_db> <reference_db>
//! - every candidate record must exist in the reference with identical bytes;
//! - for each (cf, kind, instrument) the reference must hold no extra records
//!   inside the candidate's [first_ts, last_ts] range.
use cn_futures_core::codec::{decode_key, KIND_DEPTH, KIND_OI, KIND_TRADE};
use cn_futures_l2::db::open_rocksdb_read_only;
use rocksdb::{Direction, IteratorMode};
use std::collections::BTreeMap;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let cand = open_rocksdb_read_only(Path::new(&a[1]))?;
    let refd = open_rocksdb_read_only(Path::new(&a[2]))?;
    let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), &a[1])?;
    let (mut same, mut differ, mut absent) = (0u64, 0u64, 0u64);
    for name in names.iter().filter(|n| n.contains(':')) {
        let (Some(c), Some(r)) = (cand.cf_handle(name), refd.cf_handle(name)) else {
            println!("cf {name} missing in reference");
            continue;
        };
        let mut ranges: BTreeMap<(u8, String), (u64, u64, u64)> = BTreeMap::new();
        for item in cand.iterator_cf(&c, IteratorMode::Start) {
            let (k, v) = item?;
            let (kind, inst, ts, _) = decode_key(&k)?;
            if ![KIND_TRADE, KIND_DEPTH, KIND_OI].contains(&kind) {
                continue;
            }
            match refd.get_cf(&r, &k)? {
                Some(x) if x.as_slice() == &*v => same += 1,
                Some(_) => {
                    differ += 1;
                    if differ < 6 {
                        println!("DIFF {name} kind={kind} {inst} ts={ts}");
                    }
                }
                None => {
                    absent += 1;
                    if absent < 6 {
                        println!("ABSENT in reference {name} kind={kind} {inst} ts={ts}");
                    }
                }
            }
            let e = ranges.entry((kind, inst)).or_insert((u64::MAX, 0, 0));
            e.0 = e.0.min(ts);
            e.1 = e.1.max(ts);
            e.2 += 1;
        }
        for ((kind, inst), (lo, hi, n)) in ranges {
            let start = cn_futures_core::codec::encode_key(kind, &inst, lo, 0)?;
            let mut count = 0u64;
            for item in refd.iterator_cf(&r, IteratorMode::From(&start, Direction::Forward)) {
                let (k, _) = item?;
                let (k2, i2, ts, _) = decode_key(&k)?;
                if k2 != kind || i2 != inst || ts > hi {
                    break;
                }
                count += 1;
            }
            if count != n {
                println!("COUNT {name} kind={kind} {inst}: candidate {n} vs reference {count} in [{lo},{hi}]");
                absent += count.abs_diff(n);
            }
        }
    }
    println!("identical={same} differing={differ} absent_or_extra={absent}");
    Ok(())
}
