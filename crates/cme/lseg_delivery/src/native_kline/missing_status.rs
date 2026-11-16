//! Deliver status-only contracts omitted by trade-driven minute generation.
//! No synthetic prices or Kline rows are created for contracts without trades.
use super::*;

fn event_rics(job: &Job, db: &DB, kind: u8, lo: u64, hi: u64) -> Result<BTreeSet<String>> {
    let cf = db.cf_handle(&job.cf).context("status CF")?;
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    options.set_iterate_upper_bound(vec![kind + 1]);
    let mut iter = db.raw_iterator_cf_opt(cf, options);
    iter.seek([kind]);
    let mut found = BTreeSet::new();
    while let Some(key) = iter.key() {
        if key[0] != kind {
            break;
        }
        let ric = key_ric(key)?.to_owned();
        let upper = upper_ric(kind, &ric)?;
        iter.seek(encode_all_key(kind, &ric, lo, 0, 0)?);
        if let Some(key) = iter.key() {
            if key[0] == kind && key_ric(key)? == ric && key_time(key) < hi {
                found.insert(ric);
            }
        }
        iter.seek(upper);
    }
    iter.status()?;
    Ok(found)
}

fn main_contracts(cfg: &Config, key: &str, signature: &str) -> Result<BTreeSet<String>> {
    let group = io::read_json(&cfg.run_dir().join("groups").join(format!("{key}.json")))
        .context("minute group must finish before its status-only supplement")?;
    ensure!(
        group["signature"] == signature && group["output"] == cfg.output.to_string_lossy().as_ref(),
        "minute group identity changed"
    );
    let mut found = BTreeSet::new();
    let native = cfg.run_dir().join("contracts").join(key);
    if native.exists() {
        for entry in fs::read_dir(native)? {
            let path = entry?.path();
            if path.extension().is_none_or(|v| v != "json") {
                continue;
            }
            let record = io::read_json(&path)?;
            ensure!(
                record["signature"] == signature
                    && record["output"] == cfg.output.to_string_lossy().as_ref(),
                "minute contract identity changed"
            );
            if record["stats"]["kline_rows"].as_u64().unwrap_or(0) > 0 {
                // Compare escaped checkpoint names with component(source RIC).
                found.insert(
                    path.file_stem()
                        .context("RIC checkpoint")?
                        .to_string_lossy()
                        .into(),
                );
            }
        }
    }
    if group["stats"]["rics"].as_u64() == Some(found.len() as u64) {
        return Ok(found);
    }
    if group["reused_legacy_group"] == true {
        let legacy = io::read_json(
            &cfg.project
                .join("artifacts/lseg_full_rocksdb_20261112/groups")
                .join(format!("{key}.json")),
        )?;
        ensure!(
            legacy["input_signature"] == signature,
            "legacy group identity changed"
        );
        // The original HG pilot embedded its per-RIC results in the group
        // receipt, instead of writing a contracts directory. Validate both
        // aggregate quantities and identity before using that complete list.
        if let Some(records) = legacy["contracts"].as_array() {
            let metrics = ["trades", "volume", "kline_rows", "files", "status_files"];
            let mut sums = BTreeMap::<&str, u64>::new();
            let mut names = BTreeSet::new();
            let mut reused = 0;
            for record in records {
                let ric = record["ric"].as_str().context("embedded legacy RIC")?;
                ensure!(
                    record["kline_rows"].as_u64().is_some_and(|n| n > 0),
                    "embedded contract has no minute rows"
                );
                ensure!(
                    names.insert(component(ric)),
                    "duplicate embedded legacy contract: {ric}"
                );
                reused += u64::from(record["reused"] == true);
                for metric in metrics {
                    let value = record[metric]
                        .as_u64()
                        .with_context(|| format!("embedded contract {metric}"))?;
                    let sum = sums.entry(metric).or_default();
                    *sum = sum.checked_add(value).context("legacy quantity overflow")?;
                }
            }
            sums.insert("rics", names.len() as u64);
            sums.insert("reused_rics", reused);
            for (metric, sum) in sums {
                ensure!(
                    legacy[metric].as_u64() == Some(sum)
                        && group["stats"][metric].as_u64() == Some(sum),
                    "embedded legacy aggregate mismatch: {metric}"
                );
            }
            found.extend(names);
        }
        let directory = cfg
            .project
            .join("artifacts/lseg_full_rocksdb_20261112/contracts")
            .join(key);
        let entries = if directory.exists() {
            Some(fs::read_dir(directory)?)
        } else {
            None
        };
        for entry in entries.into_iter().flatten() {
            let path = entry?.path();
            if path.extension().is_none_or(|v| v != "json") {
                continue;
            }
            let record = io::read_json(&path)?;
            ensure!(
                record["input_signature"] == signature,
                "legacy contract identity changed"
            );
            if record["kline_rows"].as_u64().unwrap_or(0) > 0 {
                found.insert(component(record["ric"].as_str().context("legacy RIC")?));
            }
        }
    }
    ensure!(
        group["stats"]["rics"].as_u64() == Some(found.len() as u64),
        "minute contract checkpoints do not cover completed group"
    );
    Ok(found)
}

pub(super) fn group_status(
    cfg: &Config,
    all: &[Arc<Source>],
    meta: &Metadata,
    year: i32,
    product: &str,
) -> Result<Stats> {
    let lo = io::year_start(year)?;
    let hi = io::year_start(year + 1)?;
    let sources: Vec<_> = all
        .iter()
        .filter(|s| {
            s.job.product == product
                && ((s.job.year - year).abs() <= 1
                    || s.range.is_some_and(|(a, b)| a < hi && b > lo)
                    || s.control_range.is_some_and(|(a, b)| a < hi && b > lo))
        })
        .map(Arc::as_ref)
        .collect();
    let signature = signature(&sources);
    let key = component(&format!("{year}:{product}"));
    let checkpoint = cfg
        .run_dir()
        .join("status_only_groups")
        .join(format!("{key}.json"));
    if cfg.ric.is_none() && checkpoint.exists() {
        let record = io::read_json(&checkpoint)?;
        ensure!(
            record["signature"] == signature
                && record["output"] == cfg.output.to_string_lossy().as_ref(),
            "status-only group identity changed"
        );
        return Ok(serde_json::from_value(record["stats"].clone())?);
    }
    let completed = main_contracts(cfg, &key, &signature)?;
    let dbs = sources
        .iter()
        .map(|s| open(&s.job))
        .collect::<Result<Vec<_>>>()?;
    for source in &sources {
        ensure!(
            source.identity["current"] == fs::read_to_string(source.job.db.join("CURRENT"))?,
            "source changed before status-only delivery: {}",
            source.job.id
        );
    }
    let mut rics = BTreeSet::new();
    for (source, db) in sources.iter().zip(&dbs) {
        for kind in [12, 13] {
            rics.extend(event_rics(&source.job, db, kind, lo, hi)?);
        }
        ensure!(
            rics.len() <= 1_000_000,
            "too many status RICs in one product/year"
        );
    }
    let mut stats = Stats::default();
    for ric in rics {
        if completed.contains(&component(&ric)) || cfg.ric.as_ref().is_some_and(|v| *v != ric) {
            continue;
        }
        // Check source minutes, rather than interpreting absence of a target
        // file as evidence that the source contained no actual trades.
        ensure!(
            trades(&sources, &dbs, &ric, year)?.bars.is_empty(),
            "actual trades lack a completed minute contract: {year}/{product}/{ric}"
        );
        let contract = cfg
            .run_dir()
            .join("status_only_contracts")
            .join(&key)
            .join(format!("{}.json", component(&ric)));
        if contract.exists() {
            let record = io::read_json(&contract)?;
            ensure!(
                record["signature"] == signature
                    && record["output"] == cfg.output.to_string_lossy().as_ref(),
                "status-only contract identity changed"
            );
            stats.add(&serde_json::from_value(record["stats"].clone())?);
            continue;
        }
        let venue = meta.venue(year, &ric);
        ensure!(
            !venue.is_empty()
                && venue
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
            "invalid exchange directory"
        );
        let cme = ["cme", "cbot", "comex", "nymex"].contains(&venue.as_str());
        let root = cfg
            .output
            .join(&venue)
            .join(year.to_string())
            .join(component(&ric));
        let mut problems = Problems::new(
            cfg.output
                .join("meta/problems/native_kline/status_only")
                .join(&key)
                .join(format!("{}.csv", component(&ric))),
        );
        let mut decoder = Decoder::new(cme);
        let mut previous = State::Unknown;
        let mut writer =
            io::StatusWriter::new_with_pool(root, product, &ric, cfg.publisher.clone())?;
        let mut events = Events::new(&sources, &dbs, &ric, lo, hi, 13)?;
        let mut rows = 0u64;
        let mut first = hi;
        let mut last = lo;
        let mut issues: BTreeMap<(String, String), (u64, u64, u64)> = BTreeMap::new();
        while let Some(i) = events.next_index()? {
            let event = events.peek(i)?;
            let decoded = decoder.decode(event.qualifiers, previous);
            if let Some(issue) = decoded
                .issue
                .as_deref()
                .filter(|v| *v != "uncalibrated_product")
            {
                let key = (issue.to_owned(), event.qualifiers.to_owned());
                ensure!(
                    issues.len() < 100_000 || issues.contains_key(&key),
                    "too many distinct status-only qualifier issues"
                );
                let item = issues.entry(key).or_insert((event.at, event.at, 0));
                item.1 = event.at;
                item.2 += 1;
            }
            let before = previous;
            previous = decoded.state;
            first = first.min(event.at);
            last = event.at;
            writer.push(StatusRow {
                at: event.at,
                before,
                decoded,
                qualifiers: event.qualifiers.into(),
            })?;
            rows += 1;
            events.advance(i);
        }
        let files = writer.finish()?;
        let mut corrections = 0u64;
        for (source, db) in sources.iter().zip(&dbs) {
            let cf = db.cf_handle(&source.job.cf).context("correction CF")?;
            let mut options = ReadOptions::default();
            options.fill_cache(false);
            options.set_iterate_upper_bound(upper_ric(12, &ric)?);
            let mut iter = db.raw_iterator_cf_opt(cf, options);
            iter.seek(encode_all_key(12, &ric, lo, 0, 0)?);
            while let Some(key) = iter.key() {
                if key[0] != 12 || key_ric(key)? != ric || key_time(key) >= hi {
                    break;
                }
                let row = decode_cme_correction(iter.value().context("correction value")?)?;
                ensure!(
                    row.ts_utc_ns == key_time(key),
                    "correction key/time mismatch"
                );
                first = first.min(row.ts_utc_ns);
                last = last.max(row.ts_utc_ns);
                problems.record(
                    &ric,
                    "source_witness_pending",
                    row.ts_utc_ns,
                    row.ts_utc_ns,
                    1,
                    &format!(
                        "{} {} order={}; {}",
                        source.job.db.display(),
                        source.job.cf,
                        order(key),
                        row.qualifiers
                    ),
                )?;
                corrections += 1;
                iter.next();
            }
            iter.status()?;
        }
        for (i, _) in io::sample_indices(files.len()) {
            let file = &files[i];
            let mut reader = io::Rows::new(&file.path)?;
            let mut raw = Events::new(
                &sources,
                &dbs,
                &ric,
                file.day * DAY,
                (file.day + 1) * DAY,
                13,
            )?;
            let mut previous = file.before;
            let mut checked = 0;
            while let Some(i) = raw.next_index()? {
                let event = raw.peek(i)?;
                let decoded = decoder.decode(event.qualifiers, previous);
                let before = previous;
                previous = decoded.state;
                let expected = StatusRow {
                    at: event.at,
                    before,
                    decoded,
                    qualifiers: event.qualifiers.into(),
                };
                let (batch, row) = reader.next_row()?.context("missing status-only row")?;
                io::compare_status(batch, row, product, &ric, &expected)?;
                checked += 1;
                raw.advance(i);
            }
            ensure!(
                reader.next_row()?.is_none() && checked == file.rows,
                "status-only sample count mismatch"
            );
        }
        ensure!(
            rows + corrections > 0,
            "status-only RIC discovery lost its source rows"
        );
        for ((issue, qualifiers), (a, b, count)) in issues {
            problems.record(&ric, &issue, a, b, count, &qualifiers)?;
        }
        problems.record(&ric, "no_trade_for_kline", first, last, rows + corrections, "source has status/correction messages but no actual trade minute in this UTC year; no kline created; initial status UNKNOWN until an explicit source phase, without inferred prior-year contract identity")?;
        if venue == "review" {
            problems.record(
                &ric,
                "venue_unresolved",
                first,
                last,
                1,
                "original RIC retained",
            )?;
        }
        if !cme {
            problems.record(
                &ric,
                "status_protocol_not_calibrated",
                first,
                last,
                1,
                "only explicit H/N interpreted for this venue",
            )?;
        }
        problems.finish()?;
        let result = Stats {
            rics: 1,
            status_only_rics: 1,
            status_rows: rows,
            status_files: files.len() as u64,
            ..Stats::default()
        };
        io::save(
            &contract,
            &json!({"signature":signature,"output":cfg.output,"stats":result}),
        )?;
        stats.add(&result);
        eprintln!("{year}/{product}/{ric}: {rows} status-only messages; no trade-based kline");
    }
    for source in &sources {
        ensure!(
            source.identity["current"] == fs::read_to_string(source.job.db.join("CURRENT"))?,
            "source changed during status-only delivery: {}",
            source.job.id
        );
    }
    if cfg.ric.is_none() {
        io::save(
            &checkpoint,
            &json!({"signature":signature,"output":cfg.output,"stats":stats}),
        )?;
    }
    Ok(stats)
}

pub fn run(mut cfg: Config) -> Result<()> {
    use rayon::prelude::*;
    use std::os::fd::AsRawFd;
    ensure!((1..=2).contains(&cfg.workers), "workers must be 1..2");
    fs::create_dir_all(cfg.run_dir())?;
    let lock = File::create(cfg.run_dir().join("run.lock"))?;
    ensure!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "native task already running"
    );
    let state = cfg.run_dir().join("status_only_state.json");
    io::save(&state, &json!({"stage":"planning","workers":cfg.workers}))?;
    let plan = io::read_json(&cfg.plan)?;
    let mut jobs: Vec<Job> = serde_json::from_value(plan["jobs"].clone())?;
    jobs.retain(|j| cfg.product.as_ref().is_none_or(|p| p == &j.product));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(cfg.workers)
        .build()?;
    let sources: Vec<Arc<Source>> = pool.install(|| {
        jobs.into_par_iter()
            .map(|job| source(&cfg, job).map(Arc::new))
            .collect::<Result<_>>()
    })?;
    let meta = Metadata::load(&cfg)?;
    let groups = target_groups(&cfg, &sources)?;
    cfg.publisher = Some(io::PublicationPool::new(crate::limits::IO_THREADS)?);
    let count = groups.len();
    let progress = Mutex::new((0usize, Stats::default(), Vec::<Value>::new()));
    pool.install(|| {
        groups.par_iter().for_each(|(year, product)| {
            let result = group_status(&cfg, &sources, &meta, *year, product);
            let failure = cfg
                .output
                .join("meta/problems/native_kline/status_only_failures")
                .join(format!("{}.json", component(&format!("{year}:{product}"))));
            let mut p = progress.lock().unwrap();
            match result {
                Ok(stats) => {
                    p.0 += 1;
                    p.1.add(&stats);
                    if failure.exists() {
                        let _ = fs::remove_file(&failure);
                    }
                }
                Err(error) => {
                    let record =
                        json!({"year":year,"product":product,"error":format!("{error:#}")});
                    eprintln!("status-only {record}");
                    let _ = io::save(&failure, &record);
                    p.2.push(record);
                }
            }
            if let Err(error) = io::save(
                &state,
                &json!({"stage":"running","groups":count,"completed":p.0,"stats":p.1,"failed":p.2}),
            ) {
                p.2.push(json!({"error":format!("saving status-only progress: {error:#}")}));
            }
        })
    });
    let p = progress.into_inner().unwrap();
    io::save(
        &state,
        &json!({"stage":if p.2.is_empty(){"complete"}else{"failed"},"groups":count,"completed":p.0,"stats":p.1,"failed":p.2}),
    )?;
    ensure!(p.2.is_empty(), "status-only groups failed");
    Ok(())
}
