use crate::{
    config::{Config, Task},
    engine::{Engine, Summary},
    input::Input,
    metadata::{Dominants, Metadata},
    output::Outputs,
    replay::Replay,
    store::atomic_json,
};
use anyhow::{ensure, Context, Result};
use mdl_replay::plan::Selection;
use serde_json::json;
use std::{collections::BTreeSet, path::Path};

pub fn run(cfg: &Config, dir: &Path, cancel: impl Fn() -> bool) -> Result<Summary> {
    cfg.validate()?;
    std::fs::create_dir_all(dir)?;
    let metadata = Metadata::load(cfg)?;
    atomic_json(&dir.join("metadata.json"), &metadata)?;
    atomic_json(&dir.join("config.json"), cfg)?;
    let dominants = Dominants::load(&cfg.roll_dir, &cfg.products)?;
    let start = crate::day_number(&cfg.start)?;
    let end = crate::day_number(&cfg.end)?;
    let days: Vec<_> = dominants.days.range(start..=end).map(|(d, _)| *d).collect();
    ensure!(!days.is_empty(), "no dominant trading days in range");
    let input_hash = if cfg.task != Task::RollCost {
        Some(crate::sha256(&cfg.input)?)
    } else {
        None
    };
    let mut input = if cfg.task != Task::RollCost {
        Some(Input::open(&cfg.input, &cfg.products)?)
    } else {
        None
    };
    let mut next_input = if let Some(i) = &mut input {
        i.read()?
    } else {
        None
    };
    let mut carry = None;
    let outputs = Outputs::new(dir)?;
    let mut engine = Engine::new(cfg.clone(), metadata.clone(), outputs)?;
    let mut source_days = vec![];
    let mut final_ts = 0;
    let mut previous_contracts = BTreeSet::new();
    for day in days {
        ensure!(!cancel(), "cancelled");
        let mapping = dominants.days[&day].clone();
        let mut contracts: BTreeSet<_> = mapping.values().cloned().collect();
        contracts.extend(previous_contracts.iter().cloned());
        contracts.extend(engine.required_contracts());
        contracts.extend(
            cfg.initial_positions
                .iter()
                .map(|p| p.contract.to_ascii_lowercase()),
        );
        if cfg.task == Task::RollCost {
            contracts = BTreeSet::from([
                cfg.roll.old_contract.to_ascii_lowercase(),
                cfg.roll.new_contract.to_ascii_lowercase(),
            ]);
        }
        let mut channels = BTreeSet::new();
        for c in &contracts {
            let s = metadata.spec(c)?;
            let ex = cn_futures_core::session::Exchange::parse(&s.exchange)
                .context("unknown exchange")?;
            channels.insert(ex.msg_stem().to_string());
        }
        let selection = Selection {
            trade_day: Some(day.to_string()),
            contracts: contracts.into_iter().collect(),
            channels: channels.into_iter().collect(),
            ..Default::default()
        };
        eprintln!(
            "cn-backtest: prepare {day}, {} contracts",
            selection.contracts.len()
        );
        let mut replay = Replay::open(cfg, selection)?;
        let (first, last) = replay.bounds();
        if final_ts > 0 {
            engine.advance(first, false)?;
        }
        engine.begin_day(day, mapping.clone())?;
        while next_input.as_ref().is_some_and(|r| r.ts < first) {
            carry = next_input.take();
            next_input = input.as_mut().unwrap().read()?;
        }
        if let Some(mut r) = carry.take() {
            r.ts = first;
            engine.input(r)?;
        }
        let mut seen = 0u64;
        while let Some(q) = replay.next(&metadata, &cancel)? {
            while next_input.as_ref().is_some_and(|r| r.ts <= q.ts) {
                let r = next_input.take().unwrap();
                carry = Some(r.clone());
                engine.input(r)?;
                next_input = input.as_mut().unwrap().read()?;
            }
            if seen % 4096 == 0 {
                ensure!(!cancel(), "cancelled");
            }
            engine.quote(q)?;
            seen += 1;
        }
        replay.verify_sources()?;
        engine.summary.source_complete &= !replay.plan.summary.partial;
        source_days.push(json!({"trading_day":day,"plan":replay.plan.summary,"sources":replay.sources(),"stream":replay.hashes()}));
        final_ts = last;
        previous_contracts = mapping.values().cloned().collect();
        eprintln!("cn-backtest: {day} verified {seen} raw ZMQ frames");
    }
    if let Some(hash) = &input_hash {
        ensure!(
            &crate::sha256(&cfg.input)? == hash,
            "input changed during run"
        );
    }
    let summary = engine.finish(final_ts)?;
    let counts = engine.sink.finish()?;
    atomic_json(&dir.join("summary.json"), &summary)?;
    let mut output_hashes = std::collections::BTreeMap::new();
    for table in crate::output::TABLES {
        let p = dir.join(format!("{table}.parquet"));
        output_hashes.insert(*table, crate::sha256(&p)?);
    }
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|x| x.status.success())
        .map(|x| String::from_utf8_lossy(&x.stdout).trim().to_string());
    atomic_json(
        &dir.join("manifest.json"),
        &json!({"schema":"cn-backtest/v1","input_sha256":input_hash,"input":cfg.input,"capital_cny":cfg.capital_cny,"input_mode":cfg.input_mode,"metadata_sha256":crate::sha256(&dir.join("metadata.json"))?,"dominant_sha256":dominants.hashes,"days":source_days,"output_sha256":output_hashes,"rows":counts,"revision":revision,"model":summary.model,"fee_historical_exact":metadata.historical_exact,"conversion_clock":"baseline-capture-producer;fixed-across-execution-delay-scenarios"}),
    )?;
    Ok(summary)
}
