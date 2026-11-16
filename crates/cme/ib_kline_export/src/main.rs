//! `ib_kline_export`: raw IB minute rows -> `<root>/<exchange>/<UTC year>/<RIC>/<YYYYMMDD>/kline_ib.parquet`.
//!
//! Pure export for comparison: no status/is_ffill columns, no filling, no
//! trimming to contract lifetime; the 23-column schema and metadata of the
//! 2026-11-03 per-RIC snapshot. Resource limits are enforced in-process with
//! fixed pools (<= 2 workers, <= 8 publish threads, no CPU-count sizing) and
//! bounded streaming (one UTC day per file in flight, bounded queues).
mod catalog;
mod ib_minute;
mod mapping;
mod mysql;
mod plan;
mod problems;
mod publish;
mod snapshot;

use anyhow::{ensure, Context, Result};
use arrow_array::{Array, BooleanArray, Int64Array};
use clap::{Args as ClapArgs, Parser, Subcommand};
use futures_util::TryStreamExt;
use plan::{Budget, Plan};
use sqlx::{Connection, Row};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

const SNAPSHOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/lseg_data/ib_minute_parquet/covered_futures_20261103";
/// IB MySQL credentials, mode 600, outside the repo.
const IB_ENV: &str = "/home/fanghaizhou/.config/market_replay/ib_mysql.env";
const PLAN: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/ib_minute_parquet/covered_futures_20261103/_audit/contract_plan.json";
const STATE_DIR: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/ib_kline_export";
const HRD: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv";
const HRD_SUPPLEMENT: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv";
const MAX_WORKERS: usize = 2;
/// Whole-process thread ceiling (main + <=2 workers + <=8 publishers + sqlite/tokio helpers).
const MAX_THREADS: u64 = 32;

#[derive(Parser)]
#[command(about = "Raw IB minute export to per-RIC / per-UTC-day kline_ib.parquet")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(ClapArgs)]
struct Common {
    /// Delivery root, e.g. /mnt/nfs/30.100_cta_data/lseg (explicit on purpose).
    #[arg(long)]
    delivery_root: PathBuf,
    /// Read-only LSEG catalog; default <delivery-root>/meta/catalog.sqlite.
    #[arg(long)]
    catalog: Option<PathBuf>,
    /// Local NVMe state: checkpoints, incremental cutoff, backups of replaced files.
    #[arg(long, default_value = STATE_DIR)]
    state_dir: PathBuf,
    /// Backups of replaced day files; default <state-dir>/backups/<UTC run time>.
    #[arg(long)]
    backup_dir: Option<PathBuf>,
    /// Source workers (fixed, at most 2).
    #[arg(long, default_value_t = 2)]
    workers: usize,
    /// Publication threads (fixed, at most 8).
    #[arg(long, default_value_t = 8)]
    publish_threads: usize,
    /// Stop if process RSS+swap exceeds this many GiB.
    #[arg(long, default_value_t = 16)]
    max_rss_gib: u64,
}

#[derive(Subcommand)]
enum Command {
    /// Backfill from the per-RIC snapshot Parquet files.
    Snapshot {
        #[command(flatten)]
        common: Common,
        #[arg(long, default_value = SNAPSHOT)]
        snapshot_dir: PathBuf,
        /// Only these RICs (comma separated); default all plan RICs.
        #[arg(long, value_delimiter = ',')]
        rics: Vec<String>,
        #[arg(long)]
        max_rics: Option<usize>,
    },
    /// Incremental export of rows with id > the NVMe cutoff from MySQL ib_data.
    Mysql {
        #[command(flatten)]
        common: Common,
        /// KEY=VALUE file with IB_MYSQL_HOST/PORT/DATABASE/USER/PASSWORD (never printed).
        #[arg(long, default_value = IB_ENV)]
        env: PathBuf,
        #[arg(long, default_value = PLAN)]
        plan: PathBuf,
        #[arg(long, value_delimiter = ',', default_values_t = [HRD.to_string(), HRD_SUPPLEMENT.to_string()])]
        hrd: Vec<String>,
        /// Create state.json at the snapshot plan cutoff (first run only).
        #[arg(long)]
        init_from_plan: bool,
        /// Cap the new cutoff (testing / splitting a large backlog).
        #[arg(long)]
        max_id: Option<i64>,
        /// Rescan this many ids below the cutoff (late-committing inserts).
        #[arg(long, default_value_t = 20000)]
        overlap_ids: i64,
        /// Only report touched days; write nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Read-only: convert MySQL rows of one con_id in (id_from, id_to] and compare with the snapshot.
    MysqlCheck {
        /// KEY=VALUE file with IB_MYSQL_HOST/PORT/DATABASE/USER/PASSWORD (never printed).
        #[arg(long, default_value = IB_ENV)]
        env: PathBuf,
        #[arg(long, default_value = SNAPSHOT)]
        snapshot_dir: PathBuf,
        #[arg(long)]
        con_id: String,
        #[arg(long)]
        id_from: i64,
        #[arg(long)]
        id_to: i64,
    },
    /// Read-only: run the delivery() port over contracts and compare with the snapshot plan.
    MapCheck {
        /// contract_info rows as JSON list (e.g. artifacts/ib_ric_export_20261103/ib_contracts.json).
        #[arg(long)]
        contracts_json: PathBuf,
        #[arg(long, default_value = PLAN)]
        plan: PathBuf,
        #[arg(long)]
        catalog: PathBuf,
        #[arg(long, value_delimiter = ',', default_values_t = [HRD.to_string(), HRD_SUPPLEMENT.to_string()])]
        hrd: Vec<String>,
    },
}

fn context(
    c: &Common,
) -> Result<(
    Arc<publish::Publisher>,
    Arc<publish::Pool>,
    Budget,
    catalog::Catalog,
)> {
    ensure!(
        (1..=MAX_WORKERS).contains(&c.workers),
        "workers must be 1..={MAX_WORKERS}"
    );
    ensure!(
        c.delivery_root.is_absolute() && c.state_dir.is_absolute(),
        "use absolute paths"
    );
    ensure!(
        !c.state_dir.starts_with(&c.delivery_root),
        "state dir must not be on the delivery disk"
    );
    let catalog_path = c
        .catalog
        .clone()
        .unwrap_or_else(|| c.delivery_root.join("meta/catalog.sqlite"));
    let catalog = catalog::Catalog::load(&catalog_path)?;
    let backup = c.backup_dir.clone().unwrap_or_else(|| {
        c.state_dir
            .join("backups")
            .join(chrono::Utc::now().format("%Y%m%dT%H%M%S%.3fZ").to_string())
    });
    ensure!(
        !backup.starts_with(&c.delivery_root),
        "backup dir must not be on the delivery disk"
    );
    let publisher = Arc::new(publish::Publisher {
        delivery_root: c.delivery_root.clone(),
        backup_root: backup,
    });
    let pool = publish::Pool::new(c.publish_threads)?;
    let budget = Budget {
        max_rss_bytes: c.max_rss_gib << 30,
        max_threads: MAX_THREADS,
    };
    Ok((publisher, pool, budget, catalog))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let summary = match cli.command {
        Command::Snapshot {
            common,
            snapshot_dir,
            rics,
            max_rics,
        } => {
            let (publisher, pool, budget, catalog) = context(&common)?;
            let ctx = snapshot::RunContext {
                delivery_root: common.delivery_root.clone(),
                state_dir: common.state_dir.clone(),
                publisher,
                pool,
                problems: Arc::new(problems::Problems::default()),
                budget,
            };
            snapshot::run(
                &ctx,
                &snapshot_dir,
                &catalog,
                &snapshot::Selection { rics, max_rics },
                common.workers,
            )?
        }
        Command::Mysql {
            common,
            env,
            plan,
            hrd,
            init_from_plan,
            max_id,
            overlap_ids,
            dry_run,
        } => {
            ensure!(overlap_ids >= 0, "overlap must be >= 0");
            let (publisher, pool, budget, catalog) = context(&common)?;
            let problems = problems::Problems::default();
            let shared = mysql::Shared {
                publisher,
                pool,
                budget,
            };
            let args = mysql::Args {
                delivery_root: common.delivery_root.clone(),
                env,
                plan,
                hrd: hrd.into_iter().map(PathBuf::from).collect(),
                state_dir: common.state_dir.clone(),
                init_from_plan,
                max_id,
                overlap_ids,
                dry_run,
                workers: common.workers,
            };
            mysql::run(&args, &catalog, Some(&shared), &problems)?
        }
        Command::MysqlCheck {
            env,
            snapshot_dir,
            con_id,
            id_from,
            id_to,
        } => mysql_check(env, snapshot_dir, con_id, id_from, id_to)?,
        Command::MapCheck {
            contracts_json,
            plan,
            catalog,
            hrd,
        } => map_check(contracts_json, plan, catalog, hrd)?,
    };
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}

fn mysql_check(
    env: PathBuf,
    snapshot_dir: PathBuf,
    con_id: String,
    id_from: i64,
    id_to: i64,
) -> Result<serde_json::Value> {
    let plan = Plan::load(&snapshot_dir.join("_audit/contract_plan.json"))?;
    let job = plan
        .mapping()?
        .remove(&con_id)
        .context("con_id not in snapshot plan")?;
    let s = ib_minute::schema(&job);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let options = mysql::options(&env)?;
    let (rows, newer, newer_sample) = rt.block_on(async {
        let mut conn = mysql::connect(&options).await?;
        let sql = format!(
            "SELECT {} FROM `minute_data` t FORCE INDEX(uk_conid_utctime) WHERE t.`con_id`=? AND t.`id`>? AND t.`id`<=? ORDER BY t.`utc_time`",
            ib_minute::SELECT_COLUMNS
        );
        let mut rows = Vec::new();
        let mut stream = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(&con_id).bind(id_from).bind(id_to).fetch(&mut conn);
        while let Some(r) = stream.try_next().await? {
            rows.push(ib_minute::decode(r)?);
            ensure!(rows.len() <= 500_000, "range too large");
        }
        drop(stream);
        let n = sqlx::query("SELECT COUNT(*), MIN(`id`), MAX(`id`), CAST(MIN(`utc_time`) AS CHAR), CAST(MAX(`utc_time`) AS CHAR) FROM `minute_data` FORCE INDEX(uk_conid_utctime) WHERE `con_id`=? AND `id`>?")
            .bind(&con_id)
            .bind(plan.cutoff_id)
            .fetch_one(&mut conn)
            .await?;
        let newer = serde_json::json!({
            "rows": n.try_get::<i64, _>(0)?, "min_id": n.try_get::<Option<i64>, _>(1)?, "max_id": n.try_get::<Option<i64>, _>(2)?,
            "min_utc": n.try_get::<Option<String>, _>(3)?, "max_utc": n.try_get::<Option<String>, _>(4)?,
        });
        let mut sample = Vec::new();
        let mut stream = sqlx::query(sqlx::AssertSqlSafe(format!("{sql} LIMIT 5").as_str())).bind(&con_id).bind(plan.cutoff_id).bind(i64::MAX).fetch(&mut conn);
        while let Some(r) = stream.try_next().await? {
            sample.push(ib_minute::decode(r)?);
        }
        drop(stream);
        conn.close().await?;
        Ok::<_, anyhow::Error>((rows, newer, sample))
    })?;
    ensure!(
        id_to <= plan.cutoff_id,
        "comparison range must be within the snapshot cutoff {}",
        plan.cutoff_id
    );
    let converted = ib_minute::batch(&rows, &job, s.clone())?;
    // Snapshot rows with id in range.
    let snap = publish::read_all(
        &snapshot_dir
            .join("data")
            .join(format!("{}.parquet", job.ric)),
    )?;
    let ids = snap
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mask = BooleanArray::from_iter(
        (0..ids.len()).map(|i| Some(ids.value(i) > id_from && ids.value(i) <= id_to)),
    );
    let expected = arrow_select::filter::filter_record_batch(&snap, &mask)?;
    let newer_batch = ib_minute::batch(&newer_sample, &job, s)?;
    let ts = newer_batch
        .column_by_name("ts_utc_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    Ok(serde_json::json!({
        "con_id": con_id, "ric": job.ric, "id_range": [id_from, id_to],
        "mysql_rows": converted.num_rows(), "snapshot_rows": expected.num_rows(),
        "identical_including_schema_metadata": converted == expected,
        "rows_after_snapshot_cutoff": newer,
        "converted_sample_after_cutoff": (0..ts.len()).map(|i| ts.value(i)).collect::<Vec<_>>(),
    }))
}

fn map_check(
    contracts_json: PathBuf,
    plan: PathBuf,
    catalog: PathBuf,
    hrd: Vec<String>,
) -> Result<serde_json::Value> {
    let contracts: Vec<mapping::Contract> = serde_json::from_reader(
        std::fs::File::open(&contracts_json)
            .with_context(|| format!("{}", contracts_json.display()))?,
    )?;
    let plan = Plan::load(&plan)?;
    let catalog = catalog::Catalog::load(&catalog)?;
    let mut expected: BTreeMap<String, (String, Option<String>)> = BTreeMap::new();
    for j in &plan.jobs {
        expected.insert(j.con_id.clone(), ("exported".into(), Some(j.ric.clone())));
    }
    for e in &plan.excluded {
        let con = e["con_id"].as_str().unwrap_or("").to_string();
        expected.insert(
            con,
            (
                e["status"].as_str().unwrap_or("").into(),
                e.get("ric").and_then(|v| v.as_str()).map(String::from),
            ),
        );
    }
    let mut cands = Vec::new();
    let mut port: BTreeMap<String, Result<String, String>> = BTreeMap::new();
    let mut wanted = BTreeSet::new();
    for c in contracts
        .iter()
        .filter(|c| mapping::SPECS.iter().any(|(s, _)| *s == c.symbol))
    {
        match mapping::candidate(c, &catalog.observed) {
            Ok(x) => {
                wanted.extend(x.possible.iter().cloned());
                cands.push(x)
            }
            Err(r) => {
                port.insert(c.con_id.clone(), Err(r));
            }
        }
    }
    let hrd = mapping::load_hrd(&hrd, &wanted)?;
    for c in &cands {
        port.insert(
            c.contract.con_id.clone(),
            mapping::resolve(c, &hrd, &catalog.observed).map(|r| r.ric),
        );
    }
    let (mut same, mut differ) = (0, Vec::new());
    for (con, got) in &port {
        let want = expected.get(con);
        let ok = match (want, got) {
            (Some((_, Some(ric))), Ok(r)) => r == ric,
            (Some((status, None)), Err(reason)) => status == reason,
            _ => false,
        };
        if ok {
            same += 1;
        } else {
            differ.push(serde_json::json!({"con_id": con, "plan": want, "port": got.clone().unwrap_or_else(|e| format!("ERR {e}"))}));
        }
    }
    Ok(serde_json::json!({
        "covered_contracts": port.len(), "plan_entries": expected.len(), "agree": same, "differ": differ.len(),
        "differences": differ,
    }))
}
