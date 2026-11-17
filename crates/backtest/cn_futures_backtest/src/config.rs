use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Task {
    Convert,
    #[default]
    Execute,
    RollCost,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Execution {
    #[default]
    Maker,
    Taker,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ContractSelection {
    #[default]
    Dominant,
    SecondaryOi,
    FarSecondaryOi,
    NearSecondaryOi,
    VolumeSecond,
}
impl ContractSelection {
    pub fn proto(self) -> cn_market_proto::ContractSelectionMode {
        use cn_market_proto::ContractSelectionMode as M;
        match self {
            Self::Dominant => M::Dominant,
            Self::SecondaryOi => M::SecondaryOi,
            Self::FarSecondaryOi => M::FarSecondaryOi,
            Self::NearSecondaryOi => M::NearSecondaryOi,
            Self::VolumeSecond => M::VolumeSecond,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub task: Task,
    pub input: PathBuf,
    pub input_mode: String,
    pub capital_cny: f64,
    pub amount_unit_cny: f64,
    pub start: String,
    pub end: String,
    pub products: Vec<String>,
    pub execution: Execution,
    pub market_grpc: String,
    pub contract_selection: ContractSelection,
    pub reference_api: String,
    pub reference_as_of: String,
    /// An explicit reference snapshot is an assumption, not historical exact fees.
    pub fee_policy: String,
    pub fee_account: Option<String>,
    pub fee_as_of: Option<String>,
    pub metadata_file: Option<PathBuf>,
    pub output_root: PathBuf,
    pub allow_partial: bool,
    pub md_delay_ms: u64,
    pub compute_delay_ms: u64,
    pub order_delay_ms: u64,
    pub cancel_delay_ms: u64,
    pub report_delay_ms: u64,
    pub quote_age_ms: u64,
    pub reprice_ms: u64,
    pub max_child_lots: i64,
    pub protection_ticks: i64,
    pub maker_offset_ticks: i64,
    pub max_pending_events: usize,
    pub initial_positions: Vec<InitialPosition>,
    pub roll: Roll,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            task: Task::Execute,
            input: "/home/fanghaizhou/ds/weights_1min.parquet".into(),
            input_mode: "capital_fraction".into(),
            capital_cny: 5_000_000.0,
            amount_unit_cny: 1.0,
            start: "2026-11-13".into(),
            end: "2026-11-13".into(),
            products: vec!["RB".into(), "SN".into()],
            execution: Execution::Maker,
            market_grpc: "http://127.0.0.1:18312".into(),
            contract_selection: ContractSelection::default(),
            reference_api: "http://127.0.0.1:8765/api/v1".into(),
            reference_as_of: "2026-11-17".into(),
            fee_policy: "reference_snapshot".into(),
            fee_account: None,
            fee_as_of: None,
            metadata_file: None,
            output_root: "/mnt/nvme-raid0-28t/fanghaizhou/backtest".into(),
            allow_partial: false,
            md_delay_ms: 0,
            compute_delay_ms: 0,
            order_delay_ms: 10,
            cancel_delay_ms: 10,
            report_delay_ms: 10,
            quote_age_ms: 2000,
            reprice_ms: 1000,
            max_child_lots: 1,
            protection_ticks: 2,
            maker_offset_ticks: 0,
            max_pending_events: 500_000,
            initial_positions: vec![],
            roll: Roll::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialPosition {
    pub contract: String,
    pub lots: i64,
    pub cost_price: f64,
    pub opened_trading_day: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Roll {
    pub old_contract: String,
    pub new_contract: String,
    pub initial_lots: i64,
    pub target_lots: Option<i64>,
    pub target_cny: Option<f64>,
    pub begin: Option<String>,
    pub deadline: Option<String>,
    pub spread_improvement_ticks: f64,
    pub use_spread_gate: bool,
    pub batch_lots: i64,
    pub max_exposure_deviation_cny: f64,
    pub max_quote_skew_ms: u64,
    pub repair_timeout_ms: u64,
}
impl Default for Roll {
    fn default() -> Self {
        Self {
            old_contract: String::new(),
            new_contract: String::new(),
            initial_lots: 1,
            target_lots: None,
            target_cny: None,
            begin: None,
            deadline: None,
            spread_improvement_ticks: 0.0,
            use_spread_gate: true,
            batch_lots: 1,
            max_exposure_deviation_cny: 100_000.0,
            max_quote_skew_ms: 1000,
            repair_timeout_ms: 2000,
        }
    }
}
impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let cfg: Self =
            toml::from_str(&std::fs::read_to_string(path)?).context("backtest config")?;
        cfg.validate()?;
        Ok(cfg)
    }
    pub fn validate(&self) -> Result<()> {
        let start = crate::day_number(&self.start)?;
        let end = crate::day_number(&self.end)?;
        ensure!(
            start >= 20231207 && start <= end,
            "MDL dates must be >= 2023-12-07 and start <= end"
        );
        ensure!(
            mdl_replay::day::to_epoch_days(end) - mdl_replay::day::to_epoch_days(start) <= 31,
            "one run supports at most 31 calendar days"
        );
        ensure!(
            self.capital_cny.is_finite() && self.capital_cny > 0.0,
            "capital_cny must be positive"
        );
        ensure!(
            self.amount_unit_cny.is_finite() && self.amount_unit_cny > 0.0,
            "amount_unit_cny must be positive"
        );
        ensure!(
            ["capital_fraction", "notional_amount"].contains(&self.input_mode.as_str()),
            "unknown input_mode"
        );
        ensure!(
            ["reference_snapshot", "account_snapshot", "unknown", "zero"]
                .contains(&self.fee_policy.as_str()),
            "unknown fee_policy"
        );
        if self.fee_policy == "account_snapshot" {
            ensure!(
                self.fee_account.as_deref().is_some_and(|s| !s.is_empty()),
                "account_snapshot requires fee_account"
            );
            chrono::DateTime::parse_from_rfc3339(
                self.fee_as_of
                    .as_deref()
                    .context("account_snapshot requires fee_as_of")?,
            )
            .context("fee_as_of requires a timezone-qualified timestamp")?;
        } else {
            ensure!(
                self.fee_account.is_none() && self.fee_as_of.is_none(),
                "fee_account/fee_as_of require account_snapshot"
            );
        }
        ensure!(
            self.reprice_ms > 0 && self.max_child_lots > 0 && self.quote_age_ms > 0,
            "positive reprice_ms, child lots and quote age required"
        );
        ensure!(
            self.protection_ticks >= 0 && self.maker_offset_ticks >= 0,
            "tick offsets must be nonnegative"
        );
        ensure!(
            self.max_pending_events >= 100 && self.max_pending_events <= 2_000_000,
            "event limit must be 100..2000000"
        );
        ensure!(
            [
                self.md_delay_ms,
                self.compute_delay_ms,
                self.order_delay_ms,
                self.cancel_delay_ms,
                self.report_delay_ms
            ]
            .iter()
            .all(|x| *x <= 60_000),
            "latencies must be <=60000ms"
        );
        ensure!(
            self.roll.batch_lots > 0
                && self.roll.max_exposure_deviation_cny.is_finite()
                && self.roll.max_exposure_deviation_cny > 0.0,
            "invalid roll batch or exposure budget"
        );
        ensure!(
            self.roll.spread_improvement_ticks.is_finite(),
            "invalid spread threshold"
        );
        if self.task == Task::RollCost {
            ensure!(
                !self.roll.old_contract.is_empty()
                    && !self.roll.new_contract.is_empty()
                    && self.roll.old_contract != self.roll.new_contract,
                "old/new roll contracts required"
            );
            ensure!(
                crate::product(&self.roll.old_contract) == crate::product(&self.roll.new_contract),
                "roll contracts must be same product"
            );
            ensure!(
                self.roll.initial_lots != 0,
                "roll initial_lots cannot be zero"
            );
        }
        ensure!(
            self.market_grpc.starts_with("http://") || self.market_grpc.starts_with("https://"),
            "market_grpc requires an HTTP(S) endpoint"
        );
        Ok(())
    }
    pub fn amount(&self, raw: f64) -> f64 {
        raw * if self.input_mode == "capital_fraction" {
            self.capital_cny
        } else {
            self.amount_unit_cny
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_paths_cannot_reenable_local_data_access() {
        for key in ["msg_root", "cache_root", "roll_dir"] {
            assert!(toml::from_str::<Config>(&format!("{key}=\"/some/source\"\n")).is_err());
        }
        let cfg: Config = toml::from_str(
            "market_grpc=\"http://remote:18312\"\ncontract_selection=\"far_secondary_oi\"\n",
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.contract_selection, ContractSelection::FarSecondaryOi);
        assert!(toml::from_str::<Config>("contract_selection=\"unknown\"\n").is_err());
    }
}
