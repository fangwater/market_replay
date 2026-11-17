//! The backtest's market-data boundary. No source paths or database credentials.
use anyhow::{ensure, Context, Result};
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, Exchange, GetContractSelectionsRequest,
    GetProductMultipliersRequest, GetProductMultipliersResponse, GetReplayFramesRequest,
    ReplayFrameBatch,
};
use prost::Message;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tonic::{transport::Channel, Streaming};

pub struct MarketApi {
    pub runtime: tokio::runtime::Runtime,
    client: CnFuturesMarketClient<Channel>,
}
impl MarketApi {
    pub fn connect(endpoint: &str) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())?
            .connect_timeout(std::time::Duration::from_secs(10));
        let client = runtime
            .block_on(async { channel.connect().await })
            .context("connect market gRPC API")?;
        Ok(Self {
            runtime,
            client: CnFuturesMarketClient::new(client),
        })
    }
    pub fn multipliers(&mut self) -> Result<GetProductMultipliersResponse> {
        Ok(self
            .runtime
            .block_on(
                self.client
                    .get_product_multipliers(GetProductMultipliersRequest { products: vec![] }),
            )?
            .into_inner())
    }
    pub fn selections(&mut self, cfg: &crate::Config, products: Vec<String>) -> Result<Selections> {
        let out = self
            .runtime
            .block_on(
                self.client
                    .get_contract_selections(GetContractSelectionsRequest {
                        exchange: Exchange::Unspecified as i32,
                        products: products.clone(),
                        mode: cfg.contract_selection.proto() as i32,
                        start_trading_day: crate::day_number(&cfg.start)?,
                        end_trading_day: crate::day_number(&cfg.end)?,
                    }),
            )?
            .into_inner();
        Selections::decode(out, cfg, &products)
    }
    pub fn replay(&mut self, req: GetReplayFramesRequest) -> Result<Streaming<ReplayFrameBatch>> {
        Ok(self
            .runtime
            .block_on(self.client.stream_replay_frames(req))?
            .into_inner())
    }
}
pub struct Selections {
    pub days: BTreeMap<u32, BTreeMap<String, String>>,
    pub provenance: Value,
}
impl Selections {
    fn decode(
        mut out: cn_market_proto::GetContractSelectionsResponse,
        cfg: &crate::Config,
        products: &[String],
    ) -> Result<Self> {
        ensure!(
            out.mode == cfg.contract_selection.proto() as i32,
            "API returned another selection mode"
        );
        let hash = std::mem::take(&mut out.snapshot_id);
        ensure!(
            hash == format!("{:x}", Sha256::digest(out.encode_to_vec())),
            "selection snapshot hash mismatch"
        );
        let mut days: BTreeMap<u32, BTreeMap<String, String>> = BTreeMap::new();
        let mut audit = Vec::new();
        for row in out.rows {
            let s = row.selected.context("API omitted effective selection")?;
            ensure!(
                !s.contract.is_empty(),
                "no {:?} selection for {} on {}: {}",
                cfg.contract_selection,
                row.product,
                row.trading_day,
                s.unavailable_reason
            );
            ensure!(
                s.ranking_source_day > 0 && s.ranking_source_day < row.trading_day,
                "selection uses same-day/future observations"
            );
            ensure!(
                crate::day_number(&cfg.start)? <= row.trading_day
                    && row.trading_day <= crate::day_number(&cfg.end)?,
                "API selection outside requested range"
            );
            ensure!(
                days.entry(row.trading_day)
                    .or_default()
                    .insert(row.product.clone(), s.contract.clone())
                    .is_none(),
                "duplicate API selection"
            );
            audit.push(json!({"trading_day":row.trading_day,"product":row.product,"contract":s.contract,
                "ranking_source_day":s.ranking_source_day,"dominant":row.dominant,"open_interest":s.open_interest,
                "volume_lots":s.volume_lots,"dominant_rule":row.dominant_rule}));
        }
        ensure!(
            !days.is_empty(),
            "no effective contract selections in range"
        );
        for (day, mapping) in &days {
            for p in products {
                ensure!(
                    mapping.contains_key(&p.to_ascii_uppercase()),
                    "API omitted {p} selection on {day}"
                );
            }
        }
        Ok(Selections {
            days,
            provenance: json!({"mode":cfg.contract_selection,"rule_version":out.rule_version,"snapshot_id":hash,"rows":audit}),
        })
    }
}
pub fn archive_exchange(value: i32) -> Result<&'static str> {
    Ok(match Exchange::try_from(value)? {
        Exchange::Cffex => "ccfx",
        Exchange::Shfe => "xsge",
        Exchange::Ine => "xsie",
        Exchange::Czce => "xzce",
        Exchange::Dce => "xdce",
        Exchange::Gfex => "xgfe",
        Exchange::Unspecified => anyhow::bail!("unspecified exchange"),
    })
}
pub fn exchange(value: &str) -> Result<Exchange> {
    Ok(match value {
        "ccfx" => Exchange::Cffex,
        "xsge" => Exchange::Shfe,
        "xsie" => Exchange::Ine,
        "xzce" => Exchange::Czce,
        "xdce" => Exchange::Dce,
        "xgfe" => Exchange::Gfex,
        _ => anyhow::bail!("unknown exchange {value}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cn_market_proto::{ContractSelection, GetContractSelectionsResponse, SelectedContract};
    fn response(source: u32, contract: &str) -> GetContractSelectionsResponse {
        let mut r = GetContractSelectionsResponse {
            mode: crate::config::ContractSelection::NearSecondaryOi.proto() as i32,
            rule_version: "test/v1".into(),
            snapshot_id: String::new(),
            rows: vec![ContractSelection {
                product: "RB".into(),
                trading_day: 20261113,
                dominant: "rb2701".into(),
                selected: Some(SelectedContract {
                    contract: contract.into(),
                    ranking_source_day: source,
                    ..Default::default()
                }),
                scheduled_next: Some(SelectedContract {
                    contract: "rb2705".into(),
                    ranking_source_day: 20261113,
                    ..Default::default()
                }),
                ..Default::default()
            }],
        };
        r.snapshot_id = format!("{:x}", Sha256::digest(r.encode_to_vec()));
        r
    }
    #[test]
    fn consumes_effective_api_choice_without_reselecting_main_or_scheduled_next() {
        let cfg = crate::Config {
            contract_selection: crate::config::ContractSelection::NearSecondaryOi,
            ..Default::default()
        };
        let s = Selections::decode(response(20261112, "rb2612"), &cfg, &["RB".into()]).unwrap();
        assert_eq!(s.days[&20261113]["RB"], "rb2612");
    }
    #[test]
    fn rejects_missing_future_or_modified_selection() {
        let cfg = crate::Config {
            contract_selection: crate::config::ContractSelection::NearSecondaryOi,
            ..Default::default()
        };
        assert!(Selections::decode(response(20261113, "rb2612"), &cfg, &[]).is_err());
        assert!(Selections::decode(response(20261112, ""), &cfg, &[]).is_err());
        let mut r = response(20261112, "rb2612");
        r.rows[0].selected.as_mut().unwrap().contract = "rb2705".into();
        assert!(Selections::decode(r, &cfg, &[]).is_err());
    }
}
