use super::{
    archive_exchange, make_bar, read_day_parquet, Duration, Exchange, GetHfqMinuteBarsRequest,
    GetHfqMinuteBarsResponse, NaiveDate, Path, Service, Shanghai, Status, TimeZone, MAX_RANGE_DAYS,
};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Generation {
    through: NaiveDate,
    generation: String,
    markets: BTreeMap<String, Market>,
}

#[derive(Deserialize)]
struct Market {
    first_day: String,
    last_day: String,
    last_contract: String,
    scheduled_next: Option<String>,
    next_gap: Option<f64>,
}

fn manifest(root: &Path) -> Result<Generation, Status> {
    let bytes = std::fs::read(root.join("_provenance/hfq/generation.json"))
        .map_err(|error| Status::unavailable(format!("HFQ archive manifest: {error}")))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| Status::internal(format!("invalid HFQ manifest: {error}")))
}

fn day(text: &str) -> Result<NaiveDate, Status> {
    NaiveDate::parse_from_str(text, "%Y%m%d")
        .map_err(|_| Status::internal("invalid trading day in HFQ manifest"))
}

impl Service {
    pub(super) fn hfq_bars(
        &self,
        request: GetHfqMinuteBarsRequest,
    ) -> Result<GetHfqMinuteBarsResponse, Status> {
        let exchange = Exchange::try_from(request.exchange).unwrap_or(Exchange::Unspecified);
        let (directory, _) = archive_exchange(exchange)
            .ok_or_else(|| Status::invalid_argument("exchange is required"))?;
        let product = request.product.trim().to_ascii_uppercase();
        if product.is_empty() || !product.bytes().all(|letter| letter.is_ascii_uppercase()) {
            return Err(Status::invalid_argument("a product code is required"));
        }
        let time = request
            .time
            .ok_or_else(|| Status::invalid_argument("time range is required"))?;
        if !(0..=MAX_RANGE_DAYS * 86_400_000).contains(&time.end_ms.saturating_sub(time.start_ms)) {
            return Err(Status::invalid_argument(
                "invalid HFQ time range (maximum 31 days)",
            ));
        }
        let date_at = |ms| {
            Shanghai
                .timestamp_millis_opt(ms)
                .single()
                .map(|t| t.date_naive())
                .ok_or_else(|| Status::invalid_argument("time bound is out of range"))
        };
        let first = date_at(time.start_ms)?;
        let last = date_at(time.end_ms)?
            .checked_add_signed(Duration::days(10))
            .ok_or_else(|| Status::invalid_argument("end is out of range"))?;
        let root = self
            .cfg
            .baseline_root
            .with_file_name("baseline_data_1min_hfq");
        let generation = manifest(&root)?;
        if !request.generation.is_empty() && request.generation != generation.generation {
            return Err(Status::aborted(
                "HFQ generation changed; reload the history",
            ));
        }
        let market = generation
            .markets
            .get(&format!("{directory}/{product}"))
            .ok_or_else(|| Status::not_found(format!("no HFQ archive for {product}")))?;
        let multiple = self
            .meta
            .read()
            .map_err(|_| Status::internal("metadata lock poisoned"))?
            .multipliers
            .get(&product)
            .filter(|value| {
                value.verified && value.volume_multiple.is_finite() && value.volume_multiple > 0.0
            })
            .map(|value| value.volume_multiple)
            .ok_or_else(|| {
                Status::failed_precondition(format!("no verified volume_multiple for {product}"))
            })?;
        let market_root = root.join(directory).join(&product);
        let mut bars = Vec::new();
        let mut date = first.max(day(&market.first_day)?);
        let last_day = day(&market.last_day)?;
        let end = last.min(last_day);
        while date <= end {
            let path = market_root.join(format!("{}.parquet", date.format("%Y%m%d")));
            if path.is_file() {
                let rows = read_day_parquet(&path).map_err(|error| {
                    Status::internal(format!("read {}: {error:#}", path.display()))
                })?;
                if rows.iter().any(|row| row.contract_id != product) {
                    return Err(Status::data_loss(format!(
                        "unexpected product in {}",
                        path.display()
                    )));
                }
                for row in rows {
                    if let Some(bar) = make_bar(
                        &row,
                        super::yyyymmdd(date),
                        multiple,
                        None,
                        false,
                        true,
                        Some((time.start_ms, time.end_ms)),
                        false,
                    )? {
                        bars.push(bar);
                    }
                }
            }
            date = date
                .succ_opt()
                .ok_or_else(|| Status::internal("HFQ date overflow"))?;
        }
        if manifest(&root)?.generation != generation.generation {
            return Err(Status::aborted(
                "HFQ generation changed; reload the history",
            ));
        }
        if bars
            .windows(2)
            .any(|pair| pair[0].open_time_ms >= pair[1].open_time_ms)
        {
            return Err(Status::data_loss("unordered or duplicate HFQ minutes"));
        }
        Ok(GetHfqMinuteBarsResponse {
            generation: generation.generation,
            basis_trading_day: super::yyyymmdd(generation.through),
            last_trading_day: super::yyyymmdd(last_day),
            last_contract: market.last_contract.clone(),
            volume_multiple: multiple,
            bars,
            scheduled_next: market.scheduled_next.clone(),
            next_gap: market.next_gap,
        })
    }
}
