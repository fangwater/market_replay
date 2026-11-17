//! Query the public contract-selection API; no local data paths.
use clap::Parser;
use cn_market_proto::{
    cn_futures_market_client::CnFuturesMarketClient, ContractSelectionMode,
    GetContractSelectionsRequest,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:18312")]
    endpoint: String,
    #[arg(long, value_delimiter = ',', default_value = "RB")]
    products: Vec<String>,
    #[arg(long, default_value = "secondary_oi")]
    mode: String,
    #[arg(long, default_value_t = 0)]
    start: u32,
    #[arg(long, default_value_t = 0)]
    end: u32,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mode = ContractSelectionMode::from_str_name(&args.mode.to_ascii_uppercase())
        .ok_or_else(|| anyhow::anyhow!("unknown mode"))?;
    let mut client = CnFuturesMarketClient::connect(args.endpoint).await?;
    let out = client
        .get_contract_selections(GetContractSelectionsRequest {
            products: args.products,
            mode: mode as i32,
            start_trading_day: args.start,
            end_trading_day: args.end,
            ..Default::default()
        })
        .await?
        .into_inner();
    println!("rule={} snapshot={}", out.rule_version, out.snapshot_id);
    for r in out.rows {
        let s = r.selected.unwrap_or_default();
        let next = r.scheduled_next.unwrap_or_default();
        println!("{} {} dominant={} selected={} source_day={} oi={:?} volume={:?} unavailable={} scheduled_next={}",r.trading_day,r.product,r.dominant,s.contract,s.ranking_source_day,s.open_interest,s.volume_lots,s.unavailable_reason,next.contract);
    }
    Ok(())
}
