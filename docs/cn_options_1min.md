# 国内商品期权 1 分钟底表（`cn-options-1min`）

将国内五家商品交易所（`XDCE`、`XGFE`、`XSGE`、`XSIE`、`XZCE`）通联商品期权 1 分钟线清洗为与期货 `baseline_data_1min` 一致的日分区 Parquet。

## 产物与口径

- **输出根目录：** `/mnt/hdd-raid5-72t/liang_torch/cn_futures_option_data/baseline_data_1min`
- **目录布局：** `{exchange}/{PRODUCT}/{TradDay:%Y%m%d}.parquet`（`exchange` 为小写 `xdce`/`xgfe`/`xsge`/`xsie`/`xzce`，`PRODUCT` 为大写品种码）
- **物理格式：** ZSTD 压缩、单 Row Group、按 `(contract_id, ts)` 严格升序去重（同键保留文件内最后一条 / 最大 `SeqNo`），先写同目录 `.tmp` 并校验 footer 行数后再原子 rename
- **严格 9 列 Schema（零衍生列）：**

| 列名 | 类型 | 历史日包来源 | 每日 MDL 来源 | 说明 |
| --- | --- | --- | --- | --- |
| `contract_id` | `string` | `CONTRACTID` | `SecurityID` | 原始期权合约代码（仅将 `XSIE` 历史日包开头品种字母统一小写，对齐每日 MDL 与期货 `baseline_data_1min`） |
| `ts` | `int64` | `TDATE` + `bartime` | `ActionDay` + `BarTime` | UTC 分钟左端 Unix 秒（通联右端点 `- 60s`，与期货 `baseline_data_1min` 的 `ts` 完全同口径） |
| `open` | `double` | `openprice` | `OpenPrice` | 开盘价 |
| `high` | `double` | `highprice` | `HighPrice` | 最高价 |
| `low` | `double` | `lowprice` | `LowPrice` | 最低价 |
| `close` | `double` | `closeprice` | `ClosePrice` | 收盘价 |
| `volume` | `double` | `volume` | `TotalVolume` | 本分钟成交手数 |
| `amount` | `double` | `value` | `TotalValue` | 本分钟成交额（元，已含乘数） |
| `open_interest` | `double` | `OPENINTS` | `OpenInterest` | 分钟末持仓量 |

## 切层与清洗规则

- **硬切层（`cutoff_day = 2026-07-31`）：**
  - `TradDay <= 2026-07-31`：仅读历史日包（`http://10.61.20.12/.../future_opt_pricemin/`）；
  - `TradDay >= 2026-08-01`：仅读每日 MDL 落盘（`http://10.61.20.11/users/el01/public/tonglian/data/`）；
  - 同一个 `TradDay` 严禁混用两层。
- **连续竞价过滤：** 仅保留有效连续交易分钟右端点（`09:01..=10:15`、`10:31..=11:30`、`13:31..=15:00`、`21:01..=23:59`、`00:00..=02:30`），自动剔除 `09:00` / `21:00` 集合竞价 bar（与期货 `baseline_data_1min` 一致）。保留连续段内 `volume == 0` 行。
- **每日 MDL 多周期与跨日归属：**
  - `mdl_13_94_0.csv`（`XSIE`）和 `mdl_13_100_0.csv`（`XGFE`）强制过滤 `Unit == 1`；
  - 扫描目标交易日 `D` 之前最近 2 个自然日目录（覆盖前一交易日夜盘与周六凌晨）及 `D` 目录，严格按行内 `TradDay == D` 归属。

## 命令行用法

配置文件：[config/cn_options_1min.toml](../config/cn_options_1min.toml)，状态文件：`/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/cn_options_1min_state.json`。

```bash
# 历史日包回补 (TradDay <= cutoff_day)
cn-options-1min backfill-hist --config config/cn_options_1min.toml --workers 6

# 每日 MDL 回补 (TradDay > cutoff_day)
cn-options-1min backfill-daily --config config/cn_options_1min.toml --workers 4

# 盘后增量日更（含钉钉通知，供 market-replay-server 定时调度）
cn-options-1min run --config config/cn_options_1min.toml

# 重叠日两层逐行对账
cn-options-1min verify-overlap --config config/cn_options_1min.toml --day 2026-08-03
```

## 接入 `market-replay-server` 每日任务

提交并通过 `scripts/deploy-release.sh <commit>` 部署后，可在 `config/market_replay_server.toml` 中追加如下 `[[job]]`：

```toml
[[job]]
name = "cn-options-1min-daily"
description = "国内商品期权 1 分钟底表：每日通联 MDL 增量清洗为 baseline_data_1min Parquet（钉钉）"
cwd = "/home/fanghaizhou/market_replay"
command = "/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin/cn-options-1min"
args = ["run", "--config", "/home/fanghaizhou/market_replay/config/cn_options_1min.toml"]
schedule = { tz = "Asia/Shanghai", at = ["18:30"] }
timeout_minutes = 90
```

## 首轮全量回补与验收记录（2026-11-27）

- **两层重叠对账（`2026-08-03`）：** `xdce`（2,682,510 行）、`xgfe`（996,300 行）、`xsge`（2,630,610 行）、`xsie`（400,890 行）四所键 100% 一致且 OHLCVA 0 差异；`xzce` 历史日包在 `15:00` 存在负成交额异常而每日 MDL 正常，印证 `cutoff_day = 2026-07-31` 的合理性。
- **全量回补结果：** 覆盖 `2017-03-31` 至 `2026-11-26` 共 **2,345 个交易日**（历史日包 2,267 天 + 每日 MDL 78 天）、**69 个商品期权品种**、**61,461 个 Parquet 文件**、**7,916,614,833 行**，磁盘占用 **7.1 GB**，残留临时文件 0 个。
- **抽样读回验证：** 抽验 `2017-03-31`、`2026-07-31`、`2026-08-03`、`2026-11-26` 跨 5 所 6 个文件，9 列 schema、ZSTD、单 Row Group、零空值、严格升序唯一 `(contract_id, ts)` 全部通过。
