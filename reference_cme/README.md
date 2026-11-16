# CME Exchange Info Summary

CN / CME 已提供统一只读 HTTP MCP 与中文查询页，包含本项目的历史定义、状态事件、
官方历史交易时段和海外佣金参考。同机其他用户可直接访问：
网页 `http://127.0.0.1:8780/`，MCP `http://127.0.0.1:8780/mcp`，
完整说明 `http://127.0.0.1:8780/usage.md`。服务代码及运维配置位于
`/home/fanghaizhou/cn_futures_db/reference/exchange_mcp/` 和 `ecosystem.mcp.config.cjs`。

给同机其他用户或 AI 使用时，在对方自己的工作目录下载独立 Markdown 说明即可：

```bash
curl -fsS http://127.0.0.1:8780/usage.md -o cn-cme-data-guide.md
```

说明包含 CN/CME 的数据范围、全部接口、参数、返回字段及示例，按 HTTP 调用即可，无需配置 MCP 客户端。

基于 Databento `GLBX.MDP3` 的 CME 六品种历史定义与交易状态查询：

- 股指：`ES`、`NQ`、`RTY`、`YM`
- 商品：`GC`、`CL`
- `definition`：2020 年至今的 point-in-time 合约定义
- `status`：按 UTC 接收日期和具体合约查询交易状态时间线

## 运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python estimate_cost.py
```

API key 从 `.env` 中的 `DATABENTO_API_KEY` 读取。`.env` 已加入 `.gitignore`，请勿提交。

脚本会将目标结束时间 `2026-09-04` 自动截到 `status` schema 当前可用的数据末端，
避免目标时间晚于 Databento 数据更新进度时请求失败。

## 下载

### SK hynix 美股 Nasdaq 十档盘口与成交

可直接转交给其他 agent 的单文件手册见 [Databento 数据流程：海力士与 CME](docs/databento_workflow_skhynix_zh.md)，
完整下载、校验、解析和查询代码均内嵌在同一份 Markdown 中，无需另附脚本。

```bash
.venv/bin/python -u download_skhynix_batch.py > download_skhynix_batch.log 2>&1
```

数据集为 `XNAS.ITCH`，schema 为 `mbp-10` 和 `trades`，只覆盖 Nasdaq
单市场。请求从 `2026-06-01` 到首次运行时两种 schema 共同可用的末端，
包含首日临时代码 `SKHYV`（2026-07-10）及之后的 `SKHY`（2026-07-13 起）。
6 月尚未上市，没有该 ADR 数据。全日下载保留数据源提供的盘前、盘后记录。

压缩 DBN 按 UTC 日分片，保存到 `batch_downloads/<job_id>/`。
`batch_downloads/skhynix_202606_present.json` 记录固定请求区间、代码映射、
费用、job ID、文件大小及 SHA-256。再次运行会恢复已记录任务，不自动追加新日期。
可用 `databento.DBNStore.from_file(path).to_df()` 读取单个文件。

```bash
.venv/bin/python download_status.py
```

数据保存为 `cme_equity_index_status_2024_2026.dbn`。该文件已加入 `.gitignore`。

### 原始商品状态批次

已下载批次包含 `GC`、`CL`、`SR3`、`KRW`、`6E`、`SR1` 从 2020 年开始的
`status` 原始数据。生产查询只解析并展示 `GC` 与 `CL`：

```bash
.venv/bin/python download_non_equity_status_batch.py
```

脚本将 job ID 保存在 `.non_equity_status_batch.json`，中断后再次运行会恢复同一个 job，
不会重复提交。下载结果位于 `batch_downloads/<job_id>/`。

### 六个生产品种的历史定义

`ES`、`NQ`、`RTY`、`YM`、`GC`、`CL` 从 2020 年开始的 point-in-time
`definition` 数据使用独立的可恢复 batch：

```bash
.venv/bin/python download_core_six_definition_batch.py
```

job ID 保存在 `.core_six_definition_batch.json`，下载结果位于
`batch_downloads/<job_id>/`。

解析为分块写入的 Parquet、样本 CSV 和统计 JSON：

```bash
.venv/bin/python parse_core_six_definition.py
```

解析结果位于 `parsed/core_six_definition_2020_present/`。

### 解析六品种状态

将股指状态文件与商品批次中的 `GC`、`CL` 过滤为单腿期货 Parquet：

```bash
.venv/bin/python parse_core_six_status.py
```

解析结果位于 `parsed/core_six_status_2020_present/`。

## 前端查询

```bash
.venv/bin/python -m cme_reference.server
```

默认监听 `0.0.0.0:8770`。可通过 `CME_REFERENCE_HOST` 和
`CME_REFERENCE_PORT` 调整地址。页面提供：

- 选定历史时点的有效单腿期货定义、合约规格与变更记录
- 选定 UTC 日期和具体合约的 `status` 状态时间线

`status` 不是静态交易时间表；它是交易所状态事件流。股指状态当前覆盖
2024-01-01 至 2026-09-03，`GC`、`CL` 覆盖 2020-01-01 至 2026-09-03。

### 与 TAS `Mkt. Condition` 比较

先在 `mkt_signal` 构建只读状态导出器，再运行全重叠区间比较：

```bash
cd /home/fanghaizhou/mkt_signal
CC=/usr/bin/gcc CXX=/usr/bin/g++ \
  BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/x86_64-linux-gnu/13/include \
  cargo build --release -p cme_tas_replay --bin cme_tas_export_status

cd /home/fanghaizhou/cn_futures_db/reference_cme
.venv/bin/python compare_tas_status.py
```

脚本排除 Databento UTC 午夜状态快照，将 LSEG 一位或两位年份 RIC 与 Databento
合约到期年统一，并分别统计计划交易时段、附加状态、异常事件和合约级覆盖。
结果写入 `reports/tas_status_comparison.md` 和
`reports/tas_status_comparison.json`。

### TAS 四阶段写入 PostgreSQL

逐 RIC 归一化只使用可直接对应 CME `SecurityTradingStatus` 的四类状态：

| TAS qualifier | action | 存储语义 |
| --- | --- | --- |
| `4[TRD_TYPE]` + `C[INST_PHASE]` | `CLOSE` | 直接事件 |
| `21[TRD_TYPE]` + `O[INST_PHASE]` | `PRE_OPEN` | 直接事件 |
| `15[TRD_TYPE]` | `NEW_PRICE_INDICATION` | 直接事件 |
| `17[TRD_TYPE]` + `T[INST_PHASE]` | `TRADING` | 直接事件 |

加载器只读生产 TAS RocksDB，检查 `replay_meta` 只扫描 `done` period，默认处理
2020 至 2026-06-01 的六品种全部 RIC。写入使用 staging table 和单一事务替换：

```bash
.venv/bin/python load_tas_actions_postgres.py --write-pg --replace
```

目标库默认为 `market_metadata`（Unix socket、5433、用户 `u171`）。对象如下：

- `public.cme_tas_action_events`：逐 RIC 的直接 CME 状态及原始 qualifier
- `public.cme_tas_action_consensus`：直接状态按品种/action/秒汇总
- `public.cme_tas_action_alignment`：将 5 秒内的状态 15/17 聚成一个开盘边界；缺少一侧时显式标记 `is_inferred`
- `public.cme_tas_action_load_metadata`：来源、period、mapping 版本和加载行数

查询一个品种的可对齐阶段边界：

```sql
SELECT product, action, event_second_utc, is_inferred,
       has_direct_action, ric_count, cluster_span_seconds
FROM public.cme_tas_action_alignment
WHERE product = 'ES'
  AND event_second_utc >= timestamptz '2026-05-01 00:00:00+00'
ORDER BY event_second_utc, action;
```

查询单个 RIC 的直接状态证据：

```sql
SELECT ric, action, event_ts_utc, mapping_rule, qualifiers
FROM public.cme_tas_action_events
WHERE ric = 'ESM26'
ORDER BY event_ts_utc, action;
```

从 PG 反读并与 Databento 比较 exact 和 `±2 秒` precision/recall：

```bash
.venv/bin/python compare_tas_actions_postgres.py
```

报告写入 `reports/tas_action_pg_comparison.md` 和 JSON 同名文件。比较会排除
Databento UTC 午夜快照，以及只是在 `NO_CANCEL` 事件上重复携带当前
`PRE_OPEN` action 的行。PG TAS 表用于 replay 审计，不替代
`cme_globex_daily_trading_intervals` 官方日历表。

### 六品种历史交易时段

将官方日历按六个 product 展开，并把跨 UTC 午夜的连续分片合并为 Chicago
`trade_date` 下的完整 session：

```bash
/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql \
  -h /mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/run \
  -p 5433 -U u171 -d market_metadata \
  -f database/cme_six_product_trading_sessions.sql
```

- `public.cme_six_product_trading_sessions`：最终官方 session；已知 outage 会保留为同一
  `trade_date` 下的多个 `segment_index`
- `public.cme_six_product_session_validation`：同一 session 加上 TAS 开/收盘观测、秒差和
  `MATCHED/PARTIAL/NO_TAS_EVIDENCE/OUTSIDE_TAS_COVERAGE`

查询一个品种的历史交易时段：

```sql
SELECT product, trade_date, segment_index, open_utc, close_utc,
       schedule_regimes, exception_names, confidence
FROM public.cme_six_product_trading_sessions
WHERE product = 'ES'
  AND trade_date BETWEEN DATE '2025-11-24' AND DATE '2025-11-28'
ORDER BY trade_date, segment_index;
```

TAS 状态行没有有效 `Exchange Time`，所以观测列只用于验证官方边界，不用于改写它。
盘中临时停盘、禁撤、隐含撮合及原因仍属于状态事件层，不并入常规 session。

2010、2011、2017–2019 虽有 `done` 分区，但早期 status 可包含大量高频
`MKT_ST_IND`，并不等于这四类阶段字段；可用 `--start-year` 显式扫描，不应只凭
分区存在就自动解释成同语义 action 覆盖。

### 经纪商佣金参考表

用户提供的海外期货佣金清单由以下 SQL 维护在同一个 `market_metadata` 数据库中：

```bash
/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql \
  -h /mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/run \
  -p 5433 -U u171 -d market_metadata \
  -f database/overseas_futures_broker_commission_reference.sql
```

对象为 `public.overseas_futures_broker_commission_reference`。初始版本
`USER_PROVIDED_BROKER_COMMISSION_V1` 覆盖 CBOT、COMEX、NYMEX、CME、CBOE、
NYBOT、ICE、IPE、LME、EUREX、SGX 和 OSE 共 51 条关联代码。表分开保存标的币种和
佣金币种，例如 `BTCEURRR` 的标的币种为 EUR、佣金为 USD。

该来源未给出费率生效日期，也未说明“每合约”是单边还是往返收费。因此初始记录统一标为
`fee_scope=BROKER_REFERENCE`、`historical_exact=false`、`charge_side=UNSPECIFIED`，
不能当作 CME 历史交易所费用或账户历史精确费用。后续新增带有效期和账户证据的版本时，
使用新 `schedule_version`，不得覆盖原始参考版本。

### PM2 后台运行

```bash
npm install --global --prefix /home/fanghaizhou/.local pm2
./scripts/pm2-start.sh
pm2 status
pm2 logs cme-exchange-info-summary
```

更新代码后再次运行 `./scripts/pm2-start.sh` 即可重载。停止并移除服务：

```bash
./scripts/pm2-stop.sh
```

`pm2 save` 会保存当前进程列表。本机已配置用户级 cron，在整机重启后执行
`pm2 resurrect` 自动恢复已保存的 PM2 服务。
