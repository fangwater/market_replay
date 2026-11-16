# IB 期货分钟线：逐 RIC Parquet 与首末时间

用户于 2026-11-03 要求对已收录品种整理单 RIC Parquet，并对照 IB／TAS 首末时间，随后明确本轮只导出 IB。本轮范围为已经确认 TAS 对应产品的 44 个 IB 期货 symbol，不含未找到同规格源的 19 个代码，不导出股票／ETF，不重新导出 TAS。

2026-11-03 已完成导出与交付验收：44 个期货品种、1,077 个单 RIC 文件、184,800,816 行、7,444,248,402 字节（约 7.44 GB）。每个文件全量读回校验通过，Python 独立检查文件集合、schema、footer、首末和行数通过。完成依据为输出 `_SUCCESS.json`、`_audit/delivery.json` 及项目 [完成回执](../artifacts/ib_ric_export_20261103/completion.json)。

## 位置与范围

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/ib_minute_parquet/covered_futures_20261103/
  data/CLH27.parquet
  data/STFM6.parquet
  data/ESH26.parquet
  _audit/contract_plan.json
  _audit/tas_bounds.json
  _audit/jobs/<RIC>.json
  _audit/rejected/<RIC>.jsonl
  _audit/ric_inventory.csv
  _audit/ric_inventory.parquet
  _audit/contract_exclusions.csv
  _audit/delivery.json
  _SUCCESS.json
```

每个实际 RIC 一个文件，一个文件只含一个唯一 IB con_id，保留其全部现有有效历史，不按年拆分，也不只截取 2026 年。源为只读 `ib_data.minute_data`，不使用股票分钟表。项目运行证据在 `artifacts/ib_ric_export_20261103/`。

本轮准备阶段：44 个品种下有 1,379 个元数据合约；1,271 个经实际 HRD 核定，其中 1,077 个有有效 IB 分钟、194 个没有有效分钟。另 66 个属于规格／场所／币种不匹配（含 SI 的 SIL、DAX 迷你／微型），42 个未在实际交付 HRD 中核定相应到期工具，全部单列原因，未凭相似代码补造 RIC。

## 合约映射

- 核对 IB symbol、local_symbol、交易所、币种、完整规格，并检查实际供应商 HRD 的到期信息。
- 交割月取 local_symbol，不能用 maturity_date 的月份直接替换。例如 CLM6 的 IB 到期日为 2026 年 5 月，交割月仍为 6 月。
- Eurex 的日期型和英文月份型 local_symbol 分别解析；JGB 的数字交易所代码使用专门规则并以 HRD 到期信息核定。
- TSR20 的 IB 最后交易日在前月末，而 HRD 日期在交割月。例如 TFM26 的 IB 到期日 2026-05-29，STFM6 的 HRD 日期 2026-06-08／09。通过交割月、XSIM、TF、USc、5,000 KG 的实际身份核定，不按日期直接拼接错误月份。
- 月度与周度 VIX 分别核定，周度根保留 VXnn，不能全部并到 VX。
- 同一个真实合约可能历史上有一位／两位年份 RIC。HRD 核定后记录实际别名，IB 文件使用最新交付中的实际 RIC 作为 canonical RIC。TAS 原始代码不改写；首末比较联合查询已核定的同合约别名。

## 标准字段与保真

核心列为 `ric, ts_utc_ns, open, high, low, close, volume`，另保留全部 IB 源列及质量追溯信息：

| 字段 | 类型／含义 |
| --- | --- |
| `ric` | String，已核定的实际 RIC |
| `ts_utc_ns` | Int64，依据源 utc_time 精确计算 UTC 纳秒 |
| `open, high, low, close, volume` | nullable Float64，保留源 DOUBLE 原值，不做价格缩放或成交量换算 |
| `con_id, symbol, exchange, maturity_date` | 合约标识和源行身份字段 |
| `id` | MySQL 源行 ID，可直接定位 |
| `utc_time` | timestamp[ns, UTC]，与 ts_utc_ns 一致 |
| `time_cst` | nullable timestamp[ns]，保留源墙上时间，不施加交易所时区转换 |
| `timestamp` | nullable Int64，源毫秒时间戳，含错误零值也原样保留 |
| `sec_type` | 源证券类型 |
| `created_at` | nullable timestamp[ns, UTC]，MySQL TIMESTAMP 在 UTC 会话下读取 |
| `utc_time_raw, time_cst_raw, created_at_raw` | 源时间字符串；包含无法转换的值，便于完整重建源行 |
| `time_quality_flags` | UInt16：1=缺 epoch；2=epoch 与 UTC 不符；4=CST 无效；8=CST 偏移不符；16=不是整分钟标签 |
| `ohlcv_quality_flags` | UInt16：1=OHLC 空／非有限；2=OHLC 约束不满足；4=成交量空／非有限；8=负成交量 |
| `identity_matches_metadata` | Boolean，源 symbol／exchange／maturity／con_id 与已核定元数据是否一致 |

不填缺失分钟、不做前后复权、不删除零成交量、不将负价格自动判为错误。UTC 无效的行保存完整原始内容到 `_audit/rejected/`，不猜测修复；UTC 有效、timestamp 错误时保留行情并标记。其它 OHLCV 和身份异常仍保留原值并标记。

每个文件的 schema metadata 记录合约、币种、乘数、交割月、源 ID 水位及字段口径；不把 IB 价格擅自转成 TAS 的 e9 定点表示。

## 首末时间对照

IB 使用有效 utc_time 的第一条、最后一条分钟标签，不过滤零成交量。TAS 只读索引查询所有已完成源年列族的对应真实合约：Trade 使用源 Date-Time，Quote 使用既有 Exch Time 秒桶，并在证据中保留其源更新时间。通过完整到期年限制一位年码的十年周期，避免混入前一轮同名 RIC。

逐 RIC 清单保存双方的首末 UTC、纳秒、别名、分钟行数、时间差和范围交集状态。这里比较的是首末范围；没有核定 IB 标签代表分钟开头还是结尾，因此保留原标签，不进行一分钟平移，也不据此断言逐分钟完整或 OHLC 一致。

## 快照与验证

- 用有界 con_id 索引读取，每个工作连接为只读一致性事务，固定源 id 上界；不是跨连接同一物理时刻的全库快照。
- 原值逐行生成 SHA256；Parquet 写入临时文件后全列、全行读回，重建原始 15 列，比较 SHA256、条数、排序、首末、标准时间与质量标记，通过后才改为正式文件。
- Python 最后独立核对完整文件集合、schema、footer、首末和行数；输出首末时间 CSV／Parquet、中文 Excel 和交付回执。
- 真实样本 6 个 RIC、1,058,926 行逐行读回通过；独立 PyMySQL 核对 36 行源数据，验证 OHLCV、标识与时间原值完全一致，包括一条有效 UTC 对应零 epoch 的异常。
- CL 交割月、TSR20 日期差异、唯一 con_id／RIC 及 44 品种覆盖已作实际映射核对。

实现：[准备与映射](../scripts/prepare_ib_ric_export.py)、[Rust 导出](../src/bin/ib_ric_parquet.rs)、[TAS 首末索引探针](../src/bin/tas_ric_bounds.rs)、[交付与对照报告](../scripts/finalize_ib_ric_export.py)。Cargo 构建产物仍使用项目独立 NVMe target。

## 本批结果

IB 实际时间范围为 `2023-05-30T11:57:00.000000000Z` 至 `2026-11-03T05:19:00.000000000Z`，各 RIC 的窗口不同。固定源 ID 上界为 `254032149`。无效 UTC 隔离行 0；12 行时间异常原值均保留：11 行源 timestamp=0，其中 2 行 CST 同时错位；另 1 行 CST 为零日期。规范时间依照源 utc_time 生成，不对异常事件时间作猜测修复。OHLCV 约束异常和源身份不一致均为 0。

| 首末分钟标签范围 | RIC 数 |
| --- | ---: |
| 成交分钟标签范围有交集 | 928 |
| 成交分钟标签范围无交集 | 88 |
| 仅TAS报价可对照 | 13 |
| 该合约TAS无成交报价 | 48 |

有交集是首末范围有交集，不表示逐分钟无缺口。无交集也不直接说明哪方错误。48 个无 TAS 成交／报价的具体合约仍有 HRD 身份证据和 IB 分钟；品种存在不代表每个到期合约都已经交付行情。

[中文首末时间对照 Excel](../reports/IB分钟线_逐RIC首末时间对照_20261103.xlsx) 列出每个 RIC 的 IB／TAS 首末 UTC、交割年月、IB 到期日、时间差、别名及文件。首条／末条时间异常码单列，Excel另有“时间异常明细”页。输出 `_audit/ric_inventory.parquet` 和 `.csv` 可直接程序读取，`_audit/time_anomalies.parquet` 保存全部 12 行异常。CLH27 的首条记录同时存在零 epoch 与 CST 错位，首末统计保留其原 UTC 并标记，不能将该统计边界直接视为已确认的真实行情开始时间。

未导出合约共 302 个：194 个无有效 IB 分钟；66 个规格／场所／币种不符；42 个未在实际交付 HRD 核定该到期合约。逐项原因保存于 `_audit/contract_exclusions.csv` 和 Excel。

读取单个 RIC：

```python
import pyarrow.parquet as pq
file = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/ib_minute_parquet/covered_futures_20261103/data/ESH26.parquet"
table = pq.read_table(file, columns=["ric", "utc_time", "open", "high", "low", "close", "volume"])
```
