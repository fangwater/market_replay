# TAS Trade / Quote Parquet

用户已确认保留全部原始 RIC（含指数与连续报价），先导出 2026 年。源是现有全品种 TAS RocksDB，不重新读取原始 CSV、不再采样或补齐。

2026-11-02 已完成 2026 年现有 1–5 月源包的全部 175 个列族导出与验收：

| 类型 | RIC 数 | 行数 |
| --- | ---: | ---: |
| trade | 1,533 | 655,182,546 |
| quote | 1,731 | 2,272,568,140 |

原始 RIC 并集 1,849 个，数据文件共 3,493 个、53,218,483,628 字节（约 53.2 GB）。每行的源 value SHA256 读回比较通过，Python 对所有文件 footer、schema、文件集合和列族/全局行数的独立核对通过；源 CURRENT 前后均为 `MANIFEST-2778920`。实际运行约 727 秒，包含全部 Parquet 逐行读回。

完成标记为输出根的 `_SUCCESS.json`，交付验收为 `_audit/delivery.json`，逐 RIC 清单为 `_audit/ric_inventory.csv` 和 `_audit/ric_inventory.parquet`。1,849 是库内实际存在 kind 1 / 3 的 RIC 数，不等于源 Report 中所有事件类型的活跃 RIC 数。

## RIC 身份

RIC 是 LSEG 工具代码，保留大小写、历史后缀和原始拼写。`ESH26` 是 2026 年 3 月 ES；`CLG26` 是 2026 年 2 月 WTI。月份码为 `F/G/H/J/K/M/N/Q/U/V/X/Z`，对应 1 至 12 月。

- `URO`、`JY`、`TU/FV/TY/US/AUL` 等是 LSEG 根，采购 ticker 可以是 `6E`、`6J`、`ZT/ZF/ZN/ZB/UB`，不能混当原始 RIC。
- `Cc1` 至 `Cc17` 是玉米连续报价别名，不是可可。直接证据为现有 TAS Reference Change 审计中 `Cc1` 的 `CORN MAR25 → CORN MAY25 → CORN JUL25` 等描述变化。旧页把 `Cc*` 写成可可是错误说明。本项目不更改该源代码，也不把它当作固定到期合约。
- `.NSEI`、`.FTXIN9`、`.FTFTCRTWNT` 是指数；其点位刷新可能在既有 `kind=1` 布局中，不能把所有输出 trade 行都解释为期货真实成交。
- `LCOTOT`、`FCPO-TOT` 等为品种累计总量类代码。只有实际存在的 `kind=1/3` 记录进入本次输出，不把 `kind=8` 总量转成成交，也不为无记录的类型制造空行情。
- `NGLND` 家族含日合约和其它期限代码，不能用普通月合约规则硬拆；`VXF3:VE` 等别名也须保留原值。
- 早期一位年份代码可跨十年重复，例如源 Report 中 `ESH0` 同时出现在 2010 和 2020 周边。按源年份隔离，未核合约参考信息时不擅自展开完整到期年或合并身份。

完整源元数据清单由 `scripts/lseg/inventory_tas_rics.py` 生成到 `artifacts/ric_inventory_20261102`。17 个源周期包含 53,348 个活跃 RIC/period 组合、15,916 种原始 RIC 字符串；这是源 Report 盘点，不是库中 trade/quote 的实际条数。库中实际输出集合以本次导出审计为准。

## 最终盘的当前交付标准

用户随后明确全量读回过于冗余，当前正式交付使用 `--readback sampled`：每个列族的 trade／quote 各选首／中／末最多3个文件完整读回，其他文件检查 footer／schema／条数／RIC／UTC日期范围／大小；目标复制也仅样本做完整文件SHA256。下文原快照的全量验证是历史已执行记录，不再要求本轮所有文件重复执行。

2026-11-09 用户指定最终目录为 `lseg/<交易所小写>/<年份>/<原始RIC>/<YYYYMMDD>/trade.parquet` 或 `quote.parquet`，UTC 日期来自 `ts_utc_ns`。导出器增加 `--layout ric-day --exchange-map <分组JSON>`，直接只读扫描 RocksDB；每个 RIC／日期／类型一个文件，不按旧单文件行数上限拆 part。当前正重新导出本轮增量及补充数据，见 [日包增量与新布局](tas_daily_increment_20261109.md)。下节是保留不覆盖的本机旧快照，不代表当前最终盘目录标准。

## 原本机快照输出目录

```text
/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/2026/
  trade/ric=ESH26/part-00000.parquet
  quote/ric=ESH26/part-00000.parquet
  trade/ric=Cc1/part-00000.parquet
  trade/ric=.NSEI/part-00000.parquet
  _audit/run.json
  _audit/p%3A2026%3AES.json
  _SUCCESS.json
```

目录中的 RIC 使用可逆 ASCII 百分号转义，记录内 `ric` 始终为原始字符串。每文件最多 4,194,304 行，Zstd 压缩，默认 row group 为 131,072 行。一个 RIC 数据多时拆多个 part，不截断历史。

2026 源 period 为 `2026-01-01_2026-06-01`，即 1 月至 5 月；不能把它称为完整 2026 年。读取全部 `p:2026:*` 列族中的 kind 1 / 3，不增加研究品种、交易时段、正价格或成交量过滤，不去重。

2026-11-09 另行交付此前新增 13 个补充列族的 Parquet：128 个 RIC、232 个文件、2,733,379,422 字节，其中 Trade 30,061,381 行、Quote 127,098,723 行。排除 5 个与原库完全重复的列族，未修改本页原快照及其统计。补充源窗口为 `[2026-01-01,2026-05-30)`，不含 5 月 30–31 日；文件以 `supp-futures11-20260101-20260530-` 为前缀，按交易所写入最终 CTA_Data 目录。完成回执及 6 月起日包进度见 [本轮 TAS 交付](tas_daily_increment_20261109.md)。

## 保留字段和精度

| 数据 | 字段 |
| --- | --- |
| trade | `ric, ts_utc_ns, exch_hms_ns, price, volume, bid, bid_size, ask, ask_size, aggressor` |
| quote | `ric, ts_utc_ns, exch_hms_ns, bid, bid_size, ask, ask_size` |

- `ric`: UTF-8；时间为 UInt64 UTC 纳秒 / 日内纳秒；价格为 Int64；数量为 UInt32；aggressor 为 UInt8。
- 价格保留既有 `×1e9` 定点整数，不转 float，不损失精度。展示实际价格时除以 `1e9`，missing 须先剔除。
- 缺失值保持原哨兵：价格 `i64::MIN`、数量 `u32::MAX`、日内时间 `u64::MAX`。未替换为零或 null，schema metadata 保存缩放和缺失约定。
- `magic/version/kind/pad` 是固定二进制头，kind 已由 trade/quote 目录区分；源 key 的 part/seq 没有增加为用户数据列。保持源 key 顺序和重复记录。
- Quote 仍是源库已有的每 RIC、每秒最后完整侧 BBO。`ts_utc_ns` 是 value 的最后源更新时间，不替换成 key 的整秒时间；没有 Quote 的秒不补行。

## 实现与验证

代码在当前项目；Cargo 构建产物在 NVMe 的 `cargo-target/market_replay`。
在仓库根目录用 `scripts/build-release.sh` 构建（它会设置 `LIBCLANG_PATH`）。

- 以只读模式打开 TAS，确认目标 period 为 done；数据写独立 HDD 目录。
- 每个文件先写 `.parquet.partial`，close/fsync 后原子改名。
- 每个输出文件均全列、全行读回，重建对应的原始 80B / 64B TAS value，比较 SHA-256、行数和首末记录。每个列族的审计记录 RIC、文件、行数、字节数和源 value 哈希。
- 所有列族结束并通过读回后才生成 `_SUCCESS.json`；出现文件不代表全任务完成。
- 真实 ES / Cc1 / NSEI 小样本为 5 文件、5,000 行，已通过 Rust 全量读回和独立 Python 字节重建对照；Python 工具为 `scripts/lseg/verify_parquet_sample.py`。
- `scripts/lseg/summarize_export.py` 在完成后独立检查所有文件 footer、schema、列族与总行数、文件集合和残留 partial，并生成 `_audit/ric_inventory.csv` / `.parquet` 与 `delivery.json`。

运行示例：

```bash
/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/lseg_tas_parquet \
  --year 2026 --period-end 2026-06-01 \
  --output /mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/2026 \
  --workers 8
```

读取单个文件：

```python
import pyarrow.parquet as pq
df = pq.read_table("/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/2026/trade/ric=ESH26/part-00000.parquet").to_pandas()
```

