# Domestic futures L2 replay

`crates/cn_futures_l2` is the only maintained implementation for replaying
Tonglian domestic-futures L2 data and exporting its derived parquet datasets.
It is a workspace crate in `/home/fanghaizhou/mkt_signal`.

The crate replaces the retired root binary `tonglian_baseline_replay`. That
binary wrote independent 5-second and 60-second ClickHouse staging tables and
used `config/tonglian_baseline_*.toml`; its source, binary registration, and six
configuration files have been removed. Do not use or restore that path.

The replacement pipeline writes one production RocksDB and derives these
outputs from it:

- dense contract-level `backtest_1s` parquet;
- contract-level `baseline_data_1min` parquet;
- `ylabel_1min` parquet;
- dominant continuous `baseline_data_1min_hfq` and `ylabel_1min_hfq` parquet.

Run the replay from the workspace root:

```bash
cd /home/fanghaizhou/mkt_signal
cargo run --release -p cn_futures_l2 -- \
  --l2-root /mnt/nfs/30.3_tonglian_comm_l2 \
  --msg-root /mnt/nfs/30.3_tonglian_msg \
  --start 2025-11-01 --end 2025-11-30 \
  --workers 32
```

Export binaries use the same package selector, for example:

```bash
cargo run --release -p cn_futures_l2 --bin export_backtest_1s -- --help
cargo run --release -p cn_futures_l2 --bin export_baseline_1min -- --help
cargo run --release -p cn_futures_l2 --bin export_ylabel_1min -- --help
cargo run --release -p cn_futures_l2 --bin export_hfq_1min -- --help
```

The data contracts remain in the sibling `preprocess` documentation repository:

- `../preprocess/data_format/cn_pipeline.md` from the workspace parent;
- `../preprocess/data_format/cn_l2_rocksdb.md` from the workspace parent;
- `../preprocess/data_format/cn_backtest_1s.md` from the workspace parent;
- `../preprocess/data_format/cn_baseline_1min.md` from the workspace parent;
- `../preprocess/data_format/cn_ylabel_1min.md` from the workspace parent;
- `../preprocess/data_format/cn_hfq_1min.md` from the workspace parent.

Those documents define source precedence, session handling, storage layout,
field semantics, and output paths. Code changes must preserve those contracts.

## msg 目录选择（2026-11-15）

交易日 D 的 `tonglian_msg` 源文件，只取 D 当天的目录，以及从 D 往前直到最近一个开市日的目录（以含中金所 `mdl_21_1_0.csv` 为准，夜盘从那天晚上开始），中间的周六、节假日目录也包含在内。`--lookback-days`（默认 16）只作为向前查找的上限；窗口内找不到开市日目录时，保留整个窗口，和原来的行为一样。

以前每个（交易所，交易日）任务都会完整读一遍 16 天内的全部目录，51 个交易日的回补读了 5.6 TB，原始数据其实只有约 400 GB。换成新规则后，单个交易日的回放快了 3 倍以上。

验证：取 09-28（中秋假期后的周一）、10-08（国庆后第一天）、11-09（周一，夜盘跨周六）、11-11（普通周三）4 个交易日 × 6 个交易所，新旧两版分别写临时 RocksDB 后比对。回放统计 24/24 一致；用同一导出程序导出的 1 分钟 parquet 316/316、1 秒 parquet 316/316（5,486 万行）逐值一致。耗时方面，旧版分别为 1372/926/1789/277 秒，新版每天约 300 秒。

## 单日导出提速（2026-11-15）

`export_baseline_1min` 原先会扫描每个品种年度分区里的全部记录，导出一天要约 18 分钟。现在改了两处：

- `list_instruments`：读到一个合约后直接 seek 到下一个合约，不再逐条遍历。
- `scan_instrument`：只读取 `[首个导出日 − 7 天, 最后导出日 + 2 天)`（上海时间零点）这个窗口。断段、前填和 TWAP 种子都只在连续交易段内传递，交易日 D 之前一定隔着上一交易日收盘后的休市，窗口外的数据不可能影响导出结果。

验证：
- 合约列表在生产 RocksDB 的 p:2026:LC、p:2026:SO、p:2026:AU、p:2025:RB 上与原全量扫描逐个一致。
- 08-27、08-31、09-07、09-11、09-15 逐日单独导出后，用同一快照裁剪，与生产文件 5 × 79 个逐值一致。
- 单日导出由约 18 分钟降到约 20 秒。

`export_backtest_1s`（`scan_kind`）仍是全量扫描，每日任务不用它。

## 改核心后的 RocksDB 逐条比对（2026-11-17）

改动 `cn_futures_core` 的事件状态机后，除了比对 parquet，还要直接比对 RocksDB 里的成交、盘口、持仓记录：把新二进制的重放写到临时库，再用 `examples/compare_l2_db` 与生产库（只读）逐条比较。临时库只放要验证的那一天；生产库是参照物，不得写入。

```bash
cn_futures_l2 --msg-root /mnt/nfs/30.3_tonglian_msg --exchange xsge --start D --end D --rocksdb-dir <临时目录>/db --workers 8
cargo run --release -p cn_futures_l2 --example compare_l2_db -- <临时目录>/db /mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/cn_l2_rocksdb
```

2026-11-17 对 20261116 的上期所、郑商所、大商所、中金所重放（可续算状态机上线后），共 15,042,235 条记录与生产库逐字节相同，缺失和多余都是 0。因为大小单分桶的输入就是这些成交记录，分桶结果也不会变。

## 大小单分桶与阈值文件

`fill_1min_size_buckets` 现在只有两种用法，共用 `cn_futures_l2::size_thresholds`（文件格式、校验、月份规则）：

```bash
# 覆盖日文件（工序 4）：必须同时给 --start 和 --end，只改写范围内文件的 12 个分桶列
fill_1min_size_buckets --start 2026-11-17 --end 2026-11-17 --workers 4
# 只生成某个品种某个来源月的阈值文件（月任务用），不碰 parquet
fill_1min_size_buckets --product SN --export-month 202610
```

- **阈值：** 来源月里每个交易日只取当天**主力合约**（`roll/result/<ex>_dominant.csv`，用 `--dominant-dir` 改）的区间成交额，逐日合并后取线性 P50/P90；一笔成交用"它自己上海时间所在月的上月"的阈值。2024、2025 两年及 2026 年 1–10 月已于 2026-11-19 正式回补，和 11 月交易日采用同一口径。2023 年及以前的档案使用旧规则（同月、品种全部合约合并）。
- **文件：** `--threshold-dir`（默认 `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/size_thresholds`）下 `dominant_by_day/<YYYYMM>/<品种>.json`，原子写入；覆盖日文件时缺文件会现算并写入。
- **缺阈值：** 上月没有该品种的主力成交（新上市）时，该品种当天文件原样保留，标准错误输出 `MISSING-THRESHOLDS ...`，不写 0、不用别的月份替代。
- **改动安全：** 只改写 `--start/--end` 范围内的日文件；`cn-daily` 传本次处理的第一天到最后一天。
- **验证工具：** `examples/dominant_thresholds_check` 从 RocksDB 和主力表独立重算 P50/P90（逐合约 seek、自己的 CSV 解析和百分位），与导出文件比较。

## 1 分钟 TWAP 口径（2026-11-23）

`twap` 是一分钟内"最新成交价"阶梯函数的时间加权平均。同一连续交易段内，分钟开头沿用上一分钟最后一笔成交价，整分钟都有价格。每个连续段的首分钟（夜盘、日盘开盘和小节休息之后，如 21:00、09:00、10:30、13:30）没有可沿用的价格：从分钟开始到第一笔快照这段时间不计入，分子和分母都只取第一笔之后的部分。

此前分母固定为 60 秒，没有价格的那段等于按 0 计入，首分钟 TWAP 系统性偏低，可低于当分钟最低价：上期所首笔快照约在 0.5 秒，偏低约 0.83%；大商所约 0.25 秒，广期所约 0.15 秒。Manager 的 1 分钟 TWAP 参考净值因此失真。

实现只有 `cn_futures_core::baseline_1min::twap_from_prints` 一处，盘后导出、盘中缓存、实时分段和 `patch_1min_twap` 共用。HFQ 以前因为无法平移这种不完整的 TWAP，把每段第一个有成交分钟的 `twap` 置空；新口径下所有价格同加一个价差，TWAP 平移是精确的，`apply_hfq_series` 不再置空。5min/15min/1h/1day HFQ 粗频底表的 `twap` 仍是各分钟 TWAP 的等权平均，以前每段第一根因首分钟为空而为空，现在有值。

CME（`cme_tas_replay`）和美股（`usstock_lseg_raw_export`）各自有一份同样口径的实现，本次未改。

### 历史数据修复（2026-11-23）

按用户要求，用 `161dc00` 的 `patch_1min_twap` 修复了生产 `baseline_data_1min` 与 `baseline_data_1min_hfq` 的全部历史（2019-08 至 20261123）。只改 `twap` 列。

- **规则：** 用同一批 RocksDB 成交，按旧口径重算一遍；只有能复现生产值的行才写入新值。第一次试跑直接用当前 RocksDB 重算全部 TWAP，有 14,752 行由有值变空、497 行变低。原因是部分旧交易日（多见于 2019–2021 和郑商所）的生产文件来自与当前 RocksDB 不同的成交，这一版没有采用。
- **原始 1 分钟：** 121,658 个文件、约 4.01 亿行，修正 3,311,580 行。另有 59,148 行（约 1.8%）的成交无法复现生产值，原样保留。
- **HFQ 1 分钟：** 121,539 个文件、约 4,027 万行，原始 TWAP 被修正的各段首分钟补值 383,230 行。
- **校验：** 逐文件、逐列比对，结果如下：
  - 除 `twap` 外各列完全一致；
  - 没有 TWAP 变低，也没有有值变空；
  - 改动后的 TWAP 都落在当分钟的 `[low, high]` 内；
  - 抽样日期上，修补工具的 HFQ 输出与 `export_hfq_1min` 基于修复后原始数据的导出逐值一致。
- **替换方式：** 新文件写入同一文件系统上的 staging（复制生产目录 ACL），在当天 `cn-daily`、`cn-hfq-daily`、`cn-hfq-bars-daily`、`cn-reference-daily` 之后逐文件替换：先把原文件硬链接到备份目录，再原子重命名新文件。共替换原始 121,655 个、HFQ 119,019 个文件；TWAP 没有变化的文件保持原样。
- **备份：** 原文件在 `/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/_twap_fix_20261123/backup/{raw,hfq}`（约 37 GB，硬链接，目录结构与生产相同）。日志、修补二进制及其 sha256、校验 JSONL 在同目录 `logs/`、`day23/logs/`、`validate/`。
- **部署：** `deploy-release.sh 161dc00` 后重启了 `market-replay-server`（含 `cn-minute-cache`），以及给 CN Manager 用的 `cn-market-nav-reader.service`（127.0.0.1:28312）。20261124 起的盘中数据按新口径生成。之后用 `cn-hfq-bars` 重建了 75 个品种的 5min/15min/1h/1day HFQ 粗频底表。
- **未变：** 因子与 ylabel（停在 2026-08-26，用户决定不再维护）仍是旧 TWAP 计算的结果。mkt_signal 依赖的 core rev 未更新：实盘 CTA 只透传 `twap`，因子与仓位不读它。
