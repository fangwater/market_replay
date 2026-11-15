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
