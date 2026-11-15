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

验证：新版对 09-28（中秋假期后的周一）、10-08（国庆后第一天）、11-09（周一，夜盘跨周六）、11-11（普通周三）6 个交易所的回放统计（成交、盘口、持仓变化、队列条数），与用旧版跑的生产回补日志逐条一致。
