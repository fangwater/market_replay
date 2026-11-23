# 国内 HFQ 粗频底表每日增量（`cn-hfq-bars`）

2026-11-23 用户要求 5min / 15min / 1h / 1day 每日增量维护，因子和 ylabel 不维护（停在 2026-08-26），并要求统一用 Rust。

## 维护范围

`/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_{5min,15min,1h,1day}_hfq/<交易所>/<品种>/<YYYYMMDD>.parquet`，
以及各目录下的 `_segments/<交易所>/<品种>/<YYYYMMDD>.json`（连续段起点、bar 的 ts / end_ts、每根 bar 的分钟数）。
75 个研究品种（`config/cn_hfq_universe_75.json`），2021-01-01 起，源是 `baseline_data_1min_hfq`。
`baseline_factor_*_hfq`、`ylabel_*_hfq`、`backtest_1s` 不再更新。

## 切 bar 规则（和 2026-10-14 / 10-28 发布时一致，没有改动）

- 5min / 15min / 1h：在每个“相邻两根分钟时间戳差恰好 60 秒”的连续段里，从段首分钟起每 N 根合成一根；遇到休市或缺分钟就收成短 bar，下一段重新开桶；短尾保留。
  所以周期不固定，例如螺纹钢日盘 10:00 的 1h bar 只有 10:00–10:14 共 15 分钟（茶歇前），14:30 的只有 30 分钟。
- 1day：每个官方交易日一根，包含夜盘和日盘，直接从 1min 合成，不从 1h 聚合。
- 字段：成交分项求和；均额、净买额 / 量 / 比例重算；OHLC 只用有成交的分钟，无成交 bar 取源最后一分钟的 close；三类 VWAP 按对应成交量加权；twap 取分钟 twap 的平均；盘口取 bar 内最后一分钟。
- 求和顺序复刻 NumPy（`np.add.reduceat` = 第一个元素 + 其余元素的 pairwise 和），所以数值和 Python 版逐位一致。

## 每日运行

服务器任务 `cn-hfq-bars-daily`（上海 18:00，排在 `cn-hfq-daily` 之后的同一个队列），命令 `cn-hfq-bars --config config/cn_daily.toml`。

- 水位：`baseline_data_1min_hfq/_provenance/hfq/generation.json` 的 `through`。
- 状态：`<数据根>/_provenance/hfq_multi/state.json`，每个品种记录已处理到的日期和每个 1min 源文件的修改时间与大小。
- 只处理水位之后的新交易日；一个日文件只依赖它自己的 1min 日文件。
- 如果任何已处理过的 1min 文件变了（换月会让该品种所有更早的价格整体平移），这个品种整体重建；没有状态的品种也整体重建。
- 写文件先写同目录的临时文件再重命名；字节完全相同的文件不动。
- 手动运行可加 `--gate 127.0.0.1:18313`（有任务运行或排队时等待）、`--product RB`、`--rebuild`；`--out-root <目录>` 只写到别处做核对，不读写状态、不加锁。

## 核对（2026-11-23）

- 用 Python 原脚本（`build_cn_hfq_5min.py`、`build_cn_hfq_coarse.py`）重建 75 个品种到 2026-11-20 后，Rust 版对 BR 全历史（801 天 × 4 套 = 3204 个文件）重建：所有列的值逐位相同（含空值）、列顺序和类型相同、bar 合同元数据相同、分段 JSON 逐字节相同。
- 和 Python 版的差别只有两处：parquet 的压缩字节不同；Rust 版不写 `pandas` 元数据（只用于 pandas 还原索引，本数据无索引）。
- 原 Python 脚本已从仓库删除，历史版本在 git 里。

## 为什么 1min HFQ 变了之后底表也要整体重算

1min HFQ 是加法复权，新的换月价差会加到该品种所有更早的价格上。2026-11-19 的 1min HFQ 重新生成后，旧的 5min 等底表和它对不上（例如 RB 同一时点差 51），所以第一次运行是 75 个品种整体重建，之后才是增量。
