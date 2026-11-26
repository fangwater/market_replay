# 交付盘 `review/` 目录：原因与处理（2026-11-26）

交付盘按 `<交易所>/<UTC 年>/<RIC>/<UTC 日>/…` 存放。交易所判不出来时，文件进 `review/`。2026-11-26 查明有两个原因，用户批准了下面三项处理。

## 原因

1. **一位数年份的 RIC 撞名。** 同一个裸 RIC 在 HRD 里对应两个相隔十年的产品。
   - 例：`TNH2^1` 是 NYMEX 的 TC1 航线（Ras Tanura–横滨）油轮运费合约，2012 年到期；`TNH2^2` 是 CBOT 的超长 10 年期美债，2022 年到期。
   - 交易所分组（`artifacts/lseg_exchange_groups_20261109/ric_catalog.csv`）按裸 RIC 合并所有版本，且"不展开一位年份"，所以判成 `conflicting_hrd_venues`，一直没有人工核实。
   - 结果：TN 在 2021–2023 年的 9 个合约（`TNZ1`、2022 和 2023 年各季月）都进了 `review/`，K 线和交易状态全是 UNKNOWN。
2. **新挂牌合约没有参考数据。** `lseg-daily` 定交易所的顺序是：以前分配过的 > 核定表 > HRD > `review`。
   - 它用的 HRD 是 2026-06-06 提取的静态文件，之后挂牌的合约查不到，一律判成 `review`，并且永不改目录。
   - `lseg-daily` 自己判过的 88 个 RIC 全部是 `review`。`review/2026` 下共 410 个 RIC，其中包括 TYH27 等本周换月要换进去的新主力。
   - 有 7 个合约（ESU31、GCQ28、GCU28、GCV28、NQZ31、RTYU27、YMM27）已经分散在两个目录：早期文件在正确目录（状态解码正确），后来的日包批次在 `review/`（状态为 UNKNOWN）。

## 处理

**1、2：判定规则**（`crates/cme/lseg_daily/src/venue_fix.rs`），按顺序取第一条命中的，每条结果都记下依据：

1. `hrd_by_date`：在该裸 RIC 的 HRD 版本里，找存续期（首个交易日 .. 最后交易日，前后各放宽 31 天）包含首个数据日的版本；只有一个交易所时采用。
2. `root_follow`：没有任何 HRD 版本覆盖时，如果同一前缀（品种代码 + 月份代码 + 年份数字）的单月合约在数据年份前后 3 年内全部位于同一个交易所目录，就跟随它；出现两个目录就不判。

其余情况一律保留 `review`，并记下原因，不做任何推断。

- `lseg-daily venues-plan --config … --out <目录>`：只读，对 `review/` 下每个 RIC-年给出判定，输出 `venue_plan.csv` 和汇总。
- `lseg-daily` 日常运行：从未交付过的新 RIC，如果 HRD 判不出来，就用规则 2；已经在 `review/` 下交付过、现在能判定的，不自动改目录，而是写进 `meta/problems/lseg_daily/venues_resolvable.json`，提示先跑 `venues-migrate`。

**3：迁移**（`lseg-daily venues-migrate --config … --plan venue_plan.csv --journal <本地目录> [--apply]`）

- **前置检查：** 持有 `lseg-daily` 的锁；有任何写交付盘的进程在跑时拒绝执行（`venue_fix::DELIVERY_WRITERS`）。不加 `--apply` 时只逐个文件检查。
- **搬文件：** 每个文件硬链接到新目录。NFS 上实测：两个路径是同一个 inode，目标已存在时报 `File exists`、不覆盖。核对 inode 一致后，才删除 `review/` 下的那份。
  - 目标已有同 inode 或字节相同的文件：跳过。
  - 目标已有内容不同的派生文件（`kline.parquet`、`session_status.parquet`）：保留目标那份，`review/` 那份先备份到 `<journal>/review_backup/` 再删除。
  - 其他文件内容不同：整个 RIC-年不动，记进 `meta/problems/lseg_daily/venue_migration.json`。
- **更新元数据：** 日志写在 `<journal>/migrate.jsonl`，state 改动前先备份。
  - 已交付过的 RIC（在 state 或核定表里），把新交易所写进 `state.venues`（state 优先于核定表），并置 `catalog_dirty`。
  - 已迁移的 RIC-年写进 `<project>/artifacts/venue_overrides/venue_overrides.csv`。K 线引擎（`lseg_delivery::native_kline::engine::VENUE_OVERRIDES`）在加载完其他所有来源后最后读这张表，所以重算时会按新交易所解码，并写到新目录。
- **迁移之后：**
  1. 对每个搬过的 RIC 跑 `lseg_kline run --year <来源年> --product <品种> --ric <RIC>`，带 `--backup-root` 和 `--replaced-log`，让交易状态按正确的交易所重新解码。
  2. `lseg_catalog build`。
  3. `lseg_catalog l2-sync`。
  4. `lseg_catalog ib-sync`。

## 2026-11-26 的结果

- `review/` 下共 1,599 个 RIC-年。
- 可判定的 876 个：按规则 1（`hrd_by_date`）判定 489 个，按规则 2（`root_follow`）判定 387 个。本研究的 16 个 CME 品种共 76 个，全部可判定：TN 2021–2023 年的 15 个由规则 1 判为 cbot，2026 年新挂牌的 61 个由规则 2 判定。
- 不能判定的 723 个，仍留在 `review/`：
  - 369 个：HRD 里同一时段就有两个交易所，比如 ICE Endex 和 ICE Europe 同时挂牌、COMEX 和 NYMEX；这需要按品种人工决定。
  - 327 个：不是标准的单月合约代码，比如 `NGLNDF0118`、`LSUSPOT`。
  - 27 个：前缀在别处从未出现过。
- 迁移演练：869 个 RIC-年、336,715 个文件可以直接链接；7 个 RIC-年只有派生文件冲突，已按上面的规则处理。
- 产物：`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/venue_review_20261126/`，包括 `venue_plan.csv`、迁移日志和 `review` 备份。

**执行结果（2026-11-26 UTC 07:18–10:55）：**

- **迁移：** 876 个 RIC-年全部搬完，共链接 338,483 个文件。
  - 7 个合约（ESU31 等）有 796 个派生文件与目标冲突，保留目标里的那份，`review` 里的已备份。
  - 第一轮迁移用的是不含派生冲突处理的旧编译，所以这 7 个是用部署版本补跑的。
  - `review/` 下剩下的 723 个 RIC-年，就是判不出来的那部分。
- **K 线重算**（部署版 `lseg_kline`，带备份和替换记录，产物在 `runtime/venue_kline_20261126/`）：
  - 875 个合约逐个用 `--ric` 重算，替换 13,920 个文件；
  - 184 个组各跑一次整组重算，另替换 11,546 个文件，其中：
    - 11,526 个是搬过的合约；
    - 12 个是组内其他合约补全被截短的尾日；
    - 8 个是 NQ 2026 年 4 个合约在 11-04、11-05 两天原来为空的 K 线（全天前向填充、状态 UNKNOWN），这次补上了真实成交。
  - 旧文件备份共 2.6 GB。
  - 抽查：TYH27、GCQ28、TNH2、TNZ3 的 K 线状态从全天 UNKNOWN 变为 TRADING / SCHEDULED_CLOSED，和同期正常目录里的合约一致。
- **索引：** `lseg_catalog build` 后，`directories` 里指向 `review/` 的只剩 724 行（判不出来的部分）。随后 `l2-sync --ric-years` 刷新了 479 个 L2 目录，没有一行指向不存在的目录。
- **迁移带来的后续修复：** 撞名的 RIC 现在会按年份落在不同交易所（共 76 个，例如 CFI2Z0：2020 年合约在 ice_europe，2030 年合约在 ice_endex）。两个以 RIC 为键的下游因此需要改：
  - `scripts/lseg/l2_exchange_map.py`：原来会因此报错退出。改为默认取最近一年的目录，`--year` 可以指定年份；同一年出现两个交易所仍然报错。映射行数和迁移前一样（42,620 行），变化只有 `review` 改到正确交易所，没有冲突。
  - `ib_kline_export`：原来会报 `ambiguous_exchange`，共 12 个有 IB 数据的 RIC。改为取最近一个来源年的交易所。

一次失误：在覆盖表写出之前试跑了 AULH27 的 K 线，结果在 `review/2026/AULH27` 下重新生成了 226 个文件。核实这些文件都是刚生成的、正确目录下都有对应文件之后，已经全部删除。重算脚本 `run_one.sh` 现在会在覆盖表不存在、或覆盖表里没有该 RIC 时拒绝运行。
