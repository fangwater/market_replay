# lseg-daily：LSEG TAS 日包每日交付

代码 `crates/cme/lseg_daily`，配置 `config/lseg_daily.toml`（生产）和 `config/lseg_daily_staging.toml`（NVMe 暂存核对）。取代 2026-11-09 的一次性 Python 流程（`scripts/lseg/{inventory_tas_remote,download_tas_daily,process_tas_daily,finalize_tas_daily,run_tas_daily_delivery}.py`）。由 market-replay-server 的 `[[job]]` 调度；IB 是另一个任务。

## 一次运行做什么

全部串行；重活都是子进程，线程数在配置 `[workers]` 里固定。

1. **盘点**：直连（`no_proxy`）读取 `future_daily/normalised/` 和各品种目录；`.part` 不算。
2. **发现**：正式文件名中，`state.delivered` 里没有、也不在未完成批次里的 (品种, 日期)，逐个 HEAD：
   - 零字节占位：记为源缺口，不下载，下次再查；
   - 本地已有同名源文件但远端指纹不同：记为源缺口，不覆盖；
   - `Last-Modified` 不到 `settle_minutes`：等下次；
   - 已交付日期之后缺了某一天：更晚的日期先等 `hole_wait_hours`（72 小时），超时后把缺的那天记为源缺口；
   - 同一品种只取最早一个年份（CF 按年），其余顺延。
3. **逐品种**（失败只影响本品种，下次从失败的阶段继续）：
   - 下载：HEAD 指纹 + `If-Match`，流式 sha256，字节数和二次 HEAD 校验，表头检查，先写 `.download.json`，再硬链接发布（不覆盖）。
   - `tas_daily_stage` → `cme_tas_replay_all`（每次一个新的 `attempt_N`）→ `tas_db_audit --require-done`。
   - 同一原始 RIC 已由别的品种目录发布过则阻断。
   - 交易所目录：driver 以前分配过的 > `tas_status_20261109/plan.json` 的已核定表 > HRD MIC/交易所代码 > `review`。
   - `lseg_tas_parquet --layout ric-day --readback sampled`，全量核对 footer、文件集合和总数，再逐文件发布：临时文件 + fsync + footer 核对 + 不覆盖的硬链接；每个 CF/类型首中末 3 个文件比对整文件 SHA256。原始文件（trade/quote/status）不一致时报错、不替换。
   - `tas_status_parquet`。
4. **K 线**：计划 = 基础计划（原样）+ 每个已交付 CF 一个 `daily` 作业（带 `coverage_days`、`exchange_map`）。先 `lseg_kline check-plan`，再对每个新批次／品种跑 `lseg_kline witness-append`（日包改撤见证追加到 `kline_witnesses`，见 [成交改撤](tas_trade_revisions.md)），再对受影响的每个 (年份, 品种) 组跑 `lseg_kline run --year --product --backup-root ... --replaced-log ...`。
   - 某组第一次跑时比对当年每一天，一次性补全以前被截短的尾日；以后只比对从（本批最早日期与上次已交付末日中较早者）前一天开始的日期。
   - 替换记录写到 `meta/problems/lseg_daily/replaced.csv`，原因分 `late_day` / `tail_completion` / `correction` / `other`。旧文件备份在 `runtime/lseg_daily/backups/<运行时刻>/`，与原文件逐字节比对过。
5. **持仓量**（配置里有 `[open_interest]` 才跑）：`tas_open_interest sync --daily-root <raw_root>`，把还没入库的已下载日包的 `Open Interest` 写进 PostgreSQL `market_metadata.lseg_open_interest`（见 [持仓量](open_interest.md)）。只处理新文件，失败记 `meta/problems/lseg_daily/open_interest.json`，下次运行自动重试；不影响前后步骤。
6. **目录**：本次没有任何失败时 `lseg_catalog build`（生产会导入 IB 清单）。`catalog_prune_expired_status` 默认关闭。
7. **钉钉**：每次运行一条消息。

交付盘上只写数据文件和问题记录：`meta/problems/lseg_daily/` 下的 `<批次>/<品种>.json`（成功后删除）、`pipeline.json`、`source_gaps.csv`、`replaced.csv`。

## 覆盖与尾日规则（lseg_kline）

- 日包 D 恰好覆盖 UTC `[D, D+1)`：`tas_db_audit --require-done` 按发布时刻核对周期边界，实测首尾记录分别在 00:00 和 23:59。所以覆盖终点总是最后一个已交付日期的次日零点，不会有半天。
- 完全落在覆盖缺口里的 UTC 日，不发布 kline.parquet，也没有 session_status。
- 没有唯一到期日的合约，网格到"有 LSEG 证据的最后一个 UTC 日"的日末，不超过覆盖终点。以前是到最后一条证据所在的分钟，这就是 BTCF27 11-07 只有 601 行的原因。
- 已存在的 kline / session_status 在打开替换时比对数值（不比元数据），不同才替换：
  1. 新内容写临时文件并 fsync，核对 footer；
  2. 旧文件拷到本机备份，逐字节比对；
  3. rename 覆盖。
  这套流程和 `ib_kline_export` 共用 `crates/common/safe_replace`。
- 改撤冲销（`correction_policy_version=1`）在原生引擎里，见 [成交改撤](tas_trade_revisions.md)。原 237 个改撤 RIC 和其它 RIC 一样重算，迟到的成交和改撤会经比对窗口替换。比对窗口以外的旧文件（原 Python 生成）原样保留，不做行数核对（其中 21 个 RIC 的预热价／有效期口径与原生引擎不同）。没有见证库时这些 RIC 拒绝重算。

## 命令

```bash
R=/home/fanghaizhou/market_replay/target/release
$R/lseg-daily run --config config/lseg_daily.toml --dry-run   # 只盘点和列计划
$R/lseg-daily run --config config/lseg_daily.toml
$R/lseg-daily status --config config/lseg_daily.toml
$R/lseg-daily test-notify --config config/lseg_daily.toml
# 选项：--products A,B  --dates YYYY-MM-DD,...  --no-new  --skip kline,open_interest,catalog  --no-notify
```

状态文件在 `runtime/lseg_daily/state.json`。第一次运行时从 `bootstrap_runs` 读取 20261109 批次已交付的日期和 RIC 归属。

## 月度合并（`lseg-daily merge`）

每天约新增 120 个日包库；K 线一组要同时打开该品种的全部来源。每月 2 日把上月的日包库按品种合并成一个库：

```bash
$R/lseg-daily merge --config config/lseg_daily.toml --month previous --dry-run   # 只列来源和目标
$R/lseg-daily merge --config config/lseg_daily.toml --month 2026-11              # 合并 + 校验 + 切换
$R/lseg-daily merge --config config/lseg_daily.toml --month 2026-11 --delete-sources  # 删除已停用的旧库
```

- **来源：** lseg-daily 状态里已完成的日包库（`Done` + `kline_done`，且 `catalog_dirty` 为假；否则整次不合并）。**20261109 批次不合并**：它在基础计划里（基础计划原样使用，作业 JSON 是提取缓存的身份），而且已经是每品种一个库。
- **月份归属：** 一个日包库整体归到它第一个交付日所在的 UTC 月份，不按键时间拆分：键不会丢也不会重复；盘口键是事件时间的秒桶，可能落在发布时间所在日/月的另一侧，必须跟着来源走；`coverage_days` 原样合并。合并后又补到的晚到日期，再跑一次同月合并：已有的月库作为来源之一并入新的 `attempt_N`，旧月库停用。
- **合并（`tas_db_merge merge`）：** 每个品种 CF 按键做流式 k 路归并，写到 `<monthly_db_root>/<YYYY-MM>/<品种>/attempt_N`（默认 `cme_tas_monthly`，与 `cme_tas_daily` 同级），CF 名和 `quote_last` 合并算子不变；关闭自动压缩，写完后全量手动压缩（bottommost=Force），后台线程 `workers.merge_compaction`（默认 2）。一次只处理一个品种。
  - 两个来源有同一个键：失败。例外是盘口（kind 3）：它的键是零 part/seq 的秒桶，按 `quote_last` 折叠（与单次回放这两天的结果相同），并计数。
  - 两个来源有同类型、同 RIC、同时钟（键前 25 字节）的记录：失败（引擎靠来源顺序打破这种平局，合并后没有了）。
  - `replay_meta`：各来源条目的并集（冲突失败、`period:*` 必须是 done），加上 `period:<合并周期>`=done 和 `merged_from`。
- **校验一（记录数）：** 每个 CF、每种 kind：各来源读到的数 = 它的审计数；合并库只读重开后的数 = 来源合计 − 折叠的盘口；读回的键值流 SHA256 与写入的完全一致，键严格递增。
- **校验二（K 线）：** 用换成月库的计划先 `lseg_kline check-plan`；每个（年份, CF 品种）组，把交付盘上从首个合并日前一天起的 kline/session_status 拷到本机暂存根（交付盘只读），在暂存根上 `lseg_kline run --backup-root --compare-from` 重算：不得有替换、新增或任何字节变化（`tas_db_merge check-staging`）；组统计（成交数、量、行数、文件数）必须等于该组最近一次生产运行。
- **切换：** 两项都通过才改状态：月库记入 `state.merged`（`merged_from`、`coverage_days`、新的 `source_index`），来源记 `merged_into`，旁边写 `<库>.retired.json`；计划里只剩月库。旧库默认保留；`--delete-sources` 只删本月已通过校验的月库的来源，且逐个确认：在 `db_root`/`monthly_db_root` 之下、`CURRENT` 与合并时一致、计划不再读它。失败的品种保持原样（仍用日库），它新建的库删除，日志留在 `runtime/lseg_daily/merge/`。
- **钉钉：** 每次一条（【LSEG 月度合并】），几行：库数/SST/占用的前后对比、校验结果、旧库处理、失败品种。

## 已知限制

- 原生 K 线写出与生产相同的 17 个元数据键（冲销过的 RIC 另加 4 个）。替换只比数值，所以旧文件只有在数值变化被替换时才换成新元数据。
- 合约明确到期（EXPIRE）之后、全部是 EXPIRED/NO_CHANGE、目录里只有 status/session_status 的日期，引擎不再写 session_status（与 `--prune-expired-status` 同一规则）。
- 每天新增约 120 个日包库，由月度合并收拢（见上）。
