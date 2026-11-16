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
4. **K 线**：计划 = 基础计划（原样）+ 每个已交付 CF 一个 `daily` 作业（带 `coverage_days`、`exchange_map`）。先 `lseg_kline check-plan`，再对受影响的每个 (年份, 品种) 组跑 `lseg_kline run --year --product --backup-root ... --replaced-log ...`。
   - 某组第一次跑时比对当年每一天，一次性补全以前被截短的尾日；以后只比对从（本批最早日期与上次已交付末日中较早者）前一天开始的日期。
   - 替换记录写到 `meta/problems/lseg_daily/replaced.csv`，原因分 `late_day` / `tail_completion` / `correction` / `other`。旧文件备份在 `runtime/lseg_daily/backups/<运行时刻>/`，与原文件逐字节比对过。
5. **目录**：本次没有任何失败时 `lseg_catalog build`（生产会导入 IB 清单）。`catalog_prune_expired_status` 默认关闭。
6. **钉钉**：每次运行一条消息。

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
- 237 个改撤保护 RIC：原生引擎没有 `correction_policy_version=1` 的冲销逻辑，所以保护期（`end_ns`）之前的已有文件一律不动。引擎会核对原基础来源的成交量与改撤记录一致；保护期内后来补到的成交记为 `protected_late_trades_not_applied`，问题文件在 `meta/problems/native_kline/`。保护期之后的新日期正常生成。

## 命令

```bash
R=/home/fanghaizhou/cn_futures_db/target/release
$R/lseg-daily run --config config/lseg_daily.toml --dry-run   # 只盘点和列计划
$R/lseg-daily run --config config/lseg_daily.toml
$R/lseg-daily status --config config/lseg_daily.toml
$R/lseg-daily test-notify --config config/lseg_daily.toml
# 选项：--products A,B  --dates YYYY-MM-DD,...  --no-new  --skip kline,catalog  --no-notify
```

状态文件在 `runtime/lseg_daily/state.json`。第一次运行时从 `bootstrap_runs` 读取 20261109 批次已交付的日期和 RIC 归属。

## 已知限制

- 原生 K 线写出的元数据键比生产 kline.parquet 少（生产文件上后来又加过键）。替换后的文件用的是引擎自己的元数据。
- 合约到期后只含状态的日期：每次重算都会重新生成，`--prune-expired-status` 又会删掉。
- 每天新增约 120 个日包库。
