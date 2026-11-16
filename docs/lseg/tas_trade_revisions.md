# TAS 历史 K 线的成交改撤

2026-11-10 用户授权修复：此前生成器只聚合 Trade 打印，未将已归档的 kind=12
Correction 关联回原成交。这是处理缺项，成交可以被交易所撤销或更正。

原始 Trade、Correction 和生产 RocksDB 只读保留。历史 `kline.parquet` 按当前已取得的
源快照重算有效成交，是事后修订后的历史口径，不是在取消发布时间才扣减当分钟。
IB 保持原始行、字段、价量和缺口，只读参与重新比较。

## 关联与语义

- 优先使用原始唯一成交 ID、原成交序号；提供的标识之间不能相互冲突。
- 精简 codec 丢失的标识回查本地原始 CSV，不能将“尚未回查”当作“原始 ID 为空”。
- ID 确实为空时，只接受同 RIC 完整候选历史中的唯一原价／原量匹配，且原始时间或一致的时钟必须提供确认。
  不能靠取消消息的当前时钟，从多笔同价同量成交中任选一笔；当前时钟也可能只是最新成交状态。
- `CAN_COND=611` 或 `CAN_COND_N=2` 明确为取消。即使同一 Correction 有 Price／Volume，
  也不能据此添加替代成交：GC 原始样本证明它们可能重述另一笔最新成交。
- 同一时刻互补的空 Qualifiers／带取消标记片段先合并；重复取消幂等。
- 明确 `INSSALCOND=2` 且价量完整时补入，若已经存在相同发布时间的原 Trade 则不重复添加。
- 非取消的明确改价／改量支持保留一笔成交并替换内容；涉及改成交时间时先列问题，不能静默改桶。
- 缺失、冲突或多候选关联不猜测删除，保留原成交并列在最终盘
  `meta/problems/tas_trade_revisions_unresolved.csv`；尚未回源的归档事件明确标为 `source_witness_pending`。

每个已应用记录在项目本地计划中保留源文件／行号及目标 Trade Parquet 的物理行。
历史计划按文件metadata中的SHA256保存在项目本地`plan_history/`；文件pending数量表示当时的计划快照，当前未解决清单以NAS问题CSV为准。
正常成功记录不发布到 NAS。不能因 Block Trade、aggressor=0 或状态外成交直接删记录。

## 重算与交付

既有 218 个 TAS RIC 使用同一账本逻辑。只从源 Trade 重算受影响分钟，重新计算
OHLC、volume、trade_count 以及依赖收盘价的后续补值。连续分钟网格、合约边界、status、
10 列公开 schema、Float64 价格和 UTC 时间口径保持一致。
本地保存改撤前基线用于断点重试；只原子替换值发生变化的 UTC 日文件。
所有新文件检查 footer／schema／条数／RIC／日期，变更日按首／中／末抽样完整读回。
量与笔数按 gross 打印加精确改撤差额验证，不再错误地要求净额等于原始打印。

SRAU26／2026-06-22 21:34 的 3,200 手按原价、原量、时钟及完整候选唯一性关联取消。
原 Trade 保留，K 线该分钟为 volume=0、trade_count=0、is_ffill=true、OHLC=96.035，
status 仍是 SCHEDULED_CLOSED；21:35—21:59 也补回96.035，22:00使用新成交。
原成交没有唯一 ID 的证据限制仍如实保留，不能声称已经完成逐 ID 核对。

## 入口

```bash
# 身份恢复可独立续跑；读取本地数据，不下载、不写生产库。
python scripts/lseg/extract_revision_identity.py --daily-only --workers 4
python scripts/lseg/extract_revision_identity.py --historical-only --workers 9
# 全部回源完成后编译及回写，默认拒绝未完成回源。
python scripts/lseg/compile_trade_revisions.py
python scripts/lseg/apply_kline_trade_revisions.py --workers 4
```

当前批次由 `ly-cme-trade-revision-backfill.service` 托管：先应用已有源证据的改撤，
再等待 `ly-cme-correction-identities-historical-fast.service` 完成年度大包回查，自动合并历史证据、
从相同 gross 基线重算并刷新中文 IB 对照表。未关联问题会继续保留，不宣称全部记录都能净额化。
项目断点与日志在 `artifacts/tas_trade_corrections_20261110/`；生产代码与历史数据在本机既定目录。

## 原生引擎（2026-11-16 起）

冲销逻辑已移植到 `crates/cme/lseg_delivery/src/native_kline/netting.rs`（`trade_revision_ledger.py` 的逐条移植，测试覆盖原 Python 全部用例）。

- 见证库（版本 2）：`manifest.json` 按追加顺序列出不可变的段，每段有年份和每 RIC 一个 jsonl（带 sha256）。
  `base` 段由 `lseg-kline witness-import --jobs <identity_jobs_daily.json>,<identity_jobs_historical.json> --rics <correction_plans.json>`
  一次性导入（按 Python 编译顺序；237 个 RIC，56,488 笔 Trade、1,638 条 Correction）。
- 日包：`lseg-daily` 在 K 线前对每个新批次/品种跑 `lseg-kline witness-append`，从下载的日包 CSV 两遍流式提取：
  全部 Correction 行，以及同 RIC、价量等于某条 Correction 原价量或新价量的 Trade 行，作为段 `daily<批次>_<品种>` 追加（按名字幂等，有锁，清单原子替换）。
  以后这些 RIC 的日包改撤按同一账本冲销；原成交在更早文件里的改撤仍可能因缺见证而未关联。
- 候选成交：本年源中价量等于某条 kind=12 原价量或新价量、发布时间不晚于最后一条 kind=12 的成交（RocksDB 直读）。
- 账本：只对受影响分钟从原始打印重建（先核对 gross 重建等于聚合结果），再算后续补价；量和笔数按 gross 加精确差额守恒。
- 没有原始见证的 kind=12 记为 `source_witness_pending`；其余未关联原因同 Python。问题写在 `meta/problems/native_kline/`，
  本地账本在 `<work>/checkpoints/netting/<年>/<RIC>.json`。
