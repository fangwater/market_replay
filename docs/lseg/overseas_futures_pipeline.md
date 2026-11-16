# 海外期货 raw 清洗与 RocksDB 管线

梳理日期：2026-11-02。依据为 `preprocess` 的数据契约、`mkt_signal` 的源码和配置，以及现有运行回执。此次只读查看既有数据；未重新执行清洗、写库或全库扫描。

## 工程位置

- 数据口径、验证与交付记录：`/home/fanghaizhou/preprocess`。
- 既有 Rust 生产实现：`/home/fanghaizhou/mkt_signal/crates/cme_tas_replay`。
- 本项目新增代码：`/home/fanghaizhou/cn_futures_db/crates/cme/lseg_delivery`（代码），`scripts/lseg`、`config/lseg`、`docs/lseg`，包含验证任务和每日任务。
- Python 用作正确性基线，Rust 优化须对齐字段语义、时间、缺失值、排序和精度。

## 数据源与三条入库链路

本机原始根：`/mnt/hdd-raid5-72t/liang_torch/lseg_data/future`。

采购源当前文档地址为 `http://10.61.20.12/public/users/u158/public/lseg_data/`；旧配置中的 `10.61.20.13/users/...` 是历史地址。本次未重新请求 HTTP 源。

源包通常是 UTC 年度半开区间下的多个 gzip CSV，附 Report / Notes；分包不是日文件，边界可能落在同一 RIC 或同一消息内部。

| 源层 | 内容 | 主要处理入口 | 结果 |
| --- | --- | --- | --- |
| `normalised` | TAS 成交、L1 Quote、结算、状态、更正等 | `cme_tas_shard` → `cme_tas_replay_all` | 全品种 TAS RocksDB |
| `normalisedLL2` | 最多十档的完整结构快照，源字段可缺失 | `cme_ll2_shard` → `cme_ll2_replay` | 每 RIC / UTC 分钟最后一条快照 |
| `rawLL2` | 消息头及有序 FID 字段补丁 | `cme_raw_ll2_shard` → `cme_raw_ll2_replay` | 重建状态后的稀疏 15s 快照及质量掩码 |
| `summarised` | 供应商 1 分钟 Summary | 独立路由与审计 | 不代替 TAS 成交、分钟网格或主力判据 |

采购分类中的 `raw` 是另一种全字段原始消息流，不等于 `rawLL2`。原批次和 `future_supplement` 补充批次须保留独立来源与重叠选择规则。

## TAS 清洗

1. 顺序读取 gzip，生成可独立解压的 zstd 分片，再并行解析；有状态处理和写入保留源顺序。
2. 按原始 `#RIC` 动态解析 product，不把研究用六品种名单套到全品种入库层。保留历史 RIC，不能擅自合并前导 `1` 等不同身份。
3. 分别编码成交、Special、Quote、结算、涨跌停、隐含波动、状态、更正等十四类消息。结算、更正、竞价和有价无量指示不能混作连续盘成交；无价 Special 单独保存。
4. Trade 等事件保留源 `Date-Time` UTC 纳秒，同纳秒以 part / seq 防止碰撞。同一源行可产生原消息及独立 IV 消息。
5. Quote 按 `Exch Time` 聚合为每秒 BBO；用 `Date-Time` 补 UTC 日期，超过 12 小时作跨日纠正，缺 `Exch Time` 才回退源发布时间。不能再次按芝加哥时区转换该日内钟面。
6. 同一秒内只用价量齐全的一侧更新该侧。换秒从空状态开始，不从前一秒继承；跨 part 的同秒用 merge 合并完整侧。下游 1s 执行行情的 standing book 是另一层逻辑。
7. 无法解析、无法路由、空 Quote 等分别记录审计，不把 `done` 理解为没有任何异常。已有 2025 回执记录过 72,104 条超长状态 Qualifiers 的 unparsed，不能称为零错误，也不能称为丢失成交。

## Normalized LL2 清洗

- 完整解析全部源行的 66 列，不按研究品种、交易所或 session 过滤。
- 按源 `Date-Time` 分 UTC 分钟，取 `(source_ts_utc_ns, source_order)` 最大的快照。
- 分片内部直接写，分片首尾的重叠分钟按上述顺序合并。
- 每行是独立快照，不向上一快照借缺失字段；没有更新的分钟不生成 key，也不前填。
- Report 的逐 RIC Count 与解析行数核对。全品种入库完成不代表研究范围内每个合约和分钟都有授权数据。

## Raw LL2 清洗

1. 检查正式分包、大小、Report / Notes 与消息 Count；`.part` 和未完成年度不能作为完整 period 入库。
2. 完整扫描每个外层消息及其 FID，校验 `Number of FIDs`；跨 gzip part 保留未结束消息和 RIC 状态。必须应用全部补丁才能提取低频末值，不能直接抽样原始行。
3. `UPDATE` 只更新出现的字段；缺 FID 沿用，显式空值写 missing 并标记为已知，从未出现的字段保持未知。不得用 Normalized 的书初始化 Raw 状态。
4. `REFRESH` 因 CSV 缺少 ClearCache / RefreshComplete 标志，采用保守规则：丢弃前序字段知识，只应用本消息显式字段。不要求固定十档或 TIMACT；仅含元数据的 REFRESH 也输出知识失效状态，防止同桶较早有效书残留。
5. 零 FID 的 `STATUS` 只推进时间和计数，不改盘口，不制造深度快照。更正只在消息当时生效，不回写历史桶。
6. 每 RIC 时间非递减，同纳秒保留源顺序。按源 `Date-Time` UTC 纳秒分桶，保留有深度变化或 REFRESH 边界的桶内最后状态；无更新的桶不前填。
7. 当前代码一次配置只写选定的 15s 或 60s。既有 2026 批次保留历史 15s + 1min 双频数据；2024 / 2025 只存 15s，分钟读取时派生。
8. 并行 part 先提取低频状态到 zstd staging，再按 part 原序衔接未知字段；REFRESH 后禁止继承旧 part。来源顺序为 `(part_index << 48) | local_message_index`。
9. 有限队列送单个 RocksDB writer，同桶保留最大源时间与顺序；data 和 quality 在同一 WriteBatch 中原子写入。

第十档存在供应商别名：`BEST_BSZ10`、`BEST_ASZ10`、`NO_BIDRD10`、`NO_ASKRD10`，不能只按前九档字段名机械拼接。

## RocksDB 目录和结构

以下三座既有目录已在本机确认存在，根为 `/mnt/nvme-raid0-28t/fanghaizhou/lseg_data`：

| 库目录 | 数据列族 | key | value |
| --- | --- | --- | --- |
| `cme_tas_rocksdb_all_products` | `p:{period_year}:{encoded_product}` | 31B：`kind(1) + RIC(16) + ts_ns(8) + part(2) + seq(4)` | 按 kind 编码的定长消息，例如 Trade 80B、Quote 64B |
| `cme_ll2_1min_rocksdb_all_products` | `p:{period_year}:{encoded_product}` | 24B：`RIC(16) + minute_ns(8)` | 432B 的 L2 快照 |
| `cme_raw_ll2_15s_rocksdb_all_products` | `s15:p:{period_year}:{encoded_product}`；旧分钟为 `p:...` | 24B：`RIC(16) + bucket_ns(8)` | 复用 432B L2 快照；另有 quality 列族 |

- 列族年份取源 period 左边界年，不是到期年或交易日年。product 特殊 ASCII 字节使用可逆 `%HH` 编码。
- key 的时间和 TAS part / seq 使用大端以保持排序；数值 value 使用小端。RIC 是固定 16B ASCII、尾部 NUL padding。
- L2 value 保存 magic / version / kind、源 UTC 纳秒、源顺序、交易所日内时间、GMT offset，以及十档价、量、委托笔数。
- L2 价格和数量为 `i64` 的十进制 × `1e9`，missing 为 `i64::MIN`；委托笔数 `u32` 的 missing 为 `u32::MAX`，不等于真实零。
- Raw 的 `q:{data_cf}` 以相同 key 保存 8B known mask。bit `6*level+field` 依次对应 BidPrice、BidSize、BuyNo、AskPrice、AskSize、SellNo，bit 60 为 TIMACT。它区分“未知”与“已知为空”。
- `replay_meta` 保存 `period:<period>` 水位及相关 census / audit。完整解析、计数核对与成功写入后才为 `done`；失败的 `writing` 不能直接追加续写。不同管线的重跑机制不同，不能互相套用。
- Raw 按既有用户要求不自动全库 verify；`done` 与“逐 key 全库扫描通过”必须分开表述。

## 实际运行回执

本次读取各 run 的 `status.json` 与 `replay_audit.json`；没有重新打开生产库读取水位或扫描 SST。

| Raw period | 回执状态 | 完整源消息数 | 保存快照数 |
| --- | --- | ---: | ---: |
| 2024-01-01 至 2025-01-01 | done / complete / write_complete | 92,368,507,219 | 556,718,540，15s |
| 2025-01-01 至 2026-01-01 | done / complete / write_complete | 142,137,516,819 | 736,669,850，15s |
| 2026-01-01 至 2026-06-01 | done / complete / write_complete | 98,322,720,819 | 432,481,922，既有双频合计 |

回执位于 NVMe 根下的 `cme_raw_ll2_runs/20261011_2024_15s_local_30core_094320`、`20261010_2025_15s_local_30core_033510`、`20261009_2026_15s_refresh_131559`。三个批次均未执行全库扫描。2024 已完成，旧页“2024 正在处理”是历史状态。

## RocksDB 到研究数据

- TAS 加官方交易日历生成逐合约 `backtest_1s` 和 `baseline_data_1min`；日历过滤属于下游产品层，不应提前套到全品种 LL2 入库。
- 2026-10-25 已给 15 个研究品种补入真实可用深度，更新 HFQ 并重算因子；YM 排除，FF 保持原状。不能继续把旧页“所有 depth 为 null”当作现状。
- 补列按优先级先取可用同分钟 Normalized donor，再取 Raw。Raw 取该 RIC 在同分钟最后状态，包含知识失效边界；不能退回较早有效书，也不能跨分钟前填。每分钟只选一个来源快照，不拼不同时间的档位。
- donor 要满足已知有效双边 L1、不交叉及已知价格档有序；错序整本拒收。缺档继续为空，不补零、不重排。
- 主键、成交列、分钟网格和主力规则保持原口径。盘口只是对既有分钟行补空列，来源和质量单独审计。
- 正式研究根为 `/mnt/hdd-raid5-72t/liang_torch/lseg_data`，主要目录为 `baseline_data_1min`、`baseline_data_1min_hfq`、`baseline_factor_1min_hfq`、`cme_volume_roll_1d`。
- 本轮补列未改 `backtest_1s`，未重算 `ylabel_*`。研究连续序列、因子和执行行情必须按各自契约使用。

## 查阅入口

- [海外期货总览](/home/fanghaizhou/preprocess/data_format/lseg_futures/pipeline.md)
- [TAS 入库契约](/home/fanghaizhou/preprocess/data_format/lseg/tas_replay_all_products.md)及 [RIC / key / merge 实现](/home/fanghaizhou/mkt_signal/crates/cme_tas_replay/src/product.rs)
- [Normalized LL2 契约](/home/fanghaizhou/preprocess/data_format/lseg/ll2_1min_rocksdb.md)及 [432B codec](/home/fanghaizhou/mkt_signal/crates/cme_tas_replay/src/ll2_1min.rs)
- [Raw LL2 契约](/home/fanghaizhou/preprocess/data_format/lseg/raw_ll2_low_frequency.md)、[字段状态实现](/home/fanghaizhou/mkt_signal/crates/cme_tas_replay/src/raw_ll2.rs)、[跨 part 衔接](/home/fanghaizhou/mkt_signal/crates/cme_tas_replay/src/raw_ll2_stage.rs)、[入库实现](/home/fanghaizhou/mkt_signal/crates/cme_tas_replay/src/bin/cme_raw_ll2_replay.rs)
- [Python Raw 正确性基线](/home/fanghaizhou/mkt_signal/scripts/cme_raw_ll2_reference.py)及 [作业 runner](/home/fanghaizhou/mkt_signal/scripts/run_cme_raw_ll2_pipeline.sh)
- [最新 15 品种补列交付记录](/home/fanghaizhou/preprocess/database/cme_lseg_level2_fill_20261025/README.md)
