# 2026 补充 TAS 扫描与 RocksDB

本页记录先写独立库的清洗验收阶段。后续已按用户要求合并进原库，当前查询可直接使用原库，见 [补充 TAS 合并](tas_supplement_merge.md)。独立库保留为验收基线，不能再与合并后的原库相加。

用户于 2026-11-03 授权将已下载的补充 TAS 清洗入库。本次处理整包，保留原始 RIC 和全部支持事件；采用独立补充库，避免原批次与补充批次中重叠的产品、同年列族和 key 混写。原始生产库及下载源文件保持只读。

2026-11-03 04:42:34 UTC 完成并验收。整包 710,632,866 源行与 Report 一致，318 个活跃 RIC、18 个产品列族，实际持久化 184,432,843 条事件，库文件共 6,294,177,300 字节（约 6.3 GB）。未解析、未匹配、过滤、窗口截断均为 0，水位直接读取为 `done`。完成依据为 [完成回执](../artifacts/tas_supplement_ingest_20261103/completion.json) 和数据库目录 `_INGEST_SUCCESS.json`。

## 数据范围与位置

- 源窗口：`2026-01-01_2026-05-30`，右端不含；不是完整 2026 年。
- 原始合并包：`future_supplement/normalised/futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/merged-Data.csv.gz`，完整路径及供应商 MD5 见 [下载记录](tas_supplement_download.md)。
- Report：710,632,866 条源事件、318 个活跃 RIC；Count 含市场状态、结算等事件，不能当成交数。
- 目标库：`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_supplement_rocksdb_2026`。
- 分片目录：`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_supplement_shards_2026`。
- 审计目录：`artifacts/tas_supplement_ingest_20261103/`。

IB 三个新增对应关系仍为 GOIL→LGO、NKD→NK、TSR20→STF。整包还包含其它补充产品；本次不只筛出三个根。原库和补充库查询结果不能对重叠产品直接相加。

## 实现和清洗口径

Cargo 通过路径依赖直接复用 `mkt_signal/crates/cme_tas_replay`，两个上游 bin 源码作为本项目的 Cargo bin 入口编译，不复制清洗逻辑、不修改参考项目；版本依据为 `provenance.json` 中的源码、规则、配置及二进制 SHA256。构建产物均位于项目独立 NVMe target。

原始文件名称与原 replay 的目录约定不同，审计目录的 `input/` 仅建立只读源文件的软链接，将完整合并 gzip 适配为一个 `part-000000`，不改写或重新拼接下载包。先使用既有 sharder 生成每份约 2,500 万源行的 zstd 分片，边界保留同 RIC、同 Date-Time 组，然后用 12 个 replay worker 并行写同一个独立库。

列族、31 字节 key、各 kind 二进制布局与既有库一致，按 `p:2026:{product}` 分列族。逐笔 Trade / Special 保留，Quote 仍按 Exch Time 秒桶形成 BBO：只在同一秒内叠加完整价量一侧，不跨秒补盘口；缺 Exch Time 回退 Date-Time。其它官方事件按既有 14-kind 规则写入。

## 验证与完成判据

已在独立临时库验证前 1,000,000 源行：19,984 条 Trade / Special 与 112,163 条 Quote 按 Python 源 CSV 基线逐字节及重复次数比较通过。直接 gzip replay 与 zstd shard replay 的全部持久化记录和各列族 key/value SHA256 完全一致；样本总落库 200,539 条，未解析与未匹配均为 0。

全量验收要求：

1. gzip 完整读至 EOF，所有分片完成，源行数与 Report 合计一致。
2. replay 正常退出、完成 flush，直接读取 `replay_meta` 为 `done`；未解析、未匹配、过滤和窗口截断计数均为 0。
3. 只读遍历补充库全部记录，校验 SST checksum、各 kind 解码、key/value RIC 及时间、列族路由、Quote 秒桶和源日期窗口，保存每个列族的 key/value SHA256。
4. 除 Quote 同 key merge 外，各 kind 的持久化记录数须等于 replay 写入计数。持久化 RIC 集合与 Report 活跃 RIC 集合一致，并确认 LGO / NK / STF 的 Trade 和 Quote 均存在。
5. 生成完成回执；更新中文清单的三个补充状态及持久化记录数。

入口：

```bash
bash scripts/build.sh --bin cme_tas_shard --bin cme_tas_replay_all --bin tas_db_audit
cme_tas_shard --config config/tas_supplement_shard_2026.toml
bash scripts/run_tas_supplement_2026.sh
```

示例中的 `cme_tas_shard` 位于 `/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/`。已完成的回放不能再次执行；失败的 `writing` 库不能直接续写，须保留失败证据、定位原因后在独立目录重新处理。本次未生成补充 Parquet。

## 实际落库结果

| IB 代码 | TAS 根 | 活跃 RIC | Trade RIC | Quote RIC | Trade 条数 | Quote 条数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GOIL | LGO | 84 | 39 | 46 | 25,171,597 | 81,360,920 |
| NKD | NK | 16 | 3 | 4 | 426,308 | 8,704,223 |
| TSR20 | STF | 18 | 14 | 16 | 852,082 | 8,585,449 |

整包共 Trade 31,988,543 条、Special 179,439 条、Quote 133,820,351 条；另有 TOT、状态、结算、更正等。活跃 RIC 包含只有状态或结算的远期工具，不代表每个 RIC 同时有成交和报价。Quote 写入操作与持久化记录差 93 条，来自同 key merge；其余事件类型持久化条数均与 replay 写入计数相同。

全量只读扫描和每 CF key/value SHA256 见 [数据库审计](../artifacts/tas_supplement_ingest_20261103/db_audit.json)。[中文 Excel 清单](../reports/IB期货与原始TAS中文清单_20261103.xlsx) 已随后更新为“已补入原TAS库”，并列出上述实际 Trade／Quote 数。本次未导出补充 Parquet，原 2026 Parquet 完成快照保持独立。
