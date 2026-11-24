# 2026 补充 TAS 合并进原库

2026-11-03 用户提出将补充数据 insert 到原 RocksDB。本次在独立补充库全量验收后实施追加，专项替代本批数据先前只放独立库的交付方式。独立补充库保留为来源与复核基线。

原库：`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products`。

待插入数据、计划和日志：项目 `artifacts/tas_supplement_merge_20261103/`；其中 `staging` 软链接指向 NVMe 的 `lseg_data/tas_supplement_merge_20261103`，SST 构建文件不落在代码盘。

## 已完成的只读核查

原库共有 2,185 个列族，12 个原始 period 水位均为 `done`。补充 18 个列族中：

- 13 个原库没有的 `p:2026:*`：AFCM、AFCQ、AFCS、AFCY、CIPM、HTE、KWR、LGO、LGOTOT、NK、SEKA、SSG、STF；计划新增 174,343,942 条。
- 5 个重叠列族：ATWQ、ATWS、ATWY、LSU、LSUTOT；补充共 10,088,901 条，与原库按事件内容、重复次数核对完全一致，全部跳过。
- Quote 差异 0；需要重分配 part / seq 的记录 0。本次实际只新增 13 个列族，不向任何已有行情列族写入记录。

不能对完整 31 字节 key 简单去重，因为原批次和补充批次 `part` 编号不共享来源命名空间。程序按 kind / RIC / 纳秒时间分组，非 Quote 对 value 多重集合比较，保留同纳秒真实重复笔数；Quote 按秒桶核对，已存在时保留原报价。本批所有重叠记录均完全相同，无需冲突裁决。

每个待插入列族生成独立 SST，源全量 key/value SHA256 必须与既有补充库审计一致。正式写入前再次核对原库 ID、全局 sequence、列族名单、原水位及 SST SHA256，拒绝原库已变化的旧计划。原库 ID 为 `61e243ef-d010-47a3-a1fd-cdacf04444b8`，只读计划 sequence 为 `98,832,137,993`。

## 备份、插入与验收

2026-11-03 05:00:42 UTC 完成并验收。新增 174,343,942 条记录、13 个列族；10,088,901 条完全一致的重复记录全部跳过。原有 5 个重叠行情列族全量对比通过，12 个原 period 水位不变。完成依据为 [合并回执](../artifacts/tas_supplement_merge_20261103/completion.json) 与原库 `_SUPPLEMENT_MERGE_SUCCESS_20261103.json`。

写入前通过 RocksDB Checkpoint 建立原库回退快照，路径为 `/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_before_supplement_20261103`。快照与原库位于同一文件系统，SST 使用硬链接，避免复制整库；保留用于回退。

2026-11-24 用户要求不再保留这类回退快照：合并早已验收，快照的 60,242 个数据文件和主库是同一份硬链接，只有 4 个元数据文件（MANIFEST、CURRENT、日志、OPTIONS）是它独有的，已删除快照目录，主库未受影响、也没有释放空间。以后合并直接进主库，不再另做回退快照。

插入使用 RocksDB 外部 SST ingest，原始 `period:*` 水位不改动。本次使用单独的 `replay_meta` key `supplement:futures_11:2026-01-01_2026-05-30`，标识本批来源、状态与回执。

验收遍历新列族所有记录，核对新增条数和 key/value SHA256；对 5 个已有列族逐条与写入前快照比较，验证原记录全部保留、value 不变。另核对原水位不变，最后才写入完成回执。

验证任务已在 NVMe 临时数据库通过：跨 part 相同事件去重、同纳秒重复笔数保留、原 Quote 保留、checkpoint、实际 SST ingest、写后字节核对、重复执行拒绝。

## 使用口径

合并完成后可直接从原库查询 LGO、NK、STF 等新产品，不需要拼接两座库。已合并的独立补充库不能再次与原库求和。

补充窗口仍是 `[2026-01-01, 2026-05-30)`，不会因原库原批次的 `period:2026-01-01_2026-06-01=done` 而扩大到 6 月 1 日。原已验收的 2026 Parquet 是合并前快照，仍不含这些新增产品，不能按原文件 `_SUCCESS.json` 推断已经追加了补充 Parquet。

复现入口为 `tas_merge_supplement plan` / `apply`，实现见 [合并程序](../src/bin/tas_merge_supplement.rs)。已完成的合并拒绝重复执行。[中文 Excel](../reports/IB期货与原始TAS中文清单_20261103.xlsx) 已改为“已补入原TAS库”。
