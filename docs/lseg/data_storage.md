# 数据落盘位置

当前用户入口为简短的 `lseg/README.md`，详细口径与排错资料在 `lseg/meta/supply.md`，回执及清单分别在 `lseg/meta/audit/`、`lseg/meta/catalog/`。元数据在meta内集中保存；后续汇总仅更新supply，不扩充日常入口。

2026-11-09，用户指定本项目最终数据写入磁盘的目录：

```text
172.16.30.100:/mnt/FuturePool/CTA_Data
```

| 项目 | 位置 |
| --- | --- |
| 最终数据目标主机 | `172.16.30.100` |
| 目标主机上的目录 | `/mnt/FuturePool/CTA_Data` |
| 本机 NFS 挂载点 | `/mnt/nfs/30.100_cta_data` |
| LSEG统一说明 | `/mnt/nfs/30.100_cta_data/lseg/README.md` |
| LSEG排错补充说明 | `/mnt/nfs/30.100_cta_data/lseg/meta/supply.md` |
| LSEG配套清单 | `/mnt/nfs/30.100_cta_data/lseg/meta/catalog/` |
| 项目代码目录 | `/home/fanghaizhou/market_replay/crates/cme/lseg_delivery`（代码），`scripts/lseg`、`config/lseg`、`docs/lseg` |
| Cargo 构建产物 | `/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay` |

后续正式数据交付以该远端目录为最终落盘目标。既有 TAS、IB Parquet 和 RocksDB 文档中的本机路径继续记录当前数据或历史输出的位置；迁移完成情况以实际交付回执为准。

用户随后要求所有说明集中到LSEG根目录：已将目录、字段、精度、分组、覆盖、缺口、校验及回执入口统一写入 `lseg/README.md`，将原根 `_catalog/` 移至 `lseg/_catalog/`，移除原根README。后台最终汇总也改为只更新这一份说明和LSEG内配套清单。当前根目录保留既有 `2026/` 与 `lseg/`；迁移完成依据为 `artifacts/lseg_readme_consolidation_20261109/completion.json`。

用户后续重新指定正式行情目录：`lseg/<交易所小写>/<年份>/<原始RIC>/<YYYYMMDD>/trade.parquet`、`quote.parquet`。例如 `lseg/cme/2026/ESH27/20260604/trade.parquet`，日期按 `ts_utc_ns` 的 UTC 自然日。直接只读扫描 RocksDB 导出，不设 `tas/` 层，不使用 `part-*`。当前正将本轮已交付增量及补充数据改按此标准导出；进度及验收见 [TAS 日包增量](tas_daily_increment_20261109.md)。

2026-11-09 已生成 [LSEG 按交易所分组索引](lseg_exchange_groups.md)。首版计划采用的大写 LSEG／tas／part 结构已被上述用户标准取代。历史清单继续记录交易所分类及旧文件来源，其一对一文件路径计划不适用于新的按日交付。

用户随后要求将分组写入最终目录根说明，已通过上述 NFS 挂载写入 `README.md`，并将中文 Excel、交易所汇总、完整 RIC 索引等 8 个配套元数据文件放到 `_catalog/lseg_exchange_groups_20261109/`。全部文件逐字节读回一致；发布回执见项目 `artifacts/cta_data_readme_delivery_20261109.json` 和目标 `_catalog/lseg_exchange_groups_20261109/documentation_delivery.json`。根目录原有 `2026/` 未改动，本次未迁移行情数据。
