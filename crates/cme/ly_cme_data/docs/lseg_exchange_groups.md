# LSEG 数据按交易所分组

当前对外说明已统一到最终盘 `lseg/README.md`，包括本页分组口径、实际交付范围、字段、抽样校验及全部清单入口；配套元数据在 `lseg/_catalog/`。根目录旧README和旧_catalog已移入LSEG目录，原绝对路径仅作为历史记录保留。
2026-11-09 用户后续指定当前正式文件结构：`lseg/<交易所小写>/<年份>/<原始RIC>/<YYYYMMDD>/trade.parquet` 或 `quote.parquet`，UTC 日期来自记录 `ts_utc_ns`，直接只读扫描 RocksDB 导出。不设 tas 层、不使用 part 文件名。本文首版的大写 LSEG 文件路径计划已经过时；交易所分类口径与源清单继续保留。实际迁移状态以 [TAS 日包增量](tas_daily_increment_20261109.md) 及新布局完成回执为准。

2026-11-09，按用户要求生成第一版交易所分组索引，用于后续向 `172.16.30.100:/mnt/FuturePool/CTA_Data` 交付数据。

## 当前结果与范围

已纳入 221 个源作业清单、182,101 个活跃 RIC／源作业组合，共 17,657 个资产源／原始 RIC 条目。其中期货来源 17,576 个 RIC，美股来源 81 个 RIC。按交易所／市场生成 27 个目录，另设 `INDICES` 和 `REVIEW`，合计 29 个索引目录。当前 1,084 个 RIC 仍待核实：279 个涉及参考交易所冲突，805 个缺少足以唯一归属的同 RIC 参考信息；3 个指数单列。

范围为当前本机及 preprocess 已保存的 LSEG 源 Report，包含原批次、补充批次和美股。期货源周期覆盖 2010 年至 2026 年现有截止日，美股源周期覆盖 2021-07-01 至 2026-08-14。它是已有清单快照的分组，不是对供应商目录的实时全量发现。仅有保存清单的远端包与本机有 Data 文件的包分别标记；清单中活跃不代表已经下载、清洗、入库或完成授权覆盖。

包含 TAS、Normalized LL2、Raw LL2、Raw、MBP、Summary，各类型保留独立来源。IB 为外部比较源，不计入 LSEG；Databento Nasdaq 底表也不计入 LSEG。baseline、HFQ、因子等研究产物不再按 Report Count 重复计算源数据覆盖。

## 分组方式

1. 以实际 HRD 的同 RIC `Market MIC` 为主，缺 MIC 时使用明确的 LSEG `Exchange Code`。ISO MIC 表用于解释市场名称，不代替 RIC 身份证据。
2. CME、CBOT、COMEX、NYMEX 分别建目录，`exchange_group` 另记 CME Group。不能把 COMEX 因 ISO operating MIC 为 XNYM 而并入 NYMEX。
3. ICE 历史 MIC、LIFFE、KCBT 等按文档中的目录家族归并，原始 MIC、交易所代码、源作业与周期均保留。目录集团是当前分类参考，不追溯每个历史日期的法律归属。
4. 同一原始 RIC 涉及不同交易所家族时放入 `REVIEW`，不凭名称、前缀或采购 ticker 强行选择。单一交易所分组也不证明跨年代 RIC 为同一具体合约。
5. COMEX 专用源包中缺少同 RIC 参考信息的条目，可按专用包分入 COMEX，依据单独标记 `dedicated_comex_source`。保留前导 `1` 等 RIC，不据此把 Globex 与复合报价合并。
6. 美股按既有发布 venue 或原始 RIC 后缀对应市场分组。它表示 RIC／订阅市场，不能把跨场所成交、NBBO 或 instrument-level 消息当成全部来自单一交易所；真正按成交场所拆分须使用 RAW 的 venue 字段。
7. `.NSEI` 等指数单列；`Cc1` 等连续报价保留交易所归属并单独标工具类型；累计总量代码保留原值，不转成普通期货成交。

特别例子：`PAZ0` 的 HRD 同时有 NYMEX 钯金和 LME `CASH SEA-ALU` 记录，已进入 `REVIEW`。这不能作为本批实际持有 LME 行情的证明。前次采购范围核查的 LME 铜／铝／镍／铅／锌仍不能用 COMEX 同类金属替代。

## 交易所目录

| 目录 | 含义 | RIC 条目数 |
| --- | --- | ---: |
| CME | 芝加哥商业交易所 | 3,162 |
| CBOT | 芝加哥期货交易所，含历史 KCBT | 1,211 |
| COMEX | 纽约商品交易所 | 1,408 |
| NYMEX | 纽约商业交易所 | 1,931 |
| ICE_EUROPE | ICE 欧洲，含历史 LIFFE | 4,809 |
| ICE_US | ICE 美国，含历史 NYSE LIFFE US | 391 |
| ICE_CANADA | ICE 加拿大，含历史 WCE | 8 |
| ICE_ENDEX | ICE Endex | 3 |
| EUREX | 欧洲期货交易所 | 771 |
| EURONEXT_PARIS | 巴黎衍生品市场 MONEP | 120 |
| CFE | Cboe 期货交易所 | 440 |
| CBOE | Cboe 历史场所代码分组 | 99 |
| SGX | 新加坡交易所，含历史 SICOM | 565 |
| HKEX_FUTURES | 香港期货交易所 | 319 |
| OSE | 大阪交易所 | 61 |
| ASX24 | 澳大利亚期货市场 | 240 |
| KRX | 韩国交易所，含历史 KOFEX | 161 |
| MONTREAL | 蒙特利尔交易所 | 160 |
| B3 | 巴西交易所，含历史 BM&F | 296 |
| BURSA_MALAYSIA | 马来西亚衍生品交易所 | 139 |
| MEXDER | 墨西哥衍生品交易所 | 38 |
| NSE_IFSC | 印度 NSE 国际交易所 | 68 |
| INE | 上海国际能源交易中心 | 89 |
| NASDAQ | 美股 Nasdaq RIC 市场 | 36 |
| NYSE | 美股 NYSE RIC 市场 | 25 |
| NYSE_ARCA | 美股 Arca RIC 市场 | 13 |
| CBOE_BZX | 美股 BZX RIC 市场 | 7 |
| INDICES | 指数，非期货交易所 | 3 |
| REVIEW | 交易所或历史身份待核实 | 1,084 |

RIC 条目不是标准品种数，也不是具体到期合约的去重数。一位年份代码可以跨十年复用；完整明细保留源作业与周期。路由根是既有 replay 的分库标识，日合约、连续报价和历史别名会产生不同根，不能把“路由根数”直接称作品种数。

## 文件和最终落盘路径

索引目录：`artifacts/lseg_exchange_groups_20261109`。

```text
exchanges/
  CME/ric_catalog.csv
  CME/ric_catalog.parquet
  CME/products.csv
  COMEX/...
  ICE_EUROPE/...
  INDICES/...
  REVIEW/...
```

- [中文 Excel](../reports/LSEG数据按交易所分组_20261109.xlsx)：交易所汇总、期货路由根、美股 RIC、待核实 RIC、全部 RIC、源包覆盖、2026 文件落盘计划及分组口径。
- `source_jobs.csv`／`.parquet`：选用的 221 个源作业清单、SHA256、视图、源周期、本机 Data 文件数量和体积；本机存在 Data 文件不等于文件完整性已经验收。
- `source_ric_details.csv`／`.parquet`：182,101 个活跃 RIC／源作业组合，保留源 `Count`、首末时间、原始 RIC 和分组依据。
- `ric_catalog.csv`／`.parquet`、`product_groups.csv`／`.parquet`：分组索引和原路由根清单。
- `duplicate_reports.json`：同源作业多个清单副本及字节是否相同，优先选择与本机行情包同目录的清单，不将副本重复统计。
- `ISO10383_MIC.csv`：本轮从 [ISO 官网](https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv) 下载的市场代码参考快照；输入哈希见完成回执。

对已验收的 2026 TAS Parquet，另生成 [逐文件计划](../artifacts/lseg_exchange_groups_20261109/tas_2026_file_plan.csv)，覆盖现有 3,493 个数据文件、53,218,483,628 字节，保留 trade／quote 和原始 RIC。示例目标路径为：

```text
172.16.30.100:/mnt/FuturePool/CTA_Data/
  LSEG/COMEX/tas/trade/HGN26/2026/part-00000.parquet
  LSEG/COMEX/tas/quote/HGN26/2026/part-00000.parquet
  LSEG/CME/tas/trade/NQM26/2026/part-00000.parquet
```

这是索引和落盘计划，分组生成阶段未迁移行情或访问目标服务器。原始 gzip 合并包、RocksDB SST 可能包含多个交易所，不能整包改名就算完成按交易所拆分；后续实际导出须使用逐 RIC 分组索引并保留源列与校验回执。该文件计划只覆盖现有 2026 TAS 数据文件，不代表其它年份或视图已导出，也不代表目标盘行情交付完成。

2026-11-09 用户随后要求写入最终根目录说明，已通过本机 NFS 挂载 `/mnt/nfs/30.100_cta_data` 新建目标根 `README.md`，写入上述分组与目录约定，并将中文 Excel、汇总、RIC 索引等 8 个配套元数据文件放在目标 `_catalog/lseg_exchange_groups_20261109/`。所有写入文件逐字节读回一致，发布回执为项目 `artifacts/cta_data_readme_delivery_20261109.json` 及目标目录内的 `documentation_delivery.json`。这是说明与元数据发布，未修改根目录已有 `2026/`，未迁移行情文件。

## 复现和验证

```bash
cd /home/fanghaizhou/cn_futures_db/crates/cme/ly_cme_data
python scripts/group_lseg_by_exchange.py
```

实现：[group_lseg_by_exchange.py](../scripts/group_lseg_by_exchange.py)。完成回执：[completion.json](../artifacts/lseg_exchange_groups_20261109/completion.json)。独立读回验证：[verification.json](../artifacts/lseg_exchange_groups_20261109/verification.json)。

验证包含源作业／RIC 主键唯一、源 Count 与逐作业明细守恒、所有活跃条目均进入明确分组或待核实目录、各分组索引读回一致、已知交易所和冲突 RIC 抽查、Excel 页数与行数、2026 文件计划源／目标路径唯一及文件数／字节数与已有交付一致。本轮仅生成项目目录内的索引和报告，未打开或改写任何生产 RocksDB。
