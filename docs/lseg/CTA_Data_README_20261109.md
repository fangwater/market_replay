# CTA_Data 数据目录说明

更新时间：2026-11-09。最终数据落盘位置：

```text
172.16.30.100:/mnt/FuturePool/CTA_Data
```

本说明记录 LSEG 数据按交易所分组的目录约定及清单。当前服务器上的 `2026/` 为既有目录，本次写入内容为本说明和 `_catalog/` 下的分组元数据；行情文件的实际布局和交付状态，以对应数据目录的验收回执为准。

## LSEG 分组范围

第一版索引依据当前本机及 preprocess 已保存的原批次、补充批次和美股源清单：

- 221 个源作业清单，182,101 个活跃 RIC／源作业组合。
- 17,657 个资产源／原始 RIC 条目：期货源 17,576 个，美股源 81 个。
- 27 个交易所／市场目录；指数单列 `INDICES`，另设 `REVIEW` 待核实目录。
- 1,084 个 RIC 待核实：279 个参考交易所冲突，805 个缺少足以唯一归属的同 RIC 参考信息。
- TAS、Normalized LL2、Raw LL2、Raw、MBP、Summary 分别保留，不混淆数据类型。

清单同时包含本机有 Data 文件的源包和仅保存远端元数据的源包，二者在“源包覆盖”表中分别标记。这些统计不代表全部包已下载、清洗、入库或具备完整授权；不同视图的源 Count 不相加作为成交笔数。IB 和 Databento 数据不计入本 LSEG 清单。

## 按交易所分组

CME、CBOT、COMEX、NYMEX 分别建目录；CME Group 作为集团参考字段保留。历史市场代码按目录家族归并，原始 MIC 和源周期继续保留。

| 目录 | 交易所／市场 | 集团参考 | RIC 条目数 |
| --- | --- | --- | ---: |
| `ASX24` | 澳大利亚期货市场 | ASX | 240 |
| `B3` | 巴西交易所（含历史BM&F） | B3 | 296 |
| `BURSA_MALAYSIA` | 马来西亚衍生品交易所 | Bursa Malaysia | 139 |
| `CBOE` | 芝加哥期权交易所 | Cboe | 99 |
| `CBOE_BZX` | Cboe BZX RIC市场 | Cboe | 7 |
| `CBOT` | 芝加哥期货交易所（含历史KCBT） | CME Group | 1,211 |
| `CFE` | Cboe期货交易所 | Cboe | 440 |
| `CME` | 芝加哥商业交易所 | CME Group | 3,162 |
| `COMEX` | 纽约商品交易所 | CME Group | 1,408 |
| `EUREX` | 欧洲期货交易所 | Deutsche Boerse | 771 |
| `EURONEXT_PARIS` | 巴黎衍生品市场MONEP | Euronext | 120 |
| `HKEX_FUTURES` | 香港期货交易所 | HKEX | 319 |
| `ICE_CANADA` | ICE加拿大（含历史WCE） | ICE | 8 |
| `ICE_ENDEX` | ICE Endex | ICE | 3 |
| `ICE_EUROPE` | ICE欧洲（含历史LIFFE） | ICE | 4,809 |
| `ICE_US` | ICE美国（含历史NYSE LIFFE US） | ICE | 391 |
| `INDICES` | 指数（单独存放） | — | 3 |
| `INE` | 上海国际能源交易中心 | SHFE | 89 |
| `KRX` | 韩国交易所（含历史KOFEX） | KRX | 161 |
| `MEXDER` | 墨西哥衍生品交易所 | BMV Group | 38 |
| `MONTREAL` | 蒙特利尔交易所 | TMX | 160 |
| `NASDAQ` | 纳斯达克RIC市场 | Nasdaq | 36 |
| `NSE_IFSC` | 印度NSE国际交易所 | NSE | 68 |
| `NYMEX` | 纽约商业交易所 | CME Group | 1,931 |
| `NYSE` | 纽约证券交易所RIC市场 | ICE | 25 |
| `NYSE_ARCA` | 纽约证券交易所Arca RIC市场 | ICE | 13 |
| `OSE` | 大阪交易所 | JPX | 61 |
| `REVIEW` | 交易所或历史身份待核实 | — | 1,084 |
| `SGX` | 新加坡交易所（含历史SICOM） | SGX | 565 |

RIC 条目数不是标准品种数，也不是按具体到期合约去重后的数量。一位年份代码可能跨十年重复，须结合源作业和源周期使用，不能仅按原始 RIC 字符串合并历史合约。

## 行情目录约定

现有 2026 TAS Parquet 的计划路径为：

```text
CTA_Data/
  README.md
  _catalog/lseg_exchange_groups_20261109/
  LSEG/
    CME/tas/trade/NQM26/2026/part-00000.parquet
    CME/tas/quote/NQM26/2026/part-00000.parquet
    COMEX/tas/trade/HGN26/2026/part-00000.parquet
    COMEX/tas/quote/HGN26/2026/part-00000.parquet
    CBOT/...
    NYMEX/...
    ICE_EUROPE/...
    INDICES/...
    REVIEW/...
```

路径规则：`LSEG/<交易所>/tas/<trade或quote>/<原始RIC转义>/<年份>/<part文件>`。原始 RIC、字段和价格精度保持现有导出定义。其它视图后续按各自数据契约安排，不把 Raw、Raw LL2 或 Normalized LL2 混在 TAS 目录。

现有 2026 TAS 文件计划覆盖 3,493 个数据文件、53,218,483,628 字节；这是一份迁移路径计划，不是目标盘行情交付完成标记，也不代表其它年份或视图已导出。原始合并 gzip 包或 RocksDB SST 可包含多个交易所，实际拆分须按 RIC 导出，不能整包改名代替。

## 使用口径

- 交易所依据实际 HRD 的同 RIC MIC／交易所代码；COMEX 专用包的缺参考条目单独标记为专用源包依据。不仅凭代码前缀或采购 ticker 归类。
- 同 RIC 历史参考涉及多个交易所时进入 `REVIEW`，待按源日期及具体合约身份核定。比如 `PAZ0` 同时涉及 NYMEX 钯金和 LME 的不同工具，不能据此认定已经收录 LME 行情。
- 美股按既有发布 venue 或 RIC 后缀所属市场分组，表示 RIC／订阅市场。RAW 可包含跨场所成交和 NBBO；真正按逐笔成交场所分组须读取其 venue 字段。
- `.NSEI` 等指数单列；`Cc1` 等连续报价保留原始代码并标记类型；累计总量代码不转成普通期货成交。
- COMEX 与 NYMEX 分开；COMEX 金属不能替代 LME 同类金属，Globex 的前导 `1` RIC 也不与复合报价自动合并。

## 配套清单

以下文件保存在本根目录下，随本说明一并写入：

- [中文 Excel：LSEG 数据按交易所分组](_catalog/lseg_exchange_groups_20261109/LSEG数据按交易所分组_20261109.xlsx)：交易所汇总、路由根、美股、待核实 RIC、全部 RIC、源包覆盖及文件落盘计划。
- [交易所汇总 CSV](_catalog/lseg_exchange_groups_20261109/exchange_summary.csv)。
- [完整 RIC 分组索引 Parquet](_catalog/lseg_exchange_groups_20261109/ric_catalog.parquet)。
- [路由根分组 CSV](_catalog/lseg_exchange_groups_20261109/product_groups.csv)：路由根是既有分库标识，不直接等于标准品种。
- [源包覆盖 CSV](_catalog/lseg_exchange_groups_20261109/source_jobs.csv)：源周期、数据类型、原始包位置状态和清单哈希。
- [2026 TAS 逐文件落盘计划](_catalog/lseg_exchange_groups_20261109/tas_2026_file_plan.csv)：包含原机器源路径与本盘目标路径；源路径属于原机器，不表示本服务器已经存在。
- [源清单生成回执](_catalog/lseg_exchange_groups_20261109/source_catalog_completion.json)及[分组验证回执](_catalog/lseg_exchange_groups_20261109/source_catalog_verification.json)。两者记录分组阶段的验证，非行情迁移完成回执。

分组实现维护于 `/home/fanghaizhou/cn_futures_db/scripts/lseg/group_lseg_by_exchange.py`，项目详细口径见该项目 `docs/lseg_exchange_groups.md`。后续增量数据沿用交易所分组并更新源日期、分组依据和验收记录。
