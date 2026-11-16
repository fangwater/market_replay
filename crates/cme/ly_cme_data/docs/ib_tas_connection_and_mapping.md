# IB 连通性与 RIC 映射核查

核查日期：2026-11-03 UTC。已实际登录 `10.61.20.11:3306/ib_data`，使用只读事务查询两张分钟表及 `contract_info`。本轮只确认工具身份和日期交集，未执行 OHLC 或成交量质量比较。

本页期货 10/16 的统计是初次按已加工集合核查的历史结果。用户随后明确应核对全部原始 TAS，最新 63 个 IB 代码的结果见 [全品种原始 TAS 核查](ib_all_futures_raw_tas.md)，不要把本页 16 品种范围当作 TAS 的源范围。

## 数据库连通性

- 认证成功：`ib_reader@%`；MySQL `8.0.46-0ubuntu0.22.04.4`。
- `minute_data`、`minute_data_stock` 可读取，字段权限显示 `select`。
- `contract_info` 实际读取 2,808 条元数据，其中 FUT 2,060 条、STK 748 条。
- 分钟表实际工具代码：期货 63 个 symbol，美股 / ETF 759 个 symbol。
- 两表唯一索引均为 `(con_id, utc_time)`，另有 `(symbol, utc_time)` 索引。代码清单使用已确认的索引分组计划，日期探针使用 con_id 索引首尾查询和有界区间查询，没有全表读取分钟行情。
- 用 `utc_time` DATETIME 和毫秒 `timestamp` 对齐本机 UTC 时间。查询必须写成显式列引用，例如 ``t.`utc_time` ``，防止把同名 `UTC_TIME` 内置函数当作数据列。

## 美股与 ETF

本轮分别核对 `preprocess` 的两套正式输入范围。26 只 Nasdaq 属于原 77 只清单的子集，不能将两者相加作为证券数。

| preprocess 数据集 | 范围 | 标识可匹配且存在日期交集 |
| --- | ---: | ---: |
| `baseline_data_1m_raw`，既有 RAW 研究链 | 77 RIC，2021-07-01 至 2025-06-30，各票实际窗口不同 | 74 |
| `baseline_data_1min/NASDAQ`，新 Nasdaq 单场所底表 | 26 RIC，最早 2021-01-04、最晚 2026-08-27，各票实际窗口不同 | 26 |

RAW 77 个 RIC 的当前磁盘目录集合与 `preprocess/database/usstock_multifrequency_20261026/source_coverage.csv` 完全一致，首末日文件均存在。Nasdaq 26 的窗口直接读取正式目录的日文件清单。

映射先解析 `.O/.N/.P/.BAT` 等来源后缀，再核对 IB 合约元数据的 symbol、主场所和 USD 币种；BZX 对应 IB 的 BATS。后缀解析不等于单独的证券身份认证，仍保留原始 RIC 和 IB con_id。`BRKb.N → BRK B` 使用明确的 B 类股映射，不能变为 BRK A。

三个例外：

| RIC | 核查结果 |
| --- | --- |
| `ARKG.BAT` | IB `minute_data_stock` 的工具代码中未找到 ARKG，`contract_info` 也无对应项 |
| `CAKE.O` | IB `minute_data_stock` 的工具代码中未找到 CAKE，`contract_info` 也无对应项 |
| `SPCX.O` | 身份冲突：本地为历史 SPAC ETF；IB con_id `890493863` 的公司名为 `SPACE EXPLORATION TECHN-CL A`，有效行情从 2026-06-12 开始，不能与历史 ETF 拼接 |

实际匹配例子：`AAPL.O → AAPL / 265598`；`BRKb.N → BRK B / 72063691`；`NBIS.O → NBIS / 88819736`。这三个 IB 合约都实际查到 2025-05-29 起的有效分钟，和各自本地窗口有交集。

Nasdaq 新底表是单场所数据，不能预先假定其成交覆盖与 IB 相同；RAW / HFQ 的复权、交易时段与成交处理也须在质量比较阶段分别统一。

## 海外期货

以当前正式分钟底表的 16 个研究品种为本轮范围，逐项核对交易所、币种、完整规格的合约乘数和 IB `local_symbol`。IB 的 `maturity_date` 是到期日，不能直接用其月份代替交割月份。例：`CLM6` 到期日是 2026-05-19，但对应的 TAS RIC 是 **CLM26**。

| TAS 根 | IB symbol | 2026 TAS 同名合约数 | 其中有成交且与 IB 日期交集的合约数 |
| --- | --- | ---: | ---: |
| ES | ES | 6 | 5 |
| NQ | NQ | 6 | 4 |
| YM | YM | 5 | 4 |
| GC | GC | 24 | 16 |
| CL | CL | 24 | 17 |
| HG | HG | 24 | 16 |
| SI | SI | 24 | 15 |
| TU | ZT | 4 | 3 |
| FV | ZF | 4 | 4 |
| TY | ZN | 4 | 4 |
| RTY | RTY | 未找到相应 IB 品种 | 0 |
| URO | EUR / 6E | 未找到相应 IB 品种 | 0 |
| JY | JPY / 6J | 未找到相应 IB 品种 | 0 |
| TN | TN | 未找到相应 IB 品种 | 0 |
| US | ZB | 未找到相应 IB 品种 | 0 |
| AUL | UB | 未找到相应 IB 品种 | 0 |

共 **10 / 16 品种**可匹配。与已导出 2026 TAS 原始 RIC 对应的 IB 合约有 **125 个**，其中 **88 个**实际查到双方成交时间范围内的 IB 分钟，另 37 个无交集。对比源为已验收的 `tas_tick_parquet/2026/_audit/ric_inventory.csv`，范围是 2026-01-01 至 2026-06-01 半开区间。

`M6E` 不能替代标准 URO；MES/MNQ/MGC 等微型产品也不能替代 ES/NQ/GC。特别是本库 `symbol=SI` 还混有 `local_symbol=SIL...`、乘数 1000 的产品；本轮排除了 38 个这类规格或 local_symbol 不同的元数据项，仅将标准 SI / 5000 规格作为同一合约候选。

其他 143 个已解析身份的 IB 合约在现有 2026 TAS 原始 RIC 集合中无记录，可能属于已经到期或该源窗口无更新的合约；不能将其直接归为 IB 数据错误。本轮没有扩展检查 `preprocess` 的其它源工具或国内期货。

## 已确认的时间异常

1. 本轮查询的美股合约中，11 个出现 `utc_time='0000-00-00 00:00:00'` 的异常记录，原始样本已保留。涉及 AMZN、CHAT、CRDO、IAK、ISRG、LITE、MCD、TER、TSLA、XLI、XLRE。例如 CHAT con_id `631539225` 的 `time_cst=2026-09-15 01:48:00`，但 UTC 日期为零、`timestamp=0`。
2. 在 620 条有效首尾时间探针中，1 条 UTC / 毫秒时间戳不一致：CLH7，con_id `342054394`，`utc_time=2026-02-02 11:24:00`，`timestamp=0`，按该 UTC 应为 `1770031440000`。

这些是有界探针发现的实际异常，不是全库异常总数。日期交集基于有效 `` `utc_time` `` 查询，未以零日期或零 epoch 推导覆盖；未改写或修复任何远程记录。

## 证据与复现

- 查询程序：[check_ib_matches.py](../scripts/check_ib_matches.py)。PyMySQL 依赖记录于 `requirements-ib.txt`，本机安装在被忽略的 `artifacts/python_deps`。
- [美股逐 RIC 映射](../artifacts/ib_match_20261103/stock_ric_matches.csv)
- [期货逐品种映射](../artifacts/ib_match_20261103/futures_product_matches.csv)
- [期货逐合约映射](../artifacts/ib_match_20261103/futures_contract_matches.csv)
- [匹配汇总](../artifacts/ib_match_20261103/match_summary.json)
- [时间异常及磁盘集合验证](../artifacts/ib_match_20261103/verification.json)
- `database_discovery.json` / `contract_discovery.json` 保存表结构和索引，`instruments_snapshot.json` 保存实际 IB 合约快照，`readonly_queries.json` 保存最终只读查询及耗时。

运行方式：

```bash
python -m pip install --target artifacts/python_deps -r requirements-ib.txt
python scripts/check_ib_matches.py --output-dir artifacts/ib_match_20261103
```

原始 RIC、公司行为、完整交易分钟覆盖、是否使用复权价、IB K 线类型、成交量单位及质量阈值仍须在后续比较阶段按具体工具确认。当前结论证明上述标识和实际日期交集可用，不证明历史窗口内逐分钟完整或双方 K 线质量一致。
