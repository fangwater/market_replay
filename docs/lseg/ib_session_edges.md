# IB 在休市、复市边界的实际表现

2026-11-10，只读抽查已保存的 ESM26、NQM26、YMM26、GCM26、CLN26 五份 IB 分钟快照，
覆盖各合约已有的 2026 年 1—6 月数据。以已有 Databento 交易状态恢复的区间核对，
不重新查询 MySQL、不改变 IB 原始行，也不据此断言 IB 服务端或采集程序的内部实现。

## 观察结果

- 计划休市：五个合约共 600 个有样本覆盖的计划休市区间，IB 在完全落入休市的分钟上均无行。
  区间数按合约累计，同一天不同合约分别计数。
- 复市补价：发现 7 段复市后首个可用分钟为零量的连续行，共 128 行。
  七段所有 OHLC 均等于该合约休市前最后一条 IB 收盘价；可以跨日、跨周末延续。
- 复市缺行：602 个整分钟复市边界中，169 个在复市首分钟没有 IB 行。
  数据覆盖不完整与零量补价并存，不能由此推导为“IB 永远等首笔成交才输出”或照搬缺行。
- 盘中停牌：另有 70 个 IB 零量分钟完全落在 Databento 明确不可交易区间，
  其中黄金 67、道指 1、原油 2。IB 并非对所有不可交易状态都停止输出分钟。

| 合约 | 计划休市区间 | 其中 IB 行 | 其它不可交易区间中的 IB 行 | 复市零量补价行 |
|---|---:|---:|---:|---:|
| ESM26 | 119 | 0 | 0 | 8 |
| NQM26 | 119 | 0 | 0 | 1 |
| YMM26 | 119 | 0 | 1 | 0 |
| GCM26 | 123 | 0 | 67 | 110 |
| CLN26 | 120 | 0 | 2 | 9 |

## 可直接复核的例子

全部时间为 UTC，行标签为分钟起点。

- ESM26：2026-02-16 17:59 的 IB 收盘为 6904；18:00—23:00 休市无行。
  23:00—23:07 的八行 OHLC 均为 6904，volume=0。
  调整前同八分钟 TAS 已有开市网格，但复市重置规则使 OHLC 为空；现有规则已跨休市补价。
- GCM26：2026-06-19 16:59 的 IB 收盘为 4145.3；之后休市，6 月 21 日 22:00 复市。
  复市开始的 85 个连续零量分钟 OHLC 均为 4145.3。
- GCM26：2026-02-25 18:37:05.175186251 状态为
  `NOT_AVAILABLE_FOR_TRADING / SURVEILLANCE_INTERVENTION`，19:45 恢复交易。
  IB 在 18:38—19:44 仍保留 67 行零量平价分钟；不应把这些行当成有效成交。

## 与本项目的区别

以上是调整前的观察。用户随后明确要求停牌继续出分钟、以status区分，并采用IB式复市补价。
用户随后确认TAS改为有效期内连续UTC分钟，包括计划休市与周末；
以 `TRADING/SCHEDULED_CLOSED/HALTED/PARTIAL/UNKNOWN` 区分状态。
复市沿用本RIC最后已知TAS成交收盘价，直到新成交；不借用IB价格，不复制IB缺行。
窗口起点使用只读RocksDB历史成交预热，无该RIC历史价格时仍为null、is_ffill=false。
OHLC为Float64实际价格，量和笔数不填充；真实成交保留。
用户另行明确IB保持原样：不补行、补价或增加状态字段，原始缺行和全部原列保持不变。
比较时排除TAS计划休市及UNKNOWN的纯补行，IB仍按原始行比较。

原因码要结合进入状态前的阶段：监管干预、市场事件、恢复处理打断开市时才建立HALTED，
停牌后的PRE_OPEN继续作为该次中断，直到复市或明确计划收盘／到期。
周末准备也可能发SURVEILLANCE_INTERVENTION，不能将原本计划关闭的市场改成盘中停牌。
SCHEDULED常见于每日例行休市、周末、节假日及提前闭市；该码本身不说明具体是哪一种。
INSTRUMENT_EXPIRATION是合约到期，不等于暂时停牌。
补行窗口优先取definition有效期及明确到期状态的较早截止；缺少精确时间才使用同RIC、同交割年份HRD到期日的UTC日末。

## 复现

入口：[inspect_ib_session_edges.py](../scripts/inspect_ib_session_edges.py)。
输入是 NVMe 工作目录 `market_status_kline_20261110/contracts/<RIC>/` 下
`kline_ib.parquet`、`kline.parquet`、`intervals.parquet`，以及同工作根目录的 `status.parquet`。

```bash
/home/fanghaizhou/market_replay/reference_cme/.venv/bin/python scripts/lseg/inspect_ib_session_edges.py
```

本地逐边界结果：`artifacts/ib_session_edges_20261110/reopen_boundaries.csv`；
不可交易区间中的 IB 行：`artifacts/ib_session_edges_20261110/closed_ib_rows.csv`。
这两份记录保留调整前的TAS缺价统计；重新运行观察入口会反映当前已更新的TAS规则。
计划休市分类使用闭市区间起始消息的 `reason_name=SCHEDULED`。
休市检查仅计完全位于已知不可交易区间的整分钟，复市检查仅计闭市至少 60 秒、
且复市事件恰在分钟起点的边界；不把部分分钟误算为整分钟停牌。
