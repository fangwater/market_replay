# SRAU26 休市成交：已确认 Block Trade，存在匹配取消消息

2026-11-10 回读 2026-06-22 原始 TAS 的字节保真适配分片，确认这笔记录的
`Qualifiers` 明确为 **`Block Trade[USER]`**，可以确认为大宗协议成交。
不能用 aggressor=0、没有买卖一或成交发生在休市时段，代替这个原始分类依据。

| 字段 | 原成交 | 后续取消消息 |
|---|---|---|
| RIC | SRAU26 | SRAU26 |
| Type | Trade | Correction |
| 发布 UTC | 2026-06-22 21:37:43.241123605 | 2026-06-22 21:45:16.421159358 |
| Exch Time | 21:34:45.000000000 | 21:34:45.000000000 |
| Price / Original Price | 96.05 | 96.05 |
| Volume / Original Volume | 3200 | 3200 |
| Qualifiers | Block Trade[USER] | 611[CAN_COND];2[CAN_COND_N] |
| Seq. No. | 7780 | 7796 |
| Unique Trade Identification | 空 | 空 |
| Original Unique Trade Identification | 空 | 22508526 |
| 分片行号 | 56736911 | 56736918 |

原成交的 `Block Trd` 列也为空；实际分类在 Qualifiers 中。
其 bid／ask／size 和主动方向字段在原始行为空，精简记录的 aggressor=0 表示没有明确主动方向。
成交到发布时间相差约178.241秒。按当前解释，成交时刻为芝加哥16:34:45，
处于该合约连续撮合盘的计划休市，市场状态与另行报送的大宗成交可以同时成立。

取消关系有很强的价量／时间证据：该RIC当天23,563条Trade中，
`Price=96.05, Volume=3200, Exch Time=21:34:45`仅出现一次；
当天唯一一条Correction的Original Price、Original Volume及Exch Time全部匹配。
但原成交未填唯一ID，在当天Trade中也未找到22508526，无法完成严格的逐ID关联。
最初因此记录为“高度疑似已撤销，逐ID关联待证”。用户随后授权修复后，
按完整候选历史中唯一原价／原量，加上成交时钟一致的回退规则应用取消；
这属于有源证据的唯一复合关联，仍不能声称已经完成逐ID核对。

同一取消发布时间还出现一条无价、Volume=650的Trade，不据此将650解释为这笔大宗的替代成交。
当前交付Trade继续保存原始打印。2026-11-10已修正`kline.parquet`：
21:34分钟volume=0、trade_count=0、is_ffill=true，OHLC沿用前收盘96.035，
status仍为SCHEDULED_CLOSED。随后21:35—21:59补价也修正为96.035，
22:00有新成交时使用96.04。IB及原始库不改写。

## 此前为什么没有冲减

此前生成器 `source_inventory` 只收集Trade Parquet，`sparse_minutes` 直接聚合价量及笔数；
没有读取Correction并关联、撤销或替换原成交。连续分钟迁移复用同一批实际成交分钟，
也没有补上这一步。因此修复前K线是未应用改撤的原始Trade打印聚合，不应称为最终有效成交口径。
这是处理链路缺项，不是“成交不允许撤销”的业务规则。

入库器支持将Correction保存为独立kind=12事件；精简结构没有保存Original Unique Trade Identification，
Trade精简结构也不保留唯一成交ID，不能把“消息已经归档”当成“分钟线已经应用改撤”。
原始Trade总量／笔数守恒校验只证明原打印未被丢失，未验证改撤后的有效成交。

原始Trade及Correction应全部留存；最终历史K线应按可靠关联后的有效成交重算，
包括OHLC、成交量、笔数和随后依赖该收盘的补价。存在ID或唯一性疑点时保留问题标记，
不能仅凭同价同量随意删成交。本案例的撤销关联仍受上文所述ID缺失限制。
修复入口、严格关联规则和未解决问题见 [TAS历史改撤](tas_trade_revisions.md)。

原始日包：
`/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalised/SRA/SRA_2026-06-22_tick.csv.gz`。

读取的既有适配分片：NVMe `lseg_data/tas_daily_shards_20261109/SRA/`
`shanghai_evolution_futures_time_and_sales_ric_list_0_tas_2026-06-01_2026-11-02/`
`merged-Data-part-000021-shard-000000.csv.zst`。

逐字段原始行、取消消息和唯一性核对保存在项目
`artifacts/sra_off_session_trade_20260622/`，结论为 `finding.json`。
最终盘问题记录为 `lseg/meta/problems/scheduled_closed_trades.csv`。
