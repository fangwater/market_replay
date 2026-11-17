# LSEG L2：按固定频率下采样的 parquet（试跑）

2026-11-17 用户要求补一份 LSEG 的 L2 数据，用 parquet 表示，并支持按固定频率切片。
本数据**只由 Normalized LL2（`normalisedLL2`）日包直接处理**，不用 Raw LL2：用户说明此前比较过，Raw LL2 没有 Normalized 之外的新增数据
（数据契约见 `preprocess/data_format/lseg/normalisedLL2.md` 和 `rawLL2.md`）。
本轮是试跑：只做 CME 品种，不并入 `lseg-daily`，输出写在本机，不写交付盘。

## 数据源（已核对）

- 远端 `http://10.61.20.12/public/users/u158/public/lseg_data/future_daily/normalisedLL2/<PRODUCT>/<PRODUCT>_<YYYY-MM-DD>_normll2.csv.gz`，
  120 个品种，目录结构和 TAS 日包（`future_daily/normalised/`）相同。
- **一个文件 = 一个 UTC 日**；文件内同一 RIC 的行连续成块；块内 `Date-Time` 不递减。
  试跑的 356 个文件全部满足；不满足时工具直接报错，不排序、不猜。
- 每行是**一次更新之后的完整 L1–L10 十档盘口**：66 列 = `#RIC, Domain, Date-Time, GMT Offset, Type`
  + 10 档 × (`BidPrice, BidSize, BuyNo, AskPrice, AskSize, SellNo`) + `Exch Time`。空字段 = 该档或该值不存在。
  `Domain` 均为 `Market Price`，`Type` 均为 `Normalized LL2`，`Date-Time` 是 UTC 纳秒。
- 同一个 `Date-Time` 上有大量更新（KWR 一天 62 万行里约 45 万行与前一行时间相同）。同刻取**文件顺序最后一条**。
- 源里**没有状态标记**：收盘时不会清空盘口，只是更新停止（ES 22:00 UTC 后只剩零星更新，22:45 起盘前更新，23:00 起大量更新）。

## 规则（工具与校验完全按此实现）

频率 `Δ` 是参数（`--freq-ms`，默认 250），必须能整除一天（86,400,000 ms），这样每天的网格相同。网格以 UTC 日 0 点为起点。

1. **网格，右闭 as-of：** 网格点 `T = k·Δ`；`T` 时刻的盘口 = `Date-Time ≤ T` 的最后一条更新。
   更新时刻 `t` 落在网格点 `T = ceil(t/Δ)·Δ`；**恰好等于 `T` 的更新属于 `T`**，晚 1 纳秒就属于下一个点。
2. **只做下采样，不前向填充：** 一个网格点只有在区间 `(T−Δ, T]` 内至少有一条更新、并且它的十档盘口
   （全部 60 个价/量/笔数字段，空值也参与比较）与**同一 RIC-日上一条已输出的行不同**时才输出一行。
   没有变化就不输出。因此相邻两行的时间间隔永远不小于 `Δ`；区间内变了又变回来（和上一条已输出的行相同）不输出；
   区间内多次更新，只看区间内最后一条。
3. **锚点（一定输出，哪怕和上一行完全相同，`anchor = true`）：**
   - 每个 RIC-日的第一行；
   - 每个**交易会话的第一行**。
4. **怎么判会话起点：** 源里没有状态，所以用更新自己判：同一 RIC 两条相邻更新相隔 **≥ `--session-gap-s`（默认 1800 秒）**，
   后一条所在的网格点就是会话起点。这和国内期货按 ≥30 分钟断段的规则一致；CME 每天的维护休市 60 分钟、周末更长，都会被识别。
   没有用 TAS 的 `kline.parquet` 状态做依据，原因：交付盘上 TAS 状态只到 11-11，且个别 RIC-日缺（例如 ES 的 11-09 只有 `kline_ib.parquet`），
   而 L2 日包要独立可重放。副作用：盘中静默 ≥30 分钟的冷门合约，静默之后的第一行会多出一个锚点（即使盘口没变）。
5. **分区和日界：** 一条更新属于它所在源文件的 UTC 日，输出行也写在这一天的文件里。因此当天最后一个网格点
   （更新落在最后 Δ 内）的 `ts_utc_ns` 可能等于**次日 0 点**，这是唯一允许超出当日的情形，footer 校验的上界含当日结束。
   不依赖前一天的文件（没有跨日携带）：一天文件里的第一行就是当天第一条更新所在的网格点。
   次日文件的第一行与这一行重复的唯一可能是次日恰好 0 点整有一条更新，此时按文件顺序后者为准。
6. **会话终点不在文件里。** 休市段没有行，所以文件不能说明一个会话在哪里结束。把不规则行还原成规则网格时：

   - 从一个行起向后 ffill，直到下一行之前的最后一个网格点（行缺失 = 盘口没变）；
   - 在下一个锚点之前，如果中间隔着休市（相邻两个行的 `last_update_ts_ns` 之间静默 ≥ 会话阈值），
     休市段的终点要用交付盘 `kline.parquet` 的 `status` 或自己的交易日历确定，不能从本文件推出。

## 输出

路径（和交付盘一致）：`<根>/<交易所小写>/<UTC 年>/<RIC，tasdb::component 转义>/<UTC YYYYMMDD>/l2_<Δ>ms.parquet`，一个 RIC-日一个文件。
交易所目录来自 `--exchange-map`（`RIC<TAB>目录`，试跑从 `catalog.sqlite` 只读生成；未在映射里的 RIC 报错）。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `ric` | string | 原始 RIC，保留大小写 |
| `ts_utc_ns` | uint64 | 网格点 `T`（UTC 纳秒） |
| `last_update_ts_ns` | uint64 | 这一行盘口来自的那条更新的 `Date-Time`（`T−Δ < 它 ≤ T`） |
| `anchor` | bool | 一定输出的行：RIC-日首行、会话首行 |
| `l1_bid` … `l10_bid` | float64，可空 | 买价 |
| `l1_bid_size` … | uint32，可空 | 买量 |
| `l1_bid_orders` … | uint32，可空 | `BuyNo`，买方笔数 |
| `l1_ask`、`l1_ask_size`、`l1_ask_orders` … | 同上 | 卖价、卖量、`SellNo` |

类型依据：价格是源里的十进制文本，按 IEEE 754 最近值读成 float64（和 `kline.parquet` 的价格同为 float64，往返不丢信息）；
量和笔数在源里全是非负整数，工具遇到非整数或超过 u32 的值直接报错（试跑没有出现）。空字段写 null，不写 0、不写 NaN。

文件属性：ZSTD，每 65,536 行一个 row group，开统计。footer 的键值元数据写明频率、规则和会话阈值，顺序固定。
写入先到 `.parquet.partial`，`fsync`、校验 footer 后改名；**不覆盖**已有文件（`--force` 才替换）。
footer 校验：行数、每个 row group 的 `ric` 最小 = 最大 = 该 RIC、`ts_utc_ns`/`last_update_ts_ns` 都落在当天 UTC 日（含结束点）。
同样的输入重跑，文件逐字节相同（单元测试和整批重跑都验证了）。写交付盘 NFS 时仍须按交付盘规则用硬链接发布，本试跑不涉及。

## 资源

- 流式：逐行读 gz，只保留当前网格点最后一条原始文本；只有网格点结束时才解析那一条的 60 个字段。
  一个 RIC 的行按 row group 边写边释放，内存与文件大小无关。
- 并发：固定 `--workers`，1..=2（`limits::AGGREGATION_WORKERS`），每个 worker 处理一个文件；不随 CPU 数扩张。批处理用 `nice`。

## 命令

下载（只读 GET、无代理、≤2 并发、零字节占位和 60 分钟内修改的文件跳过、不覆盖已有文件）：

```bash
scripts/lseg/fetch_normalised_ll2.sh <品种,品种,…|品种文件> <YYYY-MM-DD,YYYY-MM-DD,…>
```

切片：

```bash
lseg_l2_slice run --out-root <输出根> --exchange-map <映射.tsv> \
  --products <品种,…> --days <YYYY-MM-DD,…> [--freq-ms 250] [--session-gap-s 1800] [--workers 2] [--force]
```

输出每个品种日一行 JSON（行数、锚点数、非空网格点数、parquet 字节、耗时），最后一行汇总（含峰值 RSS）。

## 试跑结果（CME，UTC 2026-11-06 ~ 2026-11-11，250 ms）

**范围：** 远端 L2 里属于 CME Group 四所的 60 个品种（交付盘目录索引里 `cme`/`cbot`/`nymex`/`comex` 出现过的品种；
`Cc1`–`Cc17` 连续序列远端没有 L2，不在内）：

- cme（32）：AD BP BR BTC CD CNC DM ES FC GSD HTE IBV JY KWR LC LH LXR MP NE NK NOKA NQ PHC PLZ RTY S1R SEKA SF SRA SRI TPY URO
- cbot（16）：AUL BO C FF FV KW S SM TN TU TY US W YM YR ZP
- nymex（7）：CL HO JKM NG PA PL RB
- comex（5）：ALI GC HG HRC SI

窗口取远端已稳定的 6 个 UTC 日；**11-12 不算**（60 个品种里只有 5 个发布，另有 3 个 `.part`，未稳定）。
360 个品种日里 356 个下载到本地（`/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalisedLL2/`，不覆盖）：
IBV 11-06 是零字节占位，PL 11-11、RTY 11-10、YM 11-11 远端没有文件（源缺口，未补）。
356 个文件里 86 个只有表头（周六和休市品种）。输出写在 `/mnt/hdd-raid5-72t/liang_torch/lseg_data/l2_parquet_trial/`。

| 项 | 结果 |
| --- | --- |
| RIC-日 | 2,917（约 649 个 RIC） |
| 源更新行数 → 有更新的网格点 → 输出行 | 29.12 亿 → 1.653 亿 → **1.520 亿** |
| 输出行 / 源更新 | 5.2% |
| 输出行 / 有更新的网格点 | 91.9%（主力合约几乎每个 250 ms 都在变，去重只省 8%） |
| **输出行 / 网格点** | 占每个会话首末更新之间全部网格点的 **24.9%**；占整日 345,600 点/RIC-日的 **15.1%** |
| 锚点行 | 7,523（= 会话数，等于每个 RIC-日首行加静默 ≥30 分钟后的首行） |
| parquet | 共 **4.11 GB**；27.1 字节/行；平均每 RIC-日 1.41 MB、52,098 行，最大 315,694 行 |
| 耗时 | 每个品种日：中位 1.9 s、p90 14 s、最大 116 s；共 1,477 CPU 秒，2 个 worker 墙钟 **12 分 21 秒**（约 22.6 MB 压缩包/CPU 秒） |
| **峰值内存** | **139 MiB**（2 个 worker；每个 worker 只有一行文本 + 不超过 65,536 行的 row group 缓冲） |

按 UTC 日：周五 11-06 1.17 GB（4,346 万行）、周六 0.004 GB、周日 0.044 GB、周一 1.05 GB、周二 1.06 GB、周三 0.79 GB。

**外推**（周内 5 天取平均、周末按实测，183 天≈6 个月；输出随压缩包体量线性放大，全部品种的体量按远端 6 天压缩包 39.75 GB / CME 32.58 GB = 1.22 倍）：

| 范围 | parquet | 单 worker CPU | 2 worker 墙钟 |
| --- | --- | --- | --- |
| CME 60 个品种，6 个月 | 约 134 GB | 约 13.4 小时 | 约 7 小时 |
| 全部 120 个品种，6 个月 | 约 164 GB | 约 16.3 小时 | 约 8 小时 |

这个体量不算不合理（交付盘现有约 483 GB；作比较，ESZ26 在 11-10 的 TAS trade+quote 约 10 MB，同日的 L2 约 13 MB）；更省的办法是加大频率或只留前 N 档，不是换编码。
试过的编码：每页行数上限放宽 ±1%、zstd 9 约 −3%、`BYTE_STREAM_SPLIT`＋`DELTA_BINARY_PACKED` 在 ES 上 −34% 但在 CL 上 +34%（CL 价格档位少，字典更合适），
所以没有采用任何按列编码。

## 校验（和 Rust 工具没有任何共用代码）

独立脚本 `scripts/lseg/verify_l2_slice.py`（`numpy` + `pyarrow`）：只读源 csv.gz，按同样规则向量化重算（桶内取最后一行；本桶盘口与上一个非空桶不同或是锚点就输出），
再与 parquet 逐点比较 **输出的网格点集合、`last_update_ts_ns`、`anchor` 和全部 60 个字段**：

| RIC-日 | 范围 | 结果 |
| --- | --- | --- |
| KWRF27 / KWRX26 / KWRZ26，DMZ26（11-09） | 整日 | 0 处不一致 |
| ADZ26（11-08，周日晚盘开盘） | 整日 | 0 |
| ESZ27（全天仅 312 条更新）、ESM27、ESH27（148 万条更新）（11-10） | 整日 | 0 |
| ESZ26（11-10）、CLZ26（11-10）、GCZ26（11-10）、TYZ26（11-11） | 20 分钟 / 15 分钟窗口（行数太大，窗口内逐点比对） | 0 |
| CLZ26 11-06 窗口 | 含**真实数据里唯一一行**更新恰好落在网格点上（14:43:08.750000000，源里恰有 1 行） | 0，`last_update_ts_ns == ts_utc_ns` |

另外：合成用例（`/tmp` 里的 17 行小文件，经完整命令行路径）逐一覆盖——更新恰在 `T` 上属于 `T`、晚 1 纳秒进下一个点；
同一间隔内变了又变回来不输出；同一间隔两次更新输出一次且是后一个状态；同时间戳取文件顺序最后一条；
静默恰好等于阈值是锚点、差 1 纳秒不是；日末最后 100 ns 的更新进位到次日 0 点；`FIXA`/`FIXB` 两个 RIC 块，行集合和 60 个字段与独立计算一致。
单元测试 10 个（同上情形加参数校验、源违规报错、parquet 往返、幂等）。

- **源的保证与处理：** 356 个文件没有出现时间倒退、RIC 块不连续、跨 UTC 日的行、非整数的量/笔数；工具遇到任何一种直接报错，不排序、不猜。
  同时间戳按文件顺序，最后一条生效（真实数据里同时间戳行占约一半）。
- **幂等：** 用最终版二进制重跑 6 个品种 × 5 个 UTC 日（517 个文件）→ 与整批输出 **sha256 逐个相同**；重跑已有输出不加 `--force` 时全部保留。
- **没验证的：** 会话终点不在文件里（见规则 6）；`anchor` 在冷门合约上会有盘中静默触发的多余锚点；
  没有用 TAS 状态交叉核对会话起点；全部 120 个品种和 2026-06-01 起的历史没有跑（上面是外推）；没有写交付盘 NFS。

## 扩展日期范围

```bash
scripts/lseg/fetch_normalised_ll2.sh <品种文件|品种,…> 2026-11-12,2026-11-13
scripts/lseg/l2_exchange_map.py ALL > /tmp/ric_exchange.tsv          # 只读目录索引
lseg_l2_slice run --out-root /mnt/hdd-raid5-72t/liang_torch/lseg_data/l2_parquet_trial \
  --exchange-map /tmp/ric_exchange.tsv --products <品种,…> --days 2026-11-12,2026-11-13
```
