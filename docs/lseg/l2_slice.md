# LSEG L2：按固定频率下采样的 parquet（试跑）

2026-11-17 用户要求补一份 LSEG 的 L2 数据，用 parquet 表示，并支持按固定频率切片。
本数据**由 Normalized LL2（`normalisedLL2`）日包直接处理**。Raw LL2 只用于 Normalized 缺失的 RIC（2026-11-19 起，见 [l2_raw.md](l2_raw.md)）：此前比较过，Raw 对 Normalized 已有的 RIC 没有新增数据，但 Normalized 完全没有的品种（CL、NG、美债等）Raw 里有
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

## 流水线：分片 → 洗入 RocksDB → 提取（2026-11-17 用户定）

和 TAS 一样三段，频率只在最后一段：

1. **分片（shard）**：每个 csv.gz 只解压一次，按 RIC 边界（一个 RIC 不跨分片）写成 zstd(1) 的 csv 分片（默认约 400 万行一片），放 NVMe `runtime/l2_shards/`。
   分片编码用 `cme_tas_replay::shard::ZstdShardWriter`，和 `cme_tas_shard` 共用同一份（字节相同，有测试）。洗完立即删分片；失败的残留下次启动时清理。
2. **洗（wash）**：把分片逐行解析，写进**每个 UTC 日一个** RocksDB（`runtime/l2_db/<YYYYMMDD>/`），**全部更新**，不带任何频率。
   每个 RIC 写成一个 SST 文件再 ingest（无写放大、关自动 compaction）；每个品种日完成后写完成标记 `M|<品种>`（含 RIC 列表和更新数，同步落盘），
   读方只信有标记的品种日；重跑会先 `delete_range` 掉同一 RIC 的残留。用 `--force` 才重洗已有标记的品种日。读取用 `tasdb::SharedCache`（固定上限、后台线程 1 个）。
3. **提取（extract）**：独立步骤，**RIC 为单位并行**（一个 RIC-日 = 一个任务 = 一个 parquet，大的先跑），读 RocksDB 解码后走同一套切片规则。
   `--freq-ms` 是参数：换 1 s、5 s 只需重新提取，不碰 gz。提取完核对解出的更新数等于标记里的更新数。

调度：一个固定大小的线程池（`--workers`，≤ 8，合计线程就是这个数）；解压最大的 gz 优先；洗和提取优先于开新分片；
分片积压有字节预算（`--shard-budget-gib`，默认 16），超预算时解压线程自己去洗，所以 1 个 worker 也不会死锁。
**gzip 是多成员文件**（LC 6 个、CL 97 个成员），但 `flate2::MultiGzDecoder` 仍是单线程顺序解压，所以单个巨大文件（SRA）的分片阶段无法并行：SRA 11-10（4.0 GB gz，3.46 亿行）单独洗要 3 分 24 秒，其中分片 162 秒。

### 键和值（怎么存、delta 怎么定义）

- 键：`U | RIC | 0x00 | UTC 秒（u32 大端）`。**RIC 在最前**，所以一个 RIC 的全部数据连续；一个 RIC、一个有更新的 UTC 秒一个键（不是每条更新一个键）。
- 值：这一秒内所有更新，按文件顺序。每条更新 = `varint(时间步长 ns)` + 一条记录。第一条的步长是秒内偏移，之后是和上一条的差。
- **记录（delta）的定义：** 对比同一 RIC 上一条更新的 60 个槽（10 档 × 价/量/笔数）：
  - 标志字节：bit0 = 自包含（checkpoint），bit1 = 有价格走原始 8 字节转义，值 4 = 与上一条完全相同；
  - `changed` 位图（哪些槽变了）、`present_after` 位图（变了之后哪些槽有值，其余变成空）、可选 `raw` 位图；
  - 只写变了且有值的槽：价格 = 与该槽上次存的价格相比的整数步数（价格按 1e-9 的整数尾数，zig-zag + varint，**每个值都校验能无损还原，否则走 8 字节原始转义**），量和笔数 = varint。
  - **没有变化的行不是"没有记录"，而是 1 个字节**：因为切片需要每一条更新的时间（`last_update_ts_ns`、静默 ≥30 分钟判会话起点、哪个网格点有更新），行不能消失。
- **checkpoint：** 每个 RIC 的第一条，以及之后每 256 个有更新的秒的第一条，是自包含记录（不依赖前面的状态），所以任何一段都能独立读。
- RocksDB：zstd 级别 9、块 256 KB、不 compaction。`V` 键记格式版本 `l2db-2`。
- **一个 CF 还是每品种一个 CF：** 本次**没有**做每品种一个 CF，只用一个 CF（键以 RIC 开头）。理由：试跑每日库仅 2–4 GB；单品种要删时按标记里的 RIC 列表 `delete_range` 即可。
  没有实测对比，如果以后要对单个品种单独 compaction/整体删除再做。

### 布局选型实测（4 个输入文件、2,943 万条更新，只比 RocksDB 的 SST 大小、编码和解码速度）

| 方案 | 字节/更新 | 说明 |
| --- | --- | --- |
| A 每条更新一个键，v1 delta（原始 f64 价格），LZ4 | 12.01 | 第一版，整个试跑窗口 **13.09**（38.12 GB）|
| D 1 秒桶 + v2 delta，zstd 3，块 64 KB | 4.23 | |
| N 同上 zstd 6，块 256 KB | 3.74 | |
| **J 同上 zstd 9，块 256 KB（采用）** | **3.59** | 解码约 560 万行/秒 |
| K zstd 9，块 1 MB | 3.54 | 收益 1.5%，不值得 |
| L zstd 12，块 256 KB | 3.50 | 编码慢（42 s） |
| M zstd 9 + 64 KB 字典 | 3.57 | 编码最快，没采用（复杂度） |

所有方案的价格都逐位还原（比对 `to_bits`）。选 J：和 L 只差 2.5%，编码不慢。用的临时对比工具已删，不进仓库。

## 内存直通洗库（`wash --mode stream`，默认；2026-11-17）

不再写分片：线程读一个块（8 MiB）、按 RIC 切成**段**（约 32 MiB，只在顺序写入器本来就会写完整记录的地方切：RIC 首行、UTC 日首行、每日每 256 个非空秒），
任何线程都可以洗任何段，库里的字节和顺序洗一遍完全相同（测试 `any_segmentation_stores_the_bytes_of_a_sequential_pass`）。
8 个线程是同一种线程：队列里有段先洗段，否则读正在读的文件的下一块，否则开新文件（`--producers` = 同时读的文件数上限，默认 4）。
装包的桶打包器每个 UTC 日重启，所以每个（RIC，日）自成一体；日包进每日一个库，周期包按"一个单元一个库"、各分片首尾相连当作一个流（跨分片的 RIC 不需要特殊处理）。

洗的改动（都不改变解出的内容）：增量解析（只解析有变化的字段区域）、快速十进制、不再写读分片、段缓冲复用（避免大块分配的缺页）、zstd 级别 9 → 6。

同一批试跑（CME 60 品种 × 11-06..11-11，8 个线程，`nice -n 10`）：

| 方案 | 墙钟 | CPU 秒 | 平均核数 | 库大小 | 峰值内存 |
| --- | --- | --- | --- | --- | --- |
| 旧：分片 + 洗（zstd 9），8 | 18 分 40 秒 | 约 6,300 | 5.8 | 11.56 GB | 557 MiB |
| 内存直通，zstd 9 | 10 分 21 秒 | 4,926 | 7.9 | 11.58 GB | 3.1 GB |
| **内存直通，zstd 6（默认）** | **8 分 48 秒** | **4,187** | 7.9 | **11.94 GB（+3.3%）** | 3.2 GB |

- 仅 SRA 11-10（4.0 GB gz、3.46 亿行，单个文件）：**1 分 31 秒**、490 CPU 秒、754 MB（旧 3 分 24 秒）。
- zstd 级别（CL 11-11 单日，CPU 秒 / 库大小）：9 → 51.6 / 127.1 MB；7 → 47.6 / 130.1；**6 → 45.4 / 130.6**；5 → 44.3 / 133.6；4 → 43.9 / 136.7。`--sst-zstd-level` 可改。
- CPU 构成（CL 11-11，段内）：行解析约 33%，zstd 压缩约 24%，读 + 切段约 15%，其余为时间戳、编码和 SST 写入。
- 提取 250 ms：8 个线程 1 分 24 秒；**2,917 个 parquet 的 sha256 和 `l2_parquet_trial/` 完全一致。**
- 峰值内存 3.2 GB 来自同时读 4 个文件（各约 40 MiB 缓冲）和排队的段，仍在预算内（64 GiB）。
- 失败按单元隔离：某个文件出错只让它自己没有完成标记，其余继续；`--mode shards` 保留旧路径作对照。

## 规模和耗时（旧的分片路径，CME 60 品种，UTC 11-06 ~ 11-11，29.12 亿条更新，356 个文件，33.42 GB gz；`nice -n 10`，页缓存已逐出）

| 阶段 / `--workers` | 墙钟 | 峰值内存 | 平均核数 | 分片目录峰值 |
| --- | --- | --- | --- | --- |
| 分片 + 洗，2 | 52 分 41 秒 | 366 MiB | 2.0 | 6.2 GB |
| 分片 + 洗，4 | 27 分 23 秒 | 405 MiB | 3.9 | 6.6 GB |
| 分片 + 洗，8 | **18 分 40 秒** | 557 MiB | 5.8 | 5.8 GB |
| 分片 + 洗，仅 SRA 11-10，8 | 3 分 24 秒 | 131 MiB | — | 1.5 GiB（积压峰值）|
| 提取 250 ms，8 | **2 分 17 秒** | 458 MiB | — | — |

- 每次洗完：分片目录不存在，库目录里没有 `ingest_tmp` 的 `.sst`，输出里没有 `.partial`。
- 洗的时候分片压缩后合计 32.12 GB，但任一时刻在盘上最多 6.6 GB。
- RocksDB：**11.56 GB，3.97 字节/条更新**（原始值 14.1 B/条被压到 3.97；是源 csv.gz 的 **0.35 倍**；第一版 v1 是 38.12 GB、13.09 B/条）。分日：11-06 3.55 GB、11-09 2.86 GB、11-10 3.19 GB、11-11 1.85 GB。
- **提取吞吐：** 29.1 亿条 / 137 秒 = **约 2,130 万条/秒**（8 个 worker）；此前直接从 gz 一遍流式出片（上表）8 个 worker 用 5 分 21 秒 ≈ 910 万条/秒，其中大头是解压。
- **提取结果和旧的直接出片逐字节相同：** 2,917 个 parquet 的 sha256 与 `l2_parquet_trial/` 里的完全一致（没有多余文件）。独立校验脚本验证的是同一批文件，因此仍然有效（没有重跑）。
- 代价要说清楚：**只出一个频率时，新路径（洗 18 分 40 + 提取 2 分 17 ≈ 21 分钟）比直接流式（5 分 21）慢约 4 倍**；
  第二个频率起每次只要约 2 分钟，比再跑一遍直接流式快 2.3 倍。洗变慢的原因是 zstd 9 压缩和分片阶段，不是读盘。

**外推**（每条更新 3.97 字节，6 个月 183 天、CME 约 9.5 × 10¹⁰ 条更新；全部 120 个品种 ×1.22）：

| 范围 | RocksDB | 洗（8 worker） | 提取一个频率（8 worker） |
| --- | --- | --- | --- |
| CME 60 品种，6 个月 | 约 377 GB | 约 10 小时 | 约 1.2 小时 |
| 全部 120 品种，6 个月 | 约 460 GB | 约 12 小时 | 约 1.5 小时 |

NVMe（`/mnt/nvme-raid0-28t`）现在剩 8.0 TB，容量足够；洗的时候的分片目录峰值不到 7 GB。**未验证：** 超出试跑窗口的任何范围（按用户要求没有跑）；每品种一个 CF；提取在 2、4 个 worker 的耗时。

## 资源

- 流式：逐行读，一个 RIC 的行按 row group 边写边释放，内存与文件大小无关。
- 并发：固定 `--workers`，默认 2；上限 1..=8（`limits::L2_SLICE_WORKERS`），不随 CPU 数扩张。批处理用 `nice`。
  **超过 2 是例外：** 用户 2026-11-17 批准只对本工具放宽（其余工具的聚合上限仍是 `limits::AGGREGATION_WORKERS = 2`），
  理由：每个 worker 内存很小（约 70 MiB）、纯 CPU 型、各文件互相独立。**只用于一次性回补；** 每日任务不得超过 2。

## 命令

下载（只读 GET、无代理、≤2 并发、零字节占位和 60 分钟内修改的文件跳过、不覆盖已有文件）：

```bash
scripts/lseg/fetch_normalised_ll2.sh <品种,品种,…|品种文件> <YYYY-MM-DD,YYYY-MM-DD,…>
```

```bash
# 1+2 洗入库（频率无关；默认内存直通，--mode shards 为旧的分片路径）
lseg_l2_slice wash --products <品种,…> --days <YYYY-MM-DD,…> [--workers 2..8] [--producers 4] [--sst-zstd-level 6] [--db-root …] [--force]
# 3 提取（频率是参数）
lseg_l2_slice extract --out-root <输出根> --exchange-map <映射.tsv> --products <品种,…> --days <…> [--freq-ms 250] [--session-gap-s 1800] [--workers 2] [--force]
# 不入库，按单线程分阶段计时
lseg_l2_slice profile …
```

每个品种日输出一行 JSON，最后一行汇总（含峰值 RSS）。

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

## 并发实测（旧的直接流式出片路径，已被上面的三段取代，仅作对照；同一批 360 个品种日，2026-11-17）

同一套输入、`nice -n 10`，每轮前用 `posix_fadvise(DONTNEED)` 把原始包从页缓存里逐出（这台机器 1 TB 内存、页缓存 460–720 GB，不逐出就测不到读盘）。
共享机器：另有别人的任务在跑，整机负载 30+，`iostat` 是整块 `sdb` 的数据，包含别人的读写。

| `--workers` | 墙钟 | 加速（对 2） | 峰值内存 | 平均占用核数（CPU 时间/墙钟） | `sdb` 读 MB/s 均值/峰值 | `sdb` %util 均值/峰值 | 与 2 个 worker 的整棵输出树 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 13 分 44 秒 | 1.00× | 135 MiB | 1.6 | 72 / 129 | 42 / 73 | 基准 |
| 4 | 8 分 29 秒 | 1.62× | 261 MiB | 2.5 | 135 / 189 | 46 / 68 | **2,917 个文件逐个 sha256 相同** |
| 8 | 5 分 21 秒 | **2.57×** | 463 MiB | 4.3 | 154 / 200 | 43 / 69 | **逐个 sha256 相同** |
| 8，页缓存是热的 | 4 分 20 秒 | — | 465 MiB | 4.9 | 74 / 119（别人的读写加我们写输出） | 91 / 93 | 未比较（同一份代码） |

（第一次整批试跑的 12 分 21 秒是页缓存部分热的结果，冷缓存重测为 13 分 44 秒。）

- **读盘是不是瓶颈：** 不是主要瓶颈。冷缓存比热缓存慢 61 秒（19%）；工具自己读 33.4 GB，8 个 worker 时平均只要约 104 MB/s，
  `sdb` 的读延迟 11–15 ms、利用率没有顶满。所以读盘只占小头。
- **8 个 worker 只用到约 4.3–4.9 个核的原因：** 任务大小很不均匀，6 个 SRA 日包（压缩后 2–4 GB）每个由单个 worker 处理，需要 80–126 秒，
  占掉了最后一大段墙钟；其余是共享机器上的 CPU 竞争。CPU 总时间约 1,370 秒，8 个核理想是 172 秒。
  后续如果要再提速，办法是把大文件排在前面先开跑（不影响输出），而不是再加 worker。
- **内存：** 约 58 MiB/worker，8 个 worker 峰值 465 MiB，远在预算内。
- **回补建议：** 6 个月的一次性回补用 `--workers 8`：按实测，CME 60 个品种约 3 小时（冷缓存）、全部 120 个品种约 4 小时；
  同样的活用 2 个 worker 约 8–10 小时。**每日任务和其他工具仍按 2 个。**
  回补用 `nice`，避开 `cn-daily`（上海 16:30 开始，RocksDB 读写重）的时段。

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
lseg_l2_slice wash --products <品种,…> --days 2026-11-12,2026-11-13
lseg_l2_slice extract --out-root /mnt/hdd-raid5-72t/liang_torch/lseg_data/l2_parquet_trial \
  --exchange-map /tmp/ric_exchange.tsv --products <品种,…> --days 2026-11-12,2026-11-13
```

## 全量洗库：2025 + 2026（2026-11-18 完成）

把 Normalized LL2 的**全部品种**洗进 RocksDB，之后任意频率都从库里提取，不再重新解压。完成标志是 `runtime/l2_backfill/report.json`（`complete: true`）。

**位置和覆盖**（`/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/l2_db/`，共 923.7 GB，是压缩包 2,306 GB 的 0.40 倍）：

| 库 | 来源 | 源行数 | RIC | 时间 | 压缩包 → 库 |
| --- | --- | --- | --- | --- | --- |
| `_stream/p2025` | 2025 原始周期包，3 个文件 | 236.4 亿 | 1,495 | 2025-01-01 ~ 12-31 | 283.9 → 116.9 GB |
| `_stream/s2025` | 2025 补充包（`merged-Data.csv.gz`） | 90.4 亿 | 44 | 2025-01-01 ~ 12-31 | 96.8 → 44.2 GB |
| `_stream/p2026h1` | 2026 上半年周期包，9 个文件 | 969.5 亿 | 2,356 | 2026-01-01 ~ 05-31 | 883.2 → 383.0 GB |
| `YYYYMMDD`（每个 UTC 日一个库） | 2026-06-01 ~ 11-12 的日包，全部 120 个品种 | 881.0 亿 | — | — | 1,042.4 → 379.6 GB |

- 补充包只有 44 个 RIC 有数据（输入列表是 71 个），其余是源端版权抑制，不是漏洗。
- 周期包的每个文件是一整条 gzip 流、按 RIC 再按时间排序，所以**一个 RIC 可以横跨两个文件**（ESH26 在 `p2026h1` 的第 0、1 个文件之间，GCV26 在第 1、2 个之间）。每个周期包是一个库，文件按顺序当一条流读入。
- 覆盖到 2026-11-12。源端的缺口（远端没有的文件）记在 `runtime/l2_backfill/source_gaps.json`，共 374 个，没有 `.part` 未下完的。

**怎么跑的**：`runtime/l2_backfill/run_backfill.sh`，即

```bash
nice -n 10 <bin>/lseg_l2_slice backfill --workers 8 --producers 4 --segment-mib 512 \
  --progress <root>/progress.json --gaps <root>/source_gaps.json
```

- 每个单元（一个周期包，或一个品种日）洗完才写完成标记，再跑一次会跳过已完成的单元。
- 开始前和每个新单元前查服务器的 `/api/admin/status`：有任务在跑，或 10 分钟内有定时任务要开始（`cn-daily`、`ib-kline-daily`、`lseg-daily`、`cn-reference-daily`）就暂停，不和日更任务并发。
- 总共 19,264 个单元，墙钟约 11.7 小时（2026-11-18 00:27 ~ 12:11，含为定时任务暂停的时间）；8 个线程，内存约 8–12 GB，没有错误。
- 二进制是从固定位置的副本运行的，不要直接用构建目录里的。

**校验**（证据在 `runtime/l2_backfill/`：`report.json`、`spotcheck.jsonl`、`boundary_*.jsonl`）：

- 10 个 RIC 日的独立抽查（`scripts/lseg/verify_l2_ricday.py`，另一套 numpy 代码按源 csv.gz 重算 250 ms 切片，和库里提取的逐点比较时间点、最后更新时间、锚点、60 个盘口字段）：2025 年 5 个（原始包 3 个文件、补充包、一个后期日），2026 年 5 个（上半年包 3 个文件、2 个日包含 06-01 衔接日），**全部 0 不一致**。
- 2 个**横跨两个文件的 RIC 日**：ESH26 2026-02-10（1,006 万源行 → 24.5 万行切片）、GCV26 2026-02-13（167 万源行 → 12.4 万行切片），**0 不一致**，缝上没有多余的锚点行。
- 衔接处没有重叠：2025/2026 年交界、周期包与 2026-06-01 日包交界。
- **没有做的**：从库里把全部更新条数读回来逐一核对（没有这样的读取接口）；2025 原始包和补充包之间相同行的重复个数没有记录（两者在不同的库里，运行中没有冲突报错）。

**报告**：`scripts/lseg/l2_backfill_report.py --root <runtime/l2_backfill> --db-root <runtime/l2_db>`，只有真的完成才写 `report.json`，否则写 `report.incomplete.json`。

**怎么用这些库提取切片**：`lseg_l2_slice extract-rics --db-dirs <库1,库2,…>`（按时间顺序给出，周期包和日库可以混用）`--rics <RIC,…> --days <日期,…>`，频率用 `--freq-ms`（默认 250），输出到 `--out-root`。

**怎么延伸**：新日包用 `scripts/lseg/fetch_normalised_ll2.sh` 下载，再用同一条 `backfill` 命令加大 `--daily-end`，已完成的单元自动跳过。2024 年包可以用 `--stream p2024=<各文件,逗号分隔> --no-daily` 洗到 `_stream/p2024`。

## 交付到 NFS 交付盘（2026-11-18 完成 2025 + 2026）

`lseg_l2_slice deliver`：从洗好的库提取，**只用 `lseg_delivery::publish` 这一个发布流程**（写临时文件、fsync、校验 footer、硬链接发布、不覆盖；和 `lseg-daily` 共用同一份代码），目标是
`/mnt/nfs/30.100_cta_data/lseg/<交易所>/<UTC 年>/<RIC>/<UTC YYYYMMDD>/l2_250ms.parquet`。

- 可续跑：目标已是有效 L2 文件的 RIC-日直接跳过（只查 footer，不重新提取）；内容不同的已有文件只记问题、不动。
- 每批（默认 3000 个文件）先提取到本地 NVMe 暂存，再用 8 个线程发布；提取下一批和发布这一批重叠，暂存最多两批，批完即删。每批的首、中、末三个文件做完整 SHA-256 并从交付盘读回。
- 交付盘上**只写问题**：`meta/problems/l2_deliver/problems.csv`，第一个问题出现时才创建；进度只在本地 JSON（`--progress`）。
- 有任务在跑、排队或 10 分钟内要开始时暂停（`/api/admin/status`，和洗库同一套门控）。
- 交易所目录：先查交付目录 `catalog.sqlite`，再查已批准计划 `artifacts/tas_status_20261109/plan.json` 的 `exchanges`（只按 RIC 取，`year` 只是 2026 年作业的年份）。两边不一致记 `exchange_conflict` 且不交付；两边都没有记 `no_exchange_directory`，不猜。用 `scripts/lseg/l2_exchange_map.py ALL --plan <plan.json> --conflicts <out.tsv>` 生成映射。
- 吞吐受交付盘的元数据延迟限制（8 个线程约 31–40 个文件/秒），不是带宽或客户端 CPU。

**结果**：2025-01-01 ~ 2026-11-12，共 657,460 个文件、404.1 GB，**0 个问题**，全部 RIC-日都有交易所目录（0 未映射、0 冲突、0 个 RIC-日同时在两个库里）。

| 段（源） | 文件数 | GB | 墙钟 |
| --- | --- | --- | --- |
| 2026-06-01 ~ 11-12（日库） | 213,253 | 164.1 | 2 h |
| 2026-01-01 ~ 05-31（`p2026h1`） | 190,414 | 140.1 | 1.5 h |
| 2025（`p2025` + `s2025`） | 253,793 | 99.9 | 1.3 h |

按月 RIC-日数：2025 年每月 19,892–22,425；2026-01 ~ 05 为 38,950 / 35,795 / 39,862 / 37,175 / 38,632；2026-06 ~ 11 为 40,070 / 41,001 / 39,224 / 39,214 / 38,736 / 15,008。每月交付数与库里有数据的 RIC-日数相等。
独立核对：13 个随机文件（跨三个源、多个交易所、含冷门 RIC 和 CFE 周合约）从库里重新提取，和交付盘文件**逐字节相同**。

命令：

```bash
scripts/lseg/l2_exchange_map.py ALL --plan <plan.json> --conflicts conflicts.tsv > exchange_map.tsv
# 日库用 --db-root <l2_db>；周期包用 --stream-dbs <库1,库2>
nice -n 10 lseg_l2_slice deliver --exchange-map exchange_map.tsv --exchange-conflicts conflicts.tsv \
  --db-root <l2_db> --from 2026-06-01 --to 2026-11-12 \
  --extract-workers 6 --publish-threads 8 --batch-files 3000 --staging <本地 NVMe 目录> --progress progress.json
lseg_l2_slice publish-bench ...   # 只测发布吞吐
lseg_catalog l2-sync              # 交付完后更新目录索引（见下）
```

**目录索引**：`lseg_catalog l2-sync` 对交付盘做只 stat 的遍历，写表 `l2_directories(ric, utc_year, directory, first_day, last_day, days)` 和两条 `conventions`；不动 TAS 表。**每次全量 `lseg_catalog build` 之后要再跑一次**（build 会重建数据库），且只在没有任务运行时跑（目录库只有一个写入者）。

**2024**：`_stream/p2024` 洗完（`runtime/l2_backfill/progress_2024.json` 的 state 为 finished）之前不要打开。洗完后：重新生成映射并看冲突/未映射，然后
`deliver --stream-dbs <l2_db>/_stream/p2024 --from 2024-01-01 --to 2024-12-31`，再跑 `l2-sync`。

**新日期**：新的日库洗完后，用同一条 `deliver --db-root … --from <新日> --to <新日>` 即可；已交付的跳过。

## 补充包与 2024 主包（2026-11-18）

**清点**：`scripts/lseg/l2_supplement_inventory.py --work <dir>` 只读远端 `future_supplement/normalisedLL2/` 的目录和各包的 `merged-Report.csv.gz`（缓存在 `runtime/l2_supplement/reports/`），逐包对比主包 Report 和已交付的 `l2_directories`，给出新增 RIC、新增品种、交易所映射状态，并为每个包写出"新 RIC"清单（`only_rics/<包>.txt`）。结论：只有 5 个包带来新数据——`comex_globex` 2024（112 个 RIC）、`comex` 2025（162）、`futures_11` 2024 / 2025 / 2026（320 / 321 / 138）。`comex` 2024 与 `comex` 2026 的 RIC 都已在主包里（2026 的补充包行数更少），不洗。

**2024 主包**：`_stream/p2024` 交付 380,573 个文件（220.9 GB，0 个问题，0 未映射）；9 个分片各抽 1 个 RIC-日做源级独立核对（0 不一致），另有 11 个 RIC-日从库里重新提取，与交付盘文件逐字节相同。

**补充包**：每个包洗进自己的 `_stream/<id>`（`sg2024`、`sc2025`、`sf2024`、`sf2025`、`sf2026`；RIC 数与清单一致：112 / 162 / 358 / 359 / 282），再用 `deliver --stream-dbs <库> --only-rics <清单>` 一个库一个库地交付，**只交付该时段的新 RIC**。同一 RIC-日在两个库里会被 `deliver` 记成 `in_two_databases` 且两边都不交付，所以补充包不能和主包一起交付。

| 包 | 新增 RIC-日 | GB |
| --- | --- | --- |
| `comex_globex` 2024（`sg2024`） | 15,453 | 16.0 |
| `futures_11` 2024（`sf2024`） | 57,253 | 17.2 |
| `futures_11` 2025（`sf2025`） | 56,758 | 16.3 |
| `comex` 2025（`sc2025`） | 30,848 | 29.3 |
| `futures_11` 2026（`sf2026`） | 12,764 | 3.3 |

全部 0 个问题。**重叠**：补充包里和主包重叠的 RIC，抽 40 个 RIC-日从补充库提取，与交付盘上已有的文件**逐字节相同**（40/40），没有覆盖任何文件。独立核对：新 RIC-日 19 个，源级核对加逐字节比对，0 不一致。

**交易所映射**：先目录库，再计划，最后才用采购目录 `artifacts/lseg_exchange_groups_20261109/ric_catalog.csv`（`l2_exchange_map.py --ric-catalog`，只补前两处都没有的 RIC，不一致记入冲突；它与前两处已知的 16,969 个 RIC 100% 一致）。`comex_globex` 的 112 个 RIC 只有它有（全是 `comex`）。

**下载**：`scripts/lseg/fetch_supplement_ll2.sh <包目录…>`（不属于 lseg-daily）。

## L2 补不起来的 RIC：`meta/l2_gaps.sqlite`

TAS 有、L2 没有（或只有一部分）的 RIC-年，单独放在交付盘的 `lseg/meta/l2_gaps.sqlite`（不放进 `catalog.sqlite`：lseg-daily 每次从头重建目录，写入者锁也是同一把）。一句话查询：

```sql
select product, rics, kind, reasons from l2_unfillable_products where utc_year=2025 order by rics desc;
```

- `l2_unfillable(ric, utc_year, product, exchange_dir, tas_first_day, tas_last_day, tas_days, l2_days, l2_first_day, l2_last_day, kind, reason, evidence, checked_packages, checked_utc, n_rics)`：2024–2026 每个 RIC-年一行；2010–2023 只做了清点（没有洗库），按品种一行（`ric` 为空，`n_rics` 是该品种当年有 TAS 的 RIC 数）。
- `kind`：`none`（有 TAS、没有任何 L2 文件）、`partial`（`l2_days < 0.8 * tas_days`）、`not_washed`（2010–2023：远端有 L2、本地没洗）、`not_in_any_package`（2010–2023：没有任何包有该品种）。
- `reason`：`not_in_any_package`（所有检查过的 Report 里都没有这个 RIC）、`report_count_zero`（Report 里有但 Count 为 0）、`outside_package_window`（有数据但日期没盖住 TAS 的区间，容差 7 天）、`unmapped_exchange`、`overlap_conflict`、`in_package_not_delivered`（有数据且盖住却没交付，是缺陷，应为 0）。`evidence` 是 Report 行或一句话，`checked_packages` 是检查过的包列表（JSON）。
- `tas_days` = RIC-年目录下的日目录数（到 2026-11-12）；怀疑 `partial` 的 RIC-年才逐日检查有没有 `trade.parquet` / `quote.parquet`。`l2_days` 取自 `l2_directories`。2026-06 起的 L2 来自日包，没有 Report，所以 2026 的原因只认得到 2026-05-30 的包。
- 另有 `l2_package_inventory`（2010–2026 每个包一行，空包标注；2024 之前 `washed`/`delivered` 为 0）、`l2_year_summary`（每年主包与补充包 GB、RIC 数、L2 品种数、TAS 品种数、没有 L2 的 TAS 品种数；2012–2016 目录里没有 TAS，无从比较）和 `conventions`（方法、阈值、生成时间）。
- 重建：`scripts/lseg/l2_gaps_build.py --out <本地.sqlite> --reports <runtime/l2_supplement/reports> --packages-json … --inventory-json …`（先在任务空闲时跑 `lseg_catalog l2-sync`；NFS 只做 ≤8 线程的 listdir，约 1.5 分钟、内存 143 MB），再 `--publish <本地.sqlite>` 发布：新文件用硬链接不覆盖；刷新时加 `--replace`，是同目录里写临时文件后原子 rename，读者要么看到旧的完整文件、要么看到新的完整文件。
- 按品种的覆盖范围另有 `scripts/lseg/l2_coverage.py`（每个品种首末日和没有 L2 的月份）。

## 范围：L2 只做 2020 年起（2026-11-21）

用户决定：2020 年以前只需要 TAS 的 K 线（交付盘上已有的 `kline.parquet`），不洗也不交付 L2。2010–2019 年的 L2 包不处理；`l2_gaps.sqlite` 里这些年份的行记为 `out_of_scope`（`l2_gaps_build.py` 的 `L2_SCOPE_FROM = 2020`）。还要做的是 2020–2023 年。

## 2020–2023 年的 L2（2026-11-22 ~ 24）

- 主包（`p2020`–`p2023`，2.69 TB gz）洗进 `_stream/p2020..p2023`，交付 1,364,811 个文件，0 个问题。2021 年包里 VXM1 有一行订单数为 `-1`：洗库把负的数量和订单数读成空值并计数（`l2_slice::negative_counts`，共 21 次），不再让整年失败。
- 补充包（`comex_globex`、`futures_11`，8 个共 418 GB）洗进 `_stream/sg2020..sf2023`，只交付主包没有的 RIC（`scripts/lseg/l2_fill_wanted.py` 生成清单，主包优先）。
- Raw 补缺：仍没有 L2 但 Raw 有数据的 RIC（2020 年 99 个，2021–2023 年各约 330 个，只占 Raw 包消息的约 0.001%），用 `rawll2_segments.py` 只取需要的段，洗进 `_stream/raw2020..raw2023`，交付标 `raw` 来源。
- 补充包 + Raw 共交付 402,701 个文件，0 个问题。下载的补充包已删，RocksDB 保留。
- 2010–2019 年按用户决定不做 L2（缺口表里记为 `out_of_scope`）。
