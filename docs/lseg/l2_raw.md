# Raw LL2 → RocksDB（Normalized LL2 缺口的补充）

2026-11-19 用户要求：Normalized LL2 没有的品种（CL、NG、HO、RB、美债、谷物、外汇等，见 `l2_gaps.sqlite`）改用 **Raw LL2**（`rawLL2`）补，同样洗进 RocksDB。
Raw LL2 和 Normalized LL2 是不同的源，不能混称：本库里它们存在**不同的数据库**（`_stream/raw*`），交付时要在文件里标出来源（尚未交付）。

## 做法

Raw 是名次槽的增量赋值（一条消息 = 头行 + `Number of FIDs` 行 FID），Normalized 是整本书。`l2_raw::RawConverter` 把 Raw 逐消息还原成 Normalized 的 66 列行，
再交给原来的流式洗库（分段、增量记录、RocksDB）。口径和 `cme_tas_replay::raw_ll2` 的低频提取器一致：

- 槽（`BEST_BIDn/BSIZn/NO_BIDORDn/BEST_ASKn/ASIZn/NO_ASKORDn`，n=1..10，含第 10 档别名 `BEST_BSZ10` 等）只在消息给出时赋值，其余沿用。
- 从没给过值的槽是未知，**不用 Normalized 的值初始化**；未知和显式空值都写成空字段。
- `REFRESH` 丢弃此前全部知识（CSV 没有 ClearCache / RefreshComplete），只保留它自己给出的槽。
- 只在消息给出深度槽或是 REFRESH 时出一行（停牌等消息不出行）；`Exch Time` 列留空。
- 同一 RIC 时间不得倒退；RIC 必须连续（包按 RIC 排序）；消息被截断、FID 行数不符、未知消息类都直接报错。

用法（一次性回补，和 `backfill` 一样可续跑、受 server 任务门控）：

```
lseg_l2_slice backfill --no-daily --stream raw2024=<part0>,<part1>,… --raw-ll2-rics <RIC 列表> \
    --db-root <runtime/l2_db> --progress … --gaps …
```

`scripts/lseg/l2_raw_wanted.py` 生成 RIC 列表：Raw 有数据、TAS 有、Normalized L2 缺失（或天数少于 TAS）的 RIC。Normalized 完整覆盖的 RIC 不从 Raw 洗。

## 核对（2026-11-19）

- 转换器 vs 低频提取器（15 s 桶末值，所有 60 个槽 + known 掩码）：part 3 前 400 MB 比对 35,880 个槽、part 9 比对 508,020 个槽，逐值一致
  （`RAW_LL2_SAMPLE=<part> cargo test --release real_sample -- --ignored`）。
- 端到端：ESH24 2024-01-04 的 Raw 路径 250 ms 切片与交付盘上 Normalized 的切片相比，12,851 行时间戳完全相同，
  60 个盘口列和 `last_update_ts_ns` 逐值一致，唯一不同的是样本截断处的最后一行。

## 来源标记（交付后在 SQLite 里写清哪些版本来自 Raw）

2026-11-19 用户要求在 SQLite 里写清楚哪些版本来自 Raw。一个 RIC-年只有一个来源，Raw 只补 Normalized 没有的 RIC。

- 交付时 `lseg_l2_slice deliver --l2-source raw`：给每个交付的 RIC-年目录写标记文件 `<RIC 目录>/l2_source`（内容 `raw`）；交付前检查：目录里已有 Normalized 的 `l2_250ms.parquet` 就拒绝（不混来源），反过来 Normalized 交付进有标记的目录也拒绝。
- `lseg_catalog l2-sync` 按标记填 `catalog.sqlite` 的 `l2_directories.source`（`normalized` | `raw`，旧目录库自动加列，旧行默认 `normalized`；`build` 的 carry-over 也带着这一列）。`conventions` 里有 `l2_source` 说明。
- `l2_gaps.sqlite` 的表 `l2_raw_versions` 列出所有 Raw 来源的 RIC-年（`l2_gaps_build.py` 从目录库生成）。
- 查询：`SELECT ric, utc_year, days FROM l2_directories WHERE source='raw'`。
