# LSEG TAS 持仓量（Open Interest）入库

2026-11-25 用户要求：CME 换月要用持仓量。以前认为 CME 没有 OI，实际上 LSEG TAS（`normalised`）源文件的 `Open Interest` 列有值，只是 TAS RocksDB 和交付盘的 parquet 都没收这一列。本工具把它从原始 gzip 里提取出来，写进 PostgreSQL。

## 源里的样子（已核对）

- 在每个有成交的合约上，每天有几行 `Type=Trade` 的行带 OI。这些行没有价格和成交量，`Date` 列是这个 OI 所属的**交易日**。
- CME 每个交易日的 OI 发两次：交易日当晚约 20:15 CT 发**预估值**，次日约 09:05 CT 发**终值**。每次都是两行相同的记录，间隔约 0.2 秒。例：TYZ26 交易日 2026-11-18，预估值 6,002,177（UTC 2026-11-19 02:13），终值 5,954,926（UTC 15:05）。
- **因果含义：** 在交易日 T+1 开盘（17:00 CT）做决策时，只能拿到 T−1 的终值和 T 之前的数据；T 的预估值要到开盘之后才发出来。所以用 OI 时必须按发布时间取"当时已知"的值，不能按 `Date` 直接对齐。
- 早年的 `Date` 列是空的（2010 年样本），只有发布时间。工具把它存成 `oi_date = NULL`，**不按发布时间推算归属日**。
- 不同年份的历史包列布局不同：2021 年以前没有 `Alias Underlying RIC` 列，`Open Interest` 是第 62 列而不是第 63 列。工具一律按列名定位，不写死列号。
- 历史包只有 part 0 带表头，其余分片沿用 part 0 的表头。所有文件都是多个独立 gzip 成员拼接而成，每个成员压缩后约 10–20 MB、解压后 330–460 MB，压缩比约 40 倍。

## 存储：`market_metadata.lseg_open_interest`

DDL 在 `crates/cme/lseg_delivery/sql/lseg_open_interest.sql`，用 `tas_open_interest init-schema` 应用（幂等）。只写这个 schema，不碰其他表。

| 表 / 视图 | 内容 |
| --- | --- |
| `sources` | 每个源 gzip 一行：路径、类型（`period` / `daily`）、包名或品种、大小与 mtime（源身份）、gzip 成员数、行数统计、发布时间范围、规则版本 `lseg_oi_v1`。和它的数据行在同一个事务里写入 |
| `observations` | 同一 RIC 连续出现的、(`oi_date`, 值, `Type`) 相同的源行合并成一条，保留第一次和最后一次发布时间、行数。值变了再变回来，算新的一条 |
| `rejects` | 有值但无法原样使用的行（不是非负整数、时间或日期解析不了），保留原文，不进 `observations` |
| `by_trade_date`（视图） | 按 (RIC, 交易日)：第一次发布的值（预估值）和最后一次发布的值（终值），以及各自的发布时间 |

时刻 t 已知的 OI：取 `first_published_ns <= t` 中 `first_published_ns` 最大的那条 observation。

## 工具 `tas_open_interest`（`crates/cme/lseg_delivery`）

```bash
export LSEG_OI_DATABASE_URL='postgresql:///market_metadata?host=%2Fmnt%2Fnvme-raid0-28t%2Ffanghaizhou%2Fpostgresql%2Fdomestic_futures%2F16%2Frun&port=5433&user=u171'
B=target/release/tas_open_interest
$B init-schema
# 只扫一个文件，输出 CSV，不写库（核对、测速用）
$B scan --input <gz> [--header-from <part 0>] --output out.csv [--max-compressed-bytes N]
# 把还没入库的源全部入库；--dry-run 只列清单
$B sync --daily-root /mnt/hdd-raid5-72t/liang_torch/lseg_data/future_daily/normalised \
        --period-root /mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalised \
        --period-root /mnt/hdd-raid5-72t/liang_torch/lseg_data/future_supplement/normalised \
        --work-dir <本机临时目录> --workers 2 [--only TY,ES] [--max-sources N] [--dry-run]
```

- **增量规则：**
  - 已入库、且大小和 mtime 都没变的源，直接跳过。
  - 源变了就报错，除非加 `--replace`。`--replace` 会在同一事务里删掉旧行再重写。
  - 每个源在一个事务内完成：写 `sources` 行，用 COPY 写入数据行，再核对 COPY 行数等于扫描行数，然后提交。
  - 扫描前后各核对一次源身份（大小和 mtime）。
- **日包：** 只读下载器写过 `.download.json` 的文件，状态必须是 `download_complete_header_verified`，且字节数一致。源端本来就是 0 字节占位的文件（`source_zero_byte_placeholder_downloaded`，共 1,225 个）单独计数并跳过；其余没就绪的文件留给下次运行。
- **资源：**
  - `--workers` 上限是 `limits::AGGREGATION_WORKERS`（2），每个 worker 一次处理一个源文件。
  - 内存每个 worker 约 0.7 GiB：一个压缩窗口 128 MiB 起，只有单个成员放不下时才扩大，上限 1 GiB；一个解压缓冲 512 MiB 起，上限 4 GiB。
  - 暂存 CSV 只有几 KB 到几 MB，入库后立即删除。
- **速度：**
  - 用 libdeflate 整成员解压（`gz_members.rs`），比 zlib-ng 流式解压快约 4–5 倍。
  - 逐行先快速判断 OI 字段是否为空：无引号的行按 32 字节一块数逗号，有引号的行在引号外数逗号。只有 OI 有值的行才走完整的 CSV 解析。
  - 实测：ES 一天 9.17 GB 文本用 5.8 秒（原来 30 秒）；历史包每个 worker 每秒约 50 MB 压缩数据。

## 回补（2026-11-25）

用户批准 8 线程（`--backfill`，`limits::OI_BACKFILL_WORKERS`），范围是 2020 年起：

```bash
tas_open_interest sync --daily-root .../future_daily/normalised --period-root .../future/normalised \
  --period-root .../future_supplement/normalised --period-from 2020-01-01 --work-dir <本机> --workers 8 --backfill
```

结果：

- 共 19,798 个源文件、约 7.5 TB：64 个历史分片，加 19,734 个日包。耗时约 6 小时，平均约 343 MB/s，瓶颈在 HDD。
- 失败 0 个；跳过 0 字节占位文件 1,225 个。
- 入库 2,668 万条 observation，0 条 reject。其中 150 万条 `oi_date` 为空，同样不推算归属日。
- `oi_date` 从 2019-12-30 到 2026-11-24，发布时间到 2026-11-23（UTC）。
- 约 15 万条的 `oi_date` 晚于发布时刻的 UTC 日期，这是交易日按当地时间算的市场（亚洲时区）的正常现象。所以判断"当时已知"只看发布时间，不能用 `oi_date` 和 UTC 日期比大小。
- 2020 年以前的历史包没有入库，需要时用 `--period-from` 再跑一次。

## 每日任务

`lseg-daily` 配置里有 `[open_interest]`（`database_url`、`workers`，默认 1）时，在 K 线之后跑 `sync --daily-root <raw_root>`，失败记 `meta/problems/lseg_daily/open_interest.json`。见 [lseg_daily.md](lseg_daily.md)。

## 验证（2026-11-25）

- TY 2026-11-19 的日包（1,054 万行）：awk 统计有 12 行带 OI，工具输出 5 条 observation；入库后再查回，结果与 awk 逐条一致。ES 同日共 22 行、13 条。改用 libdeflate 前后，输出逐字节相同。
- 历史包 2010 和 2021 的 part 1（各截取 1 GB 压缩数据，表头取自 part 0）：
  - 2010：`Date` 为空，1,477 行合并成 302 条；
  - 2021：带 `Date`，每天预估值、终值各一次；
  - 两段都没有 reject。
