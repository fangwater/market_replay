# 国内期货每日 1 分钟处理与 gRPC kline 服务

最后更新：2026-11-15。

## 每日任务 `cn-daily`

代码在 `crates/cn/cn_daily`，配置在 `config/cn_daily.toml`。由 `market-replay-server` 在上海时间 16:30（UTC 08:30）排程运行，任务名 `cn-daily`。

每次运行会找出所有**已完成但还没处理**的交易日，按顺序处理：

| 步骤 | 工具 | 说明 |
| --- | --- | --- |
| 回放 | `cn_futures_l2` | 把通联 msg 写入生产 RocksDB；已经是 `done` 的交易日直接跳过 |
| 导出 | `export_baseline_1min` | RocksDB → staging（未裁剪），staging 在 NVMe 上 |
| 时段裁剪 | `trim_cn_futures_continuous_sessions` | staging → 生产 `baseline_data_1min`。时段快照固定为 `session_as_of = 2026-09-08`，必须和生产目录 `_provenance` 里的记录一致，否则工具会拒绝执行；生产目录里已有的文件不会覆盖，`--overwrite` 时才覆盖 |
| 大小单分桶 | `fill_1min_size_buckets --start 月初 --end D` | 阈值按完整自然月的 P50/P90 计算，所以每次重算当月 1 号到 D 的全部文件；每个月只跑一次 |
| 审计与通知 | 内置 | 统计文件数、行数、有成交分钟数、合约数、各所文件数，VWAP 落在快照高低价外的比例，与上一交易日相比缺少或新增的品种，以及没有已验证乘数的品种。结果写入 `logs/cn_daily/cn_daily_YYYYMMDD_audit.json`，然后发钉钉 |

规则：

- **交易日的认定：** 只有含中金所 `mdl_21_1_0.csv` 的 NFS 目录才算开市日。当天上海时间 `ready_after`（16:00）之后才认为 D 已完成：此时 15:15 国债已收盘，20:55 夜盘集合竞价还没开始，D 的 msg 文件不再追加。
- **状态文件：** `cn_daily_state.json` 记录 `last_done`。只有四步全部成功、staging 清理完毕后，才标记为完成。中途失败会停止整次运行并发钉钉，重跑时会自动跳过已经完成的部分。
- **需要人工处理的情况：** 回放中断后，RocksDB 里该交易日会留下 `writing` 状态。按 [cn_futures_l2](cn_futures_l2.md) 的 reset 流程处理后再重跑。
- **钉钉：** webhook 和加签 secret 只从环境变量 `CN_FUTURES_DB_DINGTALK_WEBHOOK_URL` / `CN_FUTURES_DB_DINGTALK_SECRET` 读取。启动脚本从仓库外的 `~/.config/market_replay/env`（权限必须是 600）加载它们。没有配置时只写日志，不发通知。用 `cn-daily test-notify --config config/cn_daily.toml` 测试连通性。

常用命令：

```bash
target/release/cn-daily status --config config/cn_daily.toml
target/release/cn-daily run --config config/cn_daily.toml --dry-run          # 只打印计划
target/release/cn-daily run --config config/cn_daily.toml --start 2026-08-27  # 首次运行或补跑
target/release/cn-daily audit --config config/cn_daily.toml --day 2026-11-13
# 服务与调度：market-replay-server（docs/market_replay_server.md）；手动运行：/admin 页面或 POST /api/admin/jobs/cn-daily/run
```

## gRPC kline：`cn_market.v1.CnFuturesMarket`

契约在 `crates/proto/cn_market_proto/proto/cn_market.proto`，服务端在 `crates/cn/cn_kline_server`，默认监听 `0.0.0.0:18312`（内网可访问），由 `market-replay-server` 提供。

这里**不做 Binance 兼容**，字段直接用期货的口径和单位：交易日、单边手数、含乘数的人民币成交额、合约乘数、最小变动价位、郑商所三位码和四位码。kline 数据也不单独维护，每次请求直接读生产 `baseline_data_1min` 的日文件。

- `GetMinuteBars(exchange, contract, trading_day | time{start_ms,end_ms})`：
  - `contract` 不区分大小写，郑商所的 `AP601` 和 `AP2601` 都可以。
  - 按时间范围查询时最长 31 天，会向后多看 10 个自然日，把夜盘所属的交易日也找进来。
  - 默认只返回 `volume > 0` 的分钟；设置 `include_empty_minutes=true` 时返回底表中已有的零成交分钟，用于连续分钟分段与小时聚合。不生成底表不存在的分钟；空分钟的未知价格保持 NaN。
- `Contract`：
  - `volume_multiple` 来自 PostgreSQL 的 `domestic_future_product_multipliers`，没有已验证的乘数时直接报错（`FAILED_PRECONDITION`）。
  - `price_tick` 来自 reference 服务里的 OpenCTP 期货公司参考数据：先按合约精确查找，查不到再用同品种的值（同品种的值不一致时报错）。`price_tick_source` 标明取自哪一种。
- `MinuteBar`：
  - `snapshot_high/low` 是 parquet 里的原值。
  - `high/low` 会在 tick 网格上扩展到包含 `vwap`：快照每秒 2 或 4 次，会漏掉快照之间的成交和大商所组合腿价成交，而真实成交区间一定包含 VWAP，并且落在 tick 网格上，所以扩展后的值不会超出真实范围。扩展过的 bar 带 `widened = true`。
  - `mid_price`（2026-11-16 新增，proto 字段 14，optional）：取自 parquet 的 `mid_price`，等于这一分钟内最后一个双边五档快照的 `(bid0 + ask0) / 2`，也就是 bar 收盘时的中间价，不是 `open_time` 时刻的因果值。这一分钟没有双边盘口时不设值。20261116 全部 133,165 根 bar 都有值，并且都落在半 tick 网格上。
  - baseline 其余字段（2026-11-16 新增，proto 字段 15–29）：`twap`，买卖方向拆分的手数、成交额、VWAP 和区间笔数，`avg_interval_turnover_cny`，净买入额、净买入量和净买入占比，`size_buckets`（大中小三档的成交额、买卖额和净额），以及 `book`（区间内最后一个双边五档快照）。全部原样取自 parquet，不另做计算。
    - **大中小单不是真实订单大小：** 把相邻快照的累计成交量、成交额做差，得到一条推算的区间成交；再按"品种 × 上海自然月"区间成交额的 P50、P90 分档（≥P90 为大，P50～P90 为中，<P50 为小）。阈值用的是整个自然月的数据，月底之前的日期并不满足因果性，实时侧要自己对齐阈值的时间口径。
    - 验证：用 `kline_check` 逐根对照 parquet，20261116 和 20261113 共 264,166 根 bar，所有字段完全一致，五档全部存在。
- 元数据（乘数和最小变动价位）每小时刷新一次；刷新失败时保留旧数据。

验证（2026-11-15）：用 `examples/kline_check.rs` 查询 20260826 全部 793 个合约，返回 135,515 根 bar，其中 1,666 根（1.23%）做了扩展。按 cn_futures_cta_manager 的 candle 规则逐根检查（OHLC 为正且有序、成交量与成交额一致、价格落在 tick 网格上、VWAP 在高低价之内），0 根不合格，RPC 错误也是 0。

```bash
target/release/examples/kline_check http://127.0.0.1:18312 \
  /mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min 20260826
```

客户端用 git + rev 依赖 `cn_market_proto`，方式和 mkt_signal 依赖 core 一样。

## 主力合约（2026-11-16 上线）

- **计算：** 全部用 Rust（`crates/cn/cn_futures_roll`）。`cn_futures_roll` 从通联行情提取日终截面，只读到上一个开市日的目录；`cn_roll_replay` 是 preprocess `cn_futures.roll_replay` 的 Rust 移植，默认规则 `local_rqdata_rule0`：持仓超过当前主力 1.1 倍、只切到更远的交割月、不切回。2019-01-01 到 2026-08-26 的 36 个历史输出文件与 Python 逐字节一致，两种规则都验证过。
- **数据：** `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/roll/`
  - `eod/<ex>_eod.csv`：2019-01-01 起的累积原始截面。
  - `result/`：已发布的主力、复权因子、连续价格、连续性数据。
  - `result.prev/`：上一次发布。
- **每日：** `cn-daily` 先对每个交易日、每个交易所提取截面（工序名"主力截面"），并入累积截面后，从 `roll_start` 全量重算到最新交易日（"主力重算"，约 5 秒）。新结果里所有已发布的主力行都必须保持不变，才会原子替换 `result/`，否则停止并发钉钉。钉钉消息里会列出"今日换月"和"明日换月"。
- **查询（HTTP JSON，不用 gRPC，内网可访问）：** 由 `cn-kline-server` 提供，监听 `0.0.0.0:18313`。
  - `GET http://172.16.30.38:18313/api/v1/dominant`：每个品种在最新交易日的 `dominant`（当天生效）、`scheduled_next`（收盘后排给下一交易日）、`rolls_next_day`、`last_roll_day`。
  - `GET http://172.16.30.38:18313/api/v1/dominant/{品种}?start=YYYY-MM-DD&end=YYYY-MM-DD`：逐日历史。
  - MCP（8780）工具 `cn_dominant_contracts`，HTTP 调用形式是 `/api/cn_dominant_contracts?product=RB`。
- **初始化（2026-11-16）：** 以 `cn_roll_replay/2019_20260826` 的原始截面加上 08-27 到 11-13 的缺口截面为输入，重算到 2026-11-13，08-26 及以前已发布的主力行逐行未变。

### 实时大小分类阈值导出

`fill_1min_size_buckets --product SN --start 2026-10-01 --end 2026-10-31 --export-thresholds /private/path/current.json`
只读打开 RocksDB，导出一个完整上海自然月的区间成交额 P50/P90 和样本数，
不覆盖 parquet。云端或本机实时信号可使用上一完整月的这份事实阈值；
盘后原有当月回填口径保持不变。
