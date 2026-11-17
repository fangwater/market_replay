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

## 盘中补缺：当天 bar 直接从 NFS MDL 重算（2026-11-17）

`cn-daily` 16:30 才产出当天 parquet。盘中 `GetMinuteBars` 现在可以补上"最后一个已归档交易日之后"的当天 bar，只读 NFS，不用 SQLite，也不接 ZMQ。ZMQ 仍然只管实盘；这条路径用来补 ZMQ 写盘失败或服务重启造成的缺口，以及给实盘因子起算时取历史（ZMQ 只需缓存一小段，等 MDL 追上后从 gRPC 取，再按时间接上）。

- **配置：** `config/cn_kline_server.toml` 的 `[live]`：`msg_root`；`products`（`["*"]` 或空表示全部六个交易所的全部品种，也可以只列几个）；`min_refresh_ms`（请求触发刷新的最小间隔，默认 2000）；`background_s`（后台刷新周期，默认 5，0 为关闭）；`lookback_days`（默认 16）；`segment_dir`（见下，不配置则整天都留在内存里，和以前一样）、`finalize_margin_s`、`tail_quiet_s`、`step_mb`、`manifest_every_s`。不配置 `[live]` 则行为和以前完全一样。收口在后台任务里做，`background_s = 0` 时不收口。
- **做法：** 用 `source::read_tail` 只读 NFS 文件里新增的完整行（残行留到下次），只保留配置品种的快照（内存里，重启后重读）。每个交易所各自加锁，另有后台任务每 `background_s` 秒追一次文件，所以请求基本都命中已追平的状态。每次请求对被查合约重跑盘后导出的同一条链：`process_instrument` → `synthesize_minutes` → `fill_session_minutes_with_twap` → 按 `session_as_of` 的时段快照裁剪。当天由所在交易所最新目录最后一行的 `TradDay` 决定，必须大于 `last_done`；`cn-daily` 处理完这一天后自动回到读 parquet。
- **分段落盘（内存不随全天增长）：** 一个合约的一天由若干连续段组成，段与段之间是至少 30 分钟的空档（夜盘、上午、下午，和盘后导出从数据里切段的规则相同）。一旦证明空档存在，空档之前的段就不会再变：用盘后导出的同一条链算好分钟行，写成 parquet，然后把这个段的快照从内存里丢掉，只带着快照到成交状态机的携带状态（`InstrumentCarry`）往后走。内存里只留当前还没收口的段。
  - **切点：** 排序后的相邻两行相差 ≥ 1800 秒时，切在前一行所在分钟的末尾（`C = floor_minute + 60 s`）。空档里没有可用的深度秒，所以 `C` 两边的段、分钟累加、按秒的盘口桶互相独立；`C` 之前那一片用 `flush_pending` 收尾，等价于单次处理时下一行到来时才吐出那条待发的盘口。切点还要求交易所的 NFS 时钟（已读到的最新快照）至少越过 `C + finalize_margin_s`（默认 60）。
  - **最后一段：** 之后没有新行时没法靠"下一行"证明空档，改用墙钟：文件已读到当前末尾（本步没有再读到字节），并且该合约最后一行距现在 ≥ `tail_quiet_s`（默认 7200，须大于 30 分钟空档加 NFS 的分批延迟）。读到一半时不会用这条规则（第一次装载时读到的最后一行相对现在当然"很旧"）。
  - **文件按顺序读：** 同一个交易日的行分在前后两个目录的文件里（前一天的夜盘在前一个文件末尾），必须读完前一个文件才读下一个，否则后一个文件较晚的行先到，前一个文件剩余的较早行后到，就被当成迟到行。
  - **迟到行保护：** 如果出现时间早于已落盘切点的行（`ts < 切点前最后一行 + 30 分钟`），说明已落盘的数据不可信：删掉该交易所当天的分段目录和状态，从文件重建，并打日志。
  - **落盘位置：** `segment_dir/<交易日>/<交易所>/<合约>/<段首分钟UTC秒>.parquet`（先写 `.tmp` 再改名）和 `manifest.json`（每个文件的恢复位置、每个合约的携带状态和切点，写入也是先临时文件再改名；有新段或每 `manifest_every_s` 秒重写一次）。`cn-daily` 处理完这一天（`last_done >= 交易日`）后，后台每轮把已发布日期的目录删掉。
  - **重启：** 读 manifest，从"所有快照都已被已落盘段覆盖"的最新检查点读到上次的读取终点（重放时时间早于合约切点的行直接丢弃），之后继续正常读。无 manifest 或 manifest 不可用时从文件头重建。
  - **服务：** 请求 = 已落盘的段 + 对当前未收口的行从携带状态续算。先在锁内克隆状态、再列目录读落盘段（中间新落的段会出现两次，内容相同，按时间去重，不会丢）。后台收口每次最多处理 24 个合约再放锁，请求最多等一个批次。
  - **分步装载：** 每步每个文件最多解析 `step_mb`（默认 256），装载一整天时的峰值不会是一整天的行数。
  - **核心改动：** `cn_futures_core::events` 新增 `InstrumentCarry` 和 `process_instrument_resumable`（`process_instrument` 现在是它的薄封装，行为不变）。进入实盘要在 mkt_signal 更新 rev。
- **响应：**
  - 来自 NFS 的 bar 带 `provisional = true`，不含 `size_buckets`（月度阈值要到月底才知道），其余字段和 parquet 同口径。
  - `DataState.live_trading_day` 是当前补缺的交易日，`live_through_ms` 是 NFS 已追到的最新快照时间（UTC 毫秒）。`open_time + 60s <= live_through_ms` 的 bar 才算走完。NFS 是整点附近分批更新，所以这个时间会滞后于 ZMQ，客户端按它决定从哪里衔接。
- **验证：** 用 `examples/live_check` 在已收盘的 20261116 和 20261113 上重建 SN，与生产 parquet 比对：共 11,128 根，成交、成交额、OHLC、twap、mid、买卖量逐值一致，缺失和多余都是 0（不做时段裁剪时会多出收盘快照那几分钟）。`examples/live_query` 查真实服务：首次请求读文件约 9 秒，之后约 46 毫秒。
- **全品种实测（2026-11-17 10:40）：** 除 SN 外，另用 `live_check` 对 20261116 的 IF（中金所）、M（大商所）、CF（郑商所）、SC（能源中心）、SI（广期所）重建，共 19,590 根，与 parquet 逐值一致，缺失和多余为 0。全品种服务启动后约 70 秒预热完成（读六个交易所当天文件），之后每次请求 10–70 毫秒。内存随当天快照数线性增长。内存里每行快照用紧凑结构（24 个 f64 加两个整数，约 216 字节，缺失值用 NaN，合约名只作为表的键）存放，请求时才还原成 `Snapshot`；最初版本直接存 `Snapshot`（两个 String 加 24 个 `Option<f64>`，约 540 字节），同一时刻实测 9.05 GB，改后 3.84 GB，同一批合约的 bar 摘要逐一相同。启动时 `wait_reference_api` 先等参考数据 API 的 `/api/v1/health` 就绪（它是 Python 子进程，比主进程晚几秒），避免第一次取元数据时报 `Connection refused` 再等 5 秒重试。
- **限制：** 盘中的最后几分钟随 NFS 追赶可能变化，盘后以 parquet 为准；没有 `size_buckets`。

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
