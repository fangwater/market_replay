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
| 大小单分桶 | `fill_1min_size_buckets --start <本次第一天> --end <本次最后一天>` | **从 2026-11-01 起的新规则**（和实时 gRPC bar 同一套文件）：阈值 = 上一个完整上海自然月里**主力合约逐交易日合并**（换月前用旧主力、换月后用新主力）的区间成交额 P50/P90，按每笔成交所属月份取上月的阈值。只改写 `--start/--end` 范围内的日文件（必须显式给出，没有全历史模式）；某品种上月没有阈值（新上市、没有主力成交）时该品种当天文件保持原样并在输出里打印 `MISSING-THRESHOLDS`，绝不编造。2026-10-31 及以前的文件仍是旧的"同月、品种全部合约合并"阈值，**不重算** |
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
    - **大中小单不是真实订单大小：** 把相邻快照的累计成交量、成交额做差，得到一条推算的区间成交；再按品种的 P50、P90 分档（≥P90 为大，P50～P90 为中，<P50 为小）。**2026-11-01 起**阈值是上一个完整上海自然月里主力合约逐交易日合并的区间成交额分位数，按每笔成交所属月份取上月的阈值，所以事先已知、满足因果性，实时 bar 和 `baseline_data_1min` 用的是同一套文件（`<size_thresholds>/dominant_by_day/<YYYYMM>/<品种>.json`）。2026-10-31 及以前的文件仍是旧规则（同一自然月、品种所有合约合并，月底之前不满足因果性），没有重算。当前生效的数值用 `GetSizeThresholds` 取。
    - 验证：用 `kline_check` 逐根对照 parquet，20261116 和 20261113 共 264,166 根 bar，所有字段完全一致，五档全部存在。
- 元数据（乘数和最小变动价位）每小时刷新一次；刷新失败时保留旧数据。

验证（2026-11-15）：用 `examples/kline_check.rs` 查询 20260826 全部 793 个合约，返回 135,515 根 bar，其中 1,666 根（1.23%）做了扩展。按 cn_futures_cta_manager 的 candle 规则逐根检查（OHLC 为正且有序、成交量与成交额一致、价格落在 tick 网格上、VWAP 在高低价之内），0 根不合格，RPC 错误也是 0。

```bash
target/release/examples/kline_check http://127.0.0.1:18312 \
  /mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min 20260826
```

客户端用 git + rev 依赖 `cn_market_proto`，方式和 mkt_signal 依赖 core 一样。

## 盘中补缺：当天 bar 直接从 NFS MDL 重算（2026-11-17）

`cn-daily` 16:30 才产出当天 parquet。盘中 `GetMinuteBars` 返回最近归档日之后的当天 bar。服务从 NFS MDL 计算前缀，并用本机 ZMQ 分钟缓存补上经过核对的连续缺口，供实盘策略初始化或恢复。[盘中分钟缓存](cn_intraday_cache.md)

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
- **分段落盘实测（2026-11-17，临时实例，端口 18412/18413）：**
  - **验证：** `live_check` 把当天的快照按文件顺序送进分段状态机，每段写成 parquet 再读回，与生产 parquet 逐值比对。SN、IF、M、CF、SC、SI 在 20261113 和 20261116 各一次，另有 RB、AU、I、TA、T、LC、JD、SR 在 20261116，缺失、多余、不一致全为 0。事件级（交易、盘口、持仓量）按切点分片并带携带状态，与一次处理的输出逐合约相同。
  - **在线对比：** 临时实例（夜盘段已落盘、上午段在内存）与线上整天留内存的旧设计，对 8 个合约（六个交易所）在 11:00 之前的 bar 做摘要，完全一致。重启后（读 manifest、重放）摘要仍然相同，没有触发重建。
  - **内存：** 12:27 临时实例 2.4 GB，线上旧设计同一时刻 4.4 GB。段落盘后内存只与"当前最大的未收口段"有关（上午段约 2.5 小时），不再随全天增长。**整天的峰值没有实测，估计在 3 GB 上下，旧设计整天预计 8–12 GB。**
  - **预热：** 无 manifest 的首次装载约 90 秒（分步读，落盘了 625 个段文件）；重启读 manifest 后约 45 秒（只重放检查点之后的范围，此时上午段还没收口所以重放范围较大，收口后更短）。
- **响应：**
  - 来自 NFS 的 bar 带 `provisional = true`，其余字段和 parquet 同口径。`size_buckets` 现在有了：配置 `[live] size_threshold_dir` 后，实时成交按上一个完整月的主力逐日阈值分档（见下"实时大小分类阈值"）；该品种没有阈值、或某分钟有成交却没有被分档（例如落盘时文件还不存在）时，`size_buckets` 不设值，不用 0 冒充。
  - `DataState.live_trading_day` 是当前补缺的交易日，`live_through_ms` 是 NFS 已追到的最新快照时间（UTC 毫秒）。`open_time + 60s <= live_through_ms` 的 bar 才算走完。NFS 在整点附近分批更新。`GetMinuteBars` 接上缓存时将响应内的水位推进到最终返回的连续分钟的结束时间。`GetDataState` 继续报告 NFS 的交易所水位。
- **验证：** 用 `examples/live_check` 在已收盘的 20261116 和 20261113 上重建 SN，与生产 parquet 比对：共 11,128 根，成交、成交额、OHLC、twap、mid、买卖量逐值一致，缺失和多余都是 0（不做时段裁剪时会多出收盘快照那几分钟）。`examples/live_query` 查真实服务：首次请求读文件约 9 秒，之后约 46 毫秒。
- **全品种实测（2026-11-17 10:40）：** 除 SN 外，另用 `live_check` 对 20261116 的 IF（中金所）、M（大商所）、CF（郑商所）、SC（能源中心）、SI（广期所）重建，共 19,590 根，与 parquet 逐值一致，缺失和多余为 0。全品种服务启动后约 70 秒预热完成（读六个交易所当天文件），之后每次请求 10–70 毫秒。内存随当天快照数线性增长。内存里每行快照用紧凑结构（24 个 f64 加两个整数，约 216 字节，缺失值用 NaN，合约名只作为表的键）存放，请求时才还原成 `Snapshot`；最初版本直接存 `Snapshot`（两个 String 加 24 个 `Option<f64>`，约 540 字节），同一时刻实测 9.05 GB，改后 3.84 GB，同一批合约的 bar 摘要逐一相同。启动时 `wait_reference_api` 先等参考数据 API 的 `/api/v1/health` 就绪（它是 Python 子进程，比主进程晚几秒），避免第一次取元数据时报 `Connection refused` 再等 5 秒重试。
- **落盘延迟（2026-11-17）：** 最后一段（午休、收盘、夜盘收盘前的那一段）不再只靠 2 小时静默，而是用交易时段表证明：合约最后一行落在某个"后面至少隔 30 分钟才开盘"的连续时段末尾（`SessionMatcher::break_end`，10:15 的 15 分钟茶歇不算），文件已读到末尾，并且墙钟已过最后一行 `delay_s`（默认 600 秒），就落盘，切点取时段结束后 `tail_grace_s`（默认 300 秒），这样收盘后几秒写入的收盘快照仍归本段；之后再到的更早的行会被标为迟到并重建该交易所。2 小时静默规则保留作兜底。
- **限制：** 盘中的最后几分钟随 NFS 追赶可能变化，盘后以 parquet 为准。
- **清单版本（2026-11-17）：** `manifest.json` 版本从 1 升到 2：升级后第一次启动会丢弃当天已落盘的旧段（它们没有分桶列）并从文件重建，约 90 秒。

### 独立的查询接口

分桶阈值和交易时段不再塞进 `GetMinuteBars`，各有一个接口（契约在 `cn_market.proto`）：

- `GetSizeThresholds(exchange, product)`：`product` 为空 = 该交易所全部品种。每个品种返回当前上海自然月的 `applies_month` 和 P50/P90（成交额，人民币，含乘数），下个月的文件已存在时再多一条；没有阈值的品种返回空列表。未知交易所或品种返回 `NOT_FOUND`。数值来自和 bar 计算完全相同的阈值文件（懒加载、缓存，缺失的文件每 5 分钟重新找一次）。
- `GetTradingSessions(exchange, product)`：`product` 为空 = 全部。返回 `snapshot_as_of` 和每个品种按段序的连续竞价窗口 `{begin_sec, end_sec}`（上海零点起的秒数，结束不含，`end_sec <= begin_sec` 表示跨过零点）。来自 `session_as_of` 对应的时段快照，需要配置 `[live]`（否则 `UNAVAILABLE`）。
- `GetDataState(exchange)`：带交易所时，`live_trading_day` / `live_through_ms` 描述这个交易所；不带则为 0。这两个值由后台线程发布到原子变量，轮询很便宜。`open_time + 60 秒 <= live_through_ms` 的分钟是走完的。

### 实时大小分类阈值（2026-11-17，和 baseline_data_1min 同一定义）

- **文件：** `<size_threshold_dir>/dominant_by_day/<YYYYMM>/<品种>.json`，`{"basis":"dominant_by_day","product","source_month","p50","p90","samples","days",...}`，原子写入。默认目录 `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/size_thresholds`。
- **口径：** 对"来源月"里每个交易日取当天主力合约（`roll/result/<ex>_dominant.csv` 的 `instrument_id`，郑商所三位/四位码按 `canonical` 对齐）的全部区间成交额，逐日合并，取线性 P50/P90（和 numpy 默认一致）。周五夜盘归周一的交易日（`trad_day_from_ts`），所以换月日周五夜盘算新主力。一笔成交用"它自己上海时间所在月的上一个月"的阈值，所以跨月的夜盘也是对的。
- **生成：** `scripts/export_size_thresholds.sh [YYYYMM]`（默认上一个自然月）：对 `baseline_data_1min` 里的每个品种调用 `fill_1min_size_buckets --product P --export-month YYYYMM`，最多 4 个并发，只读 RocksDB，已有有效文件就跳过（`FORCE=1` 重算）。算不出来的品种（新上市、上月没有主力成交）列在 `_failed.txt`，两处使用方都不设分桶、不编造。每月 1 日上海 06:00 由任务 `size-thresholds-monthly` 生成上个月。
- **单次 `fill_1min_size_buckets` 缺文件时：** 自己用 RocksDB 加主力表算出来并写入同一个文件，所以工序 4 不依赖月任务一定先跑过。
- **月初注意：** 上个月最后一个周五夜盘（上期所/能源中心，过零点后）要到下周一回放后才进 RocksDB；月初 1 日的导出可能缺这几笔。需要精确时用 `FORCE=1` 在回放之后重导一次。

## 主力合约（2026-11-16 上线）

新增 gRPC `GetContractSelections`，提供主力、OI 次主力、远月/近月 OI 次主力及成交量第二名；均明确区分当日生效与收盘后安排。口径、请求及回测配置见 [合约选择 API](cn_contract_selection.md)。现有主力生成规则及 HTTP 返回保持不变。

- **计算：** 全部用 Rust（`crates/cn/cn_futures_roll`）。`cn_futures_roll` 从通联行情提取日终截面，只读到上一个开市日的目录；`cn_roll_replay` 是 preprocess `cn_futures.roll_replay` 的 Rust 移植，默认规则 `local_rqdata_rule0`：持仓超过当前主力 1.1 倍、只切到更远的交割月、不切回。2019-01-01 到 2026-08-26 的 36 个历史输出文件与 Python 逐字节一致，两种规则都验证过。
- **数据：** `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/roll/`
  - `eod/<ex>_eod.csv`：2019-01-01 起的累积原始截面。
  - `result/`：已发布的主力、复权因子、连续价格、连续性数据。
  - `result.prev/`：上一次发布。
- **每日：** `cn-daily` 先对每个交易日、每个交易所提取截面（工序名"主力截面"），并入累积截面后，从 `roll_start` 全量重算到最新交易日（"主力重算"，约 5 秒）。新结果里所有已发布的主力行都必须保持不变，才会原子替换 `result/`，否则停止并发钉钉。钉钉消息里会列出"今日换月"和"明日换月"。
- **查询（HTTP JSON，不用 gRPC，内网可访问）：** 由 `cn-kline-server` 提供，监听 `0.0.0.0:18313`。
  - `GET http://172.16.30.38:18313/api/v1/dominant`：每个品种在最新交易日的 `dominant`（当天生效）、`scheduled_next`（收盘后排给下一交易日）、`rolls_next_day`、`last_roll_day`。
  - `GET http://172.16.30.38:18313/api/v1/dominant/{品种}?start=YYYY-MM-DD&end=YYYY-MM-DD`：逐日历史。
  - **按交易日取名单（2026-11-18）：** `GET /api/v1/dominant?trading_day=YYYYMMDD`，把柜台返回的交易日（比如 CTP 登录返回的 `20261118`）传进来，返回的名单带明确的生效日：
    - 等于 `latest_trading_day`：`source=dominant`，该日生效的主力；
    - 早于它：`source=history`，那天的历史行；
    - 晚于它（夜盘属于下一个交易日）：`source=scheduled_next`，即最近一个交易日收盘后排给下一交易日的名单，每个品种带 `previous` 和 `rolls`，并且 `calendar_verified=false`。**这里没有交易所日历（节假日），所以只检查 `trading_day` 不超过最新交易日之后 14 个自然日；它是不是“下一个交易日”以柜台为准。**超过 14 天返回 400。
    - 不带参数时，返回里新增 `dominant_trading_day`（= `latest_trading_day`），并在 `note` 里说明 `scheduled_next` 是“之后第一个交易日”的名单。已有字段不变。
    - 名单更新时间：`cn-daily` 每个交易日收盘后（上海 16:30 起，约 16:33 发布），所以下午盘后就已经有了排给下一交易日的名单。
  - MCP（8780）工具 `cn_dominant_contracts`，HTTP 调用形式是 `/api/cn_dominant_contracts?product=RB`。
- **初始化（2026-11-16）：** 以 `cn_roll_replay/2019_20260826` 的原始截面加上 08-27 到 11-13 的缺口截面为输入，重算到 2026-11-13，08-26 及以前已发布的主力行逐行未变。

### 大小分类阈值的导出（取代旧的 `--export-thresholds`）

旧的 `--export-thresholds <路径>`（同月、全部合约）已经删除。要单独导出某个品种、某个来源月的阈值：

`fill_1min_size_buckets --product SN --export-month 202610`

写入 `<size_thresholds>/dominant_by_day/202610/SN.json`（只读 RocksDB，不碰 parquet；该月必须已经结束）。`mkt_signal` 的实盘读的 JSON（`product`、`source_month`、`p50`、`p90`、`samples`）是这个文件的子集，字段名不变。
