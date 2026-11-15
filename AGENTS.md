# cn_futures_db

## 国内期货本地 NFS 源数据

来源：`/home/fanghaizhou/preprocess/AGENTS.md`「商品与股指数据」一节，以及 `preprocess/data_format/`。2026-11-14 实测核对过。

挂载根目录是 `/mnt/nfs`，全部只读，派生结果写到别的目录。

| 层 | 路径 | 覆盖 | 是否更新 |
| --- | --- | --- | --- |
| 历史日包 | `/mnt/nfs/30.3_tonglian_comm_l2/{ccfx,xdce,xgfe,xsge,xsie,xzce}/YYYYMMDD` | 截至 `20251103`，按交易所分目录 | **不更新**，已冻结 |
| 原始消息流 | `/mnt/nfs/30.3_tonglian_msg/YYYYMMDD/mdl_*.csv` | `20231207` 起 | **每日更新** |

切层规则：`TradDay <= 2025-11-03` 只用 `comm_l2`，之后只用 `msg`。同一个 `TradDay` 只能用一层，不能混用补缺。

### `tonglian_msg` 每日更新的情况（实测）

- 每个开市自然日一个目录，周日没有目录。周六的目录（比如 `20261107`、`20261114`）里只有周五夜盘跨过零点的部分：`mdl_22_*`（上期所 / 能源中心），在 UTC 18:30（北京时间 02:30）写完。
- 文件**在交易日内持续追加**：当天目录还在写，不能当作完整数据。最后修改时间：中金所 `mdl_21` 约 UTC 07:30（北京 15:30）；郑商所 `mdl_23` / 大商所 `mdl_24` 约 UTC 15:00（北京 23:00）；上期所 `mdl_22` 约 UTC 16:00（北京 00:00）。以后修改时间就不变了。
- 目录里有 `state.csv`，记录每个文件的字节数和最新时间戳，可以用来判断写到了哪里。
- 每个目录 8–9 GB 左右，单个文件最大约 3 GB，要用流式解析。
- **目录日期不是 `TradDay`**：夜盘行可能落在周六目录或前一个开市自然日的目录里。归属要用 `TradDay`，并结合 `ActionDay`、`UpdateTime` 和交易时段核对。

### msg 文件与交易所对应

| 文件 | 交易所 | 说明 |
| --- | --- | --- |
| `mdl_21_1_0` | 中金所 ccfx | 2 Hz，无夜盘 |
| `mdl_22_1_0` | 上期所 xsge | 2 Hz，有夜盘；含 `efp` 的合约丢掉 |
| `mdl_22_3_0` | 能源中心 xsie | 列与上期所相同 |
| `mdl_23_1_0` | 郑商所 xzce | 4 Hz；2025-11-04 起合约号是三位 `YMM`（`AP601` → `AP2601`）；`mdl_23_5_0` 是组合，不进主行情 |
| `mdl_24_1_0` | 大商所 xdce | 4 Hz；`mdl_24_3_0` 是 TQS、`mdl_24_5_0` 是组合，都不进主行情；`mdl_24_7_0` 是最优价委托队列 |
| `mdl_26_1_0` | 广期所 xgfe | 列与大商所相同；`_3` 是 TQS、`_5` 是组合、`_7` 是委托队列，用法同大商所 |

字段级定义以 `/home/fanghaizhou/preprocess/data_format/cn_source_msg_map.md` 和各交易所页面为准。

## mdl-replay 回放服务（`crates/mdl_replay`）

文档：[docs/mdl_replay.md](docs/mdl_replay.md)。按交易日或自然日、品种或合约、频道，把 `tonglian_msg` 的行情回放成和公司内网实时转发逐字节一致的 ZMQ 帧（`<topic> || <原行>`）。mkt_signal 的 `cn-futures-pbs --zmq` 只改 endpoint 就能直接消费。

- Release profile 和 mkt_signal 保持一致（`lto="thin"`、`codegen-units=4`、`strip`、`debug=0`）。用 `scripts/build-release.sh` 构建，`scripts/start_mdl_replay.sh` / `stop_mdl_replay.sh` 通过 PM2 启停（命名空间 `cn_futures_db`）。
- 控制面是 ZMQ REQ/ROUTER + JSON，默认 `tcp://127.0.0.1:13100`。每个回放会话一个 XPUB，默认无损（XPUB_NODROP）。
- 分片缓存在 `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/mdl_replay_cache/v1/<自然日>/<频道>/`，每个自然日约 8 GB。按自然日 LRU 自动淘汰，上限是 `cache_max_gb`（默认 48，约 6 个交易日）：每次构建后和服务端每 10 分钟各检查一次，10 分钟内用过的日期不淘汰；也可以用 `mdl-replay cache` 或 `{"cmd":"cache"}` 手动触发。NFS 只读。
- 帧必须原样透传。不得在回放链路里改写、补齐或过滤 CSV 内容；同一频道内保持源文件行序，不按 `LocalTime` 重排。
- 会话发完后要保持连接（`hold_open_s`），否则 pbs 收到断开事件后会丢弃还没消费的尾部帧。
- 改动后必须跑：`cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release`。改动解析或排序逻辑时，还要用真实日期做一次逐字节比对（方法见文档的"实测"一节）。

## 已定范围（2026-11-14，用户确认）

- 回放只支持 msg 层（2023-12-07 起）。**不做** `comm_l2` 历史日包支持。
- 回放服务的其余设计已经确认，照现状保留。

## reference/：国内期货参考数据服务（原 cn_exchange_info_summary）

2026-11-14 从 `/home/fanghaizhou/cn_exchange_info_summary` 整体并入 `reference/`。原路径现在是指向这里的软链接；旧目录备份在 `/home/fanghaizhou/cn_exchange_info_summary.pre-merge-20261114`，用户确认后再删。

- 这是一个 Python 项目，自带 `reference/AGENTS.md`，改这个目录时遵守其中的规则：只写 PostgreSQL 独立 schema `cn_futures_reference`，不碰 `public` 下的乘数表和交易时段表。
- 两个虚拟环境 `.venv` 和 `.venv-mcp` 按 `requirements.lock` / `requirements-mcp.lock` 重建（版本和搬迁前逐个一致）。它们不进版本控制。
- PM2 进程（命名空间 default）：`cn-futures-reference-api`（:8765）、`cn-futures-reference-daily-sync`（上海时间 18:15）、`cn-cme-exchange-mcp`（0.0.0.0:8780，同机其他用户在用）。分别从 `reference/ecosystem.config.cjs` 和 `ecosystem.mcp.config.cjs` 启动。
- 每日同步在 2026-10-12 到 2026-11-14 期间每天都失败，报错是 `Connection refused`（走的环境代理是 `127.0.0.1:10810`）。2026-11-14 手动同步成功，数据补到 2026-11-16。代理不可用时同步会失败，要看 `cn-futures-reference-daily-sync` 的错误日志。
- 按品种的连续交易时段在 `cn_futures_reference.continuous_session_versions`，kline 服务判断收盘边界要用它。

## kline 服务（进行中）

目标：给 `/home/fanghaizhou/cn_futures_cta_manager` 提供一个 Binance `/fapi/v1/klines` 兼容的 1 分钟 HTTP 接口（它的配置示例是 `127.0.0.1:18311`），从 tonglian_msg 计算。契约见该仓库的 `docs/native_reference_cost.md`：`volume` 是单边增量手数，`turnover` 是单边人民币增量（已含乘数），缺失分钟直接省略，不能前填。

已知的硬约束：CTA manager 要求每根 bar 的 `turnover/volume/乘数` 落在 `[low, high]` 内，否则整页拒收。原型测试中有 0.3–0.7% 的 bar 越界，全部出在收盘时刻的快照（02:30、10:15、11:30、15:00、23:00）或收盘后的成交上。修正方案和口径细节记在持久记忆里，实现后要在多个交易日上验证，并和 `baseline_data_1min`（数据到 20260826）对照。

## 仓库结构与 core 约束（2026-11-14 起）

本仓库是 `market_replay`（GitHub `fangwater/market_replay`），也就是盘后仓库：国内期货、CME、美股的盘后处理都迁到这里；实盘留在 mkt_signal。

| 路径 | 内容 |
| --- | --- |
| `crates/core/cn_futures_core` | 实盘和盘后共享的纯逻辑：交易时段、品种 universe、记录编码、快照到成交的状态机、断段、1 分钟累加 |
| `crates/cn/cn_futures_l2` | 从 mkt_signal `685896ce` 迁入：通联 → RocksDB 回放，以及 1s / 1min / ylabel / hfq 导出 |
| `crates/cn/cn_futures_trim` | 从 mkt_signal `eb58eaf4` 迁入：按 reference 服务的交易时段快照裁剪 parquet |
| `crates/cn/cn_daily` | 每日任务 `cn-daily`：回放、导出、裁剪、分桶、审计、钉钉通知，见 [docs/cn_daily_and_kline.md](docs/cn_daily_and_kline.md) |
| `crates/cn/cn_kline_server` + `crates/proto/cn_market_proto` | gRPC `cn_market.v1` 分钟线服务（读 parquet，不做 Binance 兼容） |
| `crates/common/dingtalk` | 钉钉机器人客户端（凭据只从环境变量读取） |
| `crates/mdl_replay` | 单日 MDL 回放服务 |
| `reference/` | 参考数据服务（Python） |

- `crates/core/*` **只能依赖纯计算 crate**（目前是 anyhow、chrono、chrono-tz），不得引入 RocksDB、parquet、arrow、iceoryx、网络或文件 IO 依赖，也不得依赖本仓库的其他 crate。mkt_signal 用 `git` + `rev` 固定版本依赖 core，core 里的任何改动都会进入实盘编译。
- 修改 core 后必须跑本仓库的全部测试，并用真实数据重跑一天、与生产 parquet 逐值比对（方法见下文）；要进入实盘，还需要在 mkt_signal 中更新 rev 并跑通那边的测试。
- 依赖版本以 mkt_signal 的 `Cargo.lock` 为基准，除本仓库独有的包以外保持一致。rocksdb 编译需要 `LIBCLANG_PATH`，`scripts/build-release.sh` 已经处理。

### baseline_data_1min 每日工序

生产的 1 分钟表依次经过四道工序。preprocess 的 `cn_pipeline.md` 漏写了第 3 步：

1. `cn_futures_l2 --msg-root ... --exchange X --start D --end D`：写入 RocksDB
2. `export_baseline_1min --start D --end D`
3. `trim_cn_futures_continuous_sessions --dataset baseline1min --session-as-of <快照日>`：按 `http://127.0.0.1:8765/api/v1/trading-sessions` 的时段裁剪。这个工具不允许输入和输出是同一个目录，所以先导出到 staging，再裁剪进生产目录。生产 `baseline_data_1min` 本身就是裁剪的输出目录，溯源记录在 `_provenance/continuous_sessions.json`，快照日是 2026-09-08；快照日不同时工具会拒绝执行
4. `fill_1min_size_buckets --start <月初> --end D`：按自然月的 P50/P90 做大小单分桶，只读打开 RocksDB。2026-11-15 加上了 `--start/--end`：只扫覆盖这些日期的年份分区，只改写范围内的文件。不带参数时仍然重写全部历史。已用生产数据验证，结果和不带参数时一致

迁移验证（2026-11-14）：用新二进制在临时目录按以上四步重跑 2026-08-26 的上期所和广期所，共 24 个文件、105,785 行，与生产 parquet 的 schema 和全部值完全一致；抽出 core 之后再验证广期所，5/5 个文件一致。

### 迁自 mkt_signal 的规则（cn_futures_l2）

以下规则原先在 mkt_signal `AGENTS.md`，2026-11-14 随代码迁到这里：

- `cn_futures_l2` 只从只读 PostgreSQL 表 `market_metadata.public.domestic_future_product_multipliers` 读取 `volume_multiple` 及其 `verified` 状态（Unix socket `/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run`，端口 `5433`）。不得恢复代码里的品种映射，不得调用 DataGateway 或交易所接口，不得从成交额反推乘数，也不得默认取 `1`。载入的目录里缺少某个品种时，必须带着能定位问题的上下文直接 panic；这是批处理工具的明确例外。不得写入这个元数据实例，也不得用 `5432` 端口的只读热备库。
- 以前的根二进制 `tonglian_baseline_replay` 及其 ClickHouse staging 配置已经退役，不得恢复。
- 用法文档见 [docs/cn_futures_l2.md](docs/cn_futures_l2.md)。
