# market_replay（原目录名 cn_futures_db）

2026-11-17 起本地目录从 `/home/fanghaizhou/cn_futures_db` 改名为 `/home/fanghaizhou/market_replay`，和 GitHub 仓库 `fangwater/market_replay` 同名。本仓库、mkt_signal、preprocess、cta-cn_futures-01 里对旧路径的引用都已改成新路径，PM2 命名空间也从 `cn_futures_db` 改成了 `market_replay`。过渡用的软链接已于当天删除，旧路径不再存在。

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

## 国内商品期权 1 分钟源数据（2026-11-26 实测）

后续规划在本仓库支持国内商品期权（`XDCE`、`XGFE`、`XSGE`、`XSIE`、`XZCE` 五家商品交易所）的数据清洗。两层源数据都来自公司内网通联 HTTP 归档（未挂 NFS，访问时绕开本地代理，`--noproxy '*'`）：

| 层 | 获取路径 | DataMart UID | 覆盖 | 是否更新 |
| --- | --- | --- | --- | --- |
| 历史日包 | `http://10.61.20.12/public/data/massive/future-data/tonglian-opt-etf-2009-2025/future_opt_pricemin/future_opt_priceminYYYYMM/future_opt_priceminYYYYMMDD.zip` | `option-tonglian-min-2009-2025` | `2017-03-31` 至 `2026-08-03`（`202608` 仅含 `20260803` 一天） | **不更新**，已冻结 |
| 每日 MDL 落盘 | `http://10.61.20.11/users/el01/public/tonglian/data/YYYYMMDD/mdl_13_<id>_0.csv` | `tonglian-idc-pj-deploy` | `2026-07-01` 起（实测已到 `2026-11-26`） | **每日更新** |

- **重叠窗口与切层建议：** 两层在 `2026-07-01`–`2026-08-03` 重叠，实测 `2026-08-03` `AP610C8000` `09:00` 的 OHLC、成交量、成交额、持仓量完全一致。具体切层日可在清洗设计时按全量重叠比对后确定（例如 `2026-07-31` 或 `2026-08-03`）。
- **历史日包格式：** 每个 `.zip` 内含单个 UTF-8 TSV `future_opt_priceminYYYYMMDD.txt`，按 `CLEARINGDAY`（交易日）打包，列为 `CLEARINGDAY, TDATE, CONTRACTID, MARKET, Offset, bartime, closeprice, openprice, highprice, lowprice, volume, value, vwap, OPENINTS`（注意 `closeprice` 在 `openprice` 前面）。
- **每日 MDL 文件与交易所对应（仅商品期权 1 分钟）：**
  - `mdl_13_40_0.csv`（郑商所 `XZCE`）、`mdl_13_42_0.csv`（大商所 `XDCE`）、`mdl_13_61_0.csv`（上期所 `XSGE`）：纯 1 分钟线，13 列（无 `Unit` 列）；对应的 `_41`、`_43`、`_62` 是 3/5/15 分钟等多周期文件，1 分钟清洗不读。
  - `mdl_13_94_0.csv`（能源中心 `XSIE`）、`mdl_13_100_0.csv`（广期所 `XGFE`）：14 列（含 `Unit` 列），1 分钟与多周期混在同一文件，**必须过滤 `Unit == 1`**。
  - `mdl_13_8_0`（上交所 ETF）、`mdl_13_79_0`（深交所 ETF）、`mdl_13_78_0`（中金所股指）不属于商品期权。
  - **目录日期不是 `TradDay`：** 周六目录（如 `20261121`）包含周五夜盘过零点的 `mdl_13_61_0.csv` 和 `mdl_13_94_0.csv`，归属必须按行内 `TradDay` 与 `ActionDay`/`BarTime` 处理，与期货 `tonglian_msg` 同理。
- **MySQL 实时表（备查）：** DataMart `options-1minute-data` 对应 `172.16.30.20:3307` `quotes.options_minute`（标注“实时更新”，未提供起始日期与保留策略，且页面对 OpenPrice/ClosePrice 的文字描述反了；清洗历史与盘后增量以上面两份 HTTP 归档为准）。

## mdl-replay 回放服务（`crates/mdl_replay`）

文档：[docs/mdl_replay.md](docs/mdl_replay.md)。按交易日或自然日、品种或合约、频道，把 `tonglian_msg` 的行情回放成和公司内网实时转发逐字节一致的 ZMQ 帧（`<topic> || <原行>`）。mkt_signal 的 `cn-futures-pbs --zmq` 只改 endpoint 就能直接消费。

- Release profile 和 mkt_signal 保持一致（`lto="thin"`、`codegen-units=4`、`strip`、`debug=0`）。用 `scripts/build-release.sh` 构建。运行时作为 `market-replay-server` 的服务 `mdl-replay`（默认关闭；在 `config/market_replay_server.toml` 里设 `enabled = true` 后重启服务器）。
- 控制面是 ZMQ REQ/ROUTER + JSON，默认 `tcp://127.0.0.1:13100`。每个回放会话一个 XPUB，默认无损（XPUB_NODROP）。
- 分片缓存在 `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/mdl_replay_cache/v1/<自然日>/<频道>/`，每个自然日约 8 GB。按自然日 LRU 自动淘汰，上限是 `cache_max_gb`（默认 48，约 6 个交易日）：每次构建后和服务端每 10 分钟各检查一次，10 分钟内用过的日期不淘汰；也可以用 `mdl-replay cache` 或 `{"cmd":"cache"}` 手动触发。NFS 只读。
- 帧必须原样透传。不得在回放链路里改写、补齐或过滤 CSV 内容；同一频道内保持源文件行序，不按 `LocalTime` 重排。
- 会话发完后要保持连接（`hold_open_s`），否则 pbs 收到断开事件后会丢弃还没消费的尾部帧。
- 改动后必须跑：`cargo fmt --check && cargo clippy --release --all-targets -- -D warnings && cargo test --release`。改动解析或排序逻辑时，还要用真实日期做一次逐字节比对（方法见文档的"实测"一节）。

## 统一服务进程 market-replay-server（2026-11-16 起）

用户要求"集中到一个 server 来管理，不要 PM2 启动很多东西"。PM2 里属于本项目的只有 `market-replay-server` 和数据库 `pg-domestic-futures`。说明见 [docs/market_replay_server.md](docs/market_replay_server.md)。

- **对外接口：** gRPC `cn_market.v1` 在 :18312；HTTP 在 :18313，提供主力合约 API、管理 API `/api/admin/*`，以及数据看板“数据处理进度与品种查询”（`crates/web/data_dashboard`，页面在 `/`，接口 `/api/dashboard/*`，配置 `config/data_dashboard.toml`；`/admin` 跳到看板的“服务与任务”页）。看板严格只读：catalog.sqlite 以 read_only+immutable 打开，PostgreSQL 只走 `default_transaction_read_only` 的 psql，NFS 只做少量 readdir，后台一个任务按固定间隔刷新。页面顶部的“任务进度”表只回答“跑完了没有”（任务、状态、数据到、最近一次结束、下次运行；状态取自 `/api/admin/status`，“数据到”取自各数据集已读取的进度，`size-thresholds-monthly` 另读阈值目录的月份目录列表），“换月”页只读 `/api/v1/dominant`（外加对当天换月品种的一次历史请求，用来取旧合约）；两者都没有新增访问类型。
- **常驻服务：** 参考数据 API（:8765）、参考数据每日同步、CME 参考数据（:8770）、MCP（:8780），以及默认关闭的 `mdl-replay`，都是这个进程的子进程，退出后自动按退避重启。
- **国内盘中分钟缓存：** `cn-minute-cache` 由 Market Replay 托管为独立子进程，消费六家交易所的公司 ZMQ 单腿期货行情，把闭合的 baseline_1min 按 Protobuf 写到 NVMe RocksDB。主进程用只读 secondary 从 `GetMinuteBars` 补齐连续缺口。保留一个原生交易日，盘后归档成功后自动清理。见 [盘中分钟缓存](docs/cn_intraday_cache.md)。
- **每日任务：** `cn-daily`（上海 16:30）和 `ib-kline-daily`（上海 09:30）由它按时排程，走同一个队列依次执行，各自发钉钉。以后新的任务（例如 lseg-daily）只在 `config/market_replay_server.toml` 里加 `[[job]]`，**不得再单独注册 PM2 进程**。
- **生产二进制：** 只从 `/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy/current/bin` 运行，由 `scripts/deploy-release.sh <commit>` 从干净的 checkout 构建。不得让生产配置指向 `target/release`，因为 worktree 和开发构建共用这个目录。
- 切换前的 PM2 进程列表保存在 `/mnt/nvme-raid0-28t/fanghaizhou/pm2_jlist_before_unify_20261116.json`（权限 600，内含环境变量）。各项目自己的 `ecosystem*.cjs` 和 `pm2-start.sh` 已删除，以免重复启动。

## 国内 HFQ 粗频底表（2026-11-23 起）

`baseline_data_{5min,15min,1h,1day}_hfq` 由 Rust 二进制 `cn-hfq-bars`（`crates/cn/cn_hfq_bars`）每日增量维护，服务器任务 `cn-hfq-bars-daily`（上海 18:00，排在 `cn-hfq-daily` 之后）；因子、`ylabel_*_hfq`、`backtest_1s` 按用户决定不再更新，停在 2026-08-26。切 bar 规则不变（连续段首起每 N 分钟，休市或缺分钟收成短 bar，周期不固定），换月导致旧 1min 文件变化时该品种整体重建。说明见 [docs/cn_hfq_bars.md](docs/cn_hfq_bars.md)。实盘 `cta-cn-futures`（mkt_signal）的 1h 切法与此相同，在每个连续段的 10:15、11:30、14:30、15:00 等 bar 结束点更新仓位。

## 已定范围（2026-11-14，用户确认）

- 回放只支持 msg 层（2023-12-07 起）。**不做** `comm_l2` 历史日包支持。
- 回放服务的其余设计已经确认，照现状保留。

## reference/：国内期货参考数据服务（原 cn_exchange_info_summary）

2026-11-14 从 `/home/fanghaizhou/cn_exchange_info_summary` 整体并入 `reference/`。2026-11-15 经用户确认，旧目录备份和原路径的软链接都已删除，引用旧路径的地方（快照 JSON 的 `raw_path`、cme_exchange_info_summary 的 README）都改成了新路径。

- **分工（2026-11-17 起）：** 核心数据面已迁到 Rust（`crates/ref/cn_reference`，库挂在 `market-replay-server` 进程内，直接监听 :8765）：`/api/v1/{health,schema,data-state,trading-sessions,overview,exchanges,instruments,exports/*}` 的查询与导出、`POST /api/v1/sync`、`POST /api/v1/trading-sessions/sync`、OpenCTP 抓取与解析、增量版本存储。其余路径（网页 `/`、公告、报单费规则、费用估算、`/import`、`/overrides/import`）原样转发给仍是 Python 的服务 `cn-reference-ui`（`reference/`，只监听 127.0.0.1:18766）。这几块是下一步要迁的，迁完再删 Python。`reference/AGENTS.md` 的规则仍然有效：只写 PostgreSQL 独立 schema `cn_futures_reference`，不碰 `public` 下的乘数表和交易时段表。
- 两个虚拟环境 `.venv` 和 `.venv-mcp` 按 `requirements.lock` / `requirements-mcp.lock` 重建（版本和搬迁前逐个一致）。它们不进版本控制。
- 进程由 `market-replay-server` 托管（不再单独注册 PM2）：:8765 由服务器进程自己提供（`[reference]` 段）；服务 `cn-reference-ui`（:18766，Python）、`exchange-mcp`（0.0.0.0:8780，同机其他用户在用）；每日同步是任务 `cn-reference-daily`（上海 18:15，`cn-reference sync`，走任务队列，发钉钉），不再有 `cn-reference-daily-sync` 服务。手动同步用 `cn-reference sync`，Python 的同步脚本已删除。
- 数据库连接串只读 `CN_FUTURES_REF_DATABASE_URL`（环境变量，或 `[reference].env_file` 指向的 `reference/.env`），不打印、不入库。
- 迁移验证（2026-11-17）：解析器与 Python 在真实页面上逐条一致（874 条合约记录的全部列和 sha256 record_hash，327 条时段的 hash）；GET 接口 132 个请求（含错误分支）与 Python 逐字节一致。已知差异和细节见 `docs/market_replay_server.md`。
- 每日同步在 2026-10-12 到 2026-11-14 期间每天都失败，报错是 `Connection refused`（走的环境代理是 `127.0.0.1:10810`）。2026-11-14 手动同步成功，数据补到 2026-11-16。代理不可用时同步会失败，要看任务 `cn-reference-daily` 的日志。
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
| `crates/core/cme_roll_metadata` | 共享：CME 换月元数据（mkt_signal 根 crate 的 `cme_features_1min_hfq` 也在用） |
| `crates/cme/cme_tas_replay` | 从 mkt_signal `7b8d96de` 迁入：CME TAS / LL2 / Raw LL2 回放与 1s/1min/hfq/ylabel 导出 |
| `crates/usstock/*` | 从 mkt_signal `7b8d96de` 迁入：美股 6 个 crate（TAS/normalised、MBP、RAW、Raw LL2、Nasdaq baseline、聚合）。`usstock_features_1min_hfq` 依赖实盘因子公式，仍留在 mkt_signal 根 crate，等因子公式抽进 core 后再迁 |
| `config/cme_*`、`config/usstock_*`、`scripts/*cme*`、`scripts/usstock_*`、`scripts/*ll2*`、`tests/scripts/test_cme_*` 等 | 随这些 crate 一起从 mkt_signal 迁入，共 85 个配置、脚本、Python 测试和文档。Python 测试从仓库根目录用 `python3 -m pytest tests/scripts` 运行；`tests/__init__.py` 必须保留，否则 `from scripts.x import ...` 会解析到 `tests/scripts` 上 |
| `crates/cme/lseg_delivery` | 原 `ly_cme_data` 子项目（2026-11-16 解散并入，见下文"LSEG / CME TAS 交付"）：LSEG/CME TAS tick parquet 导出、只用 LSEG 数据的 K 线与交易状态、改撤回补、SQLite 目录。配套 `scripts/lseg`、`config/lseg`、`docs/lseg` |
| `reference_cme/` | CME 参考数据服务，原 `cme_exchange_info_summary`（Python，`market-replay-server` 托管的服务 `cme-reference`，0.0.0.0:8770），是 8780 MCP 的 CME 上游 |
| `crates/mdl_replay` | 单日 MDL 回放服务 |
| `crates/web/data_dashboard` | 只读数据看板（库 + 薄的独立二进制 `data-dashboard`），由 `market-replay-server` 挂载：各数据集处理进度、品种中文名查询与合约下钻 |
| `reference/` | 参考数据服务（Python） |

- `crates/core/*` **只能依赖轻量的计算 crate**（目前有 anyhow、chrono、chrono-tz、csv、serde），可以用标准库读文件，但不得引入 RocksDB、parquet、arrow、polars、iceoryx、网络依赖，也不得依赖本仓库的非 core crate。mkt_signal 用 `git` + `rev` 固定版本依赖 core，core 里的任何改动都会进入实盘编译。
- 修改 core 后必须跑本仓库的全部测试，并用真实数据重跑一天、与生产 parquet 逐值比对（方法见下文）；要进入实盘，还需要在 mkt_signal 中更新 rev 并跑通那边的测试。
- 依赖版本以 mkt_signal 的 `Cargo.lock` 为基准，除本仓库独有的包以外保持一致。rocksdb 编译需要 `LIBCLANG_PATH`，`scripts/build-release.sh` 已经处理。

### baseline_data_1min 每日工序

生产的 1 分钟表依次经过四道工序。preprocess 的 `cn_pipeline.md` 漏写了第 3 步：

1. `cn_futures_l2 --msg-root ... --exchange X --start D --end D`：写入 RocksDB
2. `export_baseline_1min --start D --end D`
3. `trim_cn_futures_continuous_sessions --dataset baseline1min --session-as-of <快照日>`：按 `http://127.0.0.1:8765/api/v1/trading-sessions` 的时段裁剪。这个工具不允许输入和输出是同一个目录，所以先导出到 staging，再裁剪进生产目录。生产 `baseline_data_1min` 本身就是裁剪的输出目录，溯源记录在 `_provenance/continuous_sessions.json`；快照身份不同时，工具会拒绝执行。2026-11-15 快照从 2026-09-08 升级到 as_of 2026-11-14（共 327 段，请求日期 `session_as_of = 2026-11-15`）：新增的只有郑商所 `SO`（葵花籽油，2026-11-12 上市）的 4 个交易时段，其余完全不变。用两份快照裁剪同一份输入，5/5 个文件一致，所以已经裁剪过的历史文件无需重做。旧记录保存为 `continuous_sessions.20260908.json`，升级说明在 `snapshot_upgrade_20261115.json`。以后再出现新品种时，照这个流程处理：先确认新快照只是增加时段，再升级；如果已有品种的时段发生变化，就不能直接升级
4. `fill_1min_size_buckets --start <本次第一天> --end <本次最后一天>`：大小单分桶，只读打开 RocksDB，只改写范围内日文件的 12 个分桶列。**2024 年起，正式归档中有上月主力阈值的日分区采用这一口径（和实时 gRPC bar 共用同一套阈值文件）：**阈值 = 上一个完整上海自然月里主力合约逐交易日合并（换月前旧主力、换月后新主力）的区间成交额线性 P50/P90，一笔成交取它自己所在月的上月阈值；文件在 `/mnt/nvme-raid0-28t/fanghaizhou/cn_futures_data/size_thresholds/dominant_by_day/<YYYYMM>/<品种>.json`，由每月 1 日的任务 `size-thresholds-monthly`（`scripts/export_size_thresholds.sh`）生成，缺文件时工具自己算并写入。必须显式给 `--start/--end`（没有全历史模式）；`cn-daily` 传的是本次处理的第一天到最后一天，不再是月初。某品种上月没有阈值（新上市如 SO，或没有主力成交）时该品种当天文件保持原样、输出 `MISSING-THRESHOLDS`，绝不编造。2026-11-19 已将 2024、2025 两年及 2026 年 1–10 月的大小单回补到原生分钟和 HFQ 归档。2023 年及以前的文件使用旧规则（同一自然月、品种全部合约合并）。旧的 `--export-thresholds` 已删除，换成 `--export-month YYYYMM --product P`

TWAP 口径（2026-11-23 用户确认）：连续段首分钟只按第一笔快照之后的时间加权，不再把开头没有价格的时间按 0 计入；HFQ 不再置空首分钟 TWAP。详见 [docs/cn_futures_l2.md](docs/cn_futures_l2.md#1-分钟-twap-口径2026-11-23)。

迁移验证（2026-11-14）：用新二进制在临时目录按以上四步重跑 2026-08-26 的上期所和广期所，共 24 个文件、105,785 行，与生产 parquet 的 schema 和全部值完全一致；抽出 core 之后再验证广期所，5/5 个文件一致。

### 迁自 mkt_signal 的规则（cn_futures_l2）

以下规则原先在 mkt_signal `AGENTS.md`，2026-11-14 随代码迁到这里：

- `cn_futures_l2` 只从只读 PostgreSQL 表 `market_metadata.public.domestic_future_product_multipliers` 读取 `volume_multiple` 及其 `verified` 状态（Unix socket `/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/run`，端口 `5433`）。不得恢复代码里的品种映射，不得调用 DataGateway 或交易所接口，不得从成交额反推乘数，也不得默认取 `1`。载入的目录里缺少某个品种时，必须带着能定位问题的上下文直接 panic；这是批处理工具的明确例外。不得写入这个元数据实例，也不得用 `5432` 端口的只读热备库。
- 以前的根二进制 `tonglian_baseline_replay` 及其 ClickHouse staging 配置已经退役，不得恢复。
- 用法文档见 [docs/cn_futures_l2.md](docs/cn_futures_l2.md)。

## CME 与美股（2026-11-15 从 mkt_signal 迁入）

迁移验证：20 个 Python 测试文件共 223 个测试，两边结果一致。依赖版本以拆分前的 mkt_signal lock（`7b8d96de`）为基准，polars、rocksdb、arrow、parquet、chrono、flate2、csv、zstd 的版本全部一致。8 个 crate 的测试在两个仓库里逐个对比，结果一致：都是 253 个通过、2 个失败。少了 4 个测试，是因为它们属于留在 mkt_signal 的 `usstock_features_1min_hfq`。那 2 个失败在 mkt_signal 原始代码上同样失败，属于原有问题，分别是：

- `usstock_lseg_raw_replay::worker_error_does_not_publish_or_leave_building_db`
- `usstock_lseg_rawll2_replay::source::tests::reads_complete_rawll2_message`（时间戳相差 1600 秒）

为了适应新的目录层级，测试里的 `../../../preprocess` 改成了 `../../../../preprocess`。运行时默认的 `../preprocess/...` 是相对于运行目录的，从本仓库根目录运行时仍然指向 `/home/fanghaizhou/preprocess`，所以没改。

### LSEG 美股回放（规则迁自 mkt_signal AGENTS）

美股生产回放代码维护在本仓库。三种源产品的源数据契约和持久化编码都不同，必须分开维护：

- `crates/usstock/usstock_replay`：TAS / normalised 源行回放及下游工具。
- `crates/usstock/usstock_lseg_mbp_replay`：Market By Price 原始回放、模板审计、Python 正确性基准，以及 MBP 与 normalised 的比对工具。
- `crates/usstock/usstock_lseg_raw_replay`：通用 RAW zstd staging、带类型的事件回放、校验器、schema 打印、Python 正确性基准和审计夹具。

数据契约和审计结果文档仍放在 `/home/fanghaizhou/preprocess/data_format/lseg/`。不得把回放实现加回 `preprocess`。

### 新品种接入流程（以 2026-11-15 的 SO 为例）

1. **乘数：** 只能写入 PostgreSQL 的 `domestic_future_product_multipliers`，必须同时有交易所公告和通联成交额恒等式两项证据，并另外写一个增量 SQL 放在 `preprocess/database/`（SO 的是 `domestic_future_product_multipliers_20261115_so.sql`）。不得只凭 OpenCTP 的值写入。
2. **交易所映射：** 在 `cn_futures_core::segments::exchange_of` 里补上品种。core 的改动只有 mkt_signal 升级 rev 后才会进入实盘。
3. **时段快照：** 按上面的方法确认新快照只是增加时段，然后升级归档的溯源记录，并修改 `config/cn_daily.toml` 和 `config/cn_kline_server.toml` 里的 `session_as_of`。

## CME 换月（2026-11-26 起统一）

用户 2026-11-26 明确：**CME 的东西以后都在本仓库维护，不再留在 preprocess**。preprocess 里的 CME 文档（`data_format/lseg/*roll*`、`database/cme_*`）只作历史记录，不再是依据；新的规则、风险日、证据和代码只写在这里。

- **规则：** 见 [docs/cme_roll.md](docs/cme_roll.md)。16 个品种统一按国内 `local_rqdata_rule0` 的方式选主力（持仓超过当前主力 1.1 倍、只往更远月份换、不回切）。
  - **时序：** CME 的 OI 晚一天，用 T−1 的终值决定 T+1；决策时刻是开盘前 5 分钟（16:55 CT）。
  - **期限保护：** 风险日前 1 个交易日强制换月。风险日美债取首次意向日，金属取首次持仓日，外汇、股指、CL 取最后交易日。
- **风险日表：** `config/cme_roll/risk_days.csv`，每行带来源，由 `cme_roll_evidence` 生成。
  - 营业日 = 周一到周五，去掉美联储假日（周日的顺延到周一，周六的不调休）和耶稣受难日。它不等于 Globex 的交易日：哥伦布日、Juneteenth 等照常交易，但不是营业日。
  - 按规则推算的日期必须和已审核日期全部一致，才能用来补缺。
  - 缺风险日的合约不能成为候选，要报出来。
- **代码：** `crates/cme/cme_roll`，规则引擎是纯函数，只用决策时刻之前已经发布的 OI；证据工具只读不写。换月服务和复权还没做。

## reference_cme/：CME 参考数据服务（2026-11-15 并入）

原 `/home/fanghaizhou/cme_exchange_info_summary` 用 `mv` 整体搬入。2026-11-16 经用户同意，原路径的软链接已删除，各处引用（reference_cme 状态文件、preprocess 文档和证据、LSEG artifacts）都已改成新路径。

- **不进 git 的内容：** Databento 原始下载 `batch_downloads/`（约 2 GB）、解析结果 `parsed/`（服务读取的 parquet）、`*.dbn`、日志和 `.env`（含 Databento key），按它自己的 `.gitignore` 处理，只留在本机磁盘上。
- **venv：** 按 `requirements.lock`（搬迁前冻结，24 个包）在新路径重建，版本逐个一致。
- **进程：** 由 `market-replay-server` 托管（服务 `cme-reference`）。搬迁前后共比对 10 个接口（8770 的首页、health、summary、definitions、status，以及 8780 MCP 的 4 个 CME 工具），响应逐字节一致。

## 国内期货元数据 PostgreSQL（2026-11-16 起归 fanghaizhou）

`market_metadata`（端口 5433，只监听 127.0.0.1）里有乘数表、竞价窗口、`cn_futures_reference` schema、CME 的 TAS 动作事件、复权因子等。原来是 u171 的实例，2026-11-16 用户要求废弃 u171、新功能的服务一律从 fanghaizhou 启动，于是迁了过来。

- **数据目录：** `/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/main`；socket 在 `.../16/run`，日志在 `.../16/log`。以 fanghaizhou 身份停掉旧实例后，用 tar 流做物理拷贝，数据完全一致。旧目录和 u171 crontab 里对应的 `@reboot` 条目都已删除，crontab 的备份在 `/tmp/u171_crontab.bak.*`。u171 名下的另一个实例 `/mnt/nvme-raid0-28t/postgresql/16`（crypto NAV 热备，5432）不归本仓库管。
- **进程管理：** PM2 进程 `pg-domestic-futures`，配置 `deploy/pm2/pg-domestic-futures.config.cjs`。入口脚本 `run-pg-domestic-futures.sh` 会等 RAID 挂载后再 `exec postgres`。PM2 停止时发 SIGINT，postgres 执行快速关闭；`kill_timeout` 设为 60 秒，不得调小，否则 PM2 会 SIGKILL postmaster。开机由 fanghaizhou 的 PM2 resurrect 拉起，不用 systemd。
- **故障原因与修复：** 2026-11-16 整库不可连接，报错 `could not open shared memory segment "/PostgreSQL.*"`：u171 名下的 posix 动态共享内存段在 `/dev/shm` 里被删了。新实例设置 `dynamic_shared_memory_type = mmap`，共享内存段放在数据目录内。
- **连接方式：** 数据库角色仍然是 `u171`，通过 peer 映射 `market_metadata_u171`（fanghaizhou → u171）连接，所有客户端的 `user=u171` 不需要改。连接串：`postgresql:///market_metadata?host=%2Fmnt%2Fnvme-raid0-28t%2Ffanghaizhou%2Fpostgresql%2Fdomestic_futures%2F16%2Frun&port=5433&user=u171`。引用旧 socket 路径的代码、配置、文档（本仓库、reference、reference_cme、preprocess 的当前文件）已全部改到新路径；`preprocess/database/` 下按日期归档的历史证据保持原样。

## 依赖统一与构建目录（2026-11-16）

用户同意统一并升级依赖：以 `ly_cme_data` 原来那份更新的 `Cargo.lock` 为基准，整个 workspace 共用一份 lock（其中 196 个包比原来的 `market_replay` 新）。构建目录改到 NVMe 上的 `/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay`，在 `.cargo/config.toml` 里设置；`./target` 是指向它的软链接（`/home` 所在磁盘已用 92%）。

升级前后的验证：
- Rust 测试逐个一致，`ly_cme_data` 原来的 30 个也包含在内；以前被算了两遍的 4 个测试属于 `cme_tas_export_status`。仍然只有那 2 个已知失败。
- Python 测试：本仓库 223 个通过，`ly_cme_data` 的 10 个文件都 OK（用 `reference_cme/.venv` 在 `scripts/` 目录下运行 unittest）。解散后剩 5 个文件、75 个测试，运行方法见下文。
- 11-16 的 1 分钟表按生产工序重跑，80/80 个文件与生产一致；主力表全量重算后，36/36 个已发布文件逐字节一致；kline 校验 791 个合约，0 根不合格。

## LSEG / CME TAS 交付（原 ly_cme_data，2026-11-16 解散并入）

用户 2026-11-16 要求把 `ly_cme_data` 从独立子项目切掉，完全并进本仓库：

- **位置：**
  - Rust 代码在 `crates/cme/lseg_delivery`。
    - bin：`lseg_tas_parquet`（原默认 bin `ly_cme_data`）、`lseg_kline`、`lseg_catalog`、`tas_*` 工具，以及 `ib_ric_parquet`（等 IB 导出 crate 接手后删除）。
    - 共用逻辑在库里：`tasdb`（只读打开 TAS RocksDB、事件时间、路径转义、JSON 落盘、sha256）和 `limits`（固定预算）。TAS 记录编解码和 `quote_last` 合并只用 `cme_tas_replay` 里的那一份，不得再复制。
  - Python 在 `scripts/lseg/`，以后逐步改写成 Rust。路径统一从 `lseg_paths.py` 取。测试运行方法：`cd scripts/lseg && ../../reference_cme/.venv/bin/python -m unittest test_*.py`。
  - 配置在 `config/lseg/`，文档在 `docs/lseg/`。
  - 运行状态、证据和报表放在 NVMe 的 `/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/{artifacts,reports}`，不进 git。
- **历史记录：**
  - 原项目 AGENTS 全文（含逐日历史）归档在 `docs/lseg/agents_history.md`。下面没写到的口径细节以它为准，但其中关于 `ly_cme_run`、LD_PRELOAD 和"禁止服务/定时器"的规定已被本节取代。
  - 2026-11-16 经用户同意，artifacts、reference_cme 和 preprocess 里的旧路径（`ly_cme_data`、`cme_exchange_info_summary`）都已批量改成新路径（588 个文件）。另有 8 个文件记录了这些文件的 sha256，已同步更新。改写前的完整备份（`artifacts.before_path_rewrite_20261116` 和 `path_rewrite_backup_20261116_misc.tar.gz`）经用户确认，已于 2026-11-17 删除，改写后 `lseg-daily` 已正常运行。
  - **例外：** `artifacts/lseg_full_rocksdb_20261112/plan.json` 保持原样，没有改写。`lseg_kline` 用这份计划里的作业 JSON 作为源身份，与提取缓存核对，改动路径会导致 143 个日包作业报 `source identity changed`。
- **凭据：** IB MySQL 的 `IB_MYSQL_*` 放在 `~/.config/market_replay/ib_mysql.env`（权限 600），不进仓库，也不打印。
- **资源（最高优先级）：**
  - 总预算是 64 GiB 内存、32 个线程。每个工具都显式固定自己的线程池：
    - 聚合最多 2 个（`limits::AGGREGATION_WORKERS`）；
    - Parquet 发布 8 个（`limits::IO_THREADS`，实测 16 个不比 8 个快）；
    - **例外（用户 2026-11-17 批准，仅限一次性回补）：** `lseg_l2_slice`（固定频率 L2 下采样，见 `docs/lseg/l2_slice.md`）的 `--workers` 可到 8（`limits::L2_SLICE_WORKERS`，默认仍是 2）。理由：每个 worker 内存只有约 70 MiB，纯 CPU 型，各源文件独立。每日任务和其他工具不得借用这个上限；
    - **例外（用户 2026-11-24 批准，仅限一次性历史回补）：** `lseg_tas_parquet`（TAS 逐笔 trade / quote 导出）的 `--workers` 可到 8（`limits::TAS_EXPORT_WORKERS`，默认仍是 2）。每日任务 `lseg-daily` 不得借用这个上限；
    - **例外（用户 2026-11-25 批准，仅限一次性回补）：** `tas_open_interest sync --backfill`（OI 入库）的 `--workers` 可到 8（`limits::OI_BACKFILL_WORKERS`，默认仍是 2）。`lseg-daily` 不得借用这个上限；
    - RocksDB 一律用 `tasdb::open_read_only` 打开：后台线程 1 个，打开文件线程 1 个，block cache 共享且有上限。
  - 不得按机器的 CPU 或内存自动扩张，也不得出现没有预算的多进程乘多线程。Python 里 DuckDB、Arrow、BLAS 的线程数显式设为 1。
  - 读取、解析、聚合、写入都必须流式、有界。不得把整段状态或控制消息先收集进 Vec、list 或 JSON 数组再处理。
  - 原来的 `ly_cme_run` 监控器、资源锁和 `libly_cme_cpu_budget.so` 已退役。现在每日任务由 `market-replay-server` 的单一任务队列串行调度（不用 systemd，也不为单个任务注册 PM2），一次性回补用前台命令；独立的大任务不得并发运行。
- **交付盘：**
  - 路径是 `/mnt/nfs/30.100_cta_data/lseg/`，也就是 `172.16.30.100:/mnt/FuturePool/CTA_Data/lseg`。
  - 目录布局是 `<交易所小写>/<UTC 年>/<原始 RIC>/<UTC YYYYMMDD>/{trade,quote,status,session_status,market_status,kline,kline_ib}.parquet`。RIC 保留大小写，用 `tasdb::component` 做可逆转义。
  - 发布流程：
    1. 写临时文件并 fsync；
    2. 核对 footer，包括每个 row group 的 RIC 和 UTC 时间范围；
    3. 用硬链接发布，不覆盖已有文件。NFS 不支持 `RENAME_NOREPLACE`，不得改成普通 rename。
  - 抽样读回首、中、末三个文件。
  - **只记录问题**（写到 `lseg/meta/problems`），不写成功回执和进度报告。
  - 目录索引是 `lseg/meta/catalog.sqlite`。对外说明只有 `lseg/README.md` 一份，要保持简短。
- **口径：**
  - TAS（`normalised`）、Normalized LL2、Raw LL2 和采购目录里的 `raw` 是不同的源，不能混称。生产 RocksDB 只读。
  - `kline.parquet` 只有这 10 列：`ric, ts_utc_ns, open, high, low, close, volume, trade_count, is_ffill, status`。`status` 的取值是 TRADING / SCHEDULED_CLOSED / HALTED / PARTIAL / UNKNOWN。
  - 改撤按 `correction_policy_version=1` 处理，冲销在原生引擎内完成（见证库 `artifacts/tas_trade_witnesses_20261110`，见 `docs/lseg/tas_trade_revisions.md`）。原 237 个 RIC 没有见证库时拒绝重算；比对窗口外的原 Python 文件不改。
- **L2 洗库与 RocksDB 位置：** 2020 年起全部 L2 都已洗进 RocksDB，在 HDD 的 `/mnt/hdd-raid5-72t/fanghaizhou/lseg_data/l2_db/`（2026-11-24 从 NVMe 搬来，3.22 TB，600,455 个文件）：2026-06-01 起每个 UTC 日一个库，其余按包一个库 `_stream/{p2020..p2024,p2025,s2025,p2026h1,sg*,sf*,sc2025,raw*}`。已洗过哪些源文件记在 `lseg/meta/l2_sources.sqlite`，对应的原始包已从本地删除（Report 保留）。用 `lseg_l2_slice extract-rics`/`extract` 按需要的频率提取切片。用法、校验和延伸方法见 [docs/lseg/l2_slice.md](docs/lseg/l2_slice.md)。
- **L2 补充包与缺口（2026-11-18）：** 2024 主包和 5 个补充包（`comex_globex` 2024、`comex` 2025、`futures_11` 2024/2025/2026）已洗库并交付到 `lseg/`，只交付各时段的新 RIC（`deliver --only-rics`，补充包逐库交付，不覆盖已有文件）；2010–2023 只清点未洗。TAS 有而 L2 补不起来的 RIC-年记在 `lseg/meta/l2_gaps.sqlite`（独立文件，不进 `catalog.sqlite`），重建见 `docs/lseg/l2_slice.md`「L2 补不起来的 RIC」。
- **Raw LL2 补缺（2026-11-19/21）：** Normalized LL2 里完全没有的 RIC（采购目录有 L2 层但包里没有，Raw 有数据）从 Raw LL2（`rawLL2`，Legacy Level 2 增量槽）还原整本书后同样洗进 RocksDB（`_stream/raw2024`、`raw2025a..d`、`raw2026`，约 355 GB，保留），再交付到 `lseg/`：2024 年 411 个 RIC、2025 年 1816 个、2026 年 1–5 月 416 个，共 303,807 个文件。Raw 和 Normalized 是不同的源：交付时 RIC-年目录里有标记文件 `l2_source`（`raw`，或 `mixed` 加 `raw_day YYYYMMDD` 行），`catalog.sqlite` 的 `l2_directories.source`（`normalized`/`raw`/`mixed`）和表 `l2_raw_days` 记录来源，`l2_gaps.sqlite` 的 `l2_raw_versions` 列出 Raw 来源。一个 RIC-年默认只用一个来源；2026 年 4 个 RIC（HGN29、STXEU27、STXEZ27、UROZ28）已有 6 月后的 Normalized 日文件，用 `--allow-mixed` 由 Raw 补 1–5 月缺的天。Raw 包是许多独立 gzip 成员拼成且按 RIC 排序，`scripts/lseg/rawll2_segments.py` 只取含目标 RIC 的成员（2024 年 8.9 GB / 1.7 TB），洗库用 `backfill --raw-ll2-rics … --raw-ll2-segments`。用法、口径和核对见 [docs/lseg/l2_raw.md](docs/lseg/l2_raw.md)。
- **L2 范围（2026-11-21 用户决定）：** 2020 年以前只要 TAS 的 K 线，不要 L2（2010–2019 的 L2 不洗、不交付；缺口表里记为 `out_of_scope`）。L2 只做 2020 年起：2020–2023 的主包在本地待洗，补充包按需。
- **L2 索引与每日任务：** `lseg-daily` 每次重建 `catalog.sqlite`；旧版会删掉 `l2_directories`，新版（`build` 带 carry-over 和单写者锁）保留它。`lseg_catalog l2-sync` 按交付盘重新生成（约 1 小时，只在没有任务时跑）。
- **L2 缺口原因：** `l2_gaps.sqlite` 每行有 `cause`（人话原因）和 `purchase_layers`。没有 L2 的 RIC 多数是采购目录里只有 `tas`/`summarised` 层（L2 从没订购，要补需向 LSEG 新申请）；采购目录的 L2 层按 RIC 名记、不分年份，同代码的旧合约会重名（如 CGBM7 的 L2 属于 2017 年合约）。
- **IB：** 来源是 MySQL `ib_data.minute_data`，只做纯导出，用于对比："有多少切多少"，不补、不改、不对齐。
- **`review/` 交易所目录（2026-11-26）：** 两个原因。
  - 一位数年份的 RIC 撞名：比如 `TNH2` 在 HRD 里同时对应 2012 年 NYMEX 的运费合约和 2022 年 CBOT 的美债。
  - 新挂牌合约在静态 HRD（2026-06-06 提取）里查不到。

  经用户批准，增加两条判定规则，每条结果都记下依据：
  - `hrd_by_date`：取存续期覆盖数据日期的那个 HRD 版本；
  - `root_follow`：同前缀的合约在前后 3 年内只出现在一个交易所时，跟随它。

  执行方式：
  - `lseg-daily venues-plan` 只读给出判定；
  - `lseg-daily venues-migrate --apply` 用硬链接搬到正确目录，不覆盖已有文件，更新 state 和 K 线引擎的覆盖表 `artifacts/venue_overrides/venue_overrides.csv`；
  - 迁移后要逐个 RIC 重算 K 线，再跑 catalog `build`、`l2-sync`、`ib-sync`；
  - 日常运行里，新 RIC 自动套用 `root_follow`，已经在 `review/` 里的不自动改目录。

  判不出来的（HRD 双重挂牌、非单月代码）仍留在 `review/`。详见 [docs/lseg/venue_review.md](docs/lseg/venue_review.md)。
- **持仓量 OI（2026-11-25）：** CME 并非没有 OI。TAS 源文件的 `Open Interest` 列有值，但 RocksDB 和 parquet 都没收。现在由 `tas_open_interest`（`crates/cme/lseg_delivery`）从原始 gzip 提取，写进 PostgreSQL `market_metadata.lseg_open_interest`，只写这个 schema。
  - 每个交易日发两次：预估值约 20:15 CT，终值次日约 09:05 CT。`Date` 是所属交易日，早年为空时存 NULL，不推算。
  - 使用时必须按发布时间取"当时已知"的值。
  - 存储是 schema 版本 2：数据行用 `sources.source_id` 引用源文件，2026-11-26 从版本 1 迁移过来，占用从 9.3 GB 降到 4.2 GB，迁移前后校验和一致。
  - 已入库：2020-01 起的全部历史包和 2026-06-01 起的日包，发布时间到 2026-11-23，共 2,668 万条观测，0 条 reject。2020 年以前的包没有入库。
  - `lseg-daily` 配置有 `[open_interest]` 段时，每天增量入库。这个配置段要等新二进制部署之后才能加，因为旧二进制的配置解析带 `deny_unknown_fields`，遇到它会报错。
  - 详见 [docs/lseg/open_interest.md](docs/lseg/open_interest.md)。
