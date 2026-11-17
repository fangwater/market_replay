# 中国期货参考数据服务

## CN / CME 统一 MCP 与 Web 查询

已增加供同机其他用户使用的只读共享入口：中文查询页 `http://127.0.0.1:8780/`，
Streamable HTTP MCP `http://127.0.0.1:8780/mcp`，普通查询接口 `GET /api/{工具名}`。
同时覆盖本项目与 `/home/fanghaizhou/market_replay/reference_cme` 的数据，
包含 CME 官方历史交易时段和海外佣金参考。使用者无需访问项目目录或数据库。
给其他用户或 AI 的独立文件是 [同机数据使用说明](docs/cn-cme-data-guide.md)，包含全部接口、参数、返回字段及示例。
使用者在自己的目录执行下列命令即可下载，不必配置 MCP：

```bash
curl -fsS http://127.0.0.1:8780/usage.md -o cn-cme-data-guide.md
```

网页文档为 `http://127.0.0.1:8780/docs`；运维与协议细节见 [服务维护说明](docs/mcp-access.md)。

面向回测的国内期货参考数据接口及中文看板。系统同时维护 OpenCTP 经纪商参考数据、逐品种连续竞价时段、交易所公告原文、AI 分析结果和经人工审核的申报费规则。OpenCTP 不是交易所正式数据，也不会覆盖本机权威乘数表或既有交易时段表。

服务通过本机国内期货 PostgreSQL 16 实例的 Unix socket 连接 `market_metadata`，使用 `u171` peer 认证。项目数据只写入独立 schema `cn_futures_reference`，不会触碰 `public` 中已有的乘数和交易时段表。

## 账户专属费率

创元等账户的 CTP 查询结果独立存储和维护，不覆盖 OpenCTP 参考数据。接口为 `GET /api/v1/account-rates?account=...`，支持带时区的 `asOf` 和观察历史；导入由 Rust `cn-reference import-account-rates` 完成。回测可选择 `fee_policy = "account_snapshot"`。数据粒度、时间语义及操作说明见 [账户费率记录](../docs/cn_account_rates.md)。

## 增量存储

OpenCTP 同步不保存整表快照，也不归档其整页 HTML。每个合约的每个不同状态是一条增量版本：

- `start_ts`：该状态首次从数据源观测到的 UTC 时间。
- `update_ts`：该相同状态最近一次被确认的 UTC 时间。
- 数据相同：仅推进现有行的 `update_ts`。
- 数据变化：新增该合约的一行版本并写入新的 `start_ts`。

`asOf=YYYY-MM-DD` 会为每个合约选择该日及以前的最新版本。历史精确度仍取决于实际导入过的数据日期，不能把未采集日期推断为交易所正式历史费率。

连续竞价时段保存在 `cn_futures_reference.continuous_session_versions`，同样按内容增量版本化。每行对应一个交易所、品种和 OpenCTP 段号，并保存本地开始/结束时间、日盘或夜盘、是否跨午夜及原始 JSON。当前中金所规则被作为导入校验条件：`IF/IH/IC/IM` 为 `09:30-11:30、13:00-15:00`，`T/TF/TL/TS` 为 `09:30-11:30、13:00-15:15`；上游返回偏离时同步会停止，需先核验交易所规则。

OpenCTP 时段用于生成基础模板，不等同于交易所官方历史日历。表中的 `source_date` 是首次观测日期；休市日、临时调整及尚未采集的历史变化不能由静态模板推断。

手动同步当前全量期货连续竞价时段：

```bash
set -a; . ./.env; set +a
python cn_futures_ref.py sync-trading-sessions
```

相同内容只更新原版本的 `update_ts`；变更内容会生成新版本。全量响应会为消失的时段写入停用版本，按 `markets` 或 `products` 筛选的响应不会停用查询范围外的数据。

交易所公告采用独立的增量版本：公告索引、HTML 原文和 PDF、图片、Office 附件均保存在 PostgreSQL；内容哈希不变时只更新 `update_ts`，内容变化时才新增 `announcement_versions`。公告原文不会写入文件系统。

## 公告与申报费规则

公告只在网页“公告规则”视图点击“拉取并分析”，或显式执行以下命令时刷新。看板和 AI 分析只处理期货相关公告，纯期权公告不进入待审核流程；默认回看每家交易所最近 5 页，并固定核验上期所、能源中心 2024 年申报费调整基准公告的官方 URL：

```bash
set -a; . ./.env; set +a
python cn_futures_ref.py refresh-announcements
```

每日任务同步 OpenCTP 手续费参考数据和连续竞价时段，不会拉取或分析公告。每次公告刷新均在 `announcement_refresh_runs` 中记录 `trigger_type=MANUAL`。

分析服务直接读取当前用户的 `~/.codex/config.toml` 和 `~/.codex/auth.json`，使用其中的 model provider `base_url` 和 `OPENAI_API_KEY`。仓库及 `.env` 不保存或复制密钥。模型固定为 `gpt-5.6-sol`，Responses API 参数固定为 `reasoning.effort=high`，并关闭响应存储。可通过 `CN_FUTURES_REF_CODEX_HOME` 指向另一份同结构配置。

模型产出的规则默认是 `PENDING`。统计粒度、生效日或分档不完整的结果仅作为“待补充候选”保留在分析 JSON 中，不能审核通过，也不会参与计算。只有网页审核或导入时标记为 `VERIFIED` 的规则才可执行。

导入审校后的规则：

```bash
python cn_futures_ref.py import-message-rules rules.json --verified
```

规则 JSON 可为单个对象、对象数组或 `{"rules": [...]}`。每条规则必须包含 `rule_id`、`exchange`、`products`、`instrument_type`、`effective_from`、`aggregation_scope`、`information_volume`、`otr_formula`、`zero_fill_denominator`、`tiers`、`source_url` 和 `source_sha256`。当前计算器支持 `trading_day+account+instrument` 聚合。

根据订单事件计算每日申报费：

```bash
python cn_futures_ref.py message-fee --orders order_events.csv
```

CSV 必须提供 `trading_day`、`account`、`exchange`、`instrument`、`event_type`，可选 `order_id` 和 `filled_quantity`。也支持对应的中文表头：`交易日`、`账户`、`交易所`、`合约代码`、`事件类型`、`报单编号`、`成交量`。事件类型支持 `NEW/报单`、`CANCEL/撤单`、`INQUIRY/询价`、`OTHER/其他`、`FILL/成交`。分档费用按“各档位信息量 × 对应费率”累加。

## 部署

```bash
./scripts/bootstrap.sh
cp .env.example .env
```

`.env.example` 已配置 `u171` 的 Unix socket DSN。将 `PYTHON_BIN` 改为本项目虚拟环境路径：

```bash
PYTHON_BIN=/home/fanghaizhou/market_replay/reference/.venv/bin/python
```

**当前分工（2026-11-17 起）：** `/api/v1` 的查询、导出和两个同步接口（`POST /api/v1/sync`、`POST /api/v1/trading-sessions/sync`）由 Rust 实现（仓库根目录 `crates/ref/cn_reference`），直接挂在 `market-replay-server` 进程的 :8765 上，每日同步是它的任务 `cn-reference-daily`（上海 18:15）。本目录的 Python 服务现在以 `cn-reference-ui` 的名字只监听 `127.0.0.1:18766`，由 :8765 把没迁移的路径原样转发过来：网页 `/`、公告、报单费规则、费用估算、`/import`、`/overrides/import`。这些是下一步要迁的功能。下面的表格列出的是完整接口，未标注的都以 :8765 为入口。

日志在 `/mnt/nvme-raid0-28t/fanghaizhou/market_replay_server/logs/{service,job}/`，重启在 `http://127.0.0.1:18313/admin` 操作，说明见仓库根目录 `docs/market_replay_server.md`。每日任务使用 PostgreSQL advisory lock（695102482），重叠运行时只允许一个同步写入；公告人工刷新使用另一个 advisory lock。手动同步用 `cn-reference sync`（同一把锁）。

## HTTP API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/account-rates` | 独立账户手续费/保证金，按观察时间查询 |
| GET | `/api/v1/health` | 健康检查 |
| GET | `/api/v1/data-state?asOf=YYYY-MM-DD` | 增量数据状态 |
| GET | `/api/v1/trading-sessions?asOf=YYYY-MM-DD&venue=CFFEX&product=IF` | 连续竞价时段 |
| GET | `/api/v1/overview` | 按交易所分组的数据 |
| GET | `/api/v1/exchanges/{venue}` | 单个交易所数据 |
| GET | `/api/v1/instruments/{venue}/{instrument}` | 单个合约及 exchangeInfo 表示 |
| GET | `/api/v1/exports/exchange-info` | exchangeInfo JSON 导出 |
| GET | `/api/v1/exports/records.csv` | 当前合约记录 CSV 导出 |
| POST | `/api/v1/sync` | 拉取 OpenCTP 并增量合并 |
| POST | `/api/v1/trading-sessions/sync` | 拉取 OpenCTP times 并增量合并连续竞价时段 |
| POST | `/api/v1/import` | 导入 Base64 HTML/CSV 历史数据并增量合并 |
| POST | `/api/v1/overrides/import` | 导入 Base64 官方 override CSV |
| POST | `/api/v1/fees/estimate` | 单腿手续费估算 |
| POST | `/api/v1/fees/round-trip` | 往返成本估算 |
| GET | `/api/v1/announcements` | 公告、附件、分析和规则看板数据 |
| POST | `/api/v1/announcements/refresh` | 人工拉取公告并分析新内容版本 |
| GET | `/api/v1/announcements/{id}/raw` | 从 PG 读取公告 HTML 原文 |
| GET | `/api/v1/announcement-assets/{id}` | 从 PG 读取公告附件 |
| GET | `/api/v1/message-fee-rules` | 查询申报费规则 |
| POST | `/api/v1/message-fee-rules/{id}/review` | 审核或驳回候选规则 |
| POST | `/api/v1/message-fee-rules/import` | 导入 Base64 规则 JSON |
| POST | `/api/v1/message-fee/calculate` | 使用 Base64 订单事件 CSV 计算申报费 |

## 项目结构

```text
cn_futures_ref/
  models.py      字段定义、交易所别名与数值标准化
  source.py      OpenCTP 传输及 HTML/CSV 解析
  trading_sessions.py OpenCTP 连续竞价时段 JSON 校验与标准化
  announcements.py  六家交易所公告发现、原文和附件采集
  codex_ai.py    当前 Codex provider 配置与结构化分析
  message_fees.py   申报费规则验证、版本化与订单事件计算
  storage.py     PostgreSQL 合约、公告、分析和规则存储
  services.py    同步、查询、导出及费率计算
  cli.py         公告刷新、规则导入和申报费计算命令
  api.py         HTTP API 与前端静态资源服务
  server.py      环境变量驱动的服务入口
scripts/
  run-api.sh                     网页、公告、报单费服务的运行包装（market-replay-server 调用，:18766）
  run-mcp.sh                     MCP 网关运行包装
  bootstrap.sh                   创建虚拟环境
```

每日同步和手动同步现在都用 Rust：`cn-reference sync`（见仓库根目录 `crates/ref/cn_reference`）。原来的 `daily_sync.py`、`daily_scheduler.py`、`run-daily-sync.sh` 和一次性的 SQLite 迁移脚本已于 2026-11-17 删除，需要时可从 git 历史取回。
