# CN / CME 统一 HTTP 与 MCP 接入

本文件保留服务维护及 MCP 协议说明。交给同机其他用户或 AI 的独立文件为
[cn-cme-data-guide.md](cn-cme-data-guide.md)，在线地址为 `http://127.0.0.1:8780/usage.md`。
使用者只需按该文件调用本机 HTTP 接口，不必配置 MCP 客户端。

## 地址

| 用途 | 同机访问地址 |
| --- | --- |
| 中文查询页 | http://127.0.0.1:8780/ |
| MCP（Streamable HTTP） | http://127.0.0.1:8780/mcp |
| 接口目录及参数 JSON Schema | http://127.0.0.1:8780/api/tools |
| 数据覆盖与更新时间 | http://127.0.0.1:8780/api/exchange_summary |
| 网关存活检查 | http://127.0.0.1:8780/health |

同机其他 Linux 用户使用相同地址，不需要维护者的 home 目录权限、PostgreSQL 账号、Python 环境或 Databento key。服务由维护者的 PM2 进程持有。

监听地址默认为 `0.0.0.0:8780`；可信内网可将 `127.0.0.1` 换为服务器 IP（当前为 `172.16.30.38`）。`0.0.0.0` 是监听地址，不作为客户端连接地址。服务未启用登录，能连接该端口的用户均可读取这些参考数据。

## 普通 HTTP：curl / Python / 浏览器

所有查询均为 `GET /api/{工具名}`，参数名与 MCP 相同，使用下划线。省略可选参数表示采用默认值；日期格式为 `YYYY-MM-DD`，布尔值为 `true` / `false`。查询返回 JSON，错误返回非 2xx 状态及 `error`。

```bash
# 先确认数据范围及新鲜度
curl -fsS 'http://127.0.0.1:8780/api/exchange_summary'

# 国内 IF 合约规格、手续费及保证金参考
curl -fsS 'http://127.0.0.1:8780/api/cn_instruments?venue=CFFEX&product=IF&page_size=10'

# 国内连续竞价时段
curl -fsS 'http://127.0.0.1:8780/api/cn_trading_sessions?venue=CFFEX&product=IF'

# CME 历史定义
curl -fsS 'http://127.0.0.1:8780/api/cme_definitions?product=ES&as_of=2026-09-02&page_size=10'

# 指定 UTC 日期的状态事件；可增加 symbol=ESU6 查询具体合约
curl -fsS 'http://127.0.0.1:8780/api/cme_status_events?product=ES&day=2026-09-02&page_size=10'

# 官方历史交易时段，日期为 Chicago trade_date
curl -fsS 'http://127.0.0.1:8780/api/cme_trading_sessions?product=ES&start_date=2025-11-24&end_date=2025-11-28'

# 经纪商佣金参考
curl -fsS 'http://127.0.0.1:8780/api/cme_broker_commissions?product=GC'
```

Python 标准库即可调用，无需安装依赖：

```python
import json
from urllib.parse import urlencode
from urllib.request import urlopen

base = "http://127.0.0.1:8780"
params = {"product": "ES", "as_of": "2026-09-02", "page_size": 10}
with urlopen(base + "/api/cme_definitions?" + urlencode(params), timeout=65) as response:
    data = json.load(response)
for row in data["records"]:
    print(row["raw_symbol"], row["min_price_increment"], row["unit_of_measure_qty"])
```

分页接口返回 `page`、`pageSize`、`pages`、`total`、`records`。页码从 1 开始，每页最多 200 条；根据 `pages` 逐页获取，不要只读第一页。CME 官方时段一次最多查询 366 天。

## MCP 客户端

在客户端的远程 MCP 配置中填写：

- 名称：`cn-cme-exchange-reference`
- 传输：`Streamable HTTP`
- URL：`http://127.0.0.1:8780/mcp`
- 认证：无

这不是旧式 SSE `/sse` 端点，也不是需要其他用户启动服务端进程的 stdio 模式。不同客户端配置文件格式不同，以其“远程 MCP / Streamable HTTP”配置项为准。

### 浏览器访问与请求头

浏览器直接打开 `/mcp` 会转到中文接入说明；这一步只打开说明页，并不建立 MCP 会话。
普通 `curl GET /mcp` 返回连接提示。实际 MCP 客户端使用 JSON-RPC POST，
请求头为 `Content-Type: application/json`、`Accept: application/json, text/event-stream`。

旧版本直接用浏览器打开会返回 `Not Acceptable: Client must accept text/event-stream`，
这是浏览器 GET 被当成事件流请求所致，现已修正浏览器入口。
客户端应选择 **Streamable HTTP**，将 URL 填入连接配置；不要选择旧式 SSE 模式。
在其他电脑运行客户端时，使用 `http://172.16.30.38:8780/mcp`；`127.0.0.1` 仅适用于服务所在机器。

不安装 SDK 也可以验证 MCP 握手：

```bash
curl -fsS 'http://127.0.0.1:8780/mcp' \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"curl-check","version":"1"}}}'
```

成功时返回 `result.serverInfo`、`result.capabilities` 和协商后的协议版本。

可用官方 Python MCP SDK 验证（在调用者自己的环境安装 `mcp==1.30.0`）：

```python
import asyncio
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

async def main():
    async with streamable_http_client("http://127.0.0.1:8780/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools = await session.list_tools()
            print([tool.name for tool in tools.tools])
            result = await session.call_tool(
                "cme_definitions", {"product": "ES", "as_of": "2026-09-02", "page_size": 5}
            )
            if result.isError:
                raise RuntimeError(result.content)
            print(result.structuredContent)

asyncio.run(main())
```

## 查询工具

HTTP 和 MCP 提供相同的 12 个查询工具；完整类型、默认值和参数限制由 `/api/tools` 或 MCP `tools/list` 返回。

| 工具名 | 用途 | 主要参数 |
| --- | --- | --- |
| `exchange_summary` | CN/CME 实际覆盖与更新日期 | `market=all/cn/cme` |
| `cn_instruments` | CN 合约规格、手续费及保证金参考 | `venue, product, search, as_of, include_options, page, page_size` |
| `cn_instrument` | 单个合约及 exchangeInfo | 必填 `venue, instrument`；可选 `as_of` |
| `cn_trading_sessions` | 连续竞价基础模板 | `venue, product, as_of` |
| `cn_announcements` | 已采集公告、分析和规则 | `venue, limit` |
| `cn_message_fee_rules` | 申报费规则 | `status` 默认 `VERIFIED` |
| `cme_definitions` | CME 历史定义列表 | `product, as_of, search, active_only, page, page_size` |
| `cme_definition_history` | 具体合约定义及最近 50 个变更 | 必填 `symbol`；可选 `as_of` |
| `cme_status_symbols` | 指定日有状态事件的有效合约 | 必填 `product`；可选 `day` |
| `cme_status_events` | 交易状态事件 | 必填 `product`；可选 `day, symbol, page, page_size` |
| `cme_trading_sessions` | 官方历史交易时段 | 必填 `product, start_date, end_date` |
| `cme_broker_commissions` | 海外经纪商佣金参考 | `product` 对应关联代码；可选 `exchange` |

CN 交易所代码：`CFFEX / SHFE / INE / DCE / CZCE / GFEX`。CME 定义、状态及官方时段品种：`ES / NQ / RTY / YM / GC / CL`。海外佣金表还包含其他海外市场，佣金表缺失某品种时返回空数组，不补造费率。

## 数据解释

- CN `as_of` 选择该日期及以前的已观测版本。乘数、手续费和保证金为 OpenCTP 经纪商参考，不替代本机权威乘数；保留 `historicalExact=false` 和来源字段。
- CN 连续竞价时段使用 `Asia/Shanghai`，是带版本的基础模板，不推断节假日或尚未采集的历史调整。
- CME 定义 `as_of` 为 UTC 当日结束时的已观测定义；没有指定日期时使用数据自身的最新日期，不表示今天仍然有效。
- CME `day` 为 UTC 接收日期；未指定时使用最新完整数据日。午夜快照不代表真实状态切换。合约定义、状态事件不自动转换成官方交易日历。
- CME 官方时段的 `trade_date` 属于 `America/Chicago`；`open_utc`、`close_utc` 返回 UTC，并保留 `segment_index`、来源及置信度。空结果可能是休市或缺少数据，不能仅凭空结果认定休市。
- 海外佣金保留原币种、有效期、收费方向及版本。初始来源未说明生效日期和单边/往返，不能视为历史精确交易所费率。金额使用十进制字符串避免浮点损失。
- 网关不刷新数据。公告与候选规则按已有采集结果返回；只有 `VERIFIED` 规则可执行。

部署核验时（2026-10-13），CN 数据日期为 2026-10-12，CME 定义最新日期为 2026-09-03，状态事件最新完整日期为 2026-09-02。后续以 `exchange_summary` 实时返回为准。

## 维护者运行说明

代码位于 `/home/fanghaizhou/cn_futures_db/reference/exchange_mcp/`，CME 上游为 `/home/fanghaizhou/cn_futures_db/reference_cme` 的现有 Web 服务。

```bash
cd /home/fanghaizhou/cn_futures_db/reference
python3 -m venv .venv-mcp  # Python >= 3.10
.venv-mcp/bin/python -m pip install -r requirements-mcp.txt
sh scripts/run-mcp.sh

# 后台启动 / 更新，仅操作网关进程
pm2 startOrReload ecosystem.mcp.config.cjs
pm2 save
pm2 logs cn-cme-exchange-mcp

# 停止
pm2 stop cn-cme-exchange-mcp
```

验证命令（MCP 测试使用独立依赖环境）：

```bash
.venv-mcp/bin/python -m unittest discover -s tests_mcp -v
.venv/bin/python -m unittest discover -s tests -v
.venv-mcp/bin/python -m pip check
```

部署时已验证全部 12 个 HTTP 查询，以及回环地址和内网 IP 的 MCP 握手、工具发现及实际调用。

| 环境变量 | 默认值 |
| --- | --- |
| `EXCHANGE_MCP_HOST` | `0.0.0.0` |
| `EXCHANGE_MCP_PORT` | `8780` |
| `EXCHANGE_MCP_CN_URL` | `http://127.0.0.1:8765` |
| `EXCHANGE_MCP_CME_URL` | `http://127.0.0.1:8770` |
| `EXCHANGE_MCP_ALLOWED_HOSTS` | 可补充逗号分隔主机名/IP；默认允许 localhost、127.0.0.1、本机主机名及其解析 IP |
| `EXCHANGE_MCP_PYTHON` | 项目的 `.venv-mcp/bin/python` |

MCP 开启 Host/Origin 校验；使用自定义域名访问时需将该域名加入 `EXCHANGE_MCP_ALLOWED_HOSTS`。没有跨域 CORS 开放；网页查询从本网关同源调用。

当前 PM2 配置额外允许 `172.16.30.38`；更换服务器 IP 时同步更新 `ecosystem.mcp.config.cjs`。

网关复用 CN 8765 / CME 8770 的 GET API。CME 官方时段和佣金只读访问指定的 PostgreSQL 16：Unix socket `/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run`、5433、`market_metadata`、`u171` peer 认证；连接 DSN 固定，不接受调用者或环境变量改写实例。连接强制 `default_transaction_read_only=on` 并设置语句超时，不创建、写入或修改任何表。

`/health` 只表示网关存活；检查上游可用性用 `exchange_summary`，其中某一市场出现 `error` 时其他市场仍可查询。官方时段及佣金的数据库可用性需实际调用对应工具检查。

原始 Web 看板仍在 CN `http://127.0.0.1:8765/` 和 CME `http://127.0.0.1:8770/`；统一网关仅提供查询工具，不转发同步、导入、审核或任意 URL/SQL 请求。

协议实现参考：[官方 Python MCP SDK v1 文档](https://py.sdk.modelcontextprotocol.io/v1/)。网关将依赖固定在 v1，以避免 v2 API 变更影响现有运行。
