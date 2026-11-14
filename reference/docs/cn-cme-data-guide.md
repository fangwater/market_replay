# 同机 CN / CME 数据使用说明

本文件面向同一台 Linux 机器上的其他用户及其 AI 助手。它是一份独立的 Markdown 调用说明，可以直接复制到自己的项目或交给 AI 阅读。

数据由已经运行的 HTTP 服务提供，基础地址固定使用 `http://127.0.0.1:8780`。无需访问维护者的目录、连接 PostgreSQL、配置 Databento key 或启动新的服务。本文不要求配置 MCP 客户端。

## 第一步：获取说明并确认可用

在调用者自己的工作目录执行：

```bash
curl -fsS http://127.0.0.1:8780/usage.md -o cn-cme-data-guide.md
curl -fsS http://127.0.0.1:8780/api/exchange_summary
```

第一条保存本说明；第二条查询 CN 与 CME 的实际覆盖和更新日期。服务健康检查为 `GET /health`，只表示网关进程存活；数据是否可用以实际查询为准。

可直接给 AI 这样的任务：

> 先阅读 cn-cme-data-guide.md，使用其中的本机 HTTP 接口查询 CN 的 IF 和 CME 的 ES 合约规格。先检查数据覆盖日期，返回合约代码、最小变动价位、参考乘数或计量数量、每跳价值及来源日期。保留数据原始口径，不把参考手续费解释为历史精确费率。

AI 需要具备执行终端命令或发起 HTTP 请求的工具；Markdown 本身不会发起请求。

## 有什么数据

| 数据 | 范围 | 主要内容与口径 |
| --- | --- | --- |
| CN 合约规格及费用参考 | CFFEX / SHFE / INE / DCE / CZCE / GFEX | 合约、品种、参考乘数、价位、开平和平今手续费、保证金；来自 OpenCTP，逐合约版本化。 |
| CN 连续竞价时段 | 国内期货品种 | 上海时区日盘、夜盘、跨午夜段；是基础模板，不是完整节假日历。 |
| CN 公告与申报费规则 | 已采集的期货公告和规则 | 公告、已有分析、审核状态；无已审核规则时返回空数组，不表示申报费为零。 |
| CME 历史定义 | ES / NQ / RTY / YM / GC / CL | Databento GLBX.MDP3，单腿期货的最小价位、计量数量、币种、激活和到期时间、历史变更。 |
| CME 状态事件 | 同上六品种 | 交易状态、原因、附加事件；按 UTC 接收日期查询，午夜快照显式标记。 |
| CME 官方历史时段 | 同上六品种 | 按 Chicago 交易日返回 UTC 开收盘时间、分段、来源及置信度。 |
| 海外经纪商佣金参考 | CME Group 及其他海外市场 | 清单关联代码、金额、币种、收费方向、有效期和版本。 |

这些接口不提供实时行情或订单执行。CN 合约列表保留已观测合约，可能包含已到期合约，不等于当前挂牌清单。

2026-10-13 核验时：CN 合约全局最新日期为 2026-10-12；CME 定义覆盖 2020-01-01 至 2026-09-03；股指状态从 2024-01-01 开始，GC/CL 状态从 2020-01-01 开始，最新完整状态日均为 2026-09-02。以后使用时应重新调用 `exchange_summary`，不能将这组日期当作永久范围。

## HTTP 调用规则

- 所有数据查询均为 `GET /api/{工具名}`；参数放在 URL 查询串中，返回 UTF-8 JSON。
- 日期为 `YYYY-MM-DD`；布尔为 `true` / `false`。可选参数不需要时直接省略，不要发送字符串 `null`。
- 分页接口返回 `records`、`total`、`pages`、`page`、`pageSize`。请求参数叫 `page_size`，范围 1–200；页码从 1 开始。按 `pages` 获取后续页。
- 单个 CN 合约返回 `instrument`；CN 时段返回 `sessions`；规则返回 `rules`；CME 定义变更返回 `record` 和 `changes`。不要假定全部接口均返回 `records`。
- 金额、比例可能为十进制字符串；需要计算时用 Python `decimal.Decimal` 等十进制类型。`null` 为缺失，不能自动当作零。
- HTTP 400 为参数不合法；404 为接口或记录不存在；502 为上游 HTTP 服务异常；503 为 CME 数据库查询不可用。失败时检查响应中的 `error`。
- `exchange_summary` 即使 HTTP 200，也应检查 `markets.cn.error` 和 `markets.cme.error`，单个市场失败不影响另一个返回。

## 直接调用示例

```bash
# CN 合约及手续费、保证金参考
curl -fsS 'http://127.0.0.1:8780/api/cn_instruments?venue=CFFEX&product=IF&page_size=2'

# CN 连续竞价时段
curl -fsS 'http://127.0.0.1:8780/api/cn_trading_sessions?venue=CFFEX&product=IF'

# CME 历史合约定义
curl -fsS 'http://127.0.0.1:8780/api/cme_definitions?product=ES&as_of=2026-09-02&page_size=2'

# CME 状态事件
curl -fsS 'http://127.0.0.1:8780/api/cme_status_events?product=ES&day=2026-09-02&symbol=ESU6&page_size=10'

# CME 官方历史时段
curl -fsS 'http://127.0.0.1:8780/api/cme_trading_sessions?product=ES&start_date=2025-11-24&end_date=2025-11-28'

# 海外经纪商佣金参考
curl -fsS 'http://127.0.0.1:8780/api/cme_broker_commissions?product=GC'
```

## Python 示例（仅标准库）

```python
import json
from urllib.parse import urlencode
from urllib.request import urlopen

BASE = "http://127.0.0.1:8780"

def query(tool, **params):
    params = {k: str(v).lower() if isinstance(v, bool) else v
              for k, v in params.items() if v is not None}
    url = BASE + "/api/" + tool + "?" + urlencode(params)
    with urlopen(url, timeout=65) as response:
        return json.load(response)

print(query("exchange_summary"))
cn = query("cn_instruments", venue="CFFEX", product="IF", page_size=2)
for row in cn["records"]:
    print(row["instrumentId"], row["priceTick"], row["fees"], row["sourceDate"])

page = 1
while True:
    result = query("cme_definitions", product="ES", page=page, page_size=50)
    for row in result["records"]:
        print(row["raw_symbol"], row["min_price_increment"], row["tick_value"])
    if page >= result["pages"]:
        break
    page += 1
```

## 读取结果时必须保留的含义

1. CN `as_of` 是选取该日及以前已观测的版本，不是保证历史精确。全局最新日期不代表每个合约都在当天更新；同时检查每条记录的 `sourceDate`、`startTs`、`updateTs`。
2. CN 乘数、手续费及保证金属于经纪商参考；保留 `feeScope`、`historicalExact`、来源字段，不替代本机权威乘数表。
3. CME 定义的 `as_of` 截点为 UTC 当日结束；省略日期时使用数据最新日期，不代表今天仍有效。`active_only=true` 相对于所选日期筛选。
4. CME 定义中的 `contract_multiplier` 是上游原始字段，样本中可出现 `2147483647` 这类占位值，不可直接作为经济乘数。`tick_value` 是本服务按 `min_price_increment × unit_of_measure_qty` 得到的值。
5. CME 状态 `day` 按 UTC 接收日期筛选；`is_midnight_snapshot=true` 是午夜快照，不应当作真实状态切换。
6. CME 官方时段的 `trade_date` 按 `America/Chicago`；开收盘时间为 UTC，保留同日多段的 `segment_index`。空结果可能表示无数据或休市，不能仅据此认定休市。
7. 海外佣金的有效期或收费方向可能未说明。`charge_side=UNSPECIFIED` 不得自行解释为单边或往返；不能把经纪商参考表解释成历史精确的交易所收费表。

## MCP 客户端（可选）

MCP 是工具调用协议，不是 Markdown 文件。本文是说明文件，HTTP 服务负责返回数据；需要客户端自动发现工具时，可以配置已有的 MCP 入口：

- 名称：`cn-cme-exchange-reference`
- 传输：`Streamable HTTP`
- 地址：`http://127.0.0.1:8780/mcp`
- 认证：无

连接后 `tools/list` 返回本文列出的 12 个工具。`resources/list` 返回 `exchange-reference://guide`（本说明）和 `exchange-reference://catalog`（中文参数及字段目录）。浏览器打开 `/mcp` 只显示接入说明，不建立 MCP 会话。

## 辅助入口

- 网页文档及在线查询：`http://127.0.0.1:8780/docs`
- 本 Markdown：`http://127.0.0.1:8780/usage.md`
- 机器可读参数、示例及字段目录：`http://127.0.0.1:8780/api/tools`
- OpenAPI 定义：`http://127.0.0.1:8780/openapi.json`

## 全部接口、参数与返回字段

以下参数表来自实际服务定义。所有接口均为 GET，MCP 工具名为路径末尾的名称。

### 数据范围与更新日期：`exchange_summary`

查看数据实际覆盖日期；单个数据源故障不会阻止另一个返回。CME 最新完整状态日可能早于定义日。

来源与范围：CN 当前版本统计；CME 已解析 Databento GLBX.MDP3 数据统计。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `market` | 否 | `"all"` | 选择市场：all 同时查询 CN 与 CME，或指定 cn / cme。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `markets.cn.as_of / instrument_count / version_count` | 日期 / 整数 | 国内合约全局最新日期、合约数和累计版本数。 |
| `markets.cme.definition` | 对象 | 定义覆盖起止时间、latestDate、原始记录数和合约数。 |
| `markets.cme.status.coverage / latestCompleteDates` | 对象 | 股指、商品状态事件覆盖及各自最后完整日期。 |
| `markets.cn.error / markets.cme.error` | 可选字符串 | 该数据源不可用；HTTP 200 时也应检查此字段，另一市场仍可正常返回。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/exchange_summary'
```

### 国内期货合约列表：`cn_instruments`

分页查询 CN 合约规格和手续费，默认仅期货。as_of 取该日以前最近观测版本；非历史精确费率。

来源与范围：OpenCTP 经纪商参考；逐合约增量版本。列表可能包含已到期合约，不等于当前挂牌清单。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `venue` | 否 | `null` | 国内交易所代码：CFFEX 中金所、SHFE 上期所、INE 能源中心、DCE 大商所、CZCE 郑商所、GFEX 广期所。 |
| `product` | 否 | `null` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `search` | 否 | `null` | 合约代码搜索；CN 还可匹配品种代码和合约名称。 |
| `as_of` | 否 | `null` | 历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。 |
| `include_options` | 否 | `false` | 是否包含期权；默认 false，仅查询期货。 |
| `page` | 否 | `1` | 页码，从 1 开始；上限 100000。 |
| `page_size` | 否 | `50` | 每页条数，范围 1–200。继续增加 page 获取后续页。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `records[].venue / instrumentId / productId` | 字符串 | 交易所 / 合约代码 / 品种代码。 |
| `records[].contractMultiplier / priceTick` | 十进制字符串或空值 | OpenCTP 参考合约乘数 / 最小报价变动单位。 |
| `records[].fees.open / close / closeToday` | 对象 | 开仓 / 平仓 / 平今费用参数；rate 为按金额比例，perLot 为每手固定金额，保留来源口径。 |
| `records[].margin.longRate / shortRate` | 十进制字符串或空值 | 多头 / 空头保证金比例，例如 0.12 表示 12%。 |
| `records[].sourceDate / startTs / updateTs` | 日期 / UTC 时间 | 该合约来源日期 / 该版本首次观测 / 最近确认时间；各合约可能不同。 |
| `dataState / feeScope / historicalExact` | 对象 / 字符串 / 布尔 | 数据状态与参考口径；全局最新日期不代表每个合约都在该日更新。 |
| `total / pages` | 整数 | 符合条件的记录数 / 总页数；总页数至少为 1，total=0 表示无记录。 |
| `page / pageSize` | 整数 | 当前页码 / 每页条数；字段名与请求的 page_size 不同。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cn_instruments?venue=CFFEX&product=IF&page_size=2'
```

### 国内单个合约详情：`cn_instrument`

查询单个 CN 合约及 exchangeInfo，包括最小变动价位、经纪商参考乘数、手续费和保证金。

来源与范围：OpenCTP 经纪商参考；逐合约增量版本。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `venue` | 是 | `—` | 国内交易所代码：CFFEX 中金所、SHFE 上期所、INE 能源中心、DCE 大商所、CZCE 郑商所、GFEX 广期所。 |
| `instrument` | 是 | `—` | 国内具体合约代码，如 IF2612。 |
| `as_of` | 否 | `null` | 历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `instrument` | 对象 | 单个合约，字段与 cn_instruments 中的一条 records 记录相同。 |
| `exchangeInfo` | 对象 | 该合约的 exchangeInfo 表示，含来源与费用口径。 |
| `dataState` | 对象 | 查询所用的增量数据状态。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cn_instrument?venue=CFFEX&instrument=IF2612'
```

### 国内连续竞价时段：`cn_trading_sessions`

查询 Asia/Shanghai 连续竞价基础模板，含跨午夜字段。模板不能推断节假日及未采集的历史调整。

来源与范围：OpenCTP 连续竞价基础模板，非节假日交易日历。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `venue` | 否 | `null` | 国内交易所代码：CFFEX 中金所、SHFE 上期所、INE 能源中心、DCE 大商所、CZCE 郑商所、GFEX 广期所。 |
| `product` | 否 | `null` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `as_of` | 否 | `null` | 历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `timezone` | 字符串 | Asia/Shanghai。 |
| `sessions[].venue / product_id / segment_no` | 字符串 / 整数 | 交易所、品种及连续竞价段号。 |
| `sessions[].time_begin / time_end` | 本地时间 | 该段开盘 / 收盘时间，格式 HH:MM:SS。 |
| `sessions[].crosses_midnight / session_group` | 布尔 / 字符串 | 是否跨午夜 / DAY 日盘或 NIGHT 夜盘。 |
| `sessions[].source_date / start_ts / update_ts` | 日期 / UTC 时间 | 模板来源日期、首次观测及最近确认。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cn_trading_sessions?venue=CFFEX&product=IF'
```

### 国内交易所公告：`cn_announcements`

读取已采集的公告、分析和规则，不触发采集或 AI 分析。

来源与范围：已采集的交易所期货公告及已有分析结果；调用不触发刷新。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `venue` | 否 | `null` | 国内交易所代码：CFFEX 中金所、SHFE 上期所、INE 能源中心、DCE 大商所、CZCE 郑商所、GFEX 广期所。 |
| `limit` | 否 | `20` | 返回的公告数量上限，范围 1–100。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `announcements[]` | 对象数组 | 公告标题、交易所、原文地址及已有分析结果。 |
| `rules[] / summary` | 数组 / 对象 | 关联规则及审核统计。 |
| `latestRefresh / manualRefreshOnly` | 对象或空值 / 布尔 | 最近刷新记录 / 是否仅人工刷新。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cn_announcements?limit=5'
```

### 国内申报费规则：`cn_message_fee_rules`

默认只返回已审核申报费规则；待审核规则不可用于费用计算。

来源与范围：已保存的申报费规则；无已审核规则时返回空数组。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `status` | 否 | `"VERIFIED"` | 规则审核状态：VERIFIED 已审核、PENDING 待审核、REJECTED 已驳回。默认 VERIFIED。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `rules[]` | 对象数组 | 符合审核状态筛选的规则；空数组表示没有相应记录。 |
| `rules[].review_status / effective_from` | 字符串 / 日期 | 审核状态 / 生效日期；只有 VERIFIED 可用于执行。 |
| `rules[].aggregation_scope / tiers` | 字符串 / 数组 | 规则统计粒度与费用分档，依规则原始定义解释。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cn_message_fee_rules?status=VERIFIED'
```

### CME 历史合约定义列表：`cme_definitions`

分页查询 ES/NQ/RTY/YM/GC/CL 的历史定义、最小变动价位及合约规格；日期截点为 UTC 当日结束。

来源与范围：Databento GLBX.MDP3 历史定义，仅六个品种的单腿期货。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `product` | 否 | `null` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `as_of` | 否 | `null` | 历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。 |
| `search` | 否 | `null` | 合约代码搜索；CN 还可匹配品种代码和合约名称。 |
| `active_only` | 否 | `true` | 是否仅返回在所选日期已激活且未到期的 CME 合约；默认 true。 |
| `page` | 否 | `1` | 页码，从 1 开始；上限 100000。 |
| `page_size` | 否 | `50` | 每页条数，范围 1–200。继续增加 page 获取后续页。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `records[].raw_symbol / asset / exchange` | 字符串 | 具体合约 / 品种 / 上游交易所代码，例如 ESU6 / ES / XCME。 |
| `records[].min_price_increment` | 数值或空值 | 最小报价变动单位。 |
| `records[].unit_of_measure_qty / unit_of_measure` | 数值 / 字符串 | 合约计量数量与单位；例如 ES 为 50 / IPNT。 |
| `records[].tick_value` | 数值或空值 | 本服务按 min_price_increment × unit_of_measure_qty 计算的每跳价值。 |
| `records[].currency / activation / expiration` | 字符串 / UTC 时间 | 合约币种 / 激活时间 / 到期时间。 |
| `records[].ts_recv / ts_event` | UTC 时间 | 定义接收时间 / 来源事件时间。 |
| `records[].contract_multiplier` | 上游原始数值 | 保留原始字段，可能为未定义占位值（如样本中的 2147483647），不能直接作为经济乘数。 |
| `total / pages` | 整数 | 符合条件的记录数 / 总页数；总页数至少为 1，total=0 表示无记录。 |
| `page / pageSize` | 整数 | 当前页码 / 每页条数；字段名与请求的 page_size 不同。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_definitions?product=ES&as_of=2026-09-02&page_size=2'
```

### CME 合约定义变更：`cme_definition_history`

查询具体合约（如 ESU6）的最后定义及最近 50 个变更状态；as_of 默认最新已采集日期。

来源与范围：Databento GLBX.MDP3 历史定义变更；最多返回最近 50 个变更状态。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `symbol` | 是 | `—` | CME 具体合约代码，如 ESU6；建议先查询 cme_status_symbols 获取有效代码。 |
| `as_of` | 否 | `null` | 历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `symbol / asOf / observations` | 字符串 / 日期 / 整数 | 合约代码 / 查询截点 / 截点前原始观测条数。 |
| `record` | 对象 | 截点前最后一条定义；字段含义同 cme_definitions 的 records。 |
| `changes[]` | 对象数组 | 合并重复观测后的最近 50 个变更状态，按时间顺序排列。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_definition_history?symbol=ESU6'
```

### CME 可查询状态的合约：`cme_status_symbols`

列出指定 UTC 接收日期有状态事件的有效合约；day 默认该品种最新完整数据日。

来源与范围：结合历史定义，列出选定 UTC 日有状态事件的有效合约。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `product` | 是 | `—` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `day` | 否 | `null` | 状态事件的 UTC 接收日期，格式 YYYY-MM-DD；省略时使用该品种最新完整数据日。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `date / product / total` | 日期 / 字符串 / 整数 | UTC 日期 / 品种 / 合约数。 |
| `records[].symbol / events` | 字符串 / 整数 | 可用于 cme_status_events 的合约代码 / 事件数。 |
| `records[].first_event / last_event / expiration` | UTC 时间 | 当日首末事件时间 / 合约到期时间。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_status_symbols?product=ES&day=2026-09-02'
```

### CME 交易状态事件：`cme_status_events`

按 UTC 接收日期分页查询开收盘、停盘、禁撤等事件。午夜快照不是实际状态切换；事件流不是官方日历。

来源与范围：Databento 交易状态事件；按 UTC 接收日期过滤。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `product` | 是 | `—` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `day` | 否 | `null` | 状态事件的 UTC 接收日期，格式 YYYY-MM-DD；省略时使用该品种最新完整数据日。 |
| `symbol` | 否 | `null` | CME 具体合约代码，如 ESU6；建议先查询 cme_status_symbols 获取有效代码。 |
| `page` | 否 | `1` | 页码，从 1 开始；上限 100000。 |
| `page_size` | 否 | `100` | 每页条数，范围 1–200。继续增加 page 获取后续页。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `records[].symbol / ts_recv / ts_event` | 字符串 / UTC 时间 | 合约 / 接收时间 / 来源事件时间。 |
| `records[].action_name / action_label` | 字符串 | 交易状态及中文标签，如 TRADING / 交易中。 |
| `records[].reason_name / trading_event_name` | 字符串 | 原因与附加事件，例如 SCHEDULED、NO_CANCEL。 |
| `records[].is_trading / is_quoting` | 来源枚举 | 交易 / 报价标记；Y 为是、N 为否，其他值按未定义保留。 |
| `records[].is_midnight_snapshot` | 布尔 | 是否为 UTC 午夜快照；true 时不能当作实际状态切换时间。 |
| `summary` | 对象 | 筛选范围内事件数、合约数、异常事件数与起止时间。 |
| `total / pages` | 整数 | 符合条件的记录数 / 总页数；总页数至少为 1，total=0 表示无记录。 |
| `page / pageSize` | 整数 | 当前页码 / 每页条数；字段名与请求的 page_size 不同。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_status_events?product=ES&day=2026-09-02&symbol=ESU6&page_size=2'
```

### CME 官方历史交易时段：`cme_trading_sessions`

按 Chicago trade_date 查询已入库的官方 session，最多 366 天，保留停盘分段、来源及置信度。空结果不代表官方休市。

来源与范围：已入库的官方历史交易时段；交易日按 America/Chicago，时间戳为 UTC。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `product` | 是 | `—` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `start_date` | 是 | `—` | 起始 Chicago 交易日，格式 YYYY-MM-DD，包含当天。 |
| `end_date` | 是 | `—` | 结束 Chicago 交易日，包含当天；总范围最多 366 天。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `records[].product / trade_date / segment_index` | 字符串 / 日期 / 整数 | 品种 / 芝加哥交易日 / 同一交易日内的时段编号。 |
| `records[].open_utc / close_utc / duration_seconds` | UTC 时间 / 整数 | 时段开盘、收盘时间及持续秒数。 |
| `records[].exception_names / schedule_regimes` | 字符串数组 | 节假日或特殊安排名称 / 适用的交易时段规则。 |
| `records[].confidence / source_urls` | 字符串 / 数组 | 置信度 / 官方来源链接。空结果不能单独证明该日休市。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_trading_sessions?product=ES&start_date=2025-11-24&end_date=2025-11-28'
```

### 海外期货经纪商佣金参考：`cme_broker_commissions`

读取海外佣金参考表（含 CME Group 及其他海外市场）。product 为 association_code，保留币种、有效期和收费方向；未注明单边/往返不能自行推断。

来源与范围：用户提供的海外经纪商佣金清单，覆盖 CME Group 及其他海外市场。

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `product` | 否 | `null` | 品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。 |
| `exchange` | 否 | `null` | 佣金参考表中的交易所代码，如 COMEX、NYMEX、CME、CBOT；省略表示全部。 |

返回字段（主要字段，其他上游字段原样保留）：

| 字段路径 | 类型 | 含义 |
| --- | --- | --- |
| `records[].association_code / exchange` | 字符串 | 清单关联代码 / 交易所；与合约代码及 Databento 品种不能无条件互换。 |
| `records[].commission_amount / commission_currency` | 十进制字符串 / 字符串 | 佣金金额及佣金币种；可与 instrument_currency 标的币种不同。 |
| `records[].charge_basis / charge_side` | 字符串 | 计费单位及收费方向；UNSPECIFIED 表示未说明单边或往返。 |
| `records[].effective_from / effective_to` | 日期或空值 | 有效期；空值表示来源未提供，不代表任意历史日均适用。 |
| `records[].schedule_version / historical_exact / fee_scope` | 字符串 / 布尔 | 原始版本及费率口径，保留参考数据标识。 |

调用示例：

```bash
curl -fsS 'http://127.0.0.1:8780/api/cme_broker_commissions?product=GC'
```

