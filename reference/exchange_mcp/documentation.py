"""面向数据使用者的接口说明；参数定义由运行中的工具注册表生成。"""

import html
import json
from urllib.parse import urlencode


PARAMETERS = {
    "market": "选择市场：all 同时查询 CN 与 CME，或指定 cn / cme。",
    "venue": "国内交易所代码：CFFEX 中金所、SHFE 上期所、INE 能源中心、DCE 大商所、CZCE 郑商所、GFEX 广期所。",
    "product": "品种代码。CN 如 IF、rb；CME 定义/状态/时段支持 ES、NQ、RTY、YM、GC、CL；佣金接口使用 association_code（如 GC）。",
    "search": "合约代码搜索；CN 还可匹配品种代码和合约名称。",
    "as_of": "历史查询日期 YYYY-MM-DD；省略时使用数据最新日期。CN 为该日及以前的最近观测版本；CME 为 UTC 当日结束时的定义。",
    "include_options": "是否包含期权；默认 false，仅查询期货。",
    "active_only": "是否仅返回在所选日期已激活且未到期的 CME 合约；默认 true。",
    "page": "页码，从 1 开始；上限 100000。",
    "page_size": "每页条数，范围 1–200。继续增加 page 获取后续页。",
    "instrument": "国内具体合约代码，如 IF2612。",
    "symbol": "CME 具体合约代码，如 ESU6；建议先查询 cme_status_symbols 获取有效代码。",
    "day": "状态事件的 UTC 接收日期，格式 YYYY-MM-DD；省略时使用该品种最新完整数据日。",
    "limit": "返回的公告数量上限，范围 1–100。",
    "status": "规则审核状态：VERIFIED 已审核、PENDING 待审核、REJECTED 已驳回。默认 VERIFIED。",
    "start_date": "起始 Chicago 交易日，格式 YYYY-MM-DD，包含当天。",
    "end_date": "结束 Chicago 交易日，包含当天；总范围最多 366 天。",
    "start": "国内主力历史的起始交易日 YYYY-MM-DD（含当天），只在指定 product 时使用。",
    "end": "国内主力历史的结束交易日 YYYY-MM-DD（含当天），只在指定 product 时使用。",
    "exchange": "佣金参考表中的交易所代码，如 COMEX、NYMEX、CME、CBOT；省略表示全部。",
}

PAGING = [
    ("total / pages", "整数", "符合条件的记录数 / 总页数；总页数至少为 1，total=0 表示无记录。"),
    ("page / pageSize", "整数", "当前页码 / 每页条数；字段名与请求的 page_size 不同。"),
]
CN_FIELDS = [
    ("records[].venue / instrumentId / productId", "字符串", "交易所 / 合约代码 / 品种代码。"),
    ("records[].contractMultiplier / priceTick", "十进制字符串或空值", "OpenCTP 参考合约乘数 / 最小报价变动单位。"),
    ("records[].fees.open / close / closeToday", "对象", "开仓 / 平仓 / 平今费用参数；rate 为按金额比例，perLot 为每手固定金额，保留来源口径。"),
    ("records[].margin.longRate / shortRate", "十进制字符串或空值", "多头 / 空头保证金比例，例如 0.12 表示 12%。"),
    ("records[].sourceDate / startTs / updateTs", "日期 / UTC 时间", "该合约来源日期 / 该版本首次观测 / 最近确认时间；各合约可能不同。"),
    ("dataState / feeScope / historicalExact", "对象 / 字符串 / 布尔", "数据状态与参考口径；全局最新日期不代表每个合约都在该日更新。"),
]
DEFINITION_FIELDS = [
    ("records[].raw_symbol / asset / exchange", "字符串", "具体合约 / 品种 / 上游交易所代码，例如 ESU6 / ES / XCME。"),
    ("records[].min_price_increment", "数值或空值", "最小报价变动单位。"),
    ("records[].unit_of_measure_qty / unit_of_measure", "数值 / 字符串", "合约计量数量与单位；例如 ES 为 50 / IPNT。"),
    ("records[].tick_value", "数值或空值", "本服务按 min_price_increment × unit_of_measure_qty 计算的每跳价值。"),
    ("records[].currency / activation / expiration", "字符串 / UTC 时间", "合约币种 / 激活时间 / 到期时间。"),
    ("records[].ts_recv / ts_event", "UTC 时间", "定义接收时间 / 来源事件时间。"),
    ("records[].contract_multiplier", "上游原始数值", "保留原始字段，可能为未定义占位值（如样本中的 2147483647），不能直接作为经济乘数。"),
]

TOOL_DOCS = {
    "exchange_summary": {
        "group": "数据范围", "example": {}, "source": "CN 当前版本统计；CME 已解析 Databento GLBX.MDP3 数据统计。",
        "fields": [
            ("markets.cn.as_of / instrument_count / version_count", "日期 / 整数", "国内合约全局最新日期、合约数和累计版本数。"),
            ("markets.cme.definition", "对象", "定义覆盖起止时间、latestDate、原始记录数和合约数。"),
            ("markets.cme.status.coverage / latestCompleteDates", "对象", "股指、商品状态事件覆盖及各自最后完整日期。"),
            ("markets.cn.error / markets.cme.error", "可选字符串", "该数据源不可用；HTTP 200 时也应检查此字段，另一市场仍可正常返回。"),
        ],
    },
    "cn_instruments": {"group": "国内期货", "example": {"venue": "CFFEX", "product": "IF", "page_size": 2}, "source": "OpenCTP 经纪商参考；逐合约增量版本。列表可能包含已到期合约，不等于当前挂牌清单。", "fields": CN_FIELDS + PAGING},
    "cn_instrument": {"group": "国内期货", "example": {"venue": "CFFEX", "instrument": "IF2612"}, "source": "OpenCTP 经纪商参考；逐合约增量版本。", "fields": [
        ("instrument", "对象", "单个合约，字段与 cn_instruments 中的一条 records 记录相同。"),
        ("exchangeInfo", "对象", "该合约的 exchangeInfo 表示，含来源与费用口径。"),
        ("dataState", "对象", "查询所用的增量数据状态。"),
    ]},
    "cn_trading_sessions": {"group": "国内期货", "example": {"venue": "CFFEX", "product": "IF"}, "source": "OpenCTP 连续竞价基础模板，非节假日交易日历。", "fields": [
        ("timezone", "字符串", "Asia/Shanghai。"),
        ("sessions[].venue / product_id / segment_no", "字符串 / 整数", "交易所、品种及连续竞价段号。"),
        ("sessions[].time_begin / time_end", "本地时间", "该段开盘 / 收盘时间，格式 HH:MM:SS。"),
        ("sessions[].crosses_midnight / session_group", "布尔 / 字符串", "是否跨午夜 / DAY 日盘或 NIGHT 夜盘。"),
        ("sessions[].source_date / start_ts / update_ts", "日期 / UTC 时间", "模板来源日期、首次观测及最近确认。"),
    ]},
    "cn_dominant_contracts": {"group": "国内期货", "example": {"product": "RB", "start": "2026-11-01"}, "source": "market_replay 每日盘后按米筐 rule=0 从通联行情日终截面重算的主力合约表（2019-01-01 起）。", "fields": [
        ("latest_trading_day / products[]", "日期 / 对象数组", "不填品种时：已处理的最新交易日，以及每个品种一行。"),
        ("products[].dominant / scheduled_next", "字符串", "trading_day 当天生效的主力 / 当天收盘后排给下一交易日的主力。郑商所为四位年月。"),
        ("products[].rolls_next_day / last_roll_day", "布尔 / 日期", "下一交易日是否换月 / 最近一次换月的交易日。"),
        ("rows[]", "对象数组", "填品种时：trad_day、instrument_id、roll_flag、decision_open_int、scheduled_next、rule 的逐日历史。"),
    ]},
    "cn_announcements": {"group": "国内期货", "example": {"limit": 5}, "source": "已采集的交易所期货公告及已有分析结果；调用不触发刷新。", "fields": [
        ("announcements[]", "对象数组", "公告标题、交易所、原文地址及已有分析结果。"),
        ("rules[] / summary", "数组 / 对象", "关联规则及审核统计。"),
        ("latestRefresh / manualRefreshOnly", "对象或空值 / 布尔", "最近刷新记录 / 是否仅人工刷新。"),
    ]},
    "cn_message_fee_rules": {"group": "国内期货", "example": {"status": "VERIFIED"}, "source": "已保存的申报费规则；无已审核规则时返回空数组。", "fields": [
        ("rules[]", "对象数组", "符合审核状态筛选的规则；空数组表示没有相应记录。"),
        ("rules[].review_status / effective_from", "字符串 / 日期", "审核状态 / 生效日期；只有 VERIFIED 可用于执行。"),
        ("rules[].aggregation_scope / tiers", "字符串 / 数组", "规则统计粒度与费用分档，依规则原始定义解释。"),
    ]},
    "cme_definitions": {"group": "CME", "example": {"product": "ES", "as_of": "2026-09-02", "page_size": 2}, "source": "Databento GLBX.MDP3 历史定义，仅六个品种的单腿期货。", "fields": DEFINITION_FIELDS + PAGING},
    "cme_definition_history": {"group": "CME", "example": {"symbol": "ESU6"}, "source": "Databento GLBX.MDP3 历史定义变更；最多返回最近 50 个变更状态。", "fields": [
        ("symbol / asOf / observations", "字符串 / 日期 / 整数", "合约代码 / 查询截点 / 截点前原始观测条数。"),
        ("record", "对象", "截点前最后一条定义；字段含义同 cme_definitions 的 records。"),
        ("changes[]", "对象数组", "合并重复观测后的最近 50 个变更状态，按时间顺序排列。"),
    ]},
    "cme_status_symbols": {"group": "CME", "example": {"product": "ES", "day": "2026-09-02"}, "source": "结合历史定义，列出选定 UTC 日有状态事件的有效合约。", "fields": [
        ("date / product / total", "日期 / 字符串 / 整数", "UTC 日期 / 品种 / 合约数。"),
        ("records[].symbol / events", "字符串 / 整数", "可用于 cme_status_events 的合约代码 / 事件数。"),
        ("records[].first_event / last_event / expiration", "UTC 时间", "当日首末事件时间 / 合约到期时间。"),
    ]},
    "cme_status_events": {"group": "CME", "example": {"product": "ES", "day": "2026-09-02", "symbol": "ESU6", "page_size": 2}, "source": "Databento 交易状态事件；按 UTC 接收日期过滤。", "fields": [
        ("records[].symbol / ts_recv / ts_event", "字符串 / UTC 时间", "合约 / 接收时间 / 来源事件时间。"),
        ("records[].action_name / action_label", "字符串", "交易状态及中文标签，如 TRADING / 交易中。"),
        ("records[].reason_name / trading_event_name", "字符串", "原因与附加事件，例如 SCHEDULED、NO_CANCEL。"),
        ("records[].is_trading / is_quoting", "来源枚举", "交易 / 报价标记；Y 为是、N 为否，其他值按未定义保留。"),
        ("records[].is_midnight_snapshot", "布尔", "是否为 UTC 午夜快照；true 时不能当作实际状态切换时间。"),
        ("summary", "对象", "筛选范围内事件数、合约数、异常事件数与起止时间。"),
    ] + PAGING},
    "cme_trading_sessions": {"group": "CME", "example": {"product": "ES", "start_date": "2025-11-24", "end_date": "2025-11-28"}, "source": "已入库的官方历史交易时段；交易日按 America/Chicago，时间戳为 UTC。", "fields": [
        ("records[].product / trade_date / segment_index", "字符串 / 日期 / 整数", "品种 / 芝加哥交易日 / 同一交易日内的时段编号。"),
        ("records[].open_utc / close_utc / duration_seconds", "UTC 时间 / 整数", "时段开盘、收盘时间及持续秒数。"),
        ("records[].exception_names / schedule_regimes", "字符串数组", "节假日或特殊安排名称 / 适用的交易时段规则。"),
        ("records[].confidence / source_urls", "字符串 / 数组", "置信度 / 官方来源链接。空结果不能单独证明该日休市。"),
    ]},
    "cme_broker_commissions": {"group": "海外佣金", "example": {"product": "GC"}, "source": "用户提供的海外经纪商佣金清单，覆盖 CME Group 及其他海外市场。", "fields": [
        ("records[].association_code / exchange", "字符串", "清单关联代码 / 交易所；与合约代码及 Databento 品种不能无条件互换。"),
        ("records[].commission_amount / commission_currency", "十进制字符串 / 字符串", "佣金金额及佣金币种；可与 instrument_currency 标的币种不同。"),
        ("records[].charge_basis / charge_side", "字符串", "计费单位及收费方向；UNSPECIFIED 表示未说明单边或往返。"),
        ("records[].effective_from / effective_to", "日期或空值", "有效期；空值表示来源未提供，不代表任意历史日均适用。"),
        ("records[].schedule_version / historical_exact / fee_scope", "字符串 / 布尔", "原始版本及费率口径，保留参考数据标识。"),
    ]},
}


def catalog(registry):
    entries = []
    for name, (function, model, title) in registry.items():
        doc = TOOL_DOCS[name]
        schema = model.model_json_schema()
        for key, prop in schema["properties"].items():
            prop["description"] = PARAMETERS[key]
        entries.append({
            "name": name, "title": title, "description": function.__doc__,
            "inputSchema": schema, "httpMethod": "GET", "httpPath": "/api/" + name,
            "group": doc["group"], "source": doc["source"], "exampleArguments": doc["example"],
            "exampleUrl": "/api/" + name + ("?" + urlencode(doc["example"]) if doc["example"] else ""),
            "responseFields": [{"path": path, "type": kind, "description": description} for path, kind, description in doc["fields"]],
        })
    return {"tools": entries}


def table(headers, rows):
    return '<div class="table-scroll"><table><thead><tr>' + "".join("<th>" + html.escape(h) + "</th>" for h in headers) + "</tr></thead><tbody>" + "".join("<tr>" + "".join("<td>" + html.escape(str(cell)) + "</td>" for cell in row) + "</tr>" for row in rows) + "</tbody></table></div>"


def render_interfaces(registry):
    tools = catalog(registry)["tools"]
    result = ['<div class="table-scroll"><table><thead><tr><th>数据</th><th>接口（GET）与 MCP 工具名</th><th>查看</th></tr></thead><tbody>']
    for item in tools:
        name = item["name"]
        result.append(f'<tr><td>{html.escape(item["title"])}</td><td><code>/api/{name}</code></td><td><a href="#doc-{name}" data-open="doc-{name}">参数与字段</a> · <a href="{html.escape(item["exampleUrl"], quote=True)}" target="_blank" rel="noopener">真实数据</a></td></tr>')
    result.append("</tbody></table></div>")
    for item in tools:
        name = item["name"]
        result.append(f'<details class="interface" id="doc-{name}"><summary>{html.escape(item["title"])} <code>{name}</code></summary>')
        result.append(f'<p>{html.escape(item["description"])}</p><p><strong>来源与范围：</strong>{html.escape(item["source"])}</p>')
        result.append('<h3>请求参数</h3>')
        required = item["inputSchema"].get("required", [])
        rows = []
        for key, prop in item["inputSchema"]["properties"].items():
            kinds = prop.get("anyOf", [prop])
            kind = " / ".join(k.get("format", k.get("type", "")) for k in kinds if k.get("type") != "null")
            default = "必填" if key in required else "省略" if prop.get("default") is None else json.dumps(prop["default"], ensure_ascii=False)
            rows.append((key, kind, default, prop["description"]))
        result.append(table(["参数", "类型", "默认值", "说明"], rows))
        result.append('<h3>返回字段（主要字段，保留其余上游字段）</h3>')
        result.append(table(["字段路径", "类型", "含义"], TOOL_DOCS[name]["fields"]))
        result.append(f'<h3>直接调用</h3><pre data-curl-path="{html.escape(item["exampleUrl"], quote=True)}">curl -fsS \'http://127.0.0.1:8780{html.escape(item["exampleUrl"])}\'</pre>')
        result.append(f'<a href="{html.escape(item["exampleUrl"], quote=True)}" target="_blank" rel="noopener">打开真实 JSON 响应</a> · <a href="#query" data-query="{name}">在页面内试用此接口</a></details>')
    return "".join(result)


def render_markdown(registry):
    """将与网页相同的字段说明导出为可独立转交的 Markdown。"""
    parts = ["## 全部接口、参数与返回字段\n\n以下参数表来自实际服务定义。所有接口均为 GET，MCP 工具名为路径末尾的名称。\n"]
    for item in catalog(registry)["tools"]:
        parts.append(f'### {item["title"]}：`{item["name"]}`\n\n{item["description"]}\n\n来源与范围：{item["source"]}\n')
        parts.append("| 参数 | 必填 | 默认值 | 说明 |\n| --- | --- | --- | --- |")
        required = item["inputSchema"].get("required", [])
        for key, prop in item["inputSchema"]["properties"].items():
            default = "—" if key in required else json.dumps(prop.get("default"), ensure_ascii=False)
            parts.append(f'| `{key}` | {"是" if key in required else "否"} | `{default}` | {prop["description"]} |')
        parts.append("\n返回字段（主要字段，其他上游字段原样保留）：\n\n| 字段路径 | 类型 | 含义 |\n| --- | --- | --- |")
        for field in item["responseFields"]:
            parts.append(f'| `{field["path"]}` | {field["type"]} | {field["description"]} |')
        parts.append(f'\n调用示例：\n\n```bash\ncurl -fsS \'http://127.0.0.1:8780{item["exampleUrl"]}\'\n```\n')
    return "\n".join(parts)


def openapi(registry):
    paths = {}
    for item in catalog(registry)["tools"]:
        schema = item["inputSchema"]
        fields = "\n".join(f'- {f["path"]}: {f["description"]}' for f in item["responseFields"])
        parameters = [{"name": name, "in": "query", "required": name in schema.get("required", []),
                       "description": prop["description"], "schema": prop} for name, prop in schema["properties"].items()]
        paths[item["httpPath"]] = {"get": {
            "operationId": item["name"], "summary": item["title"], "tags": [item["group"]],
            "description": item["description"] + "\n" + item["source"], "parameters": parameters,
            "responses": {
                "200": {"description": "查询结果。主要字段：\n" + fields, "content": {"application/json": {"schema": {"type": "object", "additionalProperties": True}}}},
                "400": {"description": "参数不合法，返回 error 字段。"},
                "404": {"description": "接口或记录不存在。"},
                "502": {"description": "上游 HTTP 服务不可用。"},
                "503": {"description": "CME 数据库查询不可用。"},
            },
        }}
    return {"openapi": "3.1.0", "info": {"title": "CN / CME 参考数据只读接口", "version": "1.0.0"}, "servers": [{"url": "/"}], "paths": paths}
