"""复用既有 HTTP 查询，以相同参数提供 MCP tools 和普通 GET API。"""

import asyncio
import inspect
import json
import logging
import os
import socket
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal, get_type_hints
from urllib.parse import quote, urlsplit

import httpx
import psycopg
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import ConfigDict, Field, ValidationError, create_model
from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from . import database, documentation


ROOT = Path(__file__).resolve().parent.parent
UPSTREAMS = {
    "cn": os.environ.get("EXCHANGE_MCP_CN_URL", "http://127.0.0.1:8765").rstrip("/"),
    "cme": os.environ.get("EXCHANGE_MCP_CME_URL", "http://127.0.0.1:8770").rstrip("/"),
    # market_replay cn-kline-server HTTP (Rust); dominant contracts from the daily roll replay.
    "cnmarket": os.environ.get("EXCHANGE_MCP_CN_MARKET_URL", "http://127.0.0.1:18313").rstrip("/"),
}
for _url in UPSTREAMS.values():
    _parts = urlsplit(_url)
    if _parts.scheme not in ("http", "https") or not _parts.hostname or _parts.query or _parts.fragment:
        raise ValueError("上游地址必须是 HTTP/HTTPS 基础 URL")

_hosts = {"localhost", "127.0.0.1", socket.gethostname()}
try:
    _hosts.update(socket.gethostbyname_ex(socket.gethostname())[2])
except OSError:
    pass
_hosts.update(filter(None, os.environ.get("EXCHANGE_MCP_ALLOWED_HOSTS", "").split(",")))
mcp = FastMCP(
    "cn-cme-exchange-reference",
    instructions=(
        "查询中国期货与 CME 参考数据。先用 exchange_summary 查看覆盖日期。"
        "CN 手续费及乘数属于 OpenCTP 经纪商参考，CN 时段是基础模板；"
        "CME status 日期按 UTC 接收日期，官方 session 的 trade_date 按 America/Chicago。"
        "CME 佣金参考不可解释为历史精确费率。所有工具只读。"
    ),
    host=os.environ.get("EXCHANGE_MCP_HOST", "0.0.0.0"),
    port=int(os.environ.get("EXCHANGE_MCP_PORT", "8780")),
    stateless_http=True, json_response=True,
    transport_security=TransportSecuritySettings(
        allowed_hosts=[f"{host}:*" for host in sorted(_hosts)],
        allowed_origins=[f"http://{host}:*" for host in sorted(_hosts)],
    ),
)
REGISTRY = {}
Page = Annotated[int, Field(ge=1, le=100000)]
PageSize = Annotated[int, Field(ge=1, le=200)]
Code = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.+\-]+$")]
Product = Literal["ES", "NQ", "RTY", "YM", "GC", "CL"]
Venue = Literal["CFFEX", "SHFE", "INE", "DCE", "CZCE", "GFEX"]


class QueryError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def tool(title):
    """两个协议共享同一函数和参数校验；HTTP 不接受任意路径或 SQL。"""
    def register(function):
        signature = inspect.signature(function)
        hints = get_type_hints(function, include_extras=True)
        fields = {
            name: (hints[name], ... if param.default is inspect.Parameter.empty else param.default)
            for name, param in signature.parameters.items()
        }
        model = create_model(function.__name__ + "Arguments", __config__=ConfigDict(extra="forbid"), **fields)
        REGISTRY[function.__name__] = (function, model, title)
        mcp.tool(title=title, structured_output=True, annotations=ToolAnnotations(
            readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False,
        ))(function)
        return function
    return register


async def get_json(source, path, **params):
    params = {
        key: str(value).lower() if isinstance(value, bool) else value.isoformat() if isinstance(value, date) else value
        for key, value in params.items() if value is not None
    }
    try:
        async with httpx.AsyncClient(timeout=60, trust_env=False, follow_redirects=False) as client:
            response = await client.get(UPSTREAMS[source] + "/api/v1/" + path, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        if status in (400, 404):
            raise QueryError(f"{source.upper()} 查询参数无效或未找到记录，请检查日期和代码", status) from error
        raise QueryError(f"{source.upper()} 数据服务返回异常，请检查其运行状态", 502) from error
    except (httpx.RequestError, ValueError) as error:
        raise QueryError(f"{source.upper()} 数据服务不可用或返回格式无效", 502) from error


async def pg_query(function, *args):
    try:
        return await asyncio.to_thread(function, *args)
    except psycopg.Error as error:
        logging.getLogger(__name__).exception("CME PostgreSQL 只读查询失败")
        raise QueryError("CME 数据库查询不可用，请联系服务维护者", 503) from error


@tool("数据范围与更新日期")
async def exchange_summary(market: Literal["all", "cn", "cme"] = "all") -> dict[str, Any]:
    """查看数据实际覆盖日期；单个数据源故障不会阻止另一个返回。CME 最新完整状态日可能早于定义日。"""
    async def fetch(source):
        try:
            return await get_json(source, "data-state" if source == "cn" else "summary")
        except QueryError as error:
            return {"error": str(error), "status": error.status}
    sources = ["cn", "cme"] if market == "all" else [market]
    results = await asyncio.gather(*(fetch(source) for source in sources))
    return {"markets": dict(zip(sources, results)), "readOnly": True}


@tool("国内期货合约列表")
async def cn_instruments(
    venue: Venue | None = None, product: Code | None = None,
    search: Annotated[str, Field(max_length=64)] | None = None,
    as_of: date | None = None, include_options: bool = False,
    page: Page = 1, page_size: PageSize = 50,
) -> dict[str, Any]:
    """分页查询 CN 合约规格和手续费，默认仅期货。as_of 取该日以前最近观测版本；非历史精确费率。"""
    data = await get_json("cn", "overview", venue=venue, product=product, search=search,
                          asOf=as_of, includeOptions=include_options)
    records = sorted(
        (record for exchange in data["exchanges"] for record in exchange["instruments"]),
        key=lambda record: (record["venue"], record["instrumentId"]),
    )
    total = len(records)
    return {key: data[key] for key in ("dataState", "asOf", "historicalExact", "feeScope", "sourceType")} | {
        "total": total, "page": page, "pageSize": page_size,
        "pages": max(1, (total + page_size - 1) // page_size),
        "records": records[(page - 1) * page_size:page * page_size],
    }


@tool("国内单个合约详情")
async def cn_instrument(venue: Venue, instrument: Code, as_of: date | None = None) -> dict[str, Any]:
    """查询单个 CN 合约及 exchangeInfo，包括最小变动价位、经纪商参考乘数、手续费和保证金。"""
    return await get_json("cn", f"instruments/{quote(venue, safe='')}/{quote(instrument, safe='')}", asOf=as_of)


@tool("国内连续竞价时段")
async def cn_trading_sessions(
    venue: Venue | None = None, product: Code | None = None, as_of: date | None = None,
) -> dict[str, Any]:
    """查询 Asia/Shanghai 连续竞价基础模板，含跨午夜字段。模板不能推断节假日及未采集的历史调整。"""
    return await get_json("cn", "trading-sessions", venue=venue, product=product, asOf=as_of)


@tool("国内主力合约")
async def cn_dominant_contracts(
    product: Code | None = None, start: date | None = None, end: date | None = None,
) -> dict[str, Any]:
    """不填品种时返回每个品种最新交易日的主力合约与收盘后排给下一交易日的合约；填品种时返回该品种的逐日主力历史。规则为米筐 rule=0（持仓 1.1 倍、只向更远月切换、不切回），每日盘后由 market_replay 更新。"""
    if product is None:
        if start is not None or end is not None:
            raise QueryError("start/end 只在指定 product 时可用")
        return await get_json("cnmarket", "dominant")
    return await get_json("cnmarket", f"dominant/{quote(product, safe='')}", start=start, end=end)


@tool("国内交易所公告")
async def cn_announcements(venue: Venue | None = None, limit: Annotated[int, Field(ge=1, le=100)] = 20) -> dict[str, Any]:
    """读取已采集的公告、分析和规则，不触发采集或 AI 分析。"""
    return await get_json("cn", "announcements", venue=venue, limit=limit)


@tool("国内申报费规则")
async def cn_message_fee_rules(status: Literal["VERIFIED", "PENDING", "REJECTED"] = "VERIFIED") -> dict[str, Any]:
    """默认只返回已审核申报费规则；待审核规则不可用于费用计算。"""
    return await get_json("cn", "message-fee-rules", status=status)


@tool("CME 历史合约定义列表")
async def cme_definitions(
    product: Product | None = None, as_of: date | None = None,
    search: Annotated[str, Field(max_length=64)] | None = None,
    active_only: bool = True, page: Page = 1, page_size: PageSize = 50,
) -> dict[str, Any]:
    """分页查询 ES/NQ/RTY/YM/GC/CL 的历史定义、最小变动价位及合约规格；日期截点为 UTC 当日结束。"""
    return await get_json("cme", "definitions", product=product, asOf=as_of, search=search,
                          activeOnly=active_only, page=page, pageSize=page_size)


@tool("CME 合约定义变更")
async def cme_definition_history(symbol: Code, as_of: date | None = None) -> dict[str, Any]:
    """查询具体合约（如 ESU6）的最后定义及最近 50 个变更状态；as_of 默认最新已采集日期。"""
    return await get_json("cme", "definitions/" + quote(symbol, safe=""), asOf=as_of)


@tool("CME 可查询状态的合约")
async def cme_status_symbols(product: Product, day: date | None = None) -> dict[str, Any]:
    """列出指定 UTC 接收日期有状态事件的有效合约；day 默认该品种最新完整数据日。"""
    return await get_json("cme", "status/symbols", product=product, date=day)


@tool("CME 交易状态事件")
async def cme_status_events(
    product: Product, day: date | None = None, symbol: Code | None = None,
    page: Page = 1, page_size: PageSize = 100,
) -> dict[str, Any]:
    """按 UTC 接收日期分页查询开收盘、停盘、禁撤等事件。午夜快照不是实际状态切换；事件流不是官方日历。"""
    return await get_json("cme", "status", product=product, date=day, symbol=symbol, page=page, pageSize=page_size)


@tool("CME 官方历史交易时段")
async def cme_trading_sessions(product: Product, start_date: date, end_date: date) -> dict[str, Any]:
    """按 Chicago trade_date 查询已入库的官方 session，最多 366 天，保留停盘分段、来源及置信度。空结果不代表官方休市。"""
    if not 0 <= (end_date - start_date).days <= 365:
        raise QueryError("结束日期须不早于开始日期，查询范围最多 366 天")
    records = await pg_query(database.sessions, product, start_date, end_date)
    return {
        "product": product, "startDate": start_date.isoformat(), "endDate": end_date.isoformat(),
        "tradeDateTimezone": "America/Chicago", "timestampTimezone": "UTC",
        "source": "public.cme_six_product_trading_sessions",
        "total": len(records), "records": records,
    }


@tool("海外期货经纪商佣金参考")
async def cme_broker_commissions(
    product: Annotated[str, Field(min_length=1, max_length=64)] | None = None,
    exchange: Annotated[str, Field(min_length=1, max_length=32)] | None = None,
) -> dict[str, Any]:
    """读取海外佣金参考表（含 CME Group 及其他海外市场）。product 为 association_code，保留币种、有效期和收费方向；未注明单边/往返不能自行推断。"""
    records = await pg_query(database.commissions, product, exchange)
    return {"source": "public.overseas_futures_broker_commission_reference", "total": len(records), "records": records}


@mcp.custom_route("/", methods=["GET"])
@mcp.custom_route("/docs", methods=["GET"])
async def home(request: Request):
    template = Path(__file__).with_name("index.html").read_text(encoding="utf-8")
    return HTMLResponse(template.replace("{{INTERFACES}}", documentation.render_interfaces(REGISTRY)), headers={"Cache-Control": "no-store"})


@mcp.custom_route("/usage.md", methods=["GET"])
async def usage(request: Request):
    return FileResponse(ROOT / "docs" / "cn-cme-data-guide.md", media_type="text/plain; charset=utf-8", headers={"Cache-Control": "no-store"})


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request):
    return JSONResponse({"status": "ok", "service": "cn-cme-exchange-reference", "readOnly": True})


@mcp.custom_route("/api/tools", methods=["GET"])
async def catalog(request: Request):
    return JSONResponse(documentation.catalog(REGISTRY))


@mcp.custom_route("/openapi.json", methods=["GET"])
async def openapi_document(request: Request):
    return JSONResponse(documentation.openapi(REGISTRY))


@mcp.resource("exchange-reference://guide", name="使用说明", mime_type="text/markdown")
def usage_resource() -> str:
    """数据范围、HTTP 与 MCP 接入方法、日期及来源口径。"""
    return (ROOT / "docs" / "cn-cme-data-guide.md").read_text(encoding="utf-8")


@mcp.resource("exchange-reference://catalog", name="接口与数据字段", mime_type="application/json")
def catalog_resource() -> str:
    """全部接口的中文参数说明、示例和返回字段字典。"""
    return json.dumps(documentation.catalog(REGISTRY), ensure_ascii=False)


@mcp.custom_route("/api/{tool_name}", methods=["GET"])
async def http_query(request: Request):
    entry = REGISTRY.get(request.path_params["tool_name"])
    if entry is None:
        return JSONResponse({"error": "查询接口不存在，请查看 /api/tools"}, status_code=404)
    function, model, _ = entry
    try:
        if len(request.query_params) != len(request.query_params.multi_items()):
            raise QueryError("同一查询参数不能重复传入")
        arguments = model.model_validate(dict(request.query_params))
        return JSONResponse(await function(**dict(arguments)))
    except ValidationError:
        return JSONResponse({"error": "参数无效，请按 /api/tools 中的 inputSchema 提供参数"}, status_code=400)
    except QueryError as error:
        return JSONResponse({"error": str(error)}, status_code=error.status)


class McpBrowserEntry:
    """浏览器导航进入说明页；协议 POST 和事件流 GET 原样交给 SDK。"""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["method"] == "GET" and scope["path"] in ("/mcp", "/mcp/"):
            request = Request(scope)
            accept = request.headers.get("accept", "").lower()
            if "text/event-stream" not in accept:
                if "text/html" in accept:
                    response = RedirectResponse("/#mcp", status_code=303)
                else:
                    response = JSONResponse({
                        "service": "cn-cme-exchange-reference",
                        "transport": "Streamable HTTP",
                        "endpoint": "/mcp",
                        "message": "此地址用于 MCP 客户端。网页查询请打开 /，普通 HTTP 查询请查看 /api/tools。",
                        "clientSetup": "在 MCP 客户端选择 Streamable HTTP，并填写当前服务器的 /mcp 地址。",
                        "requestMethod": "POST",
                        "accept": "application/json, text/event-stream",
                        "documentation": "/usage.md",
                    })
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


def create_app():
    app = mcp.streamable_http_app()
    app.add_middleware(McpBrowserEntry)
    return app


def main():
    uvicorn.run(create_app(), host=mcp.settings.host, port=mcp.settings.port)


if __name__ == "__main__":
    main()
