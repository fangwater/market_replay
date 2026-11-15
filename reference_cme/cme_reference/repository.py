from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFINITION_PATH = (
    PROJECT_ROOT
    / "parsed"
    / "core_six_definition_2020_present"
    / "definition_outrights.parquet"
)
DEFINITION_SUMMARY_PATH = DEFINITION_PATH.with_name("summary.json")
STATUS_PATH = (
    PROJECT_ROOT
    / "parsed"
    / "core_six_status_2020_present"
    / "status_outrights.parquet"
)
STATUS_SUMMARY_PATH = STATUS_PATH.with_name("summary.json")
PRODUCTS = ("ES", "NQ", "RTY", "YM", "GC", "CL")
PRODUCT_INFO = {
    "ES": {"name": "E-mini S&P 500", "exchange": "CME", "kind": "股指"},
    "NQ": {"name": "E-mini Nasdaq-100", "exchange": "CME", "kind": "股指"},
    "RTY": {"name": "E-mini Russell 2000", "exchange": "CME", "kind": "股指"},
    "YM": {"name": "E-mini Dow", "exchange": "CBOT", "kind": "股指"},
    "GC": {"name": "Gold", "exchange": "COMEX", "kind": "商品"},
    "CL": {"name": "WTI Crude Oil", "exchange": "NYMEX", "kind": "商品"},
}

ACTION_LABELS = {
    "NONE": "状态继承",
    "PRE_OPEN": "开盘前",
    "PRE_CROSS": "集合竞价前",
    "QUOTING": "仅报价",
    "CROSS": "集合竞价",
    "ROTATION": "轮转开盘",
    "NEW_PRICE_INDICATION": "新价格指示",
    "TRADING": "交易中",
    "HALT": "停止交易",
    "PAUSE": "交易暂停",
    "SUSPEND": "停牌",
    "PRE_CLOSE": "收盘前",
    "CLOSE": "已收盘",
    "POST_CLOSE": "收盘后",
    "SSR_CHANGE": "卖空限制变更",
    "NOT_AVAILABLE_FOR_TRADING": "不可交易",
    "UNKNOWN": "未知状态",
}
REASON_LABELS = {
    "NONE": "未说明",
    "SCHEDULED": "计划内",
    "SURVEILLANCE_INTERVENTION": "市场监控干预",
    "MARKET_EVENT": "市场事件",
    "INSTRUMENT_ACTIVATION": "合约激活",
    "INSTRUMENT_EXPIRATION": "合约到期",
    "RECOVERY_IN_PROCESS": "恢复处理中",
    "REGULATORY": "监管原因",
    "ADMINISTRATIVE": "管理原因",
    "OPERATIONAL": "运营故障",
    "ORDER_IMBALANCE": "订单失衡",
    "LULD_PAUSE": "价格限制暂停",
    "MARKET_WIDE_HALT_LEVEL1": "全市场一级熔断",
    "MARKET_WIDE_HALT_LEVEL2": "全市场二级熔断",
    "MARKET_WIDE_HALT_LEVEL3": "全市场三级熔断",
    "MARKET_WIDE_HALT_CARRYOVER": "全市场熔断延续",
    "MARKET_WIDE_HALT_RESUMPTION": "全市场熔断恢复",
    "QUOTATION_NOT_AVAILABLE": "报价不可用",
    "UNKNOWN": "未知原因",
}
TRADING_EVENT_LABELS = {
    "NONE": "无附加事件",
    "NO_CANCEL": "禁止撤改单",
    "CHANGE_TRADING_SESSION": "交易时段切换",
    "IMPLIED_MATCHING_ON": "隐含撮合开启",
    "IMPLIED_MATCHING_OFF": "隐含撮合关闭",
    "UNKNOWN": "未知事件",
}

DEFINITION_COLUMNS = """
    ts_recv, ts_event, raw_symbol, asset, instrument_id,
    security_update_action, min_price_increment, display_factor,
    expiration, activation, high_limit_price, low_limit_price,
    max_price_variation, unit_of_measure_qty, min_price_increment_amount,
    price_ratio, raw_instrument_id, market_depth_implied, market_depth,
    market_segment_id, max_trade_vol, min_lot_size, min_lot_size_block,
    min_lot_size_round_lot, min_trade_vol, contract_multiplier,
    original_contract_size, appl_id, maturity_year, channel_id, currency,
    settl_currency, "group", exchange, cfi, security_type, unit_of_measure,
    match_algorithm, main_fraction, price_display_format, sub_fraction,
    maturity_month, maturity_day, maturity_week, user_defined_instrument
"""


class QueryError(ValueError):
    pass


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _validate_files() -> None:
    for path in (DEFINITION_PATH, DEFINITION_SUMMARY_PATH, STATUS_PATH, STATUS_SUMMARY_PATH):
        if not path.is_file():
            raise FileNotFoundError(path)


def parse_date(value: str | None, default: str) -> str:
    candidate = value or default
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError as exc:
        raise QueryError("日期必须使用 YYYY-MM-DD 格式") from exc


def parse_product(value: str | None, allow_all: bool = True) -> str | None:
    candidate = (value or "").strip().upper()
    if not candidate and allow_all:
        return None
    if candidate not in PRODUCTS:
        raise QueryError("品种仅支持 ES、NQ、RTY、YM、GC、CL")
    return candidate


def parse_page(value: str | None, default: int, maximum: int) -> int:
    try:
        result = int(value or default)
    except ValueError as exc:
        raise QueryError("分页参数必须是整数") from exc
    if result < 1 or result > maximum:
        raise QueryError(f"分页参数必须在 1 到 {maximum} 之间")
    return result


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _rows(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description]
    return [
        {name: _json_value(value) for name, value in zip(names, row)}
        for row in cursor.fetchall()
    ]


def _definition_cte() -> str:
    return f"""
        WITH ranked AS (
            SELECT {DEFINITION_COLUMNS},
                   row_number() OVER (
                       PARTITION BY raw_symbol
                       ORDER BY ts_recv DESC, ts_event DESC, instrument_id DESC
                   ) AS version_rank
            FROM read_parquet('{_sql_path(DEFINITION_PATH)}')
            WHERE ts_recv < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY
        ), effective AS (
            SELECT * FROM ranked
            WHERE version_rank = 1 AND security_update_action <> 'D'
        )
    """


def _definition_filters(
    as_of: str,
    product: str | None,
    search: str | None,
    active_only: bool,
) -> tuple[str, list[Any]]:
    conditions: list[str] = []
    params: list[Any] = [as_of]
    if product:
        conditions.append("asset = ?")
        params.append(product)
    needle = (search or "").strip().upper()
    if needle:
        conditions.append("upper(raw_symbol) LIKE ?")
        params.append(f"%{needle}%")
    if active_only:
        conditions.extend(
            (
                "activation < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY",
                "expiration >= CAST(? AS TIMESTAMPTZ)",
            )
        )
        params.extend((as_of, as_of))
    return (" AND ".join(conditions) if conditions else "TRUE"), params


def _enrich_definition(record: dict[str, Any]) -> dict[str, Any]:
    tick = record.get("min_price_increment")
    size = record.get("unit_of_measure_qty")
    record["tick_value"] = tick * size if tick is not None and size is not None else None
    record["product_info"] = PRODUCT_INFO.get(record.get("asset"))
    record["match_algorithm_label"] = (
        "FIFO" if record.get("match_algorithm") == "F" else record.get("match_algorithm")
    )
    return record


def definitions(
    as_of_value: str | None,
    product_value: str | None,
    search: str | None,
    active_only: bool,
    page_value: str | None,
    page_size_value: str | None,
) -> dict[str, Any]:
    as_of = parse_date(as_of_value, latest_definition_date())
    product = parse_product(product_value)
    page = parse_page(page_value, 1, 100_000)
    page_size = parse_page(page_size_value, 50, 200)
    where, params = _definition_filters(as_of, product, search, active_only)
    cte = _definition_cte()
    connection = _connect()
    try:
        total = connection.execute(
            cte + f" SELECT count(*) AS count FROM effective WHERE {where}", params
        ).fetchone()[0]
        cursor = connection.execute(
            cte
            + f"""
                SELECT * EXCLUDE (version_rank)
                FROM effective
                WHERE {where}
                ORDER BY expiration ASC NULLS LAST, raw_symbol ASC
                LIMIT ? OFFSET ?
            """,
            params + [page_size, (page - 1) * page_size],
        )
        records = [_enrich_definition(record) for record in _rows(cursor)]
    finally:
        connection.close()
    return {
        "asOf": as_of,
        "product": product,
        "activeOnly": active_only,
        "page": page,
        "pageSize": page_size,
        "total": total,
        "pages": max(1, (total + page_size - 1) // page_size),
        "records": records,
    }


def definition_overview(as_of_value: str | None) -> dict[str, Any]:
    as_of = parse_date(as_of_value, latest_definition_date())
    cte = _definition_cte()
    connection = _connect()
    try:
        cursor = connection.execute(
            cte
            + """
                SELECT asset, count(*) AS active_contracts,
                       min(expiration) AS nearest_expiration,
                       max(ts_recv) AS last_definition,
                       count(DISTINCT min_price_increment) AS tick_variants
                FROM effective
                WHERE activation < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY
                  AND expiration >= CAST(? AS TIMESTAMPTZ)
                GROUP BY asset
                ORDER BY CASE asset
                    WHEN 'ES' THEN 1 WHEN 'NQ' THEN 2 WHEN 'RTY' THEN 3
                    WHEN 'YM' THEN 4 WHEN 'GC' THEN 5 WHEN 'CL' THEN 6 ELSE 7 END
            """,
            [as_of, as_of, as_of],
        )
        products = _rows(cursor)
    finally:
        connection.close()
    for item in products:
        item["product_info"] = PRODUCT_INFO.get(item["asset"])
    return {
        "asOf": as_of,
        "activeContracts": sum(item["active_contracts"] for item in products),
        "products": products,
    }


def definition_history(symbol_value: str, as_of_value: str | None) -> dict[str, Any]:
    symbol = symbol_value.strip().upper()
    if not symbol or len(symbol) > 32:
        raise QueryError("合约代码无效")
    as_of = parse_date(as_of_value, latest_definition_date())
    connection = _connect()
    try:
        cursor = connection.execute(
            f"""
                SELECT {DEFINITION_COLUMNS}
                FROM read_parquet('{_sql_path(DEFINITION_PATH)}')
                WHERE raw_symbol = ?
                  AND ts_recv < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY
                ORDER BY ts_recv ASC, ts_event ASC
            """,
            [symbol, as_of],
        )
        observations = _rows(cursor)
    finally:
        connection.close()
    if not observations:
        raise QueryError("未找到该合约的定义记录")

    comparison_fields = (
        "instrument_id",
        "security_update_action",
        "min_price_increment",
        "unit_of_measure_qty",
        "min_price_increment_amount",
        "activation",
        "expiration",
        "market_depth",
        "max_trade_vol",
        "min_trade_vol",
        "match_algorithm",
    )
    changes: list[dict[str, Any]] = []
    previous: tuple[Any, ...] | None = None
    for record in observations:
        current = tuple(record.get(field) for field in comparison_fields)
        if current != previous:
            changes.append(_enrich_definition(record))
            previous = current
        else:
            changes[-1]["last_observed"] = record["ts_recv"]
    return {
        "symbol": symbol,
        "asOf": as_of,
        "observations": len(observations),
        "changes": changes[-50:],
        "record": _enrich_definition(observations[-1]),
    }


def status_symbols(product_value: str | None, date_value: str | None) -> dict[str, Any]:
    product = parse_product(product_value, allow_all=False)
    day = parse_date(date_value, latest_complete_status_date(product))
    connection = _connect()
    try:
        cursor = connection.execute(
            _definition_cte()
            + f""",
                day_status AS (
                    SELECT symbol, count(*) AS events,
                           min(ts_recv) AS first_event, max(ts_recv) AS last_event
                    FROM read_parquet('{_sql_path(STATUS_PATH)}')
                    WHERE product = ?
                      AND ts_recv >= CAST(? AS TIMESTAMPTZ)
                      AND ts_recv < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY
                    GROUP BY symbol
                )
                SELECT s.symbol, s.events, s.first_event, s.last_event,
                       d.expiration, d.activation
                FROM day_status s
                JOIN effective d ON d.raw_symbol = s.symbol
                  AND d.activation < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY
                  AND d.expiration >= CAST(? AS TIMESTAMPTZ)
                ORDER BY d.expiration ASC NULLS LAST, s.symbol ASC
            """,
            [day, product, day, day, day, day],
        )
        records = _rows(cursor)
    finally:
        connection.close()
    return {"date": day, "product": product, "total": len(records), "records": records}


def status_events(
    product_value: str | None,
    date_value: str | None,
    symbol_value: str | None,
    page_value: str | None,
    page_size_value: str | None,
) -> dict[str, Any]:
    product = parse_product(product_value, allow_all=False)
    day = parse_date(date_value, latest_complete_status_date(product))
    symbol = (symbol_value or "").strip().upper()
    if len(symbol) > 32:
        raise QueryError("合约代码无效")
    page = parse_page(page_value, 1, 100_000)
    page_size = parse_page(page_size_value, 250, 1000)
    conditions = [
        "product = ?",
        "ts_recv >= CAST(? AS TIMESTAMPTZ)",
        "ts_recv < CAST(? AS TIMESTAMPTZ) + INTERVAL 1 DAY",
    ]
    params: list[Any] = [product, day, day]
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    where = " AND ".join(conditions)
    connection = _connect()
    try:
        summary_cursor = connection.execute(
            f"""
                SELECT count(*) AS events, count(DISTINCT symbol) AS symbols,
                       min(ts_recv) AS first_event, max(ts_recv) AS last_event,
                       count(*) FILTER (WHERE is_trading = 'Y') AS trading_events,
                       count(*) FILTER (
                           WHERE action_name IN ('HALT', 'PAUSE', 'SUSPEND')
                              OR (action_name = 'NOT_AVAILABLE_FOR_TRADING'
                                  AND reason_name <> 'SCHEDULED')
                       ) AS exceptional_events
                FROM read_parquet('{_sql_path(STATUS_PATH)}')
                WHERE {where}
            """,
            params,
        )
        summary = _rows(summary_cursor)[0]
        cursor = connection.execute(
            f"""
                SELECT ts_recv, ts_event, product, symbol, instrument_id,
                       action, action_name, reason, reason_name,
                       trading_event, trading_event_name,
                       is_trading, is_quoting, is_short_sell_restricted,
                       source
                FROM read_parquet('{_sql_path(STATUS_PATH)}')
                WHERE {where}
                ORDER BY ts_recv ASC, ts_event ASC, instrument_id ASC
                LIMIT ? OFFSET ?
            """,
            params + [page_size, (page - 1) * page_size],
        )
        records = _rows(cursor)
    finally:
        connection.close()
    for record in records:
        record["action_label"] = ACTION_LABELS.get(record["action_name"], record["action_name"])
        record["reason_label"] = REASON_LABELS.get(record["reason_name"], record["reason_name"])
        record["trading_event_label"] = TRADING_EVENT_LABELS.get(
            record["trading_event_name"], record["trading_event_name"]
        )
        record["is_midnight_snapshot"] = record["ts_recv"].endswith("T00:00:00Z")
    total = summary["events"]
    return {
        "date": day,
        "product": product,
        "symbol": symbol or None,
        "page": page,
        "pageSize": page_size,
        "total": total,
        "pages": max(1, (total + page_size - 1) // page_size),
        "summary": summary,
        "records": records,
    }


def _read_summary(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_definition_date() -> str:
    value = _read_summary(DEFINITION_SUMMARY_PATH)["ts_recv_max"]
    return value[:10]


def latest_complete_status_date(product: str | None = None) -> str:
    summary = _read_summary(STATUS_SUMMARY_PATH)
    source = "equity" if product in (None, "ES", "NQ", "RTY", "YM") else "commodity"
    latest = summary["coverage"][source]["ts_recv_max"][:10]
    return date.fromordinal(date.fromisoformat(latest).toordinal() - 1).isoformat()


def application_summary() -> dict[str, Any]:
    _validate_files()
    definition = _read_summary(DEFINITION_SUMMARY_PATH)
    status = _read_summary(STATUS_SUMMARY_PATH)
    return {
        "dataset": "GLBX.MDP3",
        "products": [
            {"code": code, **PRODUCT_INFO[code]} for code in PRODUCTS
        ],
        "definition": {
            "rows": definition["outrights"]["rows"],
            "uniqueSymbols": definition["outrights"]["unique_raw_symbols"],
            "start": definition["ts_recv_min"],
            "end": definition["ts_recv_max"],
            "latestDate": latest_definition_date(),
        },
        "status": {
            "rows": status["rows"],
            "rowsByProduct": status["rows_by_product"],
            "coverage": status["coverage"],
            "latestCompleteDates": {
                "equity": latest_complete_status_date("ES"),
                "commodity": latest_complete_status_date("GC"),
            },
        },
    }


_validate_files()
