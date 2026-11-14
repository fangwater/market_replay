"""Application operations used by the HTTP API and local dashboard."""

import csv
from io import StringIO
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
from urllib.parse import parse_qs, urlsplit

from . import announcements, codex_ai, message_fees, source, storage, trading_sessions
from .models import (
    DB_COLUMNS, DEFAULT_SESSION_SOURCE_URL, DEFAULT_SOURCE_URL, DataError,
    OVERRIDABLE_FIELDS, SESSION_SOURCE_TYPE, SOURCE_TYPE, VENUE_NAMES, VERSION,
    canonical_decimal, normalize_venue, required_decimal,
)


_CHINA_TZ = timezone(timedelta(hours=8))


def _observed_at(records, source_date):
    """Use the source's published timestamp when present, otherwise its date."""
    values = [record.get("source_updated_at") for record in records]
    for value in values:
        match = re.search(
            r"(20\d{2})[-/](\d{2})[-/](\d{2})\s+(\d{2}):(\d{2}):(\d{2})",
            str(value or ""),
        )
        if match:
            fields = [int(item) for item in match.groups()]
            return datetime(*fields, tzinfo=_CHINA_TZ).astimezone(timezone.utc)
    return datetime.strptime(source_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def sync(database, source_url=DEFAULT_SOURCE_URL, source_date=None, timeout=30, retries=2):
    source_url = source_url or DEFAULT_SOURCE_URL
    content, content_type, resolved_url = source.fetch(
        source_url, timeout, retries, "cn-futures-ref/%s" % VERSION
    )
    return import_content(
        database, content, "html", resolved_url, source_date, content_type
    )


def import_content(database, content, source_format, source_url, source_date=None,
                   content_type=""):
    if source_format not in ("html", "csv"):
        raise DataError("source_format must be html or csv")
    text = source.decode(content, content_type)
    records = source.parse_csv(text) if source_format == "csv" else source.parse_html(text)
    date = source.source_date(text, records, source_date)
    connection = storage.connect(database)
    try:
        result = storage.merge_records(
            connection, records, date, _observed_at(records, date), source_url
        )
        state = storage.data_state(connection)
    finally:
        connection.close()
    result.update({
        "status": "synced", "sourceDate": date, "dataState": state,
        "sourceType": SOURCE_TYPE,
    })
    return result


def sync_continuous_sessions(database, source_url=DEFAULT_SESSION_SOURCE_URL,
                             source_date=None, timeout=30, retries=2):
    source_url = source_url or DEFAULT_SESSION_SOURCE_URL
    content, content_type, resolved_url = trading_sessions.fetch(
        source_url, timeout, retries
    )
    return import_continuous_sessions(
        database, content, resolved_url, source_date, content_type
    )


def import_continuous_sessions(database, content, source_url, source_date=None,
                               content_type=""):
    rows = trading_sessions.parse_payload(content, content_type)
    query = parse_qs(urlsplit(source_url).query)
    filtered = bool(query.get("markets") or query.get("products"))
    if not filtered:
        trading_sessions.validate_full_snapshot(rows)
    date = source.parse_date(
        source_date or datetime.now(_CHINA_TZ).date().isoformat(), "source date"
    )
    observed_at = datetime.now(timezone.utc).replace(microsecond=0)
    connection = storage.connect(database)
    try:
        result = storage.merge_continuous_sessions(
            connection, rows, date, observed_at, source_url,
            reconcile=not filtered,
        )
        state = storage.continuous_session_state(connection)
    finally:
        connection.close()
    result.update({
        "status": "synced", "sourceDate": date, "dataState": state,
        "sourceType": SESSION_SOURCE_TYPE, "reconciled": not filtered,
    })
    return result


def continuous_sessions(connection, as_of=None, venue=None, product=None):
    state = storage.continuous_session_state(connection, as_of)
    rows = storage.current_continuous_sessions(
        connection, as_of, normalize_venue(venue) if venue else None, product
    )
    return state, rows


def _apply_overrides(connection, record, effective_date):
    result, applied = dict(record), []
    for override in storage.active_overrides(connection, result, effective_date):
        result[override["field_name"]] = override["field_value"]
        applied.append({
            "field": override["field_name"], "value": override["field_value"],
            "effectiveFrom": override["effective_from"], "effectiveTo": override["effective_to"],
            "sourceUrl": override["source_url"], "note": override["note"],
        })
    result["applied_overrides"] = applied
    return result


def records(connection, as_of=None, venue=None, product=None, instrument=None,
            include_options=False):
    state = storage.data_state(connection, as_of)
    effective_date = source.parse_date(as_of, "as-of") if as_of else state["as_of"]
    rows = storage.current_records(
        connection, as_of, normalize_venue(venue) if venue else None, product,
        instrument, include_options,
    )
    return state, [
        _apply_overrides(connection, row, effective_date) for row in rows
    ]


def one_record(connection, as_of, venue, instrument):
    state, result = records(connection, as_of, venue, None, instrument, True)
    if not result:
        raise DataError("instrument not found: %s.%s" % (venue, instrument))
    if len(result) > 1:
        raise DataError("instrument lookup was not unique: %s.%s" % (venue, instrument))
    return state, result[0]


def fee(record, price, quantity, position_effect):
    field_map = {
        "open": ("open_rate_by_money", "open_fee_per_lot"),
        "close": ("close_rate_by_money", "close_fee_per_lot"),
        "close_yesterday": ("close_rate_by_money", "close_fee_per_lot"),
        "close_today": ("close_today_rate_by_money", "close_today_fee_per_lot"),
    }
    if position_effect not in field_map:
        raise DataError("unsupported position effect: %r" % position_effect)
    rate_field, fixed_field = field_map[position_effect]
    multiplier = required_decimal(record.get("contract_multiplier"), "contract_multiplier")
    rate = required_decimal(record.get(rate_field), rate_field)
    fixed = required_decimal(record.get(fixed_field), fixed_field)
    price, quantity = required_decimal(price, "price"), required_decimal(quantity, "quantity")
    if quantity <= 0 or quantity != quantity.to_integral_value():
        raise DataError("quantity must be a positive whole number of lots")
    notional = abs(price * multiplier * quantity)
    total = quantity * fixed + notional * rate
    return {
        "positionEffect": position_effect.upper(), "price": format(price, "f"),
        "quantity": format(quantity, "f"),
        "contractMultiplier": format(multiplier, "f"), "notional": format(notional, "f"),
        "fixedPerLot": format(fixed, "f"), "adValoremRate": format(rate, "f"),
        "fee": format(total, "f"),
    }


def select_price(record, explicit):
    if explicit is not None:
        return explicit
    for field in ("latest_price", "previous_settlement_price", "previous_close_price"):
        if canonical_decimal(record.get(field)) is not None:
            return record[field]
    raise DataError("price is required because the source has no usable market price")


def exchange_info(record):
    def product(left, right):
        left, right = canonical_decimal(left), canonical_decimal(right)
        if left is None or right is None:
            return None
        return format(Decimal(left) * Decimal(right), "f")

    def rate(effect, rate_field, fixed_field, reference):
        return {
            "positionEffect": effect, "liquidityRole": "ANY",
            "adValoremRate": record.get(rate_field),
            "fixedPerLot": record.get(fixed_field),
            "sourceReferenceOneLotFee": record.get(reference),
        }

    return {
        "venue": record["venue"], "symbol": record["instrument_id"],
        "productId": record["product_id"], "name": record.get("instrument_name"),
        "instrumentType": record["instrument_type"],
        "contractMultiplier": record.get("contract_multiplier"),
        "tickValue": product(record.get("price_tick"), record.get("contract_multiplier")),
        "filters": [
            {"filterType": "PRICE_FILTER", "tickSize": record.get("price_tick")},
            {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1", "maxQty": None,
             "status": "OPENCTP_FEE_TABLE_DOES_NOT_SUPPLY_LIMITS"},
        ],
        "fees": [
            rate("OPEN", "open_rate_by_money", "open_fee_per_lot", "calculated_open_fee_one_lot"),
            rate("CLOSE_YESTERDAY", "close_rate_by_money", "close_fee_per_lot", "calculated_close_fee_one_lot"),
            rate("CLOSE_TODAY", "close_today_rate_by_money", "close_today_fee_per_lot", "calculated_close_today_fee_one_lot"),
        ],
        "margin": {
            "longRate": record.get("long_margin_rate_by_money"),
            "longPerLot": record.get("long_margin_per_lot"),
            "shortRate": record.get("short_margin_rate_by_money"),
            "shortPerLot": record.get("short_margin_per_lot"),
        },
        "marketReference": {
            "previousSettlementPrice": record.get("previous_settlement_price"),
            "previousClosePrice": record.get("previous_close_price"),
            "latestPrice": record.get("latest_price"),
        },
        "sourceUpdatedAt": record.get("source_updated_at"),
        "startTs": record.get("start_ts"), "updateTs": record.get("update_ts"),
        "appliedOverrides": record.get("applied_overrides", []),
    }


def export_document(connection, as_of=None, venue=None, product=None, include_options=False):
    state, result = records(connection, as_of, venue, product, None, include_options)
    return {
        "schema": "cn-futures-exchange-info/v1", "asOf": state["as_of"],
        "dataState": state, "feeScope": "BROKER_REFERENCE",
        "sourceType": SOURCE_TYPE, "historicalExact": False,
        "makerTakerApplicable": False, "messageFeeSupported": False,
        "feeFormula": "quantity * fixedPerLot + abs(price * contractMultiplier * quantity) * adValoremRate",
        "symbols": [exchange_info(record) for record in result],
    }


def records_csv(records):
    fields = [field for field in DB_COLUMNS if field != "raw_json"] + [
        "source_date", "start_ts", "update_ts", "applied_overrides",
    ]
    handle = StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    for record in records:
        row = {field: record.get(field) for field in fields}
        row["applied_overrides"] = json.dumps(
            record.get("applied_overrides", []), ensure_ascii=False
        )
        writer.writerow(row)
    return handle.getvalue()


def import_overrides_csv(connection, text):
    required = {"venue", "product_id", "effective_from", "field", "value", "source_url"}
    count = 0
    reader = csv.DictReader(StringIO(text, newline=""))
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise DataError("override CSV is missing columns: %s" % ", ".join(sorted(missing)))
    for number, row in enumerate(reader, 2):
        field = str(row.get("field") or "").strip()
        value = canonical_decimal(row.get("value"))
        if field not in OVERRIDABLE_FIELDS:
            raise DataError("override line %d has unsupported field %r" % (number, field))
        if value is None:
            raise DataError("override line %d has invalid numeric value" % number)
        effective_from = source.parse_date(row.get("effective_from"), "override effective_from")
        effective_to = str(row.get("effective_to") or "").strip() or None
        if effective_to:
            effective_to = source.parse_date(effective_to, "override effective_to")
            if effective_to <= effective_from:
                raise DataError("override line %d effective_to must be after effective_from" % number)
        entry = {
            "venue": normalize_venue(row.get("venue")),
            "product_id": str(row.get("product_id") or "").strip(),
            "instrument_id": str(row.get("instrument_id") or "").strip(),
            "effective_from": effective_from, "effective_to": effective_to,
            "field_name": field, "field_value": value,
            "source_url": str(row.get("source_url") or "").strip(),
            "note": str(row.get("note") or "").strip() or None,
            "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0),
        }
        if not entry["product_id"] or not entry["source_url"]:
            raise DataError("override line %d requires product_id and source_url" % number)
        storage.upsert_override(connection, entry)
        count += 1
    connection.commit()
    return count


def round_trip(record, entry_price, exit_price, quantity, exit_effect, spread_ticks,
               slippage_ticks, extra_cost):
    entry = fee(record, entry_price, quantity, "open")
    exit_leg = fee(record, exit_price, quantity, exit_effect)
    tick_value = (
        required_decimal(record.get("price_tick"), "price_tick")
        * required_decimal(record.get("contract_multiplier"), "contract_multiplier")
        * required_decimal(quantity, "quantity")
    )
    spread = required_decimal(spread_ticks, "spread_ticks")
    slippage = required_decimal(slippage_ticks, "slippage_ticks")
    extra = required_decimal(extra_cost, "extra_cost")
    if tick_value <= 0 or min(spread, slippage, extra) < 0:
        raise DataError("tick value must be positive; costs must not be negative")
    fees = Decimal(entry["fee"]) + Decimal(exit_leg["fee"])
    market = tick_value * (spread + slippage)
    all_in = fees + market + extra
    return {
        "entry": entry, "exit": exit_leg, "feeTotal": format(fees, "f"),
        "tickValue": format(tick_value, "f"), "spreadTicks": format(spread, "f"),
        "slippageTicks": format(slippage, "f"), "marketCost": format(market, "f"),
        "extraCost": format(extra, "f"), "allInCost": format(all_in, "f"),
        "breakEvenTicks": int((all_in / tick_value).to_integral_value(rounding=ROUND_CEILING)),
    }


def announcement_dashboard(database, venue=None, limit=100):
    connection = storage.connect(database)
    try:
        items = [
            item for item in storage.list_announcements(connection, venue, limit)
            if announcements.is_futures_notice(item["title"])
        ]
        rules = [
            rule for rule in storage.list_message_fee_rules(connection)
            if rule["instrument_type"] != "OPTION"
            and (not venue or rule["exchange"].upper() == venue.upper())
        ]
        latest_refresh = storage.latest_announcement_refresh(connection)
    finally:
        connection.close()
    database_pending = sum(rule["review_status"] == "PENDING" for rule in rules)
    incomplete = 0
    for item in items:
        candidates = [
            candidate for candidate in (item.get("raw_analysis") or {}).get("rules", [])
            if candidate.get("instrument_type") != "OPTION"
        ]
        saved_keys = {rule["rule_key"] for rule in item.get("rules", [])}
        incomplete += sum(candidate.get("rule_id") not in saved_keys for candidate in candidates)
    exchanges = []
    for code, name in VENUE_NAMES.items():
        exchange_items = [item for item in items if item["venue"] == code]
        exchange_rules = [rule for rule in rules if rule["exchange"] == code]
        exchanges.append({
            "code": code,
            "name": name,
            "announcements": len(exchange_items),
            "pendingRules": sum(
                rule["review_status"] == "PENDING" for rule in exchange_rules
            ),
        })
    return {
        "manualRefreshOnly": True,
        "futuresOnly": True,
        "ai": codex_ai.configuration_status(),
        "latestRefresh": latest_refresh,
        "summary": {
            "announcements": len(items),
            "pendingRules": database_pending + incomplete,
            "executablePendingRules": database_pending,
            "incompleteCandidates": incomplete,
            "verifiedRules": sum(rule["review_status"] == "VERIFIED" for rule in rules),
            "failedAnalyses": sum(item.get("analysis_status") == "FAILED" for item in items),
        },
        "exchanges": exchanges,
        "announcements": items,
        "rules": rules,
    }


def refresh_announcements(database, page_count=5, per_source_limit=8,
                          analysis_limit=20, timeout=12, retries=0):
    connection = storage.connect(database)
    run_id = None
    if not storage.try_announcement_refresh_lock(connection):
        connection.close()
        raise DataError("已有一次公告拉取正在运行")
    try:
        run_id = storage.begin_announcement_refresh(connection)
        collection = announcements.refresh(
            connection, page_count=page_count, per_source_limit=per_source_limit,
            timeout=timeout, retries=retries,
        )
        pending = storage.pending_announcement_versions(connection, analysis_limit)
        analysis_result = {
            "attempted": len(pending), "completed": 0, "failed": 0,
            "rulesInserted": 0, "rulesConfirmed": 0, "invalidRules": [],
            "errors": [],
        }
        for notice in pending:
            assets = storage.announcement_assets(
                connection, notice["version_id"], include_content=True
            )
            try:
                analysis, response_id = codex_ai.analyze_notice(notice, assets)
                analysis_id = storage.save_announcement_analysis(
                    connection, notice["version_id"], analysis,
                    codex_ai.MODEL, codex_ai.REASONING_EFFORT, response_id,
                )
                rules = message_fees.save_analysis_rules(
                    connection, analysis_id, analysis, notice
                )
                if rules["invalid"]:
                    analysis["validation_errors"] = rules["invalid"]
                    analysis.setdefault("uncertainties", []).extend(
                        "候选规则 %s 未入库：%s" % (
                            item.get("ruleId") or "未命名", item["error"]
                        ) for item in rules["invalid"]
                    )
                    storage.save_announcement_analysis(
                        connection, notice["version_id"], analysis,
                        codex_ai.MODEL, codex_ai.REASONING_EFFORT, response_id,
                    )
                analysis_result["completed"] += 1
                analysis_result["rulesInserted"] += rules["inserted"]
                analysis_result["rulesConfirmed"] += rules["confirmed"]
                analysis_result["invalidRules"].extend(rules["invalid"])
            except Exception as error:
                message = str(error)
                storage.save_announcement_analysis(
                    connection, notice["version_id"], None,
                    codex_ai.MODEL, codex_ai.REASONING_EFFORT,
                    error_message=message[:4000],
                )
                analysis_result["failed"] += 1
                analysis_result["errors"].append({
                    "announcementId": notice["announcement_id"],
                    "title": notice["title"], "error": message,
                })
        result = {
            "status": "completed", "trigger": "MANUAL",
            "collection": collection, "analysis": analysis_result,
        }
        storage.finish_announcement_refresh(connection, run_id, result=result)
        return result
    except Exception as error:
        if run_id is not None:
            storage.finish_announcement_refresh(
                connection, run_id, error=str(error)[:4000]
            )
        raise
    finally:
        storage.release_announcement_refresh_lock(connection)
        connection.commit()
        connection.close()


def review_message_fee_rule(database, rule_id, status, note=None):
    connection = storage.connect(database)
    try:
        return storage.review_message_fee_rule(connection, rule_id, status, note)
    finally:
        connection.close()


def import_message_fee_rules(connection, text, default_review_status="PENDING"):
    return message_fees.import_json(connection, text, default_review_status)


def calculate_message_fees(connection, csv_text):
    return message_fees.calculate_csv(connection, csv_text)
