"""Message-fee rule validation, versioning, and order-event calculation."""

import csv
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from io import StringIO

from . import storage
from .models import DataError, normalize_venue
from .source import parse_date


SUPPORTED_SCOPES = {
    "trading_day+account+instrument",
    "交易日+账户+合约",
}


def _decimal(value, label, minimum=None):
    value = str(value).strip()
    if value.endswith("元/笔"):
        value = value[:-3].strip()
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise DataError("%s must be a decimal: %r" % (label, value))
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise DataError("%s is outside the supported range: %r" % (label, value))
    return result


def _decimal_text(value, label, minimum=None):
    value = _decimal(value, label, minimum)
    return "0" if value == 0 else format(value.normalize(), "f")


def normalize_rule(rule, source_url=None, source_sha256=None, analysis_id=None,
                   default_review_status="PENDING"):
    if not isinstance(rule, dict):
        raise DataError("message-fee rule must be a JSON object")
    rule_key = str(rule.get("rule_id") or rule.get("rule_key") or "").strip()
    exchange = normalize_venue(rule.get("exchange"))
    products = sorted({
        str(product).strip().upper() for product in (rule.get("products") or [])
        if str(product).strip()
    })
    if not rule_key or exchange not in {"CFFEX", "SHFE", "INE", "DCE", "CZCE", "GFEX"}:
        raise DataError("rule_id and a supported exchange are required")
    if not products:
        raise DataError("message-fee rule %s has no products" % rule_key)
    instrument_type = str(rule.get("instrument_type") or "").upper()
    if instrument_type not in ("FUTURE", "OPTION", "BOTH"):
        raise DataError("message-fee rule %s has invalid instrument_type" % rule_key)
    effective_from = parse_date(rule.get("effective_from"), "effective_from")
    effective_to = rule.get("effective_to") or None
    if effective_to:
        effective_to = parse_date(effective_to, "effective_to")
        if effective_to < effective_from:
            raise DataError("effective_to must not precede effective_from")
    scope = str(rule.get("aggregation_scope") or "").strip()
    if scope not in SUPPORTED_SCOPES:
        raise DataError("unsupported aggregation_scope: %r" % scope)
    volume = rule.get("information_volume") or {}
    normalized_volume = {
        name: _decimal_text(volume.get(name, 0), "information_volume.%s" % name, Decimal("0"))
        for name in ("new_order", "cancel", "inquiry", "other")
    }
    if all(Decimal(value) == 0 for value in normalized_volume.values()):
        raise DataError("information_volume must count at least one event type")
    zero_fill = int(rule.get("zero_fill_denominator", 1))
    if zero_fill <= 0:
        raise DataError("zero_fill_denominator must be positive")
    otr_threshold = _decimal_text(rule.get("otr_threshold", 2), "otr_threshold")
    tiers = []
    for index, tier in enumerate(rule.get("tiers") or [], 1):
        minimum = int(tier.get("min"))
        maximum = tier.get("max")
        maximum = int(maximum) if maximum is not None else None
        if minimum < 1 or (maximum is not None and maximum < minimum):
            raise DataError("tier %d has an invalid range" % index)
        tiers.append({
            "min": minimum,
            "max": maximum,
            "rate_otr_le_threshold": _decimal_text(
                tier.get("rate_otr_le_threshold"), "tier %d low OTR rate" % index,
                Decimal("0"),
            ),
            "rate_otr_gt_threshold": _decimal_text(
                tier.get("rate_otr_gt_threshold"), "tier %d high OTR rate" % index,
                Decimal("0"),
            ),
        })
    tiers.sort(key=lambda item: item["min"])
    if not tiers or tiers[0]["min"] != 1:
        raise DataError("message-fee tiers must start at information volume 1")
    for previous, current in zip(tiers, tiers[1:]):
        if previous["max"] is None or current["min"] != previous["max"] + 1:
            raise DataError("message-fee tiers must be contiguous and non-overlapping")
    if tiers[-1]["max"] is not None:
        raise DataError("the final message-fee tier must have max=null")
    review_status = str(rule.get("review_status") or default_review_status).upper()
    if review_status not in ("PENDING", "VERIFIED", "REJECTED"):
        raise DataError("invalid review_status: %s" % review_status)
    executable = {
        "rule_key": rule_key,
        "exchange": exchange,
        "products": products,
        "instrument_type": instrument_type,
        "effective_from": effective_from,
        "effective_to": effective_to,
        "aggregation_scope": "trading_day+account+instrument",
        "information_volume": normalized_volume,
        "otr_formula": str(rule.get("otr_formula") or "").strip(),
        "otr_threshold": otr_threshold,
        "zero_fill_denominator": zero_fill,
        "tiers": tiers,
        "exemptions": [str(value).strip() for value in rule.get("exemptions", []) if str(value).strip()],
        "source_url": str(source_url or rule.get("source_url") or "").strip(),
        "source_sha256": str(source_sha256 or rule.get("source_sha256") or "").strip(),
    }
    if not executable["otr_formula"] or not executable["source_url"] or not executable["source_sha256"]:
        raise DataError("otr_formula, source_url, and source_sha256 are required")
    payload = json.dumps(executable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return {
        **executable,
        "rule_hash": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "analysis_id": analysis_id,
        "products": json.dumps(products, ensure_ascii=False),
        "information_volume": json.dumps(normalized_volume, ensure_ascii=False),
        "tiers": json.dumps(tiers, ensure_ascii=False),
        "exemptions": json.dumps(executable["exemptions"], ensure_ascii=False),
        "review_status": review_status,
        "start_ts": now,
        "update_ts": now,
    }


def import_document(connection, document, default_review_status="PENDING"):
    if isinstance(document, dict) and "rules" in document:
        rules = document["rules"]
    elif isinstance(document, list):
        rules = document
    else:
        rules = [document]
    if not rules:
        raise DataError("message-fee rule document is empty")
    result = {"processed": 0, "inserted": 0, "confirmed": 0, "ruleIds": []}
    try:
        for rule in rules:
            normalized = normalize_rule(rule, default_review_status=default_review_status)
            saved = storage.upsert_message_fee_rule(connection, normalized)
            result["processed"] += 1
            result["inserted" if saved["inserted"] else "confirmed"] += 1
            result["ruleIds"].append(saved["id"])
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return result


def import_json(connection, text, default_review_status="PENDING"):
    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise DataError("message-fee rule file is not valid JSON: %s" % error)
    return import_document(connection, document, default_review_status)


def save_analysis_rules(connection, analysis_id, analysis, notice):
    result = {"inserted": 0, "confirmed": 0, "invalid": []}
    try:
        for candidate in analysis.get("rules") or []:
            try:
                rule = normalize_rule(
                    candidate,
                    source_url=notice["source_url"],
                    source_sha256=notice["content_sha256"],
                    analysis_id=analysis_id,
                    default_review_status="PENDING",
                )
                saved = storage.upsert_message_fee_rule(connection, rule)
                result["inserted" if saved["inserted"] else "confirmed"] += 1
            except (DataError, TypeError, ValueError) as error:
                result["invalid"].append({
                    "ruleId": candidate.get("rule_id") if isinstance(candidate, dict) else None,
                    "error": str(error),
                })
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return result


HEADER_ALIASES = {
    "trading_day": ("trading_day", "trade_date", "date", "交易日"),
    "account": ("account", "account_id", "investor_id", "账户", "投资者代码"),
    "exchange": ("exchange", "venue", "交易所"),
    "instrument": ("instrument", "instrument_id", "symbol", "合约", "合约代码"),
    "event_type": ("event_type", "event", "type", "事件类型"),
    "order_id": ("order_id", "order_ref", "报单编号", "订单编号"),
    "filled_quantity": ("filled_quantity", "filled_qty", "trade_volume", "成交量"),
}

EVENT_ALIASES = {
    "NEW": "new_order", "ORDER": "new_order", "INSERT": "new_order", "报单": "new_order",
    "CANCEL": "cancel", "撤单": "cancel",
    "INQUIRY": "inquiry", "QUOTE_INQUIRY": "inquiry", "询价": "inquiry",
    "OTHER": "other", "其他": "other",
    "FILL": "fill", "TRADE": "fill", "成交": "fill",
}


def _field(row, name, required=True):
    for alias in HEADER_ALIASES[name]:
        if alias in row and str(row[alias] or "").strip():
            return str(row[alias]).strip()
    if required:
        raise DataError("order events CSV is missing %s" % name)
    return ""


def parse_order_events(text):
    reader = csv.DictReader(StringIO(text, newline=""))
    if not reader.fieldnames:
        raise DataError("order events CSV has no header")
    events = []
    for number, row in enumerate(reader, 2):
        try:
            event_type_raw = _field(row, "event_type").upper()
            event_type = EVENT_ALIASES.get(event_type_raw)
            if not event_type:
                raise DataError("unsupported event_type %r" % event_type_raw)
            filled = _decimal(_field(row, "filled_quantity", False) or "0", "filled_quantity", Decimal("0"))
            events.append({
                "trading_day": parse_date(_field(row, "trading_day"), "trading_day"),
                "account": _field(row, "account"),
                "exchange": normalize_venue(_field(row, "exchange")),
                "instrument": _field(row, "instrument").upper(),
                "event_type": event_type,
                "order_id": _field(row, "order_id", False) or "row-%d" % number,
                "filled_quantity": filled,
            })
        except DataError as error:
            raise DataError("order events CSV line %d: %s" % (number, error))
    if not events:
        raise DataError("order events CSV contains no events")
    return events


def _instrument_type(instrument):
    return "OPTION" if re.search(r"-[CP]-", instrument, re.I) else "FUTURE"


def _product(instrument):
    match = re.match(r"([A-Za-z]+)", instrument)
    if not match:
        raise DataError("cannot infer product from instrument %r" % instrument)
    return match.group(1).upper()


def _rule_for(rules, event_group):
    product = _product(event_group["instrument"])
    instrument_type = _instrument_type(event_group["instrument"])
    matches = []
    for rule in rules:
        products = rule["products"] if isinstance(rule["products"], list) else json.loads(rule["products"])
        if (
            rule["exchange"].upper() == event_group["exchange"].upper()
            and product in {str(value).upper() for value in products}
            and rule["effective_from"] <= event_group["trading_day"]
            and (not rule.get("effective_to") or rule["effective_to"] >= event_group["trading_day"])
            and rule["instrument_type"] in ("BOTH", instrument_type)
        ):
            matches.append(rule)
    return max(matches, key=lambda row: (row["effective_from"], row.get("start_ts") or "", row["id"])) if matches else None


def _message_fee(information_volume, otr, rule):
    tiers = rule["tiers"] if isinstance(rule["tiers"], list) else json.loads(rule["tiers"])
    high = otr > Decimal(str(rule["otr_threshold"]))
    field = "rate_otr_gt_threshold" if high else "rate_otr_le_threshold"
    total = Decimal("0")
    breakdown = []
    for tier in tiers:
        upper = tier["max"] if tier["max"] is not None else information_volume
        count = max(0, min(information_volume, int(upper)) - int(tier["min"]) + 1)
        rate = Decimal(str(tier[field]))
        amount = rate * count
        total += amount
        if count:
            breakdown.append({
                "min": tier["min"], "max": tier["max"], "messages": count,
                "rate": format(rate, "f"), "amount": format(amount, "f"),
            })
    return total, breakdown


def calculate(events, rules):
    groups = defaultdict(list)
    for event in events:
        key = (event["trading_day"], event["account"], event["exchange"], event["instrument"])
        groups[key].append(event)
    rows, total = [], Decimal("0")
    for key, group_events in sorted(groups.items()):
        group = dict(zip(("trading_day", "account", "exchange", "instrument"), key))
        rule = _rule_for(rules, group)
        if not rule:
            rows.append({**group, "status": "NO_VERIFIED_RULE", "messageFee": None})
            continue
        weights = rule["information_volume"]
        if not isinstance(weights, dict):
            weights = json.loads(weights)
        information = sum(
            Decimal(str(weights.get(event["event_type"], 0)))
            for event in group_events if event["event_type"] != "fill"
        )
        if information != information.to_integral_value():
            raise DataError("weighted information volume must be a whole number")
        information = int(information)
        filled_orders = {
            event["order_id"] for event in group_events
            if event["filled_quantity"] > 0 or event["event_type"] == "fill"
        }
        denominator = len(filled_orders) or int(rule["zero_fill_denominator"])
        otr = Decimal(information) / Decimal(denominator) - Decimal("1")
        fee, breakdown = _message_fee(information, otr, rule)
        total += fee
        rows.append({
            **group, "status": "CALCULATED", "ruleId": rule["id"],
            "ruleKey": rule["rule_key"], "informationVolume": information,
            "filledOrderCount": len(filled_orders), "otr": format(otr, "f"),
            "messageFee": format(fee, "f"), "breakdown": breakdown,
        })
    return {
        "groups": rows,
        "calculatedGroups": sum(row["status"] == "CALCULATED" for row in rows),
        "unmatchedGroups": sum(row["status"] != "CALCULATED" for row in rows),
        "totalMessageFee": format(total, "f"),
    }


def calculate_csv(connection, text):
    rules = storage.list_message_fee_rules(connection, "VERIFIED")
    return calculate(parse_order_events(text), rules)
