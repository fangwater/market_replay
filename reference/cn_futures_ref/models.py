"""Shared data definitions and normalization rules."""

import json
import re
import unicodedata
from decimal import Decimal, InvalidOperation


VERSION = "0.5.0"
SCHEMA_VERSION = 4
SOURCE_TYPE = "OPENCTP_BROKER_REFERENCE"
DEFAULT_SOURCE_URL = "http://www.openctp.cn/fees.html"
SESSION_SOURCE_TYPE = "OPENCTP_SESSION_REFERENCE"
DEFAULT_SESSION_SOURCE_URL = "http://dict.openctp.cn/times?types=futures"

VENUE_ALIASES = {
    "CFFEX": "CFFEX", "中金所": "CFFEX", "中国金融期货交易所": "CFFEX",
    "SHFE": "SHFE", "上期所": "SHFE", "上海期货交易所": "SHFE",
    "INE": "INE", "上期能源": "INE", "上海国际能源交易中心": "INE",
    "DCE": "DCE", "大商所": "DCE", "大连商品交易所": "DCE",
    "CZCE": "CZCE", "郑商所": "CZCE", "郑州商品交易所": "CZCE",
    "GFEX": "GFEX", "广期所": "GFEX", "广州期货交易所": "GFEX",
}
VENUE_NAMES = {
    "CFFEX": "中国金融期货交易所",
    "SHFE": "上海期货交易所",
    "INE": "上海国际能源交易中心",
    "DCE": "大连商品交易所",
    "CZCE": "郑州商品交易所",
    "GFEX": "广州期货交易所",
}


class DataError(RuntimeError):
    """Raised when a source or a user supplied value is not usable."""


def header_key(value):
    value = unicodedata.normalize("NFKC", str(value or "")).replace("\ufeff", "")
    return re.sub(r"\s+", "", value)


def _aliases(*pairs):
    return {header_key(name): field for name, field in pairs}


HEADER_ALIASES = _aliases(
    ("交易所", "venue"), ("合约代码", "instrument_id"), ("合约名称", "instrument_name"),
    ("品种代码", "product_id"), ("品种名称", "product_name"),
    ("合约乘数", "contract_multiplier"), ("最小跳动", "price_tick"),
    ("最小变动价位", "price_tick"), ("开仓费率（按金额）", "open_rate_by_money"),
    ("开仓费率", "open_rate_by_money"), ("开仓费用（按手）", "open_fee_per_lot"),
    ("开仓费用/手", "open_fee_per_lot"), ("平仓费率（按金额）", "close_rate_by_money"),
    ("平仓费率", "close_rate_by_money"), ("平仓费用（按手）", "close_fee_per_lot"),
    ("平仓费用/手", "close_fee_per_lot"), ("平今费率（按金额）", "close_today_rate_by_money"),
    ("平今费率", "close_today_rate_by_money"), ("平今费用（按手）", "close_today_fee_per_lot"),
    ("平今费用/手", "close_today_fee_per_lot"), ("做多保证金率（按金额）", "long_margin_rate_by_money"),
    ("做多保证金率", "long_margin_rate_by_money"), ("做多保证金（按手）", "long_margin_per_lot"),
    ("做多保证金/手", "long_margin_per_lot"), ("做空保证金率（按金额）", "short_margin_rate_by_money"),
    ("做空保证金率", "short_margin_rate_by_money"), ("做空保证金（按手）", "short_margin_per_lot"),
    ("做空保证金/手", "short_margin_per_lot"), ("上日结算价", "previous_settlement_price"),
    ("上日收盘价", "previous_close_price"), ("最新价", "latest_price"),
    ("成交量", "volume"), ("持仓量", "open_interest"),
    ("1手开仓费用", "calculated_open_fee_one_lot"),
    ("1手平仓费用", "calculated_close_fee_one_lot"),
    ("1手平今费用", "calculated_close_today_fee_one_lot"),
    ("做多1手保证金", "calculated_long_margin_one_lot"),
    ("做空1手保证金", "calculated_short_margin_one_lot"), ("更新时间", "source_updated_at"),
)

NUMERIC_FIELDS = {
    "contract_multiplier", "price_tick", "open_rate_by_money", "open_fee_per_lot",
    "close_rate_by_money", "close_fee_per_lot", "close_today_rate_by_money",
    "close_today_fee_per_lot", "long_margin_rate_by_money", "long_margin_per_lot",
    "short_margin_rate_by_money", "short_margin_per_lot", "calculated_open_fee_one_lot",
    "calculated_close_fee_one_lot", "calculated_close_today_fee_one_lot",
    "calculated_long_margin_one_lot", "calculated_short_margin_one_lot",
    "previous_settlement_price", "previous_close_price", "latest_price", "volume", "open_interest",
}
REQUIRED_FIELDS = {
    "venue", "instrument_id", "product_id", "contract_multiplier", "price_tick",
    "open_rate_by_money", "open_fee_per_lot", "close_rate_by_money", "close_fee_per_lot",
    "close_today_rate_by_money", "close_today_fee_per_lot",
}
OVERRIDABLE_FIELDS = {
    "contract_multiplier", "price_tick", "open_rate_by_money", "open_fee_per_lot",
    "close_rate_by_money", "close_fee_per_lot", "close_today_rate_by_money",
    "close_today_fee_per_lot", "long_margin_rate_by_money", "long_margin_per_lot",
    "short_margin_rate_by_money", "short_margin_per_lot",
}
DB_COLUMNS = [
    "venue", "instrument_id", "instrument_name", "product_id", "product_name", "instrument_type",
    "contract_multiplier", "price_tick", "open_rate_by_money", "open_fee_per_lot",
    "close_rate_by_money", "close_fee_per_lot", "close_today_rate_by_money",
    "close_today_fee_per_lot", "long_margin_rate_by_money", "long_margin_per_lot",
    "short_margin_rate_by_money", "short_margin_per_lot", "calculated_open_fee_one_lot",
    "calculated_close_fee_one_lot", "calculated_close_today_fee_one_lot",
    "calculated_long_margin_one_lot", "calculated_short_margin_one_lot",
    "previous_settlement_price", "previous_close_price", "latest_price", "volume", "open_interest",
    "source_updated_at", "raw_json",
]


def canonical_decimal(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text.lower() in ("", "-", "--", "none", "null", "nan", "n/a"):
        return None
    percent = text.endswith("%")
    try:
        decimal = Decimal(text[:-1].strip() if percent else text)
    except InvalidOperation:
        return None
    if percent:
        decimal /= Decimal("100")
    return "0" if decimal == 0 else format(decimal.normalize(), "f")


def required_decimal(value, label):
    normalized = canonical_decimal(value)
    if normalized is None:
        raise DataError("missing or invalid decimal for %s: %r" % (label, value))
    return Decimal(normalized)


def normalize_venue(value):
    value = str(value or "").strip()
    return VENUE_ALIASES.get(value, value.upper())


def infer_instrument_type(instrument_id):
    value = str(instrument_id or "").upper()
    return "OPTION" if re.search(r"-[CP]-", value) or re.search(r"\d[CP]\d", value) else "FUTURE"


def normalize_row(raw):
    record = {}
    for header, value in raw.items():
        field = HEADER_ALIASES.get(header_key(header))
        if field:
            record[field] = canonical_decimal(value) if field in NUMERIC_FIELDS else (str(value or "").strip() or None)
    if record.get("venue"):
        record["venue"] = normalize_venue(record["venue"])
    for field in ("instrument_id", "product_id"):
        if record.get(field):
            record[field] = str(record[field]).strip()
    record["instrument_type"] = infer_instrument_type(record.get("instrument_id"))
    record["raw_json"] = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return record
