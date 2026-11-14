"""OpenCTP continuous-trading session transport and normalization."""

import json
from datetime import datetime, time

from . import source
from .models import DataError, DEFAULT_SESSION_SOURCE_URL, VERSION, normalize_venue


REQUIRED_FIELDS = {
    "ExchangeID", "ProductID", "SegmentNo", "TimeBegin", "TimeEnd",
    "ProductClass", "Area",
}
EXPECTED_VENUES = {"CFFEX", "CZCE", "DCE", "GFEX", "INE", "SHFE"}
CFFEX_CONTINUOUS_SESSIONS = {
    "IF": (("09:30:00", "11:30:00"), ("13:00:00", "15:00:00")),
    "IH": (("09:30:00", "11:30:00"), ("13:00:00", "15:00:00")),
    "IC": (("09:30:00", "11:30:00"), ("13:00:00", "15:00:00")),
    "IM": (("09:30:00", "11:30:00"), ("13:00:00", "15:00:00")),
    "T": (("09:30:00", "11:30:00"), ("13:00:00", "15:15:00")),
    "TF": (("09:30:00", "11:30:00"), ("13:00:00", "15:15:00")),
    "TL": (("09:30:00", "11:30:00"), ("13:00:00", "15:15:00")),
    "TS": (("09:30:00", "11:30:00"), ("13:00:00", "15:15:00")),
}


def _clock(value, field):
    try:
        return datetime.strptime(str(value), "%H:%M:%S").time()
    except (TypeError, ValueError):
        raise DataError("OpenCTP session %s must use HH:MM:SS: %r" % (field, value))


def parse_payload(content, content_type=""):
    text = source.decode(content, content_type)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as error:
        raise DataError("OpenCTP session response is not valid JSON: %s" % error)
    if not isinstance(payload, dict) or payload.get("rsp_code") != 0:
        raise DataError(
            "OpenCTP session request failed: %s" % (
                payload.get("rsp_message", "invalid response")
                if isinstance(payload, dict) else "invalid response"
            )
        )
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise DataError("OpenCTP session response contained no rows")

    records, seen = [], set()
    for index, raw in enumerate(data):
        if not isinstance(raw, dict):
            raise DataError("OpenCTP session row %d is not an object" % index)
        missing = sorted(REQUIRED_FIELDS - set(raw))
        if missing:
            raise DataError(
                "OpenCTP session row %d is missing fields: %s"
                % (index, ", ".join(missing))
            )
        venue = normalize_venue(raw["ExchangeID"])
        product_id = str(raw["ProductID"] or "").strip()
        try:
            segment_no = int(raw["SegmentNo"])
        except (TypeError, ValueError):
            raise DataError("OpenCTP session SegmentNo is invalid: %r" % raw["SegmentNo"])
        if not venue or not product_id or segment_no < 1:
            raise DataError("OpenCTP session row %d has an invalid identity" % index)
        key = (venue.upper(), product_id.upper(), segment_no)
        if key in seen:
            raise DataError("duplicate OpenCTP session row: %s.%s.%d" % key)
        seen.add(key)

        begin = _clock(raw["TimeBegin"], "TimeBegin")
        end = _clock(raw["TimeEnd"], "TimeEnd")
        if begin == end:
            raise DataError("OpenCTP session has equal begin and end times: %s.%s.%d" % key)
        records.append({
            "venue": venue,
            "product_id": product_id,
            "segment_no": segment_no,
            "time_begin": begin.isoformat(),
            "time_end": end.isoformat(),
            "crosses_midnight": end < begin,
            "session_group": "NIGHT" if begin >= time(18, 0) else "DAY",
            "product_class": str(raw["ProductClass"]),
            "area": str(raw["Area"]),
            "is_active": True,
            "raw_json": json.dumps(
                raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
        })
    grouped = {}
    for record in records:
        if record["venue"] == "CFFEX" and record["product_id"].upper() in CFFEX_CONTINUOUS_SESSIONS:
            grouped.setdefault(record["product_id"].upper(), []).append(record)
    for product_id, rows in grouped.items():
        actual = tuple(
            (row["time_begin"], row["time_end"])
            for row in sorted(rows, key=lambda item: item["segment_no"])
        )
        expected = CFFEX_CONTINUOUS_SESSIONS[product_id]
        if actual != expected:
            raise DataError(
                "OpenCTP CFFEX %s sessions changed; expected %r, got %r; "
                "verify the exchange schedule before importing" % (
                    product_id, expected, actual,
                )
            )
    return records


def validate_full_snapshot(records):
    venues = {record["venue"] for record in records}
    missing = sorted(EXPECTED_VENUES - venues)
    if missing:
        raise DataError(
            "OpenCTP full session response is missing venues: %s" % ", ".join(missing)
        )
    products = {(row["venue"], row["product_id"].upper()) for row in records}
    missing_cffex = sorted(
        product for product in CFFEX_CONTINUOUS_SESSIONS
        if ("CFFEX", product) not in products
    )
    if missing_cffex:
        raise DataError(
            "OpenCTP full session response is missing CFFEX products: %s"
            % ", ".join(missing_cffex)
        )


def fetch(url=DEFAULT_SESSION_SOURCE_URL, timeout=30, retries=2):
    return source.fetch(
        url or DEFAULT_SESSION_SOURCE_URL,
        timeout,
        retries,
        "cn-futures-ref/%s" % VERSION,
        "application/json,*/*;q=0.8",
    )
