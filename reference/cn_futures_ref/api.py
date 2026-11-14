"""Local HTTP API and Chinese static application host."""

import base64
import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit

from . import services, storage
from .models import DataError, SOURCE_TYPE, VENUE_NAMES, VERSION


STATIC_ROOT = Path(__file__).with_name("static")
MAX_REQUEST_BYTES = 16 * 1024 * 1024


def _flag(value, default=False):
    if value is None:
        return default
    return str(value).lower() in ("1", "true", "yes", "on")


def _instrument(record):
    return {
        "venue": record["venue"], "instrumentId": record["instrument_id"],
        "instrumentName": record.get("instrument_name"),
        "productId": record["product_id"], "productName": record.get("product_name"),
        "instrumentType": record["instrument_type"],
        "contractMultiplier": record.get("contract_multiplier"),
        "priceTick": record.get("price_tick"), "latestPrice": record.get("latest_price"),
        "previousSettlementPrice": record.get("previous_settlement_price"),
        "volume": record.get("volume"), "openInterest": record.get("open_interest"),
        "sourceDate": record.get("source_date"), "startTs": record.get("start_ts"),
        "updateTs": record.get("update_ts"),
        "fees": {
            "open": {"rate": record.get("open_rate_by_money"), "perLot": record.get("open_fee_per_lot")},
            "close": {"rate": record.get("close_rate_by_money"), "perLot": record.get("close_fee_per_lot")},
            "closeToday": {"rate": record.get("close_today_rate_by_money"), "perLot": record.get("close_today_fee_per_lot")},
        },
        "margin": {
            "longRate": record.get("long_margin_rate_by_money"),
            "shortRate": record.get("short_margin_rate_by_money"),
        },
        "appliedOverrides": record.get("applied_overrides", []),
    }


def overview(database, as_of=None, include_options=True, venue=None, product=None, search=None):
    connection = storage.connect(database)
    try:
        state, records = services.records(
            connection, as_of, venue, product, None, include_options
        )
    finally:
        connection.close()
    needle = (search or "").strip().upper()
    if needle:
        records = [
            record for record in records
            if needle in record["instrument_id"].upper()
            or needle in record["product_id"].upper()
            or needle in (record.get("instrument_name") or "").upper()
        ]
    grouped = {}
    for record in records:
        group = grouped.setdefault(record["venue"], {
            "code": record["venue"],
            "name": VENUE_NAMES.get(record["venue"], record["venue"]),
            "instruments": [], "products": set(), "futures": 0, "options": 0,
        })
        group["instruments"].append(_instrument(record))
        group["products"].add(record["product_id"])
        group["options" if record["instrument_type"] == "OPTION" else "futures"] += 1
    exchanges = []
    for code in sorted(grouped):
        group = grouped[code]
        group["products"] = sorted(group["products"])
        group["count"] = len(group["instruments"])
        exchanges.append(group)
    return {
        "dataState": state, "asOf": state["as_of"], "historicalExact": False,
        "feeScope": "BROKER_REFERENCE", "sourceType": SOURCE_TYPE,
        "summary": {
            "instruments": len(records), "exchanges": len(exchanges),
            "futures": sum(group["futures"] for group in exchanges),
            "options": sum(group["options"] for group in exchanges),
            "instrumentVersions": state["version_count"],
        },
        "exchanges": exchanges,
    }


class ReferenceApiHandler(BaseHTTPRequestHandler):
    server_version = "CnFuturesReference/%s" % VERSION

    def log_message(self, format_string, *args):
        return

    @property
    def database(self):
        return self.server.database

    def _json(self, value, status=HTTPStatus.OK):
        content = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def _text(self, value, content_type, filename=None):
        content = value.encode("utf-8-sig")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        if filename:
            self.send_header("Content-Disposition", 'attachment; filename="%s"' % filename)
        self.end_headers()
        self.wfile.write(content)

    def _binary(self, content, content_type, filename=None):
        content = bytes(content)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "private, max-age=3600")
        if filename:
            safe_name = str(filename).replace('"', "").replace("\r", "").replace("\n", "")
            encoded = quote(safe_name.encode("utf-8"))
            self.send_header(
                "Content-Disposition", "inline; filename*=UTF-8''%s" % encoded
            )
        self.end_headers()
        self.wfile.write(content)

    def _error(self, status, message):
        self._json({"error": {"status": status, "message": message}}, status)

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        if length > MAX_REQUEST_BYTES:
            raise DataError("request body is too large")
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise DataError("request body must be UTF-8 JSON")

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("Allow", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        try:
            parsed = urlsplit(self.path)
            if parsed.path.startswith("/api/v1/"):
                return self._get_api(parsed.path, parse_qs(parsed.query))
            return self._static(parsed.path)
        except DataError as error:
            self._error(HTTPStatus.BAD_REQUEST, str(error))
        except Exception as error:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_POST(self):
        try:
            path = urlsplit(self.path).path
            if not path.startswith("/api/v1/"):
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            return self._post_api(path, self._body())
        except DataError as error:
            self._error(HTTPStatus.BAD_REQUEST, str(error))
        except Exception as error:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def _get_api(self, path, query):
        value = lambda key, default=None: query.get(key, [default])[0]
        as_of, options = value("asOf"), _flag(value("includeOptions"), True)
        if path == "/api/v1/health":
            return self._json({"status": "ok", "sourceType": SOURCE_TYPE})
        if path == "/api/v1/schema":
            return self._json({
                "schema": "cn-futures-reference-api/v1",
                "storage": "incremental-instrument-versions",
                "endpoints": [
                    "GET /api/v1/overview", "GET /api/v1/data-state",
                    "GET /api/v1/exchanges/{venue}",
                    "GET /api/v1/instruments/{venue}/{instrument}",
                    "GET /api/v1/exports/exchange-info",
                    "GET /api/v1/exports/records.csv", "POST /api/v1/sync",
                    "GET /api/v1/trading-sessions",
                    "POST /api/v1/trading-sessions/sync",
                    "POST /api/v1/import", "POST /api/v1/overrides/import",
                    "POST /api/v1/fees/estimate", "POST /api/v1/fees/round-trip",
                    "GET /api/v1/announcements",
                    "POST /api/v1/announcements/refresh",
                    "GET /api/v1/announcements/{id}/raw",
                    "GET /api/v1/announcement-assets/{id}",
                    "GET /api/v1/message-fee-rules",
                    "POST /api/v1/message-fee-rules/{id}/review",
                    "POST /api/v1/message-fee-rules/import",
                    "POST /api/v1/message-fee/calculate",
                ],
            })
        if path == "/api/v1/data-state":
            connection = storage.connect(self.database)
            try:
                state = storage.data_state(connection, as_of)
            finally:
                connection.close()
            return self._json(state)
        if path == "/api/v1/trading-sessions":
            connection = storage.connect(self.database)
            try:
                state, rows = services.continuous_sessions(
                    connection, as_of, value("venue"), value("product")
                )
            finally:
                connection.close()
            return self._json({
                "dataState": state,
                "timezone": "Asia/Shanghai",
                "sessions": rows,
            })
        if path == "/api/v1/overview":
            return self._json(
                overview(self.database, as_of, options, value("venue"), value("product"), value("search"))
            )
        if path == "/api/v1/announcements":
            return self._json(services.announcement_dashboard(
                self.database, value("venue"), int(value("limit", 100))
            ))
        if path.startswith("/api/v1/announcements/") and path.endswith("/raw"):
            parts = path.split("/")
            if len(parts) != 6 or not parts[4].isdigit():
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            connection = storage.connect(self.database)
            try:
                raw = storage.announcement_raw(connection, int(parts[4]))
            finally:
                connection.close()
            return self._binary(
                raw["raw_content"], raw["content_type"], "%s.html" % raw["title"]
            )
        if path.startswith("/api/v1/announcement-assets/"):
            asset_id = path.rsplit("/", 1)[1]
            if not asset_id.isdigit():
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            connection = storage.connect(self.database)
            try:
                asset = storage.announcement_asset_raw(connection, int(asset_id))
            finally:
                connection.close()
            return self._binary(
                asset["raw_content"], asset["content_type"], asset["file_name"]
            )
        if path == "/api/v1/message-fee-rules":
            connection = storage.connect(self.database)
            try:
                rules = storage.list_message_fee_rules(connection, value("status"))
            finally:
                connection.close()
            return self._json({"rules": rules})
        if path.startswith("/api/v1/exchanges/"):
            venue = unquote(path.rsplit("/", 1)[1])
            return self._json(
                overview(self.database, as_of, options, venue, value("product"), value("search"))
            )
        if path.startswith("/api/v1/instruments/"):
            parts = path.split("/")
            if len(parts) != 6:
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            connection = storage.connect(self.database)
            try:
                state, record = services.one_record(
                    connection, as_of, unquote(parts[4]), unquote(parts[5])
                )
            finally:
                connection.close()
            return self._json({
                "dataState": state, "instrument": _instrument(record),
                "exchangeInfo": services.exchange_info(record),
            })
        if path == "/api/v1/exports/exchange-info":
            connection = storage.connect(self.database)
            try:
                document = services.export_document(
                    connection, as_of, value("venue"), value("product"), options
                )
            finally:
                connection.close()
            return self._json(document)
        if path == "/api/v1/exports/records.csv":
            connection = storage.connect(self.database)
            try:
                _, records = services.records(
                    connection, as_of, value("venue"), value("product"), None, options
                )
            finally:
                connection.close()
            return self._text(
                services.records_csv(records), "text/csv; charset=utf-8", "cn_futures_records.csv"
            )
        return self._error(HTTPStatus.NOT_FOUND, "not found")

    def _post_api(self, path, body):
        if path == "/api/v1/trading-sessions/sync":
            result = services.sync_continuous_sessions(
                self.database, body.get("sourceUrl") or None,
                source_date=body.get("sourceDate"),
                timeout=max(1, min(int(body.get("timeout", 30)), 120)),
                retries=max(0, min(int(body.get("retries", 2)), 5)),
            )
            return self._json(
                result, HTTPStatus.CREATED if result["inserted"] else HTTPStatus.OK
            )
        if path == "/api/v1/announcements/refresh":
            result = services.refresh_announcements(
                self.database,
                page_count=max(1, min(int(body.get("pageCount", 5)), 10)),
                per_source_limit=max(1, min(int(body.get("perSourceLimit", 8)), 50)),
                analysis_limit=max(1, min(int(body.get("analysisLimit", 20)), 100)),
                timeout=max(5, min(int(body.get("timeout", 12)), 120)),
                retries=max(0, min(int(body.get("retries", 0)), 3)),
            )
            return self._json(result, HTTPStatus.CREATED)
        if path == "/api/v1/message-fee-rules/import":
            encoded = body.get("contentBase64")
            if not isinstance(encoded, str):
                raise DataError("contentBase64 is required")
            try:
                text = base64.b64decode(encoded, validate=True).decode("utf-8-sig")
            except (ValueError, UnicodeDecodeError):
                raise DataError("contentBase64 must contain UTF-8 JSON")
            connection = storage.connect(self.database)
            try:
                result = services.import_message_fee_rules(connection, text)
            finally:
                connection.close()
            return self._json(result, HTTPStatus.CREATED if result["inserted"] else HTTPStatus.OK)
        if path == "/api/v1/message-fee/calculate":
            encoded = body.get("contentBase64")
            if not isinstance(encoded, str):
                raise DataError("contentBase64 is required")
            try:
                text = base64.b64decode(encoded, validate=True).decode("utf-8-sig")
            except (ValueError, UnicodeDecodeError):
                raise DataError("contentBase64 must contain UTF-8 CSV")
            connection = storage.connect(self.database)
            try:
                result = services.calculate_message_fees(connection, text)
            finally:
                connection.close()
            return self._json(result)
        if path.startswith("/api/v1/message-fee-rules/") and path.endswith("/review"):
            parts = path.split("/")
            if len(parts) != 6 or not parts[4].isdigit():
                return self._error(HTTPStatus.NOT_FOUND, "not found")
            return self._json(services.review_message_fee_rule(
                self.database, int(parts[4]), str(body.get("status") or "").upper(),
                body.get("note"),
            ))
        if path == "/api/v1/sync":
            result = services.sync(
                self.database, body.get("sourceUrl") or None,
                source_date=body.get("sourceDate"), timeout=int(body.get("timeout", 30)),
                retries=int(body.get("retries", 2)),
            )
            return self._json(result, HTTPStatus.CREATED if result["inserted"] else HTTPStatus.OK)
        if path == "/api/v1/import":
            encoded = body.get("contentBase64")
            if not isinstance(encoded, str):
                raise DataError("contentBase64 is required")
            try:
                content = base64.b64decode(encoded, validate=True)
            except ValueError:
                raise DataError("contentBase64 is invalid")
            if len(content) > MAX_REQUEST_BYTES:
                raise DataError("decoded import is too large")
            filename = str(body.get("fileName") or "upload")
            result = services.import_content(
                self.database, content, body.get("format"),
                body.get("sourceUrl") or "browser-upload://%s" % filename,
                body.get("sourceDate"),
            )
            return self._json(result, HTTPStatus.CREATED if result["inserted"] else HTTPStatus.OK)
        if path == "/api/v1/overrides/import":
            encoded = body.get("contentBase64")
            if not isinstance(encoded, str):
                raise DataError("contentBase64 is required")
            try:
                text = base64.b64decode(encoded, validate=True).decode("utf-8-sig")
            except (ValueError, UnicodeDecodeError):
                raise DataError("contentBase64 must contain UTF-8 CSV")
            connection = storage.connect(self.database)
            try:
                count = services.import_overrides_csv(connection, text)
            finally:
                connection.close()
            return self._json({"status": "ok", "rowsProcessed": count})
        if path in ("/api/v1/fees/estimate", "/api/v1/fees/round-trip"):
            connection = storage.connect(self.database)
            try:
                state, record = services.one_record(
                    connection, body.get("asOf"), body.get("venue"), body.get("instrument")
                )
            finally:
                connection.close()
            if path.endswith("estimate"):
                result = services.fee(
                    record, services.select_price(record, body.get("price")),
                    body.get("quantity", "1"), body.get("positionEffect"),
                )
            else:
                result = services.round_trip(
                    record, body.get("entryPrice"), body.get("exitPrice"),
                    body.get("quantity", "1"), body.get("exitEffect", "close_today"),
                    body.get("spreadTicks", "1"), body.get("slippageTicks", "0"),
                    body.get("extraCost", "0"),
                )
            result.update({
                "dataState": state, "venue": record["venue"],
                "instrumentId": record["instrument_id"],
                "appliedOverrides": record["applied_overrides"],
            })
            return self._json(result)
        return self._error(HTTPStatus.NOT_FOUND, "not found")

    def _static(self, request_path):
        relative = "index.html" if request_path in ("", "/") else request_path.lstrip("/")
        candidate = (STATIC_ROOT / relative).resolve()
        try:
            candidate.relative_to(STATIC_ROOT.resolve())
        except ValueError:
            return self._error(HTTPStatus.NOT_FOUND, "not found")
        if not candidate.is_file():
            return self._error(HTTPStatus.NOT_FOUND, "not found")
        content = candidate.read_bytes()
        content_type = mimetypes.guess_type(str(candidate))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "%s%s" % (
            content_type,
            "; charset=utf-8" if content_type.startswith("text/") or content_type == "application/javascript" else "",
        ))
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)


class ReferenceServer(ThreadingHTTPServer):
    def __init__(self, address, database):
        ThreadingHTTPServer.__init__(self, address, ReferenceApiHandler)
        self.database = str(database)
