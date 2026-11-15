from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__, repository


STATIC_ROOT = Path(__file__).with_name("static")


def _flag(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


class ReferenceHandler(BaseHTTPRequestHandler):
    server_version = f"CmeReference/{__version__}"

    def log_message(self, format_string: str, *args: object) -> None:
        return

    def _json(self, value: object, status: HTTPStatus = HTTPStatus.OK) -> None:
        content = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def _error(self, status: HTTPStatus, message: str) -> None:
        self._json({"error": {"status": int(status), "message": message}}, status)

    def do_GET(self) -> None:
        try:
            parsed = urlsplit(self.path)
            if parsed.path.startswith("/api/v1/"):
                self._api(parsed.path, parse_qs(parsed.query))
            else:
                self._static(parsed.path)
        except repository.QueryError as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        except FileNotFoundError as exc:
            self._error(HTTPStatus.SERVICE_UNAVAILABLE, f"数据文件不存在：{exc}")
        except Exception as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def _api(self, path: str, query: dict[str, list[str]]) -> None:
        value = lambda key, default=None: query.get(key, [default])[0]
        if path == "/api/v1/health":
            self._json({"status": "ok", "version": __version__})
            return
        if path == "/api/v1/summary":
            self._json(repository.application_summary())
            return
        if path == "/api/v1/definitions/overview":
            self._json(repository.definition_overview(value("asOf")))
            return
        if path == "/api/v1/definitions":
            self._json(
                repository.definitions(
                    value("asOf"),
                    value("product"),
                    value("search"),
                    _flag(value("activeOnly"), True),
                    value("page"),
                    value("pageSize"),
                )
            )
            return
        if path.startswith("/api/v1/definitions/"):
            symbol = unquote(path.rsplit("/", 1)[1])
            self._json(repository.definition_history(symbol, value("asOf")))
            return
        if path == "/api/v1/status/symbols":
            self._json(repository.status_symbols(value("product"), value("date")))
            return
        if path == "/api/v1/status":
            self._json(
                repository.status_events(
                    value("product"),
                    value("date"),
                    value("symbol"),
                    value("page"),
                    value("pageSize"),
                )
            )
            return
        self._error(HTTPStatus.NOT_FOUND, "接口不存在")

    def _static(self, raw_path: str) -> None:
        relative = unquote(raw_path).lstrip("/") or "index.html"
        target = (STATIC_ROOT / relative).resolve()
        if STATIC_ROOT.resolve() not in target.parents and target != STATIC_ROOT.resolve():
            self._error(HTTPStatus.NOT_FOUND, "页面不存在")
            return
        if not target.is_file():
            target = STATIC_ROOT / "index.html"
        content = target.read_bytes()
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in ("application/javascript", "application/json"):
            content_type += "; charset=utf-8"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(content)


class ReferenceServer(ThreadingHTTPServer):
    daemon_threads = True
