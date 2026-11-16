import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from starlette.testclient import TestClient

from exchange_mcp import database, server


class HttpGatewayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.context = TestClient(server.create_app(), base_url="http://127.0.0.1:8780")
        cls.client = cls.context.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.context.__exit__(None, None, None)

    def test_catalog_and_readonly_surface(self):
        catalog = self.client.get("/api/tools").json()["tools"]
        self.assertEqual(len(catalog), 13)
        for forbidden in ("sync", "import", "review", "http://localhost", "../sync"):
            self.assertNotEqual(self.client.get("/api/" + forbidden).status_code, 200)
        self.assertEqual(self.client.post("/api/cn_instruments", json={}).status_code, 405)
        self.assertEqual(self.client.get("/health").json()["readOnly"], True)
        self.assertIn("统一查询入口", self.client.get("/").text)
        self.assertIn("MCP 客户端", self.client.get("/usage.md").text)

    def test_invalid_parameters_rejected_before_upstream(self):
        cases = (
            "/api/cme_definitions?page_size=201",
            "/api/cme_definitions?page=0",
            "/api/cme_definitions?product=INVALID",
            "/api/cme_definitions?as_of=2026-02-30",
            "/api/cme_definitions?url=http://example.com",
            "/api/cme_definitions?product=ES&product=NQ",
            "/api/cn_instrument?venue=CFFEX&instrument=..%2Fsync",
            "/api/cme_status_events",
        )
        with patch.object(server, "get_json", new_callable=AsyncMock) as upstream:
            for path in cases:
                with self.subTest(path=path):
                    self.assertEqual(self.client.get(path).status_code, 400)
            upstream.assert_not_called()

    def test_cn_pagination_keeps_source_and_sorts(self):
        data = {
            "dataState": {"as_of": "2026-10-12"}, "asOf": "2026-10-12",
            "historicalExact": False, "feeScope": "BROKER_REFERENCE", "sourceType": "OPENCTP",
            "exchanges": [{"instruments": [
                {"venue": "CFFEX", "instrumentId": "IF2612"},
                {"venue": "CFFEX", "instrumentId": "IF2610"},
            ]}],
        }
        with patch.object(server, "get_json", new_callable=AsyncMock, return_value=data) as upstream:
            response = self.client.get("/api/cn_instruments?page=2&page_size=1&include_options=false")
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["records"][0]["instrumentId"], "IF2612")
        self.assertEqual((result["total"], result["pages"]), (2, 2))
        self.assertFalse(result["historicalExact"])
        self.assertEqual(result["feeScope"], "BROKER_REFERENCE")
        self.assertFalse(upstream.call_args.kwargs["includeOptions"])

    def test_database_range_rejected_before_connection(self):
        with patch.object(server, "pg_query", new_callable=AsyncMock) as query:
            for start, end in (("2026-01-02", "2026-01-01"), ("2020-01-01", "2026-01-01")):
                response = self.client.get(f"/api/cme_trading_sessions?product=ES&start_date={start}&end_date={end}")
                self.assertEqual(response.status_code, 400)
            query.assert_not_called()

    def test_mcp_initialize_discover_call_and_validation(self):
        headers = {"Accept": "application/json, text/event-stream"}
        def rpc(method, params):
            return self.client.post("/mcp", headers=headers, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        response = rpc("initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "test", "version": "1"},
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn("serverInfo", response.json()["result"])
        tools = rpc("tools/list", {}) .json()["result"]["tools"]
        self.assertEqual(len(tools), 13)
        self.assertTrue(all(tool["annotations"]["readOnlyHint"] for tool in tools))
        with patch.object(server, "get_json", new_callable=AsyncMock, return_value={"records": [], "total": 0}) as upstream:
            result = rpc("tools/call", {"name": "cme_definitions", "arguments": {"product": "ES"}}).json()["result"]
            self.assertFalse(result.get("isError", False))
            self.assertEqual(result["structuredContent"]["total"], 0)
            upstream.assert_awaited_once()
            invalid = rpc("tools/call", {"name": "cme_definitions", "arguments": {"page_size": 999}}).json()["result"]
            self.assertTrue(invalid["isError"])
            self.assertEqual(upstream.await_count, 1)

    def test_mcp_rejects_untrusted_host(self):
        response = self.client.post("/mcp", headers={"Host": "untrusted.invalid", "Accept": "application/json, text/event-stream"}, json={})
        self.assertEqual(response.status_code, 421)

    def test_browser_mcp_link_opens_instructions(self):
        for path in ("/mcp", "/mcp/"):
            response = self.client.get(path, headers={"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}, follow_redirects=False)
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers["location"], "/#mcp")
            page = self.client.get(response.headers["location"])
            self.assertEqual(page.status_code, 200)
            self.assertIn('id="mcp"', page.text)

    def test_plain_get_mcp_explains_connection(self):
        response = self.client.get("/mcp")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["transport"], "Streamable HTTP")
        self.assertEqual(response.json()["requestMethod"], "POST")

    def test_sse_get_and_protocol_post_are_forwarded_unchanged(self):
        async def check():
            downstream = AsyncMock()
            middleware = server.McpBrowserEntry(downstream)
            for method, accept in (("GET", b"text/event-stream"), ("POST", b"application/json")):
                scope = {"type": "http", "method": method, "path": "/mcp", "headers": [(b"accept", accept)]}
                receive, send = AsyncMock(), AsyncMock()
                await middleware(scope, receive, send)
                downstream.assert_awaited_with(scope, receive, send)
                send.assert_not_awaited()
        asyncio.run(check())


class DataBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_upstream_error_and_partial_availability(self):
        async def fake(source, path):
            if source == "cn":
                raise server.QueryError("CN 数据服务不可用", 502)
            return {"dataset": "GLBX.MDP3"}
        with patch.object(server, "get_json", side_effect=fake):
            result = await server.exchange_summary()
        self.assertIn("error", result["markets"]["cn"])
        self.assertEqual(result["markets"]["cme"]["dataset"], "GLBX.MDP3")

    async def test_get_request_maps_parameters_and_errors(self):
        async def handler(request):
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.url.params["activeOnly"], "false")
            self.assertEqual(request.url.params["asOf"], "2026-09-02")
            return httpx.Response(500, text="private database details")
        from datetime import date
        original_client = httpx.AsyncClient
        def mock_client(**kwargs):
            return original_client(transport=httpx.MockTransport(handler), **kwargs)
        with patch.object(server.httpx, "AsyncClient", side_effect=mock_client):
            with self.assertRaises(server.QueryError) as error:
                await server.get_json("cme", "definitions", activeOnly=False, asOf=date(2026, 9, 2))
        self.assertEqual(error.exception.status, 502)
        self.assertNotIn("private", str(error.exception))

    def test_postgres_fixed_target_readonly_and_parameterized(self):
        with patch.object(database.psycopg, "connect") as connect:
            connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
            database.commissions("GC' OR true --", None)
        self.assertEqual(connect.call_args.args[0], database.DATABASE_URL)
        self.assertIn("default_transaction_read_only=on", connect.call_args.kwargs["options"])
        connection = connect.return_value.__enter__.return_value
        sql, values = connection.execute.call_args.args
        self.assertNotIn("GC' OR", sql)
        self.assertEqual(values[0], "GC' OR true --")


if __name__ == "__main__":
    unittest.main()
