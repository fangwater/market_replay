import unittest

from cme_reference import repository


class RepositoryIntegrationTests(unittest.TestCase):
    def test_summary_has_only_core_six(self):
        summary = repository.application_summary()
        self.assertEqual(
            [item["code"] for item in summary["products"]],
            ["ES", "NQ", "RTY", "YM", "GC", "CL"],
        )
        self.assertEqual(summary["definition"]["uniqueSymbols"], 456)
        self.assertEqual(summary["status"]["rows"], 2_746_124)

    def test_point_in_time_definition_and_tick_value(self):
        result = repository.definitions(
            "2026-09-03", "ES", "ESU6", True, "1", "20"
        )
        self.assertEqual(result["total"], 1)
        record = result["records"][0]
        self.assertEqual(record["raw_symbol"], "ESU6")
        self.assertEqual(record["min_price_increment"], 0.25)
        self.assertEqual(record["unit_of_measure_qty"], 50.0)
        self.assertEqual(record["tick_value"], 12.5)

    def test_status_query_returns_timeline(self):
        symbols = repository.status_symbols("ES", "2026-09-02")
        self.assertIn("ESU6", [record["symbol"] for record in symbols["records"]])
        result = repository.status_events(
            "ES", "2026-09-02", "ESU6", "1", "1000"
        )
        self.assertEqual(result["total"], 6)
        self.assertEqual(result["records"][0]["action_name"], "TRADING")
        self.assertTrue(result["records"][0]["is_midnight_snapshot"])

    def test_invalid_product_is_rejected(self):
        with self.assertRaises(repository.QueryError):
            repository.status_events("SR3", "2026-09-02", None, "1", "100")


if __name__ == "__main__":
    unittest.main()
