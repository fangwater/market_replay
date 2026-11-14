import json
import unittest

from cn_futures_ref.models import DataError
from cn_futures_ref.trading_sessions import parse_payload, validate_full_snapshot


def payload(rows):
    return json.dumps({
        "rsp_code": 0,
        "rsp_message": "succeed",
        "data": rows,
    }).encode("utf-8")


def row(product, segment, begin, end, venue="CFFEX"):
    return {
        "ExchangeID": venue,
        "ProductID": product,
        "SegmentNo": segment,
        "TimeBegin": begin,
        "TimeEnd": end,
        "ProductClass": "1",
        "Area": "China",
    }


class TradingSessionParsingTests(unittest.TestCase):
    def test_keeps_stock_index_and_treasury_closes_distinct(self):
        records = parse_payload(payload([
            row("IF", 1, "09:30:00", "11:30:00"),
            row("IF", 2, "13:00:00", "15:00:00"),
            row("T", 1, "09:30:00", "11:30:00"),
            row("T", 2, "13:00:00", "15:15:00"),
        ]))
        closes = {
            record["product_id"]: record["time_end"]
            for record in records if record["segment_no"] == 2
        }
        self.assertEqual(closes, {"IF": "15:00:00", "T": "15:15:00"})

    def test_marks_a_night_session_that_crosses_midnight(self):
        records = parse_payload(payload([
            row("cu", 1, "21:00:00", "01:00:00", "SHFE"),
        ]))
        self.assertTrue(records[0]["crosses_midnight"])
        self.assertEqual(records[0]["session_group"], "NIGHT")

    def test_rejects_unverified_cffex_schedule_change(self):
        with self.assertRaisesRegex(DataError, "verify the exchange schedule"):
            parse_payload(payload([
                row("IF", 1, "09:30:00", "11:30:00"),
                row("IF", 2, "13:00:00", "15:15:00"),
            ]))

    def test_full_snapshot_requires_all_venues(self):
        records = parse_payload(payload([
            row("cu", 1, "21:00:00", "01:00:00", "SHFE"),
        ]))
        with self.assertRaisesRegex(DataError, "missing venues"):
            validate_full_snapshot(records)


if __name__ == "__main__":
    unittest.main()
