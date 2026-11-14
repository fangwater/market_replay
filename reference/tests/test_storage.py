import json
import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal

from cn_futures_ref.storage import row_dict


class StorageSerializationTests(unittest.TestCase):
    def test_serializes_all_date_and_datetime_columns(self):
        value = row_dict({
            "published_date": date(2026, 9, 2),
            "finished_at": datetime(2026, 9, 2, 10, 30, tzinfo=timezone.utc),
            "time_end": time(15, 15),
            "otr_threshold": Decimal("2.000"),
        })
        self.assertEqual(value["published_date"], "2026-09-02")
        self.assertEqual(value["finished_at"], "2026-09-02T10:30:00+00:00")
        self.assertEqual(value["time_end"], "15:15:00")
        self.assertEqual(value["otr_threshold"], "2")
        json.dumps(value)


if __name__ == "__main__":
    unittest.main()
