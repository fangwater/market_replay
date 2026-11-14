import json
import unittest

from cn_futures_ref.message_fees import calculate, normalize_rule, parse_order_events
from cn_futures_ref.models import DataError


def candidate_rule():
    return {
        "rule_id": "shfe_hc_option_20260910",
        "exchange": "SHFE",
        "products": ["HC"],
        "instrument_type": "OPTION",
        "effective_from": "2026-09-10",
        "effective_to": None,
        "aggregation_scope": "trading_day+account+instrument",
        "information_volume": {"new_order": 1, "cancel": 1, "inquiry": 0, "other": 0},
        "otr_formula": "information_volume / filled_order_count - 1",
        "otr_threshold": 2,
        "zero_fill_denominator": 1,
        "tiers": [
            {"min": 1, "max": 2, "rate_otr_le_threshold": "0", "rate_otr_gt_threshold": "0"},
            {"min": 3, "max": None, "rate_otr_le_threshold": "1", "rate_otr_gt_threshold": "3"},
        ],
        "exemptions": [],
        "source_url": "https://www.shfe.com.cn/notice.html",
        "source_sha256": "a" * 64,
        "review_status": "VERIFIED",
    }


class MessageFeeTests(unittest.TestCase):
    def normalized_rule(self):
        rule = normalize_rule(candidate_rule())
        rule.update({
            "id": 7,
            "products": json.loads(rule["products"]),
            "information_volume": json.loads(rule["information_volume"]),
            "tiers": json.loads(rule["tiers"]),
        })
        return rule

    def test_progressive_tiers_and_otr(self):
        events = []
        for index in range(4):
            events.append({
                "trading_day": "2026-09-10", "account": "A1", "exchange": "SHFE",
                "instrument": "HC2612-C-3500", "event_type": "new_order",
                "order_id": "O%d" % index, "filled_quantity": 1 if index == 0 else 0,
            })
        result = calculate(events, [self.normalized_rule()])
        self.assertEqual(result["groups"][0]["otr"], "3")
        self.assertEqual(result["groups"][0]["messageFee"], "6")
        self.assertEqual(result["totalMessageFee"], "6")

    def test_csv_aliases_and_zero_fill_denominator(self):
        text = "交易日,账户,交易所,合约代码,事件类型,报单编号,成交量\n2026-09-10,A1,SHFE,HC2612-C-3500,报单,O1,0\n"
        events = parse_order_events(text)
        result = calculate(events, [self.normalized_rule()])
        self.assertEqual(result["groups"][0]["otr"], "0")
        self.assertEqual(result["groups"][0]["messageFee"], "0")

    def test_rejects_non_contiguous_tiers(self):
        rule = candidate_rule()
        rule["tiers"][1]["min"] = 4
        with self.assertRaises(DataError):
            normalize_rule(rule)

    def test_accepts_rates_copied_with_official_units(self):
        rule = candidate_rule()
        rule["tiers"][1]["rate_otr_gt_threshold"] = "3元/笔"
        normalized = normalize_rule(rule)
        tiers = json.loads(normalized["tiers"])
        self.assertEqual(tiers[1]["rate_otr_gt_threshold"], "3")


if __name__ == "__main__":
    unittest.main()
