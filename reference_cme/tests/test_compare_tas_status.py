import unittest

import compare_tas_status as comparison


class TasStatusComparisonTests(unittest.TestCase):
    def test_tas_category_mapping(self):
        self.assertEqual(
            comparison.tas_categories(
                "4[TRD_TYPE];C  [INST_PHASE];6[SECUR_ST];2[HALT_REASN]"
            ),
            {"close", "implied_off", "halt_or_unavailable"},
        )
        self.assertEqual(
            comparison.tas_categories(
                "15[TRD_TYPE];I[ORD_ENT_ST];5[SECUR_ST];0[HALT_REASN]"
            ),
            {"open", "implied_on"},
        )
        self.assertEqual(
            comparison.tas_categories(
                "17[TRD_TYPE];T  [INST_PHASE];N[ORD_ENT_ST]"
            ),
            {"open", "no_cancel"},
        )

    def test_databento_category_mapping(self):
        categories, exceptional = comparison.databento_categories(
            "PRE_OPEN", "SCHEDULED", "NO_CANCEL"
        )
        self.assertEqual(categories, {"no_cancel"})
        self.assertFalse(exceptional)
        categories, exceptional = comparison.databento_categories(
            "HALT", "MARKET_EVENT", "NONE"
        )
        self.assertEqual(categories, {"halt_or_unavailable"})
        self.assertTrue(exceptional)

    def test_contract_normalization(self):
        self.assertEqual(
            comparison.tas_contract_key("ES", "ESH24", 2024), "ES:H:2024"
        )
        self.assertEqual(
            comparison.tas_contract_key("CL", "CLZ25^2", 2025), "CL:Z:2025"
        )
        self.assertEqual(
            comparison.tas_contract_key("GC", "GCM0", 2026), "GC:M:2030"
        )
        self.assertIsNone(comparison.tas_contract_key("ES", "NQH24", 2024))

    def test_matching_is_one_to_one(self):
        self.assertEqual(comparison.match_count({10, 11}, {10, 12}, 0), 1)
        self.assertEqual(comparison.match_count({10, 11}, {10, 12}, 1), 2)
        self.assertEqual(comparison.match_count({10}, {9, 11}, 1), 1)

    def test_shadow_qualifier_is_not_meaningful(self):
        self.assertFalse(comparison.meaningful_qualifiers("   [MKT_ST_IND]"))
        self.assertTrue(comparison.meaningful_qualifiers("BBO[MKT_ST_IND]"))


if __name__ == "__main__":
    unittest.main()
