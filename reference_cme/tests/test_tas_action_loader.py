import unittest

from load_tas_actions_postgres import Partition, parse_partition_output
from compare_tas_actions_postgres import align_contract_actions, event_metric
from tas_action_mapping import MAPPING_VERSION, ActionMapping, action_mappings


class TasActionLoaderTests(unittest.TestCase):
    def test_direct_close_and_pre_open_mappings(self):
        self.assertEqual(
            action_mappings("4[TRD_TYPE];C  [INST_PHASE]"),
            (ActionMapping("CLOSE", False, "TRD_TYPE=4;INST_PHASE=C"),),
        )
        self.assertEqual(
            action_mappings("21[TRD_TYPE];O[INST_PHASE]"),
            (ActionMapping("PRE_OPEN", False, "TRD_TYPE=21;INST_PHASE=O"),),
        )

    def test_open_statuses_keep_their_direct_cme_meanings(self):
        self.assertEqual(
            action_mappings("17[TRD_TYPE];T[INST_PHASE]"),
            (
                ActionMapping("TRADING", False, "TRD_TYPE=17;INST_PHASE=T"),
            ),
        )
        self.assertEqual(
            action_mappings("15[TRD_TYPE]"),
            (ActionMapping("NEW_PRICE_INDICATION", False, "TRD_TYPE=15"),),
        )
        self.assertEqual(MAPPING_VERSION, "cme_tas_action_v2")

    def test_irrelevant_qualifiers_do_not_emit_phase_actions(self):
        self.assertEqual(
            action_mappings("N[ORD_ENT_ST];5[SECUR_ST];2[HALT_REASN]"), ()
        )

    def test_partition_output_is_deduplicated_and_sorted(self):
        self.assertEqual(
            parse_partition_output(
                "year\tproduct\tperiod\n"
                "2025\tYM\t2025-01-01_2026-01-01\n"
                "2024\tES\t2024-01-01_2025-01-01\n"
                "2025\tYM\t2025-01-01_2026-01-01\n"
            ),
            [
                Partition(2024, "ES", "2024-01-01_2025-01-01"),
                Partition(2025, "YM", "2025-01-01_2026-01-01"),
            ],
        )

    def test_precision_and_recall_use_independent_denominators(self):
        metric = event_metric(
            {("ES", "CLOSE"): {10, 20, 30}},
            {("ES", "CLOSE"): {10, 21}},
            [("ES", "CLOSE")],
            1,
        )
        self.assertEqual(metric["exactMatches"], 1)
        self.assertEqual(metric["matchesWithinTolerance"], 2)
        self.assertEqual(metric["precisionWithinTolerancePct"], 66.6667)
        self.assertEqual(metric["recallWithinTolerancePct"], 100.0)

    def test_open_statuses_are_clustered_into_one_aligned_boundary(self):
        aligned = align_contract_actions(
            {
                ("CL:F:2025", "NEW_PRICE_INDICATION"): {100, 200},
                ("CL:F:2025", "TRADING"): {103, 210},
                ("CL:F:2025", "CLOSE"): {50},
            }
        )
        self.assertEqual(
            aligned["CL:F:2025", "NEW_PRICE_INDICATION"], {100, 200, 210}
        )
        self.assertEqual(aligned["CL:F:2025", "TRADING"], {100, 200, 210})
        self.assertEqual(aligned["CL:F:2025", "CLOSE"], {50})


if __name__ == "__main__":
    unittest.main()
