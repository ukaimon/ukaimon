from __future__ import annotations

import unittest

from core.batch_planner import generate_batch_plan_items


class BatchPlannerTests(unittest.TestCase):
    def test_randomized_blocks_keep_baseline_first(self) -> None:
        conditions = [
            {"condition_id": "C0", "concentration_value": 0, "planned_replicates": 3},
            {"condition_id": "C1", "concentration_value": 10, "planned_replicates": 3},
            {"condition_id": "C2", "concentration_value": 50, "planned_replicates": 3},
        ]
        items = generate_batch_plan_items(conditions, baseline_value=0, execution_mode="randomized_blocks", seed=1)
        self.assertEqual(len(items), 9)
        self.assertEqual(items[0]["condition_id"], "C0")
        self.assertEqual(items[3]["condition_id"], "C0")
        self.assertEqual(items[6]["condition_id"], "C0")

    def test_concentration_grouped_keeps_same_condition_contiguous(self) -> None:
        conditions = [
            {"condition_id": "C0", "concentration_value": 0, "planned_replicates": 3},
            {"condition_id": "C1", "concentration_value": 10, "planned_replicates": 2},
            {"condition_id": "C2", "concentration_value": 50, "planned_replicates": 1},
        ]
        items = generate_batch_plan_items(
            conditions,
            baseline_value=0,
            execution_mode="concentration_grouped",
            seed=1,
        )

        self.assertEqual(
            [(item["condition_id"], item["rep_no"]) for item in items],
            [
                ("C0", 1),
                ("C0", 2),
                ("C0", 3),
                ("C1", 1),
                ("C1", 2),
                ("C2", 1),
            ],
        )


if __name__ == "__main__":
    unittest.main()
