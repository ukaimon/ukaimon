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


if __name__ == "__main__":
    unittest.main()
