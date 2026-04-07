from __future__ import annotations

import unittest

from gui.navigation import RECORD_TYPE_TO_TAB_TITLE, resolve_navigation_target


class NavigationTests(unittest.TestCase):
    def test_session_usage_id_navigates_to_mip_usage(self) -> None:
        target = resolve_navigation_target(
            "session",
            "mip_usage_id",
            {"session_id": "SES-20260407-0001-AA", "mip_usage_id": "MUSE-20260407-0001-BB"},
        )
        self.assertEqual(target, ("mip_usage", "MUSE-20260407-0001-BB"))

    def test_batch_plan_blank_assigned_measurement_does_not_navigate(self) -> None:
        target = resolve_navigation_target(
            "batch_plan",
            "assigned_measurement_id",
            {"assigned_measurement_id": ""},
        )
        self.assertIsNone(target)

    def test_cross_report_condition_id_navigates_to_condition(self) -> None:
        target = resolve_navigation_target(
            "cross_report",
            "condition_id",
            {"condition_id": "COND-20260407-0001-CC"},
        )
        self.assertEqual(target, ("condition", "COND-20260407-0001-CC"))

    def test_record_type_to_tab_title_contains_measurement(self) -> None:
        self.assertEqual(RECORD_TYPE_TO_TAB_TITLE["measurement"], "測定追加")


if __name__ == "__main__":
    unittest.main()
