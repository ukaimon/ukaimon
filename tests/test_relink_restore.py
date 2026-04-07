from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices
from gui.restore_tab import filter_restore_rows


class RelinkRestoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_relink_restore_{uuid4().hex[:8]}.db"
        config = AppConfig.load(self.root / "config" / "config.example.json")
        repository = ElectrochemRepository(DatabaseManager(self.database_path))
        self.services = AppServices(self.root, config, repository)
        self.services.initialize()

    def tearDown(self) -> None:
        try:
            if self.database_path.exists():
                self.database_path.unlink()
        except PermissionError:
            pass

    def _create_base_records(self) -> tuple[str, str, str, str]:
        mip_id = self.services.create_mip(
            {
                "template_name": "temp",
                "preparation_date": "2026-04-07",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )
        usage_id = self.services.create_mip_usage(
            {
                "mip_id": mip_id,
                "cp_preparation_date": "2026-04-07",
                "coating_date": "2026-04-07",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )
        session_id = self.services.create_session(
            {
                "mip_usage_id": usage_id,
                "session_date": "2026-04-07",
                "analyte": "dopamine",
                "session_name": "session-a",
                "method_default": "CV",
                "operator": "tester",
                "electrolyte": "",
                "common_note": "",
                "tags": "",
                "status": "draft",
            }
        )
        condition_id = self.services.create_condition(
            {
                "session_id": session_id,
                "concentration_value": 0.0,
                "concentration_unit": "ppm",
                "method": "CV",
                "planned_replicates": 1,
                "common_note": "",
                "tags": "",
            }
        )
        return mip_id, usage_id, session_id, condition_id

    def test_relink_measurement_moves_assignment(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        second_condition_id = self.services.create_condition(
            {
                "session_id": session_id,
                "concentration_value": 10.0,
                "concentration_unit": "ppm",
                "method": "CV",
                "planned_replicates": 1,
                "common_note": "",
                "tags": "",
            }
        )
        self.services.generate_batch_plan(session_id, 0.0, "fixed")
        batch_items = self.services.list_batch_items(session_id)
        source_batch = next(item for item in batch_items if item["condition_id"] == condition_id)
        target_batch = next(item for item in batch_items if item["condition_id"] == second_condition_id)

        measurement_id = self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "batch_item_id": source_batch["batch_item_id"],
                "rep_no": source_batch["rep_no"],
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "raw_file_path": str(self.root / "idsサンプル" / "1527_260402CV_P33107.ids"),
                "free_memo": "",
            }
        )
        self.services.repository.update_batch_item_status(
            source_batch["batch_item_id"],
            "completed",
            assigned_measurement_id=measurement_id,
        )

        self.services.relink_measurement(measurement_id, target_batch["batch_item_id"])
        measurement = self.services.repository.get_record("measurements", measurement_id)
        source_after = self.services.repository.get_record("batch_plan_items", source_batch["batch_item_id"])
        target_after = self.services.repository.get_record("batch_plan_items", target_batch["batch_item_id"])

        self.assertEqual(measurement["condition_id"], second_condition_id)
        self.assertEqual(measurement["batch_item_id"], target_batch["batch_item_id"])
        self.assertEqual(measurement["rep_no"], target_batch["rep_no"])
        self.assertEqual(source_after["planned_status"], "waiting")
        self.assertIsNone(source_after["assigned_measurement_id"])
        self.assertEqual(target_after["planned_status"], "completed")
        self.assertEqual(target_after["assigned_measurement_id"], measurement_id)

    def test_condition_warnings_are_reported_in_aggregate_note(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        for rep_no, scan_rate in enumerate((0.05, 0.1), start=1):
            measurement_id = self.services.create_measurement(
                {
                    "session_id": session_id,
                    "condition_id": condition_id,
                    "rep_no": rep_no,
                    "chip_id": f"chip-{rep_no}",
                    "wire_id": f"wire-{rep_no}",
                    "status": "manual",
                    "noise_level": 0.1,
                    "free_memo": "",
                }
            )
            self.services.repository.insert_record(
                "measurement_conditions",
                {
                    "condition_param_id": f"MCOND-{rep_no}",
                    "measurement_id": measurement_id,
                    "method": "CV",
                    "scan_rate_v_s": scan_rate,
                    "potential_start_v": -0.2,
                    "potential_end_v": 0.6,
                    "raw_header_text": "",
                    "note": "",
                },
            )
        warnings = self.services.get_condition_warnings(session_id)
        aggregates = self.services.aggregate_session(session_id)
        aggregate = next(item for item in aggregates if item["condition_id"] == condition_id)

        self.assertIn("scan_rate", warnings[condition_id])
        self.assertIn("scan_rate", aggregate["note"])

    def test_restore_session_revives_condition_visibility(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "free_memo": "",
            }
        )
        self.services.delete_session(session_id)
        message = self.services.restore_deleted_record("session", session_id)

        self.assertIn("復元", message)
        self.assertTrue(any(row["session_id"] == session_id for row in self.services.list_sessions()))
        self.assertTrue(any(row["condition_id"] == condition_id for row in self.services.list_conditions()))

    def test_restore_measurement_reconnects_batch_item(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "fixed")
        batch_item = self.services.list_batch_items(session_id)[0]
        measurement_id = self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "batch_item_id": batch_item["batch_item_id"],
                "rep_no": batch_item["rep_no"],
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "free_memo": "",
            }
        )
        self.services.repository.update_batch_item_status(
            batch_item["batch_item_id"],
            "completed",
            assigned_measurement_id=measurement_id,
        )
        self.services.repository.insert_record(
            "measurement_conditions",
            {
                "condition_param_id": "MCOND-restore",
                "measurement_id": measurement_id,
                "method": "CV",
                "raw_header_text": "",
                "note": "",
            },
        )
        self.services.delete_measurement(measurement_id)
        message = self.services.restore_deleted_record("measurement", measurement_id)
        measurement = self.services.repository.get_record("measurements", measurement_id)
        batch_item_after = self.services.repository.get_record("batch_plan_items", batch_item["batch_item_id"])

        self.assertIn("復元", message)
        self.assertEqual(measurement["is_deleted"], 0)
        self.assertEqual(batch_item_after["planned_status"], "completed")
        self.assertEqual(batch_item_after["assigned_measurement_id"], measurement_id)

    def test_restore_mip_revives_related_records(self) -> None:
        mip_id, usage_id, session_id, condition_id = self._create_base_records()
        self.services.delete_mip(mip_id)
        message = self.services.restore_deleted_record("mip", mip_id)

        self.assertIn("復元", message)
        self.assertTrue(any(row["mip_id"] == mip_id for row in self.services.list_mips()))
        self.assertTrue(any(row["mip_usage_id"] == usage_id for row in self.services.list_mip_usages()))
        self.assertTrue(any(row["session_id"] == session_id for row in self.services.list_sessions()))
        self.assertTrue(any(row["condition_id"] == condition_id for row in self.services.list_conditions()))

    def test_measurement_header_summary_uses_ids_metadata(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        measurement_id = self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "raw_file_path": str(self.root / "idsサンプル" / "1527_260402CV_P33107.ids"),
                "free_memo": "",
            }
        )

        summary = self.services.get_measurement_header_summary(measurement_id)

        self.assertIn("1527_260402CV_P33107.ids", summary)
        self.assertIn("CyclicVoltammetry", summary)
        self.assertIn("scan 0.1V/s", summary)
        self.assertIn("cycle 5", summary)

    def test_filter_restore_rows_matches_single_search_box(self) -> None:
        rows = [
            {"record_id": "SES-20260407-0001-AA", "summary": "2026-04-07 / dopamine", "deleted_at": "2026-04-07T09:00:00"},
            {"record_id": "COND-20260407-10ppm-0001-BB", "summary": "10 ppm / CV", "deleted_at": "2026-04-07T10:00:00"},
        ]

        filtered = filter_restore_rows(rows, "dopamine")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["record_id"], "SES-20260407-0001-AA")

        filtered_by_id = filter_restore_rows(rows, "10ppm")
        self.assertEqual(len(filtered_by_id), 1)
        self.assertEqual(filtered_by_id[0]["record_id"], "COND-20260407-10ppm-0001-BB")


if __name__ == "__main__":
    unittest.main()
