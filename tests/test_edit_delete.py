from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices


class EditDeleteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_edit_delete_{uuid4().hex[:8]}.db"
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

    def test_update_session_updates_condition_analyte(self) -> None:
        _, usage_id, session_id, condition_id = self._create_base_records()
        self.services.update_session(
            session_id,
            {
                "mip_usage_id": usage_id,
                "session_date": "2026-04-08",
                "analyte": "serotonin",
                "session_name": "session-b",
                "method_default": "DPV",
                "operator": "tester2",
                "electrolyte": "",
                "common_note": "",
                "tags": "",
                "status": "draft",
            },
        )
        session = self.services.repository.get_record("sessions", session_id)
        condition = self.services.repository.get_record("conditions", condition_id)
        self.assertEqual(session["session_date"], "2026-04-08")
        self.assertEqual(session["analyte"], "serotonin")
        self.assertEqual(condition["analyte"], "serotonin")

    def test_delete_empty_session_physically_removes_related_planning_data(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
        message = self.services.delete_session(session_id)
        self.assertIn("物理削除", message)
        self.assertIsNone(self.services.repository.get_record("sessions", session_id))
        self.assertIsNone(self.services.repository.get_record("conditions", condition_id))
        self.assertEqual(self.services.list_batch_items(), [])

    def test_delete_session_with_measurement_becomes_logical_delete(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "coating_quality": "",
                "electrode_condition": "",
                "bubbling_condition": "",
                "free_memo": "",
                "raw_file_path": "",
                "exclusion_reason": "",
            }
        )
        message = self.services.delete_session(session_id)
        session = self.services.repository.get_record("sessions", session_id)
        condition = self.services.repository.get_record("conditions", condition_id)
        self.assertIn("論理削除", message)
        self.assertEqual(session["is_deleted"], 1)
        self.assertEqual(condition["is_deleted"], 1)
        self.assertEqual(self.services.list_sessions(), [])
        self.assertEqual(self.services.list_conditions(), [])
        self.assertEqual(self.services.list_measurements(), [])

    def test_delete_empty_condition_physically_removes_batch_items(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
        message = self.services.delete_condition(condition_id)
        self.assertIn("物理削除", message)
        self.assertIsNone(self.services.repository.get_record("conditions", condition_id))
        self.assertEqual(self.services.list_batch_items(session_id), [])

    def test_update_measurement_updates_metadata(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        measurement_id = self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "chip_id": "chip-a",
                "wire_id": "wire-a",
                "status": "manual",
                "noise_level": 0.1,
                "free_memo": "before",
            }
        )
        self.services.update_measurement(
            measurement_id,
            {
                "session_id": session_id,
                "condition_id": condition_id,
                "chip_id": "chip-b",
                "wire_id": "wire-b",
                "status": "manual_review",
                "noise_level": 0.5,
                "free_memo": "after",
            },
        )
        measurement = self.services.repository.get_record("measurements", measurement_id)
        self.assertEqual(measurement["chip_id"], "chip-b")
        self.assertEqual(measurement["wire_id"], "wire-b")
        self.assertEqual(measurement["free_memo"], "after")
        self.assertEqual(measurement["auto_quality_flag"], "suspect")
        self.assertEqual(measurement["final_quality_flag"], "suspect")

    def test_delete_empty_measurement_physically_resets_batch_item(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
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
        message = self.services.delete_measurement(measurement_id)
        batch_item_after = self.services.repository.get_record("batch_plan_items", batch_item["batch_item_id"])
        self.assertIn("物理削除", message)
        self.assertIsNone(self.services.repository.get_record("measurements", measurement_id))
        self.assertEqual(batch_item_after["planned_status"], "waiting")
        self.assertIsNone(batch_item_after["assigned_measurement_id"])

    def test_delete_measurement_with_analysis_becomes_logical_delete(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
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
                "condition_param_id": "MCOND-test",
                "measurement_id": measurement_id,
                "method": "CV",
            },
        )
        message = self.services.delete_measurement(measurement_id)
        measurement = self.services.repository.get_record("measurements", measurement_id)
        batch_item_after = self.services.repository.get_record("batch_plan_items", batch_item["batch_item_id"])
        self.assertIn("論理削除", message)
        self.assertEqual(measurement["is_deleted"], 1)
        self.assertEqual(batch_item_after["planned_status"], "relink_needed")
        self.assertEqual(self.services.list_measurements(session_id), [])

    def test_delete_empty_batch_item_physically_removes_record(self) -> None:
        _, _, session_id, _ = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
        batch_item = self.services.list_batch_items(session_id)[0]
        message = self.services.delete_batch_item(batch_item["batch_item_id"])
        self.assertIn("物理削除", message)
        self.assertIsNone(self.services.repository.get_record("batch_plan_items", batch_item["batch_item_id"]))

    def test_delete_assigned_batch_item_becomes_logical_delete(self) -> None:
        _, _, session_id, condition_id = self._create_base_records()
        self.services.generate_batch_plan(session_id, 0.0, "randomized_blocks")
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
        message = self.services.delete_batch_item(batch_item["batch_item_id"])
        batch_item_after = self.services.repository.get_record("batch_plan_items", batch_item["batch_item_id"])
        self.assertIn("論理削除", message)
        self.assertEqual(batch_item_after["is_deleted"], 1)
        self.assertEqual(batch_item_after["planned_status"], "skipped")
        self.assertEqual(self.services.list_batch_items(session_id), [])


if __name__ == "__main__":
    unittest.main()
