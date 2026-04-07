from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices


class ConditionIdMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_condition_id_migration_{uuid4().hex[:8]}.db"
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

    def test_initialize_normalizes_legacy_condition_ids(self) -> None:
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
        old_condition_id = "COND-20260407-10p0ppm-0001-AA"
        self.services.repository.insert_record(
            "conditions",
            {
                "condition_id": old_condition_id,
                "session_id": session_id,
                "analyte": "dopamine",
                "concentration_value": 10.0,
                "concentration_unit": "ppm",
                "method": "CV",
                "planned_replicates": 1,
                "condition_status": "pending",
                "common_note": "",
                "tags": "",
            },
        )
        self.services.repository.insert_record(
            "batch_plan_items",
            {
                "batch_item_id": "BATCH-20260407-0001-AA",
                "session_id": session_id,
                "condition_id": old_condition_id,
                "planned_order": 1,
                "rep_no": 1,
                "planned_status": "waiting",
                "assigned_measurement_id": None,
                "note": "",
            },
        )
        self.services.repository.insert_record(
            "measurements",
            {
                "measurement_id": "MEAS-20260407-0001-AA",
                "condition_id": old_condition_id,
                "session_id": session_id,
                "mip_usage_id": usage_id,
                "rep_no": 1,
                "status": "manual",
                "link_status": "manual",
                "auto_quality_flag": "valid",
                "final_quality_flag": "valid",
            },
        )
        self.services.repository.insert_record(
            "analysis_results",
            {
                "result_id": "ARES-20260407-0001-AA",
                "measurement_id": "MEAS-20260407-0001-AA",
                "condition_id": old_condition_id,
                "session_id": session_id,
            },
        )

        renamed_count = self.services.repository.normalize_legacy_condition_ids()
        self.assertEqual(renamed_count, 1)

        new_condition_id = "COND-20260407-10ppm-0001-AA"
        self.assertIsNone(self.services.repository.get_record("conditions", old_condition_id))
        self.assertIsNotNone(self.services.repository.get_record("conditions", new_condition_id))
        batch_item = self.services.repository.get_record("batch_plan_items", "BATCH-20260407-0001-AA")
        measurement = self.services.repository.get_record("measurements", "MEAS-20260407-0001-AA")
        analysis = self.services.repository.get_record("analysis_results", "ARES-20260407-0001-AA")
        self.assertEqual(batch_item["condition_id"], new_condition_id)
        self.assertEqual(measurement["condition_id"], new_condition_id)
        self.assertEqual(analysis["condition_id"], new_condition_id)


if __name__ == "__main__":
    unittest.main()
