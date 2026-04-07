from __future__ import annotations

import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices


class OperatorDefaultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_operator_defaults_{uuid4().hex[:8]}.db"
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

    def test_default_mip_operator_uses_latest_record(self) -> None:
        self.services.create_mip(
            {
                "template_name": "temp-a",
                "preparation_date": "2026-04-07",
                "operator": "Alice",
                "note": "",
                "tags": "",
            }
        )
        self.services.create_mip(
            {
                "template_name": "temp-b",
                "preparation_date": "2026-04-07",
                "operator": "Bob",
                "note": "",
                "tags": "",
            }
        )
        self.assertEqual(self.services.get_default_mip_operator(), "Bob")

    def test_default_mip_usage_operator_prefers_selected_mip(self) -> None:
        mip_id = self.services.create_mip(
            {
                "template_name": "temp-a",
                "preparation_date": "2026-04-07",
                "operator": "Alice",
                "note": "",
                "tags": "",
            }
        )
        self.services.create_mip_usage(
            {
                "mip_id": mip_id,
                "cp_preparation_date": "2026-04-07",
                "coating_date": "2026-04-07",
                "operator": "Carol",
                "note": "",
                "tags": "",
            }
        )
        self.assertEqual(self.services.get_default_mip_usage_operator(mip_id), "Alice")
        self.assertEqual(self.services.get_default_mip_usage_operator(), "Carol")

    def test_default_session_operator_prefers_selected_usage(self) -> None:
        mip_id = self.services.create_mip(
            {
                "template_name": "temp-a",
                "preparation_date": "2026-04-07",
                "operator": "Alice",
                "note": "",
                "tags": "",
            }
        )
        usage_id = self.services.create_mip_usage(
            {
                "mip_id": mip_id,
                "cp_preparation_date": "2026-04-07",
                "coating_date": "2026-04-07",
                "operator": "Carol",
                "note": "",
                "tags": "",
            }
        )
        self.services.create_session(
            {
                "mip_usage_id": usage_id,
                "session_date": "2026-04-07",
                "analyte": "dopamine",
                "session_name": "session-a",
                "method_default": "CV",
                "operator": "Dave",
                "electrolyte": "",
                "common_note": "",
                "tags": "",
                "status": "draft",
            }
        )
        self.assertEqual(self.services.get_default_session_operator(usage_id), "Carol")
        self.assertEqual(self.services.get_default_session_operator(), "Dave")


if __name__ == "__main__":
    unittest.main()
