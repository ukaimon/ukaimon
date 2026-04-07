from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.mip_fields import MIP_FIELD_DEFAULTS, with_mip_field_defaults
from core.repositories import ElectrochemRepository
from core.services import AppServices


class MipFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_mip_fields_{uuid4().hex[:8]}.db"
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

    def test_create_mip_uses_default_detail_values_when_payload_is_minimal(self) -> None:
        mip_id = self.services.create_mip(
            {
                "template_name": "default-template",
                "preparation_date": "2026-04-07",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )

        row = self.services.repository.get_record("mip_records", mip_id)
        self.assertIsNotNone(row)
        for key, default_value in MIP_FIELD_DEFAULTS.items():
            self.assertEqual(str(row[key]), default_value)

    def test_legacy_mip_table_gets_new_columns_and_defaults(self) -> None:
        legacy_database_path = self.root / "database" / f"test_mip_legacy_{uuid4().hex[:8]}.db"
        try:
            with sqlite3.connect(legacy_database_path) as connection:
                connection.execute(
                    """
                    CREATE TABLE mip_records (
                        mip_id TEXT PRIMARY KEY,
                        preparation_date TEXT,
                        template_name TEXT NOT NULL,
                        monomer TEXT,
                        crosslinker TEXT,
                        solvent TEXT,
                        initiator TEXT,
                        polymerization_method TEXT,
                        polymerization_time TEXT,
                        light_condition TEXT,
                        operator TEXT NOT NULL,
                        note TEXT,
                        tags TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO mip_records (
                        mip_id, preparation_date, template_name, operator, note, tags, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "MIP-LEGACY-0001-AA",
                        "2026-04-07",
                        "legacy-template",
                        "tester",
                        "",
                        "",
                        "2026-04-07T00:00:00",
                        "2026-04-07T00:00:00",
                    ),
                )
                connection.commit()

            config = AppConfig.load(self.root / "config" / "config.example.json")
            legacy_services = AppServices(
                self.root,
                config,
                ElectrochemRepository(DatabaseManager(legacy_database_path)),
            )
            legacy_services.initialize()

            row = legacy_services.repository.get_record("mip_records", "MIP-LEGACY-0001-AA")
            self.assertIsNotNone(row)
            for key, default_value in MIP_FIELD_DEFAULTS.items():
                self.assertEqual(str(row[key]), default_value)
        finally:
            try:
                if legacy_database_path.exists():
                    legacy_database_path.unlink()
            except PermissionError:
                pass

    def test_with_mip_field_defaults_preserves_overrides(self) -> None:
        normalized = with_mip_field_defaults({"dmso_ul": "21", "uv_irradiation_time_min": "75"})
        self.assertEqual(normalized["dmso_ul"], "21")
        self.assertEqual(normalized["uv_irradiation_time_min"], "75")
        self.assertEqual(normalized["maa_ul"], MIP_FIELD_DEFAULTS["maa_ul"])


if __name__ == "__main__":
    unittest.main()
