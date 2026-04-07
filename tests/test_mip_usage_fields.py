from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from uuid import uuid4

from core.config import AppConfig
from core.database import DatabaseManager
from core.mip_usage_fields import MIP_USAGE_FIELD_DEFAULTS, with_mip_usage_field_defaults
from core.repositories import ElectrochemRepository
from core.services import AppServices


class MipUsageFieldTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_mip_usage_fields_{uuid4().hex[:8]}.db"
        config = AppConfig.load(self.root / "config" / "config.example.json")
        repository = ElectrochemRepository(DatabaseManager(self.database_path))
        self.services = AppServices(self.root, config, repository)
        self.services.initialize()
        self.mip_id = self.services.create_mip(
            {
                "template_name": "usage-template",
                "preparation_date": "2026-04-07",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )

    def tearDown(self) -> None:
        try:
            if self.database_path.exists():
                self.database_path.unlink()
        except PermissionError:
            pass

    def _assert_usage_defaults(self, row: dict[str, object]) -> None:
        for key, default_value in MIP_USAGE_FIELD_DEFAULTS.items():
            actual_value = row[key]
            if "." in default_value:
                self.assertAlmostEqual(float(actual_value), float(default_value))
            else:
                self.assertEqual(int(float(actual_value)), int(default_value))

    def test_create_mip_usage_uses_default_detail_values_when_payload_is_minimal(self) -> None:
        usage_id = self.services.create_mip_usage(
            {
                "mip_id": self.mip_id,
                "cp_preparation_date": "2026-04-07",
                "coating_date": "2026-04-07",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )

        row = self.services.repository.get_record("mip_usage_records", usage_id)
        self.assertIsNotNone(row)
        self._assert_usage_defaults(row)

    def test_legacy_mip_usage_table_gets_new_columns_and_defaults(self) -> None:
        legacy_database_path = self.root / "database" / f"test_mip_usage_legacy_{uuid4().hex[:8]}.db"
        try:
            with sqlite3.connect(legacy_database_path) as connection:
                connection.execute(
                    """
                    CREATE TABLE mip_records (
                        mip_id TEXT PRIMARY KEY,
                        template_name TEXT NOT NULL,
                        operator TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE mip_usage_records (
                        mip_usage_id TEXT PRIMARY KEY,
                        mip_id TEXT NOT NULL,
                        cp_preparation_date TEXT,
                        coating_date TEXT,
                        kneading_count INTEGER,
                        silicone_oil_amount REAL,
                        graphite_amount REAL,
                        coating_speed_mm_min REAL,
                        coating_passes INTEGER,
                        operator TEXT,
                        note TEXT,
                        tags TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO mip_records (mip_id, template_name, operator, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    ("MIP-LEGACY-0001-AA", "legacy-template", "tester", "2026-04-07T00:00:00", "2026-04-07T00:00:00"),
                )
                connection.execute(
                    """
                    INSERT INTO mip_usage_records (
                        mip_usage_id, mip_id, cp_preparation_date, coating_date, operator, note, tags, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "MUSE-LEGACY-0001-AA",
                        "MIP-LEGACY-0001-AA",
                        "2026-04-07",
                        "2026-04-07",
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

            row = legacy_services.repository.get_record("mip_usage_records", "MUSE-LEGACY-0001-AA")
            self.assertIsNotNone(row)
            self._assert_usage_defaults(row)
        finally:
            try:
                if legacy_database_path.exists():
                    legacy_database_path.unlink()
            except PermissionError:
                pass

    def test_with_mip_usage_field_defaults_preserves_overrides(self) -> None:
        normalized = with_mip_usage_field_defaults({"kneading_count": "7", "coating_height": "7.2"})
        self.assertEqual(normalized["kneading_count"], "7")
        self.assertEqual(normalized["coating_height"], "7.2")
        self.assertEqual(normalized["coating_speed_mm_min"], MIP_USAGE_FIELD_DEFAULTS["coating_speed_mm_min"])


if __name__ == "__main__":
    unittest.main()
