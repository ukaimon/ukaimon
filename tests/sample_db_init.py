from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices


def main() -> None:
    config = AppConfig.load(ROOT / "config" / "config.example.json")
    repository = ElectrochemRepository(DatabaseManager(ROOT / "database" / "sample_init.db"))
    services = AppServices(ROOT, config, repository)
    services.initialize()

    mip_id = services.create_mip(
        {
            "template_name": "demo-template",
            "preparation_date": "2026-04-05",
            "operator": "codex",
            "note": "sample init",
            "tags": "demo",
        }
    )
    usage_id = services.create_mip_usage(
        {
            "mip_id": mip_id,
            "cp_preparation_date": "2026-04-05",
            "coating_date": "2026-04-05",
            "operator": "codex",
            "note": "",
            "tags": "demo",
        }
    )
    session_id = services.create_session(
        {
            "session_date": "2026-04-05",
            "session_name": "demo-session",
            "analyte": "dopamine",
            "method_default": "CV",
            "electrolyte": "PBS",
            "common_note": "sample init",
            "mip_usage_id": usage_id,
            "operator": "codex",
            "tags": "demo",
            "status": "draft",
        }
    )
    services.create_condition(
        {
            "session_id": session_id,
            "analyte": "dopamine",
            "concentration_value": 0.0,
            "concentration_unit": "ppm",
            "method": "CV",
            "planned_replicates": 1,
            "common_note": "",
            "tags": "demo",
        }
    )
    services.generate_batch_plan(session_id, baseline_value=0.0, execution_mode="randomized_blocks")
    print(f"Initialized sample DB at: {ROOT / 'database' / 'sample_init.db'}")


if __name__ == "__main__":
    main()
