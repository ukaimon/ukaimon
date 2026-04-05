from __future__ import annotations

from pathlib import Path

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices
from gui.main_window import MainWindow
from utils.logger import configure_logging


def main() -> None:
    root_path = Path(__file__).resolve().parent
    configure_logging(root_path / "logs")
    config = AppConfig.load(
        root_path / "config" / "config.example.json",
        root_path / "config" / "local_config.json",
    )
    database_manager = DatabaseManager(root_path / "database" / "electrochem_app.db")
    repository = ElectrochemRepository(database_manager)
    services = AppServices(root_path, config, repository)
    services.initialize()
    MainWindow(services).run()


if __name__ == "__main__":
    main()
