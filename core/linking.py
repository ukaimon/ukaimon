from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from core.models import PlannedStatus
from core.repositories import ElectrochemRepository
from core.quality import resolve_final_quality
from utils.date_utils import now_iso
from utils.file_utils import generate_id

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except Exception:  # pragma: no cover - optional dependency at runtime
    FileSystemEventHandler = object  # type: ignore[assignment]
    Observer = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class LinkDecision:
    batch_item_id: str
    session_id: str
    condition_id: str
    rep_no: int


class BatchLinker:
    def __init__(self, repository: ElectrochemRepository) -> None:
        self.repository = repository

    def choose_target(self, session_id: str | None = None) -> LinkDecision:
        batch_item = self.repository.get_next_waiting_batch_item(session_id)
        if not batch_item:
            raise ValueError("紐付け可能な waiting バッチ項目がありません。")
        return LinkDecision(
            batch_item_id=batch_item["batch_item_id"],
            session_id=batch_item["session_id"],
            condition_id=batch_item["condition_id"],
            rep_no=int(batch_item["rep_no"]),
        )

    def build_measurement_payload(
        self,
        *,
        session_id: str,
        condition_id: str,
        mip_usage_id: str | None,
        raw_file_path: str,
        batch_item_id: str | None,
        rep_no: int,
        measured_at: str | None,
        auto_quality_flag: str,
        manual_quality_flag: str | None = None,
        note: str = "",
    ) -> dict[str, object]:
        return {
            "measurement_id": generate_id("MEAS"),
            "batch_item_id": batch_item_id,
            "condition_id": condition_id,
            "session_id": session_id,
            "mip_usage_id": mip_usage_id,
            "rep_no": rep_no,
            "measured_at": measured_at or now_iso(),
            "chip_id": "",
            "wire_id": "",
            "status": "linked",
            "noise_level": None,
            "coating_quality": "",
            "electrode_condition": "",
            "bubbling_condition": "",
            "free_memo": note,
            "raw_file_path": raw_file_path,
            "link_status": PlannedStatus.COMPLETED.value if batch_item_id else "manual",
            "auto_quality_flag": auto_quality_flag,
            "manual_quality_flag": manual_quality_flag,
            "final_quality_flag": resolve_final_quality(auto_quality_flag, manual_quality_flag).value,
            "exclusion_reason": "",
        }


class _IdsCreatedHandler(FileSystemEventHandler):  # type: ignore[misc]
    def __init__(self, callback: Callable[[Path], None], target_extensions: list[str]) -> None:
        super().__init__()
        self.callback = callback
        self.target_extensions = {extension.lower() for extension in target_extensions}

    def on_created(self, event) -> None:  # pragma: no cover - watchdog integration
        if getattr(event, "is_directory", False):
            return
        path = Path(event.src_path)
        if path.suffix.lower() not in self.target_extensions:
            return
        time.sleep(0.2)
        self.callback(path)


class IdsWatchCoordinator:
    def __init__(
        self,
        watch_folder: Path,
        callback: Callable[[Path], None],
        target_extensions: list[str],
    ) -> None:
        if Observer is None:
            raise RuntimeError("watchdog が利用できないため監視を開始できません。")
        self.watch_folder = watch_folder
        self.callback = callback
        self.target_extensions = target_extensions
        self.observer: Observer | None = None

    def start(self) -> None:  # pragma: no cover - watchdog integration
        self.watch_folder.mkdir(parents=True, exist_ok=True)
        handler = _IdsCreatedHandler(self.callback, self.target_extensions)
        self.observer = Observer()
        self.observer.schedule(handler, str(self.watch_folder), recursive=False)
        self.observer.start()
        LOGGER.info("Started ids watcher: %s", self.watch_folder)

    def stop(self) -> None:  # pragma: no cover - watchdog integration
        if not self.observer:
            return
        self.observer.stop()
        self.observer.join(timeout=5)
        self.observer = None
        LOGGER.info("Stopped ids watcher: %s", self.watch_folder)
