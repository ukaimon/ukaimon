from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from threading import Lock
from uuid import uuid4


_ID_COUNTERS: dict[tuple[str, str], int] = defaultdict(int)
_ID_LOCK = Lock()


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def generate_id(prefix: str) -> str:
    normalized_prefix = prefix.strip().upper()
    date_part = datetime.now().astimezone().strftime("%Y%m%d")
    with _ID_LOCK:
        key = (normalized_prefix, date_part)
        _ID_COUNTERS[key] += 1
        serial = _ID_COUNTERS[key]
    suffix = "".join(chr(65 + int(char, 16) % 26) for char in uuid4().hex[:2])
    return f"{normalized_prefix}-{date_part}-{serial:04d}-{suffix}"


def session_output_directories(root_path: Path, session_id: str) -> dict[str, Path]:
    session_root = root_path / "data" / "sessions" / session_id
    directories = {
        "root": session_root,
        "raw": session_root / "raw",
        "processed": session_root / "processed",
        "plots": session_root / "plots",
        "exports": session_root / "exports",
        "report": session_root / "report",
    }
    ensure_directories(list(directories.values()))
    return directories
