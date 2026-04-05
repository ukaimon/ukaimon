from __future__ import annotations

from pathlib import Path
from uuid import uuid4


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def generate_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


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
