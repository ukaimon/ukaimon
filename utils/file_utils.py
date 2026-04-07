from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
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


def _normalize_condition_token(concentration_value: object, concentration_unit: object | None = None) -> str:
    value_text = str(concentration_value).strip()
    unit_text = str(concentration_unit or "").strip().lower()
    if not value_text:
        value_text = "na"
    else:
        try:
            normalized_decimal = format(Decimal(value_text).normalize(), "f")
            if "." in normalized_decimal:
                normalized_decimal = normalized_decimal.rstrip("0").rstrip(".")
            if normalized_decimal in {"-0", "+0"}:
                normalized_decimal = "0"
            value_text = normalized_decimal
        except (InvalidOperation, ValueError):
            pass
    value_text = value_text.replace("-", "m").replace(".", "p")
    value_text = "".join(character for character in value_text if character.isalnum() or character == "p")
    unit_text = "".join(character for character in unit_text if character.isalnum())
    return f"{value_text}{unit_text}"[:20] or "na"


def normalize_condition_token(concentration_value: object, concentration_unit: object | None = None) -> str:
    return _normalize_condition_token(concentration_value, concentration_unit)


def generate_condition_id(concentration_value: object, concentration_unit: object | None = None) -> str:
    normalized_prefix = "COND"
    date_part = datetime.now().astimezone().strftime("%Y%m%d")
    condition_token = normalize_condition_token(concentration_value, concentration_unit)
    with _ID_LOCK:
        key = (normalized_prefix, date_part, condition_token)
        _ID_COUNTERS[key] += 1
        serial = _ID_COUNTERS[key]
    suffix = "".join(chr(65 + int(char, 16) % 26) for char in uuid4().hex[:2])
    return f"{normalized_prefix}-{date_part}-{condition_token}-{serial:04d}-{suffix}"


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
