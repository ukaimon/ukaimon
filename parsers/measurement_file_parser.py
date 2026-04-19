from __future__ import annotations

from pathlib import Path

from core.models import ParsedMeasurementData
from parsers.ivium_ids_parser import parse_ids_file
from parsers.ivium_sqlite_parser import parse_ivium_sqlite_file


def parse_measurement_file(file_path: str | Path) -> ParsedMeasurementData:
    path = Path(file_path)
    normalized_name = path.name.lower()
    if normalized_name.endswith(".idf.sqlite"):
        return parse_ivium_sqlite_file(path)
    if path.suffix.lower() == ".sqlite":
        return parse_ivium_sqlite_file(path)
    if path.suffix.lower() == ".ids":
        return parse_ids_file(path)
    raise ValueError(f"未対応の測定ファイル形式です: {path}")
