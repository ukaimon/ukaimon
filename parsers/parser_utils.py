from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


NUMERIC_ROW_PATTERN = re.compile(
    r"^[\s\t\-+0-9Ee\.]+([\t ]+[\-+0-9Ee\.]+){1,}$"
)


def read_ids_lines(file_path: str | Path) -> list[str]:
    path = Path(file_path)
    raw_bytes = path.read_bytes()
    for encoding in ("cp932", "utf-8", "latin1"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("utf-8", errors="replace")

    for marker in ("\r\n", "\r"):
        text = text.replace(marker, "\n")
    for marker in ("\x00", "\x1c", "\x1e"):
        text = text.replace(marker, "\n")
    return [sanitize_line(line) for line in text.split("\n")]


def sanitize_line(text: str) -> str:
    return "".join(
        character
        for character in text
        if character in {"\t", " "} or unicodedata.category(character)[0] != "C"
    ).strip()


def is_numeric_row(line: str) -> bool:
    return bool(line and NUMERIC_ROW_PATTERN.match(line))


def parse_numeric_row(line: str) -> list[float] | None:
    if not is_numeric_row(line):
        return None
    values = [item for item in re.split(r"[\t ]+", line.strip()) if item]
    try:
        return [float(value) for value in values]
    except ValueError:
        return None


def collect_numeric_block(lines: list[str], start_index: int, expected_count: int | None = None) -> list[list[float]]:
    block_rows: list[list[float]] = []
    for line in lines[start_index:]:
        parsed = parse_numeric_row(line)
        if parsed is None:
            if block_rows:
                break
            continue
        block_rows.append(parsed)
        if expected_count and len(block_rows) >= expected_count:
            break
    return block_rows


def to_dataframe(block_rows: list[list[float]]) -> pd.DataFrame:
    if not block_rows:
        return pd.DataFrame()
    width = max(len(row) for row in block_rows)
    normalized = [row + [float("nan")] * (width - len(row)) for row in block_rows]
    columns = [f"col_{index}" for index in range(width)]
    return pd.DataFrame(normalized, columns=columns)


def detect_standard_columns(dataframe: pd.DataFrame) -> dict[str, str]:
    if dataframe.empty:
        return {}

    scores: dict[str, dict[str, float]] = {column_name: {} for column_name in dataframe.columns}
    for column_name in dataframe.columns:
        series = dataframe[column_name].astype(float)
        diffs = series.diff().fillna(0.0)
        monotonic_ratio = float((diffs >= 0).mean())
        value_range = float(series.max() - series.min())
        max_abs = float(series.abs().max())
        scores[column_name]["time"] = monotonic_ratio + (1.0 if max_abs > 1e-4 else 0.0)
        scores[column_name]["current"] = (1.0 if max_abs < 1e-2 else 0.0) - value_range
        scores[column_name]["potential"] = (1.0 if 0.0 < max_abs < 5.0 else 0.0) + value_range

    assigned: dict[str, str] = {}
    remaining = set(dataframe.columns)
    for semantic_name in ("potential", "current", "time"):
        best_column = None
        best_score = float("-inf")
        for column_name in remaining:
            score = scores[column_name][semantic_name]
            if score > best_score:
                best_score = score
                best_column = column_name
        if best_column is not None:
            assigned[semantic_name] = best_column
            remaining.discard(best_column)
    return assigned
