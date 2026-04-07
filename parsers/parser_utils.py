from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd


PRIMARY_DATA_MARKERS = {
    "primary_data",
    "[primary_data]",
    "primary data",
    "primarydata",
}

HEADER_KEYWORDS = {
    "potential": ("potential", "e", "voltage", "applied potential", "potential/v", "e/v", "ewe"),
    "current": ("current", "i", "current/a", "i/a", "current(a)", "current(a)"),
    "time": ("time", "t", "time/s", "t/s"),
}

NUMERIC_TOKEN_PATTERN = re.compile(r"^[\-\+]?(?:\d+(?:[\,\.]\d*)?|[\,\.]\d+)(?:[Ee][\-\+]?\d+)?$")


def read_ids_lines(file_path: str | Path) -> list[str]:
    path = Path(file_path)
    raw_bytes = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp932", "utf-8", "latin1"):
        try:
            text = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw_bytes.decode("utf-8", errors="replace")

    for marker in ("\r\n", "\r", "\x00", "\x1c", "\x1e"):
        text = text.replace(marker, "\n")
    return [sanitize_line(line) for line in text.split("\n")]


def sanitize_line(text: str) -> str:
    cleaned = "".join(
        character
        for character in text
        if character in {"\t", " "} or unicodedata.category(character)[0] != "C"
    )
    return cleaned.replace("\ufeff", "").strip()


def normalize_line_key(text: str) -> str:
    return re.sub(r"[\s_]+", " ", sanitize_line(text).lower()).strip()


def is_primary_data_marker(line: str) -> bool:
    return normalize_line_key(line) in PRIMARY_DATA_MARKERS


def _parse_float_token(token: str) -> float | None:
    normalized = token.strip().strip("\"'")
    if not normalized:
        return None
    normalized = normalized.replace("−", "-")
    if normalized.count(",") == 1 and "." not in normalized:
        normalized = normalized.replace(",", ".")
    elif normalized.count(",") > 1 and "." not in normalized:
        normalized = normalized.replace(",", "")
    if not NUMERIC_TOKEN_PATTERN.match(normalized):
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _split_tokens(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped:
        return []
    delimiter_patterns = [
        r"[\t;]+",
        r"\s{2,}",
        r"[\t ]+",
        r",(?=\s*[\-\+]?\d)",
    ]
    for pattern in delimiter_patterns:
        tokens = [token.strip() for token in re.split(pattern, stripped) if token.strip()]
        if len(tokens) >= 2:
            return tokens
    return [stripped]


def is_numeric_row(line: str) -> bool:
    return parse_numeric_row(line) is not None


def parse_numeric_row(line: str) -> list[float] | None:
    tokens = _split_tokens(line)
    if len(tokens) < 2:
        return None
    values: list[float] = []
    for token in tokens:
        parsed = _parse_float_token(token)
        if parsed is None:
            return None
        values.append(parsed)
    return values if len(values) >= 2 else None


def collect_numeric_block(
    lines: list[str],
    start_index: int,
    expected_count: int | None = None,
    max_skipped_rows: int = 2,
) -> list[list[float]]:
    block_rows: list[list[float]] = []
    skipped_rows = 0
    for line in lines[start_index:]:
        parsed = parse_numeric_row(line)
        if parsed is None:
            if not block_rows:
                continue
            skipped_rows += 1
            if skipped_rows > max_skipped_rows:
                break
            continue
        skipped_rows = 0
        block_rows.append(parsed)
        if expected_count and len(block_rows) >= expected_count:
            break
    return block_rows


def normalize_column_label(label: str) -> str:
    normalized = normalize_line_key(label)
    normalized = normalized.replace("(", " ").replace(")", " ")
    normalized = normalized.replace("[", " ").replace("]", " ")
    normalized = normalized.replace("/", " ")
    return re.sub(r"\s+", " ", normalized).strip()


def parse_column_header(line: str) -> list[str] | None:
    tokens = _split_tokens(line)
    if len(tokens) < 2:
        return None
    if all(_parse_float_token(token) is not None for token in tokens):
        return None
    normalized_tokens = [normalize_column_label(token) for token in tokens]
    matched = 0
    for token in normalized_tokens:
        if any(keyword in token for keywords in HEADER_KEYWORDS.values() for keyword in keywords):
            matched += 1
    if matched == 0:
        return None
    return normalized_tokens


def to_dataframe(block_rows: list[list[float]], column_names: list[str] | None = None) -> pd.DataFrame:
    if not block_rows:
        return pd.DataFrame()
    width = max(len(row) for row in block_rows)
    normalized_rows = [row + [float("nan")] * (width - len(row)) for row in block_rows]
    if column_names:
        normalized_names = list(column_names[:width]) + [f"col_{index}" for index in range(len(column_names), width)]
        used_names: dict[str, int] = {}
        columns: list[str] = []
        for index, column_name in enumerate(normalized_names):
            base_name = column_name or f"col_{index}"
            count = used_names.get(base_name, 0)
            used_names[base_name] = count + 1
            columns.append(base_name if count == 0 else f"{base_name}_{count + 1}")
    else:
        columns = [f"col_{index}" for index in range(width)]
    return pd.DataFrame(normalized_rows, columns=columns)


def _score_named_column(column_name: str, semantic_name: str) -> float:
    normalized = normalize_column_label(column_name)
    keywords = HEADER_KEYWORDS[semantic_name]
    if normalized in keywords:
        return 10.0
    if any(keyword in normalized for keyword in keywords):
        return 7.0
    return 0.0


def detect_standard_columns(dataframe: pd.DataFrame) -> dict[str, str]:
    if dataframe.empty:
        return {}

    assigned: dict[str, str] = {}
    remaining = list(dataframe.columns)

    for semantic_name in ("potential", "current", "time"):
        best_named_column = None
        best_named_score = 0.0
        for column_name in remaining:
            score = _score_named_column(column_name, semantic_name)
            if score > best_named_score:
                best_named_score = score
                best_named_column = column_name
        if best_named_column is not None:
            assigned[semantic_name] = best_named_column
            remaining.remove(best_named_column)

    scored_candidates: dict[str, dict[str, float]] = {column_name: {} for column_name in remaining}
    for column_name in remaining:
        series = dataframe[column_name].astype(float)
        finite = series.dropna()
        if finite.empty:
            continue
        diffs = finite.diff().dropna()
        monotonic_ratio = float((diffs >= 0).mean()) if not diffs.empty else 1.0
        non_negative_ratio = float((finite >= 0).mean())
        value_range = float(finite.max() - finite.min())
        max_abs = float(finite.abs().max())
        std = float(finite.std(ddof=0)) if len(finite) > 1 else 0.0

        scored_candidates[column_name]["time"] = (
            monotonic_ratio * 4.0
            + non_negative_ratio * 1.5
            + (1.0 if max_abs > 1e-3 else 0.0)
            + (1.0 if value_range > 0 else 0.0)
        )
        scored_candidates[column_name]["potential"] = (
            (2.5 if 0.0 < max_abs <= 10.0 else 0.0)
            + (1.5 if 0.0 < value_range <= 12.0 else 0.0)
            + (0.5 if std > 0 else 0.0)
            - (1.0 if monotonic_ratio > 0.99 and non_negative_ratio > 0.99 and max_abs > 10 else 0.0)
        )
        scored_candidates[column_name]["current"] = (
            (3.0 if max_abs <= 0.1 else 0.0)
            + (2.0 if max_abs <= 1e-3 else 0.0)
            + (0.8 if std > 0 else 0.0)
            - monotonic_ratio
            - (0.5 if non_negative_ratio in {0.0, 1.0} else 0.0)
        )

    for semantic_name in ("potential", "current", "time"):
        if semantic_name in assigned:
            continue
        best_column = None
        best_score = float("-inf")
        for column_name in remaining:
            score = scored_candidates.get(column_name, {}).get(semantic_name, float("-inf"))
            if score > best_score:
                best_score = score
                best_column = column_name
        if best_column is not None:
            assigned[semantic_name] = best_column
            remaining.remove(best_column)

    return assigned
