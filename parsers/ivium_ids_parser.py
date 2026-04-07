from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.models import ParsedMeasurementData
from parsers.measurement_conditions_parser import parse_header_key_values
from parsers.parser_utils import (
    collect_numeric_block,
    detect_standard_columns,
    is_primary_data_marker,
    parse_column_header,
    parse_numeric_row,
    read_ids_lines,
    to_dataframe,
)


def _locate_count_and_data_start(lines: list[str], marker_index: int) -> tuple[int | None, int]:
    candidate_count: int | None = None
    for index in range(marker_index + 1, min(marker_index + 12, len(lines))):
        line = lines[index].strip()
        if not line:
            continue
        parsed_numeric = parse_numeric_row(line)
        if parsed_numeric is not None:
            return candidate_count, index
        if line.isdigit():
            candidate_count = int(line)
            continue
        column_names = parse_column_header(line)
        if column_names and index + 1 < len(lines) and parse_numeric_row(lines[index + 1]) is not None:
            return candidate_count, index + 1
    return candidate_count, marker_index + 1


def _extract_column_names(lines: list[str], data_start: int) -> list[str] | None:
    for index in range(max(0, data_start - 2), data_start):
        column_names = parse_column_header(lines[index])
        if column_names:
            return column_names
    return None


def _build_block(lines: list[str], start_index: int, *, marker_index: int | None = None) -> dict[str, object] | None:
    expected_count = None
    data_start = start_index
    if marker_index is not None:
        expected_count, data_start = _locate_count_and_data_start(lines, marker_index)
    numeric_rows = collect_numeric_block(lines, data_start, expected_count)
    if len(numeric_rows) < 8:
        return None
    column_names = _extract_column_names(lines, data_start)
    header_start = max(0, (marker_index if marker_index is not None else data_start) - 160)
    header_end = marker_index if marker_index is not None else data_start
    header_lines = [candidate for candidate in lines[header_start:header_end] if candidate]
    return {
        "block_index": 0,
        "line_index": marker_index if marker_index is not None else data_start,
        "row_count": len(numeric_rows),
        "header_lines": header_lines,
        "metadata": parse_header_key_values(header_lines),
        "dataframe": to_dataframe(numeric_rows, column_names),
    }


def _find_primary_blocks(lines: list[str]) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if not is_primary_data_marker(line):
            continue
        block = _build_block(lines, index + 1, marker_index=index)
        if not block:
            continue
        block["block_index"] = len(blocks)
        blocks.append(block)
    return blocks


def _find_fallback_blocks(lines: list[str]) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    index = 0
    while index < len(lines):
        if parse_numeric_row(lines[index]) is None:
            index += 1
            continue
        block = _build_block(lines, index)
        if block:
            block["block_index"] = len(blocks)
            blocks.append(block)
            index += int(block["row_count"])
        else:
            index += 1
    return blocks


def _standardize_dataframe(dataframe: pd.DataFrame, detected_columns: dict[str, str]) -> tuple[pd.DataFrame, dict[str, str]]:
    if dataframe.empty:
        return dataframe, {}
    standardized = pd.DataFrame(index=dataframe.index)
    mapping: dict[str, str] = {}
    for semantic_name in ("potential", "current", "time"):
        source_column = detected_columns.get(semantic_name)
        if not source_column or source_column not in dataframe.columns:
            continue
        target_name = f"{semantic_name}_v" if semantic_name == "potential" else semantic_name
        if semantic_name == "current":
            target_name = "current_a"
        elif semantic_name == "time":
            target_name = "time_s"
        standardized[target_name] = dataframe[source_column].astype(float)
        mapping[semantic_name] = target_name

    for column_name in dataframe.columns:
        if column_name in detected_columns.values():
            continue
        standardized[column_name] = dataframe[column_name].astype(float)
    return standardized.dropna(how="all"), mapping


def parse_ids_file(file_path: str | Path) -> ParsedMeasurementData:
    lines = read_ids_lines(file_path)
    blocks = _find_primary_blocks(lines)
    if not blocks:
        blocks = _find_fallback_blocks(lines)
    if not blocks:
        raise ValueError(f"数値データブロックを検出できませんでした: {file_path}")

    selected_block = max(blocks, key=lambda block: (int(block["row_count"]), int(block["line_index"])))
    detected_columns = detect_standard_columns(selected_block["dataframe"])  # type: ignore[arg-type]
    standardized_dataframe, standardized_columns = _standardize_dataframe(
        selected_block["dataframe"],  # type: ignore[arg-type]
        detected_columns,
    )
    header_lines = selected_block["header_lines"]  # type: ignore[assignment]
    metadata = dict(selected_block["metadata"])  # type: ignore[arg-type]
    metadata["available_blocks"] = [
        {
            "block_index": block["block_index"],
            "row_count": block["row_count"],
            "method": block["metadata"].get("Method"),
            "starttime": block["metadata"].get("starttime_iso", block["metadata"].get("starttime")),
        }
        for block in blocks
    ]
    metadata["parser_recovered"] = not any(is_primary_data_marker(line) for line in lines)
    return ParsedMeasurementData(
        metadata=metadata,
        raw_header_text="\n".join(header_lines),
        data=standardized_dataframe,
        detected_columns=standardized_columns,
        source_file_path=str(Path(file_path).resolve()),
        data_blocks=metadata["available_blocks"],
    )
