from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.models import ParsedMeasurementData
from parsers.measurement_conditions_parser import parse_header_key_values
from parsers.parser_utils import collect_numeric_block, detect_standard_columns, parse_numeric_row, read_ids_lines, to_dataframe


def _locate_count_and_data_start(lines: list[str], primary_index: int) -> tuple[int | None, int]:
    candidate_integers: list[tuple[int, int]] = []
    first_numeric_row_index: int | None = None
    for index in range(primary_index + 1, min(primary_index + 8, len(lines))):
        line = lines[index].strip()
        if not line:
            continue
        parsed_numeric = parse_numeric_row(line)
        if parsed_numeric is not None:
            first_numeric_row_index = index
            break
        if line.isdigit():
            candidate_integers.append((index, int(line)))

    if first_numeric_row_index is None:
        return None, primary_index + 1

    count_value = None
    for index, value in candidate_integers:
        if index < first_numeric_row_index:
            count_value = value
    return count_value, first_numeric_row_index


def _find_primary_blocks(lines: list[str]) -> list[dict[str, object]]:
    blocks: list[dict[str, object]] = []
    for index, line in enumerate(lines):
        if line != "primary_data":
            continue
        expected_count, data_start = _locate_count_and_data_start(lines, index)
        numeric_rows = collect_numeric_block(lines, data_start, expected_count)
        if len(numeric_rows) < 10:
            continue
        header_start = max(0, index - 160)
        header_lines = [candidate for candidate in lines[header_start:index] if candidate]
        blocks.append(
            {
                "block_index": len(blocks),
                "line_index": index,
                "row_count": len(numeric_rows),
                "header_lines": header_lines,
                "metadata": parse_header_key_values(header_lines),
                "dataframe": to_dataframe(numeric_rows),
            }
        )
    return blocks


def _standardize_dataframe(dataframe: pd.DataFrame, detected_columns: dict[str, str]) -> tuple[pd.DataFrame, dict[str, str]]:
    if dataframe.empty:
        return dataframe, {}
    standardized = pd.DataFrame(index=dataframe.index)
    mapping: dict[str, str] = {}
    for semantic_name, source_column in detected_columns.items():
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
    return standardized, mapping


def parse_ids_file(file_path: str | Path) -> ParsedMeasurementData:
    lines = read_ids_lines(file_path)
    blocks = _find_primary_blocks(lines)
    if not blocks:
        raise ValueError(f"primary_data ブロックを検出できませんでした: {file_path}")

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
    return ParsedMeasurementData(
        metadata=metadata,
        raw_header_text="\n".join(header_lines),
        data=standardized_dataframe,
        detected_columns=standardized_columns,
        source_file_path=str(Path(file_path).resolve()),
        data_blocks=metadata["available_blocks"],
    )
