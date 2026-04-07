from __future__ import annotations

import re
from typing import Any, Iterable

from utils.date_utils import parse_ivium_datetime


KEY_VALUE_SPLITTERS = ("=", ":", "\t")


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def _first_value(metadata: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in metadata.items()}
    for key in keys:
        if key in metadata:
            return metadata[key]
        lowered_key = key.strip().lower()
        if lowered_key in lowered:
            return lowered[lowered_key]
    return None


def _append_metadata(metadata: dict[str, Any], key: str, value: str) -> None:
    normalized_key = key.strip()
    normalized_value = value.strip()
    if not normalized_key or not normalized_value:
        return
    if normalized_key in metadata and metadata[normalized_key] != normalized_value:
        existing = metadata[normalized_key]
        if isinstance(existing, list):
            if normalized_value not in existing:
                existing.append(normalized_value)
        elif str(existing) != normalized_value:
            metadata[normalized_key] = [existing, normalized_value]
    else:
        metadata[normalized_key] = normalized_value


def _iter_key_value_candidates(line: str) -> Iterable[tuple[str, str]]:
    stripped = line.strip()
    if not stripped:
        return []
    candidates: list[tuple[str, str]] = []
    for splitter in KEY_VALUE_SPLITTERS:
        if splitter not in stripped:
            continue
        key, value = stripped.split(splitter, 1)
        if key.strip() and value.strip():
            candidates.append((key.strip(), value.strip()))
            break
    if not candidates:
        match = re.match(r"^([A-Za-z][A-Za-z0-9_. /()-]+?)\s{2,}(.+)$", stripped)
        if match:
            candidates.append((match.group(1).strip(), match.group(2).strip()))
    return candidates


def parse_header_key_values(header_lines: list[str]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for line in header_lines:
        for key, value in _iter_key_value_candidates(line):
            _append_metadata(metadata, key, value)

    starttime = _first_value(metadata, "starttime", "start time", "Start Time")
    endtime = _first_value(metadata, "endtime", "end time", "End Time")
    if starttime:
        metadata["starttime_iso"] = parse_ivium_datetime(str(starttime))
    if endtime:
        metadata["endtime_iso"] = parse_ivium_datetime(str(endtime))
    return metadata


def build_measurement_conditions(metadata: dict[str, Any], raw_header_text: str) -> dict[str, Any]:
    method = _first_value(metadata, "Method", "method") or "Unknown"
    return {
        "method": method,
        "potential_start_v": _safe_float(_first_value(metadata, "E start", "Potential start", "Start potential")),
        "potential_end_v": _safe_float(_first_value(metadata, "E end", "Potential end", "End potential")),
        "potential_vertex_1_v": _safe_float(_first_value(metadata, "Vertex 1", "E vertex 1", "Vertex1")),
        "potential_vertex_2_v": _safe_float(_first_value(metadata, "Vertex 2", "E vertex 2", "Vertex2")),
        "scan_rate_v_s": _safe_float(_first_value(metadata, "Scanrate", "Scan rate", "scan rate", "dE/dt")),
        "step_v": _safe_float(_first_value(metadata, "E step", "Step potential", "Step")),
        "pulse_amplitude_v": _safe_float(_first_value(metadata, "Pulse amplitude", "Amplitude", "Pulse Amplitude")),
        "pulse_time_s": _safe_float(_first_value(metadata, "Pulse time", "Pulse width", "Pulse Time")),
        "quiet_time_s": _safe_float(_first_value(metadata, "Quiet time", "Quiet Time", "Equilibration time")),
        "cycles": _safe_int(_first_value(metadata, "N scans", "Cycles", "Number of cycles")),
        "current_range": _first_value(metadata, "Current Range", "Current range", "Range"),
        "filter_setting": _first_value(metadata, "Filter", "Filter setting", "filter"),
        "temperature_note": _first_value(metadata, "Data Options.Temperature", "Temperature", "temperature"),
        "raw_header_text": raw_header_text,
        "note": "",
    }
