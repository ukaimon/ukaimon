from __future__ import annotations

from typing import Any

from utils.date_utils import parse_ivium_datetime


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def parse_header_key_values(header_lines: list[str]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for line in header_lines:
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        metadata[key.strip()] = value.strip()
    if "starttime" in metadata:
        metadata["starttime_iso"] = parse_ivium_datetime(str(metadata["starttime"]))
    if "endtime" in metadata:
        metadata["endtime_iso"] = parse_ivium_datetime(str(metadata["endtime"]))
    return metadata


def build_measurement_conditions(metadata: dict[str, Any], raw_header_text: str) -> dict[str, Any]:
    method = metadata.get("Method", metadata.get("method", "Unknown"))
    return {
        "method": method,
        "potential_start_v": _safe_float(metadata.get("E start")),
        "potential_end_v": _safe_float(metadata.get("E end", metadata.get("Potential end"))),
        "potential_vertex_1_v": _safe_float(metadata.get("Vertex 1")),
        "potential_vertex_2_v": _safe_float(metadata.get("Vertex 2")),
        "scan_rate_v_s": _safe_float(metadata.get("Scanrate")),
        "step_v": _safe_float(metadata.get("E step")),
        "pulse_amplitude_v": _safe_float(metadata.get("Pulse amplitude")),
        "pulse_time_s": _safe_float(metadata.get("Pulse time")),
        "quiet_time_s": _safe_float(metadata.get("Quiet time")),
        "cycles": _safe_int(metadata.get("N scans", metadata.get("Cycles"))),
        "current_range": metadata.get("Current Range"),
        "filter_setting": metadata.get("Filter"),
        "temperature_note": metadata.get("Data Options.Temperature", metadata.get("Temperature")),
        "raw_header_text": raw_header_text,
        "note": "",
    }
