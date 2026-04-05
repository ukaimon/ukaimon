from __future__ import annotations

from typing import Any

import pandas as pd


def aggregate_condition_rows(
    condition_row: dict[str, Any],
    session_row: dict[str, Any],
    mip_id: str | None,
    mip_usage_id: str | None,
    measurement_rows: list[dict[str, Any]],
    analysis_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    analysis_frame = pd.DataFrame(analysis_rows)
    valid_measurements = [row for row in measurement_rows if row.get("final_quality_flag") == "valid"]
    if analysis_frame.empty:
        mean_current = None
        std_current = None
        cv_percent = None
        mean_potential = None
        std_potential = None
    else:
        mean_current = float(analysis_frame["representative_current_a"].mean())
        std_current = float(analysis_frame["representative_current_a"].std(ddof=0)) if len(analysis_frame) > 1 else 0.0
        mean_potential = float(analysis_frame["representative_potential_v"].mean())
        std_potential = float(analysis_frame["representative_potential_v"].std(ddof=0)) if len(analysis_frame) > 1 else 0.0
        cv_percent = (std_current / abs(mean_current) * 100.0) if mean_current else None

    return {
        "session_id": session_row["session_id"],
        "condition_id": condition_row["condition_id"],
        "mip_id": mip_id,
        "mip_usage_id": mip_usage_id,
        "analyte": condition_row.get("analyte") or session_row.get("analyte"),
        "concentration_value": condition_row.get("concentration_value"),
        "concentration_unit": condition_row.get("concentration_unit"),
        "method": condition_row.get("method"),
        "n_total": len(measurement_rows),
        "n_valid": len(valid_measurements),
        "n_invalid": len([row for row in measurement_rows if row.get("final_quality_flag") == "invalid"]),
        "mean_current_a": mean_current,
        "std_current_a": std_current,
        "cv_percent": cv_percent,
        "mean_potential_v": mean_potential,
        "std_potential_v": std_potential,
        "slope": None,
        "intercept": None,
        "r_squared": None,
        "note": "",
    }
