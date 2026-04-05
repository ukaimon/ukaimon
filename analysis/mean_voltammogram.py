from __future__ import annotations

import numpy as np
import pandas as pd

from core.models import MeanVoltammogramResult


def generate_mean_curve(
    curve_payloads: list[dict[str, object]],
    interpolation_enabled: bool,
    interpolation_points: int,
    interpolation_method: str,
) -> MeanVoltammogramResult:
    if not curve_payloads:
        raise ValueError("平均対象の波形がありません。")

    potentials = [payload["dataframe"]["potential_v"].to_numpy() for payload in curve_payloads]
    currents = [payload["dataframe"]["current_a"].to_numpy() for payload in curve_payloads]
    source_measurement_ids = [str(payload["measurement_id"]) for payload in curve_payloads]

    first_axis = potentials[0]
    same_axis = all(
        len(axis) == len(first_axis) and np.allclose(axis, first_axis, atol=1e-9, rtol=1e-6)
        for axis in potentials[1:]
    )

    if same_axis:
        common_axis = first_axis
        matrix = np.vstack(currents)
    else:
        if not interpolation_enabled:
            min_length = min(len(axis) for axis in potentials)
            common_axis = first_axis[:min_length]
            matrix = np.vstack([current[:min_length] for current in currents])
        else:
            axis_min = max(float(axis.min()) for axis in potentials)
            axis_max = min(float(axis.max()) for axis in potentials)
            common_axis = np.linspace(axis_min, axis_max, interpolation_points)
            matrix = np.vstack(
                [np.interp(common_axis, potential_axis, current_axis) for potential_axis, current_axis in zip(potentials, currents)]
            )

    dataframe = pd.DataFrame(
        {
            "potential_v": common_axis,
            "mean_current_a": matrix.mean(axis=0),
            "std_current_a": matrix.std(axis=0),
            "n_used": len(curve_payloads),
        }
    )
    return MeanVoltammogramResult(
        dataframe=dataframe,
        n_used=len(curve_payloads),
        source_measurement_ids=source_measurement_ids,
        interpolation_method=interpolation_method,
        interpolation_points=len(common_axis),
    )
