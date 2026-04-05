from __future__ import annotations

from itertools import pairwise

import numpy as np
import pandas as pd

from core.models import CycleAnalysisResult, MeasurementAnalysisResult, QualityFlag


def _split_cv_cycles(dataframe: pd.DataFrame) -> list[pd.DataFrame]:
    if len(dataframe) < 4:
        return [dataframe]
    potential = dataframe["potential_v"].astype(float).to_numpy()
    diff = np.diff(potential)
    sign = np.sign(diff)
    if len(sign) == 0:
        return [dataframe]
    for index, value in enumerate(sign):
        if value == 0:
            sign[index] = sign[index - 1] if index > 0 else 1
    change_points = [idx + 1 for idx in range(1, len(sign)) if sign[idx] != sign[idx - 1]]
    if not change_points:
        return [dataframe]
    boundaries = [0, *change_points, len(dataframe)]
    half_cycles = [
        dataframe.iloc[start:end].reset_index(drop=True)
        for start, end in pairwise(boundaries)
        if end - start >= 3
    ]
    if len(half_cycles) < 2:
        return [dataframe]
    cycles: list[pd.DataFrame] = []
    for index in range(0, len(half_cycles), 2):
        if index + 1 < len(half_cycles):
            cycles.append(pd.concat([half_cycles[index], half_cycles[index + 1]], ignore_index=True))
        else:
            cycles.append(half_cycles[index])
    return cycles or [dataframe]


def analyze_cv_curve(
    dataframe: pd.DataFrame,
    representative_cycle_rule: str = "Cycle1",
) -> MeasurementAnalysisResult:
    if dataframe.empty or "potential_v" not in dataframe.columns or "current_a" not in dataframe.columns:
        return MeasurementAnalysisResult(
            representative_current_a=None,
            representative_potential_v=None,
            oxidation_peak_current_a=None,
            oxidation_peak_potential_v=None,
            reduction_peak_current_a=None,
            reduction_peak_potential_v=None,
            delta_ep_v=None,
            integrated_area=None,
            quality_flag=QualityFlag.INVALID.value,
            analysis_method="CV",
            note="解析対象の標準化データが不足しています。",
        )

    cycles = _split_cv_cycles(dataframe)
    cycle_results: list[CycleAnalysisResult] = []
    for cycle_no, cycle_df in enumerate(cycles, start=1):
        oxidation_index = cycle_df["current_a"].idxmax()
        reduction_index = cycle_df["current_a"].idxmin()
        oxidation_peak_current = float(cycle_df.loc[oxidation_index, "current_a"])
        reduction_peak_current = float(cycle_df.loc[reduction_index, "current_a"])
        oxidation_peak_potential = float(cycle_df.loc[oxidation_index, "potential_v"])
        reduction_peak_potential = float(cycle_df.loc[reduction_index, "potential_v"])
        cycle_results.append(
            CycleAnalysisResult(
                cycle_no=cycle_no,
                oxidation_peak_current_a=oxidation_peak_current,
                oxidation_peak_potential_v=oxidation_peak_potential,
                reduction_peak_current_a=reduction_peak_current,
                reduction_peak_potential_v=reduction_peak_potential,
                delta_ep_v=abs(oxidation_peak_potential - reduction_peak_potential),
                integrated_area=float(np.trapz(cycle_df["current_a"], cycle_df["potential_v"])),
                quality_flag=QualityFlag.VALID.value,
            )
        )

    representative_cycle_index = 0
    if representative_cycle_rule.lower().startswith("cycle"):
        suffix = representative_cycle_rule[5:]
        if suffix.isdigit():
            representative_cycle_index = min(max(int(suffix) - 1, 0), len(cycle_results) - 1)
    elif representative_cycle_rule.lower() == "last":
        representative_cycle_index = len(cycle_results) - 1

    representative_cycle = cycle_results[representative_cycle_index]
    return MeasurementAnalysisResult(
        representative_current_a=representative_cycle.oxidation_peak_current_a,
        representative_potential_v=representative_cycle.oxidation_peak_potential_v,
        oxidation_peak_current_a=representative_cycle.oxidation_peak_current_a,
        oxidation_peak_potential_v=representative_cycle.oxidation_peak_potential_v,
        reduction_peak_current_a=representative_cycle.reduction_peak_current_a,
        reduction_peak_potential_v=representative_cycle.reduction_peak_potential_v,
        delta_ep_v=representative_cycle.delta_ep_v,
        integrated_area=representative_cycle.integrated_area,
        quality_flag=QualityFlag.VALID.value if len(dataframe) >= 20 else QualityFlag.SUSPECT.value,
        analysis_method="CV",
        cycle_results=cycle_results,
    )
