from __future__ import annotations

import numpy as np
import pandas as pd

from core.models import MeasurementAnalysisResult, QualityFlag


def analyze_dpv_curve(
    dataframe: pd.DataFrame,
    baseline_correction: bool = True,
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
            analysis_method="DPV",
            note="解析対象の標準化データが不足しています。",
        )

    signal = dataframe["current_a"].astype(float).to_numpy(copy=True)
    potential = dataframe["potential_v"].astype(float).to_numpy(copy=True)
    note = ""

    if baseline_correction and len(signal) >= 11:
        try:
            from scipy.signal import savgol_filter

            window = min(len(signal) // 2 * 2 - 1, 51)
            window = max(window, 5)
            baseline = savgol_filter(signal, window_length=window, polyorder=3)
            signal = signal - baseline
            note = "Savitzky-Golay baseline correction applied."
        except Exception:
            note = "Baseline correction failed; raw signal used."

    try:
        from scipy.signal import find_peaks

        prominence = max(float(np.nanstd(signal)), 1e-12)
        peaks, _ = find_peaks(signal, prominence=prominence)
        if len(peaks) == 0:
            peak_index = int(np.nanargmax(signal))
            quality_flag = QualityFlag.SUSPECT.value
        else:
            peak_index = int(peaks[np.nanargmax(signal[peaks])])
            quality_flag = QualityFlag.VALID.value
    except Exception:
        peak_index = int(np.nanargmax(signal))
        quality_flag = QualityFlag.SUSPECT.value
        note = f"{note} Fallback peak detection used.".strip()

    peak_current = float(signal[peak_index]) if len(signal) else None
    peak_potential = float(potential[peak_index]) if len(potential) else None
    if peak_current is None or peak_current <= 0:
        quality_flag = QualityFlag.SUSPECT.value

    return MeasurementAnalysisResult(
        representative_current_a=peak_current,
        representative_potential_v=peak_potential,
        oxidation_peak_current_a=peak_current,
        oxidation_peak_potential_v=peak_potential,
        reduction_peak_current_a=None,
        reduction_peak_potential_v=None,
        delta_ep_v=None,
        integrated_area=float(np.trapz(signal, potential)) if len(signal) > 1 else None,
        quality_flag=quality_flag,
        analysis_method="DPV",
        note=note,
    )
