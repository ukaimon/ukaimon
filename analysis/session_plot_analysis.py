from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analysis.cv_analysis import split_cv_cycles
from analysis.mean_voltammogram import generate_mean_curve
from core.models import ParsedMeasurementData


def _normalize_curve_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "potential_v" not in dataframe.columns or "current_a" not in dataframe.columns:
        return pd.DataFrame(columns=["potential_v", "current_a"])
    normalized = dataframe[["potential_v", "current_a"]].dropna().reset_index(drop=True)
    return normalized


def extract_cycle_curves(source: ParsedMeasurementData | pd.DataFrame) -> list[pd.DataFrame]:
    if isinstance(source, ParsedMeasurementData):
        block_curves: list[pd.DataFrame] = []
        for block in source.data_blocks:
            dataframe = block.get("dataframe")
            if isinstance(dataframe, pd.DataFrame):
                normalized_block = _normalize_curve_dataframe(dataframe)
                if not normalized_block.empty:
                    block_curves.append(normalized_block)
        if block_curves:
            return block_curves
        normalized = _normalize_curve_dataframe(source.data)
    else:
        normalized = _normalize_curve_dataframe(source)
    if normalized.empty:
        return []
    cycles = [
        cycle[["potential_v", "current_a"]].dropna().reset_index(drop=True)
        for cycle in split_cv_cycles(normalized)
        if not cycle.empty
    ]
    return cycles or [normalized]


def sanitize_grid(values: np.ndarray) -> np.ndarray:
    return np.round(values.astype(float), 6)


def _build_grid(start: float, stop: float, step: float) -> np.ndarray:
    if step <= 0:
        raise ValueError("step must be positive")
    count = int(round((stop - start) / step))
    grid = start + np.arange(count + 1) * step
    return sanitize_grid(grid)


def _build_desc_grid(start: float, stop: float, step: float) -> np.ndarray:
    if step <= 0:
        raise ValueError("step must be positive")
    count = int(round((start - stop) / step))
    grid = start - np.arange(count + 1) * step
    return sanitize_grid(grid)


def _split_curve_segments(curve: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    potential = curve["potential_v"].to_numpy(dtype=float)
    max_index = int(np.nanargmax(potential))
    min_index = int(np.nanargmin(potential))
    if min_index <= max_index:
        raise ValueError("Unexpected CV order: minimum potential appears before maximum.")
    segment_1 = curve.iloc[: max_index + 1].reset_index(drop=True)
    segment_2 = curve.iloc[max_index : min_index + 1].reset_index(drop=True)
    segment_3 = curve.iloc[min_index:].reset_index(drop=True)
    return segment_1, segment_2, segment_3


def _infer_step(curves: list[pd.DataFrame]) -> float:
    steps: list[float] = []
    for curve in curves:
        diffs = np.diff(curve["potential_v"].to_numpy(dtype=float))
        diffs = np.abs(diffs[np.isfinite(diffs)])
        diffs = diffs[diffs > 0]
        if diffs.size:
            steps.append(float(np.median(diffs)))
    if not steps:
        raise ValueError("Could not infer potential step.")
    return round(float(np.median(steps)), 6)


def _interpolate_segment(segment: pd.DataFrame, grid: np.ndarray) -> np.ndarray:
    x_values = segment["potential_v"].to_numpy(dtype=float)
    y_values = segment["current_a"].to_numpy(dtype=float)
    if x_values[0] > x_values[-1]:
        x_values = x_values[::-1]
        y_values = y_values[::-1]
        ascending_grid = grid[::-1]
        interpolated = np.interp(ascending_grid, x_values, y_values)
        return interpolated[::-1]
    return np.interp(grid, x_values, y_values)


def align_cv_curves(curves: list[pd.DataFrame]) -> tuple[np.ndarray, np.ndarray]:
    if not curves:
        raise ValueError("No curves provided for alignment.")

    split_segments = [_split_curve_segments(curve) for curve in curves]
    first_segments = [parts[0] for parts in split_segments]
    second_segments = [parts[1] for parts in split_segments]
    third_segments = [parts[2] for parts in split_segments]

    step = _infer_step(curves)
    start_1 = max(float(segment["potential_v"].min()) for segment in first_segments)
    peak = min(float(segment["potential_v"].max()) for segment in first_segments)
    valley = max(float(segment["potential_v"].min()) for segment in second_segments)
    end_3 = min(float(segment["potential_v"].max()) for segment in third_segments)
    if peak <= start_1 or peak <= valley or end_3 <= valley:
        raise ValueError("Aligned grid ranges could not be resolved.")

    grid_1 = _build_grid(start_1, peak, step)
    grid_2 = _build_desc_grid(peak, valley, step)
    grid_3 = _build_grid(valley, end_3, step)

    aligned_currents: list[np.ndarray] = []
    for segment_1, segment_2, segment_3 in split_segments:
        current_1 = _interpolate_segment(segment_1, grid_1)
        current_2 = _interpolate_segment(segment_2, grid_2)
        current_3 = _interpolate_segment(segment_3, grid_3)
        aligned_currents.append(np.concatenate([current_1, current_2[1:], current_3[1:]]))

    common_potential = np.concatenate([grid_1, grid_2[1:], grid_3[1:]])
    return sanitize_grid(common_potential), np.vstack(aligned_currents)


def build_cv_mean_curve(
    curves: list[pd.DataFrame],
    *,
    interpolation_enabled: bool,
    interpolation_points: int,
    interpolation_method: str,
) -> pd.DataFrame:
    if not curves:
        return pd.DataFrame(columns=["potential_v", "mean_current_a", "std_current_a", "n_used"])
    try:
        common_potential, aligned_currents = align_cv_curves(curves)
        return pd.DataFrame(
            {
                "potential_v": common_potential,
                "mean_current_a": aligned_currents.mean(axis=0),
                "std_current_a": aligned_currents.std(axis=0),
                "n_used": len(curves),
            }
        )
    except Exception:
        mean_result = generate_mean_curve(
            [
                {
                    "measurement_id": f"curve_{index}",
                    "dataframe": curve,
                }
                for index, curve in enumerate(curves, start=1)
            ],
            interpolation_enabled=interpolation_enabled,
            interpolation_points=interpolation_points,
            interpolation_method=interpolation_method,
        )
        return mean_result.dataframe[["potential_v", "mean_current_a", "std_current_a", "n_used"]].copy()


def select_representative_curve(
    source: ParsedMeasurementData | pd.DataFrame,
    *,
    representative_mode: str,
    interpolation_enabled: bool,
    interpolation_points: int,
    interpolation_method: str,
) -> tuple[pd.DataFrame, int]:
    cycles = extract_cycle_curves(source)
    if not cycles:
        return pd.DataFrame(columns=["potential_v", "current_a"]), 0
    if len(cycles) == 1:
        return cycles[0], 1

    mode = representative_mode.strip().lower()
    if mode in {"mean", "mean_of_cycles", "mean_of_detected_cycles", "average", "mean_of_5_scans"}:
        mean_frame = build_cv_mean_curve(
            cycles,
            interpolation_enabled=interpolation_enabled,
            interpolation_points=interpolation_points,
            interpolation_method=interpolation_method,
        )
        representative = mean_frame.rename(columns={"mean_current_a": "current_a"})[["potential_v", "current_a"]]
        return representative.reset_index(drop=True), len(cycles)

    if mode == "last":
        return cycles[-1], len(cycles)

    if mode.startswith("scan_"):
        suffix = mode[5:]
        if suffix.isdigit():
            selected_index = min(max(int(suffix) - 1, 0), len(cycles) - 1)
            return cycles[selected_index], len(cycles)

    if mode.startswith("cycle"):
        suffix = mode[5:]
        if suffix.isdigit():
            selected_index = min(max(int(suffix) - 1, 0), len(cycles) - 1)
            return cycles[selected_index], len(cycles)

    return cycles[0], len(cycles)


def compute_curve_minimum(curve: pd.DataFrame) -> tuple[float, float]:
    minimum_index = curve["current_a"].idxmin()
    return (
        float(curve.loc[minimum_index, "potential_v"]),
        float(curve.loc[minimum_index, "current_a"]),
    )


def compute_absolute_integral(curve: pd.DataFrame) -> float:
    potential = curve["potential_v"].to_numpy(dtype=float)
    current = curve["current_a"].to_numpy(dtype=float)
    if len(potential) < 2:
        return float("nan")
    delta_potential = np.abs(np.diff(potential))
    absolute_current_mid = (np.abs(current[:-1]) + np.abs(current[1:])) / 2.0
    return float(np.sum(absolute_current_mid * delta_potential))


def determine_branch_for_index(curve: pd.DataFrame, index: int) -> int:
    potential = curve["potential_v"].to_numpy(dtype=float)
    max_index = int(np.nanargmax(potential))
    min_index = int(np.nanargmin(potential))
    if index <= max_index:
        return 1
    if index <= min_index:
        return 2
    return 3


def interpolate_current_on_branch(curve: pd.DataFrame, target_potential: float, branch: int) -> float:
    segment = _split_curve_segments(curve)[branch - 1]
    x_values = segment["potential_v"].to_numpy(dtype=float)
    y_values = segment["current_a"].to_numpy(dtype=float)
    if not (float(np.nanmin(x_values)) <= target_potential <= float(np.nanmax(x_values))):
        raise ValueError(f"Target potential {target_potential} is outside branch {branch} range.")
    if x_values[0] > x_values[-1]:
        x_values = x_values[::-1]
        y_values = y_values[::-1]
    return float(np.interp(target_potential, x_values, y_values))


def compute_cycle1_reference_rows(cycles: list[pd.DataFrame]) -> list[dict[str, float | int]]:
    if not cycles:
        return []
    cycle_1 = cycles[0]
    minimum_index = int(cycle_1["current_a"].idxmin())
    reference_potential = float(cycle_1.loc[minimum_index, "potential_v"])
    reference_current = float(cycle_1.loc[minimum_index, "current_a"])
    branch = determine_branch_for_index(cycle_1, minimum_index)

    rows: list[dict[str, float | int]] = [
        {
            "cycle_no": 1,
            "reference_potential_v": reference_potential,
            "reference_current_a": reference_current,
            "reference_branch": branch,
        }
    ]
    for cycle_no, curve in enumerate(cycles[1:], start=2):
        try:
            current_value = interpolate_current_on_branch(curve, reference_potential, branch)
        except Exception:
            continue
        rows.append(
            {
                "cycle_no": cycle_no,
                "reference_potential_v": reference_potential,
                "reference_current_a": current_value,
                "reference_branch": branch,
            }
        )
    return rows


def fit_linear_calibration(x_values: np.ndarray, y_values: np.ndarray) -> tuple[float, float, float]:
    slope, intercept = np.polyfit(x_values, y_values, 1)
    fit_values = slope * x_values + intercept
    denominator = float(np.sum((y_values - y_values.mean()) ** 2))
    if np.isclose(denominator, 0.0):
        r_squared = float("nan")
    else:
        r_squared = 1.0 - float(np.sum((y_values - fit_values) ** 2) / denominator)
    return float(slope), float(intercept), float(r_squared)


def save_individual_cycles_plot(
    cycle_curves: list[pd.DataFrame],
    representative_curve: pd.DataFrame,
    *,
    minimum_potential_v: float,
    minimum_current_a: float,
    file_path: Path,
    title: str,
    representative_label: str,
) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8.6, 5.2))
    color_map = plt.get_cmap("viridis")
    for index, curve in enumerate(cycle_curves, start=1):
        color = color_map((index - 1) / max(len(cycle_curves) - 1, 1))
        axis.plot(
            curve["potential_v"],
            curve["current_a"],
            linewidth=1.2,
            color=color,
            label=f"Scan {index}",
        )
    axis.plot(
        representative_curve["potential_v"],
        representative_curve["current_a"],
        color="black",
        linewidth=2.0,
        label=representative_label,
    )
    axis.scatter(
        [minimum_potential_v],
        [minimum_current_a],
        color="red",
        s=28,
        zorder=5,
        label="Minimum current",
    )
    axis.set_xlabel("potential_v")
    axis.set_ylabel("current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend(fontsize=8, ncol=2)
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)


def save_condition_mean_plot(
    representative_curves: list[pd.DataFrame],
    mean_curve: pd.DataFrame,
    *,
    minimum_potential_v: float,
    minimum_current_a: float,
    file_path: Path,
    title: str,
    mean_label: str,
) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8.6, 5.2))
    for curve in representative_curves:
        axis.plot(
            curve["potential_v"],
            curve["current_a"],
            color="#B7C4CF",
            linewidth=1.0,
            alpha=0.9,
        )
    axis.plot(
        mean_curve["potential_v"],
        mean_curve["mean_current_a"],
        color="#2E4057",
        linewidth=2.2,
        label=mean_label,
    )
    axis.scatter(
        [minimum_potential_v],
        [minimum_current_a],
        color="red",
        s=30,
        zorder=5,
        label="Minimum current",
    )
    axis.set_xlabel("potential_v")
    axis.set_ylabel("current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)


def save_overlay_curves_plot(
    curve_rows: list[dict[str, object]],
    file_path: Path,
    title: str,
) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8.8, 5.2))
    for row in curve_rows:
        dataframe = row["dataframe"]
        axis.plot(
            dataframe["potential_v"],
            dataframe["mean_current_a"],
            linewidth=1.5,
            label=str(row["label"]),
        )
    axis.set_xlabel("potential_v")
    axis.set_ylabel("mean_current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend()
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)


def save_calibration_plot(
    calibration_frame: pd.DataFrame,
    file_path: Path,
    title: str,
) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(7.4, 5.0))
    if "min_current_a" in calibration_frame.columns:
        grouped_points = calibration_frame.groupby("concentration_value", sort=True)
        for concentration_value, subset in grouped_points:
            if len(subset) == 1:
                x_positions = np.array([concentration_value], dtype=float)
            else:
                x_positions = np.linspace(
                    float(concentration_value) - 0.8,
                    float(concentration_value) + 0.8,
                    len(subset),
                )
            axis.scatter(
                x_positions,
                subset["min_current_a"],
                color="#B7C4CF",
                s=24,
                alpha=0.7,
            )

    axis.errorbar(
        calibration_frame["concentration_value"],
        calibration_frame["mean_min_current_a"],
        yerr=calibration_frame["std_min_current_a"],
        fmt="o",
        capsize=3,
        linewidth=1.2,
        color="#1f77b4",
        label="condition mean",
    )

    if (
        len(calibration_frame) >= 2
        and calibration_frame["concentration_value"].nunique() >= 2
        and {"slope", "intercept", "r_squared"}.issubset(calibration_frame.columns)
        and pd.notna(calibration_frame.iloc[0]["slope"])
    ):
        fit_row = calibration_frame.iloc[0]
        x_min = float(calibration_frame["concentration_value"].min())
        x_max = float(calibration_frame["concentration_value"].max())
        fit_x = np.linspace(x_min, x_max, 200)
        fit_y = float(fit_row["slope"]) * fit_x + float(fit_row["intercept"])
        label = f"fit (R^2={fit_row['r_squared']:.3g})" if pd.notna(fit_row["r_squared"]) else "fit"
        axis.plot(fit_x, fit_y, color="#e45756", linewidth=1.8, label=label)

    axis.set_xlabel("concentration")
    axis.set_ylabel("mean_min_current_a")
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend()
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)


def save_metric_overlay_plot(
    summary_frame: pd.DataFrame,
    file_path: Path,
    title: str,
    *,
    series_column: str,
    mean_column: str,
    std_column: str,
    slope_column: str,
    intercept_column: str,
    r_squared_column: str,
    y_label: str,
) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(8.0, 5.2))
    color_map = plt.get_cmap("tab10")
    series_values = list(summary_frame[series_column].dropna().unique())
    for index, series_value in enumerate(sorted(series_values)):
        subset = summary_frame[summary_frame[series_column] == series_value].sort_values("concentration_value")
        color = color_map(index % 10)
        axis.errorbar(
            subset["concentration_value"],
            subset[mean_column],
            yerr=subset[std_column],
            fmt="o",
            color=color,
            capsize=3,
            linewidth=1.2,
            alpha=0.9,
        )
        if (
            len(subset) >= 2
            and subset["concentration_value"].nunique() >= 2
            and pd.notna(subset.iloc[0][slope_column])
        ):
            fit_x = np.linspace(
                float(subset["concentration_value"].min()),
                float(subset["concentration_value"].max()),
                200,
            )
            fit_y = float(subset.iloc[0][slope_column]) * fit_x + float(subset.iloc[0][intercept_column])
            label = str(series_value)
            if pd.notna(subset.iloc[0][r_squared_column]):
                label = f"{label} (R^2={subset.iloc[0][r_squared_column]:.3g})"
            axis.plot(fit_x, fit_y, color=color, linewidth=1.8, label=label)
        else:
            axis.plot([], [], color=color, linewidth=1.8, label=str(series_value))

    axis.set_xlabel("concentration")
    axis.set_ylabel(y_label)
    axis.set_title(title)
    axis.grid(True, alpha=0.3)
    axis.legend(fontsize=8)
    figure.tight_layout()
    figure.savefig(file_path, dpi=180)
    plt.close(figure)
