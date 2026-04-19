from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "analysis_output_cycle1_reference_potential"
CALIBRATION_DIR = OUTPUT_DIR / "calibration"

TOP_DIR_RE = re.compile(r"高さ=(?P<pressure>\d+)")
FILE_RE = re.compile(
    r"濃度=(?P<concentration>\d+)ppm__chip=(?P<chip>\d+)__rep=(?P<rep>\d+)\.txt$"
)
TARGET_CYCLES = (2, 3, 4, 5)
PLOTTED_CYCLES = (1, 2, 3, 4, 5)


@dataclass(frozen=True)
class Curve:
    potential: np.ndarray
    current: np.ndarray


@dataclass
class ReferenceMeasurement:
    path: Path
    pressure: int
    concentration_ppm: int
    chip: int
    rep: int
    cycle1_ref_potential: float
    cycle1_min_current: float
    cycle1_branch: int
    cycle_currents_at_ref: dict[int, float]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def format_sigfig(value: float, digits: int = 3) -> str:
    if not math.isfinite(value):
        return ""
    if math.isclose(value, 0.0, abs_tol=10 ** (-(digits + 1))):
        return "0"

    text = f"{value:.{digits}g}"
    if text == "-0":
        return "0"
    return text


def apply_sigfig_format(ax: plt.Axes, digits: int = 3) -> None:
    formatter = FuncFormatter(lambda value, _pos: format_sigfig(value, digits))
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_formatter(formatter)


def fit_line(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    slope, intercept = np.polyfit(x, y, 1)
    fit = slope * x + intercept
    denominator = float(np.sum((y - y.mean()) ** 2))
    if math.isclose(denominator, 0.0):
        r_squared = float("nan")
    else:
        r_squared = 1.0 - float(np.sum((y - fit) ** 2) / denominator)
    return float(slope), float(intercept), float(r_squared)


def split_curve(curve: Curve) -> tuple[Curve, Curve, Curve]:
    e = curve.potential
    i = curve.current
    max_index = int(np.nanargmax(e))
    min_index = int(np.nanargmin(e))
    if min_index <= max_index:
        raise ValueError("Unexpected CV order: minimum potential appears before maximum.")

    seg1 = Curve(e[: max_index + 1], i[: max_index + 1])
    seg2 = Curve(e[max_index : min_index + 1], i[max_index : min_index + 1])
    seg3 = Curve(e[min_index:], i[min_index:])
    return seg1, seg2, seg3


def determine_branch_for_index(curve: Curve, index: int) -> int:
    max_index = int(np.nanargmax(curve.potential))
    min_index = int(np.nanargmin(curve.potential))
    if index <= max_index:
        return 1
    if index <= min_index:
        return 2
    return 3


def interpolate_on_branch(curve: Curve, target_potential: float, branch: int) -> float:
    segment = split_curve(curve)[branch - 1]
    x = segment.potential
    y = segment.current

    if not (float(np.nanmin(x)) <= target_potential <= float(np.nanmax(x))):
        raise ValueError(f"Target potential {target_potential} is outside branch {branch} range.")

    if x[0] > x[-1]:
        x = x[::-1]
        y = y[::-1]

    return float(np.interp(target_potential, x, y))


def read_cycles(path: Path) -> list[Curve]:
    raw = pd.read_csv(path, sep="\t", engine="python")
    raw = raw.dropna(axis=1, how="all")
    if raw.shape[1] % 2 != 0:
        raise ValueError(f"Unexpected column count in {path}")

    cycles: list[Curve] = []
    for index in range(0, raw.shape[1], 2):
        potential = pd.to_numeric(raw.iloc[:, index], errors="coerce").to_numpy(dtype=float)
        current = pd.to_numeric(raw.iloc[:, index + 1], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(potential) & np.isfinite(current)
        cycles.append(Curve(potential=potential[mask], current=current[mask]))
    return cycles


def load_measurements() -> list[ReferenceMeasurement]:
    measurements: list[ReferenceMeasurement] = []
    for path in sorted(ROOT.rglob("*.txt")):
        top_match = TOP_DIR_RE.search(path.parent.name)
        file_match = FILE_RE.search(path.name)
        if top_match is None or file_match is None:
            raise ValueError(f"Could not parse metadata from {path}")

        cycles = read_cycles(path)
        if len(cycles) < 5:
            raise ValueError(f"{path.name} has fewer than 5 cycles.")

        cycle1 = cycles[0]
        cycle1_min_index = int(np.argmin(cycle1.current))
        cycle1_ref_potential = float(cycle1.potential[cycle1_min_index])
        cycle1_min_current = float(cycle1.current[cycle1_min_index])
        cycle1_branch = determine_branch_for_index(cycle1, cycle1_min_index)

        cycle_currents_at_ref = {}
        for cycle_number in TARGET_CYCLES:
            cycle_currents_at_ref[cycle_number] = interpolate_on_branch(
                cycles[cycle_number - 1],
                cycle1_ref_potential,
                cycle1_branch,
            )

        measurements.append(
            ReferenceMeasurement(
                path=path,
                pressure=int(top_match.group("pressure")),
                concentration_ppm=int(file_match.group("concentration")),
                chip=int(file_match.group("chip")),
                rep=int(file_match.group("rep")),
                cycle1_ref_potential=cycle1_ref_potential,
                cycle1_min_current=cycle1_min_current,
                cycle1_branch=cycle1_branch,
                cycle_currents_at_ref=cycle_currents_at_ref,
            )
        )

    return measurements


def save_file_tables(measurements: list[ReferenceMeasurement]) -> pd.DataFrame:
    wide_rows = []
    long_rows = []

    for measurement in measurements:
        row = {
            "pressure": measurement.pressure,
            "concentration_ppm": measurement.concentration_ppm,
            "chip": measurement.chip,
            "rep": measurement.rep,
            "file_name": measurement.path.name,
            "relative_path": measurement.path.relative_to(ROOT).as_posix(),
            "cycle1_ref_potential_V": measurement.cycle1_ref_potential,
            "cycle1_min_current_uA": measurement.cycle1_min_current,
            "cycle1_branch": measurement.cycle1_branch,
            "cycle1_current_at_cycle1_ref_uA": measurement.cycle1_min_current,
        }
        long_rows.append(
            {
                "pressure": measurement.pressure,
                "concentration_ppm": measurement.concentration_ppm,
                "chip": measurement.chip,
                "rep": measurement.rep,
                "file_name": measurement.path.name,
                "cycle": 1,
                "cycle1_ref_potential_V": measurement.cycle1_ref_potential,
                "cycle1_min_current_uA": measurement.cycle1_min_current,
                "current_at_cycle1_ref_uA": measurement.cycle1_min_current,
            }
        )
        for cycle_number in TARGET_CYCLES:
            current_value = measurement.cycle_currents_at_ref[cycle_number]
            row[f"cycle{cycle_number}_current_at_cycle1_ref_uA"] = current_value
            long_rows.append(
                {
                    "pressure": measurement.pressure,
                    "concentration_ppm": measurement.concentration_ppm,
                    "chip": measurement.chip,
                    "rep": measurement.rep,
                    "file_name": measurement.path.name,
                    "cycle": cycle_number,
                    "cycle1_ref_potential_V": measurement.cycle1_ref_potential,
                    "cycle1_min_current_uA": measurement.cycle1_min_current,
                    "current_at_cycle1_ref_uA": current_value,
                }
            )
        wide_rows.append(row)

    wide_df = pd.DataFrame(wide_rows).sort_values(["pressure", "concentration_ppm", "chip", "rep"])
    long_df = pd.DataFrame(long_rows).sort_values(["cycle", "pressure", "concentration_ppm", "chip", "rep"])
    wide_df.to_csv(OUTPUT_DIR / "file_cycle1_reference_currents.csv", index=False, encoding="utf-8-sig")
    long_df.to_csv(OUTPUT_DIR / "file_cycle1_reference_currents_long.csv", index=False, encoding="utf-8-sig")
    return long_df


def aggregate_conditions(long_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        long_df.groupby(["cycle", "pressure", "concentration_ppm"], as_index=False)
        .agg(
            n_files=("current_at_cycle1_ref_uA", "size"),
            mean_current_uA=("current_at_cycle1_ref_uA", "mean"),
            std_current_uA=("current_at_cycle1_ref_uA", "std"),
            mean_cycle1_ref_potential_V=("cycle1_ref_potential_V", "mean"),
            std_cycle1_ref_potential_V=("cycle1_ref_potential_V", "std"),
        )
        .sort_values(["cycle", "pressure", "concentration_ppm"])
    )
    grouped["std_current_uA"] = grouped["std_current_uA"].fillna(0.0)
    grouped["std_cycle1_ref_potential_V"] = grouped["std_cycle1_ref_potential_V"].fillna(0.0)
    grouped.to_csv(
        OUTPUT_DIR / "condition_cycle1_reference_currents.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return grouped


def save_fit_table(condition_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (cycle, pressure), subset in condition_df.groupby(["cycle", "pressure"], sort=True):
        x = subset["concentration_ppm"].to_numpy(dtype=float)
        y = subset["mean_current_uA"].to_numpy(dtype=float)
        slope, intercept, r_squared = fit_line(x, y)
        rows.append(
            {
                "cycle": cycle,
                "pressure": pressure,
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_squared,
            }
        )

    fits_df = pd.DataFrame(rows).sort_values(["pressure", "cycle"])
    fits_df.to_csv(OUTPUT_DIR / "cycle1_reference_calibration_fits.csv", index=False, encoding="utf-8-sig")
    return fits_df


def draw_pressure_axis(ax: plt.Axes, pressure: int, condition_df: pd.DataFrame, fits_df: pd.DataFrame) -> None:
    colors = {
        1: "#2E4057",
        2: "#4C78A8",
        3: "#F58518",
        4: "#54A24B",
        5: "#E45756",
    }
    pressure_conditions = condition_df[condition_df["pressure"] == pressure]
    pressure_fits = fits_df[fits_df["pressure"] == pressure]

    for cycle_number in PLOTTED_CYCLES:
        cycle_conditions = pressure_conditions[pressure_conditions["cycle"] == cycle_number].sort_values(
            "concentration_ppm"
        )
        cycle_fit = pressure_fits[pressure_fits["cycle"] == cycle_number].iloc[0]
        x = cycle_conditions["concentration_ppm"].to_numpy(dtype=float)
        y = cycle_conditions["mean_current_uA"].to_numpy(dtype=float)
        yerr = cycle_conditions["std_current_uA"].to_numpy(dtype=float)
        color = colors[cycle_number]

        ax.errorbar(
            x,
            y,
            yerr=yerr,
            fmt="o",
            color=color,
            capsize=3,
            linewidth=1.2,
            alpha=0.9,
        )

        fit_x = np.linspace(float(x.min()), float(x.max()), 200)
        fit_y = float(cycle_fit["slope"]) * fit_x + float(cycle_fit["intercept"])
        ax.plot(
            fit_x,
            fit_y,
            color=color,
            linewidth=2.0,
            label=(
                f"Scan {cycle_number} "
                f"(m={format_sigfig(float(cycle_fit['slope']))}, "
                f"R$^2$={format_sigfig(float(cycle_fit['r_squared']))})"
            ),
        )

    ax.set_title(f"Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Current at scan1 min-potential (uA)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8)


def plot_calibrations(condition_df: pd.DataFrame, fits_df: pd.DataFrame) -> None:
    ensure_dir(CALIBRATION_DIR)
    pressures = sorted(condition_df["pressure"].unique())

    for pressure in pressures:
        fig, ax = plt.subplots(figsize=(7, 5))
        draw_pressure_axis(ax, pressure, condition_df, fits_df)
        fig.tight_layout()
        fig.savefig(
            CALIBRATION_DIR / f"calibration_overlay_pressure_{pressure}_cycle2_to_5_at_cycle1_ref.png",
            dpi=200,
        )
        plt.close(fig)

    fig, axes = plt.subplots(1, len(pressures), figsize=(7 * len(pressures), 5), sharey=False)
    if len(pressures) == 1:
        axes = [axes]
    for ax, pressure in zip(axes, pressures):
        draw_pressure_axis(ax, pressure, condition_df, fits_df)
    fig.suptitle("Calibration Curves Using Scan 1 Minimum-Potential as Reference", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(CALIBRATION_DIR / "calibration_overlay_cycle2_to_5_at_cycle1_ref_by_pressure.png", dpi=200)
    plt.close(fig)


def main() -> None:
    ensure_dir(OUTPUT_DIR)
    ensure_dir(CALIBRATION_DIR)

    measurements = load_measurements()
    file_long_df = save_file_tables(measurements)
    condition_df = aggregate_conditions(file_long_df)
    fits_df = save_fit_table(condition_df)
    plot_calibrations(condition_df, fits_df)

    print("Measurements loaded:", len(measurements))
    print("Cycle1-reference calibration outputs:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
