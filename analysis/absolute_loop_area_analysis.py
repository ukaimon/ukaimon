from __future__ import annotations

import math
import re
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "analysis_output_absolute_loop_area"
CALIBRATION_DIR = OUTPUT_DIR / "calibration"
BY_SCAN_DIR = CALIBRATION_DIR / "by_scan"
BY_PRESSURE_DIR = CALIBRATION_DIR / "by_pressure"

TOP_DIR_RE = re.compile(r"高さ=(?P<pressure>\d+)")
FILE_RE = re.compile(
    r"濃度=(?P<concentration>\d+)ppm__chip=(?P<chip>\d+)__rep=(?P<rep>\d+)\.txt$"
)


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


def fit_line(x_values: np.ndarray, y_values: np.ndarray) -> tuple[float, float, float]:
    slope, intercept = np.polyfit(x_values, y_values, 1)
    fit_values = slope * x_values + intercept
    denominator = float(np.sum((y_values - y_values.mean()) ** 2))
    if math.isclose(denominator, 0.0):
        r_squared = float("nan")
    else:
        r_squared = 1.0 - float(np.sum((y_values - fit_values) ** 2) / denominator)
    return float(slope), float(intercept), float(r_squared)


def read_scan_curves(path: Path) -> list[tuple[np.ndarray, np.ndarray]]:
    raw = pd.read_csv(path, sep="\t", engine="python").dropna(axis=1, how="all")
    if raw.shape[1] % 2 != 0:
        raise ValueError(f"Unexpected column count in {path}")

    curves: list[tuple[np.ndarray, np.ndarray]] = []
    for index in range(0, raw.shape[1], 2):
        potential = pd.to_numeric(raw.iloc[:, index], errors="coerce").to_numpy(dtype=float)
        current = pd.to_numeric(raw.iloc[:, index + 1], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(potential) & np.isfinite(current)
        curves.append((potential[mask], current[mask]))
    return curves


def absolute_loop_area(potential: np.ndarray, current: np.ndarray) -> float:
    if len(potential) < 2:
        return float("nan")
    trapezoid = getattr(np, "trapezoid", None)
    if callable(trapezoid):
        area = float(trapezoid(current, potential))
    else:
        area = float(np.trapz(current, potential))
    return abs(area)


def load_loop_area_tables() -> tuple[pd.DataFrame, pd.DataFrame, list[int]]:
    wide_rows: list[dict[str, float | int | str]] = []
    long_rows: list[dict[str, float | int | str]] = []
    available_scans: set[int] = set()

    for path in sorted(ROOT.rglob("*.txt")):
        top_match = TOP_DIR_RE.search(path.parent.name)
        file_match = FILE_RE.search(path.name)
        if top_match is None or file_match is None:
            raise ValueError(f"Could not parse metadata from {path}")

        pressure = int(top_match.group("pressure"))
        concentration = int(file_match.group("concentration"))
        chip = int(file_match.group("chip"))
        rep = int(file_match.group("rep"))
        curves = read_scan_curves(path)

        row: dict[str, float | int | str] = {
            "pressure": pressure,
            "concentration_ppm": concentration,
            "chip": chip,
            "rep": rep,
            "file_name": path.name,
            "relative_path": path.relative_to(ROOT).as_posix(),
        }

        scan_areas: list[float] = []
        for scan_index, (potential, current) in enumerate(curves, start=1):
            loop_area_value = absolute_loop_area(potential, current)
            row[f"scan{scan_index}_absolute_loop_area_uA_V"] = loop_area_value
            if 1 <= scan_index <= 5:
                scan_areas.append(loop_area_value)
            available_scans.add(scan_index)
            long_rows.append(
                {
                    "scan": scan_index,
                    "pressure": pressure,
                    "concentration_ppm": concentration,
                    "chip": chip,
                    "rep": rep,
                    "file_name": path.name,
                    "absolute_loop_area_uA_V": loop_area_value,
                }
            )

        if scan_areas:
            row["scan1_to_5_mean_absolute_loop_area_uA_V"] = float(np.mean(scan_areas))
        wide_rows.append(row)

    wide_df = pd.DataFrame(wide_rows).sort_values(["pressure", "concentration_ppm", "chip", "rep"])
    long_df = pd.DataFrame(long_rows).sort_values(["scan", "pressure", "concentration_ppm", "chip", "rep"])
    wide_df.to_csv(OUTPUT_DIR / "file_absolute_loop_areas.csv", index=False, encoding="utf-8-sig")
    long_df.to_csv(OUTPUT_DIR / "file_absolute_loop_areas_long.csv", index=False, encoding="utf-8-sig")
    return wide_df, long_df, sorted(available_scans)


def aggregate_conditions(long_df: pd.DataFrame) -> pd.DataFrame:
    condition_df = (
        long_df.groupby(["scan", "pressure", "concentration_ppm"], as_index=False)
        .agg(
            n_files=("absolute_loop_area_uA_V", "size"),
            mean_absolute_loop_area_uA_V=("absolute_loop_area_uA_V", "mean"),
            std_absolute_loop_area_uA_V=("absolute_loop_area_uA_V", "std"),
        )
        .sort_values(["scan", "pressure", "concentration_ppm"])
    )
    condition_df["std_absolute_loop_area_uA_V"] = condition_df["std_absolute_loop_area_uA_V"].fillna(0.0)
    condition_df.to_csv(OUTPUT_DIR / "condition_absolute_loop_areas.csv", index=False, encoding="utf-8-sig")
    return condition_df


def aggregate_scan_mean(wide_df: pd.DataFrame) -> pd.DataFrame:
    mean_df = (
        wide_df.groupby(["pressure", "concentration_ppm"], as_index=False)
        .agg(
            n_files=("scan1_to_5_mean_absolute_loop_area_uA_V", "size"),
            mean_absolute_loop_area_uA_V=("scan1_to_5_mean_absolute_loop_area_uA_V", "mean"),
            std_absolute_loop_area_uA_V=("scan1_to_5_mean_absolute_loop_area_uA_V", "std"),
        )
        .sort_values(["pressure", "concentration_ppm"])
    )
    mean_df["std_absolute_loop_area_uA_V"] = mean_df["std_absolute_loop_area_uA_V"].fillna(0.0)
    mean_df["scan"] = "mean_scan1_to_5"
    mean_df.to_csv(
        OUTPUT_DIR / "condition_absolute_loop_areas_scan1_to_5_mean.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return mean_df


def save_fit_table(condition_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (scan, pressure), subset in condition_df.groupby(["scan", "pressure"], sort=True):
        x_values = subset["concentration_ppm"].to_numpy(dtype=float)
        y_values = subset["mean_absolute_loop_area_uA_V"].to_numpy(dtype=float)
        slope, intercept, r_squared = fit_line(x_values, y_values)
        rows.append(
            {
                "scan": scan,
                "pressure": pressure,
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_squared,
            }
        )
    fits_df = pd.DataFrame(rows).sort_values(["scan", "pressure"])
    fits_df.to_csv(OUTPUT_DIR / "absolute_loop_area_calibration_fits.csv", index=False, encoding="utf-8-sig")
    return fits_df


def save_scan_mean_fit_table(mean_condition_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for pressure, subset in mean_condition_df.groupby("pressure", sort=True):
        x_values = subset["concentration_ppm"].to_numpy(dtype=float)
        y_values = subset["mean_absolute_loop_area_uA_V"].to_numpy(dtype=float)
        slope, intercept, r_squared = fit_line(x_values, y_values)
        rows.append(
            {
                "scan": "mean_scan1_to_5",
                "pressure": pressure,
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_squared,
            }
        )
    mean_fits_df = pd.DataFrame(rows).sort_values("pressure")
    mean_fits_df.to_csv(
        OUTPUT_DIR / "absolute_loop_area_calibration_fits_scan1_to_5_mean.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return mean_fits_df


def draw_scan_axis(
    ax: plt.Axes,
    scan: int,
    pressure: int,
    condition_df: pd.DataFrame,
    fits_df: pd.DataFrame,
) -> None:
    pressure_colors = {68: "#4C78A8", 69: "#F58518", 70: "#E45756"}
    subset = condition_df[(condition_df["scan"] == scan) & (condition_df["pressure"] == pressure)].sort_values(
        "concentration_ppm"
    )
    fit_row = fits_df[(fits_df["scan"] == scan) & (fits_df["pressure"] == pressure)].iloc[0]
    color = pressure_colors.get(pressure, "#2E4057")

    x_values = subset["concentration_ppm"].to_numpy(dtype=float)
    y_values = subset["mean_absolute_loop_area_uA_V"].to_numpy(dtype=float)
    yerr = subset["std_absolute_loop_area_uA_V"].to_numpy(dtype=float)

    ax.errorbar(x_values, y_values, yerr=yerr, fmt="o", color=color, capsize=3, linewidth=1.2, alpha=0.95)

    fit_x = np.linspace(float(x_values.min()), float(x_values.max()), 200)
    fit_y = float(fit_row["slope"]) * fit_x + float(fit_row["intercept"])
    ax.plot(fit_x, fit_y, color=color, linewidth=2.0, label=f"Pressure {pressure}")

    ax.set_title(f"Scan {scan} | Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Absolute loop area (uA·V)")
    apply_sigfig_format(ax)
    intercept = float(fit_row["intercept"])
    intercept_sign = "+" if intercept >= 0 else "-"
    ax.text(
        0.03,
        0.05,
        "\n".join(
            [
                rf"$A = {format_sigfig(float(fit_row['slope']))}\,C {intercept_sign}\,{format_sigfig(abs(intercept))}$",
                rf"$R^2 = {format_sigfig(float(fit_row['r_squared']))}$",
            ]
        ),
        transform=ax.transAxes,
        fontsize=9,
        linespacing=1.35,
        bbox={
            "boxstyle": "round,pad=0.35",
            "facecolor": "white",
            "alpha": 0.9,
            "edgecolor": "#C8CCD3",
            "linewidth": 1.0,
        },
    )


def plot_by_scan(condition_df: pd.DataFrame, fits_df: pd.DataFrame, scans: list[int]) -> None:
    ensure_dir(BY_SCAN_DIR)
    pressures = sorted(condition_df["pressure"].unique())

    for scan in scans:
        fig, axes = plt.subplots(1, len(pressures), figsize=(7 * len(pressures), 5), sharey=False)
        if len(pressures) == 1:
            axes = [axes]
        for ax, pressure in zip(axes, pressures):
            draw_scan_axis(ax, scan, pressure, condition_df, fits_df)
        fig.suptitle(f"Absolute-Loop-Area Calibration Curves | Scan {scan}", fontsize=14)
        fig.tight_layout(rect=(0, 0, 1, 0.95))
        fig.savefig(BY_SCAN_DIR / f"absolute_loop_area_calibration_scan_{scan}.png", dpi=200)
        plt.close(fig)


def draw_pressure_overlay(
    ax: plt.Axes,
    pressure: int,
    condition_df: pd.DataFrame,
    fits_df: pd.DataFrame,
    mean_condition_df: pd.DataFrame,
    mean_fits_df: pd.DataFrame,
    scans: list[int],
) -> None:
    scan_colors = {
        1: "#2E4057",
        2: "#4C78A8",
        3: "#F58518",
        4: "#54A24B",
        5: "#E45756",
    }
    pressure_conditions = condition_df[condition_df["pressure"] == pressure]
    pressure_fits = fits_df[fits_df["pressure"] == pressure]

    for scan in scans:
        subset = pressure_conditions[pressure_conditions["scan"] == scan].sort_values("concentration_ppm")
        fit_row = pressure_fits[pressure_fits["scan"] == scan].iloc[0]
        x_values = subset["concentration_ppm"].to_numpy(dtype=float)
        y_values = subset["mean_absolute_loop_area_uA_V"].to_numpy(dtype=float)
        yerr = subset["std_absolute_loop_area_uA_V"].to_numpy(dtype=float)
        color = scan_colors.get(scan, "#777777")

        ax.errorbar(x_values, y_values, yerr=yerr, fmt="o", color=color, capsize=3, linewidth=1.2, alpha=0.9)

        fit_x = np.linspace(float(x_values.min()), float(x_values.max()), 200)
        fit_y = float(fit_row["slope"]) * fit_x + float(fit_row["intercept"])
        ax.plot(
            fit_x,
            fit_y,
            color=color,
            linewidth=2.0,
            label=(
                f"Scan {scan} "
                f"(m={format_sigfig(float(fit_row['slope']))}, "
                f"R$^2$={format_sigfig(float(fit_row['r_squared']))})"
            ),
        )

    mean_subset = mean_condition_df[mean_condition_df["pressure"] == pressure].sort_values("concentration_ppm")
    mean_fit_row = mean_fits_df[mean_fits_df["pressure"] == pressure].iloc[0]
    mean_x = mean_subset["concentration_ppm"].to_numpy(dtype=float)
    mean_y = mean_subset["mean_absolute_loop_area_uA_V"].to_numpy(dtype=float)
    mean_yerr = mean_subset["std_absolute_loop_area_uA_V"].to_numpy(dtype=float)
    ax.errorbar(mean_x, mean_y, yerr=mean_yerr, fmt="s", color="black", capsize=4, linewidth=1.4, alpha=0.95)

    mean_fit_x = np.linspace(float(mean_x.min()), float(mean_x.max()), 200)
    mean_fit_y = float(mean_fit_row["slope"]) * mean_fit_x + float(mean_fit_row["intercept"])
    ax.plot(
        mean_fit_x,
        mean_fit_y,
        color="black",
        linestyle="--",
        linewidth=2.2,
        label=(
            "Mean of Scan 1-5 "
            f"(m={format_sigfig(float(mean_fit_row['slope']))}, "
            f"R$^2$={format_sigfig(float(mean_fit_row['r_squared']))})"
        ),
    )

    ax.set_title(f"Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Absolute loop area (uA·V)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8)


def plot_by_pressure(
    condition_df: pd.DataFrame,
    fits_df: pd.DataFrame,
    mean_condition_df: pd.DataFrame,
    mean_fits_df: pd.DataFrame,
    scans: list[int],
) -> None:
    ensure_dir(BY_PRESSURE_DIR)
    pressures = sorted(condition_df["pressure"].unique())

    for pressure in pressures:
        fig, ax = plt.subplots(figsize=(7.8, 5.4))
        draw_pressure_overlay(ax, pressure, condition_df, fits_df, mean_condition_df, mean_fits_df, scans)
        fig.tight_layout()
        fig.savefig(BY_PRESSURE_DIR / f"absolute_loop_area_calibration_pressure_{pressure}_scan1_to_5.png", dpi=200)
        plt.close(fig)

    fig, axes = plt.subplots(1, len(pressures), figsize=(7.8 * len(pressures), 5.4), sharey=False)
    if len(pressures) == 1:
        axes = [axes]
    for ax, pressure in zip(axes, pressures):
        draw_pressure_overlay(ax, pressure, condition_df, fits_df, mean_condition_df, mean_fits_df, scans)
    fig.suptitle("Absolute-Loop-Area Calibration Overlay of Scan 1 to Scan 5", fontsize=15)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(BY_PRESSURE_DIR / "absolute_loop_area_calibration_overlay_scan1_to_5_by_pressure.png", dpi=200)
    plt.close(fig)
