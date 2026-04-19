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
OUTPUT_DIR = ROOT / "analysis_output_absolute_integral"
CALIBRATION_DIR = OUTPUT_DIR / "calibration"
BY_SCAN_DIR = CALIBRATION_DIR / "by_scan"
BY_PRESSURE_DIR = CALIBRATION_DIR / "by_pressure"

TOP_DIR_RE = re.compile(r"高さ=(?P<pressure>\d+)")
FILE_RE = re.compile(
    r"濃度=(?P<concentration>\d+)ppm__chip=(?P<chip>\d+)__rep=(?P<rep>\d+)\.txt$"
)
SCAN_RANGE = range(1, 6)
SCAN_MEAN_LABEL = "mean_scan1_to_5"


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


def absolute_integral(potential: np.ndarray, current: np.ndarray) -> float:
    delta_potential = np.abs(np.diff(potential))
    abs_current_mid = (np.abs(current[:-1]) + np.abs(current[1:])) / 2.0
    return float(np.sum(abs_current_mid * delta_potential))


def load_integral_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    wide_rows = []
    long_rows = []

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

        row = {
            "pressure": pressure,
            "concentration_ppm": concentration,
            "chip": chip,
            "rep": rep,
            "file_name": path.name,
            "relative_path": path.relative_to(ROOT).as_posix(),
        }

        for scan_index, (potential, current) in enumerate(curves, start=1):
            integral_value = absolute_integral(potential, current)
            row[f"scan{scan_index}_absolute_integral_uA_V"] = integral_value
            long_rows.append(
                {
                    "scan": scan_index,
                    "pressure": pressure,
                    "concentration_ppm": concentration,
                    "chip": chip,
                    "rep": rep,
                    "file_name": path.name,
                    "absolute_integral_uA_V": integral_value,
                }
            )

        scan_columns = [f"scan{scan_index}_absolute_integral_uA_V" for scan_index in SCAN_RANGE]
        row["scan1_to_5_mean_absolute_integral_uA_V"] = float(np.mean([row[column] for column in scan_columns]))
        wide_rows.append(row)

    wide_df = pd.DataFrame(wide_rows).sort_values(["pressure", "concentration_ppm", "chip", "rep"])
    long_df = pd.DataFrame(long_rows).sort_values(["scan", "pressure", "concentration_ppm", "chip", "rep"])
    wide_df.to_csv(OUTPUT_DIR / "file_absolute_integrals.csv", index=False, encoding="utf-8-sig")
    long_df.to_csv(OUTPUT_DIR / "file_absolute_integrals_long.csv", index=False, encoding="utf-8-sig")
    return wide_df, long_df


def aggregate_conditions(long_df: pd.DataFrame) -> pd.DataFrame:
    condition_df = (
        long_df.groupby(["scan", "pressure", "concentration_ppm"], as_index=False)
        .agg(
            n_files=("absolute_integral_uA_V", "size"),
            mean_absolute_integral_uA_V=("absolute_integral_uA_V", "mean"),
            std_absolute_integral_uA_V=("absolute_integral_uA_V", "std"),
        )
        .sort_values(["scan", "pressure", "concentration_ppm"])
    )
    condition_df["std_absolute_integral_uA_V"] = condition_df["std_absolute_integral_uA_V"].fillna(0.0)
    condition_df.to_csv(OUTPUT_DIR / "condition_absolute_integrals.csv", index=False, encoding="utf-8-sig")
    return condition_df


def aggregate_scan_mean(wide_df: pd.DataFrame) -> pd.DataFrame:
    mean_df = (
        wide_df.groupby(["pressure", "concentration_ppm"], as_index=False)
        .agg(
            n_files=("scan1_to_5_mean_absolute_integral_uA_V", "size"),
            mean_absolute_integral_uA_V=("scan1_to_5_mean_absolute_integral_uA_V", "mean"),
            std_absolute_integral_uA_V=("scan1_to_5_mean_absolute_integral_uA_V", "std"),
        )
        .sort_values(["pressure", "concentration_ppm"])
    )
    mean_df["std_absolute_integral_uA_V"] = mean_df["std_absolute_integral_uA_V"].fillna(0.0)
    mean_df["scan"] = SCAN_MEAN_LABEL
    mean_df.to_csv(
        OUTPUT_DIR / "condition_absolute_integrals_scan1_to_5_mean.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return mean_df


def save_fit_table(condition_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (scan, pressure), subset in condition_df.groupby(["scan", "pressure"], sort=True):
        x = subset["concentration_ppm"].to_numpy(dtype=float)
        y = subset["mean_absolute_integral_uA_V"].to_numpy(dtype=float)
        slope, intercept, r_squared = fit_line(x, y)
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
    fits_df.to_csv(OUTPUT_DIR / "absolute_integral_calibration_fits.csv", index=False, encoding="utf-8-sig")
    return fits_df


def save_scan_mean_fit_table(mean_condition_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for pressure, subset in mean_condition_df.groupby("pressure", sort=True):
        x = subset["concentration_ppm"].to_numpy(dtype=float)
        y = subset["mean_absolute_integral_uA_V"].to_numpy(dtype=float)
        slope, intercept, r_squared = fit_line(x, y)
        rows.append(
            {
                "scan": SCAN_MEAN_LABEL,
                "pressure": pressure,
                "slope": slope,
                "intercept": intercept,
                "r_squared": r_squared,
            }
        )

    mean_fits_df = pd.DataFrame(rows).sort_values("pressure")
    mean_fits_df.to_csv(
        OUTPUT_DIR / "absolute_integral_calibration_fits_scan1_to_5_mean.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return mean_fits_df


def draw_scan_axis(ax: plt.Axes, scan: int, pressure: int, condition_df: pd.DataFrame, fits_df: pd.DataFrame) -> None:
    pressure_colors = {68: "#4C78A8", 69: "#F58518", 70: "#E45756"}
    subset = condition_df[(condition_df["scan"] == scan) & (condition_df["pressure"] == pressure)].sort_values(
        "concentration_ppm"
    )
    fit_row = fits_df[(fits_df["scan"] == scan) & (fits_df["pressure"] == pressure)].iloc[0]
    color = pressure_colors.get(pressure, "#2E4057")

    x = subset["concentration_ppm"].to_numpy(dtype=float)
    y = subset["mean_absolute_integral_uA_V"].to_numpy(dtype=float)
    yerr = subset["std_absolute_integral_uA_V"].to_numpy(dtype=float)

    ax.errorbar(
        x,
        y,
        yerr=yerr,
        fmt="o",
        color=color,
        capsize=3,
        linewidth=1.2,
        alpha=0.95,
    )

    fit_x = np.linspace(float(x.min()), float(x.max()), 200)
    fit_y = float(fit_row["slope"]) * fit_x + float(fit_row["intercept"])
    ax.plot(
        fit_x,
        fit_y,
        color=color,
        linewidth=2.0,
        label=f"Pressure {pressure}",
    )

    ax.set_title(f"Scan {scan} | Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Absolute integral (uA·V)")
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


def plot_by_scan(condition_df: pd.DataFrame, fits_df: pd.DataFrame) -> None:
    ensure_dir(BY_SCAN_DIR)
    pressures = sorted(condition_df["pressure"].unique())

    for scan in SCAN_RANGE:
        fig, axes = plt.subplots(1, len(pressures), figsize=(7 * len(pressures), 5), sharey=False)
        if len(pressures) == 1:
            axes = [axes]
        for ax, pressure in zip(axes, pressures):
            draw_scan_axis(ax, scan, pressure, condition_df, fits_df)
        fig.suptitle(f"Absolute-Integral Calibration Curves | Scan {scan}", fontsize=14)
        fig.tight_layout(rect=(0, 0, 1, 0.95))
        fig.savefig(BY_SCAN_DIR / f"absolute_integral_calibration_scan_{scan}.png", dpi=200)
        plt.close(fig)


def draw_pressure_overlay(
    ax: plt.Axes,
    pressure: int,
    condition_df: pd.DataFrame,
    fits_df: pd.DataFrame,
    mean_condition_df: pd.DataFrame,
    mean_fits_df: pd.DataFrame,
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

    for scan in SCAN_RANGE:
        subset = pressure_conditions[pressure_conditions["scan"] == scan].sort_values("concentration_ppm")
        fit_row = pressure_fits[pressure_fits["scan"] == scan].iloc[0]
        x = subset["concentration_ppm"].to_numpy(dtype=float)
        y = subset["mean_absolute_integral_uA_V"].to_numpy(dtype=float)
        yerr = subset["std_absolute_integral_uA_V"].to_numpy(dtype=float)
        color = scan_colors[scan]

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
    mean_y = mean_subset["mean_absolute_integral_uA_V"].to_numpy(dtype=float)
    mean_yerr = mean_subset["std_absolute_integral_uA_V"].to_numpy(dtype=float)
    mean_color = "#000000"

    ax.errorbar(
        mean_x,
        mean_y,
        yerr=mean_yerr,
        fmt="s",
        color=mean_color,
        markerfacecolor="white",
        markersize=5,
        capsize=4,
        linewidth=1.5,
        alpha=0.95,
    )

    mean_fit_x = np.linspace(float(mean_x.min()), float(mean_x.max()), 200)
    mean_fit_y = float(mean_fit_row["slope"]) * mean_fit_x + float(mean_fit_row["intercept"])
    ax.plot(
        mean_fit_x,
        mean_fit_y,
        color=mean_color,
        linewidth=2.4,
        linestyle="--",
        label=(
            "Mean of Scan 1-5 "
            f"(m={format_sigfig(float(mean_fit_row['slope']))}, "
            f"R$^2$={format_sigfig(float(mean_fit_row['r_squared']))})"
        ),
    )

    ax.set_title(f"Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Absolute integral (uA·V)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8)


def plot_by_pressure(
    condition_df: pd.DataFrame, fits_df: pd.DataFrame, mean_condition_df: pd.DataFrame, mean_fits_df: pd.DataFrame
) -> None:
    ensure_dir(BY_PRESSURE_DIR)
    pressures = sorted(condition_df["pressure"].unique())

    for pressure in pressures:
        fig, ax = plt.subplots(figsize=(7, 5))
        draw_pressure_overlay(ax, pressure, condition_df, fits_df, mean_condition_df, mean_fits_df)
        fig.tight_layout()
        fig.savefig(BY_PRESSURE_DIR / f"absolute_integral_calibration_pressure_{pressure}_scan1_to_5.png", dpi=200)
        plt.close(fig)

    fig, axes = plt.subplots(1, len(pressures), figsize=(7 * len(pressures), 5), sharey=False)
    if len(pressures) == 1:
        axes = [axes]
    for ax, pressure in zip(axes, pressures):
        draw_pressure_overlay(ax, pressure, condition_df, fits_df, mean_condition_df, mean_fits_df)
    fig.suptitle("Absolute-Integral Calibration Overlay of Scan 1 to Scan 5", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(BY_PRESSURE_DIR / "absolute_integral_calibration_overlay_scan1_to_5_by_pressure.png", dpi=200)
    plt.close(fig)


def main() -> None:
    ensure_dir(OUTPUT_DIR)
    ensure_dir(CALIBRATION_DIR)
    ensure_dir(BY_SCAN_DIR)
    ensure_dir(BY_PRESSURE_DIR)

    wide_df, long_df = load_integral_tables()
    condition_df = aggregate_conditions(long_df)
    fits_df = save_fit_table(condition_df)
    mean_condition_df = aggregate_scan_mean(wide_df)
    mean_fits_df = save_scan_mean_fit_table(mean_condition_df)
    plot_by_scan(condition_df, fits_df)
    plot_by_pressure(condition_df, fits_df, mean_condition_df, mean_fits_df)

    print("Measurements loaded:", len(wide_df))
    print("Absolute-integral outputs:", OUTPUT_DIR)


if __name__ == "__main__":
    main()
