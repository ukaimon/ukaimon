from __future__ import annotations

import math
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parent
SCAN_RANGE = range(1, 6)
OUTPUT_DIR = ROOT / "analysis_output_scan_comparison"
CALIBRATION_DIR = OUTPUT_DIR / "calibration"


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


def load_scan_outputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    condition_frames = []
    fit_frames = []

    for scan in SCAN_RANGE:
        scan_dir = ROOT / f"analysis_output_scan{scan}"
        condition_path = scan_dir / "condition_min_currents.csv"
        fit_path = scan_dir / "calibration_fits.csv"
        if not condition_path.exists() or not fit_path.exists():
            raise FileNotFoundError(
                f"Missing scan output for scan {scan}. Expected {condition_path} and {fit_path}."
            )

        condition_df = pd.read_csv(condition_path)
        condition_df["scan"] = scan
        condition_frames.append(condition_df)

        fit_df = pd.read_csv(fit_path)
        fit_df["scan"] = scan
        fit_frames.append(fit_df)

    all_conditions = pd.concat(condition_frames, ignore_index=True)
    all_fits = pd.concat(fit_frames, ignore_index=True)
    return all_conditions, all_fits


def draw_pressure_overlay(
    ax: plt.Axes, pressure: int, conditions: pd.DataFrame, fits: pd.DataFrame, colors: dict[int, str]
) -> None:
    pressure_conditions = conditions[conditions["pressure"] == pressure]
    pressure_fits = fits[fits["pressure"] == pressure]

    for scan in SCAN_RANGE:
        scan_conditions = pressure_conditions[pressure_conditions["scan"] == scan].sort_values("concentration_ppm")
        scan_fit = pressure_fits[pressure_fits["scan"] == scan].iloc[0]
        x = scan_conditions["concentration_ppm"].to_numpy(dtype=float)
        y = scan_conditions["mean_of_file_min_current_uA"].to_numpy(dtype=float)
        yerr = scan_conditions["std_of_file_min_current_uA"].to_numpy(dtype=float)
        color = colors[scan]

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
        fit_y = float(scan_fit["slope"]) * fit_x + float(scan_fit["intercept"])
        ax.plot(
            fit_x,
            fit_y,
            color=color,
            linewidth=2.0,
            label=f"Scan {scan} (R$^2$={format_sigfig(float(scan_fit['r_squared']))})",
        )

    ax.set_title(f"Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Minimum current (uA)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8)


def plot_overlays(conditions: pd.DataFrame, fits: pd.DataFrame) -> None:
    ensure_dir(CALIBRATION_DIR)
    colors = {
        1: "#4C78A8",
        2: "#F58518",
        3: "#54A24B",
        4: "#E45756",
        5: "#7E57C2",
    }
    pressures = sorted(conditions["pressure"].unique())

    for pressure in pressures:
        fig, ax = plt.subplots(figsize=(7, 5))
        draw_pressure_overlay(ax, pressure, conditions, fits, colors)
        fig.tight_layout()
        fig.savefig(CALIBRATION_DIR / f"calibration_overlay_pressure_{pressure}_scan1_to_5.png", dpi=200)
        plt.close(fig)

    fig, axes = plt.subplots(1, len(pressures), figsize=(7 * len(pressures), 5), sharey=False)
    if len(pressures) == 1:
        axes = [axes]
    for ax, pressure in zip(axes, pressures):
        draw_pressure_overlay(ax, pressure, conditions, fits, colors)
    fig.suptitle("Calibration Overlay of Scan 1 to Scan 5", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(CALIBRATION_DIR / "calibration_overlay_scan1_to_5_by_pressure.png", dpi=200)
    plt.close(fig)


def save_summary_csv(fits: pd.DataFrame) -> None:
    ensure_dir(OUTPUT_DIR)
    fits.sort_values(["pressure", "scan"]).to_csv(
        OUTPUT_DIR / "scan1_to_5_calibration_fits.csv", index=False, encoding="utf-8-sig"
    )


def main() -> None:
    ensure_dir(OUTPUT_DIR)
    ensure_dir(CALIBRATION_DIR)
    conditions, fits = load_scan_outputs()
    save_summary_csv(fits)
    plot_overlays(conditions, fits)
    print("Created scan 1 to 5 calibration overlays in", CALIBRATION_DIR)


if __name__ == "__main__":
    main()
