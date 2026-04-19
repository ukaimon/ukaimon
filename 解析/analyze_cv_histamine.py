from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "analysis_output"
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
INDIVIDUAL_DIR = OUTPUT_DIR / "individual_voltammograms"
MEAN_DIR = OUTPUT_DIR / "mean_voltammograms"
CALIBRATION_DIR = OUTPUT_DIR / "calibration"
SCAN_INDEX_USED: int | None = None
REPRESENTATIVE_LABEL = "Mean of 5 scans"
CONDITION_MEAN_LABEL_TEMPLATE = "Condition mean (n={n})"
CONDITION_MEAN_TITLE = "Mean Voltammogram"
SUMMARY_CURVE_NAME = "mean voltammograms"
REPRESENTATIVE_MODE = "mean_of_5_scans"

TOP_DIR_RE = re.compile(r"高さ=(?P<pressure>\d+)")
FILE_RE = re.compile(
    r"濃度=(?P<concentration>\d+)ppm__chip=(?P<chip>\d+)__rep=(?P<rep>\d+)\.txt$"
)


@dataclass(frozen=True)
class Curve:
    potential: np.ndarray
    current: np.ndarray


@dataclass
class Measurement:
    path: Path
    pressure: int
    concentration_ppm: int
    chip: int
    rep: int
    cycles: list[Curve]
    representative: Curve
    min_potential: float
    min_current: float

    @property
    def label(self) -> str:
        return f"pressure_{self.pressure}_conc_{self.concentration_ppm}_chip_{self.chip}_rep_{self.rep}"


@dataclass
class ConditionAggregate:
    pressure: int
    concentration_ppm: int
    measurements: list[Measurement]
    mean_curve: Curve
    aligned_currents: np.ndarray
    min_potential: float
    min_current: float
    min_current_std: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze CV histamine measurements.")
    parser.add_argument(
        "--scan-index",
        type=int,
        default=None,
        help="1-based scan index to use for analysis instead of averaging all scans in each file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to analysis_output or analysis_output_scanN.",
    )
    args = parser.parse_args()
    if args.scan_index is not None and args.scan_index < 1:
        parser.error("--scan-index must be 1 or greater.")
    return args


def configure_output_dirs(output_dir: Path) -> None:
    global OUTPUT_DIR, INDIVIDUAL_DIR, MEAN_DIR, CALIBRATION_DIR
    OUTPUT_DIR = output_dir
    INDIVIDUAL_DIR = OUTPUT_DIR / "individual_voltammograms"
    MEAN_DIR = OUTPUT_DIR / "mean_voltammograms"
    CALIBRATION_DIR = OUTPUT_DIR / "calibration"


def configure_analysis(scan_index: int | None, output_dir: Path | None) -> None:
    global SCAN_INDEX_USED
    global REPRESENTATIVE_LABEL
    global CONDITION_MEAN_LABEL_TEMPLATE
    global CONDITION_MEAN_TITLE
    global SUMMARY_CURVE_NAME
    global REPRESENTATIVE_MODE

    SCAN_INDEX_USED = scan_index
    if scan_index is None:
        REPRESENTATIVE_LABEL = "Mean of 5 scans"
        CONDITION_MEAN_LABEL_TEMPLATE = "Condition mean (n={n})"
        CONDITION_MEAN_TITLE = "Mean Voltammogram"
        SUMMARY_CURVE_NAME = "mean voltammograms"
        REPRESENTATIVE_MODE = "mean_of_5_scans"
        resolved_output_dir = output_dir or DEFAULT_OUTPUT_DIR
    else:
        REPRESENTATIVE_LABEL = f"Scan {scan_index} (used for analysis)"
        CONDITION_MEAN_LABEL_TEMPLATE = f"Condition mean of Scan {scan_index} (n={{n}})"
        CONDITION_MEAN_TITLE = f"Mean Voltammogram of Scan {scan_index}"
        SUMMARY_CURVE_NAME = f"scan {scan_index} condition means"
        REPRESENTATIVE_MODE = f"scan_{scan_index}"
        resolved_output_dir = output_dir or (ROOT / f"analysis_output_scan{scan_index}")

    configure_output_dirs(resolved_output_dir)


def sanitize_grid(values: np.ndarray) -> np.ndarray:
    return np.round(values.astype(float), 6)


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


def build_grid(start: float, stop: float, step: float) -> np.ndarray:
    if step <= 0:
        raise ValueError("step must be positive")
    count = int(round((stop - start) / step))
    grid = start + np.arange(count + 1) * step
    return sanitize_grid(grid)


def build_desc_grid(start: float, stop: float, step: float) -> np.ndarray:
    if step <= 0:
        raise ValueError("step must be positive")
    count = int(round((start - stop) / step))
    grid = start - np.arange(count + 1) * step
    return sanitize_grid(grid)


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


def infer_step(curves: Iterable[Curve]) -> float:
    steps: list[float] = []
    for curve in curves:
        diffs = np.diff(curve.potential)
        diffs = np.abs(diffs[np.isfinite(diffs)])
        diffs = diffs[diffs > 0]
        if diffs.size:
            steps.append(float(np.median(diffs)))
    if not steps:
        raise ValueError("Could not infer potential step.")
    return round(float(np.median(steps)), 6)


def interpolate_segment(segment: Curve, grid: np.ndarray) -> np.ndarray:
    x = segment.potential
    y = segment.current

    if x[0] > x[-1]:
        x = x[::-1]
        y = y[::-1]
        asc_grid = grid[::-1]
        interpolated = np.interp(asc_grid, x, y)
        return interpolated[::-1]

    return np.interp(grid, x, y)


def align_curves(curves: list[Curve]) -> tuple[np.ndarray, np.ndarray]:
    if not curves:
        raise ValueError("No curves provided for alignment.")

    split_segments = [split_curve(curve) for curve in curves]
    seg1_list = [parts[0] for parts in split_segments]
    seg2_list = [parts[1] for parts in split_segments]
    seg3_list = [parts[2] for parts in split_segments]

    step = infer_step(curves)
    start1 = max(float(segment.potential.min()) for segment in seg1_list)
    peak = min(float(segment.potential.max()) for segment in seg1_list)
    valley = max(float(segment.potential.min()) for segment in seg2_list)
    end3 = min(float(segment.potential.max()) for segment in seg3_list)

    grid1 = build_grid(start1, peak, step)
    grid2 = build_desc_grid(peak, valley, step)
    grid3 = build_grid(valley, end3, step)

    aligned: list[np.ndarray] = []
    for seg1, seg2, seg3 in split_segments:
        current1 = interpolate_segment(seg1, grid1)
        current2 = interpolate_segment(seg2, grid2)
        current3 = interpolate_segment(seg3, grid3)
        aligned.append(np.concatenate([current1, current2[1:], current3[1:]]))

    common_potential = np.concatenate([grid1, grid2[1:], grid3[1:]])
    return sanitize_grid(common_potential), np.vstack(aligned)


def read_measurement(path: Path, scan_index: int | None = None) -> Measurement:
    top_match = TOP_DIR_RE.search(path.parent.name)
    file_match = FILE_RE.search(path.name)
    if top_match is None or file_match is None:
        raise ValueError(f"Could not parse metadata from {path}")

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

    if scan_index is None:
        rep_potential, rep_currents = align_curves(cycles)
        representative = Curve(rep_potential, rep_currents.mean(axis=0))
    else:
        if scan_index > len(cycles):
            raise ValueError(f"{path.name} has only {len(cycles)} scans; scan {scan_index} is unavailable.")
        selected = cycles[scan_index - 1]
        representative = Curve(selected.potential.copy(), selected.current.copy())

    min_index = int(np.argmin(representative.current))

    return Measurement(
        path=path,
        pressure=int(top_match.group("pressure")),
        concentration_ppm=int(file_match.group("concentration")),
        chip=int(file_match.group("chip")),
        rep=int(file_match.group("rep")),
        cycles=cycles,
        representative=representative,
        min_potential=float(representative.potential[min_index]),
        min_current=float(representative.current[min_index]),
    )


def discover_measurements(root: Path, scan_index: int | None = None) -> list[Measurement]:
    measurements = []
    for path in sorted(root.rglob("*.txt")):
        measurements.append(read_measurement(path, scan_index=scan_index))
    return measurements


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_folder_overview(measurements: list[Measurement]) -> None:
    rows = []
    for measurement in measurements:
        cycle_lengths = ",".join(str(len(curve.potential)) for curve in measurement.cycles)
        rows.append(
            {
                "representative_mode": REPRESENTATIVE_MODE,
                "pressure": measurement.pressure,
                "concentration_ppm": measurement.concentration_ppm,
                "chip": measurement.chip,
                "rep": measurement.rep,
                "file_name": measurement.path.name,
                "relative_path": measurement.path.relative_to(ROOT).as_posix(),
                "cycle_count": len(measurement.cycles),
                "cycle_lengths": cycle_lengths,
                "representative_points": len(measurement.representative.potential),
            }
        )

    df = pd.DataFrame(rows).sort_values(["pressure", "concentration_ppm", "chip", "rep"])
    df.to_csv(OUTPUT_DIR / "file_inventory.csv", index=False, encoding="utf-8-sig")


def plot_individual_measurement(measurement: Measurement) -> None:
    condition_dir = INDIVIDUAL_DIR / f"pressure_{measurement.pressure}" / f"conc_{measurement.concentration_ppm}"
    ensure_dir(condition_dir)

    fig, ax = plt.subplots(figsize=(8, 5))
    cmap = plt.get_cmap("viridis")
    if SCAN_INDEX_USED is None:
        for index, curve in enumerate(measurement.cycles, start=1):
            ax.plot(
                curve.potential,
                curve.current,
                linewidth=1.2,
                color=cmap((index - 1) / max(len(measurement.cycles) - 1, 1)),
                label=f"Scan {index}",
            )

        ax.plot(
            measurement.representative.potential,
            measurement.representative.current,
            color="black",
            linewidth=2.0,
            label=REPRESENTATIVE_LABEL,
        )
    else:
        for index, curve in enumerate(measurement.cycles, start=1):
            is_selected = index == SCAN_INDEX_USED
            ax.plot(
                curve.potential,
                curve.current,
                linewidth=2.2 if is_selected else 1.0,
                color="black" if is_selected else cmap((index - 1) / max(len(measurement.cycles) - 1, 1)),
                alpha=1.0 if is_selected else 0.35,
                label=REPRESENTATIVE_LABEL if is_selected else f"Scan {index}",
            )
    ax.scatter(
        [measurement.min_potential],
        [measurement.min_current],
        color="red",
        s=28,
        zorder=5,
        label="Minimum current",
    )

    ax.set_title(
        f"Pressure {measurement.pressure} | {measurement.concentration_ppm} ppm | chip {measurement.chip} rep {measurement.rep}"
    )
    ax.set_xlabel("Potential (V)")
    ax.set_ylabel("Current (uA)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(condition_dir / f"{measurement.label}.png", dpi=200)
    plt.close(fig)


def aggregate_conditions(measurements: list[Measurement]) -> list[ConditionAggregate]:
    grouped: dict[tuple[int, int], list[Measurement]] = {}
    for measurement in measurements:
        key = (measurement.pressure, measurement.concentration_ppm)
        grouped.setdefault(key, []).append(measurement)

    aggregates: list[ConditionAggregate] = []
    for (pressure, concentration), items in sorted(grouped.items()):
        aligned_potential, aligned_currents = align_curves([m.representative for m in items])
        mean_current = aligned_currents.mean(axis=0)
        min_index = int(np.argmin(mean_current))
        file_mins = np.array([m.min_current for m in items], dtype=float)
        aggregates.append(
            ConditionAggregate(
                pressure=pressure,
                concentration_ppm=concentration,
                measurements=items,
                mean_curve=Curve(aligned_potential, mean_current),
                aligned_currents=aligned_currents,
                min_potential=float(aligned_potential[min_index]),
                min_current=float(mean_current[min_index]),
                min_current_std=float(file_mins.std(ddof=1)) if len(file_mins) > 1 else 0.0,
            )
        )
    return aggregates


def plot_condition_mean(aggregate: ConditionAggregate) -> None:
    ensure_dir(MEAN_DIR)

    fig, ax = plt.subplots(figsize=(8, 5))
    for current in aggregate.aligned_currents:
        ax.plot(
            aggregate.mean_curve.potential,
            current,
            color="#B7C4CF",
            linewidth=1.0,
            alpha=0.9,
        )

    color_map = {0: "#4C78A8", 50: "#F58518", 100: "#E45756"}
    ax.plot(
        aggregate.mean_curve.potential,
        aggregate.mean_curve.current,
        color=color_map.get(aggregate.concentration_ppm, "#2E4057"),
        linewidth=2.4,
        label=CONDITION_MEAN_LABEL_TEMPLATE.format(n=len(aggregate.measurements)),
    )
    ax.scatter(
        [aggregate.min_potential],
        [aggregate.min_current],
        color="red",
        s=30,
        zorder=5,
        label="Minimum current",
    )

    ax.set_title(
        f"{CONDITION_MEAN_TITLE} | Pressure {aggregate.pressure} | {aggregate.concentration_ppm} ppm"
    )
    ax.set_xlabel("Potential (V)")
    ax.set_ylabel("Current (uA)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(
        MEAN_DIR / f"mean_voltammogram_pressure_{aggregate.pressure}_conc_{aggregate.concentration_ppm}.png",
        dpi=200,
    )
    plt.close(fig)


def plot_pressure_mean_overlays(aggregates: list[ConditionAggregate]) -> None:
    ensure_dir(MEAN_DIR)
    color_map = {0: "#4C78A8", 50: "#F58518", 100: "#E45756"}

    grouped: dict[int, list[ConditionAggregate]] = {}
    for aggregate in aggregates:
        grouped.setdefault(aggregate.pressure, []).append(aggregate)

    for pressure, items in sorted(grouped.items()):
        fig, ax = plt.subplots(figsize=(8, 5))
        for aggregate in sorted(items, key=lambda item: item.concentration_ppm):
            color = color_map.get(aggregate.concentration_ppm, "#2E4057")
            ax.plot(
                aggregate.mean_curve.potential,
                aggregate.mean_curve.current,
                color=color,
                linewidth=2.4,
                label=f"{aggregate.concentration_ppm} ppm",
            )
            ax.scatter(
                [aggregate.min_potential],
                [aggregate.min_current],
                color=color,
                s=28,
                zorder=5,
            )

        ax.set_title(f"{CONDITION_MEAN_TITLE} | Pressure {pressure} | All concentrations")
        ax.set_xlabel("Potential (V)")
        ax.set_ylabel("Current (uA)")
        apply_sigfig_format(ax)
        ax.legend(fontsize=9)
        fig.tight_layout()
        fig.savefig(MEAN_DIR / f"mean_voltammogram_pressure_{pressure}_all_concentrations.png", dpi=200)
        plt.close(fig)


def save_min_current_tables(aggregates: list[ConditionAggregate]) -> None:
    file_rows = []
    for aggregate in aggregates:
        for measurement in aggregate.measurements:
            file_rows.append(
                {
                    "pressure": measurement.pressure,
                    "concentration_ppm": measurement.concentration_ppm,
                    "chip": measurement.chip,
                    "rep": measurement.rep,
                    "file_name": measurement.path.name,
                    "min_potential_V": measurement.min_potential,
                    "min_current_uA": measurement.min_current,
                }
            )

    condition_rows = []
    for aggregate in aggregates:
        mins = [m.min_current for m in aggregate.measurements]
        condition_rows.append(
            {
                "pressure": aggregate.pressure,
                "concentration_ppm": aggregate.concentration_ppm,
                "n_files": len(aggregate.measurements),
                "mean_of_file_min_current_uA": float(np.mean(mins)),
                "std_of_file_min_current_uA": float(np.std(mins, ddof=1)) if len(mins) > 1 else 0.0,
                "mean_voltammogram_min_potential_V": aggregate.min_potential,
                "mean_voltammogram_min_current_uA": aggregate.min_current,
            }
        )

    pd.DataFrame(file_rows).sort_values(
        ["pressure", "concentration_ppm", "chip", "rep"]
    ).to_csv(OUTPUT_DIR / "file_min_currents.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(condition_rows).sort_values(
        ["pressure", "concentration_ppm"]
    ).to_csv(OUTPUT_DIR / "condition_min_currents.csv", index=False, encoding="utf-8-sig")


def fit_line(x: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    slope, intercept = np.polyfit(x, y, 1)
    fit = slope * x + intercept
    denominator = float(np.sum((y - y.mean()) ** 2))
    if math.isclose(denominator, 0.0):
        r_squared = float("nan")
    else:
        r_squared = 1.0 - float(np.sum((y - fit) ** 2) / denominator)
    return float(slope), float(intercept), float(r_squared)


def format_fit_annotation(slope: float, intercept: float, r_squared: float) -> str:
    intercept_sign = "+" if intercept >= 0 else "-"
    return "\n".join(
        [
            rf"$I_{{\min}} = {format_sigfig(slope)}\,C {intercept_sign}\,{format_sigfig(abs(intercept))}$",
            rf"$R^2 = {format_sigfig(r_squared)}$",
        ]
    )


def draw_calibration_axis(
    ax: plt.Axes, pressure: int, items: list[ConditionAggregate], set_limits: bool = True
) -> dict[str, float]:
    color_map = {0: "#4C78A8", 50: "#F58518", 100: "#E45756"}
    concentrations = np.array([item.concentration_ppm for item in items], dtype=float)
    mean_mins = np.array([np.mean([m.min_current for m in item.measurements]) for item in items], dtype=float)
    std_mins = np.array(
        [
            np.std([m.min_current for m in item.measurements], ddof=1) if len(item.measurements) > 1 else 0.0
            for item in items
        ],
        dtype=float,
    )
    replicate_mins: list[float] = []

    for item in items:
        mins = np.array([m.min_current for m in item.measurements], dtype=float)
        replicate_mins.extend(mins.tolist())
        if len(mins) == 1:
            x_positions = np.array([item.concentration_ppm], dtype=float)
        else:
            x_positions = np.linspace(item.concentration_ppm - 2.0, item.concentration_ppm + 2.0, len(mins))
        ax.scatter(
            x_positions,
            mins,
            color=color_map.get(item.concentration_ppm, "#2E4057"),
            alpha=0.5,
            s=28,
        )

    ax.errorbar(
        concentrations,
        mean_mins,
        yerr=std_mins,
        fmt="o",
        color="black",
        capsize=4,
        linewidth=1.4,
        label="Mean ± SD",
    )

    slope, intercept, r_squared = fit_line(concentrations, mean_mins)
    fit_x = np.linspace(concentrations.min(), concentrations.max(), 200)
    fit_y = slope * fit_x + intercept
    ax.plot(fit_x, fit_y, color="#2E4057", linewidth=1.8, label="Linear fit")

    upper_points = mean_mins + std_mins
    lower_points = mean_mins - std_mins
    all_y_values = np.concatenate(
        [
            np.array(replicate_mins, dtype=float),
            mean_mins,
            upper_points,
            lower_points,
            fit_y,
        ]
    )
    y_span = float(all_y_values.max() - all_y_values.min())
    if math.isclose(y_span, 0.0):
        y_span = 1.0
    label_offset = max(y_span * 0.035, 0.08)

    label_positions = upper_points + label_offset
    for x, y, sd in zip(concentrations, label_positions, std_mins):
        ax.text(
            x,
            y,
            f"SD={format_sigfig(sd)}",
            ha="center",
            va="bottom",
            fontsize=8,
            color="#333333",
            clip_on=True,
        )

    y_margin = max(y_span * 0.08, 0.15)
    y_lower = float(all_y_values.min() - y_margin)
    y_upper = float(label_positions.max() + y_margin)
    if set_limits:
        ax.set_ylim(y_lower, y_upper)

    ax.set_title(f"Pressure {pressure}")
    ax.set_xlabel("Concentration (ppm)")
    ax.set_ylabel("Minimum current (uA)")
    apply_sigfig_format(ax)
    ax.legend(fontsize=8)
    ax.text(
        0.03,
        0.05,
        format_fit_annotation(slope, intercept, r_squared),
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

    return {
        "pressure": pressure,
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "y_lower": y_lower,
        "y_upper": y_upper,
    }


def plot_calibration(aggregates: list[ConditionAggregate]) -> None:
    ensure_dir(CALIBRATION_DIR)

    by_pressure: dict[int, list[ConditionAggregate]] = {}
    for aggregate in aggregates:
        by_pressure.setdefault(aggregate.pressure, []).append(aggregate)

    fit_rows = []
    for pressure, items in sorted(by_pressure.items()):
        items = sorted(items, key=lambda item: item.concentration_ppm)
        fig, ax = plt.subplots(figsize=(6, 4.5))
        fit_rows.append(draw_calibration_axis(ax, pressure, items))
        fig.tight_layout()
        fig.savefig(CALIBRATION_DIR / f"calibration_pressure_{pressure}.png", dpi=200)
        plt.close(fig)

    pressures = sorted(by_pressure)
    fig, axes = plt.subplots(1, len(pressures), figsize=(6 * len(pressures), 4.8), sharey=True)
    if len(pressures) == 1:
        axes = [axes]
    combined_limits = []
    for ax, pressure in zip(axes, pressures):
        items = sorted(by_pressure[pressure], key=lambda item: item.concentration_ppm)
        combined_limits.append(draw_calibration_axis(ax, pressure, items, set_limits=False))

    global_y_lower = min(item["y_lower"] for item in combined_limits)
    global_y_upper = max(item["y_upper"] for item in combined_limits)
    for ax in axes:
        ax.set_ylim(global_y_lower, global_y_upper)

    fig.suptitle("Calibration Curves by Pressure", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(CALIBRATION_DIR / "calibration_by_pressure.png", dpi=200)
    plt.close(fig)

    pd.DataFrame(fit_rows).drop(columns=["y_lower", "y_upper"]).sort_values("pressure").to_csv(
        OUTPUT_DIR / "calibration_fits.csv", index=False, encoding="utf-8-sig"
    )


def print_summary(measurements: list[Measurement], aggregates: list[ConditionAggregate]) -> None:
    print("Measurements loaded:", len(measurements))
    for pressure in sorted({m.pressure for m in measurements}):
        subset = [m for m in measurements if m.pressure == pressure]
        counts = {}
        for measurement in subset:
            counts.setdefault(measurement.concentration_ppm, 0)
            counts[measurement.concentration_ppm] += 1
        count_text = ", ".join(f"{conc}ppm={counts[conc]}" for conc in sorted(counts))
        print(f"Pressure {pressure}: {count_text}")

    print(f"Condition minima from {SUMMARY_CURVE_NAME}:")
    for aggregate in sorted(aggregates, key=lambda item: (item.pressure, item.concentration_ppm)):
        print(
            f"  Pressure {aggregate.pressure}, {aggregate.concentration_ppm} ppm -> "
            f"{aggregate.min_current:.4f} uA at {aggregate.min_potential:.3f} V"
        )


def main() -> None:
    args = parse_args()
    configure_analysis(args.scan_index, args.output_dir)
    ensure_dir(OUTPUT_DIR)
    ensure_dir(INDIVIDUAL_DIR)
    ensure_dir(MEAN_DIR)
    ensure_dir(CALIBRATION_DIR)

    measurements = discover_measurements(ROOT, scan_index=args.scan_index)
    if not measurements:
        raise SystemExit("No .txt CV files were found.")

    save_folder_overview(measurements)
    for measurement in measurements:
        plot_individual_measurement(measurement)

    aggregates = aggregate_conditions(measurements)
    for aggregate in aggregates:
        plot_condition_mean(aggregate)
    plot_pressure_mean_overlays(aggregates)

    save_min_current_tables(aggregates)
    plot_calibration(aggregates)
    print_summary(measurements, aggregates)


if __name__ == "__main__":
    main()
