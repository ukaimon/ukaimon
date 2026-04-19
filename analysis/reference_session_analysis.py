from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd

from analysis import absolute_loop_area_analysis
from analysis.session_plot_analysis import extract_cycle_curves
from parsers.measurement_file_parser import parse_measurement_file


REFERENCE_SCRIPT_DIR = Path(__file__).resolve().parent.parent / "解析"


def _load_script_module(module_name: str, script_name: str) -> ModuleType:
    script_path = REFERENCE_SCRIPT_DIR / script_name
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"参照スクリプトを読み込めません: {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _clear_tree_contents(root: Path) -> None:
    if not root.exists():
        return
    for path in sorted(root.rglob("*"), reverse=True):
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        except OSError:
            continue


def _coating_height_to_pressure(value: Any) -> int:
    numeric = float(value)
    if abs(numeric) < 20:
        return int(round(numeric * 10))
    return int(round(numeric))


def _parse_numeric_suffix(text: str) -> int | None:
    match = re.search(r"(\d+)", str(text or ""))
    if match:
        return int(match.group(1))
    return None


def _derive_pressure(measurement_row: dict[str, Any], usage_row: dict[str, Any] | None) -> int:
    raw_file_path = str(measurement_row.get("raw_file_path") or "")
    path_match = re.search(r"高さ=(\d+)", raw_file_path)
    if path_match:
        return int(path_match.group(1))
    if usage_row and usage_row.get("coating_height") not in (None, ""):
        return _coating_height_to_pressure(usage_row["coating_height"])
    return 0


def _derive_chip_and_rep(measurement_row: dict[str, Any]) -> tuple[int, int]:
    rep_no = int(measurement_row.get("rep_no") or 0) or 1
    chip_candidate = _parse_numeric_suffix(measurement_row.get("chip_id")) or rep_no
    return chip_candidate, rep_no


def _concentration_ppm(condition_row: dict[str, Any]) -> int:
    return int(round(float(condition_row.get("concentration_value") or 0.0)))


def _write_reference_txt(curves: list[pd.DataFrame], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not curves:
        raise ValueError(f"波形がありません: {destination}")

    def _normalize_reference_curve(curve: pd.DataFrame) -> pd.DataFrame:
        normalized = curve[["potential_v", "current_a"]].dropna().reset_index(drop=True)
        if normalized.empty:
            return normalized
        potential = normalized["potential_v"].to_numpy(dtype=float)
        max_index = int(potential.argmax())
        min_index = int(potential.argmin())
        if min_index <= max_index and len(normalized) > 1:
            start_index = min_index + 1 if min_index + 1 < len(normalized) else 0
            normalized = pd.concat(
                [normalized.iloc[start_index:], normalized.iloc[:start_index]],
                ignore_index=True,
            )
        return normalized

    columns: dict[str, pd.Series] = {}
    for index, curve in enumerate(curves, start=1):
        normalized = _normalize_reference_curve(curve)
        columns[f"potential_scan{index}"] = normalized["potential_v"]
        columns[f"current_scan{index}"] = normalized["current_a"] * 1_000_000.0
    frame = pd.DataFrame(columns)
    frame.to_csv(destination, sep="\t", index=False, encoding="utf-8")


def build_reference_source_tree(
    source_root: Path,
    condition_rows: list[dict[str, Any]],
    measurement_map: dict[str, list[dict[str, Any]]],
    usage_row: dict[str, Any] | None,
) -> dict[str, Any]:
    _clear_tree_contents(source_root)
    source_root.mkdir(parents=True, exist_ok=True)

    manifest_rows: list[dict[str, Any]] = []
    for condition_row in condition_rows:
        condition_id = str(condition_row["condition_id"])
        concentration_ppm = _concentration_ppm(condition_row)
        for measurement_row in measurement_map.get(condition_id, []):
            raw_file_path = str(measurement_row.get("raw_file_path") or "").strip()
            if not raw_file_path:
                continue
            parsed = parse_measurement_file(raw_file_path)
            cycle_curves = extract_cycle_curves(parsed)
            if not cycle_curves:
                continue

            pressure = _derive_pressure(measurement_row, usage_row)
            chip, rep = _derive_chip_and_rep(measurement_row)
            destination = (
                source_root
                / f"高さ={pressure}"
                / f"濃度={concentration_ppm}ppm__chip={chip}__rep={rep}.txt"
            )
            _write_reference_txt(cycle_curves, destination)
            manifest_rows.append(
                {
                    "condition_id": condition_id,
                    "measurement_id": str(measurement_row["measurement_id"]),
                    "pressure": pressure,
                    "concentration_ppm": concentration_ppm,
                    "chip": chip,
                    "rep": rep,
                    "source_file": raw_file_path,
                    "generated_txt": str(destination),
                    "cycle_count": len(cycle_curves),
                }
            )

    manifest_path = source_root / "analysis_source_manifest.csv"
    pd.DataFrame(manifest_rows).to_csv(manifest_path, index=False, encoding="utf-8-sig")
    return {
        "source_root": str(source_root.resolve()),
        "manifest_csv": str(manifest_path.resolve()),
        "n_files": len(manifest_rows),
    }


def _run_histamine_outputs(source_root: Path, analysis_root: Path) -> dict[str, str]:
    histamine_module = _load_script_module("reference_histamine_analysis", "analyze_cv_histamine.py")
    histamine_module.ROOT = source_root

    output_paths: dict[str, str] = {}
    for scan_index in (None, 1, 2, 3, 4, 5):
        output_dir = analysis_root / ("analysis_output" if scan_index is None else f"analysis_output_scan{scan_index}")
        histamine_module.configure_analysis(scan_index, output_dir)
        histamine_module.ROOT = source_root
        histamine_module.ensure_dir(histamine_module.OUTPUT_DIR)
        histamine_module.ensure_dir(histamine_module.INDIVIDUAL_DIR)
        histamine_module.ensure_dir(histamine_module.MEAN_DIR)
        histamine_module.ensure_dir(histamine_module.CALIBRATION_DIR)

        try:
            measurements = histamine_module.discover_measurements(source_root, scan_index=scan_index)
        except ValueError as error:
            if scan_index is not None and "scan" in str(error).lower() and "unavailable" in str(error).lower():
                continue
            raise
        if not measurements:
            continue
        histamine_module.save_folder_overview(measurements)
        for measurement in measurements:
            histamine_module.plot_individual_measurement(measurement)
        aggregates = histamine_module.aggregate_conditions(measurements)
        for aggregate in aggregates:
            histamine_module.plot_condition_mean(aggregate)
        histamine_module.plot_pressure_mean_overlays(aggregates)
        histamine_module.save_min_current_tables(aggregates)
        histamine_module.plot_calibration(aggregates)

        if scan_index is None:
            output_paths["file_inventory_csv"] = str((output_dir / "file_min_currents.csv").resolve())
            output_paths["file_inventory_detail_csv"] = str((output_dir / "file_inventory.csv").resolve())
            output_paths["condition_summary_csv"] = str((output_dir / "condition_min_currents.csv").resolve())
            output_paths["calibration_fits_csv"] = str((output_dir / "calibration_fits.csv").resolve())
            output_paths["individual_plot_dir"] = str((output_dir / "individual_voltammograms").resolve())
            output_paths["mean_plot_dir"] = str((output_dir / "mean_voltammograms").resolve())
            output_paths["calibration_plot"] = str((output_dir / "calibration" / "calibration_by_pressure.png").resolve())

            mean_overlay_candidates = sorted((output_dir / "mean_voltammograms").glob("mean_voltammogram_pressure_*_all_concentrations.png"))
            if mean_overlay_candidates:
                output_paths["overlay_plot"] = str(mean_overlay_candidates[0].resolve())
    return output_paths


def _run_absolute_integral_outputs(source_root: Path, analysis_root: Path) -> dict[str, str]:
    module = _load_script_module("reference_absolute_integral_analysis", "analyze_cv_absolute_integral.py")
    module.ROOT = source_root
    module.OUTPUT_DIR = analysis_root / "analysis_output_absolute_integral"
    module.CALIBRATION_DIR = module.OUTPUT_DIR / "calibration"
    module.BY_SCAN_DIR = module.CALIBRATION_DIR / "by_scan"
    module.BY_PRESSURE_DIR = module.CALIBRATION_DIR / "by_pressure"
    module.ensure_dir(module.OUTPUT_DIR)
    module.ensure_dir(module.CALIBRATION_DIR)
    module.ensure_dir(module.BY_SCAN_DIR)
    module.ensure_dir(module.BY_PRESSURE_DIR)

    wide_df, long_df = module.load_integral_tables()
    condition_df = module.aggregate_conditions(long_df)
    fits_df = module.save_fit_table(condition_df)
    mean_condition_df = module.aggregate_scan_mean(wide_df)
    mean_fits_df = module.save_scan_mean_fit_table(mean_condition_df)
    module.plot_by_scan(condition_df, fits_df)
    module.plot_by_pressure(condition_df, fits_df, mean_condition_df, mean_fits_df)

    return {
        "absolute_integral_file_csv": str((module.OUTPUT_DIR / "file_absolute_integrals.csv").resolve()),
        "absolute_integral_long_csv": str((module.OUTPUT_DIR / "file_absolute_integrals_long.csv").resolve()),
        "absolute_integral_condition_csv": str((module.OUTPUT_DIR / "condition_absolute_integrals.csv").resolve()),
        "absolute_integral_fit_csv": str((module.OUTPUT_DIR / "absolute_integral_calibration_fits.csv").resolve()),
        "absolute_integral_plot": str((module.BY_PRESSURE_DIR / "absolute_integral_calibration_overlay_scan1_to_5_by_pressure.png").resolve()),
    }


def _run_absolute_loop_area_outputs(source_root: Path, analysis_root: Path) -> dict[str, str]:
    absolute_loop_area_analysis.ROOT = source_root
    absolute_loop_area_analysis.OUTPUT_DIR = analysis_root / "analysis_output_absolute_loop_area"
    absolute_loop_area_analysis.CALIBRATION_DIR = absolute_loop_area_analysis.OUTPUT_DIR / "calibration"
    absolute_loop_area_analysis.BY_SCAN_DIR = absolute_loop_area_analysis.CALIBRATION_DIR / "by_scan"
    absolute_loop_area_analysis.BY_PRESSURE_DIR = absolute_loop_area_analysis.CALIBRATION_DIR / "by_pressure"
    absolute_loop_area_analysis.ensure_dir(absolute_loop_area_analysis.OUTPUT_DIR)
    absolute_loop_area_analysis.ensure_dir(absolute_loop_area_analysis.CALIBRATION_DIR)
    absolute_loop_area_analysis.ensure_dir(absolute_loop_area_analysis.BY_SCAN_DIR)
    absolute_loop_area_analysis.ensure_dir(absolute_loop_area_analysis.BY_PRESSURE_DIR)

    wide_df, long_df, scans = absolute_loop_area_analysis.load_loop_area_tables()
    condition_df = absolute_loop_area_analysis.aggregate_conditions(long_df)
    fits_df = absolute_loop_area_analysis.save_fit_table(condition_df)
    mean_condition_df = absolute_loop_area_analysis.aggregate_scan_mean(wide_df)
    mean_fits_df = absolute_loop_area_analysis.save_scan_mean_fit_table(mean_condition_df)
    absolute_loop_area_analysis.plot_by_scan(condition_df, fits_df, scans)
    absolute_loop_area_analysis.plot_by_pressure(condition_df, fits_df, mean_condition_df, mean_fits_df, scans)

    return {
        "absolute_loop_area_file_csv": str((absolute_loop_area_analysis.OUTPUT_DIR / "file_absolute_loop_areas.csv").resolve()),
        "absolute_loop_area_long_csv": str((absolute_loop_area_analysis.OUTPUT_DIR / "file_absolute_loop_areas_long.csv").resolve()),
        "absolute_loop_area_condition_csv": str((absolute_loop_area_analysis.OUTPUT_DIR / "condition_absolute_loop_areas.csv").resolve()),
        "absolute_loop_area_fit_csv": str((absolute_loop_area_analysis.OUTPUT_DIR / "absolute_loop_area_calibration_fits.csv").resolve()),
        "absolute_loop_area_plot": str((absolute_loop_area_analysis.BY_PRESSURE_DIR / "absolute_loop_area_calibration_overlay_scan1_to_5_by_pressure.png").resolve()),
    }


def _run_cycle1_reference_outputs(source_root: Path, analysis_root: Path) -> dict[str, str]:
    module = _load_script_module("reference_cycle1_reference_analysis", "analyze_cycle1_reference_calibration.py")
    module.ROOT = source_root
    module.OUTPUT_DIR = analysis_root / "analysis_output_cycle1_reference_potential"
    module.CALIBRATION_DIR = module.OUTPUT_DIR / "calibration"
    module.ensure_dir(module.OUTPUT_DIR)
    module.ensure_dir(module.CALIBRATION_DIR)

    measurements = module.load_measurements()
    long_df = module.save_file_tables(measurements)
    condition_df = module.aggregate_conditions(long_df)
    fits_df = module.save_fit_table(condition_df)
    module.plot_calibrations(condition_df, fits_df)

    return {
        "cycle1_reference_file_csv": str((module.OUTPUT_DIR / "file_cycle1_reference_currents.csv").resolve()),
        "cycle1_reference_long_csv": str((module.OUTPUT_DIR / "file_cycle1_reference_currents_long.csv").resolve()),
        "cycle1_reference_condition_csv": str((module.OUTPUT_DIR / "condition_cycle1_reference_currents.csv").resolve()),
        "cycle1_reference_fit_csv": str((module.OUTPUT_DIR / "cycle1_reference_calibration_fits.csv").resolve()),
        "cycle1_reference_plot": str((module.CALIBRATION_DIR / "calibration_overlay_cycle2_to_5_at_cycle1_ref_by_pressure.png").resolve()),
    }


def _run_scan_comparison_outputs(analysis_root: Path) -> dict[str, str]:
    module = _load_script_module("reference_scan_comparison_analysis", "plot_scan_calibration_overlays.py")
    module.ROOT = analysis_root
    module.OUTPUT_DIR = analysis_root / "analysis_output_scan_comparison"
    module.CALIBRATION_DIR = module.OUTPUT_DIR / "calibration"
    module.ensure_dir(module.OUTPUT_DIR)
    module.ensure_dir(module.CALIBRATION_DIR)

    conditions, fits = module.load_scan_outputs()
    module.save_summary_csv(fits)
    module.plot_overlays(conditions, fits)
    return {
        "scan_comparison_fit_csv": str((module.OUTPUT_DIR / "scan1_to_5_calibration_fits.csv").resolve()),
        "scan_comparison_plot": str((module.CALIBRATION_DIR / "calibration_overlay_scan1_to_5_by_pressure.png").resolve()),
    }


def run_reference_session_analysis(
    source_root: Path,
    analysis_root: Path,
    *,
    clear_existing: bool = True,
) -> dict[str, str]:
    if clear_existing:
        for directory in analysis_root.glob("analysis_output*"):
            if directory.is_dir():
                _clear_tree_contents(directory)
                try:
                    directory.rmdir()
                except OSError:
                    continue

    outputs: dict[str, str] = {
        "analysis_root": str(analysis_root.resolve()),
    }
    outputs.update(_run_histamine_outputs(source_root, analysis_root))
    outputs.update(_run_absolute_integral_outputs(source_root, analysis_root))
    outputs.update(_run_absolute_loop_area_outputs(source_root, analysis_root))
    outputs.update(_run_cycle1_reference_outputs(source_root, analysis_root))
    outputs.update(_run_scan_comparison_outputs(analysis_root))
    return outputs
