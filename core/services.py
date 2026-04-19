from __future__ import annotations

import logging
import re
import subprocess
import threading
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from analysis.aggregation import aggregate_condition_rows
from analysis.cv_analysis import analyze_cv_curve
from analysis.dpv_analysis import analyze_dpv_curve
from analysis.mean_voltammogram import generate_mean_curve
from analysis.plotting import save_mean_curve_plot, save_measurement_plot
from analysis.reference_session_analysis import build_reference_source_tree, run_reference_session_analysis
from analysis.session_plot_analysis import (
    build_cv_mean_curve,
    compute_absolute_integral,
    compute_curve_minimum,
    compute_cycle1_reference_rows,
    extract_cycle_curves,
    fit_linear_calibration,
    save_calibration_plot,
    save_condition_mean_plot,
    save_individual_cycles_plot,
    save_metric_overlay_plot,
    save_overlay_curves_plot,
    select_representative_curve,
)
from core.batch_planner import generate_batch_plan_items
from core.config import AppConfig
from core.ivium_control import (
    IviumRemoteDriver,
    prepare_ivium_method_file,
    resolve_app_method_name,
    resolve_ivium_db_file_path,
    resolve_ivium_driver_path,
    resolve_ivium_method_name,
    resolve_ivium_method_template_path,
)
from core.linking import BatchLinker, IdsWatchCoordinator
from core.mip_usage_fields import with_mip_usage_field_defaults
from core.models import IviumRunState, PlannedStatus, QualityFlag
from core.quality import derive_auto_quality, resolve_final_quality
from core.repositories import ElectrochemRepository
from core.validators import require_any, require_fields
from export.csv_exporter import CSVExporter
from export.excel_exporter import ExcelExporter
from export.markdown_reporter import MarkdownReporter
from parsers.ivium_method_parser import parse_ivium_method_file
from parsers.measurement_file_parser import parse_measurement_file
from parsers.measurement_conditions_parser import build_measurement_conditions
from utils.date_utils import now_iso, today_string
from utils.file_utils import ensure_directories, generate_condition_id, generate_id, session_output_directories


LOGGER = logging.getLogger(__name__)

RESTORABLE_RECORD_TYPES = {
    "mip": "mip_records",
    "session": "sessions",
    "condition": "conditions",
    "measurement": "measurements",
}

CONDITION_WARNING_FIELDS = {
    "method": "method",
    "potential_start_v": "start",
    "potential_end_v": "end",
    "potential_vertex_1_v": "v1",
    "potential_vertex_2_v": "v2",
    "scan_rate_v_s": "scan_rate",
    "step_v": "step",
    "pulse_amplitude_v": "amplitude",
    "pulse_time_s": "pulse_time",
    "quiet_time_s": "quiet_time",
    "cycles": "cycles",
    "current_range": "range",
    "filter_setting": "filter",
}


class AppServices:
    def __init__(
        self,
        root_path: Path,
        config: AppConfig,
        repository: ElectrochemRepository,
    ) -> None:
        self.root_path = root_path
        self.config = config
        self.repository = repository
        self.batch_linker = BatchLinker(repository)
        self.csv_exporter = CSVExporter()
        self.excel_exporter = ExcelExporter()
        self.markdown_reporter = MarkdownReporter()
        self.watcher: IdsWatchCoordinator | None = None
        self.watcher_callback: Callable[[str], None] | None = None
        self._ivium_run_lock = threading.Lock()
        self._ivium_stop_event = threading.Event()
        self._ivium_run_thread: threading.Thread | None = None
        self._ivium_run_state = IviumRunState(message="待機中")
        self._ivium_driver: IviumRemoteDriver | None = None
        self._resume_watcher_after_run = False

    def initialize(self) -> None:
        ensure_directories(
            [
                self.root_path / "logs",
                self.root_path / "database",
                self.root_path / "data",
                self.root_path / "data" / "sessions",
                self.root_path / "data" / "cross_reports",
                self.root_path / "config",
            ]
        )
        self.repository.initialize()
        self.repository.normalize_legacy_condition_ids()
        self.repository.requeue_running_batch_items()

    @staticmethod
    def _normalize_warning_value(value: Any) -> str:
        if value in (None, ""):
            return "(blank)"
        if isinstance(value, float):
            return f"{value:.9g}"
        return str(value).strip()

    @staticmethod
    def _format_summary_number(value: Any, unit: str = "") -> str | None:
        if value in (None, ""):
            return None
        if isinstance(value, float):
            return f"{value:g}{unit}"
        return f"{str(value).strip()}{unit}"

    def _build_ids_header_summary(self, file_path: str | Path) -> str:
        path = Path(file_path)
        if not path.exists():
            return f"{path.name}\nファイルが見つかりません。"
        try:
            parsed = parse_measurement_file(path)
        except Exception as error:
            return f"{path.name}\nヘッダ要約を読めません: {error}"

        condition_payload = build_measurement_conditions(parsed.metadata, parsed.raw_header_text)
        lines = [path.name]

        method = str(condition_payload.get("method") or parsed.metadata.get("Method") or "Unknown").strip()
        started_at = str(parsed.metadata.get("starttime_iso") or parsed.metadata.get("starttime") or "").strip()
        lines.append(f"{method} / {started_at or '-'}")

        range_parts: list[str] = []
        start_v = self._format_summary_number(condition_payload.get("potential_start_v"), "V")
        end_v = self._format_summary_number(condition_payload.get("potential_end_v"), "V")
        vertex_1_v = self._format_summary_number(condition_payload.get("potential_vertex_1_v"), "V")
        vertex_2_v = self._format_summary_number(condition_payload.get("potential_vertex_2_v"), "V")
        if start_v and end_v:
            range_parts.append(f"{start_v}->{end_v}")
        elif start_v or vertex_1_v or vertex_2_v:
            range_preview = " / ".join(part for part in (start_v, vertex_1_v, vertex_2_v) if part)
            range_parts.append(range_preview)

        scan_rate = self._format_summary_number(condition_payload.get("scan_rate_v_s"), "V/s")
        if scan_rate:
            range_parts.append(f"scan {scan_rate}")
        cycles = self._format_summary_number(condition_payload.get("cycles"))
        if cycles:
            range_parts.append(f"cycle {cycles}")
        lines.append(" / ".join(range_parts) if range_parts else "条件: ヘッダ抽出なし")

        extra_parts: list[str] = []
        pulse_amplitude = self._format_summary_number(condition_payload.get("pulse_amplitude_v"), "V")
        if pulse_amplitude:
            extra_parts.append(f"amp {pulse_amplitude}")
        pulse_time = self._format_summary_number(condition_payload.get("pulse_time_s"), "s")
        if pulse_time:
            extra_parts.append(f"pulse {pulse_time}")
        step_v = self._format_summary_number(condition_payload.get("step_v"), "V")
        if step_v and not pulse_amplitude:
            extra_parts.append(f"step {step_v}")
        current_range = str(condition_payload.get("current_range") or "").strip()
        if current_range:
            extra_parts.append(f"range {current_range}")
        if parsed.metadata.get("parser_recovered"):
            extra_parts.append("回復読込")
        available_blocks = parsed.metadata.get("available_blocks")
        if isinstance(available_blocks, list) and len(available_blocks) > 1:
            extra_parts.append(f"block {len(available_blocks)}")
        if extra_parts:
            lines.append(" / ".join(extra_parts))
        return "\n".join(lines[:4])

    def _get_import_target(self, session_id: str | None = None, batch_item_id: str | None = None) -> dict[str, Any]:
        if batch_item_id:
            batch_item = self.repository.get_active_batch_item(batch_item_id)
            if not batch_item:
                raise ValueError("指定したバッチ項目が見つかりません。")
            assigned_measurement_id = str(batch_item.get("assigned_measurement_id") or "").strip()
            if assigned_measurement_id:
                raise ValueError("指定したバッチ項目にはすでに測定が紐付いています。")
            return {
                "batch_item_id": str(batch_item["batch_item_id"]),
                "session_id": str(batch_item["session_id"]),
                "condition_id": str(batch_item["condition_id"]),
                "rep_no": int(batch_item["rep_no"]),
            }
        decision = self.batch_linker.choose_target(session_id)
        return {
            "batch_item_id": decision.batch_item_id,
            "session_id": decision.session_id,
            "condition_id": decision.condition_id,
            "rep_no": decision.rep_no,
        }

    def _set_ivium_run_state(self, **updates: Any) -> None:
        with self._ivium_run_lock:
            payload = asdict(self._ivium_run_state)
            payload.update(updates)
            payload["updated_at"] = str(updates.get("updated_at") or now_iso())
            payload["log_lines"] = list(payload.get("log_lines", []))[-40:]
            self._ivium_run_state = IviumRunState(**payload)

    def _append_ivium_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        with self._ivium_run_lock:
            payload = asdict(self._ivium_run_state)
            log_lines = list(payload.get("log_lines", []))
            log_lines.append(f"{timestamp} {message}")
            payload["log_lines"] = log_lines[-40:]
            payload["message"] = message
            payload["updated_at"] = now_iso()
            self._ivium_run_state = IviumRunState(**payload)

    def get_ivium_run_state(self) -> dict[str, Any]:
        with self._ivium_run_lock:
            return asdict(self._ivium_run_state)

    def _append_batch_item_note(self, batch_item_id: str, note: str) -> None:
        batch_item = self.repository.get_record("batch_plan_items", batch_item_id)
        if not batch_item:
            return
        timestamped_note = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {note}"
        existing_note = str(batch_item.get("note") or "").strip()
        merged_note = timestamped_note if not existing_note else f"{existing_note}\n{timestamped_note}"
        self.repository.update_record("batch_plan_items", batch_item_id, {"note": merged_note[-4000:]})

    @staticmethod
    def _should_retry_measurement_import(error: Exception) -> bool:
        message = str(error).lower()
        transient_markers = (
            "数値データブロックを検出できませんでした",
            "unable to open database file",
            "database is locked",
            "database is malformed",
            "permission denied",
            "sharing violation",
            "file is not a database",
        )
        return any(marker.lower() in message for marker in transient_markers)

    @staticmethod
    def _wait_for_stable_file(path: Path, attempts: int = 6, delay_sec: float = 0.5) -> None:
        previous_signature: tuple[int, int] | None = None
        stable_count = 0
        for _ in range(attempts):
            if not path.exists():
                time.sleep(delay_sec)
                continue
            stat = path.stat()
            signature = (stat.st_size, int(stat.st_mtime_ns))
            if signature == previous_signature:
                stable_count += 1
                if stable_count >= 2:
                    return
            else:
                stable_count = 0
            previous_signature = signature
            time.sleep(delay_sec)

    def _merge_measurement_conditions(
        self,
        measurement_payload: dict[str, Any],
        condition_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not condition_row:
            return measurement_payload
        merged = dict(measurement_payload)
        preferred_method = condition_row.get("method") or condition_row.get("ivium_method_name")
        if preferred_method not in (None, ""):
            merged["method"] = preferred_method
        for field_name in CONDITION_WARNING_FIELDS:
            if merged.get(field_name) in (None, "") and condition_row.get(field_name) not in (None, ""):
                merged[field_name] = condition_row.get(field_name)
        return merged

    def _import_parsed_measurement(
        self,
        parsed,
        *,
        file_path: str | Path,
        session_id: str | None = None,
        batch_item_id: str | None = None,
    ) -> str:
        resolved_file_path = str(Path(file_path).resolve())
        existing_measurement = self.repository.get_measurement_by_raw_file_path(resolved_file_path)
        if existing_measurement:
            return str(existing_measurement["measurement_id"])

        decision = self._get_import_target(session_id, batch_item_id)
        session_row = self.repository.get_record("sessions", decision["session_id"])
        if not session_row:
            raise ValueError("紐付け先セッションが見つかりません。")
        condition_row = self.repository.get_record("conditions", decision["condition_id"])

        method_label, analysis_result = self._analyze_parsed_data(
            parsed.data,
            str(parsed.metadata.get("Method", session_row.get("method_default", ""))),
        )
        auto_quality = derive_auto_quality(
            status=str(parsed.metadata.get("endcondition") or parsed.metadata.get("Status") or ""),
            analysis_quality=analysis_result.quality_flag,
        )
        measurement_payload = self.batch_linker.build_measurement_payload(
            session_id=decision["session_id"],
            condition_id=decision["condition_id"],
            mip_usage_id=session_row.get("mip_usage_id"),
            raw_file_path=resolved_file_path,
            batch_item_id=decision["batch_item_id"],
            rep_no=decision["rep_no"],
            measured_at=str(parsed.metadata.get("starttime_iso", now_iso())),
            auto_quality_flag=auto_quality.value,
        )
        measurement_id = str(measurement_payload["measurement_id"])
        self.repository.insert_record("measurements", measurement_payload)
        self.repository.update_batch_item_status(
            decision["batch_item_id"],
            PlannedStatus.COMPLETED.value,
            assigned_measurement_id=measurement_id,
        )

        measurement_condition_payload = self._merge_measurement_conditions(
            build_measurement_conditions(parsed.metadata, parsed.raw_header_text),
            condition_row,
        )
        self.repository.insert_record(
            "measurement_conditions",
            {
                "condition_param_id": generate_id("MCOND"),
                "measurement_id": measurement_id,
                **measurement_condition_payload,
            },
        )
        self.repository.upsert_analysis_result(
            measurement_id,
            {
                "result_id": generate_id("ARES"),
                "measurement_id": measurement_id,
                "condition_id": decision["condition_id"],
                "session_id": decision["session_id"],
                "representative_current_a": analysis_result.representative_current_a,
                "representative_potential_v": analysis_result.representative_potential_v,
                "oxidation_peak_current_a": analysis_result.oxidation_peak_current_a,
                "oxidation_peak_potential_v": analysis_result.oxidation_peak_potential_v,
                "reduction_peak_current_a": analysis_result.reduction_peak_current_a,
                "reduction_peak_potential_v": analysis_result.reduction_peak_potential_v,
                "delta_ep_v": analysis_result.delta_ep_v,
                "integrated_area": analysis_result.integrated_area,
                "quality_flag": analysis_result.quality_flag,
                "analysis_method": method_label,
                "note": analysis_result.note,
            },
        )
        self.repository.replace_cycle_results(
            measurement_id,
            [
                {
                    "cycle_result_id": generate_id("CYCLE"),
                    "measurement_id": measurement_id,
                    "cycle_no": cycle_result.cycle_no,
                    "oxidation_peak_current_a": cycle_result.oxidation_peak_current_a,
                    "oxidation_peak_potential_v": cycle_result.oxidation_peak_potential_v,
                    "reduction_peak_current_a": cycle_result.reduction_peak_current_a,
                    "reduction_peak_potential_v": cycle_result.reduction_peak_potential_v,
                    "delta_ep_v": cycle_result.delta_ep_v,
                    "integrated_area": cycle_result.integrated_area,
                    "quality_flag": cycle_result.quality_flag,
                }
                for cycle_result in analysis_result.cycle_results
            ],
        )
        self.repository.update_record(
            "measurements",
            measurement_id,
            {
                "auto_quality_flag": auto_quality.value,
                "final_quality_flag": resolve_final_quality(auto_quality.value, None).value,
                "status": "analyzed",
            },
        )
        if self.config.plot_enabled:
            directories = session_output_directories(self.root_path, decision["session_id"])
            save_measurement_plot(
                parsed.data,
                directories["plots"] / f"{measurement_id}.png",
                f"{decision['session_id']} / {measurement_id}",
            )
        self.repository.refresh_condition_stats(condition_id=decision["condition_id"])
        self.aggregate_session(decision["session_id"])
        if self.config.mean_voltammogram_enabled:
            self.generate_mean_voltammograms(decision["session_id"], decision["condition_id"])
        return measurement_id

    def list_mips(self) -> list[dict[str, Any]]:
        return self.repository.list_records("mip_records", "preparation_date DESC, created_at DESC")

    def list_mip_usages(self) -> list[dict[str, Any]]:
        return self.repository.list_records("mip_usage_records", "coating_date DESC, created_at DESC")

    def list_sessions(self) -> list[dict[str, Any]]:
        return self.repository.list_records("sessions", "session_date DESC, created_at DESC")

    def list_conditions(self, session_id: str | None = None) -> list[dict[str, Any]]:
        if session_id:
            return self.repository.list_records(
                "conditions",
                "concentration_value ASC, created_at ASC",
                "session_id = ?",
                (session_id,),
            )
        return self.repository.list_records("conditions", "created_at DESC")

    def list_batch_items(self, session_id: str | None = None) -> list[dict[str, Any]]:
        return self.repository.list_active_batch_items(session_id)

    def list_measurements(self, session_id: str | None = None) -> list[dict[str, Any]]:
        return self.repository.list_active_measurements(session_id)

    def list_deleted_records(self, record_type: str) -> list[dict[str, Any]]:
        table_name = RESTORABLE_RECORD_TYPES.get(record_type)
        if not table_name:
            return []
        rows = self.repository.list_deleted_records(table_name)
        deleted_rows: list[dict[str, Any]] = []
        for row in rows:
            if record_type == "mip":
                summary = f"{row.get('template_name', '')} / {row.get('preparation_date', '')}"
                record_id = str(row["mip_id"])
            elif record_type == "session":
                summary = f"{row.get('session_date', '')} / {row.get('analyte', '')}"
                record_id = str(row["session_id"])
            elif record_type == "condition":
                summary = f"{row.get('concentration_value', '')} {row.get('concentration_unit', '')} / {row.get('method', '')}"
                record_id = str(row["condition_id"])
            else:
                summary = f"{row.get('condition_id', '')} / rep{row.get('rep_no', '')}"
                record_id = str(row["measurement_id"])
            deleted_rows.append(
                {
                    "record_id": record_id,
                    "summary": summary,
                    "deleted_at": str(row.get("deleted_at", "")),
                }
            )
        return deleted_rows

    def list_relink_measurements(self) -> list[dict[str, Any]]:
        return self.repository.database.fetch_all(
            """
            SELECT measurement_id, session_id, condition_id, rep_no, raw_file_path, measured_at
            FROM measurements
            WHERE COALESCE(is_deleted, 0) = 0
              AND COALESCE(raw_file_path, '') <> ''
            ORDER BY measured_at DESC, created_at DESC
            LIMIT 100
            """
        )

    def list_relink_batch_items(self) -> list[dict[str, Any]]:
        return self.repository.database.fetch_all(
            """
            SELECT batch_item_id, session_id, condition_id, rep_no, planned_status
            FROM batch_plan_items
            WHERE COALESCE(is_deleted, 0) = 0
              AND COALESCE(assigned_measurement_id, '') = ''
            ORDER BY
              CASE planned_status
                WHEN 'waiting' THEN 0
                WHEN 'relink_needed' THEN 1
                WHEN 'failed' THEN 2
                ELSE 3
              END,
              planned_order ASC,
              created_at ASC
            LIMIT 200
            """
        )

    def get_measurement_header_summary(self, measurement_id: str) -> str:
        if not measurement_id:
            return "測定 ID を選ぶと測定ファイル要約が表示されます。"
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or int(measurement.get("is_deleted", 0)) == 1:
            return "選択した測定が見つかりません。"
        raw_file_path = str(measurement.get("raw_file_path") or "").strip()
        if not raw_file_path:
            return f"{measurement_id}\n測定ファイル未登録"
        return self._build_ids_header_summary(raw_file_path)

    def load_condition_payload_from_ivium_file(self, file_path: str | Path) -> dict[str, Any]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Ivium ファイルが見つかりません: {path}")

        if path.suffix.lower() == ".imf":
            metadata, raw_header_text = parse_ivium_method_file(path)
        else:
            parsed = parse_measurement_file(path)
            metadata = parsed.metadata
            raw_header_text = parsed.raw_header_text

        ivium_method_name = resolve_ivium_method_name(str(metadata.get("Method") or ""))
        condition_payload = build_measurement_conditions(metadata, raw_header_text)
        condition_payload["method"] = resolve_app_method_name(ivium_method_name)
        condition_payload["ivium_method_name"] = ivium_method_name
        condition_payload["source_file_path"] = str(path.resolve())
        return condition_payload

    def load_condition_payload_from_ivium_template(self) -> dict[str, Any]:
        template_path = resolve_ivium_method_template_path(
            self.config.ivium_method_template_path,
            self.config.iviumsoft_exe_path,
        )
        return self.load_condition_payload_from_ivium_file(template_path)

    def home_snapshot(self) -> dict[str, Any]:
        return self.repository.get_home_snapshot()

    def get_default_mip_operator(self) -> str:
        return self.repository.get_latest_operator("mip_records")

    def get_default_mip_usage_operator(self, mip_id: str | None = None) -> str:
        if mip_id:
            mip_row = self.repository.get_record("mip_records", mip_id)
            if mip_row and int(mip_row.get("is_deleted", 0)) == 0 and str(mip_row.get("operator", "")).strip():
                return str(mip_row["operator"])
        return self.repository.get_latest_operator("mip_usage_records") or self.get_default_mip_operator()

    def get_default_session_operator(self, mip_usage_id: str | None = None) -> str:
        if mip_usage_id:
            usage_row = self.repository.get_record("mip_usage_records", mip_usage_id)
            if usage_row and int(usage_row.get("is_deleted", 0)) == 0 and str(usage_row.get("operator", "")).strip():
                return str(usage_row["operator"])
        return self.repository.get_latest_operator("sessions") or self.repository.get_latest_operator("mip_usage_records")

    def create_mip(self, payload: dict[str, Any]) -> str:
        require_fields(payload, ["template_name", "preparation_date", "operator"])
        mip_id = generate_id("MIP")
        self.repository.insert_record("mip_records", {"mip_id": mip_id, **payload})
        return mip_id

    def update_mip(self, mip_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["template_name", "preparation_date", "operator"])
        self.repository.update_record("mip_records", mip_id, payload)

    def duplicate_mip(self, mip_id: str) -> str:
        return self.repository.duplicate_record("mip_records", mip_id, "MIP")

    def delete_mip(self, mip_id: str) -> str:
        summary = self.repository.get_mip_dependency_summary(mip_id)
        if summary["usage_count"] == 0:
            self.repository.delete_record_physical("mip_records", mip_id)
            return "MIP を物理削除しました。"
        self.repository.logical_delete_mip(mip_id)
        return "関連データがあるため、MIP を論理削除しました。"

    def create_mip_usage(self, payload: dict[str, Any]) -> str:
        normalized_payload = {**payload, **with_mip_usage_field_defaults(payload)}
        require_fields(normalized_payload, ["mip_id"])
        require_any(normalized_payload, ["cp_preparation_date", "coating_date"])
        mip_usage_id = generate_id("MUSE")
        self.repository.insert_record("mip_usage_records", {"mip_usage_id": mip_usage_id, **normalized_payload})
        return mip_usage_id

    def update_mip_usage(self, mip_usage_id: str, payload: dict[str, Any]) -> None:
        normalized_payload = {**payload, **with_mip_usage_field_defaults(payload)}
        require_fields(normalized_payload, ["mip_id"])
        require_any(normalized_payload, ["cp_preparation_date", "coating_date"])
        self.repository.update_record("mip_usage_records", mip_usage_id, normalized_payload)

    def duplicate_mip_usage(self, mip_usage_id: str) -> str:
        return self.repository.duplicate_record("mip_usage_records", mip_usage_id, "MUSE")

    def delete_mip_usage(self, mip_usage_id: str) -> str:
        summary = self.repository.get_mip_usage_dependency_summary(mip_usage_id)
        if summary["session_count"] == 0:
            self.repository.delete_record_physical("mip_usage_records", mip_usage_id)
            return "MIP 使用記録を物理削除しました。"
        self.repository.logical_delete_mip_usage(mip_usage_id)
        return "関連データがあるため、MIP 使用記録を論理削除しました。"

    def create_session(self, payload: dict[str, Any]) -> str:
        require_fields(payload, ["session_date", "analyte", "mip_usage_id"])
        session_id = generate_id("SES")
        payload.setdefault("session_name", f"{payload['analyte']}_{payload['session_date']}")
        payload.setdefault("status", "draft")
        self.repository.insert_record("sessions", {"session_id": session_id, **payload})
        return session_id

    def update_session(self, session_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["session_date", "analyte", "mip_usage_id"])
        current = self.repository.get_record("sessions", session_id)
        if not current:
            raise ValueError(f"セッションが見つかりません: {session_id}")
        self.repository.update_record("sessions", session_id, payload)

        if payload["analyte"] != current.get("analyte"):
            self.repository.database.execute(
                """
                UPDATE conditions
                SET analyte = ?, updated_at = ?
                WHERE session_id = ? AND COALESCE(is_deleted, 0) = 0
                """,
                (payload["analyte"], now_iso(), session_id),
            )
        if payload["mip_usage_id"] != current.get("mip_usage_id"):
            self.repository.database.execute(
                """
                UPDATE measurements
                SET mip_usage_id = ?, updated_at = ?
                WHERE session_id = ?
                """,
                (payload["mip_usage_id"], now_iso(), session_id),
            )
        self.aggregate_session(session_id)

    def duplicate_session(self, session_id: str) -> str:
        new_id = self.repository.duplicate_record("sessions", session_id, "SES")
        self.repository.update_record("sessions", new_id, {"status": "draft"})
        return new_id

    def delete_session(self, session_id: str) -> str:
        summary = self.repository.get_session_dependency_summary(session_id)
        if summary["measurement_count"] == 0:
            self.repository.purge_session(session_id)
            return "関連する条件とバッチ計画を含めて、セッションを物理削除しました。"
        self.repository.logical_delete_session(session_id)
        return "測定データがあるため、セッションを論理削除しました。"

    def create_condition(self, payload: dict[str, Any]) -> str:
        require_fields(payload, ["session_id", "concentration_value", "concentration_unit", "method"])
        condition_id = generate_condition_id(payload["concentration_value"], payload["concentration_unit"])
        session = self.repository.get_record("sessions", str(payload["session_id"]))
        payload.setdefault("analyte", session["analyte"] if session else "")
        payload.setdefault("condition_status", "pending")
        self.repository.insert_record("conditions", {"condition_id": condition_id, **payload})
        self.repository.refresh_condition_stats(condition_id=condition_id)
        return condition_id

    def update_condition(self, condition_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["session_id", "concentration_value", "concentration_unit", "method"])
        current = self.repository.get_record("conditions", condition_id)
        if not current:
            raise ValueError(f"条件が見つかりません: {condition_id}")
        target_session = self.repository.get_record("sessions", str(payload["session_id"]))
        payload.setdefault("analyte", target_session["analyte"] if target_session else current.get("analyte", ""))

        if str(payload["session_id"]) != str(current["session_id"]):
            summary = self.repository.get_condition_dependency_summary(condition_id)
            if summary["measurement_count"] > 0:
                raise ValueError("測定データがある条件はセッションを変更できません。")
            update_time = now_iso()
            self.repository.database.execute(
                """
                UPDATE batch_plan_items
                SET session_id = ?, updated_at = ?
                WHERE condition_id = ?
                """,
                (payload["session_id"], update_time, condition_id),
            )
            self.repository.database.execute(
                """
                UPDATE aggregated_results
                SET session_id = ?
                WHERE condition_id = ?
                """,
                (payload["session_id"], condition_id),
            )
            self.repository.database.execute(
                """
                UPDATE mean_voltammogram_records
                SET session_id = ?
                WHERE condition_id = ?
                """,
                (payload["session_id"], condition_id),
            )
        self.repository.update_record("conditions", condition_id, payload)
        self.repository.refresh_condition_stats(condition_id=condition_id)
        affected_session_ids = {str(current["session_id"]), str(payload["session_id"])}
        for session_id in affected_session_ids:
            if self.repository.get_record("sessions", session_id):
                self.aggregate_session(session_id)

    def duplicate_condition(self, condition_id: str) -> str:
        current = self.repository.get_record("conditions", condition_id)
        if not current or int(current.get("is_deleted", 0)) == 1:
            raise ValueError(f"条件が見つかりません: {condition_id}")
        new_id = generate_condition_id(current.get("concentration_value"), current.get("concentration_unit"))
        cloned = dict(current)
        cloned["condition_id"] = new_id
        cloned["actual_replicates"] = 0
        cloned["n_valid"] = 0
        cloned["n_invalid"] = 0
        cloned["cv_percent"] = None
        cloned["condition_status"] = "pending"
        for column_name in ("created_at", "updated_at", "is_deleted", "deleted_at"):
            cloned.pop(column_name, None)
        self.repository.insert_record("conditions", cloned)
        return new_id

    def delete_condition(self, condition_id: str) -> str:
        summary = self.repository.get_condition_dependency_summary(condition_id)
        if summary["measurement_count"] == 0:
            self.repository.purge_condition(condition_id)
            return "条件を物理削除しました。"
        self.repository.mark_deleted("conditions", condition_id)
        return "測定データがあるため、条件を論理削除しました。"

    def generate_batch_plan(
        self,
        session_id: str,
        baseline_value: float | None,
        execution_mode: str,
    ) -> list[dict[str, Any]]:
        conditions = self.list_conditions(session_id)
        generated_items = generate_batch_plan_items(
            conditions=conditions,
            baseline_value=baseline_value,
            execution_mode=execution_mode,
        )
        self.repository.replace_batch_plan(session_id, generated_items)
        return self.list_batch_items(session_id)

    def update_batch_item(self, batch_item_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["session_id", "condition_id", "planned_order", "rep_no", "planned_status"])
        current = self.repository.get_record("batch_plan_items", batch_item_id)
        if not current:
            raise ValueError(f"バッチ計画項目が見つかりません: {batch_item_id}")
        session = self.repository.get_record("sessions", str(payload["session_id"]))
        condition = self.repository.get_record("conditions", str(payload["condition_id"]))
        if not session or int(session.get("is_deleted", 0)) == 1:
            raise ValueError("更新先のセッションが見つかりません。")
        if not condition or int(condition.get("is_deleted", 0)) == 1:
            raise ValueError("更新先の条件が見つかりません。")
        if str(condition["session_id"]) != str(payload["session_id"]):
            raise ValueError("選択した条件は指定セッションに属していません。")
        if current.get("assigned_measurement_id"):
            immutable_fields = (
                str(current.get("session_id")) != str(payload["session_id"])
                or str(current.get("condition_id")) != str(payload["condition_id"])
                or int(current.get("rep_no") or 0) != int(payload["rep_no"])
            )
            if immutable_fields:
                raise ValueError("測定が紐付いたバッチ計画はセッション・条件・rep を変更できません。")
        self.repository.update_record("batch_plan_items", batch_item_id, payload)

    def delete_batch_item(self, batch_item_id: str) -> str:
        summary = self.repository.get_batch_item_dependency_summary(batch_item_id)
        if summary["measurement_count"] == 0:
            self.repository.purge_batch_item(batch_item_id)
            return "バッチ計画項目を物理削除しました。"
        self.repository.logical_delete_batch_item(batch_item_id)
        return "測定データがあるため、バッチ計画項目を論理削除しました。"

    def create_measurement(self, payload: dict[str, Any]) -> str:
        require_fields(payload, ["condition_id", "session_id"])
        condition_id = str(payload["condition_id"])
        payload.setdefault("measurement_id", generate_id("MEAS"))
        payload.setdefault("rep_no", self.repository.get_next_rep_no(condition_id))
        payload.setdefault("measured_at", now_iso())
        payload.setdefault("status", "manual")
        payload.setdefault("link_status", "manual")
        payload.setdefault("auto_quality_flag", derive_auto_quality(payload.get("noise_level"), str(payload.get("status"))).value)
        payload.setdefault("manual_quality_flag", None)
        payload["final_quality_flag"] = resolve_final_quality(
            str(payload.get("auto_quality_flag")),
            str(payload.get("manual_quality_flag")) if payload.get("manual_quality_flag") else None,
        ).value
        session = self.repository.get_record("sessions", str(payload["session_id"]))
        payload.setdefault("mip_usage_id", session["mip_usage_id"] if session else None)
        self.repository.insert_record("measurements", payload)
        self.repository.refresh_condition_stats(condition_id=condition_id)
        return str(payload["measurement_id"])

    def update_measurement(self, measurement_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["condition_id", "session_id"])
        current = self.repository.get_record("measurements", measurement_id)
        if not current or int(current.get("is_deleted", 0)) == 1:
            raise ValueError(f"測定が見つかりません: {measurement_id}")

        session = self.repository.get_record("sessions", str(payload["session_id"]))
        condition = self.repository.get_record("conditions", str(payload["condition_id"]))
        if not session or int(session.get("is_deleted", 0)) == 1:
            raise ValueError("更新先のセッションが見つかりません。")
        if not condition or int(condition.get("is_deleted", 0)) == 1:
            raise ValueError("更新先の条件が見つかりません。")
        if str(condition["session_id"]) != str(payload["session_id"]):
            raise ValueError("選択した条件は指定セッションに属していません。")

        target_condition_id = str(payload["condition_id"])
        target_session_id = str(payload["session_id"])
        moving_target = (
            str(current.get("condition_id")) != target_condition_id
            or str(current.get("session_id")) != target_session_id
        )
        summary = self.repository.get_measurement_dependency_summary(measurement_id)
        has_related_analysis = sum(summary.values()) > 0
        if moving_target and (has_related_analysis or current.get("batch_item_id")):
            raise ValueError("解析済み、またはバッチ計画に紐付いた測定は条件・セッションを変更できません。")

        auto_quality_flag = derive_auto_quality(
            payload.get("noise_level"),
            str(payload.get("status")),
        ).value
        manual_quality_flag = str(payload.get("manual_quality_flag")) if payload.get("manual_quality_flag") else None
        update_payload = dict(payload)
        update_payload["mip_usage_id"] = session.get("mip_usage_id")
        update_payload["auto_quality_flag"] = auto_quality_flag
        update_payload["final_quality_flag"] = resolve_final_quality(auto_quality_flag, manual_quality_flag).value

        if moving_target:
            update_payload["rep_no"] = self.repository.get_next_rep_no(target_condition_id)

        self.repository.update_record("measurements", measurement_id, update_payload)
        affected_condition_ids = {str(current["condition_id"]), target_condition_id}
        affected_session_ids = {str(current["session_id"]), target_session_id}
        for condition_id in affected_condition_ids:
            self.repository.refresh_condition_stats(condition_id=condition_id)
        for session_id in affected_session_ids:
            session_row = self.repository.get_record("sessions", session_id)
            if session_row and int(session_row.get("is_deleted", 0)) == 0:
                self.aggregate_session(session_id)
                if self.config.mean_voltammogram_enabled:
                    self.generate_mean_voltammograms(session_id)

    def delete_measurement(self, measurement_id: str) -> str:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or int(measurement.get("is_deleted", 0)) == 1:
            raise ValueError(f"測定が見つかりません: {measurement_id}")
        summary = self.repository.get_measurement_dependency_summary(measurement_id)
        if sum(summary.values()) == 0:
            self.repository.purge_measurement(measurement_id)
            message = "測定を物理削除しました。"
        else:
            self.repository.logical_delete_measurement(measurement_id)
            message = "解析関連データがあるため、測定を論理削除しました。"
        condition_id = str(measurement["condition_id"])
        session_id = str(measurement["session_id"])
        self.repository.refresh_condition_stats(condition_id=condition_id)
        session = self.repository.get_record("sessions", session_id)
        if session and int(session.get("is_deleted", 0)) == 0:
                self.aggregate_session(session_id)
                if self.config.mean_voltammogram_enabled:
                    self.generate_mean_voltammograms(session_id, condition_id)
        return message

    def exclude_measurement(self, measurement_id: str, reason: str = "") -> str:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or int(measurement.get("is_deleted", 0)) == 1:
            raise ValueError(f"測定が見つかりません: {measurement_id}")
        exclusion_reason = str(reason).strip() or "誤測定のため除外"
        auto_quality_flag = str(measurement.get("auto_quality_flag") or QualityFlag.VALID.value)
        self.repository.update_record(
            "measurements",
            measurement_id,
            {
                "manual_quality_flag": QualityFlag.INVALID.value,
                "final_quality_flag": resolve_final_quality(auto_quality_flag, QualityFlag.INVALID.value).value,
                "exclusion_reason": exclusion_reason,
            },
        )
        condition_id = str(measurement["condition_id"])
        session_id = str(measurement["session_id"])
        self.repository.refresh_condition_stats(condition_id=condition_id)
        session = self.repository.get_record("sessions", session_id)
        if session and int(session.get("is_deleted", 0)) == 0:
            self.aggregate_session(session_id)
            if self.config.mean_voltammogram_enabled:
                self.generate_mean_voltammograms(session_id, condition_id)
        return f"{measurement_id} を除外しました。"

    def clear_measurement_exclusion(self, measurement_id: str) -> str:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or int(measurement.get("is_deleted", 0)) == 1:
            raise ValueError(f"測定が見つかりません: {measurement_id}")
        auto_quality_flag = str(measurement.get("auto_quality_flag") or QualityFlag.VALID.value)
        self.repository.update_record(
            "measurements",
            measurement_id,
            {
                "manual_quality_flag": None,
                "final_quality_flag": resolve_final_quality(auto_quality_flag, None).value,
                "exclusion_reason": "",
            },
        )
        condition_id = str(measurement["condition_id"])
        session_id = str(measurement["session_id"])
        self.repository.refresh_condition_stats(condition_id=condition_id)
        session = self.repository.get_record("sessions", session_id)
        if session and int(session.get("is_deleted", 0)) == 0:
            self.aggregate_session(session_id)
            if self.config.mean_voltammogram_enabled:
                self.generate_mean_voltammograms(session_id, condition_id)
        return f"{measurement_id} の除外を解除しました。"

    def restore_deleted_record(self, record_type: str, record_id: str) -> str:
        if record_type == "mip":
            row = self.repository.get_record("mip_records", record_id)
            if not row or int(row.get("is_deleted", 0)) == 0:
                raise ValueError("復元対象の MIP が見つかりません。")
            self.repository.restore_record("mip_records", record_id)
            self.repository.bulk_restore_records("mip_usage_records", "mip_id = ?", (record_id,))
            self.repository.bulk_restore_records(
                "sessions",
                "mip_usage_id IN (SELECT mip_usage_id FROM mip_usage_records WHERE mip_id = ?)",
                (record_id,),
            )
            self.repository.bulk_restore_records(
                "conditions",
                "session_id IN (SELECT session_id FROM sessions WHERE mip_usage_id IN (SELECT mip_usage_id FROM mip_usage_records WHERE mip_id = ?))",
                (record_id,),
            )
            for session in self.list_sessions():
                if session.get("mip_usage_id") in {
                    usage["mip_usage_id"] for usage in self.list_mip_usages() if usage.get("mip_id") == record_id
                }:
                    self.aggregate_session(str(session["session_id"]))
            return "MIP と関連データを復元しました。"

        if record_type == "session":
            row = self.repository.get_record("sessions", record_id)
            if not row or int(row.get("is_deleted", 0)) == 0:
                raise ValueError("復元対象のセッションが見つかりません。")
            usage = self.repository.get_record("mip_usage_records", str(row["mip_usage_id"]))
            if not usage or int(usage.get("is_deleted", 0)) == 1:
                raise ValueError("先に関連する MIP または使用記録を復元してください。")
            self.repository.restore_record("sessions", record_id)
            self.repository.bulk_restore_records("conditions", "session_id = ?", (record_id,))
            self.repository.refresh_condition_stats(session_id=record_id)
            self.aggregate_session(record_id)
            return "セッションと条件を復元しました。"

        if record_type == "condition":
            row = self.repository.get_record("conditions", record_id)
            if not row or int(row.get("is_deleted", 0)) == 0:
                raise ValueError("復元対象の条件が見つかりません。")
            session = self.repository.get_record("sessions", str(row["session_id"]))
            if not session or int(session.get("is_deleted", 0)) == 1:
                raise ValueError("先に関連セッションを復元してください。")
            self.repository.restore_record("conditions", record_id)
            self.repository.refresh_condition_stats(condition_id=record_id)
            self.aggregate_session(str(row["session_id"]))
            return "条件を復元しました。"

        if record_type == "measurement":
            row = self.repository.get_record("measurements", record_id)
            if not row or int(row.get("is_deleted", 0)) == 0:
                raise ValueError("復元対象の測定が見つかりません。")
            session = self.repository.get_record("sessions", str(row["session_id"]))
            condition = self.repository.get_record("conditions", str(row["condition_id"]))
            if not session or int(session.get("is_deleted", 0)) == 1:
                raise ValueError("先に関連セッションを復元してください。")
            if not condition or int(condition.get("is_deleted", 0)) == 1:
                raise ValueError("先に関連条件を復元してください。")
            batch_item_id = str(row.get("batch_item_id") or "").strip()
            if batch_item_id:
                batch_item = self.repository.get_record("batch_plan_items", batch_item_id)
                if batch_item and int(batch_item.get("is_deleted", 0) or 0) == 0:
                    assigned_measurement_id = str(batch_item.get("assigned_measurement_id") or "").strip()
                    if assigned_measurement_id and assigned_measurement_id != record_id:
                        raise ValueError("関連バッチ項目に別の測定が紐付いているため復元できません。")
            self.repository.restore_record("measurements", record_id)
            if batch_item_id:
                batch_item = self.repository.get_record("batch_plan_items", batch_item_id)
                if batch_item and int(batch_item.get("is_deleted", 0) or 0) == 0:
                    self.repository.update_batch_item_status(
                        batch_item_id,
                        PlannedStatus.COMPLETED.value,
                        assigned_measurement_id=record_id,
                    )
            self.repository.refresh_condition_stats(condition_id=str(row["condition_id"]))
            self.aggregate_session(str(row["session_id"]))
            if self.config.mean_voltammogram_enabled:
                self.generate_mean_voltammograms(str(row["session_id"]), str(row["condition_id"]))
            return "測定を復元しました。"

        raise ValueError("未対応の復元種別です。")

    def relink_measurement(self, measurement_id: str, batch_item_id: str) -> str:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or int(measurement.get("is_deleted", 0)) == 1:
            raise ValueError("再リンク対象の測定が見つかりません。")
        target_batch = self.repository.get_active_batch_item(batch_item_id)
        if not target_batch:
            raise ValueError("移動先のバッチ項目が見つかりません。")
        assigned_measurement_id = str(target_batch.get("assigned_measurement_id") or "").strip()
        if assigned_measurement_id and assigned_measurement_id != measurement_id:
            raise ValueError("移動先バッチ項目には別の測定が紐付いています。")

        old_batch_item_id = str(measurement.get("batch_item_id") or "").strip()
        old_condition_id = str(measurement["condition_id"])
        old_session_id = str(measurement["session_id"])
        session_row = self.repository.get_record("sessions", str(target_batch["session_id"]))
        if not session_row:
            raise ValueError("移動先セッションが見つかりません。")

        self.repository.update_record(
            "measurements",
            measurement_id,
            {
                "batch_item_id": str(target_batch["batch_item_id"]),
                "condition_id": str(target_batch["condition_id"]),
                "session_id": str(target_batch["session_id"]),
                "mip_usage_id": session_row.get("mip_usage_id"),
                "rep_no": int(target_batch["rep_no"]),
                "link_status": PlannedStatus.COMPLETED.value,
            },
        )
        self.repository.database.execute(
            """
            UPDATE analysis_results
            SET condition_id = ?, session_id = ?
            WHERE measurement_id = ?
            """,
            (target_batch["condition_id"], target_batch["session_id"], measurement_id),
        )

        if old_batch_item_id and old_batch_item_id != batch_item_id:
            self.repository.update_record(
                "batch_plan_items",
                old_batch_item_id,
                {
                    "planned_status": PlannedStatus.WAITING.value,
                    "assigned_measurement_id": None,
                },
            )
        self.repository.update_batch_item_status(
            str(target_batch["batch_item_id"]),
            PlannedStatus.COMPLETED.value,
            assigned_measurement_id=measurement_id,
        )

        affected_condition_ids = {old_condition_id, str(target_batch["condition_id"])}
        affected_session_ids = {old_session_id, str(target_batch["session_id"])}
        for condition_id in affected_condition_ids:
            self.repository.refresh_condition_stats(condition_id=condition_id)
        for session_id in affected_session_ids:
            session = self.repository.get_record("sessions", session_id)
            if session and int(session.get("is_deleted", 0)) == 0:
                self.aggregate_session(session_id)
                if self.config.mean_voltammogram_enabled:
                    self.generate_mean_voltammograms(session_id)
        return f"{measurement_id} を {batch_item_id} に再リンクしました。"

    def get_condition_warnings(self, session_id: str) -> dict[str, str]:
        rows = self.repository.database.fetch_all(
            """
            SELECT
                m.condition_id,
                m.measurement_id,
                mc.method,
                mc.potential_start_v,
                mc.potential_end_v,
                mc.potential_vertex_1_v,
                mc.potential_vertex_2_v,
                mc.scan_rate_v_s,
                mc.step_v,
                mc.pulse_amplitude_v,
                mc.pulse_time_s,
                mc.quiet_time_s,
                mc.cycles,
                mc.current_range,
                mc.filter_setting
            FROM measurements AS m
            LEFT JOIN measurement_conditions AS mc ON m.measurement_id = mc.measurement_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            WHERE m.session_id = ?
              AND COALESCE(m.is_deleted, 0) = 0
              AND COALESCE(c.is_deleted, 0) = 0
              AND COALESCE(s.is_deleted, 0) = 0
            ORDER BY m.condition_id ASC, m.rep_no ASC
            """,
            (session_id,),
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["condition_id"]), []).append(row)

        warnings: dict[str, str] = {}
        for condition_id, condition_rows in grouped.items():
            differing_fields: list[str] = []
            for field_name, label in CONDITION_WARNING_FIELDS.items():
                normalized_values = {
                    self._normalize_warning_value(row.get(field_name))
                    for row in condition_rows
                }
                if len(normalized_values) > 1:
                    differing_fields.append(label)
            if differing_fields:
                preview = ", ".join(differing_fields[:3])
                if len(differing_fields) > 3:
                    preview += f" +{len(differing_fields) - 3}"
                warnings[condition_id] = preview
        return warnings

    def _analyze_parsed_data(self, dataframe: pd.DataFrame, method: str):
        normalized_method = method.lower()
        if "dpv" in normalized_method:
            return "DPV", analyze_dpv_curve(dataframe, baseline_correction=self.config.baseline_correction)
        return "CV", analyze_cv_curve(dataframe, representative_cycle_rule=self.config.representative_cycle_rule)

    def import_measurement_file(
        self,
        file_path: str | Path,
        session_id: str | None = None,
        batch_item_id: str | None = None,
    ) -> str:
        parsed = parse_measurement_file(file_path)
        return self._import_parsed_measurement(
            parsed,
            file_path=file_path,
            session_id=session_id,
            batch_item_id=batch_item_id,
        )

    def import_ids_file(
        self,
        file_path: str | Path,
        session_id: str | None = None,
        batch_item_id: str | None = None,
    ) -> str:
        return self.import_measurement_file(file_path, session_id=session_id, batch_item_id=batch_item_id)

    def reanalyze_measurement(self, measurement_id: str) -> None:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or not measurement.get("raw_file_path"):
            raise ValueError("再解析対象の raw_file_path が見つかりません。")
        parsed = parse_measurement_file(str(measurement["raw_file_path"]))
        method_row = self.repository.get_measurement_condition(measurement_id) or {}
        method_label, analysis_result = self._analyze_parsed_data(
            parsed.data,
            str(method_row.get("method", parsed.metadata.get("Method", ""))),
        )
        self.repository.upsert_analysis_result(
            measurement_id,
            {
                "result_id": generate_id("ARES"),
                "measurement_id": measurement_id,
                "condition_id": measurement["condition_id"],
                "session_id": measurement["session_id"],
                "representative_current_a": analysis_result.representative_current_a,
                "representative_potential_v": analysis_result.representative_potential_v,
                "oxidation_peak_current_a": analysis_result.oxidation_peak_current_a,
                "oxidation_peak_potential_v": analysis_result.oxidation_peak_potential_v,
                "reduction_peak_current_a": analysis_result.reduction_peak_current_a,
                "reduction_peak_potential_v": analysis_result.reduction_peak_potential_v,
                "delta_ep_v": analysis_result.delta_ep_v,
                "integrated_area": analysis_result.integrated_area,
                "quality_flag": analysis_result.quality_flag,
                "analysis_method": method_label,
                "note": analysis_result.note,
            },
        )
        self.aggregate_session(str(measurement["session_id"]))

    def aggregate_session(self, session_id: str) -> list[dict[str, Any]]:
        session = self.repository.get_record("sessions", session_id)
        if not session or int(session.get("is_deleted", 0)) == 1:
            return []
        mip_usage = self.repository.get_record("mip_usage_records", session["mip_usage_id"])
        mip_id = mip_usage["mip_id"] if mip_usage else None
        aggregates: list[dict[str, Any]] = []
        condition_warnings = self.get_condition_warnings(session_id)
        for condition in self.list_conditions(session_id):
            measurement_rows = self.repository.list_active_measurements(session_id)
            measurement_rows = [row for row in measurement_rows if row["condition_id"] == condition["condition_id"]]
            analysis_rows = self.repository.database.fetch_all(
                """
                SELECT a.*
                FROM analysis_results AS a
                INNER JOIN measurements AS m ON m.measurement_id = a.measurement_id
                WHERE a.condition_id = ?
                  AND COALESCE(m.is_deleted, 0) = 0
                ORDER BY created_at ASC
                """,
                (condition["condition_id"],),
            )
            aggregate = aggregate_condition_rows(
                condition_row=condition,
                session_row=session,
                mip_id=mip_id,
                mip_usage_id=session.get("mip_usage_id"),
                measurement_rows=measurement_rows,
                analysis_rows=analysis_rows,
            )
            aggregate["aggregate_id"] = generate_id("AGG")
            aggregate["note"] = condition_warnings.get(str(condition["condition_id"]), "")
            aggregates.append(aggregate)
        self.repository.replace_aggregated_results(session_id, aggregates)
        self.repository.refresh_condition_stats(session_id=session_id)
        return aggregates

    def generate_mean_voltammograms(
        self,
        session_id: str,
        condition_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if condition_id:
            condition_rows = [self.repository.get_record("conditions", condition_id)]
        else:
            condition_rows = self.list_conditions(session_id)

        payloads: list[dict[str, Any]] = []
        for condition in condition_rows:
            if not condition or int(condition.get("is_deleted", 0)) == 1:
                continue
            measurement_rows = self.repository.get_condition_measurements(
                condition["condition_id"],
                self.config.mean_voltammogram_include_flags,
            )
            curves: list[dict[str, Any]] = []
            for measurement in measurement_rows:
                raw_file_path = measurement.get("raw_file_path")
                if not raw_file_path or not Path(str(raw_file_path)).exists():
                    continue
                parsed = parse_measurement_file(str(raw_file_path))
                if parsed.data.empty:
                    continue
                curves.append({"measurement_id": measurement["measurement_id"], "dataframe": parsed.data})

            if not curves:
                self.repository.log_error(
                    "平均ボルタモグラムの対象データがありません。",
                    f"condition_id={condition['condition_id']}",
                    session_id=session_id,
                )
                continue

            mean_result = generate_mean_curve(
                curves,
                interpolation_enabled=self.config.interpolation_enabled,
                interpolation_points=self.config.interpolation_points,
                interpolation_method=self.config.interpolation_method,
            )
            directories = session_output_directories(self.root_path, session_id)
            base_name = f"{condition['condition_id']}_mean_voltammogram"
            csv_path = directories["processed"] / f"{base_name}.csv"
            png_path = directories["plots"] / f"{base_name}.png"
            self.csv_exporter.export_dataframe(mean_result.dataframe, csv_path)
            if self.config.plot_enabled:
                title = (
                    f"{session_id} / {condition['condition_id']} / "
                    f"{condition.get('concentration_value')} {condition.get('concentration_unit')} / n={mean_result.n_used}"
                )
                save_mean_curve_plot(mean_result.dataframe, png_path, title)
            payloads.append(
                {
                    "mean_voltammogram_id": generate_id("MEAN"),
                    "session_id": session_id,
                    "condition_id": condition["condition_id"],
                    "include_flags": str(self.config.mean_voltammogram_include_flags),
                    "interpolation_method": mean_result.interpolation_method,
                    "interpolation_points": mean_result.interpolation_points,
                    "n_used": mean_result.n_used,
                    "source_measurement_ids_json": str(mean_result.source_measurement_ids),
                    "csv_path": str(csv_path),
                    "png_path": str(png_path) if self.config.plot_enabled else "",
                    "excel_sheet_name": f"mean_{condition['condition_id']}"[:31],
                }
            )
        self.repository.replace_mean_voltammogram_records(session_id, condition_id, payloads)
        return payloads

    def export_session_analysis_plots(self, session_id: str) -> dict[str, str]:
        bundle = self.repository.get_session_bundle(session_id)
        session_row = bundle["session"]
        usage_row = None
        if session_row.get("mip_usage_id"):
            usage_row = self.repository.get_record("mip_usage_records", str(session_row["mip_usage_id"]))

        directories = session_output_directories(self.root_path, session_id)
        analysis_root = directories["root"]
        source_root = analysis_root / "analysis_source"
        condition_rows = [
            row
            for row in sorted(
                bundle["conditions"],
                key=lambda row: (
                    row.get("concentration_value") is None,
                    float(row.get("concentration_value") or 0.0),
                    str(row.get("condition_id") or ""),
                ),
            )
            if row and int(row.get("is_deleted", 0)) == 0
        ]
        measurement_map = {
            str(condition_row["condition_id"]): self.repository.get_condition_measurements(
                str(condition_row["condition_id"]),
                self.config.mean_voltammogram_include_flags,
            )
            for condition_row in condition_rows
        }
        if not any(measurement_map.values()):
            raise ValueError("解析プロット対象の `.ids` / `.idf.sqlite` が見つかりません。")

        outputs = build_reference_source_tree(
            source_root,
            condition_rows,
            measurement_map,
            usage_row,
        )
        outputs.update(run_reference_session_analysis(source_root, analysis_root))
        return outputs

        processed_root = directories["processed"] / "session_plot_analysis"
        plots_root = directories["plots"] / "session_plot_analysis"
        individual_dir = plots_root / "individual_voltammograms"
        mean_dir = plots_root / "mean_voltammograms"
        calibration_dir = plots_root / "calibration"
        ensure_directories([processed_root, plots_root, individual_dir, mean_dir, calibration_dir])

        def _concentration_number(value: Any) -> float | None:
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        def _concentration_label(condition_row: dict[str, Any]) -> str:
            value = condition_row.get("concentration_value")
            unit = str(condition_row.get("concentration_unit") or "").strip()
            if isinstance(value, float):
                value_text = f"{value:g}"
            else:
                value_text = str(value or "").strip()
            return f"{value_text} {unit}".strip() or str(condition_row.get("condition_id") or "-")

        session_label_parts = [
            str(session_row.get("session_name") or "").strip(),
            str(session_row.get("analyte") or "").strip(),
        ]
        if usage_row and usage_row.get("coating_height") not in (None, ""):
            session_label_parts.append(f"height={usage_row.get('coating_height')}")
        session_label = " / ".join(part for part in session_label_parts if part) or session_id

        condition_rows = [
            row
            for row in sorted(
                bundle["conditions"],
                key=lambda row: (
                    _concentration_number(row.get("concentration_value")) is None,
                    _concentration_number(row.get("concentration_value")) or 0.0,
                    str(row.get("condition_id") or ""),
                ),
            )
            if row and int(row.get("is_deleted", 0)) == 0
        ]
        file_inventory_rows: list[dict[str, Any]] = []
        file_rows: list[dict[str, Any]] = []
        absolute_integral_rows: list[dict[str, Any]] = []
        cycle1_reference_rows: list[dict[str, Any]] = []
        condition_curve_payloads: dict[str, list[dict[str, object]]] = {}
        representative_mode = "mean_of_cycles"
        representative_label = "Mean of detected cycles"

        for condition_row in condition_rows:
            measurement_rows = self.repository.get_condition_measurements(
                str(condition_row["condition_id"]),
                self.config.mean_voltammogram_include_flags,
            )
            for measurement_row in measurement_rows:
                raw_file_path = str(measurement_row.get("raw_file_path") or "").strip()
                if not raw_file_path:
                    continue
                path = Path(raw_file_path)
                if not path.exists():
                    self.repository.log_error(
                        "解析プロット対象の測定ファイルが見つかりません。",
                        f"session_plot_analysis:{raw_file_path}",
                        session_id=session_id,
                        measurement_id=str(measurement_row["measurement_id"]),
                    )
                    continue
                try:
                    parsed = parse_measurement_file(path)
                    cycle_curves = extract_cycle_curves(parsed)
                    representative_curve, cycle_count = select_representative_curve(
                        parsed,
                        representative_mode=representative_mode,
                        interpolation_enabled=self.config.interpolation_enabled,
                        interpolation_points=self.config.interpolation_points,
                        interpolation_method=self.config.interpolation_method,
                    )
                except Exception as error:
                    self.repository.log_error(
                        str(error),
                        f"session_plot_analysis:{raw_file_path}",
                        session_id=session_id,
                        measurement_id=str(measurement_row["measurement_id"]),
                    )
                    continue
                if representative_curve.empty:
                    continue

                min_potential, min_current = compute_curve_minimum(representative_curve)
                concentration_label = _concentration_label(condition_row)
                measurement_id = str(measurement_row["measurement_id"])
                rep_no = int(measurement_row.get("rep_no") or 0)
                cycle_lengths = ",".join(str(len(curve)) for curve in cycle_curves)
                file_name = path.name

                individual_path = individual_dir / (
                    f"{condition_row['condition_id']}_{measurement_id}_rep{rep_no:02d}.png"
                )
                if self.config.plot_enabled:
                    save_individual_cycles_plot(
                        cycle_curves or [representative_curve],
                        representative_curve,
                        minimum_potential_v=min_potential,
                        minimum_current_a=min_current,
                        file_path=individual_path,
                        title=f"{session_label} / {concentration_label} / {measurement_id}",
                        representative_label=representative_label,
                    )

                file_inventory_rows.append(
                    {
                        "measurement_id": measurement_id,
                        "condition_id": str(condition_row["condition_id"]),
                        "concentration_value": condition_row.get("concentration_value"),
                        "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                        "rep_no": rep_no,
                        "file_name": file_name,
                        "raw_file_path": str(path.resolve()),
                        "cycle_count": cycle_count,
                        "cycle_lengths": cycle_lengths,
                        "representative_mode": representative_mode,
                        "representative_points": int(len(representative_curve)),
                        "final_quality_flag": str(measurement_row.get("final_quality_flag") or ""),
                    }
                )
                file_rows.append(
                    {
                        "measurement_id": measurement_id,
                        "condition_id": str(condition_row["condition_id"]),
                        "concentration_value": condition_row.get("concentration_value"),
                        "concentration_sort_value": _concentration_number(condition_row.get("concentration_value")),
                        "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                        "concentration_label": concentration_label,
                        "rep_no": rep_no,
                        "cycle_count": cycle_count,
                        "file_name": file_name,
                        "final_quality_flag": str(measurement_row.get("final_quality_flag") or ""),
                        "representative_mode": representative_mode,
                        "min_current_a": min_current,
                        "min_potential_v": min_potential,
                        "raw_file_path": str(path.resolve()),
                        "individual_plot_path": str(individual_path) if self.config.plot_enabled else "",
                    }
                )
                condition_curve_payloads.setdefault(str(condition_row["condition_id"]), []).append(
                    {
                        "measurement_id": measurement_id,
                        "dataframe": representative_curve,
                    }
                )

                cycle_integrals: list[float] = []
                for cycle_no, cycle_curve in enumerate(cycle_curves, start=1):
                    integral_value = compute_absolute_integral(cycle_curve)
                    cycle_integrals.append(integral_value)
                    absolute_integral_rows.append(
                        {
                            "measurement_id": measurement_id,
                            "condition_id": str(condition_row["condition_id"]),
                            "concentration_value": condition_row.get("concentration_value"),
                            "concentration_sort_value": _concentration_number(condition_row.get("concentration_value")),
                            "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                            "rep_no": rep_no,
                            "cycle_no": cycle_no,
                            "series_label": f"scan{cycle_no}",
                            "absolute_integral_a_v": integral_value,
                            "file_name": file_name,
                            "raw_file_path": str(path.resolve()),
                        }
                    )
                if cycle_integrals:
                    absolute_integral_rows.append(
                        {
                            "measurement_id": measurement_id,
                            "condition_id": str(condition_row["condition_id"]),
                            "concentration_value": condition_row.get("concentration_value"),
                            "concentration_sort_value": _concentration_number(condition_row.get("concentration_value")),
                            "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                            "rep_no": rep_no,
                            "cycle_no": 0,
                            "series_label": "mean_of_cycles",
                            "absolute_integral_a_v": float(np.mean(cycle_integrals)),
                            "file_name": file_name,
                            "raw_file_path": str(path.resolve()),
                        }
                    )

                for reference_row in compute_cycle1_reference_rows(cycle_curves):
                    cycle1_reference_rows.append(
                        {
                            "measurement_id": measurement_id,
                            "condition_id": str(condition_row["condition_id"]),
                            "concentration_value": condition_row.get("concentration_value"),
                            "concentration_sort_value": _concentration_number(condition_row.get("concentration_value")),
                            "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                            "rep_no": rep_no,
                            "cycle_no": int(reference_row["cycle_no"]),
                            "series_label": f"scan{int(reference_row['cycle_no'])}",
                            "reference_potential_v": float(reference_row["reference_potential_v"]),
                            "reference_current_a": float(reference_row["reference_current_a"]),
                            "reference_branch": int(reference_row["reference_branch"]),
                            "file_name": file_name,
                            "raw_file_path": str(path.resolve()),
                        }
                    )

        if not file_rows:
            raise ValueError("解析プロット対象の `.ids` / `.idf.sqlite` が見つかりません。")

        file_inventory_frame = pd.DataFrame(file_inventory_rows).sort_values(
            ["concentration_value", "condition_id", "rep_no", "measurement_id"],
            kind="stable",
        )
        file_frame = pd.DataFrame(file_rows).sort_values(
            ["concentration_sort_value", "condition_id", "rep_no", "measurement_id"],
            kind="stable",
        )
        file_inventory_csv = self.csv_exporter.export_dataframe(
            file_inventory_frame,
            processed_root / "file_inventory.csv",
        )
        file_min_currents_csv = self.csv_exporter.export_dataframe(
            file_frame.drop(columns=["concentration_sort_value"]),
            processed_root / "file_min_currents.csv",
        )

        condition_summary_rows: list[dict[str, Any]] = []
        overlay_rows: list[dict[str, object]] = []
        for condition_row in condition_rows:
            condition_id = str(condition_row["condition_id"])
            curve_payloads = condition_curve_payloads.get(condition_id, [])
            if not curve_payloads:
                continue
            representative_curves = [payload["dataframe"] for payload in curve_payloads]
            mean_curve_frame = build_cv_mean_curve(
                representative_curves,
                interpolation_enabled=self.config.interpolation_enabled,
                interpolation_points=self.config.interpolation_points,
                interpolation_method=self.config.interpolation_method,
            )
            mean_curve_for_minimum = mean_curve_frame.rename(columns={"mean_current_a": "current_a"})[
                ["potential_v", "current_a"]
            ]
            mean_min_potential, mean_min_current = compute_curve_minimum(mean_curve_for_minimum)
            mean_curve_csv = self.csv_exporter.export_dataframe(
                mean_curve_frame,
                processed_root / f"{condition_id}_mean_voltammogram.csv",
            )
            mean_curve_png = mean_dir / f"{condition_id}_mean_voltammogram.png"
            if self.config.plot_enabled:
                save_condition_mean_plot(
                    representative_curves,
                    mean_curve_frame,
                    minimum_potential_v=mean_min_potential,
                    minimum_current_a=mean_min_current,
                    file_path=mean_curve_png,
                    title=f"{session_label} / {_concentration_label(condition_row)} / n={len(representative_curves)}",
                    mean_label=f"Condition mean (n={len(representative_curves)})",
                )
            subset = file_frame[file_frame["condition_id"] == condition_id]
            mean_of_file_mins = float(subset["min_current_a"].mean())
            std_of_file_mins = float(subset["min_current_a"].std(ddof=1)) if len(subset) > 1 else 0.0
            condition_summary_rows.append(
                {
                    "condition_id": condition_id,
                    "concentration_value": condition_row.get("concentration_value"),
                    "concentration_sort_value": _concentration_number(condition_row.get("concentration_value")),
                    "concentration_unit": str(condition_row.get("concentration_unit") or ""),
                    "concentration_label": _concentration_label(condition_row),
                    "method": str(condition_row.get("method") or ""),
                    "n_measurements": int(len(subset)),
                    "mean_of_file_min_current_a": mean_of_file_mins,
                    "std_of_file_min_current_a": std_of_file_mins,
                    "mean_voltammogram_min_potential_v": mean_min_potential,
                    "mean_voltammogram_min_current_a": mean_min_current,
                    "mean_min_current_a": mean_of_file_mins,
                    "std_min_current_a": std_of_file_mins,
                    "mean_min_potential_v": mean_min_potential,
                    "representative_mode": representative_mode,
                    "mean_curve_csv_path": str(mean_curve_csv),
                    "mean_curve_png_path": str(mean_curve_png) if self.config.plot_enabled else "",
                }
            )
            overlay_rows.append(
                {
                    "label": _concentration_label(condition_row),
                    "dataframe": mean_curve_frame,
                }
            )

        condition_summary_frame = pd.DataFrame(
            condition_summary_rows,
            columns=[
                "condition_id",
                "concentration_value",
                "concentration_sort_value",
                "concentration_unit",
                "concentration_label",
                "method",
                "n_measurements",
                "mean_of_file_min_current_a",
                "std_of_file_min_current_a",
                "mean_voltammogram_min_potential_v",
                "mean_voltammogram_min_current_a",
                "mean_min_current_a",
                "std_min_current_a",
                "mean_min_potential_v",
                "representative_mode",
                "mean_curve_csv_path",
                "mean_curve_png_path",
            ],
        )
        if not condition_summary_frame.empty:
            condition_summary_frame = condition_summary_frame.sort_values(
                ["concentration_sort_value", "condition_id"],
                kind="stable",
            )
        for column_name in ("slope", "intercept", "r_squared"):
            condition_summary_frame[column_name] = np.nan

        calibration_fits_frame = pd.DataFrame(
            columns=[
                "session_id",
                "representative_mode",
                "n_conditions",
                "slope",
                "intercept",
                "r_squared",
            ]
        )
        if (
            len(condition_summary_frame) >= 2
            and condition_summary_frame["concentration_value"].notna().sum() >= 2
            and condition_summary_frame["concentration_value"].nunique() >= 2
        ):
            fit_frame = condition_summary_frame.dropna(subset=["concentration_value", "mean_min_current_a"]).copy()
            x_values = fit_frame["concentration_value"].to_numpy(dtype=float)
            y_values = fit_frame["mean_min_current_a"].to_numpy(dtype=float)
            slope, intercept, r_squared = fit_linear_calibration(x_values, y_values)
            condition_summary_frame["slope"] = slope
            condition_summary_frame["intercept"] = intercept
            condition_summary_frame["r_squared"] = r_squared
            calibration_fits_frame = pd.DataFrame(
                [
                    {
                        "session_id": session_id,
                        "representative_mode": representative_mode,
                        "n_conditions": int(len(fit_frame)),
                        "slope": slope,
                        "intercept": intercept,
                        "r_squared": r_squared,
                    }
                ]
            )

        condition_summary_csv = self.csv_exporter.export_dataframe(
            condition_summary_frame.drop(columns=["concentration_sort_value"], errors="ignore"),
            processed_root / "condition_min_currents.csv",
        )
        calibration_fits_csv = self.csv_exporter.export_dataframe(
            calibration_fits_frame,
            processed_root / "calibration_fits.csv",
        )

        overlay_plot_path = mean_dir / "mean_voltammogram_all_concentrations.png"
        if self.config.plot_enabled and overlay_rows:
            save_overlay_curves_plot(
                overlay_rows,
                overlay_plot_path,
                f"{session_label} / Mean Voltammograms by Concentration",
            )

        calibration_plot_path = calibration_dir / "calibration_by_concentration.png"
        calibration_plot_created = False
        if self.config.plot_enabled and not condition_summary_frame.empty:
            save_calibration_plot(
                condition_summary_frame,
                calibration_plot_path,
                f"{session_label} / Calibration by Minimum Current",
            )
            calibration_plot_created = True

        outputs = {
            "file_inventory_csv": str(file_min_currents_csv),
            "file_inventory_detail_csv": str(file_inventory_csv),
            "condition_summary_csv": str(condition_summary_csv),
            "calibration_fits_csv": str(calibration_fits_csv),
            "individual_plot_dir": str(individual_dir),
            "mean_plot_dir": str(mean_dir),
        }
        if self.config.plot_enabled and overlay_rows:
            outputs["overlay_plot"] = str(overlay_plot_path)
        if calibration_plot_created:
            outputs["calibration_plot"] = str(calibration_plot_path)

        absolute_integral_long_frame = pd.DataFrame(
            absolute_integral_rows,
            columns=[
                "measurement_id",
                "condition_id",
                "concentration_value",
                "concentration_sort_value",
                "concentration_unit",
                "rep_no",
                "cycle_no",
                "series_label",
                "absolute_integral_a_v",
                "file_name",
                "raw_file_path",
            ],
        )
        if not absolute_integral_long_frame.empty:
            absolute_integral_long_frame = absolute_integral_long_frame.sort_values(
                ["concentration_sort_value", "condition_id", "rep_no", "cycle_no", "measurement_id"],
                kind="stable",
            )
        absolute_integral_long_csv = self.csv_exporter.export_dataframe(
            absolute_integral_long_frame.drop(columns=["concentration_sort_value"], errors="ignore"),
            processed_root / "file_absolute_integrals_long.csv",
        )
        absolute_integral_wide_frame = pd.DataFrame()
        if not absolute_integral_long_frame.empty:
            absolute_integral_wide_frame = (
                absolute_integral_long_frame.pivot_table(
                    index=[
                        "measurement_id",
                        "condition_id",
                        "concentration_value",
                        "concentration_unit",
                        "rep_no",
                        "file_name",
                        "raw_file_path",
                    ],
                    columns="series_label",
                    values="absolute_integral_a_v",
                    aggfunc="first",
                )
                .reset_index()
                .rename_axis(columns=None)
            )
            absolute_integral_wide_frame = absolute_integral_wide_frame.rename(
                columns={
                    column: f"{column}_absolute_integral_a_v"
                    for column in absolute_integral_wide_frame.columns
                    if column.startswith("scan") or column == "mean_of_cycles"
                }
            )
        absolute_integral_wide_csv = self.csv_exporter.export_dataframe(
            absolute_integral_wide_frame,
            processed_root / "file_absolute_integrals.csv",
        )

        absolute_integral_condition_frame = pd.DataFrame()
        absolute_integral_fit_frame = pd.DataFrame(
            columns=[
                "session_id",
                "series_label",
                "n_conditions",
                "slope",
                "intercept",
                "r_squared",
            ]
        )
        if not absolute_integral_long_frame.empty:
            absolute_integral_condition_frame = (
                absolute_integral_long_frame.groupby(
                    [
                        "series_label",
                        "cycle_no",
                        "condition_id",
                        "concentration_value",
                        "concentration_unit",
                    ],
                    as_index=False,
                )
                .agg(
                    n_measurements=("absolute_integral_a_v", "size"),
                    mean_absolute_integral_a_v=("absolute_integral_a_v", "mean"),
                    std_absolute_integral_a_v=("absolute_integral_a_v", lambda values: float(np.std(values, ddof=1)) if len(values) > 1 else 0.0),
                )
                .sort_values(["cycle_no", "concentration_value", "condition_id"], kind="stable")
            )
            fit_rows: list[dict[str, Any]] = []
            absolute_integral_condition_frame["slope"] = np.nan
            absolute_integral_condition_frame["intercept"] = np.nan
            absolute_integral_condition_frame["r_squared"] = np.nan
            for series_label, subset in absolute_integral_condition_frame.groupby("series_label", sort=True):
                fit_input = subset.dropna(subset=["concentration_value", "mean_absolute_integral_a_v"]).copy()
                if len(fit_input) < 2 or fit_input["concentration_value"].nunique() < 2:
                    continue
                slope, intercept, r_squared = fit_linear_calibration(
                    fit_input["concentration_value"].to_numpy(dtype=float),
                    fit_input["mean_absolute_integral_a_v"].to_numpy(dtype=float),
                )
                absolute_integral_condition_frame.loc[
                    absolute_integral_condition_frame["series_label"] == series_label,
                    ["slope", "intercept", "r_squared"],
                ] = (slope, intercept, r_squared)
                fit_rows.append(
                    {
                        "session_id": session_id,
                        "series_label": series_label,
                        "n_conditions": int(len(fit_input)),
                        "slope": slope,
                        "intercept": intercept,
                        "r_squared": r_squared,
                    }
                )
            absolute_integral_fit_frame = pd.DataFrame(fit_rows) if fit_rows else absolute_integral_fit_frame
        absolute_integral_condition_csv = self.csv_exporter.export_dataframe(
            absolute_integral_condition_frame,
            processed_root / "condition_absolute_integrals.csv",
        )
        absolute_integral_fit_csv = self.csv_exporter.export_dataframe(
            absolute_integral_fit_frame,
            processed_root / "absolute_integral_calibration_fits.csv",
        )
        absolute_integral_plot_path = calibration_dir / "absolute_integral_calibration_by_cycle.png"
        if self.config.plot_enabled and not absolute_integral_condition_frame.empty:
            save_metric_overlay_plot(
                absolute_integral_condition_frame,
                absolute_integral_plot_path,
                f"{session_label} / Absolute Integral by Scan",
                series_column="series_label",
                mean_column="mean_absolute_integral_a_v",
                std_column="std_absolute_integral_a_v",
                slope_column="slope",
                intercept_column="intercept",
                r_squared_column="r_squared",
                y_label="absolute_integral_a_v",
            )
            outputs["absolute_integral_plot"] = str(absolute_integral_plot_path)
        outputs["absolute_integral_file_csv"] = str(absolute_integral_wide_csv)
        outputs["absolute_integral_long_csv"] = str(absolute_integral_long_csv)
        outputs["absolute_integral_condition_csv"] = str(absolute_integral_condition_csv)
        outputs["absolute_integral_fit_csv"] = str(absolute_integral_fit_csv)

        cycle1_reference_long_frame = pd.DataFrame(
            cycle1_reference_rows,
            columns=[
                "measurement_id",
                "condition_id",
                "concentration_value",
                "concentration_sort_value",
                "concentration_unit",
                "rep_no",
                "cycle_no",
                "series_label",
                "reference_potential_v",
                "reference_current_a",
                "reference_branch",
                "file_name",
                "raw_file_path",
            ],
        )
        if not cycle1_reference_long_frame.empty:
            cycle1_reference_long_frame = cycle1_reference_long_frame.sort_values(
                ["concentration_sort_value", "condition_id", "rep_no", "cycle_no", "measurement_id"],
                kind="stable",
            )
        cycle1_reference_long_csv = self.csv_exporter.export_dataframe(
            cycle1_reference_long_frame.drop(columns=["concentration_sort_value"], errors="ignore"),
            processed_root / "file_cycle1_reference_currents_long.csv",
        )
        cycle1_reference_wide_frame = pd.DataFrame()
        if not cycle1_reference_long_frame.empty:
            cycle1_reference_wide_frame = (
                cycle1_reference_long_frame.pivot_table(
                    index=[
                        "measurement_id",
                        "condition_id",
                        "concentration_value",
                        "concentration_unit",
                        "rep_no",
                        "file_name",
                        "raw_file_path",
                        "reference_potential_v",
                        "reference_branch",
                    ],
                    columns="series_label",
                    values="reference_current_a",
                    aggfunc="first",
                )
                .reset_index()
                .rename_axis(columns=None)
            )
            cycle1_reference_wide_frame = cycle1_reference_wide_frame.rename(
                columns={
                    column: f"{column}_current_at_cycle1_ref_a"
                    for column in cycle1_reference_wide_frame.columns
                    if column.startswith("scan")
                }
            )
        cycle1_reference_wide_csv = self.csv_exporter.export_dataframe(
            cycle1_reference_wide_frame,
            processed_root / "file_cycle1_reference_currents.csv",
        )

        cycle1_reference_condition_frame = pd.DataFrame()
        cycle1_reference_fit_frame = pd.DataFrame(
            columns=[
                "session_id",
                "series_label",
                "n_conditions",
                "slope",
                "intercept",
                "r_squared",
            ]
        )
        if not cycle1_reference_long_frame.empty:
            cycle1_reference_condition_frame = (
                cycle1_reference_long_frame.groupby(
                    [
                        "series_label",
                        "cycle_no",
                        "condition_id",
                        "concentration_value",
                        "concentration_unit",
                    ],
                    as_index=False,
                )
                .agg(
                    n_measurements=("reference_current_a", "size"),
                    mean_reference_current_a=("reference_current_a", "mean"),
                    std_reference_current_a=("reference_current_a", lambda values: float(np.std(values, ddof=1)) if len(values) > 1 else 0.0),
                    mean_reference_potential_v=("reference_potential_v", "mean"),
                    std_reference_potential_v=("reference_potential_v", lambda values: float(np.std(values, ddof=1)) if len(values) > 1 else 0.0),
                )
                .sort_values(["cycle_no", "concentration_value", "condition_id"], kind="stable")
            )
            fit_rows = []
            cycle1_reference_condition_frame["slope"] = np.nan
            cycle1_reference_condition_frame["intercept"] = np.nan
            cycle1_reference_condition_frame["r_squared"] = np.nan
            for series_label, subset in cycle1_reference_condition_frame.groupby("series_label", sort=True):
                fit_input = subset.dropna(subset=["concentration_value", "mean_reference_current_a"]).copy()
                if len(fit_input) < 2 or fit_input["concentration_value"].nunique() < 2:
                    continue
                slope, intercept, r_squared = fit_linear_calibration(
                    fit_input["concentration_value"].to_numpy(dtype=float),
                    fit_input["mean_reference_current_a"].to_numpy(dtype=float),
                )
                cycle1_reference_condition_frame.loc[
                    cycle1_reference_condition_frame["series_label"] == series_label,
                    ["slope", "intercept", "r_squared"],
                ] = (slope, intercept, r_squared)
                fit_rows.append(
                    {
                        "session_id": session_id,
                        "series_label": series_label,
                        "n_conditions": int(len(fit_input)),
                        "slope": slope,
                        "intercept": intercept,
                        "r_squared": r_squared,
                    }
                )
            cycle1_reference_fit_frame = pd.DataFrame(fit_rows) if fit_rows else cycle1_reference_fit_frame
        cycle1_reference_condition_csv = self.csv_exporter.export_dataframe(
            cycle1_reference_condition_frame,
            processed_root / "condition_cycle1_reference_currents.csv",
        )
        cycle1_reference_fit_csv = self.csv_exporter.export_dataframe(
            cycle1_reference_fit_frame,
            processed_root / "cycle1_reference_calibration_fits.csv",
        )
        cycle1_reference_plot_path = calibration_dir / "cycle1_reference_calibration_by_cycle.png"
        if self.config.plot_enabled and not cycle1_reference_condition_frame.empty:
            save_metric_overlay_plot(
                cycle1_reference_condition_frame,
                cycle1_reference_plot_path,
                f"{session_label} / Scan 1 Reference Current by Scan",
                series_column="series_label",
                mean_column="mean_reference_current_a",
                std_column="std_reference_current_a",
                slope_column="slope",
                intercept_column="intercept",
                r_squared_column="r_squared",
                y_label="current_at_scan1_reference_a",
            )
            outputs["cycle1_reference_plot"] = str(cycle1_reference_plot_path)
        outputs["file_min_currents_csv"] = str(file_min_currents_csv)
        outputs["cycle1_reference_file_csv"] = str(cycle1_reference_wide_csv)
        outputs["cycle1_reference_long_csv"] = str(cycle1_reference_long_csv)
        outputs["cycle1_reference_condition_csv"] = str(cycle1_reference_condition_csv)
        outputs["cycle1_reference_fit_csv"] = str(cycle1_reference_fit_csv)
        return outputs

    def list_session_analysis_plot_images(self, session_id: str) -> list[dict[str, str]]:
        analysis_root = self.root_path / "data" / "sessions" / session_id
        png_paths = [
            path
            for directory in sorted(analysis_root.glob("analysis_output*"))
            if directory.is_dir()
            for path in directory.rglob("*.png")
            if path.is_file()
        ]
        if png_paths:
            def _extract_first(pattern: str, text: str) -> str | None:
                match = re.search(pattern, text)
                return match.group(1) if match else None

            def _describe_pressure(text: str) -> str | None:
                value = _extract_first(r"pressure_(\d+)", text)
                return f"高さ {value}" if value else None

            def _describe_concentration(text: str) -> str | None:
                value = _extract_first(r"conc_(\d+)", text)
                return f"{value} ppm" if value else None

            def _describe_individual_plot(file_name: str) -> str:
                pressure = _extract_first(r"pressure_(\d+)", file_name)
                concentration = _extract_first(r"conc_(\d+)", file_name)
                chip = _extract_first(r"chip_(\d+)", file_name)
                rep = _extract_first(r"rep_(\d+)", file_name)
                details = [
                    f"高さ {pressure}" if pressure else None,
                    f"{concentration} ppm" if concentration else None,
                    f"chip {chip}" if chip else None,
                    f"rep {rep}" if rep else None,
                ]
                detail_text = " / ".join(part for part in details if part)
                return f"個別ボルタモグラム ({detail_text})" if detail_text else "個別ボルタモグラム"

            def _describe_mean_plot(file_name: str) -> str:
                pressure = _describe_pressure(file_name)
                if file_name.endswith("_all_concentrations.png"):
                    details = [part for part in (pressure, "全濃度") if part]
                    return f"平均ボルタモグラム ({' / '.join(details)})"
                concentration = _describe_concentration(file_name)
                details = [part for part in (pressure, concentration) if part]
                return f"平均ボルタモグラム ({' / '.join(details)})" if details else "平均ボルタモグラム"

            def _describe_calibration_plot(file_name: str) -> str:
                exact_labels = {
                    "absolute_integral_calibration_overlay_scan1_to_5_by_pressure.png": "圧力別較正オーバーレイ (scan1-5平均)",
                    "absolute_loop_area_calibration_overlay_scan1_to_5_by_pressure.png": "圧力別較正オーバーレイ (scan1-5平均)",
                    "calibration_overlay_scan1_to_5_by_pressure.png": "圧力別較正オーバーレイ (scan1-5比較)",
                    "calibration_overlay_cycle2_to_5_at_cycle1_ref_by_pressure.png": "圧力別較正オーバーレイ (scan1最小電位基準)",
                    "calibration_by_pressure.png": "圧力別較正",
                }
                if file_name in exact_labels:
                    return exact_labels[file_name]
                pressure = _describe_pressure(file_name)
                if file_name.startswith("calibration_pressure_"):
                    return f"較正曲線 ({pressure})" if pressure else "較正曲線"
                if file_name.startswith("absolute_integral_calibration_pressure_"):
                    return f"較正曲線 ({pressure} / scan1-5平均)" if pressure else "較正曲線 (scan1-5平均)"
                if file_name.startswith("absolute_loop_area_calibration_pressure_"):
                    return f"較正曲線 ({pressure} / scan1-5平均)" if pressure else "較正曲線 (scan1-5平均)"
                if file_name.startswith("calibration_overlay_pressure_"):
                    return f"較正曲線 ({pressure} / scan1最小電位基準)" if pressure else "較正曲線 (scan1最小電位基準)"
                return f"較正 / {file_name}"

            def _format_plot_label(relative_path: Path) -> str:
                root_name = relative_path.parts[0] if relative_path.parts else ""
                nested_path = Path(*relative_path.parts[1:]) if len(relative_path.parts) > 1 else Path(relative_path.name)
                file_name = nested_path.name
                root_labels = {
                    "analysis_output": "5scan平均解析",
                    "analysis_output_absolute_integral": "絶対積分解析",
                    "analysis_output_absolute_loop_area": "絶対ループ面積解析",
                    "analysis_output_cycle1_reference_potential": "scan1最小電位基準解析",
                    "analysis_output_scan_comparison": "scan比較解析",
                }
                if root_name.startswith("analysis_output_scan") and root_name != "analysis_output_scan_comparison":
                    scan_no = root_name.replace("analysis_output_scan", "")
                    root_label = f"scan{scan_no}単独解析"
                else:
                    root_label = root_labels.get(root_name, root_name)
                if "individual_voltammograms" in nested_path.parts:
                    return f"{root_label} / {_describe_individual_plot(file_name)}"
                if "mean_voltammograms" in nested_path.parts:
                    return f"{root_label} / {_describe_mean_plot(file_name)}"
                if "calibration" in nested_path.parts:
                    return f"{root_label} / {_describe_calibration_plot(file_name)}"
                return f"{root_label} / {nested_path.as_posix()}"

            root_order = {
                "analysis_output_absolute_integral": 0,
                "analysis_output_absolute_loop_area": 1,
                "analysis_output_scan_comparison": 2,
                "analysis_output_cycle1_reference_potential": 3,
                "analysis_output": 4,
                "analysis_output_scan1": 5,
                "analysis_output_scan2": 6,
                "analysis_output_scan3": 7,
                "analysis_output_scan4": 8,
                "analysis_output_scan5": 9,
            }
            file_order = {
                "absolute_integral_calibration_overlay_scan1_to_5_by_pressure.png": 0,
                "absolute_loop_area_calibration_overlay_scan1_to_5_by_pressure.png": 1,
                "calibration_overlay_scan1_to_5_by_pressure.png": 2,
                "calibration_overlay_cycle2_to_5_at_cycle1_ref_by_pressure.png": 3,
                "calibration_by_pressure.png": 4,
            }

            def _sort_key(path: Path) -> tuple[int, int, str]:
                relative_path = path.relative_to(analysis_root)
                root_name = relative_path.parts[0] if relative_path.parts else ""
                return (
                    root_order.get(root_name, 99),
                    file_order.get(path.name, 99),
                    relative_path.as_posix(),
                )

            entries: list[dict[str, str]] = []
            for path in sorted(png_paths, key=_sort_key):
                relative_path = path.relative_to(analysis_root)
                entries.append(
                    {
                        "label": _format_plot_label(relative_path),
                        "path": str(path.resolve()),
                        "relative_path": relative_path.as_posix(),
                    }
                )
            return entries

        plots_root = self.root_path / "data" / "sessions" / session_id / "plots" / "session_plot_analysis"
        if not plots_root.exists():
            return []

        def _format_plot_label(relative_path: Path) -> str:
            category_name = relative_path.parts[0] if len(relative_path.parts) > 1 else ""
            file_name = relative_path.name
            if category_name == "calibration":
                label_map = {
                    "absolute_integral_calibration_by_cycle.png": "較正 / 絶対積分 / cycle別",
                    "cycle1_reference_calibration_by_cycle.png": "較正 / scan1基準電流 / cycle別",
                    "calibration_by_concentration.png": "較正 / 最小電流 / 濃度",
                }
                return label_map.get(file_name, f"較正 / {file_name}")
            if category_name == "mean_voltammograms":
                if file_name == "mean_voltammogram_all_concentrations.png":
                    return "平均 / 全濃度重ね描き"
                return f"平均 / {file_name}"
            if category_name == "individual_voltammograms":
                return f"個別 / {file_name}"
            return relative_path.as_posix()

        category_order = {
            "calibration": 0,
            "mean_voltammograms": 1,
            "individual_voltammograms": 2,
        }
        file_order = {
            "absolute_integral_calibration_by_cycle.png": 0,
            "cycle1_reference_calibration_by_cycle.png": 1,
            "calibration_by_concentration.png": 2,
            "mean_voltammogram_all_concentrations.png": 3,
        }

        def _sort_key(path: Path) -> tuple[int, int, str]:
            relative_path = path.relative_to(plots_root)
            category_name = relative_path.parts[0] if len(relative_path.parts) > 1 else ""
            return (
                category_order.get(category_name, 9),
                file_order.get(path.name, 9),
                relative_path.as_posix(),
            )

        entries: list[dict[str, str]] = []
        for path in sorted((item for item in plots_root.rglob("*.png") if item.is_file()), key=_sort_key):
            relative_path = path.relative_to(plots_root)
            entries.append(
                {
                    "label": _format_plot_label(relative_path),
                    "path": str(path.resolve()),
                    "relative_path": relative_path.as_posix(),
                }
            )
        return entries

    def get_session_analysis_plot_summary(self, session_id: str) -> str:
        analysis_root = self.root_path / "data" / "sessions" / session_id
        main_root = analysis_root / "analysis_output"
        if main_root.exists():
            def _read_csv_path(path: Path) -> pd.DataFrame:
                if not path.exists():
                    return pd.DataFrame()
                try:
                    return pd.read_csv(path)
                except Exception:
                    return pd.DataFrame()

            summary_parts: list[str] = []
            inventory_frame = _read_csv_path(main_root / "file_inventory.csv")
            if not inventory_frame.empty and "cycle_count" in inventory_frame.columns:
                summary_parts.append(f"測定 {len(inventory_frame)}件")
                if "pressure" in inventory_frame.columns:
                    pressures = sorted(int(value) for value in inventory_frame["pressure"].dropna().unique())
                    if pressures:
                        summary_parts.append("pressure " + ", ".join(str(value) for value in pressures))
                cycle_counts = sorted(int(value) for value in inventory_frame["cycle_count"].dropna().unique())
                if cycle_counts:
                    if len(cycle_counts) == 1:
                        summary_parts.append(f"cycle {cycle_counts[0]}")
                    else:
                        summary_parts.append(f"cycle {cycle_counts[0]}-{cycle_counts[-1]}")

            scan_fit_frame = _read_csv_path(analysis_root / "analysis_output_scan_comparison" / "scan1_to_5_calibration_fits.csv")
            if not scan_fit_frame.empty and "scan" in scan_fit_frame.columns:
                scans = sorted(int(value) for value in scan_fit_frame["scan"].dropna().unique())
                if scans:
                    summary_parts.append("scan比較 " + ", ".join(f"scan{value}" for value in scans))

            absolute_integral_frame = _read_csv_path(
                analysis_root / "analysis_output_absolute_integral" / "condition_absolute_integrals_scan1_to_5_mean.csv"
            )
            if not absolute_integral_frame.empty:
                summary_parts.append("絶対積分 scan1-5平均")

            absolute_loop_area_frame = _read_csv_path(
                analysis_root / "analysis_output_absolute_loop_area" / "condition_absolute_loop_areas_scan1_to_5_mean.csv"
            )
            if not absolute_loop_area_frame.empty:
                summary_parts.append("絶対ループ面積 scan1-5平均")

            cycle1_reference_frame = _read_csv_path(
                analysis_root / "analysis_output_cycle1_reference_potential" / "condition_cycle1_reference_currents.csv"
            )
            if not cycle1_reference_frame.empty and "cycle" in cycle1_reference_frame.columns:
                cycles = sorted(int(value) for value in cycle1_reference_frame["cycle"].dropna().unique())
                if cycles:
                    summary_parts.append("scan1基準 " + ", ".join(f"scan{value}" for value in cycles))

            return " / ".join(summary_parts) if summary_parts else "解析プロットはまだ出力されていません。"

        processed_root = self.root_path / "data" / "sessions" / session_id / "processed" / "session_plot_analysis"
        if not processed_root.exists():
            return "解析プロットはまだ出力されていません。"

        def _read_csv(name: str) -> pd.DataFrame:
            path = processed_root / name
            if not path.exists():
                return pd.DataFrame()
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.DataFrame()

        def _format_series(series_values: list[str]) -> str:
            ordered = sorted(
                {value for value in series_values if value},
                key=lambda value: (value != "mean_of_cycles", value),
            )
            normalized = ["mean" if value == "mean_of_cycles" else value for value in ordered]
            return ", ".join(normalized) if normalized else "-"

        summary_parts: list[str] = []
        inventory_frame = _read_csv("file_inventory.csv")
        if not inventory_frame.empty and "cycle_count" in inventory_frame.columns:
            cycle_counts = sorted(int(value) for value in inventory_frame["cycle_count"].dropna().unique())
            if cycle_counts:
                if len(cycle_counts) == 1:
                    cycle_text = str(cycle_counts[0])
                else:
                    cycle_text = f"{cycle_counts[0]}-{cycle_counts[-1]}"
                summary_parts.append(f"測定 {len(inventory_frame)}件 / cycle {cycle_text}")

        absolute_integral_frame = _read_csv("file_absolute_integrals_long.csv")
        if not absolute_integral_frame.empty and "series_label" in absolute_integral_frame.columns:
            summary_parts.append(
                f"絶対積分 {_format_series(absolute_integral_frame['series_label'].astype(str).tolist())}"
            )

        cycle1_reference_frame = _read_csv("file_cycle1_reference_currents_long.csv")
        if not cycle1_reference_frame.empty and "series_label" in cycle1_reference_frame.columns:
            summary_parts.append(
                f"scan1基準 {_format_series(cycle1_reference_frame['series_label'].astype(str).tolist())}"
            )

        return " / ".join(summary_parts) if summary_parts else "解析プロットはまだ出力されていません。"

    def export_session_bundle(self, session_id: str) -> dict[str, str]:
        bundle = self.repository.get_session_bundle(session_id)
        directories = session_output_directories(self.root_path, session_id)
        measurements_frame = self.repository.database.query_frame(
            f"""
            SELECT *
            FROM measurements
            WHERE session_id = ? AND COALESCE(is_deleted, 0) = 0
            ORDER BY rep_no ASC
            """,
            (session_id,),
        )
        aggregates_frame = self.repository.database.query_frame(
            "SELECT * FROM aggregated_results WHERE session_id = ? ORDER BY concentration_value ASC",
            (session_id,),
        )
        measurement_csv = self.csv_exporter.export_dataframe(
            measurements_frame,
            directories["exports"] / f"{session_id}_measurements.csv",
        )
        aggregate_csv = self.csv_exporter.export_dataframe(
            aggregates_frame,
            directories["exports"] / f"{session_id}_aggregated_results.csv",
        )
        workbook_path = self.excel_exporter.export_frames(
            self.repository.get_export_frames(session_id),
            directories["exports"] / f"{session_id}_session_detail.xlsx",
        )
        markdown_path = self.markdown_reporter.export_session_report(
            session_row=bundle["session"],
            conditions=pd.DataFrame(bundle["conditions"]),
            measurements=measurements_frame,
            aggregates=aggregates_frame,
            output_path=directories["report"] / f"{session_id}_report.md",
        )
        return {
            "measurement_csv": str(measurement_csv),
            "aggregate_csv": str(aggregate_csv),
            "workbook": str(workbook_path),
            "markdown": str(markdown_path),
        }

    def export_cross_report(self, filters: dict[str, Any]) -> str:
        frame = self.repository.search_cross_measurements(filters)
        output_path = self.root_path / "data" / "cross_reports" / f"cross_report_{today_string()}.csv"
        self.csv_exporter.export_dataframe(frame, output_path)
        return str(output_path)

    def get_session_detail(self, session_id: str) -> dict[str, Any]:
        bundle = self.repository.get_session_bundle(session_id)
        bundle["condition_warnings"] = self.get_condition_warnings(session_id)
        return bundle

    def _is_iviumsoft_running(self) -> bool:
        exe_name = Path(str(self.config.iviumsoft_exe_path or "IviumSoft.exe")).name or "IviumSoft.exe"
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"IMAGENAME eq {exe_name}"],
                capture_output=True,
                text=True,
                check=False,
            )
        except Exception:
            return False
        return exe_name.lower() in result.stdout.lower()

    def _ensure_iviumsoft_ready(self) -> None:
        if self._is_iviumsoft_running():
            return
        self.launch_iviumsoft()
        time.sleep(2.0)

    def _resolve_controlled_result_file(self, started_epoch: float, db_file_name: str | None) -> Path | None:
        watch_folder = self.resolve_watch_folder()
        ids_candidates: list[Path] = []
        if watch_folder.exists():
            ids_candidates = [
                path
                for path in watch_folder.rglob("*.ids")
                if path.is_file() and path.stat().st_mtime >= started_epoch - 2
            ]
            ids_candidates.sort(key=lambda item: item.stat().st_mtime, reverse=True)
        if ids_candidates:
            return ids_candidates[0]

        db_path = resolve_ivium_db_file_path(self.config.iviumsoft_exe_path, db_file_name)
        if db_path and db_path.exists() and db_path.stat().st_mtime >= started_epoch - 10:
            return db_path
        return None

    def _load_ivium_batch_run_target(self, batch_item_id: str | None, session_id: str | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        if batch_item_id:
            batch_item = self.repository.get_active_batch_item(batch_item_id)
            if not batch_item:
                raise ValueError("指定したバッチ項目が見つかりません。")
        else:
            batch_item = self.repository.get_next_waiting_batch_item(session_id)
            if not batch_item:
                raise ValueError("実行可能な waiting バッチ項目がありません。")
        if str(batch_item.get("planned_status") or "") != PlannedStatus.WAITING.value:
            raise ValueError("Ivium 実行できるのは waiting 状態のバッチ項目です。")
        if str(batch_item.get("assigned_measurement_id") or "").strip():
            raise ValueError("このバッチ項目にはすでに測定が紐付いています。")
        session_row = self.repository.get_record("sessions", str(batch_item["session_id"]))
        condition_row = self.repository.get_record("conditions", str(batch_item["condition_id"]))
        if not session_row:
            raise ValueError("バッチ項目のセッションが見つかりません。")
        if not condition_row:
            raise ValueError("バッチ項目の条件が見つかりません。")
        return batch_item, session_row, condition_row

    def get_ivium_batch_run_preview(
        self,
        batch_item_id: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        batch_item, session_row, condition_row = self._load_ivium_batch_run_target(batch_item_id, session_id)
        concentration_value = condition_row.get("concentration_value")
        concentration_unit = str(condition_row.get("concentration_unit") or "").strip()
        concentration_text = (
            f"{concentration_value:g} {concentration_unit}".strip()
            if isinstance(concentration_value, float)
            else f"{str(concentration_value or '').strip()} {concentration_unit}".strip()
        )
        return {
            "batch_item_id": str(batch_item["batch_item_id"]),
            "session_id": str(batch_item["session_id"]),
            "condition_id": str(batch_item["condition_id"]),
            "planned_order": int(batch_item.get("planned_order") or 0),
            "rep_no": int(batch_item.get("rep_no") or 0),
            "analyte": str(session_row.get("analyte") or ""),
            "concentration_text": concentration_text or "-",
            "concentration_value": condition_row.get("concentration_value"),
            "concentration_unit": concentration_unit,
            "method": str(condition_row.get("method") or ""),
            "ivium_method_name": str(condition_row.get("ivium_method_name") or ""),
        }

    def run_ivium_batch_item(self, batch_item_id: str | None = None, session_id: str | None = None) -> str:
        with self._ivium_run_lock:
            if self._ivium_run_thread and self._ivium_run_thread.is_alive():
                raise ValueError("Ivium 実行はすでに進行中です。")

        batch_item, session_row, condition_row = self._load_ivium_batch_run_target(batch_item_id, session_id)
        template_path = resolve_ivium_method_template_path(
            self.config.ivium_method_template_path,
            self.config.iviumsoft_exe_path,
        )
        prepared_method = prepare_ivium_method_file(
            template_path=template_path,
            batch_item=batch_item,
            condition_row=condition_row,
            session_row=session_row,
        )
        self.repository.update_batch_item_status(
            str(batch_item["batch_item_id"]),
            PlannedStatus.RUNNING.value,
            assigned_measurement_id=None,
        )

        condition_label = (
            f"{condition_row.get('condition_id', '')} / "
            f"{condition_row.get('concentration_value', '')} {condition_row.get('concentration_unit', '')} / "
            f"{condition_row.get('method', '')}"
        ).strip()
        self._ivium_stop_event.clear()
        self._resume_watcher_after_run = self.watcher is not None and self.watcher_callback is not None
        if self._resume_watcher_after_run:
            self.stop_watcher()
        self._set_ivium_run_state(
            status="preparing",
            batch_item_id=str(batch_item["batch_item_id"]),
            session_id=str(batch_item["session_id"]),
            condition_id=str(batch_item["condition_id"]),
            condition_label=condition_label,
            rep_no=int(batch_item.get("rep_no") or 0),
            method_name=prepared_method.method_name,
            method_file_path=prepared_method.method_file_path,
            source_file_path="",
            source_file_type="",
            imported_measurement_id="",
            message="Ivium 実行を準備しています。",
            started_at=now_iso(),
            completed_at="",
            device_status_code=None,
            points_collected=0,
            device_serial=str(self.config.ivium_device_serial or ""),
            log_lines=[],
        )
        self._append_ivium_log("Ivium 実行を準備しています。")
        worker = threading.Thread(
            target=self._execute_ivium_batch_item,
            args=(batch_item, session_row, condition_row, prepared_method),
            daemon=True,
        )
        with self._ivium_run_lock:
            self._ivium_run_thread = worker
        worker.start()
        return f"Ivium 実行を開始しました: {batch_item['batch_item_id']}"

    def run_next_waiting_ivium_batch_item(self, session_id: str | None = None) -> str:
        selected_session_id = str(session_id or "").strip() or None
        return self.run_ivium_batch_item(batch_item_id=None, session_id=selected_session_id)

    def _execute_ivium_batch_item(
        self,
        batch_item: dict[str, Any],
        session_row: dict[str, Any],
        condition_row: dict[str, Any],
        prepared_method,
    ) -> None:
        batch_item_id = str(batch_item["batch_item_id"])
        driver: IviumRemoteDriver | None = None
        requested_serial = str(self.config.ivium_device_serial or "").strip()
        started_epoch = time.time()
        seen_activity = False
        completion_detected_at: float | None = None
        last_collect_error = ""
        try:
            self._append_ivium_log("IviumSoft の起動状態を確認しています。")
            self._ensure_iviumsoft_ready()
            driver = IviumRemoteDriver(resolve_ivium_driver_path(self.config.iviumsoft_exe_path))
            with self._ivium_run_lock:
                self._ivium_driver = driver
            driver.open()
            driver.version_check()
            if requested_serial:
                driver.select_serial(requested_serial)
            driver.connect(1)
            actual_serial = driver.read_serial() or requested_serial
            self._set_ivium_run_state(status="connected", device_serial=actual_serial)
            self._append_ivium_log(f"Ivium device に接続しました: {actual_serial or 'unknown'}")
            driver.start_method(prepared_method.method_file_path)
            self._set_ivium_run_state(status="running")
            self._append_ivium_log(f"測定開始: {prepared_method.method_name}")

            timeout_at = time.monotonic() + max(float(self.config.ivium_result_timeout_sec), 30.0)
            poll_interval = max(float(self.config.ivium_poll_interval_sec), 0.2)

            while True:
                if self._ivium_stop_event.is_set():
                    try:
                        driver.abort()
                    except Exception:
                        LOGGER.exception("Ivium abort failed")
                    raise RuntimeError("測定を中止しました。")

                device_status = driver.get_device_status()
                point_count = max(driver.get_n_datapoints(), 0)
                db_file_name = driver.get_db_file_name()
                seen_activity = seen_activity or point_count > 0 or device_status in {2, 3, 4, 5}
                if seen_activity and device_status in {0, 1}:
                    completion_detected_at = completion_detected_at or time.monotonic()
                else:
                    completion_detected_at = None

                result_file = self._resolve_controlled_result_file(started_epoch, db_file_name)
                source_file_path = str(result_file) if result_file else ""
                source_file_type = (
                    "idf_sqlite"
                    if source_file_path.lower().endswith(".idf.sqlite")
                    else ("ids" if source_file_path.lower().endswith(".ids") else "")
                )
                self._set_ivium_run_state(
                    status="collecting" if completion_detected_at is not None else "running",
                    device_status_code=device_status,
                    points_collected=point_count,
                    source_file_path=source_file_path,
                    source_file_type=source_file_type,
                )

                if result_file and completion_detected_at is not None and time.monotonic() - completion_detected_at >= 1.0:
                    try:
                        measurement_id = self.import_measurement_file(result_file, batch_item_id=batch_item_id)
                    except Exception as error:
                        current_error = str(error)
                        if current_error != last_collect_error:
                            last_collect_error = current_error
                            self._append_ivium_log(f"結果ファイルの読込待機中: {current_error}")
                    else:
                        self._set_ivium_run_state(
                            status="completed",
                            source_file_path=str(result_file),
                            source_file_type=source_file_type,
                            imported_measurement_id=measurement_id,
                            completed_at=now_iso(),
                        )
                        self._append_ivium_log(f"測定の取り込みが完了しました: {measurement_id}")
                        return

                if time.monotonic() >= timeout_at:
                    raise TimeoutError("測定結果ファイルの待機がタイムアウトしました。")
                time.sleep(poll_interval)
        except Exception as error:
            self.repository.update_batch_item_status(
                batch_item_id,
                PlannedStatus.WAITING.value,
                assigned_measurement_id=None,
            )
            self._append_batch_item_note(batch_item_id, f"Ivium 実行失敗: {error}")
            self.repository.log_error(
                str(error),
                f"ivium_run:{batch_item_id}",
                session_id=str(session_row["session_id"]),
            )
            self._set_ivium_run_state(status="failed", completed_at=now_iso())
            self._append_ivium_log(f"実行失敗: {error}")
        finally:
            if driver is not None:
                try:
                    driver.close()
                except Exception:
                    LOGGER.exception("Ivium close failed")
            with self._ivium_run_lock:
                self._ivium_driver = None
                self._ivium_run_thread = None
                resume_watcher = self._resume_watcher_after_run
                self._resume_watcher_after_run = False
            if resume_watcher and self.watcher_callback:
                try:
                    self.start_watcher(self.watcher_callback)
                except Exception as error:
                    LOGGER.exception("Failed to resume watcher")
                    self.repository.log_error(str(error), "watcher_resume")

    def abort_ivium_run(self) -> str:
        with self._ivium_run_lock:
            run_thread = self._ivium_run_thread
            driver = self._ivium_driver
        if not run_thread or not run_thread.is_alive():
            raise ValueError("進行中の Ivium 実行はありません。")
        self._ivium_stop_event.set()
        if driver is not None:
            try:
                driver.abort()
            except Exception:
                LOGGER.exception("Ivium abort request failed")
        self._append_ivium_log("中止要求を送信しました。")
        return "Ivium へ中止要求を送信しました。"

    def local_config_path(self) -> Path:
        return self.root_path / "config" / "local_config.json"

    def resolve_watch_folder(self) -> Path:
        watch_folder = Path(str(self.config.watch_folder).strip())
        if watch_folder.is_absolute():
            return watch_folder.resolve()
        return (self.root_path / watch_folder).resolve()

    def set_watch_folder(self, folder: str | Path) -> Path:
        resolved = Path(folder).expanduser().resolve()
        resolved.mkdir(parents=True, exist_ok=True)
        try:
            self.config.watch_folder = str(resolved.relative_to(self.root_path))
        except ValueError:
            self.config.watch_folder = str(resolved)
        self.config.save_local(self.local_config_path())
        return resolved

    def open_watch_folder(self) -> str:
        watch_folder = self.resolve_watch_folder()
        watch_folder.mkdir(parents=True, exist_ok=True)
        subprocess.Popen(["explorer", str(watch_folder)])
        return f"監視フォルダを開きました: {watch_folder}"

    def launch_iviumsoft(self) -> str:
        configured_path = str(self.config.iviumsoft_exe_path).strip()
        if not configured_path:
            raise ValueError("IviumSoft.exe のパスが未設定です。config/local_config.json を確認してください。")

        exe_path = Path(configured_path)
        if not exe_path.is_absolute():
            exe_path = (self.root_path / exe_path).resolve()

        if not exe_path.exists():
            raise FileNotFoundError(f"IviumSoft.exe が見つかりません: {exe_path}")
        if exe_path.suffix.lower() != ".exe":
            raise ValueError(f"実行ファイルを指定してください: {exe_path}")

        subprocess.Popen(
            [str(exe_path)],
            cwd=str(exe_path.parent),
        )
        return f"IviumSoft を起動しました: {exe_path}"

    def start_watcher(self, callback: Callable[[str], None]) -> None:
        self.stop_watcher()
        self.watcher_callback = callback
        watch_folder = self.resolve_watch_folder()

        def _on_created(path: Path) -> None:
            message = f"検知: {path.name}"
            self._wait_for_stable_file(path)
            for attempt in range(5):
                try:
                    measurement_id = self.import_measurement_file(path)
                    message = f"取り込み成功: {path.name} -> {measurement_id}"
                    break
                except Exception as error:
                    if not self._should_retry_measurement_import(error) or attempt == 4:
                        LOGGER.exception("measurement import failed: %s", path)
                        self.repository.log_error(str(error), f"watch_import:{path}")
                        message = f"取り込み失敗: {path.name} / {error}"
                        break
                    time.sleep(1.0)
            if self.watcher_callback:
                self.watcher_callback(message)

        self.watcher = IdsWatchCoordinator(watch_folder, _on_created, self.config.target_extensions)
        self.watcher.start()
        callback(f"監視開始: {watch_folder}")

    def stop_watcher(self) -> None:
        if self.watcher:
            self.watcher.stop()
            if self.watcher_callback:
                self.watcher_callback("監視停止")
        self.watcher = None

    def shutdown(self) -> None:
        self._resume_watcher_after_run = False
        self.watcher_callback = None
        self.stop_watcher()
        try:
            self.abort_ivium_run()
        except Exception:
            return
