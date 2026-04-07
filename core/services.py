from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from analysis.aggregation import aggregate_condition_rows
from analysis.cv_analysis import analyze_cv_curve
from analysis.dpv_analysis import analyze_dpv_curve
from analysis.mean_voltammogram import generate_mean_curve
from analysis.plotting import save_mean_curve_plot, save_measurement_plot
from core.batch_planner import generate_batch_plan_items
from core.config import AppConfig
from core.linking import BatchLinker, IdsWatchCoordinator
from core.models import PlannedStatus
from core.quality import derive_auto_quality, resolve_final_quality
from core.repositories import ElectrochemRepository
from core.validators import require_any, require_fields
from export.csv_exporter import CSVExporter
from export.excel_exporter import ExcelExporter
from export.markdown_reporter import MarkdownReporter
from parsers.ivium_ids_parser import parse_ids_file
from parsers.measurement_conditions_parser import build_measurement_conditions
from utils.date_utils import now_iso, today_string
from utils.file_utils import ensure_directories, generate_id, session_output_directories


LOGGER = logging.getLogger(__name__)


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

    def home_snapshot(self) -> dict[str, Any]:
        return self.repository.get_home_snapshot()

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
        require_fields(payload, ["mip_id"])
        require_any(payload, ["cp_preparation_date", "coating_date"])
        mip_usage_id = generate_id("MUSE")
        self.repository.insert_record("mip_usage_records", {"mip_usage_id": mip_usage_id, **payload})
        return mip_usage_id

    def update_mip_usage(self, mip_usage_id: str, payload: dict[str, Any]) -> None:
        require_fields(payload, ["mip_id"])
        require_any(payload, ["cp_preparation_date", "coating_date"])
        self.repository.update_record("mip_usage_records", mip_usage_id, payload)

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
        condition_id = generate_id("COND")
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
        new_id = self.repository.duplicate_record("conditions", condition_id, "COND")
        self.repository.update_record(
            "conditions",
            new_id,
            {
                "actual_replicates": 0,
                "n_valid": 0,
                "n_invalid": 0,
                "cv_percent": None,
                "condition_status": "pending",
            },
        )
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

    def _analyze_parsed_data(self, dataframe: pd.DataFrame, method: str):
        normalized_method = method.lower()
        if "dpv" in normalized_method:
            return "DPV", analyze_dpv_curve(dataframe, baseline_correction=self.config.baseline_correction)
        return "CV", analyze_cv_curve(dataframe, representative_cycle_rule=self.config.representative_cycle_rule)

    def import_ids_file(self, file_path: str | Path, session_id: str | None = None) -> str:
        parsed = parse_ids_file(file_path)
        decision = self.batch_linker.choose_target(session_id)
        session_row = self.repository.get_record("sessions", decision.session_id)
        if not session_row:
            raise ValueError("紐付け先セッションが見つかりません。")

        method_label, analysis_result = self._analyze_parsed_data(
            parsed.data,
            str(parsed.metadata.get("Method", session_row.get("method_default", ""))),
        )
        auto_quality = derive_auto_quality(status=str(parsed.metadata.get("endcondition")), analysis_quality=analysis_result.quality_flag)
        measurement_payload = self.batch_linker.build_measurement_payload(
            session_id=decision.session_id,
            condition_id=decision.condition_id,
            mip_usage_id=session_row.get("mip_usage_id"),
            raw_file_path=str(Path(file_path).resolve()),
            batch_item_id=decision.batch_item_id,
            rep_no=decision.rep_no,
            measured_at=str(parsed.metadata.get("starttime_iso", now_iso())),
            auto_quality_flag=auto_quality.value,
        )
        measurement_id = str(measurement_payload["measurement_id"])
        self.repository.insert_record("measurements", measurement_payload)
        self.repository.update_batch_item_status(
            decision.batch_item_id,
            PlannedStatus.COMPLETED.value,
            assigned_measurement_id=measurement_id,
        )

        measurement_condition_payload = build_measurement_conditions(parsed.metadata, parsed.raw_header_text)
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
                "condition_id": decision.condition_id,
                "session_id": decision.session_id,
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
            directories = session_output_directories(self.root_path, decision.session_id)
            save_measurement_plot(
                parsed.data,
                directories["plots"] / f"{measurement_id}.png",
                f"{decision.session_id} / {measurement_id}",
            )
        self.repository.refresh_condition_stats(condition_id=decision.condition_id)
        self.aggregate_session(decision.session_id)
        if self.config.mean_voltammogram_enabled:
            self.generate_mean_voltammograms(decision.session_id, decision.condition_id)
        return measurement_id

    def reanalyze_measurement(self, measurement_id: str) -> None:
        measurement = self.repository.get_record("measurements", measurement_id)
        if not measurement or not measurement.get("raw_file_path"):
            raise ValueError("再解析対象の raw_file_path が見つかりません。")
        parsed = parse_ids_file(str(measurement["raw_file_path"]))
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
        for condition in self.list_conditions(session_id):
            measurement_rows = self.repository.list_active_measurements(session_id)
            measurement_rows = [row for row in measurement_rows if row["condition_id"] == condition["condition_id"]]
            analysis_rows = self.repository.database.fetch_all(
                """
                SELECT *
                FROM analysis_results
                WHERE condition_id = ?
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
                parsed = parse_ids_file(str(raw_file_path))
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

    def export_session_bundle(self, session_id: str) -> dict[str, str]:
        bundle = self.repository.get_session_bundle(session_id)
        directories = session_output_directories(self.root_path, session_id)
        measurements_frame = self.repository.database.query_frame(
            """
            SELECT *
            FROM measurements
            WHERE session_id = ?
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
        return self.repository.get_session_bundle(session_id)

    def start_watcher(self, callback: Callable[[str], None]) -> None:
        self.stop_watcher()
        self.watcher_callback = callback
        watch_folder = (self.root_path / self.config.watch_folder).resolve()

        def _on_created(path: Path) -> None:
            message = f"検知: {path.name}"
            try:
                measurement_id = self.import_ids_file(path)
                message = f"取り込み成功: {path.name} -> {measurement_id}"
            except Exception as error:
                LOGGER.exception("ids import failed: %s", path)
                self.repository.log_error(str(error), f"watch_import:{path}")
                message = f"取り込み失敗: {path.name} / {error}"
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
