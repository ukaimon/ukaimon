from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from core.database import DatabaseManager
from core.models import ConditionState, PlannedStatus, QualityFlag
from utils.date_utils import now_iso
from utils.file_utils import generate_id, normalize_condition_token


LOGGER = logging.getLogger(__name__)

TABLE_ID_COLUMNS = {
    "mip_records": "mip_id",
    "mip_usage_records": "mip_usage_id",
    "sessions": "session_id",
    "conditions": "condition_id",
    "batch_plan_items": "batch_item_id",
    "measurements": "measurement_id",
    "measurement_conditions": "condition_param_id",
    "analysis_results": "result_id",
    "cycle_results": "cycle_result_id",
    "aggregated_results": "aggregate_id",
    "mean_voltammogram_records": "mean_voltammogram_id",
    "error_logs": "error_id",
}

TIMESTAMP_COLUMNS = {
    "mip_records": ("created_at", "updated_at"),
    "mip_usage_records": ("created_at", "updated_at"),
    "sessions": ("created_at", "updated_at"),
    "conditions": ("created_at", "updated_at"),
    "batch_plan_items": ("created_at", "updated_at"),
    "measurements": ("created_at", "updated_at"),
    "measurement_conditions": ("created_at", "updated_at"),
    "analysis_results": ("created_at",),
    "aggregated_results": ("created_at",),
    "mean_voltammogram_records": ("created_at",),
    "error_logs": ("created_at",),
}

SOFT_DELETABLE_TABLES = {
    "mip_records",
    "mip_usage_records",
    "sessions",
    "conditions",
    "batch_plan_items",
    "measurements",
}


class ElectrochemRepository:
    def __init__(self, database: DatabaseManager) -> None:
        self.database = database

    def initialize(self) -> None:
        self.database.initialize()

    def normalize_legacy_condition_ids(self) -> int:
        renamed_count = 0
        condition_rows = self.database.fetch_all("SELECT * FROM conditions ORDER BY created_at ASC, rowid ASC")
        with self.database.connect() as connection:
            for row in condition_rows:
                current_id = str(row["condition_id"])
                expected_id = self._build_normalized_condition_id(current_id, row)
                if not expected_id or expected_id == current_id:
                    continue
                exists = connection.execute(
                    "SELECT 1 FROM conditions WHERE condition_id = ?",
                    (expected_id,),
                ).fetchone()
                if exists:
                    LOGGER.warning("condition id migration skipped because target already exists: %s -> %s", current_id, expected_id)
                    continue
                cloned = dict(row)
                cloned["condition_id"] = expected_id
                columns = list(cloned.keys())
                placeholders = ", ".join("?" for _ in columns)
                connection.execute(
                    f"INSERT INTO conditions ({', '.join(columns)}) VALUES ({placeholders})",
                    tuple(cloned[column_name] for column_name in columns),
                )
                for table_name in ("batch_plan_items", "measurements", "analysis_results", "aggregated_results", "mean_voltammogram_records"):
                    connection.execute(
                        f"UPDATE {table_name} SET condition_id = ? WHERE condition_id = ?",
                        (expected_id, current_id),
                    )
                connection.execute("DELETE FROM conditions WHERE condition_id = ?", (current_id,))
                renamed_count += 1
            connection.commit()
        return renamed_count

    def _build_normalized_condition_id(self, current_id: str, row: dict[str, Any]) -> str | None:
        parts = current_id.split("-")
        if len(parts) < 5 or parts[0] != "COND":
            return None
        date_part = parts[1]
        serial = parts[-2]
        suffix = parts[-1]
        condition_token = normalize_condition_token(row.get("concentration_value"), row.get("concentration_unit"))
        return f"COND-{date_part}-{condition_token}-{serial}-{suffix}"

    def _supports_soft_delete(self, table_name: str) -> bool:
        return table_name in SOFT_DELETABLE_TABLES

    def _active_clause(self, table_name: str, alias: str | None = None) -> str:
        if not self._supports_soft_delete(table_name):
            return "1 = 1"
        prefix = f"{alias}." if alias else ""
        return f"COALESCE({prefix}is_deleted, 0) = 0"

    def _with_timestamps(self, table_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = now_iso()
        result = dict(payload)
        for column_name in TIMESTAMP_COLUMNS.get(table_name, ()):
            result.setdefault(column_name, now)
        if self._supports_soft_delete(table_name):
            result.setdefault("is_deleted", 0)
            result.setdefault("deleted_at", None)
        return result

    def insert_record(self, table_name: str, payload: dict[str, Any]) -> None:
        self.database.insert(table_name, self._with_timestamps(table_name, payload))

    def update_record(self, table_name: str, record_id: str, payload: dict[str, Any]) -> None:
        id_column = TABLE_ID_COLUMNS[table_name]
        data = dict(payload)
        if "updated_at" in TIMESTAMP_COLUMNS.get(table_name, ()):
            data["updated_at"] = now_iso()
        self.database.update(table_name, data, f"{id_column} = ?", (record_id,))

    def mark_deleted(self, table_name: str, record_id: str) -> None:
        if not self._supports_soft_delete(table_name):
            raise ValueError(f"{table_name} は論理削除に対応していません。")
        self.update_record(
            table_name,
            record_id,
            {
                "is_deleted": 1,
                "deleted_at": now_iso(),
            },
        )

    def bulk_mark_deleted(self, table_name: str, where_clause: str, params: tuple[Any, ...]) -> None:
        if not self._supports_soft_delete(table_name):
            raise ValueError(f"{table_name} は論理削除に対応していません。")
        now = now_iso()
        self.database.execute(
            f"""
            UPDATE {table_name}
            SET is_deleted = 1,
                deleted_at = COALESCE(deleted_at, ?),
                updated_at = ?
            WHERE {where_clause} AND COALESCE(is_deleted, 0) = 0
            """,
            (now, now, *params),
        )

    def delete_record_physical(self, table_name: str, record_id: str) -> None:
        self.database.delete(table_name, f"{TABLE_ID_COLUMNS[table_name]} = ?", (record_id,))

    def get_record(self, table_name: str, record_id: str) -> dict[str, Any] | None:
        id_column = TABLE_ID_COLUMNS[table_name]
        return self.database.fetch_one(
            f"SELECT * FROM {table_name} WHERE {id_column} = ?",
            (record_id,),
        )

    def list_records(
        self,
        table_name: str,
        order_by: str = "created_at DESC",
        where_clause: str | None = None,
        params: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        sql = f"SELECT * FROM {table_name}"
        conditions: list[str] = []
        if self._supports_soft_delete(table_name):
            conditions.append(self._active_clause(table_name))
        if where_clause:
            conditions.append(where_clause)
        if conditions:
            sql += " WHERE " + " AND ".join(f"({condition})" for condition in conditions)
        sql += f" ORDER BY {order_by}"
        return self.database.fetch_all(sql, params)

    def get_latest_operator(self, table_name: str) -> str:
        conditions: list[str] = ["COALESCE(TRIM(operator), '') <> ''"]
        if self._supports_soft_delete(table_name):
            conditions.append(self._active_clause(table_name))
        row = self.database.fetch_one(
            f"""
            SELECT operator
            FROM {table_name}
            WHERE {" AND ".join(f"({condition})" for condition in conditions)}
            ORDER BY updated_at DESC, created_at DESC, rowid DESC
            LIMIT 1
            """
        )
        return str((row or {}).get("operator", ""))

    def duplicate_record(self, table_name: str, record_id: str, prefix: str) -> str:
        record = self.get_record(table_name, record_id)
        if not record:
            raise ValueError(f"{table_name} の対象データが見つかりません: {record_id}")
        new_id = generate_id(prefix)
        cloned = dict(record)
        cloned[TABLE_ID_COLUMNS[table_name]] = new_id
        for column_name in ("created_at", "updated_at", "assigned_measurement_id", "is_deleted", "deleted_at"):
            cloned.pop(column_name, None)
        if table_name == "batch_plan_items":
            cloned["planned_status"] = PlannedStatus.WAITING.value
        self.insert_record(table_name, cloned)
        return new_id

    def get_next_rep_no(self, condition_id: str) -> int:
        row = self.database.fetch_one(
            "SELECT COALESCE(MAX(rep_no), 0) AS max_rep_no FROM measurements WHERE condition_id = ?",
            (condition_id,),
        )
        return int(row["max_rep_no"]) + 1 if row else 1

    def get_next_waiting_batch_item(self, session_id: str | None = None) -> dict[str, Any] | None:
        sql = f"""
            SELECT b.*
            FROM batch_plan_items AS b
            INNER JOIN sessions AS s ON s.session_id = b.session_id
            INNER JOIN conditions AS c ON c.condition_id = b.condition_id
            WHERE b.planned_status = ?
              AND {self._active_clause("batch_plan_items", "b")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
        """
        params: list[Any] = [PlannedStatus.WAITING.value]
        if session_id:
            sql += " AND b.session_id = ?"
            params.append(session_id)
        sql += " ORDER BY b.planned_order ASC LIMIT 1"
        return self.database.fetch_one(sql, tuple(params))

    def replace_batch_plan(self, session_id: str, items: list[dict[str, Any]]) -> None:
        existing_assigned = self.database.fetch_one(
            """
            SELECT COUNT(*) AS assigned_count
            FROM batch_plan_items
            WHERE session_id = ?
              AND assigned_measurement_id IS NOT NULL
              AND COALESCE(is_deleted, 0) = 0
            """,
            (session_id,),
        )
        if existing_assigned and int(existing_assigned["assigned_count"]) > 0:
            raise ValueError("実測データが紐付いたバッチ計画は安全のため置き換えできません。")
        self.database.delete(
            "batch_plan_items",
            "session_id = ? AND COALESCE(is_deleted, 0) = 0",
            (session_id,),
        )
        for item in items:
            self.insert_record(
                "batch_plan_items",
                {
                    "batch_item_id": generate_id("BATCH"),
                    "session_id": session_id,
                    "condition_id": item["condition_id"],
                    "planned_order": item["planned_order"],
                    "rep_no": item["rep_no"],
                    "planned_status": item["planned_status"],
                    "assigned_measurement_id": item.get("assigned_measurement_id"),
                    "note": item.get("note", ""),
                },
            )

    def requeue_failed_batch_items(self, session_id: str) -> None:
        self.database.execute(
            """
            UPDATE batch_plan_items
            SET planned_status = ?, updated_at = ?
            WHERE session_id = ?
              AND planned_status IN (?, ?)
              AND COALESCE(is_deleted, 0) = 0
            """,
            (
                PlannedStatus.WAITING.value,
                now_iso(),
                session_id,
                PlannedStatus.FAILED.value,
                PlannedStatus.RELINK_NEEDED.value,
            ),
        )

    def update_batch_item_status(
        self,
        batch_item_id: str,
        planned_status: str,
        assigned_measurement_id: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {"planned_status": planned_status}
        if assigned_measurement_id is not None:
            payload["assigned_measurement_id"] = assigned_measurement_id
        self.update_record("batch_plan_items", batch_item_id, payload)

    def replace_cycle_results(self, measurement_id: str, cycle_rows: list[dict[str, Any]]) -> None:
        self.database.delete("cycle_results", "measurement_id = ?", (measurement_id,))
        for row in cycle_rows:
            self.insert_record("cycle_results", row)

    def upsert_analysis_result(self, measurement_id: str, payload: dict[str, Any]) -> None:
        current = self.database.fetch_one(
            "SELECT result_id FROM analysis_results WHERE measurement_id = ?",
            (measurement_id,),
        )
        if current:
            self.database.delete("analysis_results", "measurement_id = ?", (measurement_id,))
        self.insert_record("analysis_results", payload)

    def replace_aggregated_results(self, session_id: str, payloads: list[dict[str, Any]]) -> None:
        self.database.delete("aggregated_results", "session_id = ?", (session_id,))
        for payload in payloads:
            self.insert_record("aggregated_results", payload)

    def replace_mean_voltammogram_records(
        self,
        session_id: str,
        condition_id: str | None,
        payloads: list[dict[str, Any]],
    ) -> None:
        if condition_id:
            self.database.delete(
                "mean_voltammogram_records",
                "session_id = ? AND condition_id = ?",
                (session_id, condition_id),
            )
        else:
            self.database.delete("mean_voltammogram_records", "session_id = ?", (session_id,))
        for payload in payloads:
            self.insert_record("mean_voltammogram_records", payload)

    def log_error(
        self,
        message: str,
        context: str,
        session_id: str | None = None,
        measurement_id: str | None = None,
    ) -> None:
        payload = {
            "error_id": generate_id("ERR"),
            "session_id": session_id,
            "measurement_id": measurement_id,
            "message": message,
            "context": context,
        }
        LOGGER.error("%s | %s", message, context)
        self.insert_record("error_logs", payload)

    def get_mip_dependency_summary(self, mip_id: str) -> dict[str, int]:
        row = self.database.fetch_one(
            "SELECT COUNT(*) AS usage_count FROM mip_usage_records WHERE mip_id = ?",
            (mip_id,),
        )
        return {"usage_count": int((row or {}).get("usage_count", 0))}

    def get_mip_usage_dependency_summary(self, mip_usage_id: str) -> dict[str, int]:
        row = self.database.fetch_one(
            "SELECT COUNT(*) AS session_count FROM sessions WHERE mip_usage_id = ?",
            (mip_usage_id,),
        )
        return {"session_count": int((row or {}).get("session_count", 0))}

    def get_session_dependency_summary(self, session_id: str) -> dict[str, int]:
        counts = self.database.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM conditions WHERE session_id = ?) AS condition_count,
                (SELECT COUNT(*) FROM batch_plan_items WHERE session_id = ?) AS batch_count,
                (SELECT COUNT(*) FROM measurements WHERE session_id = ?) AS measurement_count
            """,
            (session_id, session_id, session_id),
        ) or {}
        return {
            "condition_count": int(counts.get("condition_count", 0)),
            "batch_count": int(counts.get("batch_count", 0)),
            "measurement_count": int(counts.get("measurement_count", 0)),
        }

    def get_condition_dependency_summary(self, condition_id: str) -> dict[str, int]:
        counts = self.database.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM batch_plan_items WHERE condition_id = ?) AS batch_count,
                (SELECT COUNT(*) FROM measurements WHERE condition_id = ?) AS measurement_count,
                (SELECT COUNT(*) FROM aggregated_results WHERE condition_id = ?) AS aggregate_count,
                (SELECT COUNT(*) FROM mean_voltammogram_records WHERE condition_id = ?) AS mean_count
            """,
            (condition_id, condition_id, condition_id, condition_id),
        ) or {}
        return {
            "batch_count": int(counts.get("batch_count", 0)),
            "measurement_count": int(counts.get("measurement_count", 0)),
            "aggregate_count": int(counts.get("aggregate_count", 0)),
            "mean_count": int(counts.get("mean_count", 0)),
        }

    def get_batch_item_dependency_summary(self, batch_item_id: str) -> dict[str, int]:
        counts = self.database.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM measurements WHERE batch_item_id = ?) AS measurement_count,
                (SELECT COUNT(*) FROM measurements WHERE batch_item_id = ? AND COALESCE(is_deleted, 0) = 0) AS active_measurement_count
            """,
            (batch_item_id, batch_item_id),
        ) or {}
        return {
            "measurement_count": int(counts.get("measurement_count", 0)),
            "active_measurement_count": int(counts.get("active_measurement_count", 0)),
        }

    def get_measurement_dependency_summary(self, measurement_id: str) -> dict[str, int]:
        counts = self.database.fetch_one(
            """
            SELECT
                (SELECT COUNT(*) FROM measurement_conditions WHERE measurement_id = ?) AS condition_param_count,
                (SELECT COUNT(*) FROM analysis_results WHERE measurement_id = ?) AS analysis_count,
                (SELECT COUNT(*) FROM cycle_results WHERE measurement_id = ?) AS cycle_count
            """,
            (measurement_id, measurement_id, measurement_id),
        ) or {}
        return {
            "condition_param_count": int(counts.get("condition_param_count", 0)),
            "analysis_count": int(counts.get("analysis_count", 0)),
            "cycle_count": int(counts.get("cycle_count", 0)),
        }

    def logical_delete_mip(self, mip_id: str) -> None:
        self.mark_deleted("mip_records", mip_id)
        self.bulk_mark_deleted("mip_usage_records", "mip_id = ?", (mip_id,))
        self.bulk_mark_deleted(
            "sessions",
            "mip_usage_id IN (SELECT mip_usage_id FROM mip_usage_records WHERE mip_id = ?)",
            (mip_id,),
        )
        self.bulk_mark_deleted(
            "conditions",
            "session_id IN (SELECT session_id FROM sessions WHERE mip_usage_id IN (SELECT mip_usage_id FROM mip_usage_records WHERE mip_id = ?))",
            (mip_id,),
        )

    def logical_delete_mip_usage(self, mip_usage_id: str) -> None:
        self.mark_deleted("mip_usage_records", mip_usage_id)
        self.bulk_mark_deleted("sessions", "mip_usage_id = ?", (mip_usage_id,))
        self.bulk_mark_deleted(
            "conditions",
            "session_id IN (SELECT session_id FROM sessions WHERE mip_usage_id = ?)",
            (mip_usage_id,),
        )

    def logical_delete_session(self, session_id: str) -> None:
        self.mark_deleted("sessions", session_id)
        self.bulk_mark_deleted("conditions", "session_id = ?", (session_id,))

    def logical_delete_batch_item(self, batch_item_id: str) -> None:
        self.update_record(
            "batch_plan_items",
            batch_item_id,
            {
                "planned_status": PlannedStatus.SKIPPED.value,
                "is_deleted": 1,
                "deleted_at": now_iso(),
            },
        )

    def logical_delete_measurement(self, measurement_id: str) -> None:
        self.mark_deleted("measurements", measurement_id)
        measurement = self.get_record("measurements", measurement_id)
        if not measurement:
            return
        batch_item_id = measurement.get("batch_item_id")
        if batch_item_id:
            self.update_record(
                "batch_plan_items",
                str(batch_item_id),
                {
                    "planned_status": PlannedStatus.RELINK_NEEDED.value,
                },
            )

    def purge_session(self, session_id: str) -> None:
        self.database.delete("mean_voltammogram_records", "session_id = ?", (session_id,))
        self.database.delete("aggregated_results", "session_id = ?", (session_id,))
        self.database.delete("batch_plan_items", "session_id = ?", (session_id,))
        self.database.delete("conditions", "session_id = ?", (session_id,))
        self.database.delete("error_logs", "session_id = ?", (session_id,))
        self.delete_record_physical("sessions", session_id)

    def purge_condition(self, condition_id: str) -> None:
        self.database.delete("mean_voltammogram_records", "condition_id = ?", (condition_id,))
        self.database.delete("aggregated_results", "condition_id = ?", (condition_id,))
        self.database.delete("batch_plan_items", "condition_id = ?", (condition_id,))
        self.delete_record_physical("conditions", condition_id)

    def purge_batch_item(self, batch_item_id: str) -> None:
        self.delete_record_physical("batch_plan_items", batch_item_id)

    def purge_measurement(self, measurement_id: str) -> None:
        measurement = self.get_record("measurements", measurement_id)
        if not measurement:
            return
        batch_item_id = measurement.get("batch_item_id")
        if batch_item_id:
            self.update_record(
                "batch_plan_items",
                str(batch_item_id),
                {
                    "planned_status": PlannedStatus.WAITING.value,
                    "assigned_measurement_id": None,
                },
            )
        self.database.delete("measurement_conditions", "measurement_id = ?", (measurement_id,))
        self.database.delete("analysis_results", "measurement_id = ?", (measurement_id,))
        self.database.delete("cycle_results", "measurement_id = ?", (measurement_id,))
        self.database.delete("error_logs", "measurement_id = ?", (measurement_id,))
        self.delete_record_physical("measurements", measurement_id)

    def list_active_batch_items(self, session_id: str | None = None) -> list[dict[str, Any]]:
        sql = f"""
            SELECT b.*
            FROM batch_plan_items AS b
            INNER JOIN sessions AS s ON s.session_id = b.session_id
            INNER JOIN conditions AS c ON c.condition_id = b.condition_id
            WHERE {self._active_clause("batch_plan_items", "b")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
        """
        params: list[Any] = []
        if session_id:
            sql += " AND b.session_id = ?"
            params.append(session_id)
        sql += " ORDER BY b.planned_order ASC, b.created_at DESC"
        return self.database.fetch_all(sql, tuple(params))

    def list_active_measurements(self, session_id: str | None = None) -> list[dict[str, Any]]:
        sql = f"""
            SELECT m.*
            FROM measurements AS m
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            WHERE {self._active_clause("measurements", "m")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
        """
        params: list[Any] = []
        if session_id:
            sql += " AND m.session_id = ?"
            params.append(session_id)
        sql += " ORDER BY m.measured_at DESC, m.created_at DESC"
        return self.database.fetch_all(sql, tuple(params))

    def refresh_condition_stats(self, session_id: str | None = None, condition_id: str | None = None) -> None:
        filters: list[str] = [self._active_clause("conditions")]
        params: list[Any] = []
        if session_id:
            filters.append("session_id = ?")
            params.append(session_id)
        if condition_id:
            filters.append("condition_id = ?")
            params.append(condition_id)

        where_clause = f"WHERE {' AND '.join(filters)}"
        conditions = self.database.fetch_all(f"SELECT * FROM conditions {where_clause}", tuple(params))
        for condition in conditions:
            measurement_rows = self.database.fetch_all(
                """
                SELECT m.*
                FROM measurements AS m
                INNER JOIN sessions AS s ON s.session_id = m.session_id
                INNER JOIN conditions AS c ON c.condition_id = m.condition_id
                WHERE m.condition_id = ?
                  AND COALESCE(m.is_deleted, 0) = 0
                  AND COALESCE(s.is_deleted, 0) = 0
                  AND COALESCE(c.is_deleted, 0) = 0
                ORDER BY m.rep_no ASC
                """,
                (condition["condition_id"],),
            )
            analysis_frame = self.database.query_frame(
                f"""
                SELECT a.representative_current_a
                FROM analysis_results AS a
                INNER JOIN measurements AS m ON m.measurement_id = a.measurement_id
                WHERE a.condition_id = ?
                  AND a.representative_current_a IS NOT NULL
                  AND {self._active_clause("measurements", "m")}
                """,
                (condition["condition_id"],),
            )
            actual_replicates = len(measurement_rows)
            n_valid = len([row for row in measurement_rows if row.get("final_quality_flag") == QualityFlag.VALID.value])
            n_invalid = len([row for row in measurement_rows if row.get("final_quality_flag") == QualityFlag.INVALID.value])

            cv_percent = None
            if not analysis_frame.empty:
                mean_current = float(analysis_frame["representative_current_a"].mean())
                std_current = float(analysis_frame["representative_current_a"].std(ddof=0)) if len(analysis_frame) > 1 else 0.0
                if mean_current:
                    cv_percent = std_current / abs(mean_current) * 100.0

            planned_replicates = int(condition["planned_replicates"] or 0)
            if actual_replicates == 0:
                condition_status = ConditionState.PENDING.value
            elif planned_replicates and actual_replicates < planned_replicates:
                condition_status = ConditionState.IN_PROGRESS.value
            elif n_invalid > 0:
                condition_status = ConditionState.REVIEW.value
            else:
                condition_status = ConditionState.COMPLETED.value

            self.update_record(
                "conditions",
                condition["condition_id"],
                {
                    "actual_replicates": actual_replicates,
                    "n_valid": n_valid,
                    "n_invalid": n_invalid,
                    "cv_percent": cv_percent,
                    "condition_status": condition_status,
                },
            )

    def list_recent_sessions(self, limit: int = 5) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            f"""
            SELECT *
            FROM sessions
            WHERE {self._active_clause("sessions")}
            ORDER BY session_date DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def list_recent_mips(self, limit: int = 5) -> list[dict[str, Any]]:
        return self.database.fetch_all(
            f"""
            SELECT *
            FROM mip_records
            WHERE {self._active_clause("mip_records")}
            ORDER BY preparation_date DESC, created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def get_home_snapshot(self) -> dict[str, Any]:
        unfinished = self.database.fetch_one(
            f"""
            SELECT COUNT(*) AS value
            FROM conditions
            WHERE {self._active_clause("conditions")}
              AND COALESCE(condition_status, '') <> ?
            """,
            (ConditionState.COMPLETED.value,),
        )
        flagged = self.database.fetch_one(
            f"""
            SELECT COUNT(*) AS value
            FROM measurements AS m
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            WHERE {self._active_clause("measurements", "m")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
              AND m.final_quality_flag <> ?
            """,
            (QualityFlag.VALID.value,),
        )
        errors = self.database.fetch_all(
            """
            SELECT e.*
            FROM error_logs AS e
            LEFT JOIN sessions AS s ON s.session_id = e.session_id
            WHERE s.session_id IS NULL OR COALESCE(s.is_deleted, 0) = 0
            ORDER BY e.created_at DESC
            LIMIT 5
            """
        )
        return {
            "recent_sessions": self.list_recent_sessions(),
            "recent_mips": self.list_recent_mips(),
            "unfinished_condition_count": int((unfinished or {}).get("value", 0)),
            "flagged_measurement_count": int((flagged or {}).get("value", 0)),
            "recent_errors": errors,
        }

    def get_session_bundle(self, session_id: str) -> dict[str, Any]:
        session = self.get_record("sessions", session_id)
        if not session or int(session.get("is_deleted", 0)) == 1:
            raise ValueError(f"セッションが見つかりません: {session_id}")
        conditions = self.list_records("conditions", "concentration_value ASC", "session_id = ?", (session_id,))
        batch_items = self.list_active_batch_items(session_id)
        measurements = self.list_active_measurements(session_id)
        analysis_results = self.database.fetch_all(
            """
            SELECT a.*
            FROM analysis_results AS a
            INNER JOIN measurements AS m ON m.measurement_id = a.measurement_id
            WHERE a.session_id = ?
              AND COALESCE(m.is_deleted, 0) = 0
            ORDER BY a.created_at ASC
            """,
            (session_id,),
        )
        mean_records = self.list_records("mean_voltammogram_records", "created_at DESC", "session_id = ?", (session_id,))
        return {
            "session": session,
            "conditions": conditions,
            "batch_items": batch_items,
            "measurements": measurements,
            "analysis_results": analysis_results,
            "aggregated_results": self.list_records("aggregated_results", "created_at DESC", "session_id = ?", (session_id,)),
            "mean_voltammogram_records": mean_records,
        }

    def get_condition_measurements(self, condition_id: str, include_flags: list[str]) -> list[dict[str, Any]]:
        placeholders = ", ".join("?" for _ in include_flags)
        sql = f"""
            SELECT m.*, a.representative_current_a, a.representative_potential_v
            FROM measurements AS m
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            LEFT JOIN analysis_results AS a ON a.measurement_id = m.measurement_id
            WHERE m.condition_id = ?
              AND m.final_quality_flag IN ({placeholders})
              AND {self._active_clause("measurements", "m")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
            ORDER BY m.rep_no ASC
        """
        params = (condition_id, *include_flags)
        return self.database.fetch_all(sql, params)

    def get_measurement_condition(self, measurement_id: str) -> dict[str, Any] | None:
        return self.database.fetch_one(
            "SELECT * FROM measurement_conditions WHERE measurement_id = ? ORDER BY created_at DESC LIMIT 1",
            (measurement_id,),
        )

    def get_export_frames(self, session_id: str) -> dict[str, pd.DataFrame]:
        session = self.get_record("sessions", session_id)
        if not session or int(session.get("is_deleted", 0)) == 1:
            raise ValueError(f"セッションが見つかりません: {session_id}")

        mip_usage = self.get_record("mip_usage_records", session["mip_usage_id"])
        mip_record = self.get_record("mip_records", mip_usage["mip_id"]) if mip_usage else None
        measurements = self.database.query_frame(
            f"""
            SELECT m.*
            FROM measurements AS m
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            WHERE m.session_id = ?
              AND {self._active_clause("measurements", "m")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
            ORDER BY m.rep_no ASC
            """,
            (session_id,),
        )
        measurement_ids = measurements["measurement_id"].tolist() if not measurements.empty else []
        conditions = self.database.query_frame(
            f"""
            SELECT *
            FROM conditions
            WHERE session_id = ? AND {self._active_clause("conditions")}
            ORDER BY concentration_value ASC
            """,
            (session_id,),
        )

        frames: dict[str, pd.DataFrame] = {
            "MIP一覧": pd.DataFrame([mip_record]) if mip_record and int(mip_record.get("is_deleted", 0)) == 0 else pd.DataFrame(),
            "MIP使用一覧": pd.DataFrame([mip_usage]) if mip_usage and int(mip_usage.get("is_deleted", 0)) == 0 else pd.DataFrame(),
            "セッション一覧": pd.DataFrame([session]),
            "条件一覧": conditions,
            "バッチ実行計画": self.database.query_frame(
                f"""
                SELECT b.*
                FROM batch_plan_items AS b
                INNER JOIN sessions AS s ON s.session_id = b.session_id
                INNER JOIN conditions AS c ON c.condition_id = b.condition_id
                WHERE b.session_id = ?
                  AND {self._active_clause("batch_plan_items", "b")}
                  AND {self._active_clause("sessions", "s")}
                  AND {self._active_clause("conditions", "c")}
                ORDER BY b.planned_order ASC
                """,
                (session_id,),
            ),
            "測定一覧": measurements,
            "集計結果": self.database.query_frame(
                "SELECT * FROM aggregated_results WHERE session_id = ? ORDER BY created_at ASC",
                (session_id,),
            ),
            "平均ボルタモグラム": self.database.query_frame(
                "SELECT * FROM mean_voltammogram_records WHERE session_id = ? ORDER BY created_at ASC",
                (session_id,),
            ),
            "エラー一覧": self.database.query_frame(
                "SELECT * FROM error_logs WHERE session_id = ? ORDER BY created_at DESC",
                (session_id,),
            ),
        }

        if measurement_ids:
            placeholders = ", ".join("?" for _ in measurement_ids)
            frames["解析結果"] = self.database.query_frame(
                f"SELECT * FROM analysis_results WHERE measurement_id IN ({placeholders}) ORDER BY created_at ASC",
                tuple(measurement_ids),
            )
            frames["測定条件一覧"] = self.database.query_frame(
                f"SELECT * FROM measurement_conditions WHERE measurement_id IN ({placeholders})",
                tuple(measurement_ids),
            )
            frames["サイクル結果"] = self.database.query_frame(
                f"SELECT * FROM cycle_results WHERE measurement_id IN ({placeholders}) ORDER BY measurement_id, cycle_no",
                tuple(measurement_ids),
            )
        else:
            frames["解析結果"] = pd.DataFrame()
            frames["測定条件一覧"] = pd.DataFrame()
            frames["サイクル結果"] = pd.DataFrame()
        return frames

    def search_cross_measurements(self, filters: dict[str, Any]) -> pd.DataFrame:
        sql = f"""
            SELECT
                m.measurement_id,
                m.measured_at,
                m.final_quality_flag,
                m.free_memo,
                s.session_id,
                s.session_name,
                s.analyte,
                s.tags AS session_tags,
                c.condition_id,
                c.concentration_value,
                c.concentration_unit,
                c.method,
                mu.mip_usage_id,
                mr.mip_id,
                a.representative_current_a,
                a.representative_potential_v
            FROM measurements AS m
            INNER JOIN sessions AS s ON s.session_id = m.session_id
            INNER JOIN conditions AS c ON c.condition_id = m.condition_id
            LEFT JOIN mip_usage_records AS mu ON mu.mip_usage_id = s.mip_usage_id
            LEFT JOIN mip_records AS mr ON mr.mip_id = mu.mip_id
            LEFT JOIN analysis_results AS a ON a.measurement_id = m.measurement_id
            WHERE {self._active_clause("measurements", "m")}
              AND {self._active_clause("sessions", "s")}
              AND {self._active_clause("conditions", "c")}
        """
        params: list[Any] = []
        if analyte := str(filters.get("analyte", "")).strip():
            sql += " AND s.analyte LIKE ?"
            params.append(f"%{analyte}%")
        if method := str(filters.get("method", "")).strip():
            sql += " AND c.method LIKE ?"
            params.append(f"%{method}%")
        if quality := str(filters.get("quality_flag", "")).strip():
            sql += " AND m.final_quality_flag = ?"
            params.append(quality)
        if keyword := str(filters.get("keyword", "")).strip():
            sql += " AND (s.session_name LIKE ? OR s.tags LIKE ? OR m.free_memo LIKE ? OR mr.mip_id LIKE ?)"
            params.extend([f"%{keyword}%"] * 4)
        if mip_id := str(filters.get("mip_id", "")).strip():
            sql += " AND mr.mip_id = ?"
            params.append(mip_id)
        sql += " ORDER BY m.measured_at DESC, m.created_at DESC"
        return self.database.query_frame(sql, tuple(params))
