from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from core.mip_fields import MIP_FIELD_DEFAULTS, MIP_FIELD_SQL_COLUMNS
from core.mip_usage_fields import MIP_USAGE_FIELD_DEFAULTS, MIP_USAGE_FIELD_SQL_COLUMNS


CONDITION_FIELD_SQL_COLUMNS = {
    "ivium_method_name": "TEXT",
    "potential_start_v": "REAL",
    "potential_end_v": "REAL",
    "potential_vertex_1_v": "REAL",
    "potential_vertex_2_v": "REAL",
    "scan_rate_v_s": "REAL",
    "step_v": "REAL",
    "pulse_amplitude_v": "REAL",
    "pulse_time_s": "REAL",
    "quiet_time_s": "REAL",
    "cycles": "INTEGER",
    "current_range": "TEXT",
    "filter_setting": "TEXT",
}


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS mip_records (
        mip_id TEXT PRIMARY KEY,
        preparation_date TEXT,
        template_name TEXT NOT NULL,
        monomer TEXT,
        crosslinker TEXT,
        solvent TEXT,
        initiator TEXT,
        polymerization_method TEXT,
        polymerization_time TEXT,
        light_condition TEXT,
        dmso_ul TEXT DEFAULT '19',
        histamine_dihydrochloride_g TEXT DEFAULT '0.3680',
        maa_ul TEXT DEFAULT '340',
        vinylferrocene_g TEXT DEFAULT '0.1509',
        edma_ml TEXT DEFAULT '3.171',
        ig_g TEXT DEFAULT '0.25',
        nitrogen_flow_l_min TEXT DEFAULT '6.0',
        rotator_rpm TEXT DEFAULT '400',
        uv_intensity_mw_cm2 TEXT DEFAULT '4.0',
        uv_irradiation_time_min TEXT DEFAULT '60',
        acetic_acid_ml TEXT DEFAULT '400',
        heated_pure_water_ml TEXT DEFAULT '600',
        acetone_ml TEXT DEFAULT '20',
        operator TEXT NOT NULL,
        note TEXT,
        tags TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mip_usage_records (
        mip_usage_id TEXT PRIMARY KEY,
        mip_id TEXT NOT NULL,
        cp_preparation_date TEXT,
        coating_date TEXT,
        kneading_count INTEGER DEFAULT 5,
        silicone_oil_amount REAL,
        graphite_amount REAL,
        chip_type TEXT DEFAULT '',
        coating_speed_mm_min REAL DEFAULT 2000,
        coating_passes INTEGER DEFAULT 3,
        coating_height REAL DEFAULT 6.8,
        operator TEXT,
        note TEXT,
        tags TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (mip_id) REFERENCES mip_records (mip_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        session_date TEXT NOT NULL,
        session_name TEXT,
        analyte TEXT NOT NULL,
        method_default TEXT,
        electrolyte TEXT,
        common_note TEXT,
        mip_usage_id TEXT NOT NULL,
        operator TEXT,
        tags TEXT,
        status TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (mip_usage_id) REFERENCES mip_usage_records (mip_usage_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS conditions (
        condition_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        analyte TEXT,
        concentration_value REAL NOT NULL,
        concentration_unit TEXT NOT NULL,
        method TEXT NOT NULL,
        ivium_method_name TEXT,
        potential_start_v REAL,
        potential_end_v REAL,
        potential_vertex_1_v REAL,
        potential_vertex_2_v REAL,
        scan_rate_v_s REAL,
        step_v REAL,
        pulse_amplitude_v REAL,
        pulse_time_s REAL,
        quiet_time_s REAL,
        cycles INTEGER,
        current_range TEXT,
        filter_setting TEXT,
        planned_replicates INTEGER,
        actual_replicates INTEGER DEFAULT 0,
        n_valid INTEGER DEFAULT 0,
        n_invalid INTEGER DEFAULT 0,
        cv_percent REAL,
        condition_status TEXT,
        common_note TEXT,
        tags TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions (session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS batch_plan_items (
        batch_item_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        planned_order INTEGER NOT NULL,
        rep_no INTEGER NOT NULL,
        planned_status TEXT NOT NULL,
        assigned_measurement_id TEXT,
        note TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (session_id) REFERENCES sessions (session_id),
        FOREIGN KEY (condition_id) REFERENCES conditions (condition_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS measurements (
        measurement_id TEXT PRIMARY KEY,
        batch_item_id TEXT,
        condition_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        mip_usage_id TEXT,
        rep_no INTEGER NOT NULL,
        measured_at TEXT,
        chip_id TEXT,
        wire_id TEXT,
        status TEXT,
        noise_level REAL,
        coating_quality TEXT,
        electrode_condition TEXT,
        bubbling_condition TEXT,
        free_memo TEXT,
        raw_file_path TEXT,
        link_status TEXT,
        auto_quality_flag TEXT,
        manual_quality_flag TEXT,
        final_quality_flag TEXT,
        exclusion_reason TEXT,
        is_deleted INTEGER NOT NULL DEFAULT 0,
        deleted_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (batch_item_id) REFERENCES batch_plan_items (batch_item_id),
        FOREIGN KEY (condition_id) REFERENCES conditions (condition_id),
        FOREIGN KEY (session_id) REFERENCES sessions (session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS measurement_conditions (
        condition_param_id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        method TEXT,
        potential_start_v REAL,
        potential_end_v REAL,
        potential_vertex_1_v REAL,
        potential_vertex_2_v REAL,
        scan_rate_v_s REAL,
        step_v REAL,
        pulse_amplitude_v REAL,
        pulse_time_s REAL,
        quiet_time_s REAL,
        cycles INTEGER,
        current_range TEXT,
        filter_setting TEXT,
        temperature_note TEXT,
        raw_header_text TEXT,
        note TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS analysis_results (
        result_id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        representative_current_a REAL,
        representative_potential_v REAL,
        oxidation_peak_current_a REAL,
        oxidation_peak_potential_v REAL,
        reduction_peak_current_a REAL,
        reduction_peak_potential_v REAL,
        delta_ep_v REAL,
        integrated_area REAL,
        quality_flag TEXT,
        analysis_method TEXT,
        note TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cycle_results (
        cycle_result_id TEXT PRIMARY KEY,
        measurement_id TEXT NOT NULL,
        cycle_no INTEGER NOT NULL,
        oxidation_peak_current_a REAL,
        oxidation_peak_potential_v REAL,
        reduction_peak_current_a REAL,
        reduction_peak_potential_v REAL,
        delta_ep_v REAL,
        integrated_area REAL,
        quality_flag TEXT,
        FOREIGN KEY (measurement_id) REFERENCES measurements (measurement_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS aggregated_results (
        aggregate_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        mip_id TEXT,
        mip_usage_id TEXT,
        analyte TEXT,
        concentration_value REAL,
        concentration_unit TEXT,
        method TEXT,
        n_total INTEGER,
        n_valid INTEGER,
        n_invalid INTEGER,
        mean_current_a REAL,
        std_current_a REAL,
        cv_percent REAL,
        mean_potential_v REAL,
        std_potential_v REAL,
        slope REAL,
        intercept REAL,
        r_squared REAL,
        note TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (condition_id) REFERENCES conditions (condition_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mean_voltammogram_records (
        mean_voltammogram_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        include_flags TEXT,
        interpolation_method TEXT,
        interpolation_points INTEGER,
        n_used INTEGER,
        source_measurement_ids_json TEXT,
        csv_path TEXT,
        png_path TEXT,
        excel_sheet_name TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (condition_id) REFERENCES conditions (condition_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS error_logs (
        error_id TEXT PRIMARY KEY,
        session_id TEXT,
        measurement_id TEXT,
        message TEXT NOT NULL,
        context TEXT,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_conditions_session ON conditions (session_id)",
    "CREATE INDEX IF NOT EXISTS idx_batch_session ON batch_plan_items (session_id, planned_order)",
    "CREATE INDEX IF NOT EXISTS idx_measurements_condition ON measurements (condition_id, rep_no)",
    "CREATE INDEX IF NOT EXISTS idx_measurements_session ON measurements (session_id, measured_at)",
    "CREATE INDEX IF NOT EXISTS idx_analysis_measurement ON analysis_results (measurement_id)",
]

SOFT_DELETE_COLUMNS = {
    "mip_records": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
    "mip_usage_records": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
    "sessions": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
    "conditions": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
    "batch_plan_items": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
    "measurements": {
        "is_deleted": "INTEGER NOT NULL DEFAULT 0",
        "deleted_at": "TEXT",
    },
}


class DatabaseManager:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    def connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            for statement in SCHEMA_STATEMENTS:
                connection.execute(statement)
            self._apply_soft_delete_migrations(connection)
            self._apply_mip_record_migrations(connection)
            self._apply_mip_usage_record_migrations(connection)
            self._apply_condition_migrations(connection)
            connection.commit()

    def _apply_soft_delete_migrations(self, connection: sqlite3.Connection) -> None:
        for table_name, columns in SOFT_DELETE_COLUMNS.items():
            existing_columns = {
                row["name"]
                for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
            for column_name, column_definition in columns.items():
                if column_name not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
                    )

    def _apply_mip_record_migrations(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(mip_records)").fetchall()
        }
        for column_name, column_definition in MIP_FIELD_SQL_COLUMNS.items():
            if column_name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE mip_records ADD COLUMN {column_name} {column_definition}"
                )
        for column_name, default_value in MIP_FIELD_DEFAULTS.items():
            connection.execute(
                f"""
                UPDATE mip_records
                SET {column_name} = ?
                WHERE {column_name} IS NULL OR TRIM(CAST({column_name} AS TEXT)) = ''
                """,
                (default_value,),
            )

    def _apply_mip_usage_record_migrations(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(mip_usage_records)").fetchall()
        }
        for column_name, column_definition in MIP_USAGE_FIELD_SQL_COLUMNS.items():
            if column_name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE mip_usage_records ADD COLUMN {column_name} {column_definition}"
                )
        for column_name, default_value in MIP_USAGE_FIELD_DEFAULTS.items():
            connection.execute(
                f"""
                UPDATE mip_usage_records
                SET {column_name} = ?
                WHERE {column_name} IS NULL OR TRIM(CAST({column_name} AS TEXT)) = ''
                """,
                (default_value,),
            )

    def _apply_condition_migrations(self, connection: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(conditions)").fetchall()
        }
        for column_name, column_definition in CONDITION_FIELD_SQL_COLUMNS.items():
            if column_name not in existing_columns:
                connection.execute(
                    f"ALTER TABLE conditions ADD COLUMN {column_name} {column_definition}"
                )

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        with self.connect() as connection:
            connection.execute(sql, params)
            connection.commit()

    def executemany(self, sql: str, params_seq: list[tuple[Any, ...]]) -> None:
        with self.connect() as connection:
            connection.executemany(sql, params_seq)
            connection.commit()

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(sql, params).fetchone()
        return dict(row) if row else None

    def insert(self, table_name: str, payload: dict[str, Any]) -> None:
        columns = ", ".join(payload.keys())
        placeholders = ", ".join("?" for _ in payload)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.execute(sql, tuple(payload.values()))

    def update(
        self,
        table_name: str,
        payload: dict[str, Any],
        where_clause: str,
        where_params: tuple[Any, ...],
    ) -> None:
        assignments = ", ".join(f"{column} = ?" for column in payload.keys())
        sql = f"UPDATE {table_name} SET {assignments} WHERE {where_clause}"
        self.execute(sql, tuple(payload.values()) + where_params)

    def delete(self, table_name: str, where_clause: str, where_params: tuple[Any, ...]) -> None:
        self.execute(f"DELETE FROM {table_name} WHERE {where_clause}", where_params)

    def query_frame(self, sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
        with self.connect() as connection:
            return pd.read_sql_query(sql, connection, params=params)
