from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from core.models import ParsedMeasurementData
from parsers.measurement_conditions_parser import parse_header_key_values


def _fetch_key_value_rows(connection: sqlite3.Connection, table_name: str, measurement_id: int) -> dict[str, object]:
    rows = connection.execute(
        f"SELECT k, v FROM {table_name} WHERE measurement_id = ? ORDER BY k",
        (measurement_id,),
    ).fetchall()
    payload: dict[str, object] = {}
    for row in rows:
        key = str(row["k"]).strip()
        value = row["v"]
        if key:
            payload[key] = value
    return payload


def _extract_report_metadata(report_text: str | None) -> dict[str, object]:
    if not report_text:
        return {}
    lines = [segment.strip() for segment in str(report_text).replace("\x1c", "\n").replace("\x1e", "\n").splitlines() if segment.strip()]
    return parse_header_key_values(lines)


def parse_ivium_sqlite_file(file_path: str | Path) -> ParsedMeasurementData:
    path = Path(file_path)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        measurement_row = connection.execute(
            "SELECT measurement_id, start_time, end_time FROM measurement ORDER BY measurement_id DESC LIMIT 1"
        ).fetchone()
        if measurement_row is None:
            raise ValueError(f"measurement テーブルにデータがありません: {path}")

        measurement_id = int(measurement_row["measurement_id"])
        method_metadata = _fetch_key_value_rows(connection, "method", measurement_id)
        report_metadata = _extract_report_metadata(method_metadata.get("Report.Process data"))
        device_metadata = _fetch_key_value_rows(connection, "devicerecord", measurement_id)

        metadata: dict[str, object] = {
            **method_metadata,
            **report_metadata,
            **device_metadata,
            "starttime": measurement_row["start_time"],
            "endtime": measurement_row["end_time"],
            "starttime_iso": measurement_row["start_time"],
            "endtime_iso": measurement_row["end_time"],
            "DbFileName": report_metadata.get("DbFileName", str(path)),
        }

        point_frame = pd.read_sql_query(
            """
            SELECT t, x, y
            FROM point
            ORDER BY point_id ASC
            """,
            connection,
        )
    if point_frame.empty:
        raise ValueError(f"point テーブルに測定点がありません: {path}")

    dataframe = pd.DataFrame(
        {
            "potential_v": point_frame["x"].astype(float),
            "current_a": point_frame["y"].astype(float),
            "time_s": point_frame["t"].astype(float),
        }
    )

    header_lines = [
        f"{key}={value}"
        for key, value in metadata.items()
        if isinstance(value, (str, int, float)) and key not in {"mt"}
    ]

    return ParsedMeasurementData(
        metadata=metadata,
        raw_header_text="\n".join(header_lines),
        data=dataframe.dropna(how="all"),
        detected_columns={
            "potential": "potential_v",
            "current": "current_a",
            "time": "time_s",
        },
        source_file_path=str(path.resolve()),
        file_type="idf_sqlite",
        data_blocks=[
            {
                "block_index": 0,
                "row_count": len(dataframe),
                "method": metadata.get("Method"),
                "starttime": metadata.get("starttime_iso"),
                "dataframe": dataframe.dropna(how="all"),
                "metadata": metadata,
                "detected_columns": {
                    "potential": "potential_v",
                    "current": "current_a",
                    "time": "time_s",
                },
            }
        ],
    )
