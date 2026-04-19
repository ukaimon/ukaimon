from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

import pandas as pd


class QualityFlag(str, Enum):
    VALID = "valid"
    SUSPECT = "suspect"
    INVALID = "invalid"


class PlannedStatus(str, Enum):
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RELINK_NEEDED = "relink_needed"
    SKIPPED = "skipped"


class ConditionState(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    REVIEW = "review"


@dataclass(slots=True)
class ParsedMeasurementData:
    metadata: dict[str, object]
    raw_header_text: str
    data: pd.DataFrame
    detected_columns: dict[str, str]
    source_file_path: str
    file_type: str = "ids"
    data_blocks: list[dict[str, object]] = field(default_factory=list)


@dataclass(slots=True)
class CycleAnalysisResult:
    cycle_no: int
    oxidation_peak_current_a: float | None
    oxidation_peak_potential_v: float | None
    reduction_peak_current_a: float | None
    reduction_peak_potential_v: float | None
    delta_ep_v: float | None
    integrated_area: float | None
    quality_flag: str


@dataclass(slots=True)
class MeasurementAnalysisResult:
    representative_current_a: float | None
    representative_potential_v: float | None
    oxidation_peak_current_a: float | None
    oxidation_peak_potential_v: float | None
    reduction_peak_current_a: float | None
    reduction_peak_potential_v: float | None
    delta_ep_v: float | None
    integrated_area: float | None
    quality_flag: str
    analysis_method: str
    cycle_results: list[CycleAnalysisResult] = field(default_factory=list)
    note: str = ""


@dataclass(slots=True)
class MeanVoltammogramResult:
    dataframe: pd.DataFrame
    n_used: int
    source_measurement_ids: list[str]
    interpolation_method: str
    interpolation_points: int


@dataclass(slots=True)
class IviumRunState:
    status: str = "idle"
    batch_item_id: str = ""
    session_id: str = ""
    condition_id: str = ""
    condition_label: str = ""
    rep_no: int = 0
    method_name: str = ""
    method_file_path: str = ""
    source_file_path: str = ""
    source_file_type: str = ""
    imported_measurement_id: str = ""
    message: str = ""
    started_at: str = ""
    updated_at: str = ""
    completed_at: str = ""
    device_status_code: int | None = None
    points_collected: int = 0
    device_serial: str = ""
    log_lines: list[str] = field(default_factory=list)
