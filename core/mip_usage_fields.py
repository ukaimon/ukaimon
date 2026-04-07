from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MipUsageFieldSpec:
    key: str
    label: str
    default_value: str
    sql_definition: str


MIP_USAGE_FIELD_GROUPS: tuple[tuple[str, tuple[MipUsageFieldSpec, ...]], ...] = (
    (
        "混錬",
        (
            MipUsageFieldSpec("kneading_count", "混錬回数 [回]", "5", "INTEGER DEFAULT 5"),
        ),
    ),
    (
        "塗布",
        (
            MipUsageFieldSpec("coating_speed_mm_min", "塗布速度 [mm/min]", "2000", "REAL DEFAULT 2000"),
            MipUsageFieldSpec("coating_passes", "塗布回数 [回]", "5", "INTEGER DEFAULT 5"),
            MipUsageFieldSpec("coating_height", "塗布高さ", "6.8", "REAL DEFAULT 6.8"),
        ),
    ),
)

MIP_USAGE_FIELD_SPECS: tuple[MipUsageFieldSpec, ...] = tuple(
    spec
    for _group_name, specs in MIP_USAGE_FIELD_GROUPS
    for spec in specs
)

MIP_USAGE_FIELD_DEFAULTS = {spec.key: spec.default_value for spec in MIP_USAGE_FIELD_SPECS}

MIP_USAGE_FIELD_SQL_COLUMNS = {
    spec.key: spec.sql_definition
    for spec in MIP_USAGE_FIELD_SPECS
}


def with_mip_usage_field_defaults(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = dict(MIP_USAGE_FIELD_DEFAULTS)
    if not payload:
        return normalized
    for key in MIP_USAGE_FIELD_DEFAULTS:
        value = payload.get(key)
        if value in (None, ""):
            continue
        normalized[key] = str(value).strip()
    return normalized
