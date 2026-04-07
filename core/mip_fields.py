from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MipFieldSpec:
    key: str
    label: str
    default_value: str


MIP_FIELD_GROUPS: tuple[tuple[str, tuple[MipFieldSpec, ...]], ...] = (
    (
        "材料",
        (
            MipFieldSpec("dmso_ul", "DMSO [μL]", "19"),
            MipFieldSpec("histamine_dihydrochloride_g", "ヒスタミン二塩酸塩 [g]", "0.3680"),
            MipFieldSpec("maa_ul", "MAA [μL]", "340"),
            MipFieldSpec("vinylferrocene_g", "ビニルフェロセン [g]", "0.1509"),
            MipFieldSpec("edma_ml", "EDMA [mL]", "3.171"),
            MipFieldSpec("ig_g", "IG [g]", "0.25"),
        ),
    ),
    (
        "重合条件",
        (
            MipFieldSpec("nitrogen_flow_l_min", "窒素流量 [L/min]", "6.0"),
            MipFieldSpec("rotator_rpm", "ローテーター [RPM]", "400"),
            MipFieldSpec("uv_intensity_mw_cm2", "UV強度 [mW/㎠]", "4.0"),
            MipFieldSpec("uv_irradiation_time_min", "UV時間 [min]", "60"),
        ),
    ),
    (
        "吸引ろ過",
        (
            MipFieldSpec("acetic_acid_ml", "酢酸 [mL]", "400"),
            MipFieldSpec("heated_pure_water_ml", "加熱純水 [mL]", "600"),
            MipFieldSpec("acetone_ml", "アセトン [mL]", "20"),
        ),
    ),
)

MIP_FIELD_SPECS: tuple[MipFieldSpec, ...] = tuple(
    spec
    for _group_name, specs in MIP_FIELD_GROUPS
    for spec in specs
)

MIP_FIELD_DEFAULTS = {spec.key: spec.default_value for spec in MIP_FIELD_SPECS}

MIP_FIELD_SQL_COLUMNS = {
    spec.key: f"TEXT DEFAULT '{spec.default_value}'"
    for spec in MIP_FIELD_SPECS
}


def with_mip_field_defaults(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = dict(MIP_FIELD_DEFAULTS)
    if not payload:
        return normalized
    for key in MIP_FIELD_DEFAULTS:
        value = payload.get(key)
        if value in (None, ""):
            continue
        normalized[key] = str(value).strip()
    return normalized
