from __future__ import annotations

from core.models import QualityFlag


def derive_auto_quality(
    noise_level: float | None = None,
    status: str | None = None,
    analysis_quality: str | None = None,
) -> QualityFlag:
    if status and status.lower() in {"failed", "error", "aborted"}:
        return QualityFlag.INVALID
    if analysis_quality == QualityFlag.INVALID.value:
        return QualityFlag.INVALID
    if noise_level is None:
        return QualityFlag.VALID
    if noise_level >= 0.8:
        return QualityFlag.INVALID
    if noise_level >= 0.4:
        return QualityFlag.SUSPECT
    return QualityFlag.VALID


def resolve_final_quality(
    auto_flag: str | None,
    manual_flag: str | None,
) -> QualityFlag:
    if manual_flag == QualityFlag.INVALID.value:
        return QualityFlag.INVALID
    if auto_flag == QualityFlag.INVALID.value:
        return QualityFlag.SUSPECT
    if manual_flag == QualityFlag.SUSPECT.value:
        return QualityFlag.SUSPECT
    if auto_flag == QualityFlag.SUSPECT.value:
        return QualityFlag.SUSPECT
    return QualityFlag.VALID
