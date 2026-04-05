from __future__ import annotations


def require_fields(payload: dict[str, object], required_fields: list[str]) -> None:
    missing = [field for field in required_fields if not str(payload.get(field, "")).strip()]
    if missing:
        raise ValueError(f"必須項目が未入力です: {', '.join(missing)}")


def require_any(payload: dict[str, object], field_names: list[str]) -> None:
    if not any(str(payload.get(name, "")).strip() for name in field_names):
        raise ValueError(f"次のいずれかを入力してください: {', '.join(field_names)}")
