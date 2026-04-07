from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Mapping, Sequence


RECORD_TYPE_TO_TAB_TITLE = {
    "mip": "MIP 管理",
    "mip_usage": "MIP 使用記録",
    "session": "セッション管理",
    "condition": "条件管理",
    "batch_item": "バッチ実行計画",
    "measurement": "測定追加",
}

TREE_NAVIGATION_MAP: dict[str, dict[str, str]] = {
    "mip": {
        "mip_id": "mip",
    },
    "mip_usage": {
        "mip_usage_id": "mip_usage",
        "mip_id": "mip",
    },
    "session": {
        "session_id": "session",
        "mip_usage_id": "mip_usage",
    },
    "condition": {
        "condition_id": "condition",
        "session_id": "session",
    },
    "batch_plan": {
        "batch_item_id": "batch_item",
        "session_id": "session",
        "condition_id": "condition",
        "assigned_measurement_id": "measurement",
    },
    "measurement": {
        "measurement_id": "measurement",
        "session_id": "session",
        "condition_id": "condition",
    },
    "session_detail_condition": {
        "condition_id": "condition",
    },
    "session_detail_measurement": {
        "measurement_id": "measurement",
        "condition_id": "condition",
    },
    "cross_report": {
        "measurement_id": "measurement",
        "session_id": "session",
        "condition_id": "condition",
    },
}


def resolve_navigation_target(
    context: str,
    column_name: str,
    row_values: Mapping[str, object],
) -> tuple[str, str] | None:
    record_type = TREE_NAVIGATION_MAP.get(context, {}).get(column_name)
    if not record_type:
        return None
    raw_value = row_values.get(column_name, "")
    record_id = str(raw_value).strip()
    if not record_id:
        return None
    return record_type, record_id


def extract_tree_navigation_target(
    tree: ttk.Treeview,
    columns: Sequence[str],
    event: tk.Event,
    context: str,
) -> tuple[str, str] | None:
    row_id = tree.identify_row(event.y)
    column_token = tree.identify_column(event.x)
    if not row_id or not column_token.startswith("#"):
        return None
    column_index = int(column_token[1:]) - 1
    if column_index < 0 or column_index >= len(columns):
        return None
    values = tree.item(row_id, "values")
    row_values = {
        column_name: values[index] if index < len(values) else ""
        for index, column_name in enumerate(columns)
    }
    return resolve_navigation_target(context, columns[column_index], row_values)


def select_tree_record(tree: ttk.Treeview, record_id: str, column_index: int = 0) -> bool:
    target = str(record_id)
    for item_id in tree.get_children():
        values = tree.item(item_id, "values")
        if column_index < len(values) and str(values[column_index]) == target:
            tree.selection_set(item_id)
            tree.focus(item_id)
            tree.see(item_id)
            return True
    return False
