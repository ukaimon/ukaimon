from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from gui.navigation import enable_bulk_tree_actions, extract_tree_navigation_target, get_selected_tree_values, select_tree_record


class ConditionTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_condition_id: str | None = None
        self.save_button_label = tk.StringVar(value="新規追加")
        self.session_id_var = tk.StringVar()
        self.concentration_var = tk.StringVar()
        self.unit_var = tk.StringVar(value="ppm")
        self.method_var = tk.StringVar(value="CV")
        self.planned_replicates_var = tk.StringVar(value="3")

        form = ttk.LabelFrame(self, text="濃度条件")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(form, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        ttk.Label(form, text="濃度").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.concentration_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="単位").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.unit_var, width=12).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="測定法").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.method_var, width=16).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="予定回数").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.planned_replicates_var, width=12).grid(row=1, column=3, sticky="w")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_condition).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        self.tree_columns = (
            "condition_id",
            "session_id",
            "concentration_value",
            "concentration_unit",
            "method",
            "planned_replicates",
            "actual_replicates",
            "n_valid",
            "n_invalid",
            "condition_status",
        )
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=14)
        headings = ["条件 ID", "セッション", "濃度", "単位", "測定法", "予定", "実測", "valid", "invalid", "状態"]
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=110)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)
        enable_bulk_tree_actions(self.tree)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)

    def _selected_condition_id(self) -> str | None:
        selected_ids = self._selected_condition_ids()
        if not selected_ids:
            return None
        return selected_ids[0]

    def _selected_condition_ids(self) -> list[str]:
        return get_selected_tree_values(self.tree)

    def _load_selected(self) -> None:
        condition_id = self._selected_condition_id()
        if not condition_id:
            return
        row = self.services.repository.get_record("conditions", condition_id)
        if not row:
            messagebox.showerror("条件編集", "選択した条件が見つかりません。")
            return
        self.editing_condition_id = condition_id
        self.save_button_label.set("更新保存")
        self.session_id_var.set(str(row.get("session_id", "")))
        self.concentration_var.set(str(row.get("concentration_value", "")))
        self.unit_var.set(str(row.get("concentration_unit", "")))
        self.method_var.set(str(row.get("method", "")))
        self.planned_replicates_var.set(str(row.get("planned_replicates", "")))

    def focus_record(self, condition_id: str) -> None:
        if select_tree_record(self.tree, condition_id):
            self._load_selected()

    def _reset_form(self) -> None:
        self.editing_condition_id = None
        self.save_button_label.set("新規追加")
        self.concentration_var.set("")
        self.unit_var.set("ppm")
        self.method_var.set("CV")
        self.planned_replicates_var.set("3")

    def _save_condition(self) -> None:
        payload = {
            "session_id": self.session_id_var.get(),
            "concentration_value": float(self.concentration_var.get()),
            "concentration_unit": self.unit_var.get(),
            "method": self.method_var.get(),
            "planned_replicates": int(self.planned_replicates_var.get()) if self.planned_replicates_var.get() else None,
            "common_note": "",
            "tags": "",
        }
        try:
            if self.editing_condition_id:
                self.services.update_condition(self.editing_condition_id, payload)
            else:
                self.services.create_condition(payload)
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件登録", str(error))

    def _duplicate_selected(self) -> None:
        condition_id = self._selected_condition_id()
        if not condition_id:
            return
        try:
            self.services.duplicate_condition(condition_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件複製", str(error))

    def _delete_selected(self) -> None:
        condition_ids = self._selected_condition_ids()
        if not condition_ids:
            return
        label = "選択した条件を削除しますか？" if len(condition_ids) == 1 else f"選択した {len(condition_ids)} 件の条件を削除しますか？"
        if not messagebox.askyesno("条件削除", label):
            return
        try:
            messages = [self.services.delete_condition(condition_id) for condition_id in condition_ids]
            self._reset_form()
            self.refresh_app()
            summary = messages[0] if len(messages) == 1 else f"{len(condition_ids)} 件を削除しました。"
            messagebox.showinfo("条件削除", summary)
        except Exception as error:
            messagebox.showerror("条件削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "condition")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and self.session_id_var.get() not in session_ids:
            self.session_id_var.set(session_ids[0])

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_conditions():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["condition_id"],
                    row["session_id"],
                    row["concentration_value"],
                    row["concentration_unit"],
                    row["method"],
                    row.get("planned_replicates", ""),
                    row.get("actual_replicates", 0),
                    row.get("n_valid", 0),
                    row.get("n_invalid", 0),
                    row.get("condition_status", ""),
                ),
            )
