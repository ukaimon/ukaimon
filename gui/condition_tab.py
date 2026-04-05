from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class ConditionTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
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
        ttk.Button(form, text="新規追加", command=self._create_condition).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left")

        columns = (
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
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=14)
        headings = ["条件 ID", "セッション", "濃度", "単位", "測定法", "予定", "実測", "valid", "invalid", "状態"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=110)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _create_condition(self) -> None:
        try:
            self.services.create_condition(
                {
                    "session_id": self.session_id_var.get(),
                    "concentration_value": float(self.concentration_var.get()),
                    "concentration_unit": self.unit_var.get(),
                    "method": self.method_var.get(),
                    "planned_replicates": int(self.planned_replicates_var.get()) if self.planned_replicates_var.get() else None,
                    "common_note": "",
                    "tags": "",
                }
            )
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件登録", str(error))

    def _duplicate_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        condition_id = self.tree.item(selection[0], "values")[0]
        try:
            self.services.duplicate_condition(condition_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件複製", str(error))

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
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
