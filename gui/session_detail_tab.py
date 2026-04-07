from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from gui.navigation import extract_tree_navigation_target


class SessionDetailTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.session_id_var = tk.StringVar()
        self.summary_var = tk.StringVar(value="")

        controls = ttk.LabelFrame(self, text="セッション詳細")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(controls, textvariable=self.session_id_var, width=28, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        self.session_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_tab())
        ttk.Button(controls, text="集計更新", command=self._aggregate).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="平均ボルタモグラム出力", command=self._generate_mean).grid(row=0, column=3, padx=6)
        ttk.Button(controls, text="セッション出力", command=self._export_bundle).grid(row=0, column=4, padx=6)
        ttk.Label(controls, textvariable=self.summary_var, justify="left").grid(row=1, column=0, columnspan=5, sticky="w", padx=6, pady=(0, 6))

        tables = ttk.Frame(self)
        tables.pack(fill="both", expand=True, padx=12, pady=12)

        self.condition_columns = ("condition_id", "concentration_value", "method", "actual_replicates", "n_valid", "n_invalid", "condition_status", "warning")
        self.condition_tree = ttk.Treeview(
            tables,
            columns=self.condition_columns,
            show="headings",
            height=8,
        )
        for column, heading in zip(
            self.condition_columns,
            ("条件 ID", "濃度", "測定法", "実測", "valid", "invalid", "状態", "差分"),
        ):
            self.condition_tree.heading(column, text=heading)
            self.condition_tree.column(column, width=110 if column != "warning" else 200)
        self.condition_tree.pack(fill="x", pady=(0, 12))
        self.condition_tree.bind("<Double-1>", self._handle_condition_double_click)

        self.measurement_columns = ("measurement_id", "condition_id", "rep_no", "measured_at", "final_quality_flag", "raw_file_path")
        self.measurement_tree = ttk.Treeview(
            tables,
            columns=self.measurement_columns,
            show="headings",
            height=10,
        )
        for column, heading in zip(
            self.measurement_columns,
            ("測定 ID", "条件 ID", "rep", "測定日時", "品質", "raw_file_path"),
        ):
            self.measurement_tree.heading(column, text=heading)
            self.measurement_tree.column(column, width=140 if column != "raw_file_path" else 280)
        self.measurement_tree.pack(fill="both", expand=True)
        self.measurement_tree.bind("<Double-1>", self._handle_measurement_double_click)

    def _aggregate(self) -> None:
        try:
            self.services.aggregate_session(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション集計", str(error))

    def _generate_mean(self) -> None:
        try:
            self.services.generate_mean_voltammograms(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("平均ボルタモグラム", str(error))

    def _export_bundle(self) -> None:
        try:
            outputs = self.services.export_session_bundle(self.session_id_var.get())
            messagebox.showinfo("セッション出力", "\n".join(f"{key}: {value}" for key, value in outputs.items()))
        except Exception as error:
            messagebox.showerror("セッション出力", str(error))

    def _handle_condition_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.condition_tree, self.condition_columns, event, "session_detail_condition")
        if target:
            navigator(*target)

    def _handle_measurement_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.measurement_tree, self.measurement_columns, event, "session_detail_measurement")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
            self.session_id_var.set(session_ids[0])
        if not self.session_id_var.get():
            return

        detail = self.services.get_session_detail(self.session_id_var.get())
        session = detail["session"]
        self.summary_var.set(
            f"{session['session_date']} / {session['session_name']} / 測定対象物質={session['analyte']} / 測定法={session.get('method_default', '')}"
        )
        condition_warnings = detail.get("condition_warnings", {})

        for tree in (self.condition_tree, self.measurement_tree):
            for item in tree.get_children():
                tree.delete(item)

        for row in detail["conditions"]:
            self.condition_tree.insert(
                "",
                "end",
                values=(
                    row["condition_id"],
                    row["concentration_value"],
                    row["method"],
                    row.get("actual_replicates", 0),
                    row.get("n_valid", 0),
                    row.get("n_invalid", 0),
                    row.get("condition_status", ""),
                    condition_warnings.get(str(row["condition_id"]), ""),
                ),
            )
        for row in detail["measurements"]:
            self.measurement_tree.insert(
                "",
                "end",
                values=(
                    row["measurement_id"],
                    row["condition_id"],
                    row["rep_no"],
                    row.get("measured_at", ""),
                    row.get("final_quality_flag", ""),
                    row.get("raw_file_path", ""),
                ),
            )
