from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class BatchPlanTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.session_id_var = tk.StringVar()
        self.baseline_var = tk.StringVar(value="0")
        self.mode_var = tk.StringVar(value="randomized_blocks")

        controls = ttk.LabelFrame(self, text="バッチ実行計画")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(controls, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        ttk.Label(controls, text="ベースライン濃度").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.baseline_var, width=12).grid(row=0, column=3, sticky="w")
        ttk.Label(controls, text="実行モード").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            controls,
            textvariable=self.mode_var,
            width=20,
            state="readonly",
            values=("fixed", "randomized_blocks", "fully_randomized"),
        ).grid(row=0, column=5, sticky="w")
        ttk.Button(controls, text="計画生成", command=self._generate_plan).grid(row=0, column=6, padx=6)
        ttk.Button(controls, text="failed 再キュー化", command=self._requeue_failed).grid(row=0, column=7, padx=6)

        columns = ("batch_item_id", "session_id", "condition_id", "planned_order", "rep_no", "planned_status", "assigned_measurement_id")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=15)
        headings = ["バッチ ID", "セッション", "条件 ID", "順序", "rep", "状態", "測定 ID"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _generate_plan(self) -> None:
        try:
            baseline = float(self.baseline_var.get()) if self.baseline_var.get() else None
            self.services.generate_batch_plan(self.session_id_var.get(), baseline, self.mode_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("バッチ計画", str(error))

    def _requeue_failed(self) -> None:
        try:
            self.services.repository.requeue_failed_batch_items(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("再キュー化", str(error))

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
            self.session_id_var.set(session_ids[0])
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_batch_items():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["batch_item_id"],
                    row["session_id"],
                    row["condition_id"],
                    row["planned_order"],
                    row["rep_no"],
                    row["planned_status"],
                    row.get("assigned_measurement_id", ""),
                ),
            )
