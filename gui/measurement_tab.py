from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class MeasurementTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.session_id_var = tk.StringVar()
        self.condition_id_var = tk.StringVar()
        self.chip_id_var = tk.StringVar()
        self.wire_id_var = tk.StringVar()
        self.status_var = tk.StringVar(value="manual")
        self.noise_var = tk.StringVar(value="0.0")
        self.memo_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="測定追加")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(form, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        self.session_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_condition_choices())
        ttk.Label(form, text="条件").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        self.condition_combo = ttk.Combobox(form, textvariable=self.condition_id_var, width=24, state="readonly")
        self.condition_combo.grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="chip_id").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.chip_id_var, width=20).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="wire_id").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.wire_id_var, width=20).grid(row=1, column=3, sticky="w")
        ttk.Label(form, text="status").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.status_var, width=20).grid(row=2, column=1, sticky="w")
        ttk.Label(form, text="noise_level").grid(row=2, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.noise_var, width=20).grid(row=2, column=3, sticky="w")
        ttk.Label(form, text="メモ").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.memo_var, width=60).grid(row=3, column=1, columnspan=3, sticky="we")
        ttk.Button(form, text="測定保存", command=self._create_measurement).grid(row=0, column=4, rowspan=4, padx=6)

        columns = ("measurement_id", "session_id", "condition_id", "rep_no", "measured_at", "status", "final_quality_flag", "raw_file_path")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=14)
        headings = ["測定 ID", "セッション", "条件", "rep", "測定日時", "状態", "品質", "raw_file_path"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140 if column != "raw_file_path" else 260)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _refresh_condition_choices(self) -> None:
        session_id = self.session_id_var.get()
        conditions = self.services.list_conditions(session_id) if session_id else []
        condition_ids = [row["condition_id"] for row in conditions]
        self.condition_combo["values"] = condition_ids
        if condition_ids and self.condition_id_var.get() not in condition_ids:
            self.condition_id_var.set(condition_ids[0])

    def _create_measurement(self) -> None:
        try:
            self.services.create_measurement(
                {
                    "session_id": self.session_id_var.get(),
                    "condition_id": self.condition_id_var.get(),
                    "chip_id": self.chip_id_var.get(),
                    "wire_id": self.wire_id_var.get(),
                    "status": self.status_var.get(),
                    "noise_level": float(self.noise_var.get()) if self.noise_var.get() else None,
                    "coating_quality": "",
                    "electrode_condition": "",
                    "bubbling_condition": "",
                    "free_memo": self.memo_var.get(),
                    "raw_file_path": "",
                    "exclusion_reason": "",
                }
            )
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("測定保存", str(error))

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
            self.session_id_var.set(session_ids[0])
        self._refresh_condition_choices()

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_measurements():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["measurement_id"],
                    row["session_id"],
                    row["condition_id"],
                    row["rep_no"],
                    row.get("measured_at", ""),
                    row.get("status", ""),
                    row.get("final_quality_flag", ""),
                    row.get("raw_file_path", ""),
                ),
            )
