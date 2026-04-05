from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class CrossReportTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.analyte_var = tk.StringVar()
        self.method_var = tk.StringVar()
        self.keyword_var = tk.StringVar()
        self.quality_var = tk.StringVar()
        self.mip_id_var = tk.StringVar()

        controls = ttk.LabelFrame(self, text="横断検索・比較")
        controls.pack(fill="x", padx=12, pady=12)
        entries = [
            ("Analyte", self.analyte_var),
            ("測定法", self.method_var),
            ("キーワード", self.keyword_var),
            ("品質", self.quality_var),
            ("MIP ID", self.mip_id_var),
        ]
        for index, (label, variable) in enumerate(entries):
            ttk.Label(controls, text=label).grid(row=0, column=index * 2, sticky="w", padx=6, pady=6)
            widget: ttk.Widget
            if label == "品質":
                widget = ttk.Combobox(controls, textvariable=variable, width=12, values=("", "valid", "suspect", "invalid"))
            else:
                widget = ttk.Entry(controls, textvariable=variable, width=16)
            widget.grid(row=0, column=index * 2 + 1, sticky="w")
        ttk.Button(controls, text="検索", command=self.refresh_tab).grid(row=0, column=10, padx=6)
        ttk.Button(controls, text="CSV 出力", command=self._export_csv).grid(row=0, column=11, padx=6)

        columns = (
            "measurement_id",
            "session_id",
            "analyte",
            "condition_id",
            "concentration_value",
            "method",
            "final_quality_flag",
            "representative_current_a",
        )
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=18)
        headings = ["測定 ID", "セッション", "Analyte", "条件", "濃度", "測定法", "品質", "代表電流"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _filters(self) -> dict[str, str]:
        return {
            "analyte": self.analyte_var.get(),
            "method": self.method_var.get(),
            "keyword": self.keyword_var.get(),
            "quality_flag": self.quality_var.get(),
            "mip_id": self.mip_id_var.get(),
        }

    def _export_csv(self) -> None:
        try:
            path = self.services.export_cross_report(self._filters())
            messagebox.showinfo("横断 CSV", path)
        except Exception as error:
            messagebox.showerror("横断 CSV", str(error))

    def refresh_tab(self) -> None:
        frame = self.services.repository.search_cross_measurements(self._filters())
        for item in self.tree.get_children():
            self.tree.delete(item)
        for _, row in frame.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    row.get("measurement_id", ""),
                    row.get("session_id", ""),
                    row.get("analyte", ""),
                    row.get("condition_id", ""),
                    row.get("concentration_value", ""),
                    row.get("method", ""),
                    row.get("final_quality_flag", ""),
                    row.get("representative_current_a", ""),
                ),
            )
