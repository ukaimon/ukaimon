from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class ExportTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.session_id_var = tk.StringVar()
        self.last_output_var = tk.StringVar(value="まだ出力していません")

        controls = ttk.LabelFrame(self, text="レポート出力")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(controls, textvariable=self.session_id_var, width=28, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        ttk.Button(controls, text="セッション一式出力", command=self._export_session).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="横断 CSV 出力", command=self._export_cross).grid(row=0, column=3, padx=6)
        ttk.Label(controls, textvariable=self.last_output_var, justify="left").grid(row=1, column=0, columnspan=4, sticky="w", padx=6, pady=(0, 6))

    def _export_session(self) -> None:
        try:
            outputs = self.services.export_session_bundle(self.session_id_var.get())
            self.last_output_var.set("\n".join(f"{key}: {value}" for key, value in outputs.items()))
        except Exception as error:
            messagebox.showerror("セッション出力", str(error))

    def _export_cross(self) -> None:
        try:
            path = self.services.export_cross_report({})
            self.last_output_var.set(path)
        except Exception as error:
            messagebox.showerror("横断出力", str(error))

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
            self.session_id_var.set(session_ids[0])
