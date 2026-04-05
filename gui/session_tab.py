from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from utils.date_utils import today_string


class SessionTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.mip_usage_id_var = tk.StringVar()
        self.session_date_var = tk.StringVar(value=today_string())
        self.analyte_var = tk.StringVar()
        self.session_name_var = tk.StringVar()
        self.method_var = tk.StringVar(value="CV")
        self.operator_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="セッション登録")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="使用 ID").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.usage_combo = ttk.Combobox(form, textvariable=self.mip_usage_id_var, width=24, state="readonly")
        self.usage_combo.grid(row=0, column=1, sticky="w")
        ttk.Label(form, text="測定日").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.session_date_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="Analyte").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.analyte_var, width=24).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="セッション名").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.session_name_var, width=28).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="測定法").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.method_var, width=16).grid(row=1, column=3, sticky="w")
        ttk.Label(form, text="担当者").grid(row=1, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.operator_var, width=20).grid(row=1, column=5, sticky="w")
        ttk.Button(form, text="新規追加", command=self._create_session).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left")

        columns = ("session_id", "session_date", "analyte", "session_name", "mip_usage_id", "method_default")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=12)
        headings = ["セッション ID", "測定日", "Analyte", "セッション名", "使用 ID", "測定法"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=160)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _create_session(self) -> None:
        try:
            self.services.create_session(
                {
                    "mip_usage_id": self.mip_usage_id_var.get(),
                    "session_date": self.session_date_var.get(),
                    "analyte": self.analyte_var.get(),
                    "session_name": self.session_name_var.get(),
                    "method_default": self.method_var.get(),
                    "operator": self.operator_var.get(),
                    "electrolyte": "",
                    "common_note": "",
                    "tags": "",
                    "status": "draft",
                }
            )
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション登録", str(error))

    def _duplicate_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        session_id = self.tree.item(selection[0], "values")[0]
        try:
            self.services.duplicate_session(session_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション複製", str(error))

    def refresh_tab(self) -> None:
        usage_ids = [row["mip_usage_id"] for row in self.services.list_mip_usages()]
        self.usage_combo["values"] = usage_ids
        if usage_ids and not self.mip_usage_id_var.get():
            self.mip_usage_id_var.set(usage_ids[0])

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_sessions():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["session_id"],
                    row["session_date"],
                    row["analyte"],
                    row.get("session_name", ""),
                    row["mip_usage_id"],
                    row.get("method_default", ""),
                ),
            )
