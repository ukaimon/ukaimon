from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from utils.date_utils import today_string


class MipTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.template_name_var = tk.StringVar()
        self.preparation_date_var = tk.StringVar(value=today_string())
        self.operator_var = tk.StringVar()
        self.note_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="MIP 登録")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="テンプレート名").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.template_name_var, width=28).grid(row=0, column=1, sticky="w")
        ttk.Label(form, text="調製日").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.preparation_date_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="担当者").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.operator_var, width=20).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="メモ").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.note_var, width=60).grid(row=1, column=1, columnspan=5, sticky="we", padx=(0, 6))
        ttk.Button(form, text="新規追加", command=self._create_mip).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left")

        columns = ("mip_id", "preparation_date", "template_name", "operator", "note")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=12)
        headings = ["MIP ID", "調製日", "テンプレート名", "担当者", "メモ"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140 if column != "note" else 260)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _create_mip(self) -> None:
        try:
            self.services.create_mip(
                {
                    "template_name": self.template_name_var.get(),
                    "preparation_date": self.preparation_date_var.get(),
                    "operator": self.operator_var.get(),
                    "note": self.note_var.get(),
                    "tags": "",
                }
            )
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("MIP 登録", str(error))

    def _duplicate_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            return
        mip_id = self.tree.item(selection[0], "values")[0]
        try:
            self.services.duplicate_mip(mip_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("MIP 複製", str(error))

    def refresh_tab(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_mips():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["mip_id"],
                    row["preparation_date"],
                    row["template_name"],
                    row["operator"],
                    row.get("note", ""),
                ),
            )
