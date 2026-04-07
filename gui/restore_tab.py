from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from gui.navigation import get_selected_tree_values


RESTORE_TYPE_LABELS = {
    "mip": "MIP",
    "session": "セッション",
    "condition": "条件",
    "measurement": "測定",
}


class RestoreTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.record_type_var = tk.StringVar(value="セッション")

        controls = ttk.LabelFrame(self, text="削除済みデータの復元")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="対象").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.record_type_combo = ttk.Combobox(
            controls,
            textvariable=self.record_type_var,
            width=18,
            state="readonly",
            values=tuple(RESTORE_TYPE_LABELS.values()),
        )
        self.record_type_combo.grid(row=0, column=1, sticky="w")
        self.record_type_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_tab())
        ttk.Button(controls, text="復元", command=self._restore_selected).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="再読込", command=self.refresh_tab).grid(row=0, column=3, padx=6)

        self.tree_columns = ("record_id", "summary", "deleted_at")
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=18)
        for column_name, heading, width in (
            ("record_id", "ID", 260),
            ("summary", "概要", 420),
            ("deleted_at", "削除日時", 200),
        ):
            self.tree.heading(column_name, text=heading)
            self.tree.column(column_name, width=width)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _selected_record_id(self) -> str | None:
        selected_ids = get_selected_tree_values(self.tree)
        return selected_ids[0] if selected_ids else None

    def _restore_selected(self) -> None:
        record_id = self._selected_record_id()
        if not record_id:
            return
        try:
            record_type = next(
                key for key, label in RESTORE_TYPE_LABELS.items() if label == self.record_type_var.get()
            )
            message = self.services.restore_deleted_record(record_type, record_id)
            self.refresh_app()
            messagebox.showinfo("復元", message)
        except Exception as error:
            messagebox.showerror("復元", str(error))

    def refresh_tab(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        record_type = next(
            key for key, label in RESTORE_TYPE_LABELS.items() if label == self.record_type_var.get()
        )
        for row in self.services.list_deleted_records(record_type):
            self.tree.insert("", "end", values=(row["record_id"], row["summary"], row["deleted_at"]))
