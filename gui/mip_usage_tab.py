from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from utils.date_utils import today_string


class MipUsageTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_usage_id: str | None = None
        self.save_button_label = tk.StringVar(value="新規追加")
        self.mip_id_var = tk.StringVar()
        self.cp_date_var = tk.StringVar(value=today_string())
        self.coating_date_var = tk.StringVar(value=today_string())
        self.operator_var = tk.StringVar()
        self.note_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="MIP 使用記録")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="MIP ID").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.mip_combo = ttk.Combobox(form, textvariable=self.mip_id_var, width=24, state="readonly")
        self.mip_combo.grid(row=0, column=1, sticky="w")
        ttk.Label(form, text="CP 調製日").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.cp_date_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="塗布日").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.coating_date_var, width=16).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="担当者").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.operator_var, width=24).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="メモ").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.note_var, width=48).grid(row=1, column=3, columnspan=3, sticky="we")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_usage).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        columns = ("mip_usage_id", "mip_id", "cp_preparation_date", "coating_date", "operator", "note")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=12)
        headings = ["使用 ID", "MIP ID", "CP 調製日", "塗布日", "担当者", "メモ"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140 if column != "note" else 260)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _selected_usage_id(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            return None
        return str(self.tree.item(selection[0], "values")[0])

    def _load_selected(self) -> None:
        usage_id = self._selected_usage_id()
        if not usage_id:
            return
        row = self.services.repository.get_record("mip_usage_records", usage_id)
        if not row:
            messagebox.showerror("MIP 使用記録編集", "選択した使用記録が見つかりません。")
            return
        self.editing_usage_id = usage_id
        self.save_button_label.set("更新保存")
        self.mip_id_var.set(str(row.get("mip_id", "")))
        self.cp_date_var.set(str(row.get("cp_preparation_date", "")))
        self.coating_date_var.set(str(row.get("coating_date", "")))
        self.operator_var.set(str(row.get("operator", "")))
        self.note_var.set(str(row.get("note", "")))

    def _reset_form(self) -> None:
        self.editing_usage_id = None
        self.save_button_label.set("新規追加")
        self.cp_date_var.set(today_string())
        self.coating_date_var.set(today_string())
        self.operator_var.set("")
        self.note_var.set("")

    def _save_usage(self) -> None:
        payload = {
            "mip_id": self.mip_id_var.get(),
            "cp_preparation_date": self.cp_date_var.get(),
            "coating_date": self.coating_date_var.get(),
            "operator": self.operator_var.get(),
            "note": self.note_var.get(),
            "tags": "",
        }
        try:
            if self.editing_usage_id:
                self.services.update_mip_usage(self.editing_usage_id, payload)
            else:
                self.services.create_mip_usage(payload)
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("MIP 使用記録", str(error))

    def _duplicate_selected(self) -> None:
        usage_id = self._selected_usage_id()
        if not usage_id:
            return
        try:
            self.services.duplicate_mip_usage(usage_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("MIP 使用記録複製", str(error))

    def _delete_selected(self) -> None:
        usage_id = self._selected_usage_id()
        if not usage_id:
            return
        if not messagebox.askyesno("MIP 使用記録削除", "選択した MIP 使用記録を削除しますか？"):
            return
        try:
            message = self.services.delete_mip_usage(usage_id)
            self._reset_form()
            self.refresh_app()
            messagebox.showinfo("MIP 使用記録削除", message)
        except Exception as error:
            messagebox.showerror("MIP 使用記録削除", str(error))

    def refresh_tab(self) -> None:
        mip_ids = [row["mip_id"] for row in self.services.list_mips()]
        self.mip_combo["values"] = mip_ids
        if mip_ids and self.mip_id_var.get() not in mip_ids:
            self.mip_id_var.set(mip_ids[0])

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_mip_usages():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["mip_usage_id"],
                    row["mip_id"],
                    row.get("cp_preparation_date", ""),
                    row.get("coating_date", ""),
                    row.get("operator", ""),
                    row.get("note", ""),
                ),
            )
