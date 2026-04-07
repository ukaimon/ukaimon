from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices
from gui.navigation import extract_tree_navigation_target, select_tree_record
from utils.date_utils import today_string


class SessionTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_session_id: str | None = None
        self.save_button_label = tk.StringVar(value="新規追加")
        self.mip_usage_id_var = tk.StringVar()
        self.session_date_var = tk.StringVar(value=today_string())
        self.analyte_var = tk.StringVar()
        self.session_name_var = tk.StringVar()
        self.method_var = tk.StringVar(value="CV")
        self.operator_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="セッション登録 / 編集")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="使用 ID").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.usage_combo = ttk.Combobox(form, textvariable=self.mip_usage_id_var, width=24, state="readonly")
        self.usage_combo.grid(row=0, column=1, sticky="w")
        self.usage_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_usage_selected())
        ttk.Label(form, text="測定日").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.session_date_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="測定対象物質").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.analyte_var, width=24).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="セッション名").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.session_name_var, width=28).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="測定法").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.method_var, width=16).grid(row=1, column=3, sticky="w")
        ttk.Label(form, text="担当者").grid(row=1, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.operator_var, width=20).grid(row=1, column=5, sticky="w")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_session).grid(row=0, column=6, rowspan=2, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        self.tree_columns = ("session_id", "session_date", "analyte", "session_name", "mip_usage_id", "method_default")
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=12)
        headings = ["セッション ID", "測定日", "測定対象物質", "セッション名", "使用 ID", "測定法"]
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=160)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)

    def _apply_default_operator(self, force: bool = False) -> None:
        if self.editing_session_id:
            return
        if force or not self.operator_var.get().strip():
            self.operator_var.set(self.services.get_default_session_operator(self.mip_usage_id_var.get() or None))

    def _on_usage_selected(self) -> None:
        self._apply_default_operator(force=True)

    def _selected_session_id(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            return None
        return str(self.tree.item(selection[0], "values")[0])

    def _load_selected(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            return
        row = self.services.repository.get_record("sessions", session_id)
        if not row:
            messagebox.showerror("セッション編集", "選択したセッションが見つかりません。")
            return
        self.editing_session_id = session_id
        self.save_button_label.set("更新保存")
        self.mip_usage_id_var.set(str(row.get("mip_usage_id", "")))
        self.session_date_var.set(str(row.get("session_date", "")))
        self.analyte_var.set(str(row.get("analyte", "")))
        self.session_name_var.set(str(row.get("session_name", "")))
        self.method_var.set(str(row.get("method_default", "")))
        self.operator_var.set(str(row.get("operator", "")))

    def focus_record(self, session_id: str) -> None:
        if select_tree_record(self.tree, session_id):
            self._load_selected()

    def _reset_form(self) -> None:
        self.editing_session_id = None
        self.save_button_label.set("新規追加")
        self.session_date_var.set(today_string())
        self.analyte_var.set("")
        self.session_name_var.set("")
        self.method_var.set("CV")
        self.operator_var.set("")
        self._apply_default_operator(force=True)

    def _save_session(self) -> None:
        payload = {
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
        try:
            if self.editing_session_id:
                self.services.update_session(self.editing_session_id, payload)
            else:
                self.services.create_session(payload)
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション登録", str(error))

    def _duplicate_selected(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            return
        try:
            self.services.duplicate_session(session_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション複製", str(error))

    def _delete_selected(self) -> None:
        session_id = self._selected_session_id()
        if not session_id:
            return
        if not messagebox.askyesno("セッション削除", "選択したセッションを削除しますか？"):
            return
        try:
            message = self.services.delete_session(session_id)
            self._reset_form()
            self.refresh_app()
            messagebox.showinfo("セッション削除", message)
        except Exception as error:
            messagebox.showerror("セッション削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "session")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        usage_ids = [row["mip_usage_id"] for row in self.services.list_mip_usages()]
        self.usage_combo["values"] = usage_ids
        selection_changed = False
        if usage_ids and self.mip_usage_id_var.get() not in usage_ids:
            self.mip_usage_id_var.set(usage_ids[0])
            selection_changed = True

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
        self._apply_default_operator(force=selection_changed)
