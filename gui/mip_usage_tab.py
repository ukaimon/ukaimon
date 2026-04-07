from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.mip_usage_fields import MIP_USAGE_FIELD_GROUPS, MIP_USAGE_FIELD_SPECS, with_mip_usage_field_defaults
from core.services import AppServices
from gui.navigation import enable_bulk_tree_actions, extract_tree_navigation_target, get_selected_tree_values, select_tree_record
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
        self.detail_vars = {
            spec.key: tk.StringVar(value=spec.default_value)
            for spec in MIP_USAGE_FIELD_SPECS
        }

        form = ttk.LabelFrame(self, text="MIP 使用記録")
        form.pack(fill="x", padx=12, pady=12)
        form.columnconfigure(3, weight=1)
        form.columnconfigure(5, weight=1)
        ttk.Label(form, text="MIP ID").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.mip_combo = ttk.Combobox(form, textvariable=self.mip_id_var, width=24, state="readonly")
        self.mip_combo.grid(row=0, column=1, sticky="w")
        self.mip_combo.bind("<<ComboboxSelected>>", lambda _event: self._on_mip_selected())
        ttk.Label(form, text="CP 調製日").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.cp_date_var, width=16).grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="塗布日").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.coating_date_var, width=16).grid(row=0, column=5, sticky="w")
        ttk.Label(form, text="担当者").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.operator_var, width=24).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="メモ").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.note_var, width=48).grid(row=1, column=3, columnspan=3, sticky="we")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_usage).grid(row=0, column=6, rowspan=2, padx=6)
        details = ttk.Frame(form)
        details.grid(row=2, column=0, columnspan=7, sticky="we", padx=6, pady=(0, 6))
        for column_index, (group_name, specs) in enumerate(MIP_USAGE_FIELD_GROUPS):
            details.columnconfigure(column_index, weight=1)
            group = ttk.LabelFrame(details, text=group_name)
            group.grid(row=0, column=column_index, sticky="nsew", padx=(0, 6 if column_index < len(MIP_USAGE_FIELD_GROUPS) - 1 else 0))
            for row_index, spec in enumerate(specs):
                ttk.Label(group, text=spec.label).grid(row=row_index, column=0, sticky="w", padx=6, pady=4)
                ttk.Entry(group, textvariable=self.detail_vars[spec.key], width=14).grid(
                    row=row_index,
                    column=1,
                    sticky="w",
                    padx=(0, 6),
                    pady=4,
                )

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        self.tree_columns = ("mip_usage_id", "mip_id", "cp_preparation_date", "coating_date", "operator", "note")
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=12)
        headings = ["使用 ID", "MIP ID", "CP 調製日", "塗布日", "担当者", "メモ"]
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140 if column != "note" else 260)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)
        enable_bulk_tree_actions(self.tree)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)

    def _apply_default_operator(self, force: bool = False) -> None:
        if self.editing_usage_id:
            return
        if force or not self.operator_var.get().strip():
            self.operator_var.set(self.services.get_default_mip_usage_operator(self.mip_id_var.get() or None))

    def _on_mip_selected(self) -> None:
        self._apply_default_operator(force=True)

    def _selected_usage_id(self) -> str | None:
        selected_ids = self._selected_usage_ids()
        if not selected_ids:
            return None
        return selected_ids[0]

    def _selected_usage_ids(self) -> list[str]:
        return get_selected_tree_values(self.tree)

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
        self._set_detail_values(row)

    def focus_record(self, mip_usage_id: str) -> None:
        if select_tree_record(self.tree, mip_usage_id):
            self._load_selected()

    def _set_detail_values(self, payload: dict[str, object] | None = None) -> None:
        detail_values = with_mip_usage_field_defaults(payload)
        for spec in MIP_USAGE_FIELD_SPECS:
            self.detail_vars[spec.key].set(str(detail_values[spec.key]))

    def _reset_form(self) -> None:
        self.editing_usage_id = None
        self.save_button_label.set("新規追加")
        self.cp_date_var.set(today_string())
        self.coating_date_var.set(today_string())
        self.operator_var.set("")
        self.note_var.set("")
        self._set_detail_values()
        self._apply_default_operator(force=True)

    def _save_usage(self) -> None:
        payload = {
            "mip_id": self.mip_id_var.get(),
            "cp_preparation_date": self.cp_date_var.get(),
            "coating_date": self.coating_date_var.get(),
            "operator": self.operator_var.get(),
            "note": self.note_var.get(),
            "tags": "",
            **{spec.key: self.detail_vars[spec.key].get() for spec in MIP_USAGE_FIELD_SPECS},
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
        usage_ids = self._selected_usage_ids()
        if not usage_ids:
            return
        label = (
            "選択した MIP 使用記録を削除しますか？"
            if len(usage_ids) == 1
            else f"選択した {len(usage_ids)} 件の MIP 使用記録を削除しますか？"
        )
        if not messagebox.askyesno("MIP 使用記録削除", label):
            return
        try:
            messages = [self.services.delete_mip_usage(usage_id) for usage_id in usage_ids]
            self._reset_form()
            self.refresh_app()
            summary = messages[0] if len(messages) == 1 else f"{len(usage_ids)} 件を削除しました。"
            messagebox.showinfo("MIP 使用記録削除", summary)
        except Exception as error:
            messagebox.showerror("MIP 使用記録削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "mip_usage")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        mip_ids = [row["mip_id"] for row in self.services.list_mips()]
        self.mip_combo["values"] = mip_ids
        selection_changed = False
        if mip_ids and self.mip_id_var.get() not in mip_ids:
            self.mip_id_var.set(mip_ids[0])
            selection_changed = True
        if not mip_ids:
            self.mip_id_var.set("")

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
        self._apply_default_operator(force=selection_changed)
