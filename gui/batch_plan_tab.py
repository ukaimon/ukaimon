from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.models import PlannedStatus
from core.services import AppServices
from gui.navigation import extract_tree_navigation_target, select_tree_record


class BatchPlanTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_batch_item_id: str | None = None
        self.session_id_var = tk.StringVar()
        self.baseline_var = tk.StringVar(value="0")
        self.mode_var = tk.StringVar(value="randomized_blocks")
        self.edit_session_id_var = tk.StringVar()
        self.edit_condition_id_var = tk.StringVar()
        self.edit_order_var = tk.StringVar()
        self.edit_rep_var = tk.StringVar()
        self.edit_status_var = tk.StringVar(value=PlannedStatus.WAITING.value)
        self.edit_note_var = tk.StringVar()

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

        edit_form = ttk.LabelFrame(self, text="選択中の項目編集")
        edit_form.pack(fill="x", padx=12, pady=(0, 12))
        ttk.Label(edit_form, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.edit_session_combo = ttk.Combobox(edit_form, textvariable=self.edit_session_id_var, width=24, state="readonly")
        self.edit_session_combo.grid(row=0, column=1, sticky="w")
        self.edit_session_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_edit_condition_choices())
        ttk.Label(edit_form, text="条件").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        self.edit_condition_combo = ttk.Combobox(edit_form, textvariable=self.edit_condition_id_var, width=24, state="readonly")
        self.edit_condition_combo.grid(row=0, column=3, sticky="w")
        ttk.Label(edit_form, text="順序").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(edit_form, textvariable=self.edit_order_var, width=12).grid(row=1, column=1, sticky="w")
        ttk.Label(edit_form, text="rep").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(edit_form, textvariable=self.edit_rep_var, width=12).grid(row=1, column=3, sticky="w")
        ttk.Label(edit_form, text="状態").grid(row=1, column=4, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            edit_form,
            textvariable=self.edit_status_var,
            width=20,
            state="readonly",
            values=tuple(status.value for status in PlannedStatus),
        ).grid(row=1, column=5, sticky="w")
        ttk.Label(edit_form, text="メモ").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(edit_form, textvariable=self.edit_note_var, width=60).grid(row=2, column=1, columnspan=5, sticky="we")
        ttk.Button(edit_form, text="更新保存", command=self._save_selected).grid(row=0, column=6, rowspan=3, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        self.tree_columns = ("batch_item_id", "session_id", "condition_id", "planned_order", "rep_no", "planned_status", "assigned_measurement_id")
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=15)
        headings = ["バッチ ID", "セッション", "条件 ID", "順序", "rep", "状態", "測定 ID"]
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)

    def _selected_batch_item_id(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            return None
        return str(self.tree.item(selection[0], "values")[0])

    def _refresh_edit_condition_choices(self) -> None:
        session_id = self.edit_session_id_var.get()
        conditions = self.services.list_conditions(session_id) if session_id else []
        condition_ids = [row["condition_id"] for row in conditions]
        self.edit_condition_combo["values"] = condition_ids
        if condition_ids and self.edit_condition_id_var.get() not in condition_ids:
            self.edit_condition_id_var.set(condition_ids[0])

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

    def _load_selected(self) -> None:
        batch_item_id = self._selected_batch_item_id()
        if not batch_item_id:
            return
        row = self.services.repository.get_record("batch_plan_items", batch_item_id)
        if not row or int(row.get("is_deleted", 0)) == 1:
            messagebox.showerror("バッチ計画編集", "選択した項目が見つかりません。")
            return
        self.editing_batch_item_id = batch_item_id
        self.edit_session_id_var.set(str(row.get("session_id", "")))
        self._refresh_edit_condition_choices()
        self.edit_condition_id_var.set(str(row.get("condition_id", "")))
        self.edit_order_var.set(str(row.get("planned_order", "")))
        self.edit_rep_var.set(str(row.get("rep_no", "")))
        self.edit_status_var.set(str(row.get("planned_status", PlannedStatus.WAITING.value)))
        self.edit_note_var.set(str(row.get("note", "")))

    def focus_record(self, batch_item_id: str) -> None:
        if select_tree_record(self.tree, batch_item_id):
            self._load_selected()

    def _reset_form(self) -> None:
        self.editing_batch_item_id = None
        self.edit_session_id_var.set("")
        self.edit_condition_id_var.set("")
        self.edit_order_var.set("")
        self.edit_rep_var.set("")
        self.edit_status_var.set(PlannedStatus.WAITING.value)
        self.edit_note_var.set("")
        self._refresh_edit_condition_choices()

    def _save_selected(self) -> None:
        if not self.editing_batch_item_id:
            messagebox.showinfo("バッチ計画編集", "編集したい項目を一覧から選択してください。")
            return
        try:
            self.services.update_batch_item(
                self.editing_batch_item_id,
                {
                    "session_id": self.edit_session_id_var.get(),
                    "condition_id": self.edit_condition_id_var.get(),
                    "planned_order": int(self.edit_order_var.get()),
                    "rep_no": int(self.edit_rep_var.get()),
                    "planned_status": self.edit_status_var.get(),
                    "note": self.edit_note_var.get(),
                },
            )
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("バッチ計画編集", str(error))

    def _delete_selected(self) -> None:
        batch_item_id = self._selected_batch_item_id()
        if not batch_item_id:
            return
        if not messagebox.askyesno("バッチ計画削除", "選択した項目を削除しますか？"):
            return
        try:
            message = self.services.delete_batch_item(batch_item_id)
            self._reset_form()
            self.refresh_app()
            messagebox.showinfo("バッチ計画削除", message)
        except Exception as error:
            messagebox.showerror("バッチ計画削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "batch_plan")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and self.session_id_var.get() not in session_ids:
            self.session_id_var.set(session_ids[0])
        self.edit_session_combo["values"] = session_ids
        if self.editing_batch_item_id and self.edit_session_id_var.get():
            self._refresh_edit_condition_choices()
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
