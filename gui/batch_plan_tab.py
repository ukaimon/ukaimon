from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.models import PlannedStatus
from core.services import AppServices
from gui.navigation import enable_bulk_tree_actions, extract_tree_navigation_target, get_selected_tree_values, select_tree_record


IVIUM_DEVICE_STATUS_LABELS = {
    0: "idle",
    1: "idle(display)",
    2: "measuring",
    3: "measuring(display)",
    4: "pause",
    5: "requested pause",
}

EXECUTION_MODE_OPTIONS = (
    ("濃度順（repごと）", "fixed"),
    ("ブロックランダム", "randomized_blocks"),
    ("完全ランダム", "fully_randomized"),
    ("濃度ごと連続", "concentration_grouped"),
)
EXECUTION_MODE_LABEL_TO_KEY = {label: key for label, key in EXECUTION_MODE_OPTIONS}
EXECUTION_MODE_KEY_TO_LABEL = {key: label for label, key in EXECUTION_MODE_OPTIONS}


class BatchPlanTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_batch_item_id: str | None = None
        self.session_id_var = tk.StringVar()
        self.baseline_var = tk.StringVar(value="0")
        self.mode_var = tk.StringVar(value=EXECUTION_MODE_KEY_TO_LABEL["randomized_blocks"])
        self.edit_session_id_var = tk.StringVar()
        self.edit_condition_id_var = tk.StringVar()
        self.edit_order_var = tk.StringVar()
        self.edit_rep_var = tk.StringVar()
        self.edit_status_var = tk.StringVar(value=PlannedStatus.WAITING.value)
        self.edit_note_var = tk.StringVar()
        self.preview_batch_var = tk.StringVar(value="-")
        self.preview_condition_id_var = tk.StringVar(value="-")
        self.preview_concentration_var = tk.StringVar(value="濃度未選択")
        self.preview_method_var = tk.StringVar(value="-")
        self.preview_rep_var = tk.StringVar(value="-")
        self.plan_summary_var = tk.StringVar(value="計画一覧は未作成です。")
        self.ivium_status_var = tk.StringVar(value="idle")
        self.ivium_batch_var = tk.StringVar(value="-")
        self.ivium_condition_var = tk.StringVar(value="-")
        self.ivium_method_var = tk.StringVar(value="-")
        self.ivium_device_var = tk.StringVar(value="-")
        self.ivium_points_var = tk.StringVar(value="0")
        self.ivium_source_var = tk.StringVar(value="-")
        self.ivium_message_var = tk.StringVar(value="待機中")
        self._last_run_signature = ""

        controls = ttk.LabelFrame(self, text="バッチ実行計画")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(controls, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        self.session_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_tab())
        ttk.Label(controls, text="ベースライン濃度").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.baseline_var, width=12).grid(row=0, column=3, sticky="w")
        ttk.Label(controls, text="実行モード").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Combobox(
            controls,
            textvariable=self.mode_var,
            width=20,
            state="readonly",
            values=tuple(label for label, _key in EXECUTION_MODE_OPTIONS),
        ).grid(row=0, column=5, sticky="w")
        ttk.Button(controls, text="計画生成", command=self._generate_plan).grid(row=0, column=6, padx=6)
        ttk.Button(controls, text="failed 再キュー化", command=self._requeue_failed).grid(row=0, column=7, padx=6)
        ttk.Button(controls, text="IviumSoft 起動", command=self._launch_iviumsoft).grid(row=0, column=8, padx=6)
        ttk.Button(controls, text="選択項目を実行", command=self._run_selected).grid(row=0, column=9, padx=6)
        ttk.Button(controls, text="次の waiting を実行", command=self._run_next_waiting).grid(row=0, column=10, padx=6)
        ttk.Button(controls, text="中止", command=self._abort_run).grid(row=0, column=11, padx=6)

        preview_frame = ttk.LabelFrame(self, text="測定前確認")
        preview_frame.pack(fill="x", padx=12, pady=(0, 12))
        preview_frame.columnconfigure(1, weight=1)
        preview_frame.columnconfigure(3, weight=1)
        ttk.Label(preview_frame, text="測定濃度").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(
            preview_frame,
            textvariable=self.preview_concentration_var,
            font=("Yu Gothic UI", 16, "bold"),
            foreground="#8b0000",
        ).grid(row=0, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, text="バッチ ID").grid(row=1, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, textvariable=self.preview_batch_var).grid(row=1, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, text="条件 ID").grid(row=1, column=2, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, textvariable=self.preview_condition_id_var).grid(row=1, column=3, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, text="測定法").grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, textvariable=self.preview_method_var).grid(row=2, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, text="rep").grid(row=2, column=2, sticky="w", padx=6, pady=4)
        ttk.Label(preview_frame, textvariable=self.preview_rep_var).grid(row=2, column=3, sticky="w", padx=6, pady=4)
        ttk.Label(
            preview_frame,
            text="実行前にこの濃度が実サンプルと一致しているか確認してください。",
        ).grid(row=3, column=0, columnspan=4, sticky="w", padx=6, pady=(4, 6))

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

        status_frame = ttk.LabelFrame(self, text="Ivium 実行状態")
        status_frame.pack(fill="x", padx=12, pady=(0, 12))
        for column_index in range(4):
            status_frame.columnconfigure(column_index * 2 + 1, weight=1)
        ttk.Label(status_frame, text="状態").grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_status_var).grid(row=0, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="バッチ ID").grid(row=0, column=2, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_batch_var).grid(row=0, column=3, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="条件").grid(row=1, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_condition_var, wraplength=500).grid(row=1, column=1, columnspan=3, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="Method").grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_method_var).grid(row=2, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="Device").grid(row=2, column=2, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_device_var).grid(row=2, column=3, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="Points").grid(row=3, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_points_var).grid(row=3, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="Result").grid(row=3, column=2, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_source_var, wraplength=500).grid(row=3, column=3, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, text="Message").grid(row=4, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(status_frame, textvariable=self.ivium_message_var, wraplength=950).grid(row=4, column=1, columnspan=3, sticky="w", padx=6, pady=4)

        self.ivium_log_list = tk.Listbox(self, height=6)
        self.ivium_log_list.pack(fill="x", padx=12, pady=(0, 12))

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        list_frame = ttk.LabelFrame(self, text="計画一覧")
        list_frame.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(list_frame, textvariable=self.plan_summary_var, justify="left").pack(fill="x", padx=6, pady=(6, 0))

        self.tree_columns = (
            "batch_item_id",
            "planned_order",
            "condition_id",
            "concentration",
            "method",
            "rep_no",
            "planned_status",
            "assigned_measurement_id",
        )
        self.tree = ttk.Treeview(list_frame, columns=self.tree_columns, show="headings", height=15)
        headings = ["バッチ ID", "順序", "条件 ID", "濃度", "測定法", "rep", "状態", "測定 ID"]
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
        column_widths = {
            "batch_item_id": 170,
            "planned_order": 70,
            "condition_id": 170,
            "concentration": 120,
            "method": 110,
            "rep_no": 60,
            "planned_status": 110,
            "assigned_measurement_id": 170,
        }
        for column, width in column_widths.items():
            self.tree.column(column, width=width, anchor="w")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)
        enable_bulk_tree_actions(self.tree)
        self.tree.bind("<<TreeviewSelect>>", self._handle_tree_select)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)
        self.after(500, self._poll_ivium_state)

    def _selected_batch_item_id(self) -> str | None:
        selected_ids = self._selected_batch_item_ids()
        if not selected_ids:
            return None
        return selected_ids[0]

    def _selected_batch_item_ids(self) -> list[str]:
        return get_selected_tree_values(self.tree)

    def _refresh_edit_condition_choices(self) -> None:
        session_id = self.edit_session_id_var.get()
        conditions = self.services.list_conditions(session_id) if session_id else []
        condition_ids = [row["condition_id"] for row in conditions]
        self.edit_condition_combo["values"] = condition_ids
        if condition_ids and self.edit_condition_id_var.get() not in condition_ids:
            self.edit_condition_id_var.set(condition_ids[0])

    @staticmethod
    def _format_condition_concentration(condition_row: dict[str, object] | None) -> str:
        if not condition_row:
            return "-"
        value = condition_row.get("concentration_value")
        unit = str(condition_row.get("concentration_unit") or "").strip()
        if value in (None, ""):
            return unit or "-"
        if isinstance(value, float):
            value_text = f"{value:g}"
        else:
            value_text = str(value).strip()
        return f"{value_text} {unit}".strip()

    def _clear_run_preview(self) -> None:
        self.preview_batch_var.set("-")
        self.preview_condition_id_var.set("-")
        self.preview_concentration_var.set("濃度未選択")
        self.preview_method_var.set("-")
        self.preview_rep_var.set("-")

    def _apply_run_preview(self, preview: dict[str, object]) -> None:
        self.preview_batch_var.set(str(preview.get("batch_item_id") or "-"))
        self.preview_condition_id_var.set(str(preview.get("condition_id") or "-"))
        self.preview_concentration_var.set(str(preview.get("concentration_text") or "濃度未選択"))
        method_text = str(preview.get("method") or preview.get("ivium_method_name") or "-")
        if preview.get("ivium_method_name"):
            method_text = f"{preview.get('method') or '-'} / {preview.get('ivium_method_name')}"
        self.preview_method_var.set(method_text)
        self.preview_rep_var.set(str(preview.get("rep_no") or "-"))

    def _load_run_preview(self, batch_item_id: str | None = None, session_id: str | None = None) -> dict[str, object] | None:
        try:
            preview = self.services.get_ivium_batch_run_preview(batch_item_id=batch_item_id, session_id=session_id)
        except Exception:
            self._clear_run_preview()
            return None
        self._apply_run_preview(preview)
        return preview

    @staticmethod
    def _confirm_run(preview: dict[str, object]) -> bool:
        concentration_text = str(preview.get("concentration_text") or "-")
        return messagebox.askyesno(
            "測定前確認",
            "\n".join(
                [
                    "この条件で測定を開始します。",
                    "",
                    f"濃度: {concentration_text}",
                    f"条件 ID: {preview.get('condition_id') or '-'}",
                    f"バッチ ID: {preview.get('batch_item_id') or '-'}",
                    f"rep: {preview.get('rep_no') or '-'}",
                    "",
                    "実サンプルの濃度と一致していますか？",
                ]
            ),
        )

    def _selected_execution_mode(self) -> str:
        return EXECUTION_MODE_LABEL_TO_KEY.get(self.mode_var.get(), "randomized_blocks")

    def _generate_plan(self) -> None:
        try:
            baseline = float(self.baseline_var.get()) if self.baseline_var.get() else None
            self.services.generate_batch_plan(self.session_id_var.get(), baseline, self._selected_execution_mode())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("バッチ計画", str(error))

    def _requeue_failed(self) -> None:
        try:
            self.services.repository.requeue_failed_batch_items(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("再キュー化", str(error))

    def _launch_iviumsoft(self) -> None:
        try:
            self.services.launch_iviumsoft()
            self._poll_ivium_state()
        except Exception as error:
            messagebox.showerror("IviumSoft 起動", str(error))

    def _run_selected(self) -> None:
        batch_item_id = self._selected_batch_item_id()
        if not batch_item_id:
            messagebox.showinfo("Ivium 実行", "実行したいバッチ項目を選択してください。")
            return
        try:
            preview = self._load_run_preview(batch_item_id=batch_item_id)
            if not preview:
                raise ValueError("実行前確認用の濃度情報を取得できません。")
            if not self._confirm_run(preview):
                return
            self.services.run_ivium_batch_item(batch_item_id=batch_item_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("Ivium 実行", str(error))

    def _run_next_waiting(self) -> None:
        try:
            preview = self._load_run_preview(session_id=self.session_id_var.get().strip() or None)
            if not preview:
                raise ValueError("次の waiting 項目の濃度情報を取得できません。")
            if not self._confirm_run(preview):
                return
            self.services.run_next_waiting_ivium_batch_item(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("Ivium 実行", str(error))

    def _abort_run(self) -> None:
        try:
            self.services.abort_ivium_run()
        except Exception as error:
            messagebox.showerror("Ivium 中止", str(error))

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
            self._load_run_preview(batch_item_id=batch_item_id)

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
        batch_item_ids = self._selected_batch_item_ids()
        if not batch_item_ids:
            return
        label = "選択した項目を削除しますか？" if len(batch_item_ids) == 1 else f"選択した {len(batch_item_ids)} 件の項目を削除しますか？"
        if not messagebox.askyesno("バッチ計画削除", label):
            return
        try:
            messages = [self.services.delete_batch_item(batch_item_id) for batch_item_id in batch_item_ids]
            self._reset_form()
            self.refresh_app()
            summary = messages[0] if len(messages) == 1 else f"{len(batch_item_ids)} 件を削除しました。"
            messagebox.showinfo("バッチ計画削除", summary)
        except Exception as error:
            messagebox.showerror("バッチ計画削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "batch_plan")
        if target:
            navigator(*target)

    def _handle_tree_select(self, _event: tk.Event) -> None:
        if len(self._selected_batch_item_ids()) == 1:
            self._load_selected()
            self._load_run_preview(batch_item_id=self._selected_batch_item_id())
        else:
            self._clear_run_preview()

    def _poll_ivium_state(self) -> None:
        state = self.services.get_ivium_run_state()
        device_status_code = state.get("device_status_code")
        device_status = IVIUM_DEVICE_STATUS_LABELS.get(device_status_code, str(device_status_code or "-"))
        status_label = str(state.get("status") or "idle")
        if device_status_code is not None:
            status_label = f"{status_label} / {device_status}"
        self.ivium_status_var.set(status_label)
        self.ivium_batch_var.set(str(state.get("batch_item_id") or "-"))
        self.ivium_condition_var.set(str(state.get("condition_label") or "-"))
        self.ivium_method_var.set(str(state.get("method_name") or "-"))
        self.ivium_device_var.set(str(state.get("device_serial") or "-"))
        self.ivium_points_var.set(str(state.get("points_collected") or 0))
        self.ivium_source_var.set(str(state.get("source_file_path") or "-"))
        self.ivium_message_var.set(str(state.get("message") or ""))

        log_lines = list(state.get("log_lines") or [])
        self.ivium_log_list.delete(0, tk.END)
        for line in log_lines:
            self.ivium_log_list.insert(tk.END, line)
        if log_lines:
            self.ivium_log_list.yview_moveto(1.0)

        signature = "|".join(
            [
                str(state.get("status") or ""),
                str(state.get("batch_item_id") or ""),
                str(state.get("imported_measurement_id") or ""),
                str(state.get("completed_at") or ""),
            ]
        )
        if signature != self._last_run_signature and str(state.get("status") or "") in {"completed", "failed"}:
            self._last_run_signature = signature
            self.refresh_app()

        self.after(800, self._poll_ivium_state)

    def refresh_tab(self) -> None:
        selected_batch_item_id = self._selected_batch_item_id()
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and self.session_id_var.get() not in session_ids:
            self.session_id_var.set(session_ids[0])
        self.edit_session_combo["values"] = session_ids
        if self.editing_batch_item_id and self.edit_session_id_var.get():
            self._refresh_edit_condition_choices()
        selected_session_id = self.session_id_var.get().strip() or None
        condition_rows = self.services.list_conditions(selected_session_id) if selected_session_id else self.services.list_conditions()
        condition_map = {str(row["condition_id"]): row for row in condition_rows}
        batch_rows = self.services.list_batch_items(selected_session_id)
        status_counts = {status.value: 0 for status in PlannedStatus}
        for row in batch_rows:
            status_counts[str(row.get("planned_status") or "")] = status_counts.get(str(row.get("planned_status") or ""), 0) + 1
        if batch_rows:
            self.plan_summary_var.set(
                " / ".join(
                    [
                        f"表示 {len(batch_rows)} 件",
                        f"waiting {status_counts.get(PlannedStatus.WAITING.value, 0)}",
                        f"running {status_counts.get(PlannedStatus.RUNNING.value, 0)}",
                        f"completed {status_counts.get(PlannedStatus.COMPLETED.value, 0)}",
                        f"failed {status_counts.get(PlannedStatus.FAILED.value, 0)}",
                        f"relink {status_counts.get(PlannedStatus.RELINK_NEEDED.value, 0)}",
                    ]
                )
            )
        elif selected_session_id:
            self.plan_summary_var.set("選択中セッションのバッチ計画はまだありません。")
        else:
            self.plan_summary_var.set("バッチ計画はまだありません。")
        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in batch_rows:
            condition_row = condition_map.get(str(row["condition_id"]))
            self.tree.insert(
                "",
                "end",
                values=(
                    row["batch_item_id"],
                    row["planned_order"],
                    row["condition_id"],
                    self._format_condition_concentration(condition_row),
                    str((condition_row or {}).get("method") or "-"),
                    row["rep_no"],
                    row["planned_status"],
                    row.get("assigned_measurement_id", ""),
                ),
            )
        if selected_batch_item_id and select_tree_record(self.tree, selected_batch_item_id):
            self._load_run_preview(batch_item_id=selected_batch_item_id)
        else:
            self._load_run_preview(session_id=self.session_id_var.get().strip() or None)
