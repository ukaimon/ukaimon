from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

from core.services import AppServices


class MeasurementTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_measurement_id: str | None = None
        self.save_button_label = tk.StringVar(value="測定保存")
        self.session_id_var = tk.StringVar()
        self.condition_id_var = tk.StringVar()
        self.chip_id_var = tk.StringVar()
        self.wire_id_var = tk.StringVar()
        self.status_var = tk.StringVar(value="manual")
        self.noise_var = tk.StringVar(value="0.0")
        self.memo_var = tk.StringVar()

        form = ttk.LabelFrame(self, text="測定追加 / 編集")
        form.pack(fill="x", padx=12, pady=12)
        ttk.Label(form, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(form, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        self.session_combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_condition_choices())
        ttk.Label(form, text="条件").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        self.condition_combo = ttk.Combobox(form, textvariable=self.condition_id_var, width=24, state="readonly")
        self.condition_combo.grid(row=0, column=3, sticky="w")
        ttk.Label(form, text="chip_id").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.chip_id_var, width=20).grid(row=1, column=1, sticky="w")
        ttk.Label(form, text="wire_id").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.wire_id_var, width=20).grid(row=1, column=3, sticky="w")
        ttk.Label(form, text="status").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.status_var, width=20).grid(row=2, column=1, sticky="w")
        ttk.Label(form, text="noise_level").grid(row=2, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.noise_var, width=20).grid(row=2, column=3, sticky="w")
        ttk.Label(form, text="メモ").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.memo_var, width=60).grid(row=3, column=1, columnspan=3, sticky="we")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_measurement).grid(row=0, column=4, rowspan=4, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))

        columns = ("measurement_id", "session_id", "condition_id", "rep_no", "measured_at", "status", "final_quality_flag", "raw_file_path")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=14)
        headings = ["測定 ID", "セッション", "条件", "rep", "測定日時", "状態", "品質", "raw_file_path"]
        for column, heading in zip(columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=140 if column != "raw_file_path" else 260)
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)

    def _selected_measurement_id(self) -> str | None:
        selection = self.tree.selection()
        if not selection:
            return None
        return str(self.tree.item(selection[0], "values")[0])

    def _refresh_condition_choices(self) -> None:
        session_id = self.session_id_var.get()
        conditions = self.services.list_conditions(session_id) if session_id else []
        condition_ids = [row["condition_id"] for row in conditions]
        self.condition_combo["values"] = condition_ids
        if condition_ids and self.condition_id_var.get() not in condition_ids:
            self.condition_id_var.set(condition_ids[0])

    def _load_selected(self) -> None:
        measurement_id = self._selected_measurement_id()
        if not measurement_id:
            return
        row = self.services.repository.get_record("measurements", measurement_id)
        if not row or int(row.get("is_deleted", 0)) == 1:
            messagebox.showerror("測定編集", "選択した測定が見つかりません。")
            return
        self.editing_measurement_id = measurement_id
        self.save_button_label.set("更新保存")
        self.session_id_var.set(str(row.get("session_id", "")))
        self._refresh_condition_choices()
        self.condition_id_var.set(str(row.get("condition_id", "")))
        self.chip_id_var.set(str(row.get("chip_id", "")))
        self.wire_id_var.set(str(row.get("wire_id", "")))
        self.status_var.set(str(row.get("status", "")))
        self.noise_var.set("" if row.get("noise_level") is None else str(row.get("noise_level")))
        self.memo_var.set(str(row.get("free_memo", "")))

    def _reset_form(self) -> None:
        self.editing_measurement_id = None
        self.save_button_label.set("測定保存")
        self.condition_id_var.set("")
        self.chip_id_var.set("")
        self.wire_id_var.set("")
        self.status_var.set("manual")
        self.noise_var.set("0.0")
        self.memo_var.set("")
        self._refresh_condition_choices()

    def _save_measurement(self) -> None:
        payload = {
            "session_id": self.session_id_var.get(),
            "condition_id": self.condition_id_var.get(),
            "chip_id": self.chip_id_var.get(),
            "wire_id": self.wire_id_var.get(),
            "status": self.status_var.get(),
            "noise_level": float(self.noise_var.get()) if self.noise_var.get() else None,
            "free_memo": self.memo_var.get(),
        }
        try:
            if self.editing_measurement_id:
                self.services.update_measurement(self.editing_measurement_id, payload)
            else:
                self.services.create_measurement(payload)
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("測定保存", str(error))

    def _delete_selected(self) -> None:
        measurement_id = self._selected_measurement_id()
        if not measurement_id:
            return
        if not messagebox.askyesno("測定削除", "選択した測定を削除しますか？"):
            return
        try:
            message = self.services.delete_measurement(measurement_id)
            self._reset_form()
            self.refresh_app()
            messagebox.showinfo("測定削除", message)
        except Exception as error:
            messagebox.showerror("測定削除", str(error))

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and self.session_id_var.get() not in session_ids:
            self.session_id_var.set(session_ids[0])
        self._refresh_condition_choices()

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_measurements():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["measurement_id"],
                    row["session_id"],
                    row["condition_id"],
                    row["rep_no"],
                    row.get("measured_at", ""),
                    row.get("status", ""),
                    row.get("final_quality_flag", ""),
                    row.get("raw_file_path", ""),
                ),
            )
