from __future__ import annotations

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from core.services import AppServices
from gui.navigation import enable_bulk_tree_actions, extract_tree_navigation_target, get_selected_tree_values, select_tree_record


class ConditionTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.editing_condition_id: str | None = None
        self.save_button_label = tk.StringVar(value="新規追加")
        self.session_id_var = tk.StringVar()
        self.concentration_var = tk.StringVar()
        self.unit_var = tk.StringVar(value="ppm")
        self.method_var = tk.StringVar(value="CV")
        self.ivium_method_name_var = tk.StringVar(value="CyclicVoltammetry")
        self.planned_replicates_var = tk.StringVar(value="3")
        self.potential_start_var = tk.StringVar()
        self.potential_end_var = tk.StringVar()
        self.vertex_1_var = tk.StringVar()
        self.vertex_2_var = tk.StringVar()
        self.scan_rate_var = tk.StringVar()
        self.step_var = tk.StringVar()
        self.pulse_amplitude_var = tk.StringVar()
        self.pulse_time_var = tk.StringVar()
        self.quiet_time_var = tk.StringVar()
        self.cycles_var = tk.StringVar()
        self.current_range_var = tk.StringVar()
        self.filter_setting_var = tk.StringVar()
        self.ivium_import_status_var = tk.StringVar(value="Ivium 条件を読み込むと、フォームへ反映されます。")

        form = ttk.LabelFrame(self, text="濃度条件 / Ivium パラメータ")
        form.pack(fill="x", padx=12, pady=12)
        for column_index in range(8):
            form.columnconfigure(column_index, weight=1 if column_index in {1, 3, 5, 7} else 0)

        ttk.Label(form, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(form, textvariable=self.session_id_var, width=24, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="we")
        ttk.Label(form, text="濃度").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.concentration_var, width=16).grid(row=0, column=3, sticky="we")
        ttk.Label(form, text="単位").grid(row=0, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.unit_var, width=12).grid(row=0, column=5, sticky="we")
        ttk.Label(form, text="予定回数").grid(row=0, column=6, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.planned_replicates_var, width=12).grid(row=0, column=7, sticky="we")

        ttk.Label(form, text="測定法").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        method_combo = ttk.Combobox(
            form,
            textvariable=self.method_var,
            width=18,
            values=("CV", "LSV", "DPV"),
        )
        method_combo.grid(row=1, column=1, sticky="we")
        method_combo.bind("<<ComboboxSelected>>", lambda _event: self._sync_ivium_method_name())
        ttk.Label(form, text="Ivium Method").grid(row=1, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.ivium_method_name_var, width=22).grid(row=1, column=3, sticky="we")
        ttk.Label(form, text="Current Range").grid(row=1, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.current_range_var, width=18).grid(row=1, column=5, sticky="we")
        ttk.Label(form, text="Filter").grid(row=1, column=6, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.filter_setting_var, width=18).grid(row=1, column=7, sticky="we")

        ttk.Label(form, text="E start [V]").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.potential_start_var, width=14).grid(row=2, column=1, sticky="we")
        ttk.Label(form, text="E end [V]").grid(row=2, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.potential_end_var, width=14).grid(row=2, column=3, sticky="we")
        ttk.Label(form, text="Vertex1 [V]").grid(row=2, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.vertex_1_var, width=14).grid(row=2, column=5, sticky="we")
        ttk.Label(form, text="Vertex2 [V]").grid(row=2, column=6, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.vertex_2_var, width=14).grid(row=2, column=7, sticky="we")

        ttk.Label(form, text="Scanrate [V/s]").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.scan_rate_var, width=14).grid(row=3, column=1, sticky="we")
        ttk.Label(form, text="Step [V]").grid(row=3, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.step_var, width=14).grid(row=3, column=3, sticky="we")
        ttk.Label(form, text="Cycles").grid(row=3, column=4, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.cycles_var, width=14).grid(row=3, column=5, sticky="we")
        ttk.Label(form, text="Quiet [s]").grid(row=3, column=6, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.quiet_time_var, width=14).grid(row=3, column=7, sticky="we")

        ttk.Label(form, text="Pulse Amp [V]").grid(row=4, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.pulse_amplitude_var, width=14).grid(row=4, column=1, sticky="we")
        ttk.Label(form, text="Pulse Time [s]").grid(row=4, column=2, sticky="w", padx=6, pady=6)
        ttk.Entry(form, textvariable=self.pulse_time_var, width=14).grid(row=4, column=3, sticky="we")
        ttk.Button(form, textvariable=self.save_button_label, command=self._save_condition).grid(row=0, column=8, rowspan=5, padx=6)

        actions = ttk.Frame(self)
        actions.pack(fill="x", padx=12)
        ttk.Button(actions, text="Ivium 条件読込", command=self._import_ivium_condition_file).pack(side="left")
        ttk.Button(actions, text="現在のテンプレート読込", command=self._import_ivium_template).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を編集", command=self._load_selected).pack(side="left")
        ttk.Button(actions, text="編集解除", command=self._reset_form).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="選択を複製", command=self._duplicate_selected).pack(side="left", padx=(6, 0))
        ttk.Button(actions, text="削除", command=self._delete_selected).pack(side="left", padx=(6, 0))
        ttk.Label(self, textvariable=self.ivium_import_status_var, wraplength=1200, justify="left").pack(fill="x", padx=12, pady=(6, 0))

        self.tree_columns = (
            "condition_id",
            "session_id",
            "concentration_value",
            "concentration_unit",
            "method",
            "ivium_method_name",
            "scan_rate_v_s",
            "planned_replicates",
            "actual_replicates",
            "condition_status",
        )
        self.tree = ttk.Treeview(self, columns=self.tree_columns, show="headings", height=14)
        headings = ["条件 ID", "セッション", "濃度", "単位", "測定法", "Ivium", "scan", "予定", "実測", "状態"]
        widths = {
            "condition_id": 180,
            "session_id": 140,
            "concentration_value": 90,
            "concentration_unit": 80,
            "method": 80,
            "ivium_method_name": 150,
            "scan_rate_v_s": 90,
            "planned_replicates": 70,
            "actual_replicates": 70,
            "condition_status": 100,
        }
        for column, heading in zip(self.tree_columns, headings):
            self.tree.heading(column, text=heading)
            self.tree.column(column, width=widths.get(column, 110))
        self.tree.pack(fill="both", expand=True, padx=12, pady=12)
        enable_bulk_tree_actions(self.tree)
        self.tree.bind("<Double-1>", self._handle_tree_double_click)

    @staticmethod
    def _optional_float(value: str) -> float | None:
        text = value.strip()
        return float(text) if text else None

    @staticmethod
    def _optional_int(value: str) -> int | None:
        text = value.strip()
        return int(text) if text else None

    def _sync_ivium_method_name(self) -> None:
        aliases = {
            "CV": "CyclicVoltammetry",
            "LSV": "LinearSweep",
        }
        suggested = aliases.get(self.method_var.get().strip().upper())
        if suggested:
            self.ivium_method_name_var.set(suggested)

    def _apply_ivium_condition_payload(self, payload: dict[str, object]) -> None:
        self.method_var.set(str(payload.get("method") or self.method_var.get()))
        self.ivium_method_name_var.set(str(payload.get("ivium_method_name") or self.ivium_method_name_var.get()))
        self.potential_start_var.set("" if payload.get("potential_start_v") is None else str(payload.get("potential_start_v")))
        self.potential_end_var.set("" if payload.get("potential_end_v") is None else str(payload.get("potential_end_v")))
        self.vertex_1_var.set("" if payload.get("potential_vertex_1_v") is None else str(payload.get("potential_vertex_1_v")))
        self.vertex_2_var.set("" if payload.get("potential_vertex_2_v") is None else str(payload.get("potential_vertex_2_v")))
        self.scan_rate_var.set("" if payload.get("scan_rate_v_s") is None else str(payload.get("scan_rate_v_s")))
        self.step_var.set("" if payload.get("step_v") is None else str(payload.get("step_v")))
        self.pulse_amplitude_var.set("" if payload.get("pulse_amplitude_v") is None else str(payload.get("pulse_amplitude_v")))
        self.pulse_time_var.set("" if payload.get("pulse_time_s") is None else str(payload.get("pulse_time_s")))
        self.quiet_time_var.set("" if payload.get("quiet_time_s") is None else str(payload.get("quiet_time_s")))
        self.cycles_var.set("" if payload.get("cycles") is None else str(payload.get("cycles")))
        self.current_range_var.set(str(payload.get("current_range") or ""))
        self.filter_setting_var.set(str(payload.get("filter_setting") or ""))
        source_file_path = str(payload.get("source_file_path") or "")
        source_name = source_file_path if not source_file_path else source_file_path.split("\\")[-1]
        self.ivium_import_status_var.set(f"Ivium 条件を反映しました: {source_name}。保存すると条件へ反映されます。")

    def _import_ivium_condition_file(self) -> None:
        file_path = filedialog.askopenfilename(
            filetypes=[
                ("Ivium files", "*.imf *.ids *.idf.sqlite"),
                ("Ivium method", "*.imf"),
                ("Ivium ids", "*.ids"),
                ("Ivium sqlite", "*.idf.sqlite"),
            ]
        )
        if not file_path:
            return
        try:
            payload = self.services.load_condition_payload_from_ivium_file(file_path)
            self._apply_ivium_condition_payload(payload)
        except Exception as error:
            messagebox.showerror("Ivium 条件読込", str(error))

    def _import_ivium_template(self) -> None:
        try:
            payload = self.services.load_condition_payload_from_ivium_template()
            self._apply_ivium_condition_payload(payload)
        except Exception as error:
            messagebox.showerror("Ivium テンプレート読込", str(error))

    def _selected_condition_id(self) -> str | None:
        selected_ids = self._selected_condition_ids()
        if not selected_ids:
            return None
        return selected_ids[0]

    def _selected_condition_ids(self) -> list[str]:
        return get_selected_tree_values(self.tree)

    def _load_selected(self) -> None:
        condition_id = self._selected_condition_id()
        if not condition_id:
            return
        row = self.services.repository.get_record("conditions", condition_id)
        if not row:
            messagebox.showerror("条件編集", "選択した条件が見つかりません。")
            return
        self.editing_condition_id = condition_id
        self.save_button_label.set("更新保存")
        self.session_id_var.set(str(row.get("session_id", "")))
        self.concentration_var.set(str(row.get("concentration_value", "")))
        self.unit_var.set(str(row.get("concentration_unit", "")))
        self.method_var.set(str(row.get("method", "")))
        self.ivium_method_name_var.set(str(row.get("ivium_method_name", "")))
        self.planned_replicates_var.set(str(row.get("planned_replicates", "")))
        self.potential_start_var.set("" if row.get("potential_start_v") is None else str(row.get("potential_start_v")))
        self.potential_end_var.set("" if row.get("potential_end_v") is None else str(row.get("potential_end_v")))
        self.vertex_1_var.set("" if row.get("potential_vertex_1_v") is None else str(row.get("potential_vertex_1_v")))
        self.vertex_2_var.set("" if row.get("potential_vertex_2_v") is None else str(row.get("potential_vertex_2_v")))
        self.scan_rate_var.set("" if row.get("scan_rate_v_s") is None else str(row.get("scan_rate_v_s")))
        self.step_var.set("" if row.get("step_v") is None else str(row.get("step_v")))
        self.pulse_amplitude_var.set("" if row.get("pulse_amplitude_v") is None else str(row.get("pulse_amplitude_v")))
        self.pulse_time_var.set("" if row.get("pulse_time_s") is None else str(row.get("pulse_time_s")))
        self.quiet_time_var.set("" if row.get("quiet_time_s") is None else str(row.get("quiet_time_s")))
        self.cycles_var.set("" if row.get("cycles") is None else str(row.get("cycles")))
        self.current_range_var.set(str(row.get("current_range", "")))
        self.filter_setting_var.set(str(row.get("filter_setting", "")))

    def focus_record(self, condition_id: str) -> None:
        if select_tree_record(self.tree, condition_id):
            self._load_selected()

    def _reset_form(self) -> None:
        self.editing_condition_id = None
        self.save_button_label.set("新規追加")
        self.concentration_var.set("")
        self.unit_var.set("ppm")
        self.method_var.set("CV")
        self.ivium_method_name_var.set("CyclicVoltammetry")
        self.planned_replicates_var.set("3")
        self.potential_start_var.set("")
        self.potential_end_var.set("")
        self.vertex_1_var.set("")
        self.vertex_2_var.set("")
        self.scan_rate_var.set("")
        self.step_var.set("")
        self.pulse_amplitude_var.set("")
        self.pulse_time_var.set("")
        self.quiet_time_var.set("")
        self.cycles_var.set("")
        self.current_range_var.set("")
        self.filter_setting_var.set("")
        self.ivium_import_status_var.set("Ivium 条件を読み込むと、フォームへ反映されます。")

    def _save_condition(self) -> None:
        payload = {
            "session_id": self.session_id_var.get(),
            "concentration_value": float(self.concentration_var.get()),
            "concentration_unit": self.unit_var.get(),
            "method": self.method_var.get(),
            "ivium_method_name": self.ivium_method_name_var.get().strip(),
            "potential_start_v": self._optional_float(self.potential_start_var.get()),
            "potential_end_v": self._optional_float(self.potential_end_var.get()),
            "potential_vertex_1_v": self._optional_float(self.vertex_1_var.get()),
            "potential_vertex_2_v": self._optional_float(self.vertex_2_var.get()),
            "scan_rate_v_s": self._optional_float(self.scan_rate_var.get()),
            "step_v": self._optional_float(self.step_var.get()),
            "pulse_amplitude_v": self._optional_float(self.pulse_amplitude_var.get()),
            "pulse_time_s": self._optional_float(self.pulse_time_var.get()),
            "quiet_time_s": self._optional_float(self.quiet_time_var.get()),
            "cycles": self._optional_int(self.cycles_var.get()),
            "current_range": self.current_range_var.get().strip(),
            "filter_setting": self.filter_setting_var.get().strip(),
            "planned_replicates": self._optional_int(self.planned_replicates_var.get()),
            "common_note": "",
            "tags": "",
        }
        try:
            if self.editing_condition_id:
                self.services.update_condition(self.editing_condition_id, payload)
            else:
                self.services.create_condition(payload)
            self._reset_form()
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件登録", str(error))

    def _duplicate_selected(self) -> None:
        condition_id = self._selected_condition_id()
        if not condition_id:
            return
        try:
            self.services.duplicate_condition(condition_id)
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("条件複製", str(error))

    def _delete_selected(self) -> None:
        condition_ids = self._selected_condition_ids()
        if not condition_ids:
            return
        label = "選択した条件を削除しますか？" if len(condition_ids) == 1 else f"選択した {len(condition_ids)} 件の条件を削除しますか？"
        if not messagebox.askyesno("条件削除", label):
            return
        try:
            messages = [self.services.delete_condition(condition_id) for condition_id in condition_ids]
            self._reset_form()
            self.refresh_app()
            summary = messages[0] if len(messages) == 1 else f"{len(condition_ids)} 件を削除しました。"
            messagebox.showinfo("条件削除", summary)
        except Exception as error:
            messagebox.showerror("条件削除", str(error))

    def _handle_tree_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.tree, self.tree_columns, event, "condition")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and self.session_id_var.get() not in session_ids:
            self.session_id_var.set(session_ids[0])

        for item in self.tree.get_children():
            self.tree.delete(item)
        for row in self.services.list_conditions():
            self.tree.insert(
                "",
                "end",
                values=(
                    row["condition_id"],
                    row["session_id"],
                    row["concentration_value"],
                    row["concentration_unit"],
                    row["method"],
                    row.get("ivium_method_name", ""),
                    row.get("scan_rate_v_s", ""),
                    row.get("planned_replicates", ""),
                    row.get("actual_replicates", 0),
                    row.get("condition_status", ""),
                ),
            )
