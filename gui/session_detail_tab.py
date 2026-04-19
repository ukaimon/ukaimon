from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

try:
    from PIL import Image, ImageTk
except ImportError:  # pragma: no cover - Pillow is expected in the app environment.
    Image = None
    ImageTk = None

from core.services import AppServices
from gui.navigation import extract_tree_navigation_target


class SessionDetailTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.session_id_var = tk.StringVar()
        self.summary_var = tk.StringVar(value="")
        self.analysis_plot_var = tk.StringVar()
        self.analysis_plot_summary_var = tk.StringVar(value="解析プロットはまだ出力されていません。")
        self.analysis_plot_status_var = tk.StringVar(value="解析プロットはまだ出力されていません。")
        self._analysis_plot_entries: dict[str, str] = {}
        self._analysis_plot_image: object | None = None
        self._analysis_plot_reload_job: str | None = None

        scroll_shell = ttk.Frame(self)
        scroll_shell.pack(fill="both", expand=True)
        self.scroll_canvas = tk.Canvas(scroll_shell, highlightthickness=0, borderwidth=0)
        self.scrollbar = ttk.Scrollbar(scroll_shell, orient="vertical", command=self.scroll_canvas.yview)
        self.scroll_canvas.configure(yscrollcommand=self.scrollbar.set)
        self.scrollbar.pack(side="right", fill="y")
        self.scroll_canvas.pack(side="left", fill="both", expand=True)

        self.scroll_body = ttk.Frame(self.scroll_canvas)
        self._scroll_window_id = self.scroll_canvas.create_window((0, 0), window=self.scroll_body, anchor="nw")
        self.scroll_body.bind("<Configure>", self._handle_scroll_body_configure)
        self.scroll_canvas.bind("<Configure>", self._handle_scroll_canvas_configure)

        controls = ttk.LabelFrame(self.scroll_body, text="セッション詳細")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="セッション").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.session_combo = ttk.Combobox(controls, textvariable=self.session_id_var, width=28, state="readonly")
        self.session_combo.grid(row=0, column=1, sticky="w")
        self.session_combo.bind("<<ComboboxSelected>>", lambda _event: self.refresh_tab())
        ttk.Button(controls, text="集計更新", command=self._aggregate).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="平均ボルタモグラム出力", command=self._generate_mean).grid(row=0, column=3, padx=6)
        ttk.Button(controls, text="セッション出力", command=self._export_bundle).grid(row=0, column=4, padx=6)
        ttk.Button(controls, text="解析プロット出力", command=self._export_analysis_plots).grid(row=0, column=5, padx=6)
        ttk.Label(controls, textvariable=self.summary_var, justify="left").grid(row=1, column=0, columnspan=6, sticky="w", padx=6, pady=(0, 6))

        content = ttk.Panedwindow(self.scroll_body, orient=tk.VERTICAL)
        content.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        tables = ttk.Frame(content)
        content.add(tables, weight=3)

        self.condition_columns = ("condition_id", "concentration_value", "method", "actual_replicates", "n_valid", "n_invalid", "condition_status", "warning")
        self.condition_tree = ttk.Treeview(
            tables,
            columns=self.condition_columns,
            show="headings",
            height=8,
        )
        for column, heading in zip(
            self.condition_columns,
            ("条件 ID", "濃度", "測定法", "実測", "valid", "invalid", "状態", "差分"),
        ):
            self.condition_tree.heading(column, text=heading)
            self.condition_tree.column(column, width=110 if column != "warning" else 200)
        self.condition_tree.pack(fill="x", pady=(0, 12))
        self.condition_tree.bind("<Double-1>", self._handle_condition_double_click)

        self.measurement_columns = ("measurement_id", "condition_id", "rep_no", "measured_at", "final_quality_flag", "raw_file_path")
        self.measurement_tree = ttk.Treeview(
            tables,
            columns=self.measurement_columns,
            show="headings",
            height=8,
        )
        for column, heading in zip(
            self.measurement_columns,
            ("測定 ID", "条件 ID", "rep", "測定日時", "品質", "raw_file_path"),
        ):
            self.measurement_tree.heading(column, text=heading)
            self.measurement_tree.column(column, width=140 if column != "raw_file_path" else 280)
        self.measurement_tree.pack(fill="both", expand=True, pady=(0, 12))
        self.measurement_tree.bind("<Double-1>", self._handle_measurement_double_click)

        ttk.Label(tables, text="解析結果一覧").pack(anchor="w", pady=(0, 4))
        self.analysis_columns = (
            "measurement_id",
            "analysis_method",
            "representative_current_a",
            "representative_potential_v",
            "oxidation_peak_current_a",
            "reduction_peak_current_a",
            "delta_ep_v",
            "quality_flag",
        )
        self.analysis_tree = ttk.Treeview(
            tables,
            columns=self.analysis_columns,
            show="headings",
            height=7,
        )
        for column, heading, width in (
            ("measurement_id", "測定 ID", 150),
            ("analysis_method", "解析法", 90),
            ("representative_current_a", "代表電流", 110),
            ("representative_potential_v", "代表電位", 110),
            ("oxidation_peak_current_a", "酸化ピーク", 110),
            ("reduction_peak_current_a", "還元ピーク", 110),
            ("delta_ep_v", "ΔEp", 90),
            ("quality_flag", "品質", 90),
        ):
            self.analysis_tree.heading(column, text=heading)
            self.analysis_tree.column(column, width=width)
        self.analysis_tree.pack(fill="x")
        self.analysis_tree.bind("<Double-1>", self._handle_analysis_double_click)

        viewer = ttk.LabelFrame(content, text="解析プロット表示")
        content.add(viewer, weight=2)

        viewer_controls = ttk.Frame(viewer)
        viewer_controls.pack(fill="x", padx=8, pady=(8, 6))
        ttk.Label(viewer_controls, text="表示画像").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.analysis_plot_combo = ttk.Combobox(
            viewer_controls,
            textvariable=self.analysis_plot_var,
            width=72,
            state="readonly",
        )
        self.analysis_plot_combo.grid(row=0, column=1, sticky="ew")
        self.analysis_plot_combo.bind("<<ComboboxSelected>>", lambda _event: self._load_selected_analysis_plot())
        ttk.Button(viewer_controls, text="一覧更新", command=self._refresh_analysis_plot_list).grid(row=0, column=2, padx=6)
        viewer_controls.columnconfigure(1, weight=1)

        ttk.Label(viewer, textvariable=self.analysis_plot_summary_var, justify="left", wraplength=1280).pack(fill="x", padx=8, pady=(0, 6))
        ttk.Label(viewer, textvariable=self.analysis_plot_status_var, justify="left").pack(fill="x", padx=8, pady=(0, 6))
        self.analysis_plot_preview = tk.Label(
            viewer,
            text="解析プロットはまだ出力されていません。",
            justify="center",
            anchor="center",
            background="#ffffff",
            relief="groove",
            borderwidth=1,
            padx=12,
            pady=12,
        )
        self.analysis_plot_preview.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.analysis_plot_preview.bind("<Configure>", self._handle_analysis_plot_preview_resize, add="+")

        for widget in (
            self,
            self.scroll_canvas,
            self.scroll_body,
            controls,
            content,
            tables,
            viewer,
            self.session_combo,
            self.condition_tree,
            self.measurement_tree,
            self.analysis_tree,
            self.analysis_plot_combo,
            self.analysis_plot_preview,
        ):
            widget.bind("<MouseWheel>", self._handle_mousewheel, add="+")

    def _handle_scroll_body_configure(self, _event: tk.Event) -> None:
        self.scroll_canvas.configure(scrollregion=self.scroll_canvas.bbox("all"))

    def _handle_scroll_canvas_configure(self, event: tk.Event) -> None:
        self.scroll_canvas.itemconfigure(self._scroll_window_id, width=event.width)

    def _handle_mousewheel(self, event: tk.Event) -> None:
        if not self.winfo_ismapped():
            return
        delta = getattr(event, "delta", 0)
        if delta == 0:
            return
        self.scroll_canvas.yview_scroll(int(-delta / 120), "units")

    def _handle_analysis_plot_preview_resize(self, _event: tk.Event) -> None:
        if self._analysis_plot_reload_job:
            self.after_cancel(self._analysis_plot_reload_job)
        if not self.analysis_plot_var.get().strip():
            return
        self._analysis_plot_reload_job = self.after(120, self._load_selected_analysis_plot)

    def _aggregate(self) -> None:
        try:
            self.services.aggregate_session(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("セッション集計", str(error))

    def _generate_mean(self) -> None:
        try:
            self.services.generate_mean_voltammograms(self.session_id_var.get())
            self.refresh_app()
        except Exception as error:
            messagebox.showerror("平均ボルタモグラム", str(error))

    def _export_bundle(self) -> None:
        try:
            outputs = self.services.export_session_bundle(self.session_id_var.get())
            messagebox.showinfo("セッション出力", "\n".join(f"{key}: {value}" for key, value in outputs.items()))
        except Exception as error:
            messagebox.showerror("セッション出力", str(error))

    def _export_analysis_plots(self) -> None:
        try:
            outputs = self.services.export_session_analysis_plots(self.session_id_var.get())
            preferred_path = (
                outputs.get("absolute_loop_area_plot")
                or outputs.get("absolute_integral_plot")
                or outputs.get("cycle1_reference_plot")
                or outputs.get("calibration_plot")
                or outputs.get("overlay_plot")
                or ""
            )
            self._refresh_analysis_plot_list(preferred_path=preferred_path)
            messagebox.showinfo("解析プロット出力", "\n".join(f"{key}: {value}" for key, value in outputs.items()))
        except Exception as error:
            messagebox.showerror("解析プロット出力", str(error))

    def _refresh_analysis_plot_list(self, preferred_path: str | None = None) -> None:
        session_id = self.session_id_var.get().strip()
        if not session_id:
            self.analysis_plot_combo["values"] = []
            self.analysis_plot_var.set("")
            self.analysis_plot_summary_var.set("解析プロットはまだ出力されていません。")
            self._clear_analysis_plot_preview("セッションを選ぶと解析プロットを表示できます。")
            return

        plot_entries = self.services.list_session_analysis_plot_images(session_id)
        self.analysis_plot_summary_var.set(self.services.get_session_analysis_plot_summary(session_id))
        self._analysis_plot_entries = {
            entry["label"]: entry["path"]
            for entry in plot_entries
        }
        labels = [entry["label"] for entry in plot_entries]
        self.analysis_plot_combo["values"] = labels

        if not labels:
            self.analysis_plot_var.set("")
            self._clear_analysis_plot_preview("解析プロットはまだ出力されていません。")
            return

        selected_label = self.analysis_plot_var.get()
        if preferred_path:
            selected_label = next(
                (
                    entry["label"]
                    for entry in plot_entries
                    if entry["path"] == preferred_path
                ),
                selected_label,
            )
        elif not selected_label:
            selected_label = next(
                (
                    entry["label"]
                    for entry in plot_entries
                    if "絶対ループ面積解析" in entry["label"]
                ),
                selected_label,
            )
        if selected_label not in self._analysis_plot_entries:
            selected_label = labels[0]
        self.analysis_plot_var.set(selected_label)
        self._load_selected_analysis_plot()

    def _clear_analysis_plot_preview(self, message: str) -> None:
        self._analysis_plot_reload_job = None
        self._analysis_plot_image = None
        self.analysis_plot_status_var.set(message)
        self.analysis_plot_preview.configure(image="", text=message)

    def _load_selected_analysis_plot(self) -> None:
        self._analysis_plot_reload_job = None
        selected_label = self.analysis_plot_var.get().strip()
        plot_path = self._analysis_plot_entries.get(selected_label, "")
        if not plot_path:
            self._clear_analysis_plot_preview("表示できる解析プロットがありません。")
            return
        try:
            available_width = self.analysis_plot_preview.winfo_width() - 24
            available_height = self.analysis_plot_preview.winfo_height() - 24
            max_width = available_width if available_width > 120 else 720
            max_height = available_height if available_height > 120 else 560
            if Image is not None and ImageTk is not None:
                with Image.open(plot_path) as source_image:
                    width, height = source_image.size
                    scale = min(max_width / max(width, 1), max_height / max(height, 1), 1.0)
                    display_width = max(1, int(round(width * scale)))
                    display_height = max(1, int(round(height * scale)))
                    if scale < 1.0:
                        resampling = getattr(Image, "Resampling", Image)
                        preview_source = source_image.resize(
                            (display_width, display_height),
                            resampling.LANCZOS,
                        )
                    else:
                        preview_source = source_image.copy()
                preview_image = ImageTk.PhotoImage(preview_source)
            else:
                image = tk.PhotoImage(file=plot_path)
                width = max(image.width(), 1)
                height = max(image.height(), 1)
                width_factor = max(1, (width + max_width - 1) // max_width)
                height_factor = max(1, (height + max_height - 1) // max_height)
                factor = max(width_factor, height_factor)
                preview_image = image.subsample(factor, factor) if factor > 1 else image
                display_width = preview_image.width()
                display_height = preview_image.height()
        except Exception as error:
            self._clear_analysis_plot_preview(f"{selected_label}\n画像を読み込めませんでした: {error}")
            return

        self._analysis_plot_image = preview_image
        self.analysis_plot_status_var.set(
            f"{selected_label} (原寸 {width} x {height}px / 表示 {display_width} x {display_height}px)"
        )
        self.analysis_plot_preview.configure(image=preview_image, text="")

    def _handle_condition_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.condition_tree, self.condition_columns, event, "session_detail_condition")
        if target:
            navigator(*target)

    def _handle_measurement_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.measurement_tree, self.measurement_columns, event, "session_detail_measurement")
        if target:
            navigator(*target)

    def _handle_analysis_double_click(self, event: tk.Event) -> None:
        navigator = getattr(self, "navigate_to_record", None)
        if not callable(navigator):
            return
        target = extract_tree_navigation_target(self.analysis_tree, self.analysis_columns, event, "session_detail_measurement")
        if target:
            navigator(*target)

    def refresh_tab(self) -> None:
        session_ids = [row["session_id"] for row in self.services.list_sessions()]
        self.session_combo["values"] = session_ids
        if session_ids and not self.session_id_var.get():
            self.session_id_var.set(session_ids[0])
        if not self.session_id_var.get():
            return

        detail = self.services.get_session_detail(self.session_id_var.get())
        session = detail["session"]
        self.summary_var.set(
            f"{session['session_date']} / {session['session_name']} / 測定対象物質={session['analyte']} / 測定法={session.get('method_default', '')}"
        )
        condition_warnings = detail.get("condition_warnings", {})

        for tree in (self.condition_tree, self.measurement_tree, self.analysis_tree):
            for item in tree.get_children():
                tree.delete(item)

        for row in detail["conditions"]:
            self.condition_tree.insert(
                "",
                "end",
                values=(
                    row["condition_id"],
                    row["concentration_value"],
                    row["method"],
                    row.get("actual_replicates", 0),
                    row.get("n_valid", 0),
                    row.get("n_invalid", 0),
                    row.get("condition_status", ""),
                    condition_warnings.get(str(row["condition_id"]), ""),
                ),
            )
        for row in detail["measurements"]:
            self.measurement_tree.insert(
                "",
                "end",
                values=(
                    row["measurement_id"],
                    row["condition_id"],
                    row["rep_no"],
                    row.get("measured_at", ""),
                    row.get("final_quality_flag", ""),
                    row.get("raw_file_path", ""),
                ),
            )
        for row in detail["analysis_results"]:
            self.analysis_tree.insert(
                "",
                "end",
                values=(
                    row.get("measurement_id", ""),
                    row.get("analysis_method", ""),
                    row.get("representative_current_a", ""),
                    row.get("representative_potential_v", ""),
                    row.get("oxidation_peak_current_a", ""),
                    row.get("reduction_peak_current_a", ""),
                    row.get("delta_ep_v", ""),
                    row.get("quality_flag", ""),
                ),
            )
        self._refresh_analysis_plot_list()
