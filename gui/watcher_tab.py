from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from core.services import AppServices


class WatcherTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self._auto_watch_attempted = False
        self.watch_folder_var = tk.StringVar(value=str((services.root_path / services.config.watch_folder).resolve()))
        self.relink_measurement_var = tk.StringVar()
        self.relink_batch_item_var = tk.StringVar()
        self.header_summary_var = tk.StringVar(value="測定 ID を選ぶと .ids 要約が表示されます。")

        controls = ttk.LabelFrame(self, text="Ivium .ids ファイル監視")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="監視フォルダ").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.watch_folder_var, width=80).grid(row=0, column=1, sticky="we")
        ttk.Button(controls, text="参照", command=self._choose_folder).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="監視開始", command=self._start_watch).grid(row=0, column=3, padx=6)
        ttk.Button(controls, text="監視停止", command=self._stop_watch).grid(row=0, column=4, padx=6)
        ttk.Button(controls, text="単一ファイル解析", command=self._import_single_file).grid(row=0, column=5, padx=6)

        relink = ttk.LabelFrame(self, text="手動修正")
        relink.pack(fill="x", padx=12, pady=(0, 12))
        relink.columnconfigure(6, weight=1)
        ttk.Label(relink, text="測定 ID").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        self.relink_measurement_combo = ttk.Combobox(relink, textvariable=self.relink_measurement_var, width=28, state="readonly")
        self.relink_measurement_combo.grid(row=0, column=1, sticky="w")
        self.relink_measurement_combo.bind("<<ComboboxSelected>>", lambda _event: self._update_header_summary())
        ttk.Label(relink, text="移動先バッチ ID").grid(row=0, column=2, sticky="w", padx=6, pady=6)
        self.relink_batch_combo = ttk.Combobox(relink, textvariable=self.relink_batch_item_var, width=28, state="readonly")
        self.relink_batch_combo.grid(row=0, column=3, sticky="w")
        ttk.Button(relink, text="再リンク", command=self._relink_measurement).grid(row=0, column=4, padx=6)
        ttk.Button(relink, text="指定項目へ取込", command=self._import_single_file_to_target).grid(row=0, column=5, padx=6)
        summary_frame = ttk.LabelFrame(relink, text=".ids 要約")
        summary_frame.grid(row=0, column=6, rowspan=2, sticky="nsew", padx=(12, 6), pady=6)
        ttk.Label(
            summary_frame,
            textvariable=self.header_summary_var,
            justify="left",
            wraplength=420,
        ).pack(fill="x", padx=6, pady=6)

        self.status_list = tk.Listbox(self, height=16)
        self.status_list.pack(fill="both", expand=True, padx=12, pady=12)
        self.after(150, self._auto_start_watch)

    def _append_status(self, message: str) -> None:
        self.status_list.insert(tk.END, message)
        self.status_list.yview_moveto(1.0)
        self.refresh_app()

    def _choose_folder(self) -> None:
        folder = filedialog.askdirectory(initialdir=self.watch_folder_var.get())
        if folder:
            self.watch_folder_var.set(folder)
            self.services.config.watch_folder = str(Path(folder).resolve().relative_to(self.services.root_path))

    def _begin_watch(self, *, show_error_dialog: bool) -> None:
        try:
            watch_path = Path(self.watch_folder_var.get())
            if watch_path.is_absolute():
                try:
                    self.services.config.watch_folder = str(watch_path.resolve().relative_to(self.services.root_path))
                except ValueError:
                    self.services.config.watch_folder = str(watch_path.resolve())
            self.services.start_watcher(self._append_status)
        except Exception as error:
            if show_error_dialog:
                messagebox.showerror("監視開始", str(error))
            else:
                self._append_status(f"自動監視開始に失敗: {error}")

    def _start_watch(self) -> None:
        self._begin_watch(show_error_dialog=True)

    def _auto_start_watch(self) -> None:
        if self._auto_watch_attempted:
            return
        self._auto_watch_attempted = True
        self._begin_watch(show_error_dialog=False)

    def _stop_watch(self) -> None:
        self.services.stop_watcher()

    def _import_single_file(self) -> None:
        file_path = filedialog.askopenfilename(filetypes=[("Ivium ids", "*.ids")])
        if not file_path:
            return
        try:
            measurement_id = self.services.import_ids_file(file_path)
            self._append_status(f"手動取り込み成功: {measurement_id}")
        except Exception as error:
            messagebox.showerror("単一ファイル解析", str(error))

    def _import_single_file_to_target(self) -> None:
        if not self.relink_batch_item_var.get():
            messagebox.showinfo("指定項目へ取込", "移動先バッチ ID を選択してください。")
            return
        file_path = filedialog.askopenfilename(filetypes=[("Ivium ids", "*.ids")])
        if not file_path:
            return
        try:
            measurement_id = self.services.import_ids_file(file_path, batch_item_id=self.relink_batch_item_var.get())
            self._append_status(f"指定取込成功: {measurement_id}")
            self.refresh_tab()
        except Exception as error:
            messagebox.showerror("指定項目へ取込", str(error))

    def _relink_measurement(self) -> None:
        if not self.relink_measurement_var.get() or not self.relink_batch_item_var.get():
            messagebox.showinfo("再リンク", "測定 ID と移動先バッチ ID を選択してください。")
            return
        try:
            message = self.services.relink_measurement(
                self.relink_measurement_var.get(),
                self.relink_batch_item_var.get(),
            )
            self._append_status(message)
            self.refresh_tab()
        except Exception as error:
            messagebox.showerror("再リンク", str(error))

    def _update_header_summary(self) -> None:
        measurement_id = self.relink_measurement_var.get().strip()
        self.header_summary_var.set(self.services.get_measurement_header_summary(measurement_id))

    def refresh_tab(self) -> None:
        if self.status_list.size() == 0:
            self.status_list.insert(tk.END, "待機中")
        measurement_ids = [row["measurement_id"] for row in self.services.list_relink_measurements()]
        batch_item_ids = [row["batch_item_id"] for row in self.services.list_relink_batch_items()]
        self.relink_measurement_combo["values"] = measurement_ids
        self.relink_batch_combo["values"] = batch_item_ids
        if measurement_ids and self.relink_measurement_var.get() not in measurement_ids:
            self.relink_measurement_var.set(measurement_ids[0])
        if not measurement_ids:
            self.relink_measurement_var.set("")
        if batch_item_ids and self.relink_batch_item_var.get() not in batch_item_ids:
            self.relink_batch_item_var.set(batch_item_ids[0])
        if not batch_item_ids:
            self.relink_batch_item_var.set("")
        self._update_header_summary()
