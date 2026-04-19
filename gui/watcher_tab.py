from __future__ import annotations

import threading
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
        self.watch_folder_var = tk.StringVar(value=str(services.resolve_watch_folder()))
        self.relink_measurement_var = tk.StringVar()
        self.relink_batch_item_var = tk.StringVar()
        self.exclusion_reason_var = tk.StringVar(value="誤測定のため除外")
        self.header_summary_var = tk.StringVar(value="測定 ID を選ぶと測定ファイル要約が表示されます。")

        controls = ttk.LabelFrame(self, text="Ivium 測定ファイル監視")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="監視フォルダ").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.watch_folder_var, width=80).grid(row=0, column=1, sticky="we")
        ttk.Button(controls, text="参照", command=self._choose_folder).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="監視開始", command=self._start_watch).grid(row=0, column=3, padx=6)
        ttk.Button(controls, text="監視停止", command=self._stop_watch).grid(row=0, column=4, padx=6)
        ttk.Button(controls, text="単一ファイル解析", command=self._import_single_file).grid(row=0, column=5, padx=6)
        ttk.Button(controls, text="監視フォルダを開く", command=self._open_watch_folder).grid(row=0, column=6, padx=6)
        ttk.Button(controls, text="パスをコピー", command=self._copy_watch_folder).grid(row=0, column=7, padx=6)
        ttk.Button(controls, text="IviumSoft 起動", command=self._launch_iviumsoft).grid(row=0, column=8, padx=6)

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
        ttk.Label(relink, text="除外理由").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(relink, textvariable=self.exclusion_reason_var, width=46).grid(row=1, column=1, columnspan=2, sticky="we")
        ttk.Button(relink, text="取込取消", command=self._cancel_imported_measurement).grid(row=1, column=3, padx=6)
        ttk.Button(relink, text="除外", command=self._exclude_measurement).grid(row=1, column=4, padx=6)
        ttk.Button(relink, text="除外解除", command=self._clear_measurement_exclusion).grid(row=1, column=5, padx=6)
        summary_frame = ttk.LabelFrame(relink, text="測定ファイル要約")
        summary_frame.grid(row=0, column=6, rowspan=3, sticky="nsew", padx=(12, 6), pady=6)
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
        if threading.current_thread() is not threading.main_thread():
            self.after(0, lambda: self._append_status(message))
            return
        self.status_list.insert(tk.END, message)
        self.status_list.yview_moveto(1.0)
        self.refresh_app()

    def _choose_folder(self) -> None:
        folder = filedialog.askdirectory(initialdir=self.watch_folder_var.get())
        if folder:
            try:
                resolved = self.services.set_watch_folder(folder)
                self.watch_folder_var.set(str(resolved))
                self._append_status(f"監視フォルダを保存しました: {resolved}")
            except Exception as error:
                messagebox.showerror("監視フォルダ", str(error))

    def _begin_watch(self, *, show_error_dialog: bool) -> None:
        try:
            watch_path = self.services.set_watch_folder(self.watch_folder_var.get())
            self.watch_folder_var.set(str(watch_path))
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

    def _open_watch_folder(self) -> None:
        try:
            message = self.services.open_watch_folder()
            self._append_status(message)
        except Exception as error:
            messagebox.showerror("監視フォルダ", str(error))

    def _copy_watch_folder(self) -> None:
        watch_folder = str(self.services.resolve_watch_folder())
        self.clipboard_clear()
        self.clipboard_append(watch_folder)
        self.update()
        self._append_status(f"監視フォルダのパスをコピーしました: {watch_folder}")

    def _launch_iviumsoft(self) -> None:
        try:
            message = self.services.launch_iviumsoft()
            self._append_status(message)
        except Exception as error:
            messagebox.showerror("IviumSoft 起動", str(error))

    def _import_single_file(self) -> None:
        file_path = filedialog.askopenfilename(filetypes=[("Ivium measurement", "*.ids *.idf.sqlite"), ("Ivium ids", "*.ids"), ("Ivium sqlite", "*.idf.sqlite")])
        if not file_path:
            return
        try:
            measurement_id = self.services.import_measurement_file(file_path)
            self._append_status(f"手動取り込み成功: {measurement_id}")
        except Exception as error:
            messagebox.showerror("単一ファイル解析", str(error))

    def _import_single_file_to_target(self) -> None:
        if not self.relink_batch_item_var.get():
            messagebox.showinfo("指定項目へ取込", "移動先バッチ ID を選択してください。")
            return
        file_path = filedialog.askopenfilename(filetypes=[("Ivium measurement", "*.ids *.idf.sqlite"), ("Ivium ids", "*.ids"), ("Ivium sqlite", "*.idf.sqlite")])
        if not file_path:
            return
        try:
            measurement_id = self.services.import_measurement_file(file_path, batch_item_id=self.relink_batch_item_var.get())
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

    def _cancel_imported_measurement(self) -> None:
        measurement_id = self.relink_measurement_var.get().strip()
        if not measurement_id:
            messagebox.showinfo("取込取消", "測定 ID を選択してください。")
            return
        if not messagebox.askyesno("取込取消", f"{measurement_id} の取込結果を取り消しますか？"):
            return
        try:
            message = self.services.delete_measurement(measurement_id)
            self._append_status(f"取込取消: {measurement_id} / {message}")
            self.refresh_tab()
        except Exception as error:
            messagebox.showerror("取込取消", str(error))

    def _exclude_measurement(self) -> None:
        measurement_id = self.relink_measurement_var.get().strip()
        if not measurement_id:
            messagebox.showinfo("除外", "測定 ID を選択してください。")
            return
        reason = self.exclusion_reason_var.get().strip() or "誤測定のため除外"
        if not messagebox.askyesno("除外", f"{measurement_id} を除外しますか？\n理由: {reason}"):
            return
        try:
            message = self.services.exclude_measurement(measurement_id, reason)
            self._append_status(message)
            self.refresh_tab()
        except Exception as error:
            messagebox.showerror("除外", str(error))

    def _clear_measurement_exclusion(self) -> None:
        measurement_id = self.relink_measurement_var.get().strip()
        if not measurement_id:
            messagebox.showinfo("除外解除", "測定 ID を選択してください。")
            return
        if not messagebox.askyesno("除外解除", f"{measurement_id} の除外を解除しますか？"):
            return
        try:
            message = self.services.clear_measurement_exclusion(measurement_id)
            self._append_status(message)
            self.refresh_tab()
        except Exception as error:
            messagebox.showerror("除外解除", str(error))

    def _update_header_summary(self) -> None:
        measurement_id = self.relink_measurement_var.get().strip()
        self.header_summary_var.set(self.services.get_measurement_header_summary(measurement_id))

    def refresh_tab(self) -> None:
        if self.status_list.size() == 0:
            self.status_list.insert(tk.END, "待機中")
        self.watch_folder_var.set(str(self.services.resolve_watch_folder()))
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
