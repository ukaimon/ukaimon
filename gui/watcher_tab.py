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

        controls = ttk.LabelFrame(self, text="Ivium .ids ファイル監視")
        controls.pack(fill="x", padx=12, pady=12)
        ttk.Label(controls, text="監視フォルダ").grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(controls, textvariable=self.watch_folder_var, width=80).grid(row=0, column=1, sticky="we")
        ttk.Button(controls, text="参照", command=self._choose_folder).grid(row=0, column=2, padx=6)
        ttk.Button(controls, text="監視開始", command=self._start_watch).grid(row=0, column=3, padx=6)
        ttk.Button(controls, text="監視停止", command=self._stop_watch).grid(row=0, column=4, padx=6)
        ttk.Button(controls, text="単一ファイル解析", command=self._import_single_file).grid(row=0, column=5, padx=6)

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

    def refresh_tab(self) -> None:
        if self.status_list.size() == 0:
            self.status_list.insert(tk.END, "待機中")
