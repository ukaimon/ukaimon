from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from core.services import AppServices


class HomeTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master)
        self.services = services
        self.refresh_app = refresh_app
        self.summary_var = tk.StringVar(value="読み込み中...")
        self.sessions_var = tk.StringVar(value="")
        self.mips_var = tk.StringVar(value="")
        self.errors_var = tk.StringVar(value="")

        ttk.Label(self, text="ホーム", font=("Yu Gothic UI", 16, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(self, textvariable=self.summary_var, justify="left").pack(anchor="w", padx=12)

        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, padx=12, pady=12)
        ttk.Label(body, text="最近のセッション").grid(row=0, column=0, sticky="w")
        ttk.Label(body, textvariable=self.sessions_var, justify="left").grid(row=1, column=0, sticky="nw", pady=(0, 12))
        ttk.Label(body, text="最近の MIP").grid(row=2, column=0, sticky="w")
        ttk.Label(body, textvariable=self.mips_var, justify="left").grid(row=3, column=0, sticky="nw", pady=(0, 12))
        ttk.Label(body, text="最近のエラー").grid(row=4, column=0, sticky="w")
        ttk.Label(body, textvariable=self.errors_var, justify="left").grid(row=5, column=0, sticky="nw")

    def refresh_tab(self) -> None:
        snapshot = self.services.home_snapshot()
        self.summary_var.set(
            "\n".join(
                [
                    f"未完了条件: {snapshot['unfinished_condition_count']}",
                    f"要確認データ: {snapshot['flagged_measurement_count']}",
                ]
            )
        )
        self.sessions_var.set(
            "\n".join(
                f"{row['session_date']} / {row['session_id']} / {row['analyte']}"
                for row in snapshot["recent_sessions"]
            )
            or "まだありません"
        )
        self.mips_var.set(
            "\n".join(
                f"{row['preparation_date']} / {row['mip_id']} / {row['template_name']}"
                for row in snapshot["recent_mips"]
            )
            or "まだありません"
        )
        self.errors_var.set(
            "\n".join(f"{row['created_at']} / {row['message']}" for row in snapshot["recent_errors"])
            or "エラーはありません"
        )
