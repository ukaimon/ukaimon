from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from core.services import AppServices


class HomeTab(ttk.Frame):
    def __init__(self, master: ttk.Notebook, services: AppServices, refresh_app) -> None:
        super().__init__(master, padding=12)
        self.services = services
        self.refresh_app = refresh_app
        self.summary_var = tk.StringVar(value="読み込み中...")
        self.sessions_var = tk.StringVar(value="")
        self.mips_var = tk.StringVar(value="")
        self.errors_var = tk.StringVar(value="")

        ttk.Label(self, text="ホーム", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            self,
            text="最近の進捗と要確認項目をまとめて見られます。",
            style="Subtle.TLabel",
        ).pack(anchor="w", pady=(2, 10))

        summary_card = ttk.Frame(self, style="Card.TFrame", padding=16)
        summary_card.pack(fill="x")
        ttk.Label(summary_card, text="サマリー", style="CardTitle.TLabel").pack(anchor="w")
        ttk.Label(summary_card, textvariable=self.summary_var, style="CardBody.TLabel", justify="left").pack(anchor="w", pady=(8, 0))

        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, pady=(12, 0))
        for column in range(3):
            body.columnconfigure(column, weight=1, uniform="home")

        self._build_card(body, 0, "最近のセッション", self.sessions_var)
        self._build_card(body, 1, "最近の MIP", self.mips_var)
        self._build_card(body, 2, "最近のエラー", self.errors_var)

    def _build_card(self, parent: ttk.Frame, column: int, title: str, variable: tk.StringVar) -> None:
        card = ttk.Frame(parent, style="Card.TFrame", padding=16)
        card.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 8, 0), pady=0)
        ttk.Label(card, text=title, style="CardTitle.TLabel").pack(anchor="w")
        ttk.Separator(card).pack(fill="x", pady=10)
        ttk.Label(
            card,
            textvariable=variable,
            style="CardBody.TLabel",
            justify="left",
            wraplength=360,
        ).pack(anchor="w")

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
