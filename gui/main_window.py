from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from core.services import AppServices
from gui.batch_plan_tab import BatchPlanTab
from gui.condition_tab import ConditionTab
from gui.cross_report_tab import CrossReportTab
from gui.export_tab import ExportTab
from gui.home_tab import HomeTab
from gui.measurement_tab import MeasurementTab
from gui.mip_tab import MipTab
from gui.mip_usage_tab import MipUsageTab
from gui.session_detail_tab import SessionDetailTab
from gui.session_tab import SessionTab
from gui.watcher_tab import WatcherTab


class MainWindow:
    def __init__(self, services: AppServices) -> None:
        self.services = services
        self.root = tk.Tk()
        self.root.title("電気化学実験データ管理 GUI")
        self.root.geometry("1500x960")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        style = ttk.Style(self.root)
        style.configure("TLabel", font=("Yu Gothic UI", 10))
        style.configure("TButton", font=("Yu Gothic UI", 10))

        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True)

        self.tabs = [
            ("ホーム", HomeTab(notebook, services, self.refresh_all)),
            ("MIP 管理", MipTab(notebook, services, self.refresh_all)),
            ("MIP 使用記録", MipUsageTab(notebook, services, self.refresh_all)),
            ("セッション管理", SessionTab(notebook, services, self.refresh_all)),
            ("条件管理", ConditionTab(notebook, services, self.refresh_all)),
            ("バッチ実行計画", BatchPlanTab(notebook, services, self.refresh_all)),
            ("測定追加", MeasurementTab(notebook, services, self.refresh_all)),
            (".ids 監視", WatcherTab(notebook, services, self.refresh_all)),
            ("セッション詳細", SessionDetailTab(notebook, services, self.refresh_all)),
            ("横断比較", CrossReportTab(notebook, services, self.refresh_all)),
            ("レポート出力", ExportTab(notebook, services, self.refresh_all)),
        ]
        for title, tab in self.tabs:
            notebook.add(tab, text=title)

    def refresh_all(self) -> None:
        for _, tab in self.tabs:
            if hasattr(tab, "refresh_tab"):
                tab.refresh_tab()

    def _on_close(self) -> None:
        self.services.stop_watcher()
        self.root.destroy()

    def run(self) -> None:
        self.refresh_all()
        self.root.mainloop()
