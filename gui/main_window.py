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
from gui.navigation import RECORD_TYPE_TO_TAB_TITLE
from gui.restore_tab import RestoreTab
from gui.session_detail_tab import SessionDetailTab
from gui.session_tab import SessionTab
from gui.theme import apply_app_theme
from gui.watcher_tab import WatcherTab


class MainWindow:
    def __init__(self, services: AppServices) -> None:
        self.services = services
        self.root = tk.Tk()
        self.root.title("電気化学実験データ管理 GUI")
        self.root.geometry("1680x1040")
        self.root.minsize(1380, 880)
        try:
            self.root.state("zoomed")
        except tk.TclError:
            pass
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        apply_app_theme(self.root)

        shell = ttk.Frame(self.root, style="Card.TFrame", padding=10)
        shell.pack(fill="both", expand=True, padx=8, pady=8)

        self.notebook = ttk.Notebook(shell)
        self.notebook.pack(fill="both", expand=True)

        self.tabs = [
            ("ホーム", HomeTab(self.notebook, services, self.refresh_all)),
            ("MIP 管理", MipTab(self.notebook, services, self.refresh_all)),
            ("MIP 使用記録", MipUsageTab(self.notebook, services, self.refresh_all)),
            ("セッション管理", SessionTab(self.notebook, services, self.refresh_all)),
            ("条件管理", ConditionTab(self.notebook, services, self.refresh_all)),
            ("バッチ実行計画", BatchPlanTab(self.notebook, services, self.refresh_all)),
            ("測定追加", MeasurementTab(self.notebook, services, self.refresh_all)),
            (".ids 監視", WatcherTab(self.notebook, services, self.refresh_all)),
            ("復元", RestoreTab(self.notebook, services, self.refresh_all)),
            ("セッション詳細", SessionDetailTab(self.notebook, services, self.refresh_all)),
            ("横断比較", CrossReportTab(self.notebook, services, self.refresh_all)),
            ("レポート出力", ExportTab(self.notebook, services, self.refresh_all)),
        ]
        self.tabs_by_title: dict[str, ttk.Frame] = {}
        for title, tab in self.tabs:
            self.notebook.add(tab, text=title)
            self.tabs_by_title[title] = tab
            setattr(tab, "navigate_to_record", self.navigate_to_record)

    def navigate_to_record(self, record_type: str, record_id: str) -> None:
        tab_title = RECORD_TYPE_TO_TAB_TITLE.get(record_type)
        if not tab_title or not record_id:
            return
        tab = self.tabs_by_title.get(tab_title)
        if not tab:
            return
        if hasattr(tab, "refresh_tab"):
            tab.refresh_tab()
        self.notebook.select(tab)
        focus_record = getattr(tab, "focus_record", None)
        if callable(focus_record):
            focus_record(record_id)

    def refresh_all(self) -> None:
        for _, tab in self.tabs:
            if hasattr(tab, "refresh_tab"):
                tab.refresh_tab()

    def _on_close(self) -> None:
        self.services.shutdown()
        self.root.destroy()

    def run(self) -> None:
        self.refresh_all()
        self.root.mainloop()
