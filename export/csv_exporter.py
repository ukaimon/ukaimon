from __future__ import annotations

from pathlib import Path

import pandas as pd


class CSVExporter:
    def export_dataframe(self, dataframe: pd.DataFrame, output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        dataframe.to_csv(output_path, index=False, encoding="utf-8-sig")
        return output_path
