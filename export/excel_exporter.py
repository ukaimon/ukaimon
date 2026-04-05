from __future__ import annotations

from pathlib import Path

import pandas as pd


def _sanitize_sheet_name(name: str) -> str:
    invalid = set("\\/*?:[]")
    cleaned = "".join("_" if character in invalid else character for character in name)
    return cleaned[:31] or "Sheet"


class ExcelExporter:
    def export_frames(self, frames: dict[str, pd.DataFrame], output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for sheet_name, dataframe in frames.items():
                dataframe.to_excel(writer, sheet_name=_sanitize_sheet_name(sheet_name), index=False)
        return output_path
