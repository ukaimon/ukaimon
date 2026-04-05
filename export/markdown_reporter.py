from __future__ import annotations

from pathlib import Path

import pandas as pd


def _frame_to_pipe_table(dataframe: pd.DataFrame, max_rows: int = 8) -> str:
    if dataframe.empty:
        return "_データなし_"
    preview = dataframe.head(max_rows).fillna("")
    headers = list(preview.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in preview.iterrows():
        lines.append("| " + " | ".join(str(row[column]) for column in headers) + " |")
    return "\n".join(lines)


class MarkdownReporter:
    def export_session_report(
        self,
        session_row: dict[str, object],
        conditions: pd.DataFrame,
        measurements: pd.DataFrame,
        aggregates: pd.DataFrame,
        output_path: Path,
    ) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        content = f"""# セッションレポート

## セッション概要

- session_id: {session_row.get("session_id", "")}
- session_name: {session_row.get("session_name", "")}
- session_date: {session_row.get("session_date", "")}
- analyte: {session_row.get("analyte", "")}
- method_default: {session_row.get("method_default", "")}
- electrolyte: {session_row.get("electrolyte", "")}

## 条件一覧

{_frame_to_pipe_table(conditions)}

## 測定一覧

{_frame_to_pipe_table(measurements)}

## 集計結果

{_frame_to_pipe_table(aggregates)}
"""
        output_path.write_text(content, encoding="utf-8")
        return output_path
