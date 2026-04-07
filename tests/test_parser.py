from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from parsers.ivium_ids_parser import parse_ids_file


class ParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_root = Path("tests") / "_tmp_parser" / uuid.uuid4().hex
        self.temp_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def _write_ids(self, name: str, content: str) -> Path:
        path = self.temp_root / name
        path.write_text(content.strip() + "\n", encoding="utf-8")
        return path

    def test_parse_sample_ids(self) -> None:
        parsed = parse_ids_file(Path("idsサンプル") / "1527_260402CV_P33107.ids")
        self.assertEqual(parsed.metadata.get("Method"), "CyclicVoltammetry")
        self.assertEqual(parsed.data.shape[1], 3)
        self.assertIn("potential_v", parsed.data.columns)
        self.assertIn("current_a", parsed.data.columns)
        self.assertIn("time_s", parsed.data.columns)

    def test_parser_recovers_without_primary_data_marker(self) -> None:
        file_path = self._write_ids(
            "fallback.ids",
            """
            Method: CyclicVoltammetry
            Start Time: 2026/04/07 10:00:00
            Potential/V Current/A Time/s
            0.0 0.01 0.0
            0.1 0.02 0.1
            0.2 0.03 0.2
            0.3 0.04 0.3
            0.4 0.05 0.4
            0.5 0.06 0.5
            0.6 0.07 0.6
            0.7 0.08 0.7
            0.8 0.09 0.8
            """,
        )
        parsed = parse_ids_file(file_path)
        self.assertTrue(parsed.metadata.get("parser_recovered"))
        self.assertEqual(parsed.metadata.get("Method"), "CyclicVoltammetry")
        self.assertEqual(list(parsed.detected_columns.keys()), ["potential", "current", "time"])
        self.assertIn("potential_v", parsed.data.columns)
        self.assertIn("current_a", parsed.data.columns)
        self.assertIn("time_s", parsed.data.columns)

    def test_parser_skips_broken_rows_inside_numeric_block(self) -> None:
        file_path = self._write_ids(
            "broken.ids",
            """
            Method=CyclicVoltammetry
            primary_data
            10
            Potential/V Current/A Time/s
            0.0 0.01 0.0
            0.1 0.02 0.1
            broken row should be ignored
            0.2 0.03 0.2
            0.3 0.04 0.3
            0.4 0.05 0.4
            0.5 0.06 0.5
            0.6 0.07 0.6
            0.7 0.08 0.7
            0.8 0.09 0.8
            """,
        )
        parsed = parse_ids_file(file_path)
        self.assertGreaterEqual(len(parsed.data), 8)
        self.assertAlmostEqual(float(parsed.data["potential_v"].iloc[2]), 0.2)
        self.assertIn("current_a", parsed.data.columns)


if __name__ == "__main__":
    unittest.main()
