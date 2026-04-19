from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from analysis.session_plot_analysis import extract_cycle_curves
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

    def test_parser_preserves_multiple_primary_blocks_as_cycles(self) -> None:
        file_path = self._write_ids(
            "multi_block.ids",
            """
            Method=CyclicVoltammetry
            primary_data
            8
            Potential/V Current/A Time/s
            0.0 0.01 0.0
            0.1 0.02 0.1
            0.2 0.03 0.2
            0.3 0.04 0.3
            0.4 0.05 0.4
            0.5 0.06 0.5
            0.4 0.05 0.6
            0.3 0.04 0.7

            Method=CyclicVoltammetry
            primary_data
            8
            Potential/V Current/A Time/s
            0.0 0.11 1.0
            0.1 0.12 1.1
            0.2 0.13 1.2
            0.3 0.14 1.3
            0.4 0.15 1.4
            0.5 0.16 1.5
            0.4 0.15 1.6
            0.3 0.14 1.7
            """,
        )
        parsed = parse_ids_file(file_path)
        cycles = extract_cycle_curves(parsed)

        self.assertEqual(len(parsed.metadata.get("available_blocks", [])), 2)
        self.assertEqual(len(parsed.data_blocks), 2)
        self.assertEqual(len(cycles), 2)
        self.assertAlmostEqual(float(cycles[1]["current_a"].iloc[0]), 0.11)

    def test_parser_handles_mixed_spacing_numeric_rows_without_truncation(self) -> None:
        file_path = self._write_ids(
            "mixed_spacing.ids",
            """
            Method=CyclicVoltammetry
            primary_data
            10
            Potential/V Current/A Time/s
            0.00000E+00  -2.00000E-06 -9.00000E-04
            1.00000E-03  -1.00000E-06 1.00000E-04
            2.00000E-03  -8.00000E-07 1.10000E-03
            3.00000E-03  -6.00000E-07 2.10000E-03
            4.00000E-03  -5.00000E-07 3.10000E-03
            4.48000E-01  5.83979E-08  4.47063E-01
            4.47000E-01  4.79879E-08  4.45930E-01
            4.46000E-01  2.71194E-08  4.44903E-01
            4.45000E-01  1.46907E-08  4.43886E-01
            4.44000E-01  -2.09172E-09 4.42942E-01
            """,
        )
        parsed = parse_ids_file(file_path)

        self.assertEqual(len(parsed.data), 10)
        self.assertAlmostEqual(float(parsed.data["potential_v"].iloc[-1]), 0.444)
        self.assertAlmostEqual(float(parsed.data["current_a"].iloc[-1]), -2.09172e-09)


if __name__ == "__main__":
    unittest.main()
