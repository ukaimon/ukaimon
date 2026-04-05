from __future__ import annotations

import unittest
from pathlib import Path

from parsers.ivium_ids_parser import parse_ids_file


class ParserTests(unittest.TestCase):
    def test_parse_sample_ids(self) -> None:
        parsed = parse_ids_file(Path("idsサンプル") / "1527_260402CV_P33107.ids")
        self.assertEqual(parsed.metadata.get("Method"), "CyclicVoltammetry")
        self.assertEqual(parsed.data.shape[1], 3)
        self.assertIn("potential_v", parsed.data.columns)
        self.assertIn("current_a", parsed.data.columns)
        self.assertIn("time_s", parsed.data.columns)


if __name__ == "__main__":
    unittest.main()
