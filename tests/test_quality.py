from __future__ import annotations

import unittest

from core.quality import derive_auto_quality, resolve_final_quality


class QualityTests(unittest.TestCase):
    def test_manual_invalid_wins(self) -> None:
        self.assertEqual(resolve_final_quality("valid", "invalid").value, "invalid")

    def test_auto_invalid_becomes_suspect(self) -> None:
        self.assertEqual(resolve_final_quality("invalid", None).value, "suspect")

    def test_high_noise_marks_invalid(self) -> None:
        self.assertEqual(derive_auto_quality(noise_level=0.95).value, "invalid")


if __name__ == "__main__":
    unittest.main()
