from __future__ import annotations

import unittest

from utils.file_utils import generate_condition_id, generate_id


class IdGenerationTests(unittest.TestCase):
    def test_generate_id_uses_readable_datetime_format(self) -> None:
        generated_id = generate_id("mip")
        self.assertRegex(generated_id, r"^MIP-\d{8}-\d{4}-[A-Z]{2}$")

    def test_generate_id_is_unique_across_multiple_calls(self) -> None:
        generated_ids = [generate_id("SES") for _ in range(20)]
        self.assertEqual(len(generated_ids), len(set(generated_ids)))

    def test_generate_condition_id_includes_concentration_token(self) -> None:
        generated_id = generate_condition_id(10.5, "ppm")
        self.assertRegex(generated_id, r"^COND-\d{8}-10p5ppm-\d{4}-[A-Z]{2}$")

    def test_generate_condition_id_is_unique_per_same_condition(self) -> None:
        generated_ids = [generate_condition_id(0, "ppm") for _ in range(5)]
        self.assertEqual(len(generated_ids), len(set(generated_ids)))

    def test_generate_condition_id_trims_trailing_decimal_zero(self) -> None:
        generated_id = generate_condition_id(10.0, "ppm")
        self.assertRegex(generated_id, r"^COND-\d{8}-10ppm-\d{4}-[A-Z]{2}$")

    def test_generate_condition_id_zero_uses_simple_zero_token(self) -> None:
        generated_id = generate_condition_id(0.0, "ppm")
        self.assertRegex(generated_id, r"^COND-\d{8}-0ppm-\d{4}-[A-Z]{2}$")


if __name__ == "__main__":
    unittest.main()
