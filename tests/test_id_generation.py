from __future__ import annotations

import unittest

from utils.file_utils import generate_id


class IdGenerationTests(unittest.TestCase):
    def test_generate_id_uses_readable_datetime_format(self) -> None:
        generated_id = generate_id("mip")
        self.assertRegex(generated_id, r"^MIP-\d{8}-\d{4}-[A-Z]{2}$")

    def test_generate_id_is_unique_across_multiple_calls(self) -> None:
        generated_ids = [generate_id("SES") for _ in range(20)]
        self.assertEqual(len(generated_ids), len(set(generated_ids)))


if __name__ == "__main__":
    unittest.main()
