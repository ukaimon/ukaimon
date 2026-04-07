from __future__ import annotations

import shutil
import unittest
import uuid
from pathlib import Path

from bootstrap_launcher import build_install_command, venv_is_usable, wheelhouse_has_wheels


class BootstrapLauncherTests(unittest.TestCase):
    def make_temp_root(self) -> Path:
        root = Path("tests") / "_tmp_bootstrap_launcher" / uuid.uuid4().hex
        root.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, root, True)
        return root

    def test_wheelhouse_detection_false_without_wheels(self) -> None:
        root = self.make_temp_root()
        self.assertFalse(wheelhouse_has_wheels(root))

    def test_wheelhouse_detection_true_with_wheel_file(self) -> None:
        root = self.make_temp_root()
        wheelhouse = root / "vendor" / "wheels"
        wheelhouse.mkdir(parents=True)
        (wheelhouse / "watchdog-6.0.0-py3-none-any.whl").write_text("placeholder", encoding="utf-8")
        self.assertTrue(wheelhouse_has_wheels(root))

    def test_install_command_uses_wheelhouse_when_available(self) -> None:
        root = self.make_temp_root()
        wheelhouse = root / "vendor" / "wheels"
        wheelhouse.mkdir(parents=True)
        (wheelhouse / "numpy.whl").write_text("placeholder", encoding="utf-8")
        command = build_install_command(Path("D:/sample/.venv/Scripts/python.exe"), root)
        self.assertIn("--no-index", command)
        self.assertTrue(any(part.startswith("--find-links=") for part in command))

    def test_install_command_skips_wheelhouse_flags_when_unavailable(self) -> None:
        root = self.make_temp_root()
        command = build_install_command(Path("D:/sample/.venv/Scripts/python.exe"), root)
        self.assertNotIn("--no-index", command)

    def test_venv_is_not_usable_when_python_missing(self) -> None:
        root = self.make_temp_root()
        self.assertFalse(venv_is_usable(root / ".venv" / "Scripts" / "python.exe", root))


if __name__ == "__main__":
    unittest.main()
