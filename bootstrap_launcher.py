from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Iterable


MINIMUM_PYTHON = (3, 11)
REQUIRED_MODULES = (
    "pandas",
    "numpy",
    "scipy",
    "matplotlib",
    "openpyxl",
    "watchdog",
)


def project_root() -> Path:
    return Path(__file__).resolve().parent


def venv_python_path(root: Path) -> Path:
    return root / ".venv" / "Scripts" / "python.exe"


def wheelhouse_path(root: Path) -> Path:
    return root / "vendor" / "wheels"


def wheelhouse_has_wheels(root: Path) -> bool:
    wheelhouse = wheelhouse_path(root)
    return wheelhouse.exists() and any(wheelhouse.glob("*.whl"))


def build_install_command(venv_python: Path, root: Path) -> list[str]:
    command = [str(venv_python), "-m", "pip", "install"]
    if wheelhouse_has_wheels(root):
        command.extend(["--no-index", f"--find-links={wheelhouse_path(root)}"])
    command.extend(["-r", str(root / "requirements.txt")])
    return command


def run_command(command: list[str], root: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=root,
        check=check,
        text=True,
    )


def ensure_python_version() -> None:
    if sys.version_info < MINIMUM_PYTHON:
        version_text = ".".join(str(value) for value in MINIMUM_PYTHON)
        raise RuntimeError(f"Python {version_text} 以上が必要です。")


def venv_is_usable(venv_python: Path, root: Path) -> bool:
    if not venv_python.exists():
        return False
    probe = subprocess.run(
        [str(venv_python), "-c", "import sys"],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return probe.returncode == 0


def ensure_venv(root: Path) -> Path:
    venv_python = venv_python_path(root)
    if venv_is_usable(venv_python, root):
        return venv_python
    if venv_python.exists():
        print("既存の `.venv` をこの PC 用に作り直します。")
        run_command([sys.executable, "-m", "venv", "--clear", str(root / ".venv")], root)
    else:
        print("`.venv` が見つからないため、この PC 用に仮想環境を作成します。")
        run_command([sys.executable, "-m", "venv", str(root / ".venv")], root)
    return venv_python


def ensure_dependencies(root: Path, venv_python: Path, modules: Iterable[str] = REQUIRED_MODULES) -> None:
    probe_command = [
        str(venv_python),
        "-c",
        (
            "import importlib.util, sys; "
            f"modules = {list(modules)!r}; "
            "missing = [name for name in modules if importlib.util.find_spec(name) is None]; "
            "sys.exit(1 if missing else 0)"
        ),
    ]
    probe = run_command(probe_command, root, check=False)
    if probe.returncode == 0:
        return

    if wheelhouse_has_wheels(root):
        print("ローカルの wheelhouse から依存関係をインストールします。")
    else:
        print("依存関係をインストールします。初回は数分かかることがあります。")
    run_command(build_install_command(venv_python, root), root)


def launch_app(root: Path, venv_python: Path) -> int:
    return run_command([str(venv_python), str(root / "app.py")], root, check=False).returncode


def main() -> int:
    root = project_root()
    try:
        ensure_python_version()
        venv_python = ensure_venv(root)
        ensure_dependencies(root, venv_python)
        return launch_app(root, venv_python)
    except Exception as error:  # pragma: no cover - bootstrap CLI
        print(f"起動準備に失敗しました: {error}")
        if not wheelhouse_has_wheels(root):
            print("ネットワークがない PC で使う場合は、事前に vendor/wheels を用意してください。")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
