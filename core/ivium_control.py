from __future__ import annotations

import ctypes
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


IVIUM_METHOD_NAME_ALIASES = {
    "cv": "CyclicVoltammetry",
    "cyclic voltammetry": "CyclicVoltammetry",
    "cyclicvoltammetry": "CyclicVoltammetry",
    "lsv": "LinearSweep",
    "linear sweep": "LinearSweep",
    "linearsweep": "LinearSweep",
    "dpv": "DifferentialPulseVoltammetry",
    "differential pulse voltammetry": "DifferentialPulseVoltammetry",
    "differentialpulsevoltammetry": "DifferentialPulseVoltammetry",
}

APP_METHOD_NAME_ALIASES = {
    "cv": "CV",
    "cyclic voltammetry": "CV",
    "cyclicvoltammetry": "CV",
    "lsv": "LSV",
    "linear sweep": "LSV",
    "linearsweep": "LSV",
    "dpv": "DPV",
    "differential pulse voltammetry": "DPV",
    "differentialpulsevoltammetry": "DPV",
}

CONDITION_TO_METHOD_KEYS = {
    "potential_start_v": "E start",
    "potential_end_v": "E end",
    "potential_vertex_1_v": "Vertex 1",
    "potential_vertex_2_v": "Vertex 2",
    "scan_rate_v_s": "Scanrate",
    "step_v": "E step",
    "pulse_amplitude_v": "Pulse amplitude",
    "pulse_time_s": "Pulse time",
    "cycles": "N scans",
    "current_range": "Current Range",
    "filter_setting": "Filter",
}


@dataclass(slots=True)
class PreparedIviumMethod:
    method_name: str
    method_file_path: str
    applied_parameters: dict[str, str]


def resolve_ivium_method_name(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "CyclicVoltammetry"
    return IVIUM_METHOD_NAME_ALIASES.get(text.lower(), text)


def resolve_app_method_name(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "CV"
    return APP_METHOD_NAME_ALIASES.get(text.lower(), text)


def resolve_ivium_driver_path(iviumsoft_exe_path: str | Path) -> Path:
    exe_path = Path(iviumsoft_exe_path)
    base_dir = exe_path if exe_path.is_dir() else exe_path.parent
    candidates = [
        base_dir / "Software Development Driver" / "Ivium_remdriver64.dll",
        base_dir / "Software Development Driver" / "Ivium_remdriver.dll",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Ivium driver DLL が見つかりません: {base_dir}")


def resolve_ivium_method_template_path(configured_path: str | Path, iviumsoft_exe_path: str | Path) -> Path:
    configured_text = str(configured_path or "").strip()
    if configured_text:
        candidate = Path(configured_text)
        if candidate.exists():
            return candidate
    exe_path = Path(iviumsoft_exe_path)
    base_dir = exe_path if exe_path.is_dir() else exe_path.parent
    default_path = base_dir / "iviumsoft.imf"
    if default_path.exists():
        return default_path
    raise FileNotFoundError("Ivium のメソッドテンプレートが見つかりません。")


def resolve_ivium_db_file_path(iviumsoft_exe_path: str | Path, db_file_name: str | None) -> Path | None:
    text = str(db_file_name or "").strip()
    if not text:
        return None
    candidate = Path(text)
    if candidate.is_absolute():
        return candidate
    exe_path = Path(iviumsoft_exe_path)
    base_dir = exe_path if exe_path.is_dir() else exe_path.parent
    return (base_dir / "DataServer" / "measurements" / candidate).resolve()


def _format_method_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.9g}"
    return str(value).strip()


def _sanitize_name(value: str) -> str:
    sanitized = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return sanitized.strip("_") or "ivium"


def build_ivium_method_updates(
    batch_item: dict[str, Any],
    condition_row: dict[str, Any],
    session_row: dict[str, Any],
) -> tuple[str, dict[str, str]]:
    method_name = resolve_ivium_method_name(
        str(condition_row.get("ivium_method_name") or condition_row.get("method") or session_row.get("method_default") or "")
    )
    title = f"{batch_item['batch_item_id']}_{condition_row.get('condition_id', '')}_rep{batch_item.get('rep_no', '')}"
    updates: dict[str, str] = {
        "Method": method_name,
        "Title": title,
    }
    if condition_row.get("quiet_time_s") not in (None, ""):
        quiet_time = _format_method_value(condition_row["quiet_time_s"])
        updates["Quiet time"] = quiet_time
        updates["Equilibration time"] = quiet_time
    for field_name, method_key in CONDITION_TO_METHOD_KEYS.items():
        value = condition_row.get(field_name)
        if value in (None, ""):
            continue
        updates[method_key] = _format_method_value(value)
    if condition_row.get("current_range") not in (None, ""):
        updates["AutoCR"] = "false"
    return method_name, updates


def render_ivium_method_text(template_text: str, updates: dict[str, str]) -> str:
    pending_updates = dict(updates)
    rendered_lines: list[str] = []
    for raw_line in template_text.replace("\r\n", "\n").split("\n"):
        if "=" not in raw_line:
            rendered_lines.append(raw_line)
            continue
        key, _value = raw_line.split("=", 1)
        normalized_key = key.strip()
        if normalized_key in pending_updates:
            rendered_lines.append(f"{normalized_key}={pending_updates.pop(normalized_key)}")
        else:
            rendered_lines.append(raw_line)
    for key, value in pending_updates.items():
        rendered_lines.append(f"{key}={value}")
    return "\n".join(rendered_lines).rstrip() + "\n"


def prepare_ivium_method_file(
    *,
    template_path: str | Path,
    batch_item: dict[str, Any],
    condition_row: dict[str, Any],
    session_row: dict[str, Any],
) -> PreparedIviumMethod:
    template = Path(template_path)
    if not template.exists():
        raise FileNotFoundError(f"メソッドテンプレートが見つかりません: {template}")
    method_name, updates = build_ivium_method_updates(batch_item, condition_row, session_row)
    rendered_text = render_ivium_method_text(template.read_text(encoding="mbcs", errors="ignore"), updates)
    output_dir = Path(tempfile.gettempdir()) / "cv_auto_ivium_methods"
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = (
        f"{_sanitize_name(str(batch_item.get('batch_item_id', 'batch')))}_"
        f"{_sanitize_name(str(condition_row.get('condition_id', 'condition')))}_"
        f"rep{int(batch_item.get('rep_no', 0) or 0):02d}.imf"
    )
    method_file_path = output_dir / file_name
    method_file_path.write_text(rendered_text, encoding="mbcs", errors="ignore")
    return PreparedIviumMethod(
        method_name=method_name,
        method_file_path=str(method_file_path),
        applied_parameters=updates,
    )


class IviumRemoteDriver:
    def __init__(self, dll_path: str | Path) -> None:
        self.dll_path = Path(dll_path)
        self.dll = ctypes.WinDLL(str(self.dll_path))
        self._configure_signatures()

    def _configure_signatures(self) -> None:
        self.dll.IV_open.restype = ctypes.c_long
        self.dll.IV_close.restype = ctypes.c_long
        self.dll.IV_getdevicestatus.restype = ctypes.c_long
        self.dll.IV_VersionCheck.restype = ctypes.c_long
        self.dll.IV_abort.restype = ctypes.c_long
        self.dll.IV_VersionDll.restype = ctypes.c_long
        self.dll.IV_selectdevice.argtypes = [ctypes.POINTER(ctypes.c_long)]
        self.dll.IV_connect.argtypes = [ctypes.POINTER(ctypes.c_long)]
        self.dll.IV_SelectChannel.argtypes = [ctypes.POINTER(ctypes.c_long)]
        self.dll.IV_selectdevice.restype = None
        self.dll.IV_readSN.argtypes = [ctypes.c_char_p]
        self.dll.IV_readSN.restype = ctypes.c_long
        self.dll.IV_SelectSn.argtypes = [ctypes.c_char_p]
        self.dll.IV_SelectSn.restype = ctypes.c_long
        self.dll.IV_startmethod.argtypes = [ctypes.c_char_p]
        self.dll.IV_startmethod.restype = ctypes.c_long
        self.dll.IV_setmethodparameter.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
        self.dll.IV_setmethodparameter.restype = ctypes.c_long
        self.dll.IV_Ndatapoints.argtypes = [ctypes.POINTER(ctypes.c_long)]
        self.dll.IV_Ndatapoints.restype = ctypes.c_long
        self.dll.IV_getcellstatus.argtypes = [ctypes.POINTER(ctypes.c_long)]
        self.dll.IV_getcellstatus.restype = ctypes.c_long
        self.dll.IV_getDbFileName.argtypes = [ctypes.c_char_p]
        self.dll.IV_getDbFileName.restype = ctypes.c_long

    @staticmethod
    def _check_status(result: int, action: str) -> int:
        if int(result) < 0:
            raise RuntimeError(f"Ivium driver 呼び出しに失敗しました: {action} ({result})")
        return int(result)

    @staticmethod
    def _encode_text(value: str) -> bytes:
        return value.encode("mbcs", errors="ignore")

    @staticmethod
    def _decode_text(value: bytes) -> str:
        return value.split(b"\x00", 1)[0].decode("mbcs", errors="ignore").strip()

    def open(self) -> int:
        return self._check_status(self.dll.IV_open(), "IV_open")

    def close(self) -> int:
        return self._check_status(self.dll.IV_close(), "IV_close")

    def version_check(self) -> int:
        return self._check_status(self.dll.IV_VersionCheck(), "IV_VersionCheck")

    def select_serial(self, serial_number: str) -> int:
        return self._check_status(self.dll.IV_SelectSn(self._encode_text(serial_number)), "IV_SelectSn")

    def connect(self, connect_value: int = 1) -> int:
        value = ctypes.c_long(connect_value)
        self._check_status(self.dll.IV_connect(ctypes.byref(value)), "IV_connect")
        return int(value.value)

    def read_serial(self) -> str:
        buffer = ctypes.create_string_buffer(256)
        self._check_status(self.dll.IV_readSN(buffer), "IV_readSN")
        return self._decode_text(buffer.raw)

    def start_method(self, method_file_path: str | Path) -> int:
        return self._check_status(self.dll.IV_startmethod(self._encode_text(str(method_file_path))), "IV_startmethod")

    def abort(self) -> int:
        return self._check_status(self.dll.IV_abort(), "IV_abort")

    def get_device_status(self) -> int:
        return int(self.dll.IV_getdevicestatus())

    def get_cell_status(self) -> int:
        value = ctypes.c_long()
        self._check_status(self.dll.IV_getcellstatus(ctypes.byref(value)), "IV_getcellstatus")
        return int(value.value)

    def get_n_datapoints(self) -> int:
        value = ctypes.c_long()
        self._check_status(self.dll.IV_Ndatapoints(ctypes.byref(value)), "IV_Ndatapoints")
        return int(value.value)

    def get_db_file_name(self) -> str:
        buffer = ctypes.create_string_buffer(2048)
        self._check_status(self.dll.IV_getDbFileName(buffer), "IV_getDbFileName")
        return self._decode_text(buffer.raw)
