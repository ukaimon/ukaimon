from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class AppConfig:
    watch_folder: str = "idsサンプル"
    output_folder: str = "data"
    iviumsoft_exe_path: str = ""
    ivium_method_template_path: str = ""
    ivium_device_serial: str = ""
    ivium_result_timeout_sec: float = 180.0
    ivium_poll_interval_sec: float = 1.0
    target_extensions: list[str] = field(default_factory=lambda: [".ids"])
    parser_type: str = "ivium_ids"
    representative_cycle_rule: str = "Cycle1"
    baseline_correction: bool = True
    plot_enabled: bool = True
    autosave_excel: bool = True
    mean_voltammogram_enabled: bool = True
    mean_voltammogram_include_flags: list[str] = field(default_factory=lambda: ["valid"])
    interpolation_enabled: bool = True
    interpolation_points: int = 400
    interpolation_method: str = "linear"

    @classmethod
    def load(cls, example_path: Path, local_path: Path | None = None) -> "AppConfig":
        payload: dict[str, object] = {}
        if example_path.exists():
            payload.update(json.loads(example_path.read_text(encoding="utf-8")))
        if local_path and local_path.exists():
            payload.update(json.loads(local_path.read_text(encoding="utf-8")))
        return cls(**payload)

    def save_local(self, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "watch_folder": self.watch_folder,
            "output_folder": self.output_folder,
            "iviumsoft_exe_path": self.iviumsoft_exe_path,
            "ivium_method_template_path": self.ivium_method_template_path,
            "ivium_device_serial": self.ivium_device_serial,
            "ivium_result_timeout_sec": self.ivium_result_timeout_sec,
            "ivium_poll_interval_sec": self.ivium_poll_interval_sec,
            "target_extensions": self.target_extensions,
            "parser_type": self.parser_type,
            "representative_cycle_rule": self.representative_cycle_rule,
            "baseline_correction": self.baseline_correction,
            "plot_enabled": self.plot_enabled,
            "autosave_excel": self.autosave_excel,
            "mean_voltammogram_enabled": self.mean_voltammogram_enabled,
            "mean_voltammogram_include_flags": self.mean_voltammogram_include_flags,
            "interpolation_enabled": self.interpolation_enabled,
            "interpolation_points": self.interpolation_points,
            "interpolation_method": self.interpolation_method,
        }
