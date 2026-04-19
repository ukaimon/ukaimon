from __future__ import annotations

import shutil
import unittest
from pathlib import Path
from uuid import uuid4

import pandas as pd

from core.config import AppConfig
from core.database import DatabaseManager
from core.repositories import ElectrochemRepository
from core.services import AppServices


class SessionPlotAnalysisTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path.cwd()
        self.database_path = self.root / "database" / f"test_session_plot_analysis_{uuid4().hex[:8]}.db"
        config = AppConfig.load(self.root / "config" / "config.example.json")
        repository = ElectrochemRepository(DatabaseManager(self.database_path))
        self.services = AppServices(self.root, config, repository)
        self.services.initialize()
        self.session_id = self._create_session_fixture()
        self.session_output_root = self.root / "data" / "sessions" / self.session_id

    def tearDown(self) -> None:
        shutil.rmtree(self.session_output_root, ignore_errors=True)
        try:
            if self.database_path.exists():
                self.database_path.unlink()
        except PermissionError:
            pass

    def _create_session_fixture(self) -> str:
        sample_file = self.root / "idsサンプル" / "1538_260402CV_P28635.ids"
        mip_id = self.services.create_mip(
            {
                "template_name": "temp",
                "preparation_date": "2026-04-10",
                "operator": "tester",
                "note": "",
                "tags": "",
            }
        )
        usage_id = self.services.create_mip_usage(
            {
                "mip_id": mip_id,
                "cp_preparation_date": "2026-04-10",
                "coating_date": "2026-04-10",
                "operator": "tester",
                "coating_height": "6.8",
                "note": "",
                "tags": "",
            }
        )
        session_id = self.services.create_session(
            {
                "mip_usage_id": usage_id,
                "session_date": "2026-04-10",
                "analyte": "histamine",
                "session_name": "plot-analysis",
                "method_default": "CV",
                "operator": "tester",
                "electrolyte": "",
                "common_note": "",
                "tags": "",
                "status": "draft",
            }
        )
        condition_zero = self.services.create_condition(
            {
                "session_id": session_id,
                "concentration_value": 0.0,
                "concentration_unit": "ppm",
                "method": "CV",
                "planned_replicates": 2,
                "common_note": "",
                "tags": "",
            }
        )
        condition_high = self.services.create_condition(
            {
                "session_id": session_id,
                "concentration_value": 50.0,
                "concentration_unit": "ppm",
                "method": "CV",
                "planned_replicates": 1,
                "common_note": "",
                "tags": "",
            }
        )
        self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_zero,
                "chip_id": "chip-1",
                "wire_id": "wire-1",
                "status": "manual",
                "noise_level": 0.1,
                "raw_file_path": str(sample_file),
            }
        )
        self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_zero,
                "chip_id": "chip-2",
                "wire_id": "wire-2",
                "status": "manual",
                "noise_level": 0.1,
                "raw_file_path": str(sample_file),
            }
        )
        self.services.create_measurement(
            {
                "session_id": session_id,
                "condition_id": condition_high,
                "chip_id": "chip-3",
                "wire_id": "wire-3",
                "status": "manual",
                "noise_level": 0.1,
                "raw_file_path": str(sample_file),
            }
        )
        return session_id

    def test_export_session_analysis_plots_reads_ids_and_writes_outputs(self) -> None:
        outputs = self.services.export_session_analysis_plots(self.session_id)

        required_keys = {
            "analysis_root",
            "source_root",
            "manifest_csv",
            "file_inventory_csv",
            "file_inventory_detail_csv",
            "condition_summary_csv",
            "calibration_fits_csv",
            "absolute_integral_file_csv",
            "absolute_integral_long_csv",
            "absolute_integral_condition_csv",
            "absolute_integral_fit_csv",
            "absolute_loop_area_file_csv",
            "absolute_loop_area_long_csv",
            "absolute_loop_area_condition_csv",
            "absolute_loop_area_fit_csv",
            "cycle1_reference_file_csv",
            "cycle1_reference_long_csv",
            "cycle1_reference_condition_csv",
            "cycle1_reference_fit_csv",
            "individual_plot_dir",
            "mean_plot_dir",
            "overlay_plot",
            "calibration_plot",
            "absolute_integral_plot",
            "absolute_loop_area_plot",
            "cycle1_reference_plot",
            "scan_comparison_fit_csv",
            "scan_comparison_plot",
        }
        self.assertTrue(required_keys.issubset(outputs.keys()))
        for key in required_keys:
            self.assertTrue(Path(outputs[key]).exists(), key)

        inventory_frame = pd.read_csv(outputs["file_inventory_csv"])
        inventory_detail_frame = pd.read_csv(outputs["file_inventory_detail_csv"])
        condition_frame = pd.read_csv(outputs["condition_summary_csv"])
        fit_frame = pd.read_csv(outputs["calibration_fits_csv"])
        absolute_integral_long_frame = pd.read_csv(outputs["absolute_integral_long_csv"])
        absolute_integral_condition_frame = pd.read_csv(outputs["absolute_integral_condition_csv"])
        absolute_loop_area_long_frame = pd.read_csv(outputs["absolute_loop_area_long_csv"])
        absolute_loop_area_condition_frame = pd.read_csv(outputs["absolute_loop_area_condition_csv"])
        cycle1_reference_long_frame = pd.read_csv(outputs["cycle1_reference_long_csv"])
        cycle1_reference_condition_frame = pd.read_csv(outputs["cycle1_reference_condition_csv"])

        self.assertEqual(len(inventory_frame), 3)
        self.assertEqual(len(inventory_detail_frame), 3)
        self.assertEqual(len(condition_frame), 2)
        self.assertEqual(len(fit_frame), 1)
        self.assertIn("min_current_uA", inventory_frame.columns)
        self.assertIn("pressure", inventory_frame.columns)
        self.assertIn("cycle_lengths", inventory_detail_frame.columns)
        self.assertIn("mean_of_file_min_current_uA", condition_frame.columns)
        self.assertIn("mean_voltammogram_min_current_uA", condition_frame.columns)
        self.assertIn("slope", fit_frame.columns)
        self.assertIn("absolute_integral_uA_V", absolute_integral_long_frame.columns)
        self.assertIn("mean_absolute_integral_uA_V", absolute_integral_condition_frame.columns)
        self.assertIn("absolute_loop_area_uA_V", absolute_loop_area_long_frame.columns)
        self.assertIn("mean_absolute_loop_area_uA_V", absolute_loop_area_condition_frame.columns)
        self.assertIn("current_at_cycle1_ref_uA", cycle1_reference_long_frame.columns)
        self.assertIn("mean_current_uA", cycle1_reference_condition_frame.columns)
        self.assertTrue((self.session_output_root / "analysis_output").exists())
        self.assertTrue((self.session_output_root / "analysis_output_scan1").exists())
        self.assertTrue((self.session_output_root / "analysis_output_absolute_integral").exists())
        self.assertTrue((self.session_output_root / "analysis_output_absolute_loop_area").exists())
        self.assertTrue((self.session_output_root / "analysis_output_cycle1_reference_potential").exists())
        self.assertTrue((self.session_output_root / "analysis_output_scan_comparison").exists())

    def test_list_session_analysis_plot_images_returns_generated_pngs(self) -> None:
        self.services.export_session_analysis_plots(self.session_id)

        entries = self.services.list_session_analysis_plot_images(self.session_id)
        summary = self.services.get_session_analysis_plot_summary(self.session_id)

        self.assertGreaterEqual(len(entries), 3)
        self.assertTrue(entries[0]["path"].endswith("absolute_integral_calibration_overlay_scan1_to_5_by_pressure.png"))
        self.assertIn("絶対積分解析", entries[0]["label"])
        self.assertIn("圧力別較正オーバーレイ", entries[0]["label"])
        self.assertTrue(any("絶対ループ面積解析" in entry["label"] for entry in entries))
        self.assertIn("絶対ループ面積", summary)
        self.assertIn("scan比較", summary)
        for entry in entries:
            self.assertTrue(Path(entry["path"]).exists(), entry["path"])
            self.assertTrue(entry["label"])
            self.assertTrue(entry["relative_path"])


if __name__ == "__main__":
    unittest.main()
