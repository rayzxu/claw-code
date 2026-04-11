#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))

try:
    from openpyxl import Workbook
except Exception:  # pragma: no cover
    Workbook = None  # type: ignore[assignment]

import materials_search_lib


@unittest.skipIf(Workbook is None, "openpyxl is not installed")
class MaterialsSearchTests(unittest.TestCase):
    def test_default_data_dir_points_inside_plugin(self) -> None:
        self.assertEqual(materials_search_lib.default_material_data_dir(), ROOT / "data" / "material")

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        self._write_master()
        self._write_b()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_master(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "Sheet1"
        ws.append([
            "材料編號",
            "化學名",
            "CAS number",
            "IUPAC Name",
            "SMILES",
            "固化劑體系",
            "分類",
            "密度",
            "粘度",
        ])
        ws.append(["H-50", "Cyclohexane diamine", "123-45-6", "iupac", "C1", "胺类体系", "固化剂", "1.2 g/cm3", "500 cP"])
        ws.append(["EL-127", "Epoxy resin", "25068-38-6", None, None, "", "树脂", 1.16, None])
        wb.save(self.data_dir / materials_search_lib.MASTER_MAP_FILENAME)

    def _write_b(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = "B 剂"
        ws.append(["材料", "体系", "功能", "备注"])
        ws.append(["H-50", "胺类", "固化剂", ""])
        ws.append(["H-19-G", "酸酐", "促进剂", ""])
        wb.save(self.data_dir / materials_search_lib.B_CLASSIFICATION_FILENAME)

    def run_tool(self, payload: dict) -> dict:
        return materials_search_lib.run_tool({"data_dir": str(self.data_dir), **payload})

    def test_schema_lookup_search_filter_and_validate(self) -> None:
        schema = self.run_tool({"action": "schema"})
        self.assertEqual(schema["status"], "ok")
        self.assertEqual(schema["results"][0]["dataset_count"], 2)
        self.assertEqual(schema["safety_results"][0]["dataset_count"], 2)

        lookup = self.run_tool({"action": "lookup", "materials": ["H-50"], "cas_numbers": ["25068-38-6"]})
        self.assertEqual(lookup["status"], "ok")
        self.assertEqual({row["material_code"] for row in lookup["results"]}, {"H-50", "EL-127"})
        self.assertTrue(lookup["safety_results"][0]["matched"])

        search = self.run_tool({"action": "search", "query": "cyclohexane", "field": "chemical"})
        self.assertEqual(search["results"][0]["material_code"], "H-50")

        filtered = self.run_tool({"action": "filter", "family": "胺类体系", "category": "固化剂", "has_fields": ["粘度"]})
        self.assertEqual(filtered["results"][0]["material_code"], "H-50")

        validation = self.run_tool({"action": "validate", "materials": ["H-50"], "target_family": "酸酐体系"})
        self.assertEqual(validation["safety_results"][0]["overall_status"], "conditional")

    def test_error_paths_stay_structured(self) -> None:
        missing = materials_search_lib.run_tool({"action": "schema", "data_dir": str(self.data_dir / "missing")})
        self.assertEqual(missing["status"], "error")
        self.assertIn("errors", missing)

        empty_search = self.run_tool({"action": "search", "query": ""})
        self.assertEqual(empty_search["status"], "warning")
        self.assertEqual(empty_search["results"], [])

        unmatched = self.run_tool({"action": "lookup", "materials": ["UNKNOWN"]})
        self.assertEqual(unmatched["status"], "warning")
        self.assertTrue(unmatched["warnings"])

    def test_wrapper_emits_json(self) -> None:
        wrapper = ROOT / "tools" / "materials_search"
        payload = {"action": "lookup", "materials": ["H-50"], "data_dir": str(self.data_dir)}
        completed = subprocess.run(
            [str(wrapper)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
        )
        decoded = json.loads(completed.stdout)
        self.assertEqual(decoded["status"], "ok")
        self.assertEqual(decoded["results"][0]["material_code"], "H-50")


if __name__ == "__main__":
    unittest.main()
