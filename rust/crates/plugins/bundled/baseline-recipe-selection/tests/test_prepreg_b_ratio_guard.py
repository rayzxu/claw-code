#!/usr/bin/env python3
from __future__ import annotations

import json
import pickle
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

import prepreg_b_ratio_guard_lib


@unittest.skipIf(Workbook is None, "openpyxl is not installed")
class PrepregBRatioGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.master_path = self.root / prepreg_b_ratio_guard_lib.MASTER_MAP_FILENAME
        self.history_path = self.root / "recipe-store-v3-test.pkl"
        self._write_master()
        self._write_history()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_master(self) -> None:
        wb = Workbook()
        ws = wb.active
        ws.title = prepreg_b_ratio_guard_lib.MASTER_MAP_SHEET
        ws.append(
            [
                "材料編號",
                "固化劑體系",
                "分類",
                "25℃粘度（mPa.s）",
                "70℃粘度（mPa.s）",
                "固体含量（%）",
                "熔点（℃）",
                "软化点（℃）",
                "外观",
            ]
        )
        ws.append(["PG1", "预浸料", "固化剂", None, None, None, 214.0, None, "白色粉末"])
        ws.append(["PG2", "预浸料", "促进剂/固化剂", None, None, None, 160.0, None, "白色粉末"])
        ws.append(["EL-128", None, "树脂", 6800, None, None, None, None, None])
        ws.append(["Z-213", None, "消泡剂", None, None, None, None, None, "无色液体"])
        wb.save(self.master_path)

    def _write_history(self) -> None:
        records = [
            {
                "experimentId": "exp-1",
                "hardener_system_family": "预浸料体系",
                "materialGroups": {
                    "hardener": {
                        "groupName": "B1",
                        "proportion": 12.0,
                        "components": {"PG1": 33.333, "EL-128": 58.334, "Z-213": 8.333},
                    }
                },
            },
            {
                "experimentId": "exp-2",
                "hardener_system_family": "预浸料体系",
                "materialGroups": {
                    "hardener": {
                        "groupName": "B2",
                        "proportion": 15.0,
                        "components": {"PG1": 30.0, "PG2": 10.0, "EL-128": 60.0},
                    }
                },
            },
            {
                "experimentId": "exp-3",
                "hardener_system_family": "预浸料体系",
                "materialGroups": {
                    "hardener": {
                        "groupName": "B3",
                        "proportion": 10.0,
                        "components": {"PG1": 25.0, "EL-128": 65.0, "Z-213": 10.0},
                    }
                },
            },
        ]
        payload = {
            "source_files": [{"path": "test.json"}],
            "cache_signature": {"path": "test.json", "cache_key": "test"},
            "export_metadata": {"payload_sources": {"merged_payload": len(records)}},
            "dataset_count": len(records),
            "records": records,
            "indexes": {},
            "supported_tests": [],
        }
        self.history_path.write_bytes(pickle.dumps(payload))

    def run_tool(self, payload: dict[str, object]) -> dict[str, object]:
        return prepreg_b_ratio_guard_lib.run_tool(
            {
                "history_path": str(self.history_path),
                "workbooks": {"master_map_path": str(self.master_path)},
                **payload,
            }
        )

    def test_schema_and_pass_case(self) -> None:
        schema = prepreg_b_ratio_guard_lib.run_tool({"action": "schema"})
        self.assertEqual(schema["status"], "ok")
        self.assertIn("analyze", schema["query_summary"]["schema"]["supported_actions"])

        analyzed = self.run_tool(
            {
                "action": "analyze",
                "components": [
                    {"material": "PG1", "amount": 4.0},
                    {"material": "EL-128", "amount": 8.0},
                    {"material": "Z-213", "amount": 1.0},
                ],
            }
        )
        self.assertEqual(analyzed["status"], "ok")
        self.assertEqual(analyzed["hard_filter"]["status"], "pass")
        self.assertAlmostEqual(analyzed["input_summary"]["liquid_to_solid_ratio"], 2.25, places=4)
        self.assertEqual(analyzed["history_summary"]["dataset_count"], 3)
        self.assertTrue(analyzed["example_records"])

    def test_fail_case_and_wrapper(self) -> None:
        analyzed = self.run_tool(
            {
                "action": "analyze",
                "components": [
                    {"material": "PG1", "amount": 6.0},
                    {"material": "EL-128", "amount": 6.0},
                ],
            }
        )
        self.assertEqual(analyzed["status"], "warning")
        self.assertEqual(analyzed["hard_filter"]["status"], "fail")
        self.assertIn("至少再增加", analyzed["suggestions"][0])

        wrapper = ROOT / "tools" / "prepreg_b_ratio_guard"
        completed = subprocess.run(
            [str(wrapper)],
            input=json.dumps(
                {
                    "action": "analyze",
                    "history_path": str(self.history_path),
                    "workbooks": {"master_map_path": str(self.master_path)},
                    "components": [
                        {"material": "PG1", "amount": 4.0},
                        {"material": "EL-128", "amount": 8.0},
                        {"material": "Z-213", "amount": 1.0},
                    ],
                },
                ensure_ascii=False,
            ),
            text=True,
            capture_output=True,
            check=True,
        )
        decoded = json.loads(completed.stdout)
        self.assertEqual(decoded["hard_filter"]["status"], "pass")


if __name__ == "__main__":
    unittest.main()
