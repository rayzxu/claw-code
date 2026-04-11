#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"
sys.path.insert(0, str(TOOLS))

import baseline_recipe_selection_lib


def build_point(point_id: str, payload: dict) -> dict:
    return {"id": point_id, "payload": payload}


class BaselineRecipeSelectionTests(unittest.TestCase):
    def test_default_export_path_points_inside_plugin(self) -> None:
        self.assertEqual(
            baseline_recipe_selection_lib.resolve_default_export_path(),
            ROOT / "data" / "qdrant_exports" / "aws_test_mixture_recipes.json",
        )

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_root = Path(cls.temp_dir.name)

        resin = [
            build_point(
                "rec-1",
                {
                    "experimentId": "exp-1",
                    "materialGroups": {
                        "resin": {"groupName": "A1", "proportion": 100.0, "components": {"EL-127": 86.55, "C-2475": 13.4}},
                        "hardener": {"groupName": "B1", "proportion": 22.0, "components": {"H-50": 19.4, "WG-24": 20.0}},
                    },
                    "mix_ratio": "100:22",
                    "hardener_system_family": "胺类体系",
                    "hc_subtype_primary": "脂环族胺类",
                    "department": "研发部",
                    "tests": {
                        "resin_viscosity": {"25": 420.0},
                        "EEW": 185.0,
                        "resin_density": {"25": 1.16},
                    },
                    "curing_condition_temp1": 80,
                    "curing_condition_time1": 120,
                },
            ),
            build_point(
                "rec-2",
                {
                    "experimentId": "exp-2",
                    "materialGroups": {
                        "resin": {"groupName": "A2", "proportion": 100.0, "components": {"EL-127": 60.0, "C-2460": 40.0}},
                        "hardener": {"groupName": "B2", "proportion": 30.0, "components": {"H-19-G": 100.0}},
                    },
                    "mix_ratio": "100:30",
                    "hardener_system_family": "酸酐类体系",
                    "hc_subtype_primary": "酸酐",
                    "department": "研发部",
                    "tests": {
                        "resin_viscosity": {"25": 680.0},
                        "EEW": 188.0,
                    },
                    "curing_condition_temp1": 120,
                    "curing_condition_time1": 180,
                },
            ),
        ]
        hardener = [
            build_point("rec-1", {"experimentId": "exp-1", "tests": {"hardener_viscosity": {"25": 55.0}, "AHEW": 61.0}}),
            build_point("rec-2", {"experimentId": "exp-2", "tests": {"hardener_viscosity": {"25": 90.0}, "AHEW": 110.0}}),
        ]
        mixture = [
            build_point("rec-1", {"experimentId": "exp-1", "tests": {"mixed_viscosity": {"25": 210.0}, "pot_life": {"25": 40.0}, "gel_time": {"25": 30.0}}}),
            build_point("rec-2", {"experimentId": "exp-2", "tests": {"mixed_viscosity": {"25": 640.0}, "pot_life": {"25": 18.0}, "gel_time": {"25": 12.0}}}),
        ]
        mechanics = [
            build_point("rec-1", {"experimentId": "exp-1", "tests": {"Tg": 132.0, "impact_resistance": 18.0}}),
            build_point("rec-2", {"experimentId": "exp-2", "tests": {"Tg": 98.0, "impact_resistance": 9.0}}),
        ]

        cls.raw_dir = cls.temp_root / "raw"
        cls.raw_dir.mkdir(parents=True, exist_ok=True)
        for collection, items in {
            "resin": resin,
            "hardener": hardener,
            "mixture": mixture,
            "mechanics": mechanics,
        }.items():
            (cls.raw_dir / f"{collection}.json").write_text(
                json.dumps(
                    {
                        "collection": collection,
                        "points_count": len(items),
                        "items": items,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

        merged_items = baseline_recipe_selection_lib.load_baseline_raw_records_from_export(cls.raw_dir)
        cls.merged_export_path = cls.temp_root / "merged_export.json"
        cls.merged_export_path.write_text(
            json.dumps(
                {
                    "env": "test",
                    "qdrant_url": "offline",
                    "exported_at": "2026-04-09T00:00:00Z",
                    "collections": ["resin", "hardener", "mixture", "mechanics"],
                    "items": [
                        {
                            "experimentId": record["id"],
                            "collections_present": ["resin", "hardener", "mixture", "mechanics"],
                            "source_record_counts": {"resin": 1, "hardener": 1, "mixture": 1, "mechanics": 1},
                            "source_point_ids": {"resin": [record["id"]], "hardener": [record["id"]], "mixture": [record["id"]], "mechanics": [record["id"]]},
                            "merged_payload": record["payload"],
                        }
                        for record in merged_items
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_dir.cleanup()

    def setUp(self) -> None:
        baseline_recipe_selection_lib._CACHE.clear()

    def run_tool(self, payload: dict, *, export_path: Path | None = None) -> dict:
        old_path = os.environ.get("BASELINE_RECIPE_EXPORT_PATH")
        try:
            os.environ["BASELINE_RECIPE_EXPORT_PATH"] = str(export_path or self.merged_export_path)
            return baseline_recipe_selection_lib.run_tool(payload, now_seconds=time.time())
        finally:
            if old_path is None:
                os.environ.pop("BASELINE_RECIPE_EXPORT_PATH", None)
            else:
                os.environ["BASELINE_RECIPE_EXPORT_PATH"] = old_path

    def test_schema_and_ranked_selection(self) -> None:
        schema = baseline_recipe_selection_lib.run_tool({"action": "schema"})
        self.assertEqual(schema["status"], "ok")
        self.assertIn("schema", schema["query_summary"])

        selected = self.run_tool(
            {
                "action": "select",
                "material_groups": {
                    "resin": {"required": ["EL-127"]},
                    "hardener": {"required": ["H-50"], "recommended": ["WG-24"]},
                },
                "test_constraints": {
                    "Tg": {"operator": "greater", "startValue": 120, "endValue": 120},
                    "混合粘度": {"temperature": 25, "operator": "less", "startValue": 0, "endValue": 250},
                },
                "hardener_system_type": "AMINE",
                "ranking_priority": [
                    {"index": 1, "ranking_priority": "Tg"},
                    {"index": 2, "ranking_priority": "混合粘度", "temperature": 25},
                ],
                "top_k": 5,
            }
        )
        self.assertEqual(selected["status"], "ok")
        self.assertEqual(selected["hardener_system_families"], ["胺类体系"])
        self.assertEqual(selected["baseline_ids"], ["exp-1"])
        self.assertEqual(selected["baseline_count"], 1)
        record = selected["records"][0]
        self.assertEqual(record["experiment_id"], "exp-1")
        self.assertIn("selection_score", record)
        self.assertIn("Tg", record["tests_mechanics"])
        self.assertIn("mixed_viscosity", record["tests_mixture"])
        self.assertIn("resin", record["material_groups"])
        self.assertIn("hardener", record["material_groups"])

    def test_raw_export_directory_is_supported(self) -> None:
        selected = self.run_tool(
            {
                "action": "select",
                "material_groups": {"hardener": {"required": ["H-50"]}},
                "top_k": 1,
            },
            export_path=self.raw_dir,
        )
        self.assertEqual(selected["status"], "ok")
        self.assertEqual(selected["baseline_ids"], ["exp-1"])

    def test_warning_and_error_paths(self) -> None:
        missing = self.run_tool({"action": "select"}, export_path=self.temp_root / "missing.json")
        self.assertEqual(missing["status"], "error")
        self.assertTrue(missing["errors"])

        invalid_top_k = self.run_tool({"action": "select", "top_k": 0})
        self.assertEqual(invalid_top_k["status"], "warning")
        self.assertEqual(invalid_top_k["records"], [])

        unmatched = self.run_tool({"action": "select", "hardener_system_type": "PREPREG", "top_k": 5})
        self.assertEqual(unmatched["status"], "warning")
        self.assertEqual(unmatched["records"], [])

        unknown_priority = self.run_tool(
            {
                "action": "select",
                "test_constraints": {"Tg": {"operator": "greater", "startValue": 120, "endValue": 120}},
                "ranking_priority": [{"index": 1, "ranking_priority": "未知测试项"}],
            }
        )
        self.assertEqual(unknown_priority["status"], "warning")
        self.assertTrue(any("Unknown ranking_priority" in warning for warning in unknown_priority["warnings"]))

    def test_wrapper_uses_local_export_file(self) -> None:
        wrapper = ROOT / "tools" / "baseline_recipe_selection"
        payload = {
            "action": "select",
            "material_groups": {"hardener": {"required": ["H-50"]}},
            "top_k": 1,
        }
        env = os.environ.copy()
        env["BASELINE_RECIPE_EXPORT_PATH"] = str(self.merged_export_path)
        completed = subprocess.run(
            [str(wrapper)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
            env=env,
        )
        decoded = json.loads(completed.stdout)
        self.assertEqual(decoded["status"], "ok")
        self.assertEqual(decoded["baseline_ids"], ["exp-1"])


if __name__ == "__main__":
    unittest.main()
