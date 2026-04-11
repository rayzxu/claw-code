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

import material_facts_lookup_lib


class MaterialFactsLookupTests(unittest.TestCase):
    def test_default_export_path_points_inside_plugin(self) -> None:
        self.assertEqual(
            material_facts_lookup_lib.default_material_export_path(),
            ROOT / "data" / "qdrant_exports" / "aws_test_rag_expert.json",
        )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.export_path = Path(self.tmp.name) / "expert_export.json"
        self._write_export()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _write_export(self) -> None:
        payload = {
            "collection": "expert_materials",
            "with_vectors": True,
            "items": [
                {
                    "id": "pt-1",
                    "payload": {
                        "id": "doc-1",
                        "material_codes": ["H-50"],
                        "text": "H-50 粘度 500 cP，胺值 466 mgKOH/g，适用于胺类体系固化剂场景。",
                    },
                    "vector": [1.0, 0.0, 0.0],
                },
                {
                    "id": "pt-2",
                    "payload": {
                        "id": "doc-2",
                        "material_codes": ["EL-127"],
                        "text": "EL-127 epoxy resin EEW 185 g/eq，密度 1.16 g/cm3。",
                    },
                    "vector": [0.0, 1.0, 0.0],
                },
                {
                    "id": "pt-3",
                    "payload": {
                        "id": "doc-3",
                        "material_codes": ["H-50", "EL-127"],
                        "text": "H-50 与 EL-127 的组合记录，涉及低温冲击和韧性描述。",
                    },
                    "vector": [0.7, 0.7, 0.0],
                },
            ],
        }
        self.export_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def run_tool(self, payload: dict) -> dict:
        return material_facts_lookup_lib.run_tool({"export_path": str(self.export_path), **payload})

    def test_schema_lookup_search_and_profile(self) -> None:
        schema = self.run_tool({"action": "schema"})
        self.assertEqual(schema["status"], "ok")
        self.assertEqual(schema["profile"]["dataset_count"], 3)
        self.assertEqual(schema["retrieval_mode"], "schema")

        lookup = self.run_tool({"action": "lookup", "materials": ["H-50"], "limit": 10})
        self.assertEqual(lookup["retrieval_mode"], "exact")
        self.assertEqual(len(lookup["evidence"]), 2)
        self.assertEqual(lookup["profile"]["evidence_count"], 2)

        search = self.run_tool({"action": "search", "query": "H-50 粘度", "mode": "lexical", "limit": 10})
        self.assertEqual(search["status"], "ok")
        self.assertEqual(search["retrieval_mode"], "lexical")
        self.assertEqual(search["evidence"][0]["material_codes"][0], "H-50")

        profile = self.run_tool({"action": "profile", "materials": ["EL-127"], "query": "EEW 密度", "mode": "lexical", "limit": 10})
        self.assertEqual(profile["retrieval_mode"], "lexical")
        self.assertGreaterEqual(profile["profile"]["property_counts"]["density"], 1)

    def test_structured_error_paths(self) -> None:
        missing = material_facts_lookup_lib.run_tool({"action": "schema", "export_path": str(self.export_path.parent / "missing.json")})
        self.assertEqual(missing["status"], "error")
        self.assertTrue(missing["errors"])

        empty_search = self.run_tool({"action": "search", "query": ""})
        self.assertEqual(empty_search["status"], "warning")
        self.assertEqual(empty_search["evidence"], [])

        unmatched = self.run_tool({"action": "lookup", "materials": ["UNKNOWN"]})
        self.assertEqual(unmatched["status"], "warning")
        self.assertTrue(unmatched["warnings"])

    def test_wrapper_emits_json(self) -> None:
        wrapper = ROOT / "tools" / "material_facts_lookup"
        payload = {"action": "lookup", "materials": ["H-50"], "export_path": str(self.export_path)}
        completed = subprocess.run(
            [str(wrapper)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            check=True,
        )
        decoded = json.loads(completed.stdout)
        self.assertEqual(decoded["status"], "ok")
        self.assertEqual(decoded["evidence"][0]["material_codes"][0], "H-50")


if __name__ == "__main__":
    unittest.main()
