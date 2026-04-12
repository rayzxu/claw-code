#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from typing import Any

import prepreg_b_ratio_guard_lib


def _read_payload() -> tuple[dict[str, Any], list[str]]:
    raw = sys.stdin.read().strip()
    if not raw:
        raw = os.environ.get("CLAWD_TOOL_INPUT", "").strip()
    if not raw:
        return {}, []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {}, [f"JSONDecodeError: {exc}"]
    if not isinstance(payload, dict):
        return {}, ["Tool input must be a JSON object."]
    return payload, []


def main() -> int:
    if prepreg_b_ratio_guard_lib.dependency_error() is not None:
        print(prepreg_b_ratio_guard_lib.dependency_error(), file=sys.stderr)
        return 2

    payload, input_errors = _read_payload()
    if input_errors:
        result = prepreg_b_ratio_guard_lib.base_response("unknown")
        result["status"] = "error"
        result["errors"] = input_errors
    else:
        result = prepreg_b_ratio_guard_lib.run_tool(payload)

    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
