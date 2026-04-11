from __future__ import annotations

import json
import math
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

QDRANT_COLLECTIONS = ("resin", "hardener", "mixture", "mechanics")
CACHE_TTL_SECONDS = float(os.environ.get("QDRANT_BASELINE_CACHE_TTL_SECONDS", "300"))
_CACHE: dict[str, dict[str, Any]] = {}

SYSTEM_TYPE_TO_FAMILY = {
    "AMINE": "胺类体系",
    "ANHYDRIDE": "酸酐类体系",
    "PREPREG": "预浸料体系",
    "SPECIAL_AMINE": "特殊胺类体系",
}

SYSTEM_FAMILY_VALUES = [
    "胺类体系",
    "酸酐类体系",
    "特殊胺类体系",
    "预浸料体系",
]

MECH_FIELD_MAPPING = {
    "泊松比": "poisson_ratio",
    "冲击韧性": "impact_resistance",
    "弯曲模量": "flexural_modulus",
    "弯曲强度": "flexural_strength",
    "断裂延伸率": "elongation",
    "拉伸模量": "tensile_modulus",
    "拉伸强度": "tensile_strength",
    "压缩强度": "compressive_strength",
    "压缩模量": "compressive_modulus",
    "Tg": "Tg",
    "升温放热峰": "exothermic_peak_heating",
    "恒温放热峰": "exothermic_peak_isothermal",
}


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def compact_text(value: Any, max_length: int = 240) -> str:
    text = clean_text(value)
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def source_env_status() -> list[dict[str, Any]]:
    return [
        {
            "env": "BASELINE_RECIPE_EXPORT_PATH",
            "configured": bool(clean_text(os.environ.get("BASELINE_RECIPE_EXPORT_PATH"))),
        },
        {
            "env": "GD_QDRANT_EXPORT_PATH",
            "configured": bool(clean_text(os.environ.get("GD_QDRANT_EXPORT_PATH"))),
        },
    ]


def as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := clean_text(item))]


def limit_from(value: Any, default: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(number, 100))


def normalize_token(value: Any) -> str:
    return re.sub(r"[\s_\-:/]+", "", clean_text(value)).lower()


def normalize_temperature_value(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def is_temperature_indexed(value: Any) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    if "startValue" in value or "endValue" in value or "operator" in value or "temperature" in value:
        return False
    return all(normalize_temperature_value(key) is not None for key in value.keys())


def parse_material_preferences(material_groups: Any) -> dict[str, dict[str, list[str]]]:
    base = material_groups if isinstance(material_groups, dict) else {}

    def parse_side(raw_side: Any) -> dict[str, list[str]]:
        out = {"required": [], "recommended": []}
        if raw_side is None:
            return out
        if isinstance(raw_side, list):
            out["required"] = as_string_list(raw_side)
            return out
        if not isinstance(raw_side, dict):
            return out
        if isinstance(raw_side.get("required"), list) or isinstance(raw_side.get("recommended"), list):
            out["required"] = as_string_list(raw_side.get("required"))
            out["recommended"] = as_string_list(raw_side.get("recommended"))
            return out
        group_type = clean_text(raw_side.get("type") or "required").lower()
        components = as_string_list(raw_side.get("component"))
        if group_type == "recommended":
            out["recommended"] = components
        else:
            out["required"] = components
        return out

    parsed = {
        "resin": parse_side(base.get("resin")),
        "hardener": parse_side(base.get("hardener")),
    }
    for side in ("resin", "hardener"):
        parsed[side]["required"] = dedupe_keep_order(parsed[side]["required"])
        parsed[side]["recommended"] = dedupe_keep_order(parsed[side]["recommended"])
    return parsed


def normalize_material_targets(material_groups: Any) -> dict[str, list[str]]:
    parsed = parse_material_preferences(material_groups)
    return {
        "resin": dedupe_keep_order(parsed["resin"]["required"] + parsed["resin"]["recommended"]),
        "hardener": dedupe_keep_order(parsed["hardener"]["required"] + parsed["hardener"]["recommended"]),
    }


def parse_hardener_families_from_expert_opinion(expert_opinion: str) -> list[str]:
    text = clean_text(expert_opinion)
    if not text:
        return []
    families: list[str] = []
    if re.search(r"预浸料", text):
        families.append("预浸料体系")
    if re.search(r"酸酐", text):
        families.append("酸酐类体系")
    if re.search(r"特殊胺|改性胺|潜伏性胺", text):
        families.append("特殊胺类体系")
    if re.search(r"胺类|胺体系|聚醚胺|芳香胺|聚酰胺|酚醛胺", text):
        families.append("胺类体系")
    return [family for family in dedupe_keep_order(families) if family in SYSTEM_FAMILY_VALUES]


def resolve_target_families(payload: dict[str, Any]) -> list[str]:
    hardener_system_type = clean_text(payload.get("hardener_system_type")).upper()
    if hardener_system_type in SYSTEM_TYPE_TO_FAMILY:
        return [SYSTEM_TYPE_TO_FAMILY[hardener_system_type]]
    return parse_hardener_families_from_expert_opinion(clean_text(payload.get("expert_opinion")))


def _as_path(value: Any) -> Path | None:
    text = clean_text(value)
    if not text:
        return None
    return Path(text).expanduser()


def plugin_root() -> Path:
    root = os.environ.get("CLAWD_PLUGIN_ROOT")
    if root:
        return Path(root).expanduser()
    return Path(__file__).resolve().parents[1]


def resolve_default_export_path() -> Path:
    default_path = plugin_root() / "data" / "qdrant_exports" / "aws_test_mixture_recipes.json"
    return Path(
        os.environ.get(
            "BASELINE_RECIPE_EXPORT_PATH",
            os.environ.get(
                "GD_QDRANT_EXPORT_PATH",
                str(default_path),
            ),
        )
    ).expanduser()


def merge_payloads_by_value_length(records: list[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for record in records:
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if key not in merged:
                merged[key] = value
                continue
            existing = merged[key]
            if isinstance(value, dict) and isinstance(existing, dict):
                has_resin_new = "resin" in value
                has_hardener_new = "hardener" in value
                has_resin_existing = "resin" in existing
                has_hardener_existing = "hardener" in existing
                if (
                    has_resin_new and not has_hardener_new and not has_resin_existing and has_hardener_existing
                ) or (
                    has_hardener_new and not has_resin_new and has_resin_existing and not has_hardener_existing
                ):
                    merged[key] = {**existing, **value}
                    continue
            len_new = len(value) if hasattr(value, "__len__") else 0
            len_existing = len(existing) if hasattr(existing, "__len__") else 0
            if len_new > len_existing:
                merged[key] = value
    return merged


def _collection_items_to_records(collection: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        record_id = clean_text(item.get("id"))
        payload = dict(item.get("payload") or {})
        if not record_id or not isinstance(payload, dict):
            continue
        if "tests" in payload:
            payload[f"tests_{collection}"] = payload["tests"]
            del payload["tests"]
        out.append({"id": record_id, "payload": payload})
    return out


def _merged_items_to_records(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        experiment_id = clean_text(item.get("experimentId"))
        payload = item.get("merged_payload")
        if not experiment_id or not isinstance(payload, dict):
            continue
        out.append({"id": experiment_id, "payload": dict(payload)})
    return out


def _load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_raw_collection_file(path: Path) -> tuple[str, list[dict[str, Any]]]:
    parsed = _load_json_file(path)
    if not isinstance(parsed, dict):
        raise ValueError(f"Raw collection export must be a JSON object: {path}")
    collection = clean_text(parsed.get("collection"))
    items = parsed.get("items")
    if collection not in QDRANT_COLLECTIONS or not isinstance(items, list):
        raise ValueError(f"Unsupported raw collection export shape: {path}")
    return collection, _collection_items_to_records(collection, items)


def _load_raw_collection_family_from(path: Path) -> dict[str, list[dict[str, Any]]]:
    base_dir = path if path.is_dir() else path.parent
    grouped: dict[str, list[dict[str, Any]]] = {}
    for collection in QDRANT_COLLECTIONS:
        candidate = base_dir / f"{collection}.json"
        if not candidate.is_file():
            continue
        loaded_collection, records = _load_raw_collection_file(candidate)
        grouped[loaded_collection] = records
    if grouped:
        return grouped
    loaded_collection, records = _load_raw_collection_file(path)
    return {loaded_collection: records}


def _merge_raw_collection_records(grouped_by_collection: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for collection in QDRANT_COLLECTIONS:
        records = grouped_by_collection.get(collection) or []
        for record in records:
            record_id = clean_text(record.get("id"))
            payload = dict(record.get("payload") or {})
            experiment_id = clean_text(payload.get("experimentId")) or record_id
            if not experiment_id:
                continue
            grouped[experiment_id].append({"id": experiment_id, "payload": payload})

    merged_records: list[dict[str, Any]] = []
    for record_id, record_list in grouped.items():
        if not record_id:
            continue
        merged_records.append({"id": record_id, "payload": merge_payloads_by_value_length(record_list)})
    return merged_records


def load_baseline_raw_records_from_export(export_path: Path) -> list[dict[str, Any]]:
    if export_path.is_dir():
        grouped = _load_raw_collection_family_from(export_path)
        return _merge_raw_collection_records(grouped)
    parsed = _load_json_file(export_path)
    if isinstance(parsed, dict):
        items = parsed.get("items")
        if isinstance(items, list) and items and isinstance(items[0], dict) and "merged_payload" in items[0]:
            return _merged_items_to_records(items)
        if isinstance(items, list) and items and isinstance(items[0], dict) and "payload" in items[0]:
            collection = clean_text(parsed.get("collection"))
            if collection in QDRANT_COLLECTIONS:
                grouped = _load_raw_collection_family_from(export_path)
                return _merge_raw_collection_records(grouped)
    raise ValueError(f"Unsupported baseline recipe export format: {export_path}")


def get_cached_baseline_raw_records(
    export_path: Path,
    *,
    now_seconds: float | None = None,
    loader: Any = None,
) -> list[dict[str, Any]]:
    now = time.time() if now_seconds is None else float(now_seconds)
    cache_key = json.dumps({"export_path": str(export_path.resolve())}, sort_keys=True)
    cached = _CACHE.get(cache_key)
    mtime = export_path.stat().st_mtime if export_path.exists() else None
    if cached and cached.get("mtime") == mtime and (now - float(cached.get("ts") or 0.0)) <= CACHE_TTL_SECONDS:
        records = cached.get("records")
        return records if isinstance(records, list) else []
    fetch = loader or load_baseline_raw_records_from_export
    records = fetch(export_path)
    _CACHE[cache_key] = {"ts": now, "mtime": mtime, "records": records}
    return records if isinstance(records, list) else []


def get_payload(item: dict[str, Any]) -> dict[str, Any]:
    payload = item.get("payload")
    return payload if isinstance(payload, dict) else {}


def extract_record_components(item: dict[str, Any]) -> dict[str, dict[str, float]]:
    payload = get_payload(item)
    material_groups = payload.get("materialGroups") or payload.get("materialGroup") or {}
    if not isinstance(material_groups, dict):
        return {"resin": {}, "hardener": {}}

    def side_components(key: str, zh_key: str) -> dict[str, float]:
        side = material_groups.get(key)
        if isinstance(side, dict):
            components = side.get("components")
            if isinstance(components, dict):
                out: dict[str, float] = {}
                for name, value in components.items():
                    try:
                        out[str(name)] = float(value)
                    except Exception:
                        continue
                return out
        raw = material_groups.get(zh_key)
        if isinstance(raw, dict):
            out = {}
            for name, value in raw.items():
                try:
                    out[str(name)] = float(value)
                except Exception:
                    continue
            return out
        return {}

    return {
        "resin": side_components("resin", "树脂配方"),
        "hardener": side_components("hardener", "固化剂配方"),
    }


def extract_record_families(item: dict[str, Any]) -> list[str]:
    payload = get_payload(item)
    families: list[str] = []
    family = clean_text(payload.get("hardener_system_family"))
    if family:
        families.append(family)
    family_all = payload.get("hardener_system_family_all")
    if isinstance(family_all, list):
        families.extend([clean_text(value) for value in family_all if clean_text(value)])
    return [family for family in dedupe_keep_order(families) if family in SYSTEM_FAMILY_VALUES]


def filter_records_by_families(records: list[dict[str, Any]], families: list[str]) -> list[dict[str, Any]]:
    if not families:
        return list(records)
    target = set(families)
    return [record for record in records if set(extract_record_families(record)) & target]


def extract_candidate_value(item: dict[str, Any], test_name: str) -> Any:
    if test_name in item:
        return item.get(test_name)
    payload = get_payload(item)
    tests_resin = payload.get("tests_resin") if isinstance(payload.get("tests_resin"), dict) else {}
    tests_hardener = payload.get("tests_hardener") if isinstance(payload.get("tests_hardener"), dict) else {}
    tests_mixture = payload.get("tests_mixture") if isinstance(payload.get("tests_mixture"), dict) else {}
    tests_mechanics = payload.get("tests_mechanics") if isinstance(payload.get("tests_mechanics"), dict) else {}

    if test_name == "环氧当量":
        return tests_resin.get("EEW")
    if test_name == "胺当量":
        return tests_hardener.get("AHEW")
    if test_name == "树脂粘度":
        return tests_resin.get("resin_viscosity")
    if test_name == "树脂密度":
        return tests_resin.get("resin_density")
    if test_name == "固化剂粘度":
        return tests_hardener.get("hardener_viscosity")
    if test_name == "固化剂密度":
        return tests_hardener.get("hardener_density")
    if test_name == "混合粘度":
        return tests_mixture.get("mixed_viscosity")
    if test_name == "可用时间":
        return tests_mixture.get("pot_life")
    if test_name == "凝胶时间":
        return tests_mixture.get("gel_time")

    mech_field = MECH_FIELD_MAPPING.get(test_name)
    if mech_field:
        return tests_mechanics.get(mech_field)
    return None


def as_numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except Exception:
            return None
    return None


def extract_base_value(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        start = value.get("startValue")
        end = value.get("endValue")
        if start is not None and end is not None:
            return (float(start) + float(end)) / 2.0
        if start is not None:
            return float(start)
        if end is not None:
            return float(end)
    return None


def temperature_weight(delta_temp: int) -> float:
    if delta_temp == 0:
        return 0.6
    if delta_temp == 1:
        return 0.3
    return 0.1


def compute_test_similarity(test_result: dict[str, Any], candidate_result: dict[str, Any]) -> float:
    total_score = 0.0
    total_items = 0
    for test_name, input_value in (test_result or {}).items():
        candidate_value = candidate_result.get(test_name)
        if isinstance(candidate_value, dict):
            temp_raw = (input_value or {}).get("temperature") if isinstance(input_value, dict) else None
            try:
                input_temp = int(float(temp_raw)) if temp_raw is not None else 0
            except Exception:
                input_temp = 0
            input_base_value = extract_base_value(input_value)
            if input_base_value is None:
                continue
            weighted_score = 0.0
            for candidate_temp_raw, candidate_temp_value in candidate_value.items():
                try:
                    candidate_temp = int(float(candidate_temp_raw))
                except (TypeError, ValueError):
                    continue
                candidate_numeric = as_numeric(candidate_temp_value)
                if candidate_numeric is None:
                    continue
                delta_temp = abs(candidate_temp - input_temp)
                weight = temperature_weight(delta_temp)
                relative_diff = abs(candidate_numeric - input_base_value) / (abs(input_base_value) + 1e-8)
                score = (1.0 - min(relative_diff, 1.0)) * 100.0
                weighted_score += score * weight
            total_score += min(weighted_score, 100.0)
            total_items += 1
        else:
            input_base_value = extract_base_value(input_value)
            candidate_numeric = as_numeric(candidate_value)
            if input_base_value is None or candidate_numeric is None:
                continue
            relative_diff = abs(candidate_numeric - input_base_value) / (abs(input_base_value) + 1e-8)
            total_score += (1.0 - min(relative_diff, 1.0)) * 100.0
            total_items += 1
    final_score = (total_score / total_items) * 0.2 if total_items > 0 else 0.0
    return round(final_score, 4)


def build_candidate_test_result(item: dict[str, Any], test_constraints: dict[str, Any]) -> dict[str, Any]:
    return {str(test_name): extract_candidate_value(item, str(test_name)) for test_name in (test_constraints or {})}


def target_base_value(spec: Any) -> float | None:
    return extract_base_value(spec)


def candidate_base_value(spec: Any, candidate_value: Any) -> float | None:
    numeric = as_numeric(candidate_value)
    if numeric is not None:
        return numeric
    if isinstance(candidate_value, dict):
        target_temp = None
        if isinstance(spec, dict) and "temperature" in spec:
            try:
                target_temp = float(spec.get("temperature"))
            except Exception:
                target_temp = None
        best_delta = None
        best_value = None
        for temp_key, temp_value in candidate_value.items():
            try:
                temp = float(temp_key)
            except Exception:
                continue
            numeric_value = as_numeric(temp_value)
            if numeric_value is None:
                continue
            delta = abs(temp - target_temp) if target_temp is not None else 0.0
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_value = numeric_value
        return best_value
    return None


def data_count(item: dict[str, Any], test_constraints: dict[str, Any]) -> int:
    count = 0
    for test_name in (test_constraints or {}).keys():
        candidate = extract_candidate_value(item, str(test_name))
        if as_numeric(candidate) is not None:
            count += 1
        elif isinstance(candidate, dict) and any(as_numeric(value) is not None for value in candidate.values()):
            count += 1
    return count


def vector_distance(item: dict[str, Any], test_constraints: dict[str, Any]) -> float:
    if not test_constraints:
        return float("inf")
    squared_sum = 0.0
    dim_count = 0
    for test_name, spec in (test_constraints or {}).items():
        target = target_base_value(spec)
        if target is None:
            continue
        candidate = candidate_base_value(spec, extract_candidate_value(item, str(test_name)))
        if candidate is None:
            continue
        squared_sum += (candidate - target) ** 2
        dim_count += 1
    if dim_count == 0:
        return float("inf")
    return math.sqrt(squared_sum)


def side_score(candidate_names: set[str], selected_names: set[str]) -> float:
    if not selected_names:
        return 1.0
    denom = max(len(selected_names), 1)
    coverage = len(candidate_names & selected_names) / denom
    extra_penalty = len(candidate_names - selected_names) / denom
    return max(0.0, coverage - 0.5 * extra_penalty)


def material_similarity_score(item: dict[str, Any], material_groups: dict[str, Any]) -> float:
    prefs = parse_material_preferences(material_groups)
    resin_required = set(map(str, prefs.get("resin", {}).get("required") or []))
    resin_recommended = set(map(str, prefs.get("resin", {}).get("recommended") or []))
    hardener_required = set(map(str, prefs.get("hardener", {}).get("required") or []))
    hardener_recommended = set(map(str, prefs.get("hardener", {}).get("recommended") or []))
    if not (resin_required or resin_recommended or hardener_required or hardener_recommended):
        return 0.0

    components = extract_record_components(item)
    resin_names = set(map(str, components.get("resin", {}).keys()))
    hardener_names = set(map(str, components.get("hardener", {}).keys()))

    def required_coverage(candidate: set[str], required: set[str]) -> float:
        if not required:
            return 1.0
        return len(candidate & required) / max(len(required), 1)

    resin_req_cov = required_coverage(resin_names, resin_required)
    hard_req_cov = required_coverage(hardener_names, hardener_required)
    resin_rec_score = side_score(resin_names, resin_recommended) if resin_recommended else 0.0
    hard_rec_score = side_score(hardener_names, hardener_recommended) if hardener_recommended else 0.0

    resin_constrained = bool(resin_required or resin_recommended)
    hardener_constrained = bool(hardener_required or hardener_recommended)

    resin_score = (
        0.85 * resin_req_cov + 0.15 * max(0.0, resin_rec_score)
        if resin_required else max(0.0, resin_rec_score)
    )
    hardener_score = (
        0.85 * hard_req_cov + 0.15 * max(0.0, hard_rec_score)
        if hardener_required else max(0.0, hard_rec_score)
    )

    if resin_constrained and hardener_constrained:
        raw = 1.0 * resin_score + 2.0 * hardener_score
        scale = 3.0
    elif resin_constrained:
        raw = resin_score
        scale = 1.0
    elif hardener_constrained:
        raw = hardener_score
        scale = 1.0
    else:
        return 0.0

    all_required_hit = (
        (not resin_required or resin_required.issubset(resin_names))
        and (not hardener_required or hardener_required.issubset(hardener_names))
    )
    if not all_required_hit:
        raw *= 0.2

    return max(0.0, min(1.0, raw / max(scale, 1e-8)))


def test_similarity_score(item: dict[str, Any], test_constraints: dict[str, Any]) -> float:
    if not isinstance(test_constraints, dict) or not test_constraints:
        return 0.0
    candidate_test_result = build_candidate_test_result(item, test_constraints)
    try:
        raw_score = float(compute_test_similarity(test_constraints, candidate_test_result))
        return max(0.0, min(1.0, raw_score / 20.0))
    except Exception:
        return 0.0


def single_test_similarity_score(item: dict[str, Any], test_name: str, test_spec: Any) -> float:
    candidate_value = extract_candidate_value(item, str(test_name))
    target = target_base_value(test_spec)
    candidate = candidate_base_value(test_spec, candidate_value)
    if target is None or candidate is None:
        return 0.0
    relative_diff = abs(candidate - target) / (abs(target) + 1e-8)
    return max(0.0, min(1.0, 1.0 - min(relative_diff, 1.0)))


def resolve_single_priority_spec(raw_spec: Any, temperature: float | None) -> Any:
    if isinstance(raw_spec, dict) and is_temperature_indexed(raw_spec):
        if not raw_spec:
            return {}
        chosen_key = None
        if temperature is not None:
            best_delta = None
            for key in raw_spec.keys():
                key_temp = normalize_temperature_value(key)
                if key_temp is None:
                    continue
                delta = abs(key_temp - temperature)
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    chosen_key = str(key)
        if chosen_key is None:
            try:
                chosen_key = min(raw_spec.keys(), key=lambda value: float(value))
            except Exception:
                chosen_key = str(next(iter(raw_spec.keys())))
        chosen = raw_spec.get(chosen_key, {})
        if isinstance(chosen, dict):
            spec = dict(chosen)
            if temperature is not None:
                spec["temperature"] = temperature
            else:
                chosen_temp = normalize_temperature_value(chosen_key)
                if chosen_temp is not None:
                    spec["temperature"] = chosen_temp
            return spec
        return chosen
    if isinstance(raw_spec, dict):
        spec = dict(raw_spec)
        if temperature is not None:
            spec["temperature"] = temperature
        return spec
    return raw_spec


def priority_label(test_name: str, temperature: float | None) -> str:
    if temperature is None:
        return test_name
    if float(temperature).is_integer():
        return f"{test_name}@{int(temperature)}"
    return f"{test_name}@{temperature}"


def resolve_priority_test_specs(
    test_constraints: dict[str, Any],
    ranking_priority: list[Any] | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(test_constraints, dict) or not test_constraints:
        return [], []
    if not isinstance(ranking_priority, list) or not ranking_priority:
        return [], []
    available = [str(key) for key in test_constraints.keys()]
    resolved_items: list[dict[str, Any]] = []
    warnings: list[str] = []
    seen_names = set()

    for position, item in enumerate(ranking_priority):
        if not isinstance(item, dict):
            warnings.append(f"ranking_priority[{position}] is not an object and was ignored.")
            continue
        key = clean_text(item.get("ranking_priority"))
        if not key:
            warnings.append(f"ranking_priority[{position}] is missing ranking_priority.")
            continue
        if key not in test_constraints:
            warnings.append(f"Unknown ranking_priority test name ignored: {key}")
            continue
        seen_names.add(key)
        try:
            order = int(item.get("index"))
        except Exception:
            order = position + 1
        temperature = normalize_temperature_value(item.get("temperature"))
        spec = resolve_single_priority_spec(test_constraints.get(key), temperature)
        resolved_items.append(
            {
                "index": order,
                "position": position,
                "test_name": key,
                "spec": spec,
                "label": priority_label(key, temperature),
            }
        )

    tail_index = max([int(item.get("index", 0) or 0) for item in resolved_items], default=0)
    tail_position = len(resolved_items)
    for key in available:
        if key in seen_names:
            continue
        spec = resolve_single_priority_spec(test_constraints.get(key), None)
        tail_index += 1
        resolved_items.append(
            {
                "index": tail_index,
                "position": tail_position,
                "test_name": key,
                "spec": spec,
                "label": priority_label(key, None),
            }
        )
        tail_position += 1

    resolved_items.sort(key=lambda item: (int(item["index"]), int(item["position"])))
    return resolved_items, warnings


def rank_baseline_records(
    records: list[dict[str, Any]],
    material_groups: dict[str, Any] | None,
    test_constraints: dict[str, Any] | None,
    ranking_priority: list[Any] | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not isinstance(records, list) or not records:
        return [], []
    test_constraints = test_constraints if isinstance(test_constraints, dict) else {}
    material_groups = material_groups if isinstance(material_groups, dict) else {}

    has_materials = bool(
        parse_material_preferences(material_groups).get("resin", {}).get("required")
        or parse_material_preferences(material_groups).get("resin", {}).get("recommended")
        or parse_material_preferences(material_groups).get("hardener", {}).get("required")
        or parse_material_preferences(material_groups).get("hardener", {}).get("recommended")
    )
    has_tests = bool(test_constraints)

    priority_specs, warnings = resolve_priority_test_specs(test_constraints, ranking_priority) if has_tests else ([], [])
    scored: list[tuple[tuple[float, ...], float, int, int, float, dict[str, Any]]] = []
    for index, item in enumerate(records):
        material_norm = material_similarity_score(item, material_groups) if has_materials else 0.0
        test_norm = test_similarity_score(item, test_constraints) if has_tests else 0.0
        if has_materials and has_tests:
            final_score = 0.7 * material_norm + 0.3 * test_norm
        elif has_materials:
            final_score = material_norm
        elif has_tests:
            final_score = test_norm
        else:
            final_score = 0.0
        available_count = data_count(item, test_constraints) if has_tests else 0
        distance = vector_distance(item, test_constraints) if has_tests else float("inf")
        priority_vector: tuple[float, ...] = ()
        if priority_specs:
            priority_vector = tuple(
                single_test_similarity_score(item, str(spec.get("test_name")), spec.get("spec"))
                for spec in priority_specs
            )
        scored.append((priority_vector, final_score, available_count, index, distance, item))

    if priority_specs:
        scored.sort(
            key=lambda row: (
                tuple(-value for value in row[0]),
                -row[1],
                -row[2],
                row[4],
                row[3],
            )
        )
    else:
        scored.sort(key=lambda row: (-row[1], -row[2], row[4], row[3]))

    ranked: list[dict[str, Any]] = []
    for priority_vector, score, _, _, _, item in scored:
        current = dict(item)
        current["selection_score"] = round(score, 4)
        current["priority_scores"] = {}
        if priority_specs:
            current["priority_scores"] = {
                str(spec.get("label")): round(priority_vector[idx], 4)
                for idx, spec in enumerate(priority_specs)
            }
        ranked.append(current)
    return ranked, warnings


def extract_condition_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx in range(1, 11):
        temp = payload.get(f"curing_condition_temp{idx}")
        if temp is None:
            temp = payload.get(f"conditionTemp{idx}")
        duration = payload.get(f"curing_condition_time{idx}")
        if duration is None:
            duration = payload.get(f"conditionTime{idx}")
        if temp is None or duration is None:
            continue
        out.append({"index": idx, "condition_temp": temp, "condition_time": duration})
    return out


def normalize_material_group(side: str, group: Any, mix_ratio: Any) -> dict[str, Any] | None:
    if not isinstance(group, dict):
        return None
    components = group.get("components")
    if not isinstance(components, dict) or not components:
        return None
    proportion = group.get("proportion")
    if proportion is None:
        if side == "resin":
            proportion = 100.0
        elif isinstance(mix_ratio, str) and ":" in mix_ratio:
            try:
                _, rhs = mix_ratio.split(":", 1)
                proportion = float(rhs.strip())
            except Exception:
                proportion = 0.0
        else:
            proportion = 0.0
    return {
        "group_name": group.get("groupName") or f"{side}_history",
        "proportion": proportion,
        "components": components,
    }


def normalize_record_output(item: dict[str, Any]) -> dict[str, Any]:
    payload = get_payload(item)
    material_groups = payload.get("materialGroups") or payload.get("materialGroup") or {}
    resin_group = normalize_material_group("resin", material_groups.get("resin"), payload.get("mix_ratio"))
    hardener_group = normalize_material_group("hardener", material_groups.get("hardener"), payload.get("mix_ratio"))
    normalized_groups = {}
    if resin_group:
        normalized_groups["resin"] = resin_group
    if hardener_group:
        normalized_groups["hardener"] = hardener_group
    return {
        "id": clean_text(item.get("id")),
        "experiment_id": clean_text(payload.get("experimentId") or item.get("id")),
        "selection_score": item.get("selection_score", 0.0),
        "priority_scores": item.get("priority_scores") if isinstance(item.get("priority_scores"), dict) else {},
        "mix_ratio": payload.get("mix_ratio"),
        "hardener_system_family": payload.get("hardener_system_family"),
        "hc_subtype_primary": payload.get("hc_subtype_primary"),
        "department": payload.get("department"),
        "material_groups": normalized_groups,
        "condition_data": extract_condition_data(payload),
        "tests": payload.get("tests") if isinstance(payload.get("tests"), dict) else {},
        "tests_resin": payload.get("tests_resin") if isinstance(payload.get("tests_resin"), dict) else {},
        "tests_hardener": payload.get("tests_hardener") if isinstance(payload.get("tests_hardener"), dict) else {},
        "tests_mixture": payload.get("tests_mixture") if isinstance(payload.get("tests_mixture"), dict) else {},
        "tests_mechanics": payload.get("tests_mechanics") if isinstance(payload.get("tests_mechanics"), dict) else {},
    }


def base_response(action: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "action": action,
        "source_files": source_env_status(),
        "query_summary": {"action": action},
        "hardener_system_families": [],
        "baseline_ids": [],
        "baseline_count": 0,
        "records": [],
        "warnings": [],
        "errors": [],
    }


def apply_status(response: dict[str, Any]) -> dict[str, Any]:
    if response["errors"]:
        response["status"] = "error"
    elif response["warnings"]:
        response["status"] = "warning"
    else:
        response["status"] = "ok"
    return response


def schema_payload() -> dict[str, Any]:
    return {
        "supported_actions": ["schema", "select"],
        "data_source": "local_export_json",
        "optional_environment": ["BASELINE_RECIPE_EXPORT_PATH", "GD_QDRANT_EXPORT_PATH"],
        "supported_export_formats": ["merged_export_file", "raw_collection_directory", "raw_collection_file"],
        "collections": list(QDRANT_COLLECTIONS),
        "hardener_system_types": sorted(SYSTEM_TYPE_TO_FAMILY.keys()),
        "hardener_system_families": SYSTEM_FAMILY_VALUES,
        "supported_test_names": [
            "环氧当量",
            "树脂粘度",
            "树脂密度",
            "胺当量",
            "固化剂粘度",
            "固化剂密度",
            "混合粘度",
            "可用时间",
            "凝胶时间",
            *sorted(MECH_FIELD_MAPPING.keys()),
        ],
    }


def run_tool(
    payload: dict[str, Any],
    *,
    loader: Any = None,
    now_seconds: float | None = None,
) -> dict[str, Any]:
    action = clean_text(payload.get("action") or "schema").lower()
    response = base_response(action)
    response["query_summary"].update(
        {
            "material_groups": parse_material_preferences(payload.get("material_groups")),
            "test_constraint_names": sorted(list((payload.get("test_constraints") or {}).keys()))
            if isinstance(payload.get("test_constraints"), dict) else [],
            "hardener_system_type": clean_text(payload.get("hardener_system_type")).upper(),
            "expert_opinion_excerpt": compact_text(payload.get("expert_opinion")),
            "top_k": limit_from(payload.get("top_k"), 5),
            "ranking_priority": payload.get("ranking_priority") if isinstance(payload.get("ranking_priority"), list) else [],
        }
    )

    if action == "schema":
        response["query_summary"]["schema"] = schema_payload()
        return response

    if action != "select":
        response["errors"].append(f"Unsupported action: {action}")
        return apply_status(response)

    input_export_path = _as_path(payload.get("export_path"))
    default_export_path = resolve_default_export_path()
    export_path = input_export_path or default_export_path
    env_export_path = _as_path(os.environ.get("BASELINE_RECIPE_EXPORT_PATH")) or _as_path(
        os.environ.get("GD_QDRANT_EXPORT_PATH")
    )
    response["query_summary"]["export_path"] = str(export_path)
    response["source_files"] = [
        {
            "path": str(export_path),
            "exists": export_path.exists(),
            "source": "input" if input_export_path else ("env" if env_export_path else "default"),
        },
        *source_env_status(),
    ]
    if not export_path.exists():
        response["errors"].append(f"Baseline recipe export file not found: {export_path}")
        return apply_status(response)

    top_k = int(response["query_summary"]["top_k"])
    if top_k <= 0:
        response["warnings"].append("top_k must be greater than 0; returning no ranked records.")
        return apply_status(response)

    target_families = resolve_target_families(payload)
    response["hardener_system_families"] = target_families

    try:
        raw_records = get_cached_baseline_raw_records(
            export_path,
            now_seconds=now_seconds,
            loader=loader,
        )
    except Exception as exc:
        response["errors"].append(f"{type(exc).__name__}: {exc}")
        return apply_status(response)

    filtered_records = filter_records_by_families(raw_records, target_families)
    ranked_records, ranking_warnings = rank_baseline_records(
        filtered_records,
        payload.get("material_groups"),
        payload.get("test_constraints") if isinstance(payload.get("test_constraints"), dict) else {},
        payload.get("ranking_priority") if isinstance(payload.get("ranking_priority"), list) else [],
    )
    selected_records = ranked_records[:top_k]
    response["baseline_ids"] = [clean_text(record.get("id")) for record in selected_records if clean_text(record.get("id"))]
    response["baseline_count"] = len(selected_records)
    response["records"] = [normalize_record_output(record) for record in selected_records]
    response["warnings"].extend(ranking_warnings)

    if not filtered_records:
        response["warnings"].append("No baseline candidates matched the requested hardener system filter.")
    elif not selected_records:
        response["warnings"].append("No ranked baseline records were produced from the candidate pool.")

    return apply_status(response)
