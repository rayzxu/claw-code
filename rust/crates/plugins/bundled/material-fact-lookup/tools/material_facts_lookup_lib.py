from __future__ import annotations

import json
import math
import os
import re
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

def plugin_root() -> Path:
    root = os.environ.get("CLAWD_PLUGIN_ROOT")
    if root:
        return Path(root).expanduser()
    return Path(__file__).resolve().parents[1]


def default_material_export_path() -> Path:
    return plugin_root() / "data" / "qdrant_exports" / "aws_test_rag_expert.json"

DEFAULT_MATERIAL_EXPORT_PATH = Path(
    os.environ.get(
        "GD_MATERIAL_EXPORT_PATH",
        str(default_material_export_path()),
    )
).expanduser()

PROPERTY_PATTERNS: dict[str, re.Pattern[str]] = {
    "viscosity": re.compile(r"粘度|黏度|viscosity|mPa.?s|Pa.?s|cps|cp\b", re.IGNORECASE),
    "tg": re.compile(r"\bTg\b|玻璃化转变温度", re.IGNORECASE),
    "epoxy_value": re.compile(r"环氧值|环氧当量|EEW|epoxy", re.IGNORECASE),
    "amine_value": re.compile(r"胺值|AHEW|活泼氢当量|amine", re.IGNORECASE),
    "density": re.compile(r"密度|density|g/cm3|g/ml", re.IGNORECASE),
    "tensile": re.compile(r"拉伸|tensile", re.IGNORECASE),
    "flexural": re.compile(r"弯曲|flexural", re.IGNORECASE),
    "impact": re.compile(r"冲击|impact", re.IGNORECASE),
    "anhydride": re.compile(r"酸酐|anhydride", re.IGNORECASE),
    "functionality": re.compile(r"官能度|functionality", re.IGNORECASE),
}
TOKEN_RE = re.compile(r"[\u4e00-\u9fffA-Za-z0-9.\-_/]+")


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def compact_text(value: Any, max_length: int = 360) -> str:
    text = clean_text(value)
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + "..."


def tokenize_text(value: Any) -> list[str]:
    if value is None:
        return []
    return [token.casefold() for token in TOKEN_RE.findall(str(value))]


def normalize_material_code(value: Any) -> str:
    return clean_text(value).casefold()


def dedupe_preserve_order(values: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    out: list[Any] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def source_file_info(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def load_qdrant_export(path: Path) -> dict[str, Any]:
    raw = load_json_file(path)
    if not isinstance(raw, dict):
        raise ValueError(f"Qdrant export must be a JSON object: {path}")
    items = raw.get("items")
    if not isinstance(items, list):
        raise ValueError(f"Qdrant export 'items' must be a list: {path}")
    export = dict(raw)
    export["items"] = items
    return export


def qdrant_export_metadata(export: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {"item_count": len(export.get("items") or [])}
    for key in (
        "collection",
        "collections",
        "points_count",
        "vectors_count",
        "with_vectors",
        "env",
        "qdrant_url",
        "exported_at",
    ):
        if key in export:
            metadata[key] = export[key]
    return metadata


def qdrant_source_file_info(path: Path, export: dict[str, Any]) -> dict[str, Any]:
    info = source_file_info(path)
    info["qdrant_export"] = qdrant_export_metadata(export)
    return info


def pick_qdrant_payload(item: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    for key in ("payload", "merged_payload"):
        candidate = item.get(key)
        if isinstance(candidate, dict) and candidate:
            return candidate, key
    for key in ("payload", "merged_payload"):
        candidate = item.get(key)
        if isinstance(candidate, dict):
            return candidate, key
    return {}, None


def normalize_qdrant_item(item: dict[str, Any]) -> dict[str, Any]:
    payload, payload_key = pick_qdrant_payload(item)
    item_id = ""
    for container in (item, payload):
        for key in ("id", "experimentId"):
            value = container.get(key)
            text = clean_text(value)
            if text:
                item_id = text
                break
        if item_id:
            break
    return {
        "id": item_id,
        "payload": payload,
        "payload_key": payload_key,
        "vector": item.get("vector"),
    }


def iter_qdrant_items(export: dict[str, Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for raw_item in export.get("items") or []:
        if isinstance(raw_item, dict):
            normalized.append(normalize_qdrant_item(raw_item))
    return normalized


def normalize_vector(raw: Any) -> list[float]:
    if not isinstance(raw, list) or not raw:
        return []
    values = [float(value) for value in raw]
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        return []
    return [value / norm for value in values]


def resolve_export_path(payload: dict[str, Any]) -> Path:
    explicit = clean_text(payload.get("export_path"))
    return Path(explicit) if explicit else DEFAULT_MATERIAL_EXPORT_PATH


@lru_cache(maxsize=4)
def load_material_store_cached(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    export = load_qdrant_export(path)
    items = iter_qdrant_items(export)
    entries: list[dict[str, Any]] = []
    code_index: dict[str, list[int]] = defaultdict(list)
    payload_sources: Counter[str] = Counter()

    for item in items:
        payload = item.get("payload") or {}
        payload_key = str(item.get("payload_key") or "unknown")
        payload_sources[payload_key] += 1
        codes = dedupe_preserve_order(
            [clean_text(code) for code in (payload.get("material_codes") or []) if clean_text(code)]
        )
        normalized_codes = dedupe_preserve_order(
            [normalize_material_code(code) for code in codes if normalize_material_code(code)]
        )
        entry = {
            "point_id": clean_text(item.get("id")),
            "id": clean_text(payload.get("id") or item.get("id")),
            "material_codes": codes,
            "material_code_tokens": normalized_codes,
            "text": clean_text(payload.get("text")),
            "payload_source": payload_key,
            "vector": normalize_vector(item.get("vector")),
        }
        entry_index = len(entries)
        entries.append(entry)
        for token in normalized_codes:
            code_index[token].append(entry_index)

    vector_dim = 0
    for entry in entries:
        if entry["vector"]:
            vector_dim = len(entry["vector"])
            break

    return {
        "source_files": [qdrant_source_file_info(path, export)],
        "export_metadata": {"payload_sources": dict(payload_sources)},
        "dataset_count": len(entries),
        "entries": entries,
        "code_index": {key: value for key, value in code_index.items()},
        "vector_dim": vector_dim,
    }


def load_material_store(payload: dict[str, Any]) -> dict[str, Any]:
    return load_material_store_cached(str(resolve_export_path(payload)))


def build_query_embedding(query: str) -> tuple[list[float] | None, list[str]]:
    warnings: list[str] = []
    query = clean_text(query)
    if not query:
        warnings.append("Empty query cannot be embedded.")
        return None, warnings
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except Exception as exc:
        warnings.append(f"Semantic embedding unavailable; downgraded to lexical retrieval: {type(exc).__name__}: {exc}")
        return None, warnings
    try:
        model = SentenceTransformer(os.environ.get("GD_EMBED_MODEL_NAME", "BAAI/bge-m3"))
        vector = model.encode([query], normalize_embeddings=True, convert_to_numpy=False)[0]
    except Exception as exc:
        warnings.append(f"Semantic embedding unavailable; downgraded to lexical retrieval: {type(exc).__name__}: {exc}")
        return None, warnings
    values = [float(value) for value in vector]
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        warnings.append("Embedding query vector is zero; downgraded to lexical retrieval.")
        return None, warnings
    return [value / norm for value in values], warnings


def dot_product(left: list[float], right: list[float]) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    return sum(a * b for a, b in zip(left, right))


def lexical_score_for_entry(entry: dict[str, Any], query: str, query_tokens: list[str]) -> float:
    text_cf = entry["text"].casefold()
    full_query = clean_text(query).casefold()
    score = 0.0
    if full_query and full_query in text_cf:
        score += max(1.0, 0.75 * max(len(query_tokens), 1))
    for token in query_tokens:
        if token in entry.get("material_code_tokens", []):
            score += 2.0
        elif token and token in text_cf:
            score += 1.0
    return score / max(len(query_tokens), 1)


def material_schema(payload: dict[str, Any]) -> dict[str, Any]:
    store = load_material_store(payload)
    return {
        "dataset_count": store["dataset_count"],
        "vector_enabled": bool(store["vector_dim"]),
        "vector_dim": store["vector_dim"],
        "supported_retrieval_modes": ["hybrid", "semantic", "lexical"],
        "property_dimensions": list(PROPERTY_PATTERNS.keys()),
    }


def resolve_material_candidate_indices(
    store: dict[str, Any],
    materials: list[str] | None = None,
) -> tuple[list[int], list[str]]:
    warnings: list[str] = []
    if not materials:
        return list(range(store["dataset_count"])), warnings
    candidate_indices: list[int] = []
    missing: list[str] = []
    for material in materials:
        token = normalize_material_code(material)
        hits = store["code_index"].get(token, [])
        if not hits:
            missing.append(material)
            continue
        candidate_indices.extend(hits)
    if missing:
        warnings.append(f"No material knowledge evidence found for: {', '.join(missing)}")
    return dedupe_preserve_order(candidate_indices), warnings


def material_entry_to_output(
    entry: dict[str, Any],
    *,
    score: float | None,
    source_file: str,
    requested_materials: list[str] | None = None,
) -> dict[str, Any]:
    requested_tokens = {normalize_material_code(value) for value in (requested_materials or [])}
    matched_requested = [
        code
        for code in entry.get("material_codes", [])
        if normalize_material_code(code) in requested_tokens
    ]
    out = {
        "source_file": source_file,
        "point_id": entry.get("point_id"),
        "id": entry.get("id"),
        "material_codes": entry.get("material_codes", []),
        "text_snippet": compact_text(entry.get("text"), max_length=360),
        "score": score,
    }
    if matched_requested:
        out["matched_requested_materials"] = matched_requested
    return out


def lookup_material_entries(
    payload: dict[str, Any],
    *,
    materials: list[str] | None,
    limit: int = 10,
) -> tuple[list[dict[str, Any]], list[str]]:
    store = load_material_store(payload)
    indices, warnings = resolve_material_candidate_indices(store, materials)
    entries = [store["entries"][index] for index in indices[: max(limit, 0)]]
    return entries, warnings


def search_material_entries(
    payload: dict[str, Any],
    *,
    query: str,
    materials: list[str] | None = None,
    mode: str = "hybrid",
    limit: int = 10,
) -> tuple[list[tuple[dict[str, Any], float]], str, list[str]]:
    store = load_material_store(payload)
    warnings: list[str] = []
    indices, filter_warnings = resolve_material_candidate_indices(store, materials)
    warnings.extend(filter_warnings)
    if not indices:
        return [], "lexical" if mode == "hybrid" else mode, warnings

    mode = mode if mode in {"hybrid", "semantic", "lexical"} else "hybrid"
    query_tokens = tokenize_text(query)
    lexical_scores = [lexical_score_for_entry(store["entries"][index], query, query_tokens) for index in indices]
    max_lexical = max(lexical_scores) if lexical_scores else 0.0
    normalized_lexical = [
        (score / max_lexical) if max_lexical > 0 else 0.0
        for score in lexical_scores
    ]

    if mode == "lexical":
        score_vector = lexical_scores
        retrieval_mode = "lexical"
    else:
        query_vector, embed_warnings = build_query_embedding(query)
        warnings.extend(embed_warnings)
        if query_vector is None or not store["vector_dim"]:
            score_vector = lexical_scores
            retrieval_mode = "lexical"
        else:
            semantic_scores = [
                dot_product(store["entries"][index]["vector"], query_vector)
                for index in indices
            ]
            if mode == "semantic":
                score_vector = semantic_scores
                retrieval_mode = "semantic"
            else:
                score_vector = [
                    semantic_scores[pos] + (0.15 * normalized_lexical[pos])
                    for pos in range(len(indices))
                ]
                retrieval_mode = "hybrid"

    ranked_pairs = sorted(
        [(indices[pos], float(score_vector[pos])) for pos in range(len(indices))],
        key=lambda item: item[1],
        reverse=True,
    )
    if retrieval_mode == "lexical":
        ranked_pairs = [pair for pair in ranked_pairs if pair[1] > 0]
    ranked_pairs = ranked_pairs[: max(limit, 0)]
    return [(store["entries"][index], score) for index, score in ranked_pairs], retrieval_mode, warnings


def summarize_counter(counter: Counter[str], top_n: int = 20) -> list[dict[str, Any]]:
    return [{"name": key, "count": count} for key, count in counter.most_common(top_n)]


def profile_material_entries(entries: list[dict[str, Any]]) -> dict[str, Any]:
    property_counts: Counter[str] = Counter()
    property_examples: dict[str, list[str]] = defaultdict(list)
    material_code_counter: Counter[str] = Counter()
    multi_material_evidence_count = 0
    for entry in entries:
        codes = entry.get("material_codes", [])
        if len(codes) > 1:
            multi_material_evidence_count += 1
        material_code_counter.update(codes)
        text = entry.get("text") or ""
        for property_name, pattern in PROPERTY_PATTERNS.items():
            if pattern.search(text):
                property_counts[property_name] += 1
                if len(property_examples[property_name]) < 3:
                    property_examples[property_name].append(str(entry.get("id") or entry.get("point_id") or ""))
    return {
        "evidence_count": len(entries),
        "multi_material_evidence_count": multi_material_evidence_count,
        "property_counts": dict(property_counts),
        "property_examples": dict(property_examples),
        "material_code_frequency": summarize_counter(material_code_counter, top_n=20),
    }


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


def base_response(action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    source_files: list[dict[str, Any]] = []
    if payload is not None:
        path = resolve_export_path(payload)
        source_files.append(
            {
                "path": str(path),
                "exists": path.exists(),
                **(source_file_info(path) if path.exists() else {}),
            }
        )
    return {
        "status": "ok",
        "action": action,
        "source_files": source_files,
        "query_summary": {"action": action},
        "retrieval_mode": None,
        "evidence": [],
        "profile": {},
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


def run_tool(payload: dict[str, Any]) -> dict[str, Any]:
    action = clean_text(payload.get("action") or "schema").lower()
    response = base_response(action, payload)
    response["query_summary"].update(
        {
            "materials": as_string_list(payload.get("materials")),
            "query": clean_text(payload.get("query")),
            "mode": clean_text(payload.get("mode") or "hybrid"),
            "limit": limit_from(payload.get("limit"), 10),
            "export_path": str(resolve_export_path(payload)),
        }
    )

    try:
        if action == "schema":
            response["retrieval_mode"] = "schema"
            response["profile"] = material_schema(payload)
        elif action == "lookup":
            materials = response["query_summary"]["materials"]
            if not materials:
                response["warnings"].append("lookup requires at least one material.")
            entries, warnings = lookup_material_entries(
                payload,
                materials=materials,
                limit=response["query_summary"]["limit"],
            )
            store = load_material_store(payload)
            source_file = store["source_files"][0]["path"]
            response["retrieval_mode"] = "exact"
            response["evidence"] = [
                material_entry_to_output(entry, score=None, source_file=source_file, requested_materials=materials)
                for entry in entries
            ]
            response["profile"] = profile_material_entries(entries)
            response["warnings"].extend(warnings)
        elif action == "search":
            query = response["query_summary"]["query"]
            if not query:
                response["warnings"].append("search requires a non-empty query.")
            else:
                ranked_entries, retrieval_mode, warnings = search_material_entries(
                    payload,
                    query=query,
                    materials=response["query_summary"]["materials"],
                    mode=response["query_summary"]["mode"],
                    limit=response["query_summary"]["limit"],
                )
                raw_entries = [entry for entry, _ in ranked_entries]
                store = load_material_store(payload)
                source_file = store["source_files"][0]["path"]
                response["retrieval_mode"] = retrieval_mode
                response["evidence"] = [
                    material_entry_to_output(
                        entry,
                        score=score,
                        source_file=source_file,
                        requested_materials=response["query_summary"]["materials"],
                    )
                    for entry, score in ranked_entries
                ]
                response["profile"] = profile_material_entries(raw_entries)
                response["warnings"].extend(warnings)
        elif action == "profile":
            materials = response["query_summary"]["materials"]
            query = response["query_summary"]["query"]
            if query:
                ranked_entries, retrieval_mode, warnings = search_material_entries(
                    payload,
                    query=query,
                    materials=materials,
                    mode=response["query_summary"]["mode"],
                    limit=response["query_summary"]["limit"],
                )
                raw_entries = [entry for entry, _ in ranked_entries]
                evidence = ranked_entries
            else:
                if not materials:
                    response["warnings"].append("profile requires at least one material or a query.")
                raw_entries, warnings = lookup_material_entries(
                    payload,
                    materials=materials,
                    limit=response["query_summary"]["limit"],
                )
                retrieval_mode = "exact"
                evidence = [(entry, None) for entry in raw_entries]
            store = load_material_store(payload)
            source_file = store["source_files"][0]["path"]
            response["retrieval_mode"] = retrieval_mode
            response["evidence"] = [
                material_entry_to_output(
                    entry,
                    score=score,
                    source_file=source_file,
                    requested_materials=materials,
                )
                for entry, score in evidence
            ]
            response["profile"] = profile_material_entries(raw_entries)
            response["warnings"].extend(warnings)
        else:
            response["errors"].append(f"Unsupported action: {action}")
    except Exception as exc:
        response["errors"].append(f"{type(exc).__name__}: {exc}")

    return apply_status(response)
