from __future__ import annotations

import json
import math
import re
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .evaluation import (
    _cosine_similarity,
    _fallback_similarity,
    _load_backends,
    _normalize_cosine,
    _rerank_similarity,
)


@dataclass
class RuleFieldSpec:
    protocol_name: str
    file_name: str
    field_name: str
    normalized_name: str
    path: str
    path_parts: Tuple[str, ...]
    bit_length: Optional[int]
    default_value: Optional[str]
    source_tag: str
    semantic_group: Optional[str]


SEMANTIC_GROUP_PATTERNS: Dict[str, Tuple[str, ...]] = {
    "longitude": ("经度", "longitude", "lon"),
    "latitude": ("纬度", "latitude", "lat"),
    "altitude": ("高度", "高程", "海拔", "altitude", "height", "elevation"),
    "pitch": ("俯仰", "pitch"),
    "roll": ("翻滚", "横滚", "roll"),
    "yaw": ("偏航", "航向", "yaw", "heading"),
    "speed": ("速度", "speed", "velocity"),
    "direction": ("方向", "bearing", "course"),
    "time": ("时间", "时刻", "飞临时间", "time"),
    "day": ("日", "day"),
    "hour": ("小时", "hour"),
    "minute": ("分钟", "minute", "min"),
    "second": ("秒", "second", "sec"),
    "threat_type": ("威胁类型",),
    "threat_form": ("威胁形式",),
    "target_id": ("目标编号", "targetid"),
    "unit_id": ("作战单元编号", "unitid"),
    "quantity": ("数量", "目标数量", "quantity", "count"),
    "name": ("名称", "name"),
    "info": ("信息", "info"),
    "fpi": ("fpi",),
    "gpi": ("gpi",),
}

DIRECT_RULE_TYPES = {"direct", "transcoding", "mapping"}
EXPR_RULE_TYPES = {"expression", "python_expr", "python_block", "formula"}
CONST_RULE_TYPES = {"const", "constant"}
ALLOWED_RULE_TYPES = DIRECT_RULE_TYPES | EXPR_RULE_TYPES | CONST_RULE_TYPES
RULE_REF_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[^\s\+\-\*/<>=!\(\),?:]+")
NUMERIC_LITERAL_PATTERN = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$")


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    if ":" in tag:
        return tag.split(":", 1)[1]
    return tag


def _parse_int(raw: Optional[str]) -> Optional[int]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _normalize_lookup_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[\s_\-./:：，,()\[\]{}]+", "", text)
    return text


def _infer_semantic_group(text: str) -> Optional[str]:
    normalized = _normalize_lookup_value(text)
    if not normalized:
        return None
    for group, patterns in SEMANTIC_GROUP_PATTERNS.items():
        if any(_normalize_lookup_value(pattern) in normalized for pattern in patterns):
            return group
    return None


def _field_text(spec: RuleFieldSpec) -> str:
    parts = [
        f"字段名:{spec.field_name}",
        f"路径:{spec.path}",
        f"协议:{spec.protocol_name}",
        f"文件:{spec.file_name}",
    ]
    if spec.bit_length is not None:
        parts.append(f"位宽:{spec.bit_length}")
    if spec.default_value is not None:
        parts.append(f"默认值:{spec.default_value}")
    if spec.semantic_group:
        parts.append(f"语义组:{spec.semantic_group}")
    return " | ".join(parts)


def _append_field_specs(
    node: ET.Element,
    protocol_name: str,
    file_name: str,
    path_parts: Tuple[str, ...],
    output: List[RuleFieldSpec],
) -> None:
    local = _local_name(node.tag)
    node_name = str(node.attrib.get("name") or "").strip() or local
    current_path = path_parts + ((node_name,) if node_name else ())

    if local in {"Item", "StructMess"}:
        field_name = node_name
        normalized_name = _normalize_lookup_value(field_name)
        if normalized_name:
            output.append(
                RuleFieldSpec(
                    protocol_name=protocol_name,
                    file_name=file_name,
                    field_name=field_name,
                    normalized_name=normalized_name,
                    path="/".join(current_path),
                    path_parts=current_path,
                    bit_length=_parse_int((node.text or "").strip()) or _parse_int(node.attrib.get("bitLength")) or _parse_int(node.attrib.get("length")),
                    default_value=str(node.attrib.get("defaultValue") or "").strip() or None,
                    source_tag=local,
                    semantic_group=_infer_semantic_group(field_name),
                )
            )
        return

    next_path = current_path if local in {"Field", "Group", "NameSpace"} else path_parts
    for child in list(node):
        _append_field_specs(child, protocol_name, file_name, next_path, output)


def _load_protocol_fields_from_dir(path_like: Any, field_name: str) -> List[RuleFieldSpec]:
    if isinstance(path_like, (list, tuple)):
        dirs = [Path(str(item or "").strip()) for item in path_like]
    else:
        dirs = [Path(str(path_like or "").strip())]
    if not dirs or any(not str(item) for item in dirs):
        raise ValueError(f"{field_name}不能为空")

    results: List[RuleFieldSpec] = []
    for index, directory in enumerate(dirs):
        label = field_name if len(dirs) == 1 else f"{field_name}[{index}]"
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"{label}不存在: {directory}")
        xml_files = sorted(directory.glob("*.xml"))
        if not xml_files:
            raise ValueError(f"{label}下未找到 XML 文件: {directory}")
        for xml_file in xml_files:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            protocol_name = xml_file.stem
            _append_field_specs(root, protocol_name, xml_file.name, tuple(), results)
    return results


def _load_json_like(value: Any, field_name: str) -> Any:
    if value is None:
        raise ValueError(f"{field_name}不能为空")
    if isinstance(value, (dict, list)):
        return value
    raw = str(value).strip()
    if not raw:
        raise ValueError(f"{field_name}不能为空")
    candidate = Path(raw)
    if candidate.exists() and candidate.is_file():
        return json.loads(candidate.read_text(encoding="utf-8-sig"))
    return json.loads(raw)


def _extract_rule_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return _extract_rule_items(payload["data"])
    if isinstance(payload, dict) and isinstance(payload.get("body"), dict):
        return _extract_rule_items(payload["body"])
    if isinstance(payload, dict):
        conversions = payload.get("conversions")
        if isinstance(conversions, list):
            items: List[Dict[str, Any]] = []
            for conversion in conversions:
                if not isinstance(conversion, dict):
                    continue
                rules = conversion.get("rules")
                if isinstance(rules, list):
                    items.extend(item for item in rules if isinstance(item, dict))
            if items:
                return items
        for key in ("rules", "generated_rules", "target_field_rules", "normalized_rules"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _normalize_source_fields(item: Dict[str, Any]) -> List[str]:
    raw = item.get("source_fields")
    values: List[str] = []
    if isinstance(raw, list):
        values = [str(value).strip() for value in raw if str(value).strip()]
    elif isinstance(raw, str):
        values = [value.strip() for value in raw.split(",") if value.strip()]
    fallback = str(item.get("field_name") or item.get("source_field") or "").strip()
    if not values and fallback:
        values = [fallback]
    return values


def _infer_rule_type(item: Dict[str, Any], formula: str, source_fields: List[str]) -> str:
    raw_type = str(item.get("rule_type") or item.get("formula_kind") or item.get("conversion_mode") or "").strip().lower()
    if raw_type in ALLOWED_RULE_TYPES:
        return raw_type
    normalized_formula = formula.strip()
    if not source_fields and NUMERIC_LITERAL_PATTERN.fullmatch(normalized_formula):
        return "const"
    if normalized_formula == "0":
        return "const"
    if len(source_fields) == 1:
        cleaned = source_fields[0].split(".")[-1].strip()
        if cleaned and cleaned in formula:
            return "direct"
    return "expression"


def _normalize_rule_items(payload: Any) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    for item in _extract_rule_items(payload):
        target_field = str(item.get("target_field") or item.get("field_name") or "").strip()
        formula = str(item.get("formula") or item.get("rule") or item.get("conversion_formula") or "").strip()
        source_fields = _normalize_source_fields(item)
        if not target_field:
            continue
        rules.append(
            {
                "target_field": target_field,
                "formula": formula,
                "source_fields": source_fields,
                "rule_type": _infer_rule_type(item, formula, source_fields),
                "default_value": item.get("default_value"),
                "description": item.get("description"),
                "when": item.get("when"),
            }
        )
    return rules


def _build_field_indices(fields: Sequence[RuleFieldSpec]) -> Tuple[Dict[str, RuleFieldSpec], Dict[str, List[RuleFieldSpec]]]:
    by_name: Dict[str, RuleFieldSpec] = {}
    alias_index: Dict[str, List[RuleFieldSpec]] = {}
    for field in fields:
        by_name.setdefault(field.normalized_name, field)
        for alias in {
            field.normalized_name,
            _normalize_lookup_value(field.field_name),
            _normalize_lookup_value(field.path_parts[-1] if field.path_parts else field.field_name),
        }:
            if not alias:
                continue
            bucket = alias_index.setdefault(alias, [])
            if field not in bucket:
                bucket.append(field)
    return by_name, alias_index


def _resolve_rule_field_refs(field_names: Sequence[str], alias_index: Dict[str, List[RuleFieldSpec]]) -> List[RuleFieldSpec]:
    results: List[RuleFieldSpec] = []
    seen = set()
    for field_name in field_names:
        key = _normalize_lookup_value(field_name.split(".")[-1])
        if not key:
            continue
        for candidate in alias_index.get(key, []):
            signature = (candidate.protocol_name, candidate.normalized_name, candidate.path)
            if signature in seen:
                continue
            seen.add(signature)
            results.append(candidate)
    return results


def _score_text_similarity(query_text: str, doc_text: str, backend: Any) -> float:
    if backend.embedding is not None:
        return _normalize_cosine(_cosine_similarity(backend.embedding, query_text, doc_text))
    return _fallback_similarity(query_text, doc_text)


def _semantic_conflict(target_group: Optional[str], source_group: Optional[str]) -> bool:
    if not target_group or not source_group:
        return False
    directional = {"longitude", "latitude", "altitude", "pitch", "roll", "yaw", "speed", "direction"}
    if target_group in directional and source_group in directional and target_group != source_group:
        return True
    if target_group in {"threat_type", "threat_form"} and source_group in {"threat_type", "threat_form"} and target_group != source_group:
        return True
    return False


def _score_dimension_consistency(target_field: RuleFieldSpec, source_fields: Sequence[RuleFieldSpec], rule_type: str) -> Tuple[float, bool]:
    if rule_type in CONST_RULE_TYPES or not source_fields:
        return 0.0, False
    if any(_semantic_conflict(target_field.semantic_group, source.semantic_group) for source in source_fields):
        return 0.0, False
    if target_field.semantic_group and any(source.semantic_group == target_field.semantic_group for source in source_fields):
        return 100.0, True
    if target_field.bit_length and any(source.bit_length == target_field.bit_length for source in source_fields if source.bit_length):
        return 80.0, True
    if rule_type in EXPR_RULE_TYPES and len(source_fields) > 1:
        return 70.0, True
    if not target_field.semantic_group and all(source.semantic_group is None for source in source_fields):
        return 60.0, True
    return 35.0, False


def _formula_is_valid(formula: str, rule_type: str) -> bool:
    text = str(formula or "").strip()
    if not text:
        return False
    if rule_type in CONST_RULE_TYPES:
        return bool(NUMERIC_LITERAL_PATTERN.fullmatch(text))
    if text.count("(") != text.count(")"):
        return False
    if NUMERIC_LITERAL_PATTERN.fullmatch(text):
        return True
    if rule_type in DIRECT_RULE_TYPES:
        return bool(RULE_REF_PATTERN.search(text) or re.search(r"[A-Za-z0-9_\u4e00-\u9fff]+", text))
    return len(text) >= 2


def _safe_percent(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator * 100.0, 4)


def evaluate_protocol_rules(
    source_protocol_dirs: Any,
    target_protocol_dir: Any,
    conversion_rules: Any,
    coarse_top_k: int = 10,
    coarse_similarity_threshold: float = 0.55,
    fine_similarity_threshold: float = 0.75,
    use_model_inference: bool = True,
    allow_modelscope_download: bool = True,
    trace_id: Optional[str] = None,
) -> Dict[str, Any]:
    source_fields = _load_protocol_fields_from_dir(source_protocol_dirs, "source_protocol_dirs")
    target_fields = _load_protocol_fields_from_dir(target_protocol_dir, "target_protocol_dir")
    rules = _normalize_rule_items(_load_json_like(conversion_rules, "conversion_rules"))
    backend = _load_backends(use_model_inference=use_model_inference, allow_modelscope_download=allow_modelscope_download)
    trace = str(trace_id or uuid.uuid4())

    _, source_alias_index = _build_field_indices(source_fields)
    target_index, target_alias_index = _build_field_indices(target_fields)
    rule_map: Dict[str, Dict[str, Any]] = {}
    duplicate_targets = set()
    for rule in rules:
        normalized_target = _normalize_lookup_value(rule["target_field"])
        if normalized_target in rule_map:
            duplicate_targets.add(normalized_target)
        rule_map.setdefault(normalized_target, rule)

    field_results: List[Dict[str, Any]] = []
    coarse_recall_hit_count = 0
    fine_accept_count = 0
    non_zero_rule_count = 0
    zero_fallback_count = 0
    covered_field_count = 0
    converted_field_count = 0
    invalid_rule_count = 0
    field_match_values: List[float] = []
    semantic_values: List[float] = []
    dimension_values: List[float] = []
    structure_values: List[float] = []
    convertible_field_match_values: List[float] = []
    convertible_semantic_values: List[float] = []
    convertible_dimension_values: List[float] = []
    convertible_structure_values: List[float] = []

    for target_field in target_fields:
        target_key = target_field.normalized_name
        rule = rule_map.get(target_key)
        target_text = _field_text(target_field)

        scored_candidates: List[Dict[str, Any]] = []
        for source_field in source_fields:
            similarity = _score_text_similarity(target_text, _field_text(source_field), backend)
            if similarity < float(coarse_similarity_threshold):
                continue
            scored_candidates.append(
                {
                    "field_name": source_field.field_name,
                    "protocol_name": source_field.protocol_name,
                    "path": source_field.path,
                    "semantic_group": source_field.semantic_group,
                    "coarse_similarity": round(similarity, 4),
                    "_spec": source_field,
                    "content": _field_text(source_field),
                }
            )
        scored_candidates.sort(key=lambda item: item["coarse_similarity"], reverse=True)
        top_candidates = scored_candidates[: max(int(coarse_top_k), 1)]
        coarse_hit = bool(top_candidates)
        if coarse_hit:
            coarse_recall_hit_count += 1

        fine_candidates = []
        if top_candidates:
            if backend.reranker is not None:
                reranked = backend.reranker.rerank_pairs(target_text, top_candidates, top_k=len(top_candidates), text_key="content")
                for item in reranked:
                    probability = item.get("rerank_probability")
                    if probability is not None:
                        item["fine_similarity"] = round(float(probability), 4)
                    else:
                        item["fine_similarity"] = round(1.0 / (1.0 + math.exp(-float(item.get("rerank_score", 0.0)))), 4)
                    fine_candidates.append(item)
            else:
                for item in top_candidates:
                    item = dict(item)
                    item["fine_similarity"] = item["coarse_similarity"]
                    fine_candidates.append(item)
        accepted_candidates = [item for item in fine_candidates if float(item.get("fine_similarity", 0.0)) >= float(fine_similarity_threshold)]

        issues: List[str] = []
        status = "missing_rule"
        rule_type = None
        formula = None
        source_match_specs: List[RuleFieldSpec] = []
        field_match_score = 0.0
        semantic_score = 0.0
        dimension_score = 0.0
        structure_score = 100.0
        structure_pass = True
        formula_valid = False

        if rule:
            covered_field_count += 1
            rule_type = str(rule["rule_type"]).strip().lower()
            formula = str(rule["formula"]).strip()
            if rule_type in CONST_RULE_TYPES and formula == "0":
                zero_fallback_count += 1
                status = "fallback_zero"
            else:
                non_zero_rule_count += 1
            if target_key in duplicate_targets:
                issues.append("目标字段存在重复规则")
                structure_score -= 25.0
            formula_valid = _formula_is_valid(formula, rule_type)
            if not formula_valid:
                issues.append("公式为空或格式不可解析")
                structure_score -= 20.0
            source_match_specs = _resolve_rule_field_refs(rule.get("source_fields") or [], source_alias_index)
            if rule_type not in CONST_RULE_TYPES and not source_match_specs:
                issues.append("源字段未在原协议 XML 中命中")
                structure_score -= 20.0
            if rule_type not in ALLOWED_RULE_TYPES:
                issues.append("规则类型不受支持")
                structure_score -= 15.0
            structure_score = max(structure_score, 0.0)
            structure_pass = structure_score >= 80.0

            declared_keys = {
                _normalize_lookup_value(item.field_name)
                for item in source_match_specs
            }
            matched_fine = [
                item for item in accepted_candidates
                if _normalize_lookup_value(str(item.get("field_name") or "")) in declared_keys
            ]
            if rule_type in CONST_RULE_TYPES:
                field_match_score = 0.0
                semantic_score = 0.0
                dimension_score = 0.0
            else:
                semantic_score = round(
                    sum(float(item.get("fine_similarity", 0.0)) for item in matched_fine) / max(len(matched_fine), 1) * 100.0,
                    4,
                )
                field_match_pass = bool(declared_keys) and len(matched_fine) == len(declared_keys) and formula_valid
                field_match_score = 100.0 if field_match_pass else (_safe_percent(len(matched_fine), max(len(declared_keys), 1)))
                dimension_score, dimension_pass = _score_dimension_consistency(target_field, source_match_specs, rule_type)
                if not matched_fine:
                    issues.append("精排序未支持当前规则声明的源字段")
                if not dimension_pass:
                    issues.append("量纲/物理语义一致性不足")
                if field_match_pass and semantic_score >= fine_similarity_threshold * 100.0 and dimension_pass and structure_pass:
                    status = "pass"
                    fine_accept_count += 1
                    converted_field_count += 1
                else:
                    status = "fail"
        else:
            issues.append("目标字段缺少规则")

        if rule and status == "fallback_zero":
            issues.append("无法可靠转换，规则显式置 0")
        if rule and status == "fail" and not formula_valid:
            invalid_rule_count += 1

        field_match_values.append(field_match_score)
        semantic_values.append(semantic_score)
        dimension_values.append(dimension_score)
        structure_values.append(structure_score)
        is_convertible_rule = bool(rule) and not (
            rule_type in CONST_RULE_TYPES and formula == "0"
        )
        if is_convertible_rule:
            convertible_field_match_values.append(field_match_score)
            convertible_semantic_values.append(semantic_score)
            convertible_dimension_values.append(dimension_score)
            convertible_structure_values.append(structure_score)
        field_results.append(
            {
                "target_field": target_field.field_name,
                "target_protocol": target_field.protocol_name,
                "rule_type": rule_type,
                "formula": formula,
                "status": status,
                "candidate_count": len(top_candidates),
                "coarse_hit": coarse_hit,
                "coarse_candidates": [
                    {
                        "field_name": item["field_name"],
                        "protocol_name": item["protocol_name"],
                        "coarse_similarity": round(float(item["coarse_similarity"]) * 100.0, 4),
                    }
                    for item in top_candidates[:5]
                ],
                "fine_candidates": [
                    {
                        "field_name": item["field_name"],
                        "protocol_name": item["protocol_name"],
                        "fine_similarity": round(float(item["fine_similarity"]) * 100.0, 4),
                    }
                    for item in accepted_candidates[:5]
                ],
                "declared_source_fields": list(rule.get("source_fields") or []) if rule else [],
                "resolved_source_fields": [item.field_name for item in source_match_specs],
                "field_match_correctness": round(field_match_score, 4),
                "semantic_fidelity": round(semantic_score, 4),
                "structure_integrity": round(structure_score, 4),
                "dimension_consistency_accuracy": round(dimension_score, 4),
                "issues": issues,
            }
        )

    existing_target_keys = {field.normalized_name for field in target_fields}
    for target_key, rule in rule_map.items():
        if target_key in existing_target_keys:
            continue
        invalid_rule_count += 1
        field_match_values.append(0.0)
        semantic_values.append(0.0)
        dimension_values.append(0.0)
        structure_values.append(0.0)
        field_results.append(
            {
                "target_field": rule["target_field"],
                "target_protocol": None,
                "rule_type": rule["rule_type"],
                "formula": rule["formula"],
                "status": "invalid_target",
                "candidate_count": 0,
                "coarse_hit": False,
                "coarse_candidates": [],
                "fine_candidates": [],
                "declared_source_fields": list(rule.get("source_fields") or []),
                "resolved_source_fields": [],
                "field_match_correctness": 0.0,
                "semantic_fidelity": 0.0,
                "structure_integrity": 0.0,
                "dimension_consistency_accuracy": 0.0,
                "issues": ["规则目标字段未在目标协议 XML 中找到"],
            }
        )

    target_field_count = len(target_fields)
    convertible_field_count = len(convertible_field_match_values)
    field_match_accuracy = round(sum(convertible_field_match_values) / max(convertible_field_count, 1), 4)
    semantic_fidelity = round(sum(convertible_semantic_values) / max(convertible_field_count, 1), 4)
    structure_integrity = round(sum(convertible_structure_values) / max(convertible_field_count, 1), 4)
    dimension_accuracy = round(sum(convertible_dimension_values) / max(convertible_field_count, 1), 4)
    overall_correctness_score = round(
        field_match_accuracy * 0.30
        + semantic_fidelity * 0.25
        + structure_integrity * 0.20
        + dimension_accuracy * 0.25,
        4,
    )
    field_coverage_rate = _safe_percent(covered_field_count, target_field_count)
    final_conversion_rate = _safe_percent(converted_field_count, target_field_count)

    return {
        "trace_id": trace,
        "coarse_recall": {
            "candidate_top_k": int(coarse_top_k),
            "similarity_threshold": float(coarse_similarity_threshold),
            "recall_rate": _safe_percent(coarse_recall_hit_count, target_field_count),
            "hit_target_count": coarse_recall_hit_count,
            "target_field_count": target_field_count,
        },
        "fine_rerank": {
            "similarity_threshold": float(fine_similarity_threshold),
            "accepted_rule_count": fine_accept_count,
            "evaluated_rule_count": non_zero_rule_count,
        },
        "scores": {
            "field_match_accuracy": field_match_accuracy,
            "semantic_fidelity": semantic_fidelity,
            "structure_integrity": structure_integrity,
            "overall_correctness_score": overall_correctness_score,
            "dimension_consistency_accuracy": dimension_accuracy,
            "field_coverage_rate": field_coverage_rate,
            "final_conversion_rate": final_conversion_rate,
        },
        "summary": {
            "target_field_count": target_field_count,
            "rule_count": len(rules),
            "covered_field_count": covered_field_count,
            "converted_field_count": converted_field_count,
            "convertible_field_count": convertible_field_count,
            "non_zero_rule_count": non_zero_rule_count,
            "zero_fallback_count": zero_fallback_count,
            "duplicate_target_count": len(duplicate_targets),
            "invalid_rule_count": invalid_rule_count,
        },
        "field_results": field_results,
        "strategy": {
            "model_inference_enabled": use_model_inference,
            "allow_modelscope_download": allow_modelscope_download,
            "embedding_backend": backend.embedding_info.__dict__,
            "reranker_backend": backend.reranker_info.__dict__,
            "degraded": not (backend.embedding_info.available and backend.reranker_info.available),
        },
    }
