# 接口3: QA字段智能抽取与规则校验
# POST /api/knowledge/extract_validate_qa

import sys
import os
import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from flask import Flask, request, jsonify

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.llm.local_llm import LocalLLM, get_llm
from shared.llm.prompt_templates import PromptTemplates
from shared.database.mysql_client import MySQLClient
from shared.database.models import QAPair
from shared.code_generation_adapter import (
    build_code_generation_payload,
    build_generator_rules_payload,
    read_protocol_dir_content,
    resolve_protocol_field_specs,
    resolve_protocol_type_names,
)
from shared.protocol_schema import (
    build_schema_prompt_context,
    guess_message_code,
    resolve_message_schema,
    validate_with_schema,
)
from shared.protocol_conversion import (
    ProtocolConversionKnowledgeBase,
    build_protocol_doc_index,
    evaluate_protocol_conversion,
    evaluate_protocol_conversion_rate,
    execute_protocol_conversion,
    export_protocol_rules,
    generate_and_convert_protocol_bundle,
    generate_protocol_field_rules,
    validate_protocol_rules,
)

app = Flask(__name__)

# 初始化组件
_llm: Optional[LocalLLM] = None
_db: Optional[MySQLClient] = None
_SIMPLE_RULE_TYPES = {"const", "direct", "expression", "conditional"}
_NUMERIC_LITERAL_PATTERN = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)$")


def get_llm_instance() -> LocalLLM:
    """获取LLM实例（延迟初始化）"""
    global _llm
    if _llm is None:
        _llm = get_llm()
    return _llm


def get_db_instance() -> MySQLClient:
    """获取数据库实例（延迟初始化）"""
    global _db
    if _db is None:
        _db = MySQLClient()
    return _db


def _handle_batch_extract_validate_payload(data: Dict[str, Any]):
    items = data.get("items", [])
    if not items:
        return jsonify({
            "code": 400,
            "message": "items 数组不能为空",
            "data": None
        }), 400

    results = []
    success_count = 0
    failed_count = 0

    for item in items:
        try:
            qa_id = item.get("qa_id")
            question = item.get("question", "")
            answer = item.get("answer", "")
            protocol_type = item.get("protocol_type", "Link16")
            message_code = item.get("message_code")

            if not qa_id or not question or not answer:
                results.append({
                    "qa_id": qa_id,
                    "status": "failed",
                    "error": "缺少必要参数"
                })
                failed_count += 1
                continue

            result = run_extraction_pipeline(
                qa_id=qa_id,
                question=question,
                answer=answer,
                protocol_type=protocol_type,
                message_code=message_code,
            )

            results.append({
                "qa_id": qa_id,
                "status": "success",
                "result": result
            })
            success_count += 1
        except Exception as e:
            results.append({
                "qa_id": item.get("qa_id", "unknown"),
                "status": "failed",
                "error": str(e)
            })
            failed_count += 1

    return jsonify({
        "code": 200,
        "message": "success",
        "data": {
            "total": len(items),
            "success": success_count,
            "failed": failed_count,
            "results": results
        }
    })


def _merge_protocol_request_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(data)
    source_protocol_input = payload.get("source_protocol_dirs")
    source_protocol_field_name = "source_protocol_dirs"
    if source_protocol_input is None:
        source_protocol_input = payload.get("source_protocol_dir")
        source_protocol_field_name = "source_protocol_dir"

    if not payload.get("source_protocol") and source_protocol_input:
        source_protocol_types = resolve_protocol_type_names(source_protocol_input, source_protocol_field_name)
        payload["source_protocol"] = {
            "name": str(payload.get("source_protocol_name") or "").strip() or (source_protocol_types[0] if len(source_protocol_types) == 1 else None),
            "protocol_type": str(payload.get("source_protocol_type") or "").strip() or (source_protocol_types[0] if len(source_protocol_types) == 1 else None),
            "message_code": str(payload.get("message_code") or "").strip() or None,
            "content": read_protocol_dir_content(source_protocol_input, source_protocol_field_name),
        }
    if source_protocol_input:
        payload["source_message"] = _build_source_message_from_protocol_specs(
            source_protocol_input,
            source_protocol_field_name,
        )

    target_protocol = payload.get("target_protocol")
    if not isinstance(target_protocol, dict):
        target_protocol = {}
    target_protocol_dir = payload.get("target_protocol_dir")
    if target_protocol_dir:
        target_protocol_types = resolve_protocol_type_names(target_protocol_dir, "target_protocol_dir")
        target_protocol = dict(target_protocol)
        target_protocol.setdefault("name", target_protocol_types[0] if len(target_protocol_types) == 1 else None)
        target_protocol.setdefault("protocol_type", target_protocol_types[0] if len(target_protocol_types) == 1 else None)
        target_protocol["content"] = read_protocol_dir_content(target_protocol_dir, "target_protocol_dir")
    if target_protocol:
        payload["target_protocol"] = target_protocol

    return payload


def _save_protocol_rules_file(
    data: Dict[str, Any],
    rules_payload: Dict[str, Any],
) -> Optional[str]:
    rules_output_dir = "output/rules"
    rules_file_name = str(data.get("rules_file_name") or "07_protocol_generate_rules.json").strip() or "07_protocol_generate_rules.json"

    if not rules_output_dir:
        rules_output_dir = "output/rules"
    output_dir = Path(rules_output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / rules_file_name

    output_path.write_text(
        json.dumps(rules_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(output_path)


def _build_source_message_from_protocol_specs(path_like: Any, field_name: str) -> Dict[str, Any]:
    source_message: Dict[str, Any] = {}
    for spec in resolve_protocol_field_specs(path_like, field_name):
        display_name = str(spec.get("field_name") or "").strip()
        if not display_name or display_name in source_message:
            continue
        source_message[display_name] = spec.get("default_value")
    return source_message


def _resolve_protocol_name(protocol_payload: Any) -> Optional[str]:
    if isinstance(protocol_payload, dict):
        for key in ("protocol_type", "name", "message_type"):
            value = str(protocol_payload.get(key) or "").strip()
            if value:
                return value
        return None
    value = str(protocol_payload or "").strip()
    return value or None


def _flatten_manual_evidence_text(raw_evidence: Any) -> Optional[str]:
    if isinstance(raw_evidence, str):
        text = raw_evidence.strip()
        return text or None
    if not isinstance(raw_evidence, list):
        return None

    parts: List[str] = []
    for item in raw_evidence:
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            text = str(item.get("content") or item.get("text") or "").strip()
        else:
            text = str(item or "").strip()
        if text:
            parts.append(text)
    if not parts:
        return None
    return "\n".join(parts)


def _normalize_manual_writeback_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ValueError("rules不能为空，且必须是数组")

    normalized_rules: List[Dict[str, Any]] = []
    invalid_targets: List[str] = []
    for index, item in enumerate(raw_rules):
        if not isinstance(item, dict):
            raise ValueError(f"rules[{index}] 必须是对象")

        target_field = str(item.get("target_field") or "").strip()
        formula = str(
            item.get("formula")
            or item.get("rule")
            or item.get("conversion_formula")
            or item.get("expression")
            or ""
        ).strip()
        if not target_field:
            invalid_targets.append(f"rules[{index}].target_field")
            continue
        if not formula:
            invalid_targets.append(f"{target_field}.formula")
            continue

        source_fields = item.get("source_fields")
        if isinstance(source_fields, list):
            normalized_source_fields = [
                str(value).strip()
                for value in source_fields
                if str(value).strip()
            ]
        elif isinstance(source_fields, str):
            normalized_source_fields = [
                value.strip()
                for value in source_fields.split(",")
                if value.strip()
            ]
        else:
            normalized_source_fields = []

        if not normalized_source_fields:
            field_name = str(item.get("field_name") or item.get("source_field") or "").strip()
            if field_name:
                normalized_source_fields = [field_name]

        normalized_rule = {
            "concept_name": str(item.get("concept_name") or target_field).strip() or target_field,
            "field_name": normalized_source_fields[0] if normalized_source_fields else "",
            "source_fields": normalized_source_fields,
            "target_field": target_field,
            "conversion_mode": str(item.get("conversion_mode") or item.get("mode") or "transcoding").strip().lower() or "transcoding",
            "formula": formula,
            "description": str(item.get("description") or "").strip() or _flatten_manual_evidence_text(item.get("evidence")),
            "confidence": item.get("confidence"),
            "unit": item.get("unit"),
            "bit_length": item.get("bit_length"),
            "status": "approved",
            "source": "manual_review",
        }
        normalized_rules.append(normalized_rule)

    if invalid_targets:
        raise ValueError(f"存在缺失必要字段的规则: {', '.join(invalid_targets)}")
    if not normalized_rules:
        raise ValueError("没有可写回的有效规则")
    return normalized_rules


def _displayize_rule_records(
    rule_records: Any,
    protocol_dir: Optional[Path],
    target_protocol_name: Optional[str],
    source_protocol_name: Optional[str],
) -> Any:
    if not isinstance(rule_records, list) or not rule_records:
        return rule_records

    merged_rules = []
    for item in rule_records:
        if not isinstance(item, dict):
            merged_rules.append(item)
            continue
        surrogate_rule = {
            "field_name": item.get("field_name"),
            "source_fields": item.get("source_fields"),
            "target_field": item.get("target_field"),
            "conversion_mode": item.get("conversion_mode"),
            "formula": item.get("formula") or item.get("rule"),
            "rule": item.get("rule") or item.get("formula"),
            "description": item.get("description"),
            "concept_name": item.get("concept_name"),
        }
        display_payload = build_generator_rules_payload(
            raw_rules={"normalized_rules": [surrogate_rule]},
            protocol_dir=protocol_dir,
            target_protocol_name=target_protocol_name,
            source_protocol_name=source_protocol_name,
            preserve_display_names=True,
        )
        display_rules = (((display_payload.get("conversions") or [{}])[0]).get("rules") or [])
        merged = dict(item)
        if display_rules:
            display_rule = display_rules[0]
            merged["target_field"] = display_rule.get("target_field", merged.get("target_field"))
            merged["target_actual_field"] = display_rule.get("target_actual_field", merged.get("target_actual_field"))
            merged["target_path"] = display_rule.get("target_path", merged.get("target_path"))
            if "source_fields" in merged:
                merged["source_fields"] = display_rule.get("source_fields", merged.get("source_fields"))
            merged["source_actual_fields"] = display_rule.get("source_actual_fields", merged.get("source_actual_fields"))
            merged["source_paths"] = display_rule.get("source_paths", merged.get("source_paths"))
            display_formula = display_rule.get("formula")
            if display_formula:
                if "formula" in merged:
                    merged["formula"] = display_formula
                if "rule" in merged:
                    merged["rule"] = display_formula
        merged_rules.append(merged)
    return merged_rules


def _filter_display_writeback_rules(rule_records: Any) -> Any:
    if not isinstance(rule_records, list):
        return rule_records
    filtered_rules = []
    for item in rule_records:
        if not isinstance(item, dict):
            continue
        source_fields = [str(value).strip() for value in (item.get("source_fields") or []) if str(value).strip()]
        formula = str(item.get("formula") or item.get("rule") or "").strip()
        target_field = str(item.get("target_field") or "").strip()
        if not target_field or not source_fields or not formula or formula == "0":
            continue
        filtered_rules.append(item)
    return filtered_rules


def _merge_protocol_dirs(source_protocol_dir: str, target_protocol_dir: str) -> Optional[Path]:
    if not source_protocol_dir or not target_protocol_dir:
        return None
    merged_root = Path(tempfile.mkdtemp(prefix="protocol_rules_", dir=str(Path("tmp").resolve())))
    copied_names: set[str] = set()
    for field_name, directory_text in (
        ("source_protocol_dir", source_protocol_dir),
        ("target_protocol_dir", target_protocol_dir),
    ):
        directory_values = directory_text if isinstance(directory_text, list) else [directory_text]
        for index, item in enumerate(directory_values):
            directory = Path(str(item or "").strip()).resolve()
            label = field_name if len(directory_values) == 1 else f"{field_name}[{index}]"
            if not directory.exists() or not directory.is_dir():
                raise ValueError(f"{label} 不存在: {directory}")
            xml_files = sorted(directory.glob("*.xml"))
            if not xml_files:
                raise ValueError(f"{label} 下未找到 XML 文件: {directory}")
            for xml_file in xml_files:
                target_path = merged_root / xml_file.name
                if xml_file.name in copied_names:
                    if target_path.read_text(encoding="utf-8-sig") != xml_file.read_text(encoding="utf-8-sig"):
                        raise ValueError(f"{label} 中存在重名但内容不同的 XML 文件: {xml_file.name}")
                    continue
                shutil.copy(xml_file, target_path)
                copied_names.add(xml_file.name)
    return merged_root


def _iter_conversion_rules(rules_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rules: List[Dict[str, Any]] = []
    for conversion in rules_payload.get("conversions") or []:
        if not isinstance(conversion, dict):
            continue
        for rule in conversion.get("rules") or []:
            if isinstance(rule, dict):
                rules.append(rule)
    return rules


def _build_protocol_spec_index(protocol_dir: Optional[Path]) -> Dict[str, List[Dict[str, Any]]]:
    if not protocol_dir:
        return {}
    index: Dict[str, List[Dict[str, Any]]] = {}
    for spec in resolve_protocol_field_specs(protocol_dir, "protocol_dir"):
        protocol_name = str(spec.get("protocol") or "").strip()
        if not protocol_name:
            continue
        index.setdefault(protocol_name, []).append(spec)
    return index


def _field_candidates(spec: Dict[str, Any]) -> set[str]:
    path_parts = spec.get("path_parts") or []
    return {
        str(value).strip().upper()
        for value in (
            spec.get("actual_field"),
            spec.get("field_name"),
            spec.get("label"),
            path_parts[-1] if path_parts else None,
        )
        if str(value or "").strip()
    }


def _find_protocol_field_matches(
    protocol_specs: Dict[str, List[Dict[str, Any]]],
    protocol_name: Optional[str],
    field_name: Optional[str],
    field_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    normalized_protocol = str(protocol_name or "").strip()
    normalized_field = str(field_name or "").strip()
    normalized_path = str(field_path or "").strip()
    if "." in normalized_field:
        normalized_field = normalized_field.split(".", 1)[1].strip()
    if not normalized_protocol or (not normalized_field and not normalized_path):
        return []
    if normalized_path:
        path_key = normalized_path.upper()
        path_matches = []
        for spec in protocol_specs.get(normalized_protocol, []):
            spec_path = "/".join(str(part).strip() for part in (spec.get("path_parts") or []) if str(part).strip())
            if spec_path.upper() == path_key:
                path_matches.append(spec)
        if path_matches:
            return path_matches
    if not normalized_field:
        return []
    field_key = normalized_field.upper()
    return [
        spec
        for spec in protocol_specs.get(normalized_protocol, [])
        if field_key in _field_candidates(spec)
    ]


def _evaluate_rule_validation_result(
    rules_payload: Dict[str, Any],
    protocol_dir: Optional[Path],
) -> Dict[str, bool]:
    conversions = rules_payload.get("conversions")
    protocol_compliance = (
        isinstance(rules_payload, dict)
        and isinstance(rules_payload.get("version"), str)
        and isinstance(rules_payload.get("project_name"), str)
        and bool(rules_payload.get("project_name"))
        and isinstance(conversions, list)
        and bool(conversions)
    )
    field_legality = True
    position_accuracy = True
    conversion_logic = True
    protocol_specs = _build_protocol_spec_index(protocol_dir)

    for conversion in conversions or []:
        if not isinstance(conversion, dict):
            protocol_compliance = False
            conversion_logic = False
            continue

        conversion_name = str(conversion.get("name") or "").strip()
        conversion_mode = str(conversion.get("mode") or "").strip()
        conversion_sources = conversion.get("sources")
        conversion_target = conversion.get("target")
        conversion_rules = conversion.get("rules")
        if (
            not conversion_name
            or conversion_mode not in {"simple", "joint"}
            or not isinstance(conversion_sources, list)
            or not conversion_sources
            or not isinstance(conversion_target, dict)
            or not str(conversion_target.get("protocol") or "").strip()
            or not isinstance(conversion_rules, list)
        ):
            protocol_compliance = False

        alias_to_protocol: Dict[str, str] = {}
        for source in conversion_sources or []:
            if not isinstance(source, dict):
                protocol_compliance = False
                continue
            alias = str(source.get("alias") or "").strip()
            protocol_name = str(source.get("protocol") or "").strip()
            if not alias or not protocol_name or alias in alias_to_protocol:
                protocol_compliance = False
                continue
            alias_to_protocol[alias] = protocol_name

        only_source_protocol = next(iter(alias_to_protocol.values())) if len(alias_to_protocol) == 1 else None
        target_protocol = str((conversion_target or {}).get("protocol") or "").strip()
        seen_target_fields: set[str] = set()
        for rule in conversion_rules or []:
            if not isinstance(rule, dict):
                protocol_compliance = False
                conversion_logic = False
                continue
            target_field = str(rule.get("target_field") or "").strip()
            target_actual_field = str(rule.get("target_actual_field") or "").strip()
            target_path = str(rule.get("target_path") or "").strip()
            formula = str(rule.get("formula") or "").strip()
            rule_type = str(rule.get("rule_type") or "").strip().lower()
            source_fields = [str(item).strip() for item in (rule.get("source_fields") or []) if str(item).strip()]
            source_actual_fields = [str(item).strip() for item in (rule.get("source_actual_fields") or []) if str(item).strip()]
            source_paths = [str(item).strip() for item in (rule.get("source_paths") or []) if str(item).strip()]
            if not target_field or not formula or not rule_type:
                protocol_compliance = False
            target_identity = target_actual_field or target_path or target_field
            if target_identity in seen_target_fields:
                protocol_compliance = False
            seen_target_fields.add(target_identity)

            target_matches = _find_protocol_field_matches(
                protocol_specs,
                target_protocol,
                target_actual_field or target_field,
                field_path=target_path,
            )
            if len(target_matches) != 1:
                field_legality = False
                position_accuracy = False
            else:
                target_spec = target_matches[0]
                if not (target_spec.get("path_parts") and target_spec.get("bit_length") is not None):
                    position_accuracy = False

            for index, source_ref in enumerate(source_fields):
                alias, _, source_field = source_ref.partition(".")
                resolved_source_protocol = alias_to_protocol.get(alias.strip()) if source_field else only_source_protocol
                resolved_source_field = source_field.strip() if source_field else alias.strip()
                resolved_source_actual = ""
                if index < len(source_actual_fields):
                    actual_ref = source_actual_fields[index]
                    if "." in actual_ref:
                        _, _, resolved_source_actual = actual_ref.partition(".")
                    else:
                        resolved_source_actual = actual_ref
                resolved_source_path = source_paths[index] if index < len(source_paths) else None
                source_matches = _find_protocol_field_matches(
                    protocol_specs,
                    resolved_source_protocol,
                    resolved_source_actual or resolved_source_field,
                    field_path=resolved_source_path,
                )
                if len(source_matches) != 1:
                    field_legality = False
                    position_accuracy = False
                else:
                    source_spec = source_matches[0]
                    if not (source_spec.get("path_parts") and source_spec.get("bit_length") is not None):
                        position_accuracy = False

            if rule_type not in _SIMPLE_RULE_TYPES:
                conversion_logic = False
                continue
            if re.search(r"\bresult\b", formula, flags=re.IGNORECASE):
                conversion_logic = False
            if rule_type == "const":
                if not _NUMERIC_LITERAL_PATTERN.fullmatch(formula):
                    conversion_logic = False
            elif rule_type == "direct":
                if len(source_fields) != 1 or formula != source_fields[0]:
                    conversion_logic = False
            elif not source_fields and not _NUMERIC_LITERAL_PATTERN.fullmatch(formula):
                conversion_logic = False

    return {
        "field_legality": bool(field_legality),
        "position_accuracy": bool(position_accuracy),
        "conversion_logic": bool(conversion_logic),
        "protocol_compliance": bool(protocol_compliance),
    }


def _extract_first_int(text: str, patterns: List[str]) -> Optional[int]:
    raw_text = str(text or "")
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            continue
    return None


def _extract_first_float_and_unit(text: str, patterns: List[str]) -> Tuple[Optional[float], Optional[str]]:
    raw_text = str(text or "")
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            value = float(match.group(1))
        except (TypeError, ValueError):
            continue
        unit = None
        if match.lastindex and match.lastindex >= 2:
            unit = str(match.group(2) or "").strip() or None
        return value, unit
    return None, None


def _normalize_field_name(field_name: Optional[str]) -> Optional[str]:
    normalized = str(field_name or "").strip().strip("，。,:：")
    if not normalized:
        return None
    return normalized.upper()


def _extract_field_name_from_qa(question: str, answer: str) -> Optional[str]:
    candidates = [
        answer,
        question,
    ]
    patterns = [
        r"字段名称\s*[:：]?\s*([A-Za-z][A-Za-z0-9_./\-]*)",
        r"协议中\s*([A-Za-z][A-Za-z0-9_./\-]*)\s*字段",
        r"\b([A-Z][A-Z0-9_./\-]{2,})\b",
    ]
    for text in candidates:
        raw_text = str(text or "")
        for pattern in patterns:
            match = re.search(pattern, raw_text)
            if not match:
                continue
            normalized = _normalize_field_name(match.group(1))
            if normalized:
                return normalized
    return None


def _extract_bit_location(answer: str) -> Tuple[Optional[int], Optional[int]]:
    text = str(answer or "")
    range_match = re.search(r"位段(?:为|是)?\s*(\d+)\s*[-~～]\s*(\d+)", text, flags=re.IGNORECASE)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        return start, max(1, end - start + 1)

    bit_start = _extract_first_int(
        text,
        [
            r"起始位\s*[:：]?\s*(\d+)",
            r"start(?:_bit)?\s*[:=]?\s*(\d+)",
            r"位段(?:为|是)?\s*(\d+)",
        ],
    )
    bit_width = _extract_first_int(
        text,
        [
            r"位宽\s*[:：]?\s*(\d+)\s*位",
            r"占用\s*(\d+)\s*位",
            r"bit[_\s-]*width\s*[:=]?\s*(\d+)",
        ],
    )
    if bit_start is not None and bit_width is None and re.search(r"位段(?:为|是)?\s*\d+\b", text, flags=re.IGNORECASE):
        bit_width = 1
    return bit_start, bit_width


def _extract_range_and_unit(answer: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    text = str(answer or "")
    range_match = re.search(
        r"(?:范围|range)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)\s*([A-Za-z%°/]+)?\s*(?:到|to|TO|~|～|—|–|-)\s*([+\-]?\d+(?:\.\d+)?)\s*([A-Za-z%°/]+)?",
        text,
        flags=re.IGNORECASE,
    )
    if not range_match:
        return None, None, None

    range_min = float(range_match.group(1))
    range_max = float(range_match.group(3))
    unit = str(range_match.group(2) or range_match.group(4) or "").strip() or None
    return range_min, range_max, unit


def _looks_like_formula(text: str) -> bool:
    normalized = str(text or "").strip()
    if not normalized:
        return False
    if re.search(r"\bresult\s*=", normalized, flags=re.IGNORECASE):
        return True
    if re.search(r"\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*", normalized):
        return True
    if re.search(r"[\u4e00-\u9fff]", normalized):
        return False
    return bool(re.search(r"[A-Za-z_][A-Za-z0-9_]*\s*[*+/%-]\s*[\dA-Za-z_(]", normalized))


def _strip_formula_suffix(text: str) -> Optional[str]:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    normalized = normalized.strip("`")
    normalized = re.sub(r"[。；;]+$", "", normalized)
    return normalized or None


def extract_structured_info_from_qa_text(question: str, answer: str) -> Dict[str, Any]:
    """从规则化 QA 文本中直接提取字段参数，作为 LLM 抽取失败时的兜底。"""
    field_name = _extract_field_name_from_qa(question, answer)
    bit_start, bit_width = _extract_bit_location(answer)
    resolution, resolution_unit = _extract_first_float_and_unit(
        answer,
        [
            r"(?:分辨率|resolution)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)\s*([A-Za-z%°/]+)?",
        ],
    )
    range_min, range_max, range_unit = _extract_range_and_unit(answer)
    unit = range_unit or resolution_unit

    meaning = None
    meaning_match = re.search(r"(?:表示|用于|含义是)\s*([^。；;]+)", answer)
    if meaning_match:
        meaning = str(meaning_match.group(1) or "").strip() or None

    conversion_formula = None
    if _looks_like_formula(answer):
        conversion_formula = _strip_formula_suffix(answer)

    return {
        "field_name": field_name,
        "bit_width": bit_width,
        "bit_start": bit_start,
        "resolution": resolution,
        "unit": unit,
        "range_min": range_min,
        "range_max": range_max,
        "meaning": meaning,
        "conversion_formula": conversion_formula,
    }


def _merge_extracted_candidates(
    base: Dict[str, Any],
    extra: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in extra.items():
        if merged.get(key) in (None, "") and value not in (None, ""):
            merged[key] = value
    return merged


def extract_field_info(
    question: str,
    answer: str,
    protocol_type: str = "Link16",
    schema_context: str = "",
) -> Dict[str, Any]:
    """
    使用LLM从问答内容中抽取结构化字段信息

    Args:
        question: 问题文本
        answer: 答案文本
        protocol_type: 协议类型

    Returns:
        抽取的字段信息字典
    """
    llm = get_llm_instance()

    # 获取格式化的prompt
    system_prompt, user_prompt = PromptTemplates.format_qa_extract(
        question=question,
        answer=answer,
        protocol_type=protocol_type
    )

    if schema_context:
        user_prompt = (
            f"{user_prompt}\n\n"
            f"请额外遵循以下协议Schema约束（仅在信息明确时填充）：\n{schema_context}"
        )

    fallback_extracted = extract_structured_info_from_qa_text(question, answer)

    extracted = None
    try:
        raw_response = llm.generate(
            prompt=user_prompt,
            system_prompt=system_prompt,
            max_new_tokens=256,
            temperature=0.0,
            top_p=1.0,
            enable_thinking=False,
        )
        parser = getattr(llm, "parse_json_from_response", LocalLLM.parse_json_from_response)
        extracted = parser(raw_response, prefer=dict)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"抽取阶段LLM调用失败，转为规则兜底: {exc}")
        extracted = None
    extracted = extracted or {}

    # 确保所有字段都存在
    default_info = {
        "field_name": None,
        "bit_width": None,
        "bit_start": None,
        "resolution": None,
        "unit": None,
        "range_min": None,
        "range_max": None,
        "meaning": None,
        "conversion_formula": None,
    }

    # 合并抽取结果，处理可能的字段名变体
    result = default_info.copy()

    # 字段名映射
    field_mappings = {
        "field_name": ["field_name", "fieldName", "field", "name"],
        "bit_width": ["bit_width", "bitWidth", "bit", "width", "bits"],
        "bit_start": ["bit_start", "bitStart", "start_bit", "offset"],
        "resolution": ["resolution", "res"],
        "unit": ["unit", "units"],
        "range_min": ["range_min", "rangeMin", "min", "min_value", "minimum"],
        "range_max": ["range_max", "rangeMax", "max", "max_value", "maximum"],
        "meaning": ["meaning", "description", "desc"],
        "conversion_formula": ["conversion_formula", "formula", "expression"],
    }

    for target_key, source_keys in field_mappings.items():
        for source_key in source_keys:
            if source_key in extracted and extracted[source_key] is not None:
                result[target_key] = extracted[source_key]
                break

    result = _merge_extracted_candidates(result, fallback_extracted)

    if not any(value is not None for value in result.values()):
        result["extraction_error"] = "LLM未能返回有效的JSON结果"

    return result


def validate_extracted_info(
    extracted_info: Dict[str, Any],
    protocol_type: str = "Link16",
    message_schema: Optional[Dict[str, Any]] = None,
    message_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    根据��议类型执行规则校验

    Args:
        extracted_info: 抽取的字段信息
        protocol_type: 协议类型

    Returns:
        校验结果字典
    """
    # 获取校验规则
    rules = PromptTemplates.get_validation_rules(protocol_type)

    check_items = []
    all_passed = True

    for rule_name, rule_config in rules.items():
        try:
            passed = rule_config["check"](extracted_info)

            if passed:
                message = rule_config["pass_msg"](extracted_info)
            else:
                message = rule_config["fail_msg"]
                all_passed = False

            check_items.append({
                "rule_name": rule_name,
                "description": rule_config["description"],
                "passed": passed,
                "message": message,
                "status": "PASS" if passed else "FAIL",
                "msg": message,
            })
        except Exception as e:
            # 校验过程出错，记录但不中断
            check_items.append({
                "rule_name": rule_name,
                "description": rule_config["description"],
                "passed": False,
                "message": f"校验过程出错: {str(e)}",
                "status": "FAIL",
                "msg": f"校验过程出错: {str(e)}",
            })
            all_passed = False

    schema_check_items = validate_with_schema(extracted_info, message_schema)
    for item in schema_check_items:
        check_items.append(item)
        if not item.get("passed"):
            all_passed = False

    return {
        "passed": all_passed,
        "check_items": check_items,
        "protocol_type": protocol_type,
        "message_code": message_code,
        "schema_applied": bool(message_schema),
        "checked_at": datetime.now().isoformat()
    }


def save_extraction_result(
    qa_id: str,
    question: str,
    answer: str,
    extracted_info: Dict[str, Any],
    validation_result: Dict[str, Any],
    protocol_type: str = "Link16"
) -> bool:
    """
    保存抽取结果到数据库

    Args:
        qa_id: QA ID
        question: 问题
        answer: 答案
        extracted_info: 抽取信息
        validation_result: 校验结果
        protocol_type: 协议类型

    Returns:
        是否保存成功
    """
    try:
        db = get_db_instance()

        # 检查是否已存在
        existing_qa = db.get_qa_by_id(qa_id)

        if existing_qa:
            # 更新现有记录
            with db.connection() as conn:
                cursor = conn.cursor()
                update_query = """
                    UPDATE qa_pairs
                    SET question = %s, answer = %s, extracted_info = %s,
                        validation_result = %s, protocol_type = %s
                    WHERE qa_id = %s
                """
                update_params = (
                    question,
                    answer,
                    json.dumps(extracted_info),
                    json.dumps(validation_result),
                    protocol_type,
                    qa_id
                )
                if hasattr(db, "_execute"):
                    db._execute(cursor, update_query, update_params)
                else:
                    cursor.execute(update_query, update_params)
        else:
            # 创建新记录
            qa_pair = QAPair(
                qa_id=qa_id,
                source_block_ids=[],
                question=question,
                answer=answer,
                extracted_info=extracted_info,
                validation_result=validation_result,
                protocol_type=protocol_type
            )
            db.insert_qa(qa_pair)

        return True
    except Exception as e:
        print(f"保存抽取结果失败: {e}")
        return False


def run_extraction_pipeline(
    qa_id: str,
    question: str,
    answer: str,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    执行完整的抽取校验流程

    Args:
        qa_id: QA ID
        question: 问题文本
        answer: 答案文本
        protocol_type: 协议类型

    Returns:
        完整的处理结果
    """
    detected_message_code = message_code or guess_message_code(f"{question}\n{answer}")
    protocol_schema, resolved_message_code, message_schema = resolve_message_schema(
        protocol_type,
        detected_message_code,
    )
    schema_context = build_schema_prompt_context(
        protocol_schema,
        message_schema,
        resolved_message_code,
    )

    # Step 1: 抽取字段信息
    extracted_info = extract_field_info(
        question,
        answer,
        protocol_type,
        schema_context=schema_context,
    )

    extracted_field_name = extracted_info.get("field_name")
    if extracted_field_name:
        protocol_schema, resolved_message_code, message_schema = resolve_message_schema(
            protocol_type,
            detected_message_code,
            extracted_field_name,
        )

    # Step 2: 执行规则校验
    validation_result = validate_extracted_info(
        extracted_info,
        protocol_type,
        message_schema=message_schema,
        message_code=resolved_message_code,
    )

    # Step 3: 保存结果
    save_success = save_extraction_result(
        qa_id=qa_id,
        question=question,
        answer=answer,
        extracted_info=extracted_info,
        validation_result=validation_result,
        protocol_type=protocol_type
    )

    return {
        "qa_id": qa_id,
        "message_code": resolved_message_code,
        "schema_applied": bool(message_schema),
        "extracted_info": extracted_info,
        "validation_result": validation_result,
        "save_success": save_success
    }


@app.route("/api/knowledge/extract_validate_qa", methods=["POST"])
def extract_validate_qa():
    """
    QA字段智能抽取与规则校验接口

    输入参数:
    {
        "qa_id": "qa_2024",
        "question": "J12.0协议中LATITUDE字段参数详情？",
        "answer": "LATITUDE字段长度23位，分辨率0.0013分，范围-90到+90度。",
        "protocol_type": "Link16"
    }

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "qa_id": "qa_2024",
            "extracted_info": {...},
            "validation_result": {...}
        }
    }
    """
    try:
        data = request.json

        # 参数校验
        if not data:
            return jsonify({
                "code": 400,
                "message": "请求体不能为空",
                "data": None
            }), 400

        if bool(data.get("batch")):
            if "items" not in data:
                return jsonify({
                    "code": 400,
                    "message": "batch=true 时必须包含 items 数组",
                    "data": None
                }), 400
            return _handle_batch_extract_validate_payload(data)

        qa_id = data.get("qa_id")
        question = data.get("question", "")
        answer = data.get("answer", "")
        protocol_type = data.get("protocol_type", "Link16")
        message_code = data.get("message_code")

        if not qa_id:
            return jsonify({
                "code": 400,
                "message": "qa_id 参数必填",
                "data": None
            }), 400

        if not question or not answer:
            return jsonify({
                "code": 400,
                "message": "question 和 answer 参数不能为空",
                "data": None
            }), 400

        # 执行抽取校验流程
        result = run_extraction_pipeline(
            qa_id=qa_id,
            question=question,
            answer=answer,
            protocol_type=protocol_type,
            message_code=message_code,
        )

        return jsonify({
            "code": 200,
            "message": "success",
            "data": result
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"处理失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/knowledge/extract_validate_qa/batch", methods=["POST"])
def extract_validate_qa_batch():
    """
    批量QA字段智能抽取与规则校验接口

    输入参数:
    {
        "items": [
            {
                "qa_id": "qa_001",
                "question": "...",
                "answer": "...",
                "protocol_type": "Link16"
            },
            ...
        ]
    }

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "total": 10,
            "success": 8,
            "failed": 2,
            "results": [...]
        }
    }
    """
    try:
        data = request.json

        if not data or "items" not in data:
            return jsonify({
                "code": 400,
                "message": "请求体必须包含 items 数组",
                "data": None
            }), 400

        return _handle_batch_extract_validate_payload(data)

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"处理失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/knowledge/extract_validate_qa/<qa_id>", methods=["GET"])
def get_extraction_result(qa_id: str):
    """
    获取已有的抽取校验结果

    路径参数:
        qa_id: QA ID

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "qa_id": "qa_2024",
            "question": "...",
            "answer": "...",
            "extracted_info": {...},
            "validation_result": {...}
        }
    }
    """
    try:
        db = get_db_instance()
        qa_pair = db.get_qa_by_id(qa_id)

        if not qa_pair:
            return jsonify({
                "code": 404,
                "message": f"未找到 qa_id={qa_id} 的记录",
                "data": None
            }), 404

        return jsonify({
            "code": 200,
            "message": "success",
            "data": qa_pair.to_dict()
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"查询失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/knowledge/validation_rules", methods=["GET"])
def get_validation_rules():
    """
    获取可用的校验规则列表

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "rules": [
                {
                    "name": "RangeCoverageCheck",
                    "description": "量程覆盖校验"
                },
                ...
            ]
        }
    }
    """
    try:
        protocol_type = request.args.get("protocol_type", "Link16")
        rules = PromptTemplates.get_validation_rules(protocol_type)

        rule_list = [
            {
                "name": name,
                "description": config["description"]
            }
            for name, config in rules.items()
        ]

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "protocol_type": protocol_type,
                "rules": rule_list
            }
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"获取规则失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/knowledge/protocol_convert", methods=["POST"])
def protocol_convert():
    """执行协议转换，支持字段转义与字段转换两类公式。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        source_message = data.get("source_message")
        if not source_message:
            return jsonify({
                "code": 400,
                "message": "source_message不能为空",
                "data": None,
            }), 400

        result = execute_protocol_conversion(
            source_message=source_message,
            llm_formula_output=data.get("llm_formula_output"),
            protocol_type=data.get("protocol_type", "Link16"),
            message_code=data.get("message_code"),
            use_knowledge_base=bool(data.get("use_knowledge_base", True)),
        )

        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"知识库文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"协议转换失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/data/build_protocol_doc_index", methods=["POST"])
def build_protocol_doc_index_route():
    """基于训练阶段文档块建立可复用的协议文档索引。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        project_id = str(data.get("project_id") or "").strip()
        dataset_id = str(data.get("dataset_id") or "").strip()
        if not project_id:
            return jsonify({
                "code": 400,
                "message": "project_id不能为空",
                "data": None,
            }), 400

        db = get_db_instance()
        source_block_ids = data.get("source_block_ids") or []
        if source_block_ids:
            blocks = db.get_blocks_by_ids(source_block_ids)
        else:
            blocks = db.get_blocks_by_project(project_id)

        result = build_protocol_doc_index(
            project_id=project_id,
            dataset_id=dataset_id,
            blocks=blocks,
            protocol_type=str(data.get("protocol_type") or "").strip(),
            message_codes=data.get("message_codes"),
            file_names=data.get("file_names"),
            source_block_ids=source_block_ids,
            doc_set_id=str(data.get("doc_set_id") or "").strip(),
            index_ref=str(data.get("index_ref") or "").strip(),
            tags=data.get("tags"),
            rebuild=bool(data.get("rebuild", False)),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except ValueError as exc:
        return jsonify({
            "code": 400,
            "message": str(exc),
            "data": None,
        }), 400
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"协议文档索引构建失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_generate_rules", methods=["POST"])
def protocol_generate_rules():
    """基于原/目标协议定义生成目标协议字段规则。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        normalized_payload = _merge_protocol_request_payload(data)
        result = generate_protocol_field_rules(
            source_protocol=normalized_payload.get("source_protocol"),
            target_protocol=normalized_payload.get("target_protocol"),
            source_message=normalized_payload.get("source_message"),
            use_knowledge_base=True,
            use_page_index=True,
            use_trained_docs=bool(normalized_payload.get("use_trained_docs", False)),
            project_id=str(normalized_payload.get("project_id") or "").strip(),
            dataset_id=str(normalized_payload.get("dataset_id") or "").strip(),
            doc_set_id=str(normalized_payload.get("doc_set_id") or "").strip(),
            index_ref=str(normalized_payload.get("index_ref") or "").strip(),
            max_empty_rule_retries=int(data.get("max_empty_rule_retries", 3)),
            required_target_fields=resolve_protocol_field_specs(
                normalized_payload.get("target_protocol_dir"),
                "target_protocol_dir",
            ) if normalized_payload.get("target_protocol_dir") else None,
        )
        source_protocol_dir = normalized_payload.get("source_protocol_dirs")
        if source_protocol_dir is None:
            source_protocol_dir = str(normalized_payload.get("source_protocol_dir") or "").strip()
        target_protocol_dir = str(normalized_payload.get("target_protocol_dir") or "").strip()
        protocol_dir = _merge_protocol_dirs(source_protocol_dir, target_protocol_dir)
        source_protocol_name = None
        source_protocol_meta = normalized_payload.get("source_protocol")
        if isinstance(source_protocol_meta, dict):
            source_protocol_name = _resolve_protocol_name(source_protocol_meta)
        target_protocol_name = _resolve_protocol_name(normalized_payload.get("target_protocol"))
        concept_rules_payload = {
            "normalized_rules": list(result.get("concept_normalized_rules") or result.get("normalized_rules") or []),
        }
        rules_payload = build_generator_rules_payload(
            raw_rules=concept_rules_payload,
            protocol_dir=protocol_dir,
            target_protocol_name=target_protocol_name,
            source_protocol_name=source_protocol_name,
            project_name=str(normalized_payload.get("project_name") or "").strip() or None,
            preserve_display_names=True,
        )
        rules_json_path = _save_protocol_rules_file(normalized_payload, concept_rules_payload)
        kg_writeback_payload = dict(result.get("kg_writeback_payload") or {})
        if kg_writeback_payload:
            kg_writeback_payload["rules"] = _displayize_rule_records(
                kg_writeback_payload.get("rules"),
                protocol_dir=protocol_dir,
                target_protocol_name=target_protocol_name,
                source_protocol_name=source_protocol_name,
            )
            kg_writeback_payload["rules"] = _filter_display_writeback_rules(
                kg_writeback_payload.get("rules"),
            )
            for rule_item in kg_writeback_payload["rules"]:
                if isinstance(rule_item, dict):
                    rule_item.pop("formula_kind", None)
        validation_result = _evaluate_rule_validation_result(
            rules_payload=rules_payload,
            protocol_dir=protocol_dir,
        )
        result_summary = dict(result.get("summary") or {})
        response_payload = {
            "conversion_rules_json": rules_json_path,
            "validation_result": validation_result,
            "kg_writeback_payload": kg_writeback_payload,
            "summary": {
                "knowledge_graph_field_count": int(result_summary.get("knowledge_graph_rule_count") or 0),
                "llm_converted_field_count": int(result_summary.get("llm_rule_count") or 0),
            },
        }
        return jsonify({
            "code": 200,
            "message": "success",
            "data": response_payload,
        })
    except ValueError as exc:
        return jsonify({
            "code": 400,
            "message": str(exc),
            "data": None,
        }), 400
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"协议规则生成失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/code_generation/generate", methods=["POST"])
def code_generation_generate():
    """基于协议 XML、转换规则和端口配置生成 Qt/C++ 协议转换工程。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        source_protocol_dirs = data.get("source_protocol_dirs")
        if source_protocol_dirs is None:
            source_protocol_dirs = data.get("source_protocol_dir")
        target_protocol_dir = data.get("target_protocol_dir")
        conversion_rules_json = data.get("conversion_rules_json")
        port_config_json = data.get("port_config_json")
        output_dir = data.get("output_dir")

        if not source_protocol_dirs:
            return jsonify({
                "code": 400,
                "message": "source_protocol_dirs不能为空",
                "data": None,
            }), 400
        if not target_protocol_dir:
            return jsonify({
                "code": 400,
                "message": "target_protocol_dir不能为空",
                "data": None,
            }), 400
        if conversion_rules_json is None:
            return jsonify({
                "code": 400,
                "message": "conversion_rules_json不能为空",
                "data": None,
            }), 400
        if port_config_json is None:
            return jsonify({
                "code": 400,
                "message": "port_config_json不能为空",
                "data": None,
            }), 400
        if not output_dir:
            return jsonify({
                "code": 400,
                "message": "output_dir不能为空",
                "data": None,
            }), 400

        response_payload = build_code_generation_payload(
            source_protocol_dir=source_protocol_dirs,
            target_protocol_dir=target_protocol_dir,
            conversion_rules_json=conversion_rules_json,
            conversion_matrix_json=data.get("conversion_matrix_json"),
            port_config_json=port_config_json,
            output_dir=output_dir,
            target_protocol_name=str(data.get("target_protocol_name") or "").strip() or None,
            project_name=str(data.get("project_name") or "").strip() or None,
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": response_payload,
        })
    except ValueError as exc:
        return jsonify({
            "code": 400,
            "message": str(exc),
            "data": None,
        }), 400
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"代码生成失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_convert_bundle", methods=["POST"])
def protocol_convert_bundle():
    """先生成规则，再执行整包协议转换。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        source_message = data.get("source_message")
        if not source_message:
            return jsonify({
                "code": 400,
                "message": "source_message不能为空",
                "data": None,
            }), 400

        result = generate_and_convert_protocol_bundle(
            source_protocol=data.get("source_protocol"),
            target_protocol=data.get("target_protocol"),
            source_message=source_message,
            use_knowledge_base=bool(data.get("use_knowledge_base", True)),
            use_page_index=bool(data.get("use_page_index", False)),
            use_trained_docs=bool(data.get("use_trained_docs", False)),
            project_id=str(data.get("project_id") or "").strip(),
            dataset_id=str(data.get("dataset_id") or "").strip(),
            doc_set_id=str(data.get("doc_set_id") or "").strip(),
            index_ref=str(data.get("index_ref") or "").strip(),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except ValueError as exc:
        return jsonify({
            "code": 400,
            "message": str(exc),
            "data": None,
        }), 400
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"知识库文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"整包协议转换失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_rule_validate", methods=["POST"])
def protocol_rule_validate():
    """校验协议转换规则，补齐量纲/位宽/映射合法性检查。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        result = validate_protocol_rules(
            llm_formula_output=data.get("llm_formula_output"),
            protocol_type=data.get("protocol_type", "Link16"),
            message_code=data.get("message_code"),
            source_message=data.get("source_message"),
            source_fields=data.get("source_fields"),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"知识库文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"规则校验失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_rule_export", methods=["POST"])
def protocol_rule_export():
    """导出标准化协议转换规则，支持 JSON/YAML 与差异对比。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        result = export_protocol_rules(
            llm_formula_output=data.get("llm_formula_output"),
            protocol_type=data.get("protocol_type", "Link16"),
            message_code=data.get("message_code"),
            export_format=data.get("export_format", "json"),
            compare_with_knowledge_base=bool(data.get("compare_with_knowledge_base", False)),
            baseline_rules=data.get("baseline_rules"),
            source_fields=data.get("source_fields"),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"知识库文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"规则导出失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_rules/manual_writeback", methods=["POST"])
def protocol_rules_manual_writeback():
    """人工审核通过后的规则写回知识图谱。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        protocol_type = str(data.get("protocol_type") or "").strip()
        target_protocol_type = str(data.get("target_protocol_type") or "").strip()
        if not protocol_type:
            return jsonify({
                "code": 400,
                "message": "protocol_type不能为空",
                "data": None,
            }), 400
        if not target_protocol_type:
            return jsonify({
                "code": 400,
                "message": "target_protocol_type不能为空",
                "data": None,
            }), 400

        normalized_rules = _normalize_manual_writeback_rules(data.get("rules"))
        knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)

        existing_rules = knowledge_base.list_rules(
            message_code=data.get("source_message_code"),
            target_protocol_type=target_protocol_type,
            target_message_code=data.get("target_message_code"),
        )
        existing_signatures = {
            (
                str(rule.target_field or "").strip().upper(),
                tuple(str(item or "").strip().upper() for item in (rule.source_fields or [])),
                str(rule.formula or "").strip(),
            )
            for rule in existing_rules
        }

        written_rules = knowledge_base.upsert_generated_rules(
            normalized_rules,
            protocol_type=protocol_type,
            message_code=data.get("source_message_code"),
            target_protocol_type=target_protocol_type,
            target_message_code=data.get("target_message_code"),
            source="manual_review",
        )

        created_count = 0
        updated_count = 0
        results: List[Dict[str, Any]] = []
        for rule in written_rules:
            signature = (
                str(rule.target_field or "").strip().upper(),
                tuple(str(item or "").strip().upper() for item in (rule.source_fields or [])),
                str(rule.formula or "").strip(),
            )
            action = "updated" if signature in existing_signatures else "created"
            if action == "created":
                created_count += 1
            else:
                updated_count += 1
            results.append(
                {
                    "target_field": rule.target_field,
                    "source_fields": list(rule.source_fields or []),
                    "formula": rule.formula,
                    "status": rule.status,
                    "source": rule.source,
                    "rule_id": rule.edge_id,
                    "action": action,
                }
            )

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "total": len(normalized_rules),
                "written": len(written_rules),
                "created": created_count,
                "updated": updated_count,
                "knowledge_graph": knowledge_base.to_summary(),
                "results": results,
            },
        })
    except ValueError as exc:
        return jsonify({
            "code": 400,
            "message": str(exc),
            "data": None,
        }), 400
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"知识图谱文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"人工审核规则写回失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_conversion_evaluate", methods=["POST"])
def protocol_conversion_evaluate():
    """评估协议转换正确性，支持 embedding/reranker 与降级策略。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        converted_message = data.get("converted_message")
        reference_message = data.get("reference_message")
        if not converted_message:
            return jsonify({
                "code": 400,
                "message": "converted_message不能为空",
                "data": None,
            }), 400
        if not reference_message:
            return jsonify({
                "code": 400,
                "message": "reference_message不能为空",
                "data": None,
            }), 400

        use_model_inference = data.get("use_model_inference")
        allow_modelscope_download = data.get("allow_modelscope_download")
        result = evaluate_protocol_conversion(
            converted_message=converted_message,
            reference_message=reference_message,
            protocol_type=data.get("protocol_type", "Link16"),
            message_code=data.get("message_code"),
            source_message=data.get("source_message"),
            field_weights=data.get("field_weights"),
            trace_id=data.get("trace_id"),
            use_model_inference=True if use_model_inference is None else bool(use_model_inference),
            allow_modelscope_download=True if allow_modelscope_download is None else bool(allow_modelscope_download),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"评估模型文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"协议转换正确性评估失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/api/knowledge/protocol_conversion_rate_evaluate", methods=["POST"])
def protocol_conversion_rate_evaluate():
    """评估协议转换率，输出覆盖率、成功率与低损失率等聚合指标。"""
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({
                "code": 400,
                "message": "请求体必须是JSON对象",
                "data": None,
            }), 400

        converted_message = data.get("converted_message")
        reference_message = data.get("reference_message")
        if not converted_message:
            return jsonify({
                "code": 400,
                "message": "converted_message不能为空",
                "data": None,
            }), 400
        if not reference_message:
            return jsonify({
                "code": 400,
                "message": "reference_message不能为空",
                "data": None,
            }), 400

        use_model_inference = data.get("use_model_inference")
        allow_modelscope_download = data.get("allow_modelscope_download")
        result = evaluate_protocol_conversion_rate(
            converted_message=converted_message,
            reference_message=reference_message,
            protocol_type=data.get("protocol_type", "Link16"),
            message_code=data.get("message_code"),
            source_message=data.get("source_message"),
            field_weights=data.get("field_weights"),
            field_scores=data.get("field_scores"),
            correctness_result=data.get("correctness_result") if isinstance(data.get("correctness_result"), dict) else None,
            trace_id=data.get("trace_id"),
            use_model_inference=True if use_model_inference is None else bool(use_model_inference),
            allow_modelscope_download=True if allow_modelscope_download is None else bool(allow_modelscope_download),
            confidence_threshold=float(data.get("confidence_threshold", 80.0)),
            low_loss_threshold=float(data.get("low_loss_threshold", 20.0)),
        )
        return jsonify({
            "code": 200,
            "message": "success",
            "data": result,
        })
    except FileNotFoundError as exc:
        return jsonify({
            "code": 404,
            "message": f"评估模型文件不存在: {str(exc)}",
            "data": None,
        }), 404
    except Exception as exc:
        return jsonify({
            "code": 500,
            "message": f"协议转换率评估失败: {str(exc)}",
            "data": None,
        }), 500


@app.route("/health", methods=["GET"])
def health():
    """健康检查接口"""
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=True)
