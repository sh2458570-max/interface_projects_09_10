from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

import yaml

from .converter import ConversionRule, parse_llm_formula_output
from .knowledge_base import KnowledgeRule, ProtocolConversionKnowledgeBase


def _rule_key(rule: Dict[str, Any]) -> str:
    message_code = str(rule.get("message_code") or "").strip().upper()
    field_name = str(rule.get("field_name") or "").strip().upper()
    return f"{message_code}:{field_name}"


def _rule_to_payload(rule: ConversionRule, message_code: Optional[str] = None) -> Dict[str, Any]:
    payload = rule.to_dict()
    payload["message_code"] = str(message_code or "").strip().upper() or None
    return payload


def _knowledge_rule_to_payload(rule: KnowledgeRule) -> Dict[str, Any]:
    return rule.to_dict()


def serialize_rule_package(
    protocol_type: str,
    message_code: Optional[str],
    rules: Iterable[ConversionRule],
    export_format: str = "json",
    knowledge_base: Optional[ProtocolConversionKnowledgeBase] = None,
) -> Dict[str, Any]:
    """Serialize normalized rules into a JSON or YAML rule package."""
    normalized_rules = [_rule_to_payload(rule, message_code=message_code) for rule in rules]
    payload = {
        "protocol_type": protocol_type,
        "message_code": str(message_code or "").strip().upper() or None,
        "embedding_model": (knowledge_base.embedding_model if knowledge_base else "qwen3-0.6b-embedding"),
        "version": (knowledge_base.version if knowledge_base else "draft"),
        "rules": normalized_rules,
    }
    normalized_format = str(export_format or "json").strip().lower()
    if normalized_format == "yaml":
        serialized = yaml.safe_dump(payload, allow_unicode=True, sort_keys=False)
    else:
        normalized_format = "json"
        serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    return {
        "export_format": normalized_format,
        "payload": payload,
        "serialized_text": serialized,
    }


def diff_rule_sets(current_rules: List[Dict[str, Any]], baseline_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compare two normalized rule sets and summarize added/removed/modified fields."""
    current_index = {_rule_key(rule): rule for rule in current_rules}
    baseline_index = {_rule_key(rule): rule for rule in baseline_rules}

    added = []
    removed = []
    modified = []
    unchanged = []

    for key in sorted(set(current_index) | set(baseline_index)):
        current = current_index.get(key)
        baseline = baseline_index.get(key)
        if current and not baseline:
            added.append(current)
            continue
        if baseline and not current:
            removed.append(baseline)
            continue
        assert current is not None and baseline is not None
        changed_fields = {}
        for field in ["conversion_mode", "formula", "target_field", "unit", "bit_length"]:
            if current.get(field) != baseline.get(field):
                changed_fields[field] = {
                    "current": current.get(field),
                    "baseline": baseline.get(field),
                }
        if changed_fields:
            modified.append(
                {
                    "field_name": current.get("field_name"),
                    "message_code": current.get("message_code") or baseline.get("message_code"),
                    "changes": changed_fields,
                }
            )
        else:
            unchanged.append(current)

    return {
        "added_count": len(added),
        "removed_count": len(removed),
        "modified_count": len(modified),
        "unchanged_count": len(unchanged),
        "added": added,
        "removed": removed,
        "modified": modified,
    }


def export_protocol_rules(
    llm_formula_output: Any,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
    export_format: str = "json",
    compare_with_knowledge_base: bool = False,
    baseline_rules: Optional[Any] = None,
    source_fields: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Export LLM protocol rules and optionally diff them against a baseline."""
    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    rules = parse_llm_formula_output(llm_formula_output, source_fields=source_fields)
    export_result = serialize_rule_package(
        protocol_type=protocol_type,
        message_code=message_code,
        rules=rules,
        export_format=export_format,
        knowledge_base=knowledge_base,
    )

    baseline_payloads: List[Dict[str, Any]] = []
    if baseline_rules is not None:
        parsed_baseline = parse_llm_formula_output(baseline_rules, source_fields=source_fields)
        baseline_payloads = [_rule_to_payload(rule, message_code=message_code) for rule in parsed_baseline]
    elif compare_with_knowledge_base:
        kb_rules = knowledge_base.list_rules(message_code=message_code, field_names=[rule.field_name for rule in rules])
        baseline_payloads = [_knowledge_rule_to_payload(rule) for rule in kb_rules]

    diff_summary = diff_rule_sets(export_result["payload"]["rules"], baseline_payloads) if baseline_payloads else None
    return {
        "protocol_type": protocol_type,
        "message_code": str(message_code or "").strip().upper() or None,
        "knowledge_base": knowledge_base.to_summary(),
        "export_format": export_result["export_format"],
        "rule_count": len(export_result["payload"]["rules"]),
        "exported_rules": export_result["payload"]["rules"],
        "exported_text": export_result["serialized_text"],
        "diff_summary": diff_summary,
    }
