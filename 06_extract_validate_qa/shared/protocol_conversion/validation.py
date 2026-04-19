from __future__ import annotations

import ast
import re
from typing import Any, Dict, Iterable, List, Optional

from .converter import (
    ConversionRule,
    _evaluate_arithmetic_formula,
    _parse_mapping_formula,
    normalize_source_message,
    parse_llm_formula_output,
)
from .knowledge_base import ProtocolConversionKnowledgeBase


UNIT_FAMILY_MAP = {
    "degree": "angle",
    "deg": "angle",
    "min": "angle",
    "minute": "angle",
    "rad": "angle",
    "radian": "angle",
    "ft": "length",
    "feet": "length",
    "m": "length",
    "meter": "length",
    "km": "length",
    "nm": "length",
    "knot": "speed",
    "kt": "speed",
    "m/s": "speed",
    "fps": "speed",
    "s": "time",
    "sec": "time",
    "ms": "time",
    "label": "categorical",
    "enum": "categorical",
}
BITS_CALL_PATTERN = re.compile(r"\b(?:signed|unsigned)\s*\(")


def _normalize_unit(unit: Optional[str]) -> Optional[str]:
    normalized = str(unit or "").strip().lower()
    return normalized or None


def _unit_family(unit: Optional[str]) -> Optional[str]:
    normalized = _normalize_unit(unit)
    if not normalized:
        return None
    return UNIT_FAMILY_MAP.get(normalized, normalized)


def _normalize_formula(formula: str) -> str:
    return re.sub(r"\s+", "", str(formula or "")).lower()


def _build_check(name: str, passed: bool, severity: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {
        "check_name": name,
        "passed": passed,
        "severity": severity,
        "message": message,
    }
    if details:
        payload["details"] = details
    return payload


def _resolve_bit_length(rule: ConversionRule, protocol_type: str, message_code: Optional[str]) -> Optional[int]:
    if rule.bit_length:
        return int(rule.bit_length)
    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    kb_rule = knowledge_base.find_rule(rule.field_name, message_code=message_code)
    if kb_rule and kb_rule.bit_length:
        return int(kb_rule.bit_length)
    return None


def _validate_mapping_rule(rule: ConversionRule) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    range_rules, exact_rules = _parse_mapping_formula(rule.formula)
    has_rule = bool(range_rules or exact_rules)
    checks.append(
        _build_check(
            name="MappingSyntax",
            passed=has_rule,
            severity="error",
            message="映射公式可解析" if has_rule else "映射公式未解析出任何精确值或区间规则",
        )
    )
    overlaps: List[Dict[str, float]] = []
    ordered = sorted(range_rules, key=lambda item: (item[0], item[1]))
    for index in range(len(ordered) - 1):
        left = ordered[index]
        right = ordered[index + 1]
        if left[1] >= right[0]:
            overlaps.append({"left_start": left[0], "left_end": left[1], "right_start": right[0], "right_end": right[1]})
    checks.append(
        _build_check(
            name="MappingRangeOverlap",
            passed=not overlaps,
            severity="error",
            message="映射区间无重叠" if not overlaps else "映射区间存在重叠",
            details={"overlaps": overlaps} if overlaps else None,
        )
    )
    checks.append(
        _build_check(
            name="MappingUnitConvention",
            passed=not _normalize_unit(rule.unit),
            severity="warning",
            message="枚举映射未声明物理单位" if not _normalize_unit(rule.unit) else "枚举映射通常不建议声明物理单位",
            details={"unit": rule.unit} if rule.unit else None,
        )
    )
    return checks


def _validate_transcoding_rule(
    rule: ConversionRule,
    protocol_type: str,
    message_code: Optional[str],
    source_value: Any,
    source_message: Dict[str, Any],
) -> List[Dict[str, Any]]:
    checks: List[Dict[str, Any]] = []
    try:
        ast.parse(rule.formula, mode="eval")
        checks.append(_build_check("ExpressionSyntax", True, "error", "算术表达式语法合法"))
    except SyntaxError as exc:
        checks.append(
            _build_check(
                "ExpressionSyntax",
                False,
                "error",
                f"算术表达式语法错误: {exc.msg}",
            )
        )
        return checks

    resolved_bit_length = _resolve_bit_length(rule, protocol_type, message_code)
    needs_bits = bool(BITS_CALL_PATTERN.search(rule.formula) or re.search(r"\bbits\b", rule.formula))
    checks.append(
        _build_check(
            "BitLengthResolved",
            passed=(not needs_bits) or bool(resolved_bit_length),
            severity="error",
            message=(
                f"位宽已解析: {resolved_bit_length}" if ((not needs_bits) or resolved_bit_length) else "公式依赖 bits/signed/unsigned，但未提供 bit_length"
            ),
            details={"bit_length": resolved_bit_length},
        )
    )

    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    kb_rule = knowledge_base.find_rule(rule.field_name, message_code=message_code)
    expected_unit = rule.unit or (kb_rule.unit if kb_rule else None)
    expected_family = _unit_family(expected_unit)
    checks.append(
        _build_check(
            "DimensionAnalysis",
            passed=expected_family != "categorical",
            severity="warning",
            message=(
                f"规则量纲类型识别为 {expected_family or 'unknown'}"
                if expected_family != "categorical"
                else "transcoding 规则不应输出离散枚举量纲"
            ),
            details={"unit": expected_unit, "family": expected_family},
        )
    )

    if source_value is not None:
        preview_rule = ConversionRule(
            field_name=rule.field_name,
            source_fields=list(rule.source_fields),
            conversion_mode=rule.conversion_mode,
            formula=rule.formula,
            target_field=rule.target_field,
            unit=rule.unit,
            bit_length=resolved_bit_length,
            source=rule.source,
            description=rule.description,
        )
        try:
            preview_value = _evaluate_arithmetic_formula(rule.formula, source_value, preview_rule, source_message)
            checks.append(
                _build_check(
                    "ExecutionPreview",
                    True,
                    "error",
                    "样例值预执行成功",
                    details={"source_value": source_value, "preview_value": preview_value},
                )
            )
        except Exception as exc:
            checks.append(
                _build_check(
                    "ExecutionPreview",
                    False,
                    "error",
                    f"样例值预执行失败: {exc}",
                    details={"source_value": source_value},
                )
            )
    else:
        checks.append(_build_check("ExecutionPreview", True, "info", "未提供样例值，跳过预执行"))
    return checks


def _validate_rule(
    rule: ConversionRule,
    protocol_type: str,
    message_code: Optional[str],
    source_message: Dict[str, Any],
) -> Dict[str, Any]:
    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    kb_rule = knowledge_base.find_rule(rule.field_name, message_code=message_code)
    checks = [
        _build_check(
            "ModeSupported",
            passed=rule.conversion_mode in {"transcoding", "mapping"},
            severity="error",
            message=f"支持的规则模式: {rule.conversion_mode}" if rule.conversion_mode in {"transcoding", "mapping"} else f"不支持的规则模式: {rule.conversion_mode}",
        ),
        _build_check(
            "KnowledgeBaseAnchor",
            passed=kb_rule is not None,
            severity="warning",
            message="知识库中存在同名锚点规则" if kb_rule else "知识库中未找到同名锚点规则，将仅按LLM公式执行",
            details={"message_code": kb_rule.message_code if kb_rule else None},
        ),
    ]
    source_value = source_message.get(rule.field_name)
    if rule.conversion_mode == "mapping":
        checks.extend(_validate_mapping_rule(rule))
    else:
        checks.extend(_validate_transcoding_rule(rule, protocol_type, message_code, source_value, source_message))

    failed = [item for item in checks if item["severity"] == "error" and not item["passed"]]
    warnings = [item for item in checks if item["severity"] == "warning" and not item["passed"]]
    return {
        "field_name": rule.field_name,
        "target_field": rule.target_field or rule.field_name,
        "conversion_mode": rule.conversion_mode,
        "formula": rule.formula,
        "passed": not failed,
        "warning_count": len(warnings),
        "error_count": len(failed),
        "source_value": source_value,
        "checks": checks,
    }


def validate_protocol_rules(
    llm_formula_output: Any,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
    source_message: Optional[Any] = None,
    source_fields: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Validate protocol conversion rules before execution or export."""
    normalized_source = normalize_source_message(source_message or {})
    source_field_names = list(source_fields or normalized_source.keys())
    rules = parse_llm_formula_output(llm_formula_output, source_fields=source_field_names)
    results = [_validate_rule(rule, protocol_type, message_code, normalized_source) for rule in rules]
    passed_rules = sum(1 for item in results if item["passed"])
    warning_rules = sum(1 for item in results if item["warning_count"] > 0)
    failed_rules = len(results) - passed_rules
    return {
        "protocol_type": protocol_type,
        "message_code": str(message_code or "").strip().upper() or None,
        "normalized_source_message": normalized_source,
        "normalized_rules": [rule.to_dict() for rule in rules],
        "validation_results": results,
        "summary": {
            "total_rules": len(results),
            "passed_rules": passed_rules,
            "failed_rules": failed_rules,
            "warning_rules": warning_rules,
        },
    }
