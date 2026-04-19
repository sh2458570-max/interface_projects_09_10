"""Protocol schema loading and schema-based validation helpers."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_DIR = ROOT_DIR / "data" / "protocol_schemas"
FALLBACK_SCHEMA_KEY = "default"
TEMPLATE_PACK_DIR = "template_packs"

PROTOCOL_ALIASES = {
    "link16": "link16",
    "link_16": "link16",
    "link-16": "link16",
}


def normalize_protocol_type(protocol_type: Optional[str]) -> str:
    """Normalize protocol type into a schema key."""
    raw = (protocol_type or "").strip().lower()
    if not raw:
        return "link16"
    compact = raw.replace(" ", "").replace("-", "").replace("_", "")
    if compact == "link16":
        return "link16"
    return PROTOCOL_ALIASES.get(raw, compact)


def _schema_dir() -> Path:
    return Path(os.getenv("PROTOCOL_SCHEMA_DIR", str(DEFAULT_SCHEMA_DIR)))


def _schema_file_path(schema_key: str) -> Path:
    return _schema_dir() / f"{schema_key}.json"


def _template_pack_file_path(pack_name: str) -> Path:
    normalized = str(pack_name).strip()
    filename = normalized if normalized.endswith(".json") else f"{normalized}.json"
    return _schema_dir() / TEMPLATE_PACK_DIR / filename


def _schema_candidates(protocol_type: Optional[str]) -> List[Path]:
    key = normalize_protocol_type(protocol_type)
    candidates = [_schema_file_path(key)]
    fallback = _schema_file_path(FALLBACK_SCHEMA_KEY)
    if key != FALLBACK_SCHEMA_KEY and fallback not in candidates:
        candidates.append(fallback)
    return candidates


def _load_schema_file(file_path: Path) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {}
    return data


@lru_cache(maxsize=16)
def _load_template_pack(pack_name: str) -> Dict[str, Any]:
    if not pack_name:
        return {}
    file_path = _template_pack_file_path(pack_name)
    if not file_path.exists():
        return {}
    return _load_schema_file(file_path)


@lru_cache(maxsize=32)
def load_protocol_schema(protocol_type: Optional[str]) -> Dict[str, Any]:
    """Load protocol schema, with fallback to default.json when missing."""
    for file_path in _schema_candidates(protocol_type):
        if not file_path.exists():
            continue
        data = _load_schema_file(file_path)
        if data:
            return data
    return {}


def guess_message_code(text: str) -> Optional[str]:
    """Guess message code like J12.0 from free text."""
    if not text:
        return None
    match = re.search(r"(J\d+\.\d+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).upper()


def _derive_message_family(message_code: Optional[str]) -> Optional[str]:
    if not message_code:
        return None
    normalized = str(message_code).strip().upper()
    if not normalized:
        return None
    match = re.match(r"([A-Z]+\d+)", normalized)
    if not match:
        return None
    return match.group(1)


def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_dict(existing, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _get_templates(protocol_schema: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}

    template_pack_name = protocol_schema.get("template_pack")
    if isinstance(template_pack_name, str) and template_pack_name.strip():
        pack_data = _load_template_pack(template_pack_name.strip())
        pack_templates = pack_data.get("templates")
        if isinstance(pack_templates, dict):
            for key, value in pack_templates.items():
                if not isinstance(value, dict):
                    continue
                result[str(key)] = deepcopy(value)

    templates = protocol_schema.get("templates")
    if isinstance(templates, dict):
        for key, value in templates.items():
            if not isinstance(value, dict):
                continue
            result[str(key)] = deepcopy(value)

    template_overrides = protocol_schema.get("template_overrides")
    if isinstance(template_overrides, dict):
        for template_name, override in template_overrides.items():
            if not isinstance(override, dict):
                continue
            key = str(template_name)
            base = result.get(key, {})
            if not isinstance(base, dict):
                base = {}
            result[key] = _merge_dict(base, override)

    return result


def _match_template_name(templates: Dict[str, Dict[str, Any]], template_name: Optional[str]) -> Optional[str]:
    if not template_name:
        return None
    if template_name in templates:
        return template_name

    lowered = str(template_name).strip().lower()
    for key in templates:
        if key.lower() == lowered:
            return key
    return None


def _resolve_template_name(
    protocol_schema: Dict[str, Any],
    message_code: Optional[str],
) -> Optional[str]:
    templates = _get_templates(protocol_schema)
    if not templates:
        return None

    normalized_code = str(message_code).upper() if message_code else None

    message_mapping = protocol_schema.get("message_template_mapping")
    if isinstance(message_mapping, dict) and normalized_code:
        match = _match_template_name(templates, message_mapping.get(normalized_code))
        if match:
            return match

    family = _derive_message_family(normalized_code)
    family_mapping = protocol_schema.get("family_template_mapping")
    if not isinstance(family_mapping, dict):
        family_mapping = protocol_schema.get("message_family_mapping")

    if isinstance(family_mapping, dict) and family:
        match = _match_template_name(templates, family_mapping.get(family))
        if match:
            return match

    default_template = _match_template_name(templates, protocol_schema.get("default_template"))
    if default_template:
        return default_template

    return next(iter(templates), None)


def _build_message_schema_from_template(template_name: str, template: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fields = template.get("fields")
    if not isinstance(fields, list) or not fields:
        return None

    schema: Dict[str, Any] = {
        "description": template.get("description", f"Template {template_name}"),
        "fields": fields,
        "template_status": template.get("template_status", "generic"),
        "template_name": template_name,
    }
    aliases = template.get("aliases")
    if isinstance(aliases, list) and aliases:
        schema["aliases"] = aliases
    return schema


def _resolve_template_schema(
    protocol_schema: Dict[str, Any],
    message_code: Optional[str],
) -> Optional[Dict[str, Any]]:
    templates = _get_templates(protocol_schema)
    if not templates:
        return None

    template_name = _resolve_template_name(protocol_schema, message_code)
    if not template_name:
        return None

    template = templates.get(template_name)
    if not template:
        return None

    return _build_message_schema_from_template(template_name, template)


def _normalize_lookup_value(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    if not text:
        return ""
    return re.sub(r"[\s_\-]+", "", text)


def _field_in_schema(schema: Optional[Dict[str, Any]], field_name: Optional[str]) -> bool:
    if not schema or not field_name:
        return False

    normalized = _normalize_lookup_value(field_name)
    if not normalized:
        return False

    fields = schema.get("fields") or []
    if not isinstance(fields, list):
        return False

    for field in fields:
        if not isinstance(field, dict):
            continue
        candidates = [field.get("name")]
        aliases = field.get("aliases")
        if isinstance(aliases, list):
            candidates.extend(aliases)
        for candidate in candidates:
            if _normalize_lookup_value(candidate) == normalized:
                return True
    return False


def _resolve_template_schema_by_field_name(
    protocol_schema: Dict[str, Any],
    field_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    normalized = _normalize_lookup_value(field_name)
    if not normalized:
        return None

    templates = _get_templates(protocol_schema)
    for template_name, template in templates.items():
        if not isinstance(template, dict):
            continue
        fields = template.get("fields") or []
        if not isinstance(fields, list):
            continue
        for field in fields:
            if not isinstance(field, dict):
                continue
            candidates = [field.get("name")]
            aliases = field.get("aliases")
            if isinstance(aliases, list):
                candidates.extend(aliases)
            for candidate in candidates:
                if _normalize_lookup_value(candidate) == normalized:
                    return _build_message_schema_from_template(template_name, template)
    return None


def resolve_message_schema(
    protocol_type: Optional[str],
    message_code: Optional[str] = None,
    field_name: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[str], Optional[Dict[str, Any]]]:
    """Resolve protocol schema and best-effort message schema."""
    protocol_schema = load_protocol_schema(protocol_type)
    if not protocol_schema:
        return {}, None, None

    normalized_code = str(message_code).upper() if message_code else None

    messages = protocol_schema.get("messages")
    if isinstance(messages, dict) and normalized_code:
        message_schema = messages.get(normalized_code)
        if isinstance(message_schema, dict):
            if field_name and not _field_in_schema(message_schema, field_name):
                template_schema = _resolve_template_schema_by_field_name(protocol_schema, field_name)
                if template_schema:
                    return protocol_schema, normalized_code, template_schema
            return protocol_schema, normalized_code, message_schema

    template_schema = _resolve_template_schema(protocol_schema, normalized_code)
    if field_name and not _field_in_schema(template_schema, field_name):
        field_template_schema = _resolve_template_schema_by_field_name(protocol_schema, field_name)
        if field_template_schema:
            template_schema = field_template_schema
    if template_schema:
        return protocol_schema, normalized_code, template_schema

    return protocol_schema, normalized_code, None


def build_schema_prompt_context(
    protocol_schema: Dict[str, Any],
    message_schema: Optional[Dict[str, Any]],
    message_code: Optional[str],
    max_fields: int = 12,
) -> str:
    """Build compact schema context for extraction prompt."""
    if not protocol_schema:
        return ""

    lines = []
    lines.append(f"protocol={protocol_schema.get('protocol_type', 'unknown')}")
    if protocol_schema.get("version"):
        lines.append(f"version={protocol_schema.get('version')}")
    if message_code:
        lines.append(f"message_code={message_code}")

    template_name = (message_schema or {}).get("template_name")
    if template_name:
        lines.append(f"template={template_name}")

    if message_schema:
        fields = message_schema.get("fields") or []
        if isinstance(fields, list) and fields:
            lines.append("allowed_fields:")
            for field in fields[:max_fields]:
                if not isinstance(field, dict):
                    continue
                field_name = str(field.get("name") or "").strip()
                if not field_name:
                    continue
                bit_start = field.get("bit_start")
                bit_length = field.get("bit_length")
                unit = field.get("unit")
                parts = [field_name]
                if bit_start is not None:
                    parts.append(f"start={bit_start}")
                if bit_length is not None:
                    parts.append(f"len={bit_length}")
                if unit:
                    parts.append(f"unit={unit}")
                lines.append("- " + ", ".join(parts))

    return "\n".join(lines)


def _normalize_formula(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", "", str(value)).lower()


def _parse_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_check_item(
    rule_name: str,
    description: str,
    passed: bool,
    pass_msg: str,
    fail_msg: str,
) -> Dict[str, Any]:
    msg = pass_msg if passed else fail_msg
    return {
        "rule_name": rule_name,
        "description": description,
        "passed": passed,
        "message": msg,
        "status": "PASS" if passed else "FAIL",
        "msg": msg,
    }


def validate_with_schema(
    extracted_info: Dict[str, Any],
    message_schema: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Run additional schema-based checks for extracted info."""
    if not message_schema:
        return []

    fields = message_schema.get("fields") or []
    if not isinstance(fields, list) or not fields:
        return []

    check_items: List[Dict[str, Any]] = []
    extracted_name = str(extracted_info.get("field_name") or "").strip()
    normalized_name = extracted_name.lower()

    matched_field = None
    if normalized_name:
        for field in fields:
            if not isinstance(field, dict):
                continue
            candidates = [str(field.get("name") or "").strip().lower()]
            for alias in field.get("aliases") or []:
                candidates.append(str(alias).strip().lower())
            if normalized_name in candidates:
                matched_field = field
                break

    check_items.append(
        _build_check_item(
            rule_name="SchemaFieldNameMatch",
            description="字段名称与协议schema匹配",
            passed=matched_field is not None,
            pass_msg=f"字段 {extracted_name} 存在于schema中",
            fail_msg=f"字段 {extracted_name or '<empty>'} 未在schema中定义",
        )
    )

    if not matched_field:
        return check_items

    expected_width = matched_field.get("bit_length")
    actual_width = extracted_info.get("bit_width")
    expected_width_int = _parse_int(expected_width)
    actual_width_int = _parse_int(actual_width)
    width_passed = (
        expected_width is None
        or actual_width is None
        or (
            expected_width_int is not None
            and actual_width_int is not None
            and actual_width_int == expected_width_int
        )
        or str(actual_width).strip() == str(expected_width).strip()
    )
    check_items.append(
        _build_check_item(
            rule_name="SchemaBitWidthMatch",
            description="位宽与schema一致",
            passed=width_passed,
            pass_msg=f"位宽匹配: {actual_width}",
            fail_msg=f"位宽不一致: expected={expected_width}, actual={actual_width}",
        )
    )

    expected_start = matched_field.get("bit_start")
    actual_start = extracted_info.get("bit_start")
    expected_start_int = _parse_int(expected_start)
    actual_start_int = _parse_int(actual_start)
    start_passed = (
        expected_start is None
        or actual_start is None
        or (
            expected_start_int is not None
            and actual_start_int is not None
            and actual_start_int == expected_start_int
        )
        or str(actual_start).strip() == str(expected_start).strip()
    )
    check_items.append(
        _build_check_item(
            rule_name="SchemaBitStartMatch",
            description="起始位与schema一致",
            passed=start_passed,
            pass_msg=f"起始位匹配: {actual_start}",
            fail_msg=f"起始位不一致: expected={expected_start}, actual={actual_start}",
        )
    )

    expected_min = matched_field.get("range_min")
    expected_max = matched_field.get("range_max")
    actual_min = extracted_info.get("range_min")
    actual_max = extracted_info.get("range_max")

    expected_min_float = _parse_float(expected_min)
    expected_max_float = _parse_float(expected_max)
    actual_min_float = _parse_float(actual_min)
    actual_max_float = _parse_float(actual_max)

    range_passed = True
    if expected_min_float is not None and actual_min_float is not None and actual_min_float < expected_min_float:
        range_passed = False
    if expected_max_float is not None and actual_max_float is not None and actual_max_float > expected_max_float:
        range_passed = False

    check_items.append(
        _build_check_item(
            rule_name="SchemaRangeWithinBounds",
            description="量程在schema范围内",
            passed=range_passed,
            pass_msg="量程满足schema约束",
            fail_msg=(
                f"量程超出schema范围: expected=[{expected_min},{expected_max}], "
                f"actual=[{actual_min},{actual_max}]"
            ),
        )
    )

    expected_unit = _normalize_lookup_value(matched_field.get("unit"))
    actual_unit = _normalize_lookup_value(extracted_info.get("unit"))
    expected_units = set()
    if expected_unit:
        expected_units.add(expected_unit)
    unit_aliases = matched_field.get("unit_aliases")
    if isinstance(unit_aliases, list):
        for alias in unit_aliases:
            normalized_alias = _normalize_lookup_value(alias)
            if normalized_alias:
                expected_units.add(normalized_alias)
    unit_passed = not expected_units or not actual_unit or actual_unit in expected_units
    check_items.append(
        _build_check_item(
            rule_name="SchemaUnitMatch",
            description="单位与schema一致",
            passed=unit_passed,
            pass_msg=f"单位匹配: {extracted_info.get('unit') or '<unknown>'}",
            fail_msg=(
                f"单位不一致: expected={sorted(expected_units) or ['<unspecified>']}, "
                f"actual={extracted_info.get('unit') or '<unknown>'}"
            ),
        )
    )

    expected_formula = _normalize_formula(matched_field.get("formula"))
    actual_formula = _normalize_formula(extracted_info.get("conversion_formula"))
    formula_passed = not expected_formula or not actual_formula or expected_formula == actual_formula
    check_items.append(
        _build_check_item(
            rule_name="SchemaFormulaMatch",
            description="转换公式与schema一致",
            passed=formula_passed,
            pass_msg="转换公式匹配",
            fail_msg="转换公式与schema不一致",
        )
    )

    return check_items
