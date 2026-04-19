from __future__ import annotations

import ast
import copy
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]


def _resolve_project_generator_root(anchor: Path) -> Path:
    """Finds the active generator package root from the current workspace."""

    for base in (anchor, *anchor.parents):
        for dirname in ("code_generation", "code_generate"):
            candidate = base / dirname
            if (candidate / "project_generator").is_dir():
                return candidate
    raise ImportError("未找到可用的 project_generator 目录")


PROJECT_GENERATOR_ROOT = _resolve_project_generator_root(ROOT_DIR)
if str(PROJECT_GENERATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_GENERATOR_ROOT))

from project_generator.loaders import load_choreography, load_mappings  # type: ignore
from project_generator.renderer import render_project  # type: ignore
from project_generator.xml_parser import load_protocols  # type: ignore


_JSON_PATH_KEYS = ("path", "file", "file_path")
_MAPPING_PAIR_PATTERN = re.compile(r"\s*([^=,]+?)\s*=\s*([^,]+?)\s*(?:,|$)")


def _normalize_runtime_name(value: Any) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", str(value or "").strip()).strip("_").lower()


def _load_json_like(value: Any, field_name: str) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return copy.deepcopy(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        candidate = Path(raw)
        if candidate.exists():
            return json.loads(candidate.read_text(encoding="utf-8-sig"))
        if raw[0] in "{[":
            return json.loads(raw)
    raise ValueError(f"{field_name} 必须是 JSON 对象、数组或可读取的 JSON 文件路径")


def _load_text_file(path_like: Any, field_name: str) -> str:
    path = Path(str(path_like or "").strip())
    if not path.exists() or not path.is_file():
        raise ValueError(f"{field_name} 对应文件不存在: {path}")
    return path.read_text(encoding="utf-8-sig")


def _resolve_protocol_dir(path_like: Any, field_name: str) -> Path:
    directory = Path(str(path_like or "").strip())
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"{field_name} 不存在: {directory}")
    return directory


def _resolve_protocol_dirs(path_like: Any, field_name: str) -> List[Path]:
    if isinstance(path_like, (list, tuple)):
        if not path_like:
            raise ValueError(f"{field_name} 不能为空")
        return [
            _resolve_protocol_dir(item, f"{field_name}[{index}]")
            for index, item in enumerate(path_like)
        ]
    return [_resolve_protocol_dir(path_like, field_name)]


def _list_protocol_xml_files(directory: Path, field_name: str) -> List[Path]:
    xml_files = sorted(directory.glob("*.xml"))
    if not xml_files:
        raise ValueError(f"{field_name} 下未找到 XML 文件: {directory}")
    return xml_files


def _preferred_field_display_name(
    actual_field: str,
    label: Optional[str],
    path_parts: Optional[Iterable[Any]],
) -> str:
    for candidate in (
        str(label or "").strip(),
        str((list(path_parts or [])[-1]) if path_parts else "").strip(),
        str(actual_field or "").strip(),
    ):
        if candidate:
            return candidate
    return str(actual_field or "").strip()


def read_protocol_dir_content(path_like: Any, field_name: str = "source_protocol_dir") -> str:
    contents: List[str] = []
    for index, directory in enumerate(_resolve_protocol_dirs(path_like, field_name)):
        label = field_name if index == 0 and not isinstance(path_like, (list, tuple)) else f"{field_name}[{index}]"
        xml_files = _list_protocol_xml_files(directory, label)
        contents.extend(xml_file.read_text(encoding="utf-8-sig") for xml_file in xml_files)
    return "\n\n".join(contents)


def resolve_protocol_type_names(path_like: Any, field_name: str) -> List[str]:
    protocol_names: List[str] = []
    seen = set()
    for directory in _resolve_protocol_dirs(path_like, field_name):
        for protocol_name in _normalize_protocol_names(directory):
            if protocol_name in seen:
                continue
            seen.add(protocol_name)
            protocol_names.append(protocol_name)
    return protocol_names


def resolve_protocol_field_specs(path_like: Any, field_name: str) -> List[Dict[str, Any]]:
    specs: List[Dict[str, Any]] = []
    seen = set()
    for directory in _resolve_protocol_dirs(path_like, field_name):
        for protocol in load_protocols(directory):
            for item in protocol.fields:
                actual_field = str(item.cpp_name or item.label or "").strip()
                if not actual_field:
                    continue
                signature = (protocol.type_name, actual_field)
                if signature in seen:
                    continue
                seen.add(signature)
                display_field = _preferred_field_display_name(
                    actual_field=actual_field,
                    label=str(item.label or "").strip() or None,
                    path_parts=item.path_parts,
                )
                specs.append(
                    {
                        "protocol": protocol.type_name,
                        "field_name": display_field,
                        "actual_field": actual_field,
                        "default_value": item.default_value,
                        "bit_length": item.bit_length,
                        "label": str(item.label or "").strip() or display_field,
                        "path_parts": list(item.path_parts or []),
                    }
                )
    return specs


def _materialize_protocol_dirs(
    source_protocol_dir: Any,
    target_protocol_dir: Any,
    workspace_root: Path,
) -> Tuple[Path, Optional[Path]]:
    materialized_root = workspace_root / "protocols"
    materialized_root.mkdir(parents=True, exist_ok=True)

    copied_names: set[str] = set()
    for field_name, path_like in (
        ("source_protocol_dir", source_protocol_dir),
        ("target_protocol_dir", target_protocol_dir),
    ):
        is_multi_dir = isinstance(path_like, (list, tuple))
        for index, directory in enumerate(_resolve_protocol_dirs(path_like, field_name)):
            label = field_name if not is_multi_dir else f"{field_name}[{index}]"
            for xml_file in _list_protocol_xml_files(directory, label):
                target_path = materialized_root / xml_file.name
                if xml_file.name in copied_names:
                    if target_path.read_text(encoding="utf-8-sig") != xml_file.read_text(encoding="utf-8-sig"):
                        raise ValueError(f"{label} 中存在重名但内容不同的 XML 文件: {xml_file.name}")
                    continue
                target_path.write_text(xml_file.read_text(encoding="utf-8-sig"), encoding="utf-8")
                copied_names.add(xml_file.name)
    return materialized_root, materialized_root


def _normalize_protocol_names(protocol_dir: Path) -> List[str]:
    protocols = load_protocols(protocol_dir)
    return [protocol.type_name for protocol in protocols]


def _protocol_field_index(protocol_dir: Path) -> Dict[str, List[Tuple[str, str]]]:
    index: Dict[str, List[Tuple[str, str]]] = {}
    for protocol in load_protocols(protocol_dir):
        for field in protocol.fields:
            actual_field = str(field.cpp_name or field.label or "").strip()
            if not actual_field:
                continue
            for candidate in {
                actual_field,
                str(field.label or "").strip(),
                str(field.path_parts[-1] if field.path_parts else "").strip(),
            }:
                if not candidate:
                    continue
                key = candidate.upper()
                entries = index.setdefault(key, [])
                entry = (protocol.type_name, actual_field)
                if entry not in entries:
                    entries.append(entry)
    return index


def _protocol_field_display_index(protocol_dir: Path) -> Dict[Tuple[str, str], str]:
    index: Dict[Tuple[str, str], str] = {}
    for protocol in load_protocols(protocol_dir):
        for field in protocol.fields:
            actual_field = str(field.cpp_name or field.label or "").strip()
            if not actual_field:
                continue
            index[(protocol.type_name, actual_field.upper())] = _preferred_field_display_name(
                actual_field=actual_field,
                label=str(field.label or "").strip() or None,
                path_parts=field.path_parts,
            )
    return index


def _build_target_protocol_field_lookup(
    protocol_dir: Optional[Path],
    target_protocol_name: Optional[str],
) -> Tuple[Dict[str, str], set[str], set[str]]:
    if not protocol_dir or not target_protocol_name:
        return {}, set(), set()

    target_specs = [
        item
        for item in resolve_protocol_field_specs(protocol_dir, "protocol_dir")
        if str(item.get("protocol") or "").strip() == str(target_protocol_name).strip()
    ]
    if not target_specs:
        return {}, set(), set()

    lookup_candidates: Dict[str, set[str]] = {}
    actual_fields: set[str] = set()
    for spec in target_specs:
        actual_field = str(spec.get("actual_field") or spec.get("field_name") or "").strip()
        if not actual_field:
            continue
        actual_fields.add(actual_field)
        candidate_keys = {
            actual_field.upper(),
            str(spec.get("field_name") or "").strip().upper(),
            str(spec.get("label") or "").strip().upper(),
        }
        for part in spec.get("path_parts") or []:
            candidate_keys.add(str(part).strip().upper())
        for key in candidate_keys:
            if not key:
                continue
            lookup_candidates.setdefault(key, set()).add(actual_field)

    resolved_lookup: Dict[str, str] = {}
    ambiguous_keys: set[str] = set()
    for key, values in lookup_candidates.items():
        if len(values) == 1:
            resolved_lookup[key] = next(iter(values))
        else:
            ambiguous_keys.add(key)
    return resolved_lookup, actual_fields, ambiguous_keys


def _build_protocol_spec_maps(
    protocol_dir: Optional[Path],
    protocol_name: Optional[str],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    if not protocol_dir or not protocol_name:
        return {}, {}
    actual_to_spec: Dict[str, Dict[str, Any]] = {}
    path_to_actual: Dict[str, str] = {}
    for spec in resolve_protocol_field_specs(protocol_dir, "protocol_dir"):
        if str(spec.get("protocol") or "").strip() != str(protocol_name or "").strip():
            continue
        actual_field = str(spec.get("actual_field") or spec.get("field_name") or "").strip()
        if not actual_field:
            continue
        actual_to_spec[actual_field] = spec
        path = "/".join(str(part).strip() for part in (spec.get("path_parts") or []) if str(part).strip())
        if path:
            path_to_actual[path.upper()] = actual_field
    return actual_to_spec, path_to_actual


def _build_target_concept_spec_lookup(
    protocol_dir: Optional[Path],
    protocol_name: Optional[str],
) -> Dict[str, List[Dict[str, Any]]]:
    if not protocol_dir or not protocol_name:
        return {}
    concept_lookup: Dict[str, List[Dict[str, Any]]] = {}
    for spec in resolve_protocol_field_specs(protocol_dir, "protocol_dir"):
        if str(spec.get("protocol") or "").strip() != str(protocol_name or "").strip():
            continue
        keys = {
            str(spec.get("field_name") or "").strip().upper(),
            str(spec.get("label") or "").strip().upper(),
        }
        for part in spec.get("path_parts") or []:
            keys.add(str(part).strip().upper())
        for key in keys:
            if not key:
                continue
            concept_lookup.setdefault(key, []).append(spec)
    return concept_lookup


def _resolve_target_field_from_rule(
    rule: Dict[str, Any],
    target_protocol_name: Optional[str],
    protocol_field_index: Dict[str, List[Tuple[str, str]]],
    target_field_lookup: Dict[str, str],
    target_actual_to_spec: Dict[str, Dict[str, Any]],
    target_path_to_actual: Dict[str, str],
) -> str:
    explicit_actual = str(rule.get("target_actual_field") or "").strip()
    if explicit_actual and explicit_actual in target_actual_to_spec:
        return explicit_actual
    explicit_path = str(rule.get("target_path") or "").strip()
    if explicit_path:
        resolved_from_path = target_path_to_actual.get(explicit_path.upper())
        if resolved_from_path:
            return resolved_from_path
    raw_target_field = str(rule.get("target_field") or "").strip()
    normalized_target_key = raw_target_field.upper()
    if normalized_target_key in target_field_lookup:
        return target_field_lookup[normalized_target_key]
    return _resolve_protocol_field_name(
        raw_target_field,
        target_protocol_name,
        protocol_field_index,
    )


def _display_protocol_field_name(
    actual_field: str,
    protocol_name: Optional[str],
    protocol_display_index: Dict[Tuple[str, str], str],
) -> str:
    normalized = str(actual_field or "").strip()
    if not normalized or not protocol_name:
        return normalized
    return protocol_display_index.get((protocol_name, normalized.upper()), normalized)


def _normalize_target_protocol_name(
    explicit_target: Optional[str],
    protocol_names: List[str],
    rules_payload: List[Dict[str, Any]],
) -> str:
    if explicit_target:
        return explicit_target
    for rule in rules_payload:
        target = str(rule.get("target_protocol_type") or rule.get("target_protocol_name") or "").strip()
        if target:
            return target
    if len(protocol_names) == 1:
        return protocol_names[0]
    if protocol_names:
        return protocol_names[-1]
    raise ValueError("无法推断目标协议名称，请显式提供 target_protocol_name 或 target_protocol.protocol_type")


def _normalize_source_protocol_name(
    explicit_source: Optional[str],
    protocol_names: List[str],
    resolved_target: str,
    rules_payload: List[Dict[str, Any]],
) -> str:
    if explicit_source:
        return explicit_source
    for rule in rules_payload:
        source_name = str(rule.get("source_protocol_type") or rule.get("source_protocol_name") or "").strip()
        if source_name:
            return source_name
    inferred_sources = [name for name in protocol_names if name != resolved_target]
    if len(inferred_sources) == 1:
        return inferred_sources[0]
    raise ValueError("无法推断源协议名称，请显式提供 source_protocol_name 或 source_protocol.protocol_type")


def _rewrite_formula_with_alias(formula: str, source_fields: List[str], alias: str) -> str:
    resolved = str(formula or "").strip()
    if not resolved:
        return resolved
    if source_fields:
        primary = source_fields[0]
        resolved = re.sub(r"(?<![A-Za-z0-9_\.])value\b", f"{alias}.{primary}", resolved)
        for field in source_fields:
            resolved = re.sub(rf"(?<![A-Za-z0-9_\.]){re.escape(field)}\b", f"{alias}.{field}", resolved)
    return resolved


def _rewrite_formula_with_alias_map(formula: str, field_ref_map: Dict[str, str]) -> str:
    resolved = str(formula or "").strip()
    if not resolved:
        return resolved
    unique_refs = {field_ref for field_ref in field_ref_map.values() if field_ref}
    if len(unique_refs) == 1 and field_ref_map:
        resolved = re.sub(r"(?<![A-Za-z0-9_\.])value\b", next(iter(unique_refs)), resolved)
    for field in sorted(field_ref_map, key=len, reverse=True):
        resolved = re.sub(rf"(?<![A-Za-z0-9_\.]){re.escape(field)}\b", field_ref_map[field], resolved)
    return resolved


def _convert_python_expr_to_generator(expr: str) -> str:
    normalized = str(expr or "").strip()
    if not normalized:
        return normalized
    if " if " not in normalized and " and " not in normalized and " or " not in normalized and " not " not in normalized:
        return normalized

    def _render(node: ast.AST) -> str:
        if isinstance(node, ast.Expression):
            return _render(node.body)
        if isinstance(node, ast.IfExp):
            return f"({_render(node.test)} ? {_render(node.body)} : {_render(node.orelse)})"
        if isinstance(node, ast.BoolOp):
            op = "&&" if isinstance(node.op, ast.And) else "||"
            return f"({f' {op} '.join(_render(value) for value in node.values)})"
        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                return f"!({_render(node.operand)})"
            if isinstance(node.op, ast.USub):
                return f"-({_render(node.operand)})"
            if isinstance(node.op, ast.UAdd):
                return f"+({_render(node.operand)})"
        if isinstance(node, ast.BinOp):
            operator_map = {
                ast.Add: "+",
                ast.Sub: "-",
                ast.Mult: "*",
                ast.Div: "/",
                ast.Mod: "%",
            }
            if isinstance(node.op, ast.Pow):
                return f"pow({_render(node.left)}, {_render(node.right)})"
            operator = operator_map.get(type(node.op))
            if operator:
                return f"({_render(node.left)} {operator} {_render(node.right)})"
        if isinstance(node, ast.Compare):
            compare_map = {
                ast.Eq: "==",
                ast.NotEq: "!=",
                ast.Gt: ">",
                ast.GtE: ">=",
                ast.Lt: "<",
                ast.LtE: "<=",
            }
            left = _render(node.left)
            parts: List[str] = []
            for operator, comparator in zip(node.ops, node.comparators):
                symbol = compare_map.get(type(operator))
                if not symbol:
                    raise ValueError("unsupported compare operator")
                right = _render(comparator)
                parts.append(f"({left} {symbol} {right})")
                left = right
            return parts[0] if len(parts) == 1 else f"({' && '.join(parts)})"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            return f"{node.func.id}({', '.join(_render(arg) for arg in node.args)})"
        if isinstance(node, ast.Attribute):
            return f"{_render(node.value)}.{node.attr}"
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool):
                return "1" if node.value else "0"
            if node.value is None:
                return "0"
            return str(node.value)
        raise ValueError(f"unsupported ast node: {type(node).__name__}")

    try:
        parsed = ast.parse(normalized, mode="eval")
        return _render(parsed)
    except Exception:
        return normalized


def _normalize_formula_for_generator(formula: str) -> str:
    normalized = str(formula or "").strip()
    single_line_result_assign = re.fullmatch(r"result\s*=\s*(.+)", normalized)
    if single_line_result_assign and "\n" not in normalized:
        normalized = single_line_result_assign.group(1).strip()
    return _convert_python_expr_to_generator(normalized)


def _build_conversion_source_alias_lookup(sources: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    lookup: Dict[str, List[str]] = {}
    for source in sources:
        if not isinstance(source, dict):
            continue
        protocol = str(source.get("protocol") or "").strip()
        alias = str(source.get("alias") or "").strip()
        if not protocol or not alias:
            continue
        aliases = lookup.setdefault(protocol, [])
        if alias not in aliases:
            aliases.append(alias)
    return lookup


def _build_rule_field_ref_map(rule: Dict[str, Any], conversion_sources: List[Dict[str, Any]]) -> Dict[str, str]:
    field_ref_candidates: Dict[str, List[str]] = {}
    source_alias_lookup = _build_conversion_source_alias_lookup(conversion_sources)
    for item in rule.get("source_fields") or []:
        field_name = ""
        field_ref = ""
        if isinstance(item, str):
            normalized = item.strip()
            if "." not in normalized:
                continue
            alias_name, field_name = normalized.split(".", 1)
            field_ref = f"{alias_name}.{field_name}"
        elif isinstance(item, dict):
            field_name = str(item.get("field") or "").strip()
            alias_name = str(item.get("alias") or "").strip()
            if not alias_name:
                protocol_name = str(item.get("protocol") or "").strip()
                aliases = source_alias_lookup.get(protocol_name, [])
                if len(aliases) == 1:
                    alias_name = aliases[0]
            if alias_name and field_name:
                field_ref = f"{alias_name}.{field_name}"
        if not field_name or not field_ref:
            continue
        candidates = field_ref_candidates.setdefault(field_name, [])
        if field_ref not in candidates:
            candidates.append(field_ref)
    return {
        field_name: refs[0]
        for field_name, refs in field_ref_candidates.items()
        if len(refs) == 1
    }


def _resolve_protocol_field_name(
    field_name: str,
    protocol_name: Optional[str],
    protocol_field_index: Dict[str, List[Tuple[str, str]]],
) -> str:
    normalized = str(field_name or "").strip()
    if not normalized or not protocol_name:
        return normalized

    candidates = [normalized]
    if normalized.startswith("namespace_"):
        candidates.append(normalized[len("namespace_") :])

    for candidate in candidates:
        matches = [entry for entry in protocol_field_index.get(candidate.upper(), []) if entry[0] == protocol_name]
        if matches:
            return matches[0][1]
    return normalized


def _normalize_conversion_rules_payload(
    rules_payload: Dict[str, Any],
    protocol_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    normalized_payload = copy.deepcopy(rules_payload)
    conversions = normalized_payload.get("conversions")
    if not isinstance(conversions, list):
        return normalized_payload
    protocol_field_index = _protocol_field_index(protocol_dir) if protocol_dir else {}
    for conversion in conversions:
        if not isinstance(conversion, dict):
            continue
        conversion_sources = conversion.get("sources") or []
        alias_protocol_lookup = {
            str(source.get("alias") or "").strip(): str(source.get("protocol") or "").strip()
            for source in conversion_sources
            if isinstance(source, dict)
        }
        target_protocol_name = None
        target_payload = conversion.get("target")
        if isinstance(target_payload, dict):
            target_protocol_name = str(target_payload.get("protocol") or "").strip() or None
        rules = conversion.get("rules")
        if not isinstance(rules, list):
            continue
        valid_target_fields: set[str] = set()
        if target_protocol_name:
            valid_target_fields = {
                actual_field
                for key, entries in protocol_field_index.items()
                for protocol_name, actual_field in entries
                if protocol_name == target_protocol_name
            }
        target_actual_to_spec, target_path_to_actual = _build_protocol_spec_maps(protocol_dir, target_protocol_name)
        normalized_rules: List[Dict[str, Any]] = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            source_fields = rule.get("source_fields")
            source_actual_fields = rule.get("source_actual_fields")
            formula_field_ref_map: Dict[str, str] = {}
            if isinstance(source_fields, list):
                normalized_source_fields: List[Any] = []
                for index, item in enumerate(source_fields):
                    explicit_actual = ""
                    if isinstance(source_actual_fields, list) and index < len(source_actual_fields):
                        explicit_actual = str(source_actual_fields[index] or "").strip()
                    if isinstance(item, str):
                        normalized_item = item.strip()
                        if "." in normalized_item:
                            alias_name, field_name = normalized_item.split(".", 1)
                            actual_field = explicit_actual.split(".", 1)[1] if explicit_actual and "." in explicit_actual else explicit_actual
                            if not actual_field:
                                actual_field = _resolve_protocol_field_name(
                                    field_name,
                                    alias_protocol_lookup.get(alias_name),
                                    protocol_field_index,
                                )
                            formula_field_ref_map[f"{alias_name}.{field_name}"] = f"{alias_name}.{actual_field}"
                            formula_field_ref_map[field_name] = f"{alias_name}.{actual_field}"
                            normalized_item = f"{alias_name}.{actual_field}"
                        normalized_source_fields.append(normalized_item)
                    elif isinstance(item, dict):
                        normalized_item = copy.deepcopy(item)
                        protocol_name = str(normalized_item.get("protocol") or "").strip()
                        field_name = str(normalized_item.get("field") or "").strip()
                        resolved_field_name = _resolve_protocol_field_name(
                            field_name,
                            protocol_name or alias_protocol_lookup.get(str(normalized_item.get("alias") or "").strip()),
                            protocol_field_index,
                        )
                        alias_name = str(normalized_item.get("alias") or "").strip()
                        if alias_name and field_name:
                            formula_field_ref_map[f"{alias_name}.{field_name}"] = f"{alias_name}.{resolved_field_name}"
                            formula_field_ref_map[field_name] = f"{alias_name}.{resolved_field_name}"
                        normalized_item["field"] = resolved_field_name
                        normalized_source_fields.append(normalized_item)
                    else:
                        normalized_source_fields.append(item)
                rule["source_fields"] = normalized_source_fields

            raw_target_field = str(rule.get("target_field") or "").strip()
            target_actual_field = str(rule.get("target_actual_field") or "").strip()
            target_path = str(rule.get("target_path") or "").strip()
            if target_actual_field:
                resolved_target_field = target_actual_field
            elif target_path:
                resolved_target_field = target_path_to_actual.get(target_path.upper(), raw_target_field)
            elif raw_target_field:
                resolved_target_field = _resolve_protocol_field_name(
                    raw_target_field,
                    target_protocol_name,
                    protocol_field_index,
                )
            else:
                resolved_target_field = raw_target_field
            if resolved_target_field:
                if valid_target_fields and resolved_target_field not in valid_target_fields:
                    continue
                rule["target_field"] = resolved_target_field
                rule["target_actual_field"] = resolved_target_field
                target_spec = target_actual_to_spec.get(resolved_target_field) or {}
                if target_spec and not rule.get("target_path"):
                    rule["target_path"] = "/".join(
                        str(part).strip() for part in (target_spec.get("path_parts") or []) if str(part).strip()
                    ) or None

            field_ref_map = _build_rule_field_ref_map(rule, conversion_sources)
            formula = str(rule.get("formula") or "")
            when_expression = rule.get("when")
            combined_field_ref_map = dict(formula_field_ref_map)
            combined_field_ref_map.update(field_ref_map)
            if combined_field_ref_map:
                formula = _rewrite_formula_with_alias_map(formula, combined_field_ref_map)
                if when_expression is not None:
                    when_expression = _rewrite_formula_with_alias_map(
                        str(when_expression or ""),
                        combined_field_ref_map,
                    )
            rule["formula"] = _normalize_formula_for_generator(formula)
            if rule.get("when") is not None:
                rule["when"] = _normalize_formula_for_generator(str(when_expression or ""))
            normalized_source_refs = [
                str(item).strip()
                for item in (rule.get("source_fields") or [])
                if isinstance(item, str) and str(item).strip()
            ]
            rule["rule_type"] = str(rule.get("rule_type") or "").strip().lower() or _detect_rule_type(
                str(rule.get("formula") or ""),
                normalized_source_refs,
            )
            normalized_rules.append(rule)
        conversion["rules"] = normalized_rules
    return normalized_payload


def _validate_generated_cpp_syntax(output_path: Path, conversion_units: List[str]) -> Dict[str, Any]:
    compiler = shutil.which("g++") or shutil.which("clang++")
    if not compiler:
        return {
            "status": "skipped",
            "compiler": None,
            "checked_files": [],
            "error_count": 0,
        }

    checked_files: List[str] = []
    errors: List[Tuple[str, str]] = []
    for file_name in conversion_units:
        file_path = output_path / file_name
        if not file_path.exists():
            errors.append((file_name, "生成的转换单元文件不存在"))
            continue
        checked_files.append(file_name)
        process = subprocess.run(
            [compiler, "-std=c++17", "-fsyntax-only", "-I", str(output_path), str(file_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        if process.returncode != 0:
            message = (process.stderr or process.stdout or "未知语法错误").strip()
            errors.append((file_name, message))

    if errors:
        first_file, first_error = errors[0]
        raise ValueError(f"生成后的 C++ 转换单元语法校验失败: {first_file}: {first_error}")

    return {
        "status": "passed",
        "compiler": compiler,
        "checked_files": checked_files,
        "error_count": 0,
    }


def _convert_mapping_formula(formula: str, source_ref: str) -> str:
    raw_formula = str(formula or "")
    if re.search(r"^\s*result\s*=", raw_formula, flags=re.IGNORECASE):
        return raw_formula
    if any(token in raw_formula for token in ("==", "!=", ">=", "<=", "?", ":", "&&", "||")):
        return raw_formula
    pairs = list(_MAPPING_PAIR_PATTERN.finditer(str(formula or "")))
    if not pairs:
        return formula

    fallback = "0"
    expression = fallback
    for match in reversed(pairs):
        raw_key = match.group(1).strip()
        raw_value = match.group(2).strip()
        key = raw_key
        value = raw_value
        expression = f"({source_ref} == {key} ? {value} : {expression})"
    return expression


def _detect_rule_type(formula: str, source_fields: List[str]) -> str:
    stripped = str(formula or "").strip()
    if not source_fields:
        return "const"
    if "?" in stripped and ":" in stripped:
        return "conditional"
    if len(source_fields) == 1 and stripped == source_fields[0]:
        return "direct"
    return "expression"


def _contains_unresolved_result_placeholder(formula: str) -> bool:
    return bool(re.search(r"\bresult\b", str(formula or "").strip(), flags=re.IGNORECASE))


def _build_protocol_alias(protocol_name: str, used_aliases: set[str]) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "_", str(protocol_name or "").strip()).strip("_").lower() or "src"
    alias = base
    suffix = 2
    while alias in used_aliases:
        alias = f"{base}_{suffix}"
        suffix += 1
    used_aliases.add(alias)
    return alias


def _extract_normalized_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    if isinstance(raw_rules, dict):
        if isinstance(raw_rules.get("normalized_rules"), list):
            return raw_rules["normalized_rules"]
        data = raw_rules.get("data")
        if isinstance(data, dict) and isinstance(data.get("normalized_rules"), list):
            return data["normalized_rules"]
    if isinstance(raw_rules, list):
        return raw_rules
    raise ValueError("conversion_rules_json 不是可识别的规则结构")


def _build_display_formula(
    formula: str,
    display_ref_map: Dict[str, str],
) -> str:
    if not display_ref_map:
        return formula
    return _rewrite_formula_with_alias_map(formula, display_ref_map)


def _target_rule_identity(rule: Dict[str, Any], fallback_index: int) -> Tuple[str, str]:
    actual_field = str(rule.get("target_actual_field") or "").strip()
    if actual_field:
        return ("actual", actual_field)
    target_path = str(rule.get("target_path") or "").strip()
    if target_path:
        return ("path", target_path)
    target_field = str(rule.get("target_field") or "").strip()
    if target_field:
        return ("field", target_field)
    return ("index", str(fallback_index))


def _target_rule_priority(rule: Dict[str, Any]) -> Tuple[int, int, int]:
    formula = str(rule.get("formula") or rule.get("rule") or "").strip()
    has_non_zero_formula = int(formula not in {"", "0", "0.0", "0U", "0L"})
    has_source_fields = int(bool(rule.get("source_fields") or rule.get("source_actual_fields")))
    has_description = int(bool(str(rule.get("description") or "").strip()))
    return (has_non_zero_formula, has_source_fields, has_description)


def _dedupe_rules_by_target_identity(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[str, str], Tuple[int, Dict[str, Any]]] = {}
    order: List[Tuple[str, str]] = []
    for index, rule in enumerate(rules):
        identity = _target_rule_identity(rule, index)
        existing = deduped.get(identity)
        if existing is None:
            deduped[identity] = (index, rule)
            order.append(identity)
            continue
        existing_index, existing_rule = existing
        if _target_rule_priority(rule) > _target_rule_priority(existing_rule):
            deduped[identity] = (existing_index, rule)
    return [deduped[identity][1] for identity in order]


def build_generator_rules_payload(
    raw_rules: Any,
    protocol_dir: Optional[Path] = None,
    target_protocol_name: Optional[str] = None,
    source_protocol_name: Optional[str] = None,
    project_name: Optional[str] = None,
    preserve_display_names: bool = False,
) -> Dict[str, Any]:
    payload = _load_json_like(raw_rules, "conversion_rules_json")

    if isinstance(payload, dict) and isinstance(payload.get("conversions"), list):
        rules_payload = _normalize_conversion_rules_payload(payload, protocol_dir=protocol_dir)
        rules_payload.setdefault("version", "1.0")
        rules_payload.setdefault("project_name", project_name or "generated_project")
        for conversion in rules_payload.get("conversions") or []:
            rules = conversion.get("rules")
            if isinstance(rules, list):
                conversion["rules"] = _dedupe_rules_by_target_identity(rules)
    else:
        normalized_rules = _extract_normalized_rules(payload)
        protocol_names = _normalize_protocol_names(protocol_dir) if protocol_dir else []
        resolved_target = _normalize_target_protocol_name(target_protocol_name, protocol_names, normalized_rules)
        available_source_protocols = [name for name in protocol_names if name != resolved_target]
        protocol_field_index = _protocol_field_index(protocol_dir) if protocol_dir else {}
        protocol_display_index = _protocol_field_display_index(protocol_dir) if protocol_dir else {}
        target_field_lookup, valid_target_fields, ambiguous_target_keys = _build_target_protocol_field_lookup(
            protocol_dir,
            resolved_target,
        )
        target_actual_to_spec, target_path_to_actual = _build_protocol_spec_maps(
            protocol_dir,
            resolved_target,
        )
        target_concept_spec_lookup = _build_target_concept_spec_lookup(
            protocol_dir,
            resolved_target,
        )
        single_source_protocol = None
        if len(available_source_protocols) <= 1:
            single_source_protocol = _normalize_source_protocol_name(
                source_protocol_name,
                protocol_names,
                resolved_target,
                normalized_rules,
            )
            available_source_protocols = [single_source_protocol]

        protocol_aliases: Dict[str, str] = {}
        used_aliases: set[str] = set()
        converted_rules: List[Dict[str, Any]] = []
        referenced_protocols: List[str] = []
        for rule in normalized_rules:
            source_fields = [str(item).strip() for item in (rule.get("source_fields") or []) if str(item).strip()]
            explicit_source_actual_fields = [
                str(item).strip()
                for item in (rule.get("source_actual_fields") or [])
                if str(item).strip()
            ]
            explicit_source_paths = [
                str(item).strip()
                for item in (rule.get("source_paths") or [])
                if str(item).strip()
            ]
            if not source_fields and rule.get("field_name"):
                source_fields = [str(rule["field_name"]).strip()]
            field_ref_map: Dict[str, str] = {}
            field_refs: List[str] = []
            display_field_refs: List[str] = []
            display_ref_map: Dict[str, str] = {}
            output_source_actual_fields: List[str] = []
            output_source_paths: List[Optional[str]] = []
            for field in source_fields:
                source_index = len(output_source_actual_fields)
                preferred_actual_field = explicit_source_actual_fields[source_index] if source_index < len(explicit_source_actual_fields) else ""
                matched_entries = [
                    entry for entry in protocol_field_index.get(field.upper(), []) if entry[0] in available_source_protocols
                ]
                if len({entry[0] for entry in matched_entries}) > 1:
                    raise ValueError(f"字段 {field} 同时匹配多个源协议，无法自动推断来源")
                if preferred_actual_field and matched_entries:
                    exact_entries = [entry for entry in matched_entries if entry[1] == preferred_actual_field]
                    if exact_entries:
                        matched_entries = exact_entries
                if matched_entries:
                    resolved_protocol, actual_field = matched_entries[0]
                else:
                    resolved_protocol = single_source_protocol or None
                    actual_field = preferred_actual_field or field
                if not resolved_protocol:
                    raise ValueError(f"字段 {field} 无法匹配到源协议")
                if resolved_protocol not in protocol_aliases:
                    protocol_aliases[resolved_protocol] = _build_protocol_alias(resolved_protocol, used_aliases)
                alias = protocol_aliases[resolved_protocol]
                field_ref = f"{alias}.{actual_field}"
                display_name = _display_protocol_field_name(
                    actual_field=actual_field,
                    protocol_name=resolved_protocol,
                    protocol_display_index=protocol_display_index,
                )
                display_field_ref = f"{alias}.{display_name}"
                field_ref_map[field] = field_ref
                field_refs.append(field_ref)
                display_field_refs.append(display_field_ref)
                display_ref_map[field_ref] = display_field_ref
                display_ref_map[actual_field] = display_name
                output_source_actual_fields.append(f"{alias}.{actual_field}")
                output_source_paths.append(explicit_source_paths[source_index] if source_index < len(explicit_source_paths) else None)
                if resolved_protocol not in referenced_protocols:
                    referenced_protocols.append(resolved_protocol)
            if rule.get("conversion_mode") == "mapping" and source_fields:
                formula = _rewrite_formula_with_alias_map(
                    _convert_mapping_formula(
                        str(rule.get("formula") or rule.get("rule") or ""),
                        field_ref_map[source_fields[0]],
                    ),
                    field_ref_map,
                )
            else:
                formula = _rewrite_formula_with_alias_map(str(rule.get("formula") or rule.get("rule") or ""), field_ref_map)
            formula = _normalize_formula_for_generator(formula)
            raw_target_field = str(rule.get("target_field") or "").strip()
            normalized_target_key = raw_target_field.upper()
            explicit_target_actual = str(rule.get("target_actual_field") or "").strip()
            explicit_target_path = str(rule.get("target_path") or "").strip()
            target_specs: List[Dict[str, Any]] = []
            if explicit_target_actual or explicit_target_path:
                resolved_target_field = _resolve_target_field_from_rule(
                    rule=rule,
                    target_protocol_name=resolved_target,
                    protocol_field_index=protocol_field_index,
                    target_field_lookup=target_field_lookup,
                    target_actual_to_spec=target_actual_to_spec,
                    target_path_to_actual=target_path_to_actual,
                )
                if valid_target_fields and resolved_target_field not in valid_target_fields:
                    continue
                target_spec = target_actual_to_spec.get(resolved_target_field)
                if target_spec:
                    target_specs = [target_spec]
            else:
                target_specs = list(target_concept_spec_lookup.get(normalized_target_key, []))
                if not target_specs:
                    resolved_target_field = _resolve_target_field_from_rule(
                        rule=rule,
                        target_protocol_name=resolved_target,
                        protocol_field_index=protocol_field_index,
                        target_field_lookup=target_field_lookup,
                        target_actual_to_spec=target_actual_to_spec,
                        target_path_to_actual=target_path_to_actual,
                    )
                    if valid_target_fields and resolved_target_field not in valid_target_fields:
                        continue
                    target_spec = target_actual_to_spec.get(resolved_target_field)
                    if target_spec:
                        target_specs = [target_spec]
                elif valid_target_fields:
                    target_specs = [
                        spec for spec in target_specs
                        if str(spec.get("actual_field") or spec.get("field_name") or "").strip() in valid_target_fields
                    ]
            if not target_specs:
                continue
            if _contains_unresolved_result_placeholder(formula):
                formula = "0"
            for target_spec in target_specs:
                resolved_target_field = str(
                    target_spec.get("actual_field") or target_spec.get("field_name") or ""
                ).strip()
                if not resolved_target_field:
                    continue
                target_field = resolved_target_field
                output_source_fields = field_refs
                output_formula = formula
                if preserve_display_names:
                    target_field = _display_protocol_field_name(
                        actual_field=resolved_target_field,
                        protocol_name=resolved_target,
                        protocol_display_index=protocol_display_index,
                    )
                    output_source_fields = display_field_refs
                    output_formula = _build_display_formula(formula, display_ref_map)
                converted_rules.append(
                    {
                        "target_field": target_field,
                        "target_actual_field": resolved_target_field,
                        "target_path": "/".join(str(part).strip() for part in (target_spec.get("path_parts") or []) if str(part).strip()) or None,
                        "formula": output_formula,
                        "source_fields": output_source_fields,
                        "source_actual_fields": output_source_actual_fields if output_source_fields else [],
                        "source_paths": output_source_paths if output_source_fields else [],
                        "rule_type": _detect_rule_type(output_formula, output_source_fields),
                        "when": None,
                        "default_value": 0 if rule.get("conversion_mode") == "mapping" else None,
                        "description": str(rule.get("description") or rule.get("concept_name") or "").strip() or None,
                    }
                )
        converted_rules = _dedupe_rules_by_target_identity(converted_rules)

        if not referenced_protocols and single_source_protocol:
            protocol_aliases[single_source_protocol] = protocol_aliases.get(
                single_source_protocol,
                _build_protocol_alias(single_source_protocol, used_aliases),
            )
            referenced_protocols.append(single_source_protocol)

        conversion_sources = [
            {"alias": protocol_aliases[protocol], "protocol": protocol}
            for protocol in referenced_protocols
        ]
        conversion_mode = "joint" if len(conversion_sources) > 1 else "simple"
        source_name_for_project = "_".join(protocol.lower() for protocol in referenced_protocols) or "source"
        rules_payload = {
            "version": "1.0",
            "project_name": project_name or f"{source_name_for_project}_to_{resolved_target.lower()}",
            "conversions": [
                {
                    "name": f"{'_'.join(referenced_protocols) or 'Source'}To{resolved_target}",
                    "mode": conversion_mode,
                    "sources": conversion_sources,
                    "target": {"protocol": resolved_target},
                    "rules": converted_rules,
                }
            ],
        }

    return rules_payload


def build_runtime_mappings_payload(
    conversion_rules_json: Any,
    port_config_json: Any,
    protocol_dir: Optional[Path] = None,
    target_protocol_name: Optional[str] = None,
    source_protocol_name: Optional[str] = None,
    project_name: Optional[str] = None,
) -> Dict[str, Any]:
    rules_payload = build_generator_rules_payload(
        raw_rules=conversion_rules_json,
        protocol_dir=protocol_dir,
        target_protocol_name=target_protocol_name,
        source_protocol_name=source_protocol_name,
        project_name=project_name,
    )
    port_payload = _load_json_like(port_config_json, "port_config_json")
    if not isinstance(port_payload, dict):
        raise ValueError("port_config_json 必须是对象")

    mappings_payload = copy.deepcopy(rules_payload)
    runtime_payload = copy.deepcopy(mappings_payload.get("runtime") or {})
    runtime_payload.setdefault("loop_sleep_ms", 2)
    runtime_payload.setdefault("check_data_interval_ms", 5000)
    normalized_port_payload = normalize_port_config(
        port_payload,
        conversions=mappings_payload.get("conversions") or [],
    )
    runtime_payload["endpoints"] = normalized_port_payload["endpoints"]
    runtime_payload["transport"] = normalized_port_payload["transport"]
    mappings_payload["runtime"] = runtime_payload
    return mappings_payload


def _validate_port(value: Any, field_name: str) -> int:
    port = int(value)
    if port < 1 or port > 65535:
        raise ValueError(f"{field_name} 非法端口号: {port}")
    return port


def _normalize_filter_config(raw_filter_config: Any) -> Dict[str, Any]:
    if raw_filter_config in (None, "", {}):
        raw_filter_config = {}
    if isinstance(raw_filter_config, str):
        text = raw_filter_config.strip()
        raw_filter_config = json.loads(text) if text else {}
    if not isinstance(raw_filter_config, dict):
        raise ValueError("filterConfig 必须是对象、JSON 字符串或 null")

    crc_check = raw_filter_config.get("crcCheck") or {}
    loop_config = raw_filter_config.get("loopConfig") or {}
    aggregation = raw_filter_config.get("aggregation") or {}
    aggregation_type = raw_filter_config.get("aggregationType") or {}
    normalized = {
        "crcCheck": {
            "enabled": bool(crc_check.get("enabled", False)),
            "bindElement": str(crc_check.get("bindElement") or "").strip() or None,
        },
        "loopConfig": {
            "type": str(loop_config.get("type") or "NONE").strip().upper() or "NONE",
        },
        "aggregation": {
            "mode": str(aggregation.get("mode") or "SINGLE").strip().upper() or "SINGLE",
            "count": None if aggregation.get("count") in (None, "") else int(aggregation.get("count")),
            "timeMs": None if aggregation.get("timeMs") in (None, "") else int(aggregation.get("timeMs")),
        },
        "aggregationType": {
            "type": str(aggregation_type.get("type") or "TIME").strip().upper() or "TIME",
            "bindElement": str(aggregation_type.get("bindElement") or "").strip() or None,
        },
    }
    if normalized["crcCheck"]["enabled"] and not normalized["crcCheck"]["bindElement"]:
        raise ValueError("crcCheck.enabled=true 时必须提供 bindElement")
    return normalized


def normalize_port_config(
    port_payload: Dict[str, Any],
    conversions: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    message_type = str(port_payload.get("messageType") or "").strip()
    if not message_type:
        raise ValueError("port_config_json.messageType 不能为空")
    recv_ip = str(port_payload.get("recvIp") or "127.0.0.1").strip() or "127.0.0.1"
    send_ip = str(port_payload.get("sendIp") or "127.0.0.1").strip() or "127.0.0.1"
    recv_port = _validate_port(port_payload.get("recvPort"), "recvPort")
    send_port = _validate_port(port_payload.get("sendPort"), "sendPort")
    rule_items = port_payload.get("messageRuleDetailList")
    if not isinstance(rule_items, list) or not rule_items:
        raise ValueError("port_config_json.messageRuleDetailList 不能为空，且必须是数组")

    normalized_rules: List[Dict[str, Any]] = []
    for index, item in enumerate(rule_items):
        if not isinstance(item, dict):
            raise ValueError(f"messageRuleDetailList[{index}] 必须是对象")
        message_name = str(item.get("messageName") or "").strip()
        if not message_name:
            raise ValueError(f"messageRuleDetailList[{index}].messageName 不能为空")
        normalized_rules.append(
            {
                "messageName": message_name,
                "delayRequirement": int(item.get("delayRequirement") or 0),
                "filterConfig": _normalize_filter_config(item.get("filterConfig")),
            }
        )

    recv_endpoint_name = _normalize_runtime_name(message_type) or "recv_message"
    send_endpoint_name = f"{recv_endpoint_name}_send"
    if conversions:
        source_alias_names: List[str] = []
        target_protocol_names: List[str] = []
        for conversion in conversions:
            for source in conversion.get("sources") or []:
                alias_name = _normalize_runtime_name(source.get("alias") or source.get("protocol"))
                if alias_name and alias_name not in source_alias_names:
                    source_alias_names.append(alias_name)
            target_name = _normalize_runtime_name((conversion.get("target") or {}).get("protocol"))
            if target_name and target_name not in target_protocol_names:
                target_protocol_names.append(target_name)
        if len(source_alias_names) == 1:
            recv_endpoint_name = source_alias_names[0]
        if len(target_protocol_names) == 1:
            send_endpoint_name = target_protocol_names[0]

    return {
        "transport": {
            "messageType": message_type,
            "recvIp": recv_ip,
            "recvPort": recv_port,
            "sendIp": send_ip,
            "sendPort": send_port,
            "messageRuleDetailList": normalized_rules,
        },
        "endpoints": [
            {
                "ip": recv_ip,
                "port": recv_port,
                "type": "udp",
                "recv": 1,
                "feedBackPort": recv_port,
                "name": recv_endpoint_name,
            },
            {
                "ip": send_ip,
                "port": send_port,
                "type": "udp",
                "recv": 0,
                "feedBackPort": recv_port,
                "name": send_endpoint_name,
            },
        ],
    }


def build_code_generation_payload(
    source_protocol_dir: Any,
    target_protocol_dir: Any,
    conversion_rules_json: Any,
    conversion_matrix_json: Any,
    port_config_json: Any,
    output_dir: Any,
    target_protocol_name: Optional[str] = None,
    project_name: Optional[str] = None,
) -> Dict[str, Any]:
    output_dir_text = str(output_dir or "").strip()
    if not output_dir_text:
        raise ValueError("output_dir 不能为空")
    output_path = Path(output_dir_text).resolve()

    temp_workspace = Path(tempfile.mkdtemp(prefix="codegen_api_"))
    protocol_dir, created_protocol_dir = _materialize_protocol_dirs(
        source_protocol_dir=source_protocol_dir,
        target_protocol_dir=target_protocol_dir,
        workspace_root=temp_workspace,
    )
    mappings_payload = build_runtime_mappings_payload(
        conversion_rules_json=conversion_rules_json,
        port_config_json=port_config_json,
        protocol_dir=protocol_dir,
        target_protocol_name=target_protocol_name,
        project_name=project_name,
    )

    choreography_payload = _load_json_like(conversion_matrix_json, "conversion_matrix_json")
    if isinstance(choreography_payload, dict):
        if not choreography_payload or (
            set(choreography_payload.keys()) == {"joint_groups"}
            and not (choreography_payload.get("joint_groups") or [])
        ):
            choreography_payload = None
    mappings_path = temp_workspace / "mappings.json"
    mappings_path.write_text(json.dumps(mappings_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    protocols = load_protocols(protocol_dir)
    mappings = load_mappings(mappings_path)
    choreography = None
    if choreography_payload:
        choreography_path = temp_workspace / "choreography.json"
        choreography_path.write_text(json.dumps(choreography_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        choreography = load_choreography(choreography_path)
    render_project(output_path, protocols, mappings, choreography)

    generated_files = sorted(
        str(path.relative_to(output_path)).replace(os.sep, "/")
        for path in output_path.rglob("*")
        if path.is_file()
    )
    conversion_units = [
        file_name for file_name in generated_files if file_name.endswith(".cpp") and "_to_" in file_name
    ]
    syntax_validation = _validate_generated_cpp_syntax(output_path, conversion_units)
    manifest_path = output_path / "protocol_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    return {
        "status": "success",
        "project_name": mappings_payload.get("project_name"),
        "mode": (mappings_payload.get("conversions") or [{}])[0].get("mode", "simple"),
        "output": {
            "project_dir": str(output_path),
            "files": generated_files,
            "conversion_units": conversion_units,
        },
        "warnings": [],
        "summary": {
            "protocol_count": len(protocols),
            "conversion_count": len(mappings_payload.get("conversions") or []),
            "joint_group_count": len((choreography_payload or {}).get("joint_groups") or []),
        },
        "syntax_validation": syntax_validation,
        "manifest": manifest,
        "temp_protocol_dir": str(created_protocol_dir) if created_protocol_dir else None,
    }
