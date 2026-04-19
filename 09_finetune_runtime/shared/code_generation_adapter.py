from __future__ import annotations

import copy
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[1]
PROJECT_GENERATOR_ROOT = ROOT_DIR / "code_generate"
if str(PROJECT_GENERATOR_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_GENERATOR_ROOT))

from project_generator.loaders import load_choreography, load_mappings  # type: ignore
from project_generator.renderer import render_project  # type: ignore
from project_generator.xml_parser import load_protocols  # type: ignore


_JSON_PATH_KEYS = ("path", "file", "file_path")
_MAPPING_PAIR_PATTERN = re.compile(r"\s*([^=,]+?)\s*=\s*([^,]+?)\s*(?:,|$)")


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


def read_protocol_dir_content(path_like: Any) -> str:
    directory = Path(str(path_like or "").strip())
    if not directory.exists() or not directory.is_dir():
        raise ValueError(f"source_protocol_dir 不存在: {directory}")
    xml_files = sorted(directory.glob("*.xml"))
    if not xml_files:
        raise ValueError(f"source_protocol_dir 下未找到 XML 文件: {directory}")
    return "\n\n".join(xml_file.read_text(encoding="utf-8-sig") for xml_file in xml_files)


def _materialize_protocol_dir(protocol_xml_dir: Any, workspace_root: Path) -> Tuple[Path, Optional[Path]]:
    if isinstance(protocol_xml_dir, str):
        candidate = Path(protocol_xml_dir.strip())
        if candidate.exists() and candidate.is_dir():
            return candidate.resolve(), None

    materialized_root = workspace_root / "protocols"
    materialized_root.mkdir(parents=True, exist_ok=True)

    if isinstance(protocol_xml_dir, dict):
        files_payload = protocol_xml_dir.get("files")
        if isinstance(files_payload, list):
            for index, item in enumerate(files_payload, start=1):
                if not isinstance(item, dict):
                    raise ValueError("protocol_xml_dir.files 中的元素必须是对象")
                file_name = str(item.get("file_name") or item.get("name") or f"protocol_{index}.xml").strip()
                content = str(item.get("content") or "")
                (materialized_root / file_name).write_text(content, encoding="utf-8")
            return materialized_root, materialized_root

        wrote_any = False
        for file_name, content in protocol_xml_dir.items():
            if file_name in {"files", "dir"}:
                continue
            (materialized_root / str(file_name)).write_text(str(content or ""), encoding="utf-8")
            wrote_any = True
        if wrote_any:
            return materialized_root, materialized_root

    raise ValueError("protocol_xml_dir 必须是目录路径，或包含 XML 文件内容的对象")


def _normalize_protocol_names(protocol_dir: Path) -> List[str]:
    protocols = load_protocols(protocol_dir)
    return [protocol.type_name for protocol in protocols]


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
    return protocol_names[-1]


def _rewrite_formula_with_alias(formula: str, source_fields: List[str], alias: str) -> str:
    resolved = str(formula or "").strip()
    if not resolved:
        return resolved
    if source_fields:
        primary = source_fields[0]
        resolved = re.sub(r"\bvalue\b", f"{alias}.{primary}", resolved)
        for field in source_fields:
            resolved = re.sub(rf"\b{re.escape(field)}\b", f"{alias}.{field}", resolved)
    return resolved


def _convert_mapping_formula(formula: str, source_ref: str) -> str:
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


def build_generator_rules_payload(
    raw_rules: Any,
    protocol_dir: Path,
    port_config: Any,
    target_protocol_name: Optional[str] = None,
    project_name: Optional[str] = None,
) -> Dict[str, Any]:
    payload = _load_json_like(raw_rules, "conversion_rules_json")
    port_payload = _load_json_like(port_config, "port_config_json")
    if not isinstance(port_payload, dict):
        raise ValueError("port_config_json 必须是对象")

    if isinstance(payload, dict) and isinstance(payload.get("conversions"), list):
        rules_payload = copy.deepcopy(payload)
        rules_payload.setdefault("version", "1.0")
        rules_payload.setdefault("project_name", project_name or "generated_project")
    else:
        normalized_rules = _extract_normalized_rules(payload)
        protocol_names = _normalize_protocol_names(protocol_dir)
        resolved_target = _normalize_target_protocol_name(target_protocol_name, protocol_names, normalized_rules)
        source_protocols = [name for name in protocol_names if name != resolved_target]
        if len(source_protocols) != 1:
            raise ValueError("当前仅支持将单源 normalized_rules 自动转换为代码生成规则；多源请直接提供 generator 格式 conversion_rules_json")

        source_protocol = source_protocols[0]
        alias = "src"
        converted_rules: List[Dict[str, Any]] = []
        for rule in normalized_rules:
            source_fields = [str(item).strip() for item in (rule.get("source_fields") or []) if str(item).strip()]
            if not source_fields and rule.get("field_name"):
                source_fields = [str(rule["field_name"]).strip()]
            if rule.get("conversion_mode") == "mapping" and source_fields:
                formula = _convert_mapping_formula(str(rule.get("formula") or rule.get("rule") or ""), f"{alias}.{source_fields[0]}")
            else:
                formula = _rewrite_formula_with_alias(str(rule.get("formula") or rule.get("rule") or ""), source_fields, alias)
            converted_rules.append(
                {
                    "target_field": str(rule.get("target_field") or "").strip(),
                    "formula": formula,
                    "source_fields": [f"{alias}.{field}" for field in source_fields],
                    "rule_type": _detect_rule_type(formula, [f"{alias}.{field}" for field in source_fields]),
                    "when": None,
                    "default_value": 0 if rule.get("conversion_mode") == "mapping" else None,
                    "description": str(rule.get("description") or rule.get("concept_name") or "").strip() or None,
                }
            )

        rules_payload = {
            "version": "1.0",
            "project_name": project_name or f"{source_protocol.lower()}_to_{resolved_target.lower()}",
            "conversions": [
                {
                    "name": f"{source_protocol}To{resolved_target}",
                    "mode": "simple",
                    "sources": [{"alias": alias, "protocol": source_protocol}],
                    "target": {"protocol": resolved_target},
                    "rules": converted_rules,
                }
            ],
        }

    runtime_payload = rules_payload.setdefault("runtime", {})
    runtime_payload.setdefault("loop_sleep_ms", 2)
    runtime_payload.setdefault("check_data_interval_ms", 5000)
    runtime_payload["endpoints"] = normalize_port_config(port_payload)
    return rules_payload


def normalize_port_config(port_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    endpoints = port_payload.get("endpoints")
    if isinstance(endpoints, list):
        return copy.deepcopy(endpoints)

    ports = port_payload.get("ports")
    if not isinstance(ports, list) or not ports:
        raise ValueError("port_config_json 必须提供 ports 或 endpoints")

    normalized: List[Dict[str, Any]] = []
    for item in ports:
        if not isinstance(item, dict):
            raise ValueError("port_config_json.ports 中的元素必须是对象")
        port = int(item["port"])
        if port < 1 or port > 65535:
            raise ValueError(f"非法端口号: {port}")
        feedback_port = int(item.get("feedback_port", port))
        normalized.append(
            {
                "ip": str(item.get("ip") or "127.0.0.1"),
                "port": port,
                "type": str(item.get("type") or "udp"),
                "recv": 1 if str(item.get("role") or "").strip().lower() == "recv" else 0,
                "feed_back_port": feedback_port,
                "name": str(item.get("protocol") or item.get("name") or "").strip(),
            }
        )
    return normalized


def build_code_generation_payload(
    protocol_xml_dir: Any,
    conversion_rules_json: Any,
    conversion_matrix_json: Any,
    port_config_json: Any,
    output_dir: Any,
    target_protocol_name: Optional[str] = None,
    project_name: Optional[str] = None,
) -> Dict[str, Any]:
    output_path = Path(str(output_dir or "").strip()).resolve()
    if not str(output_path):
        raise ValueError("output_dir 不能为空")

    temp_workspace = Path(tempfile.mkdtemp(prefix="codegen_api_"))
    protocol_dir, created_protocol_dir = _materialize_protocol_dir(protocol_xml_dir, temp_workspace)
    mappings_payload = build_generator_rules_payload(
        raw_rules=conversion_rules_json,
        protocol_dir=protocol_dir,
        port_config=port_config_json,
        target_protocol_name=target_protocol_name,
        project_name=project_name,
    )

    choreography_payload = _load_json_like(conversion_matrix_json, "conversion_matrix_json")
    mappings_path = temp_workspace / "mappings.json"
    mappings_path.write_text(json.dumps(mappings_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    choreography = None
    if choreography_payload:
        choreography_path = temp_workspace / "choreography.json"
        choreography_path.write_text(json.dumps(choreography_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        choreography = load_choreography(choreography_path)

    protocols = load_protocols(protocol_dir)
    mappings = load_mappings(mappings_path)
    render_project(output_path, protocols, mappings, choreography)

    generated_files = sorted(
        str(path.relative_to(output_path)).replace(os.sep, "/")
        for path in output_path.rglob("*")
        if path.is_file()
    )
    conversion_units = [
        file_name for file_name in generated_files if file_name.endswith(".cpp") and "_to_" in file_name
    ]
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
        "manifest": manifest,
        "temp_protocol_dir": str(created_protocol_dir) if created_protocol_dir else None,
    }

