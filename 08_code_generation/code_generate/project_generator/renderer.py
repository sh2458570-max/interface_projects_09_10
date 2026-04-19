"""Project rendering logic for the protocol project generator."""

from __future__ import annotations

import re
from pathlib import Path

from project_generator.models import BranchNode, ChoreographySpec, GroupNode, MappingSpec, ProtocolSpec, ScalarNode
from project_generator.reference_profiles import render_reference_project
from project_generator.templates import (
    mapping_file_base,
    render_choreography_cpp,
    render_choreography_header,
    render_codec_cpp,
    render_codec_header,
    render_config_xml,
    render_generator_readme,
    render_main_cpp,
    render_mapping_cpp,
    render_mapping_header,
    render_messageconvert_cpp,
    render_messageconvert_header,
    render_pro_file,
    render_protocol_header,
)
from project_generator.utils import dump_json, ensure_directory, to_snake_name, write_text


_FIELD_REF_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")


def _resolve_field_name(field_name: str, valid_fields: set[str]) -> str:
    """Resolves one field name with backward-compatible aliases."""

    candidates = [field_name]
    if field_name.startswith("namespace_"):
        candidates.append(field_name[len("namespace_") :])
    for candidate in candidates:
        if candidate in valid_fields:
            return candidate
    normalized_candidates = {_legacy_normalize(candidate) for candidate in candidates}
    for valid_field in valid_fields:
        if _legacy_normalize(valid_field) in normalized_candidates:
            return valid_field
    raise ValueError(f"未知字段: {field_name}")


def _legacy_normalize(field_name: str) -> str:
    """Normalizes legacy flattened group field names for compatibility."""

    parts = [part for part in field_name.split("_") if part]
    normalized: list[str] = []
    index = 0
    while index < len(parts):
        normalized.append(parts[index])
        if (
            index + 1 < len(parts)
            and parts[index].startswith(("u5faau73af", "group"))
            and parts[index + 1].isdigit()
        ):
            index += 2
        else:
            index += 1
    return "_".join(normalized)


def _rewrite_expression(expression: str | None, alias_fields: dict[str, set[str]]) -> str | None:
    """Rewrites legacy alias.field references to current flattened names."""

    if expression is None:
        return None

    def repl(match: re.Match[str]) -> str:
        alias = match.group(1)
        field_name = match.group(2)
        if alias not in alias_fields:
            return match.group(0)
        return f"{alias}.{_resolve_field_name(field_name, alias_fields[alias])}"

    return _FIELD_REF_RE.sub(repl, expression)


def _build_alias_field_lookup(conversion, protocol_lookup: dict[str, ProtocolSpec]) -> dict[str, set[str]]:
    """Builds alias-to-field lookup for one conversion."""

    return {
        source.alias: {field.cpp_name for field in protocol_lookup[source.protocol].fields}
        for source in conversion.sources
    }


def _mapping_signature(
    conversion_name: str,
    target_type: str,
    source_pairs: list[tuple[str, str]],
) -> str:
    """Builds one C++ mapping function signature."""

    arguments = ", ".join(f"const {protocol_type}& {alias}" for alias, protocol_type in source_pairs)
    return f"{target_type} convert_{to_snake_name(conversion_name)}({arguments})"


def _process_method_name(conversion) -> str:
    """Builds one process method name with newB-style defaults."""

    if conversion.runtime.process_method:
        return conversion.runtime.process_method
    if len(conversion.sources) == 1:
        return f"{conversion.sources[0].protocol}dataPro"
    source_part = "_".join(source.protocol for source in conversion.sources)
    return f"{source_part}dataPro"


def _mapping_body(conversion, target_protocol: ProtocolSpec, alias_fields: dict[str, set[str]]) -> str:
    """Renders assignment statements for one conversion."""

    valid_fields = {field.cpp_name for field in target_protocol.fields}
    lines = []
    for rule in conversion.rules:
        target_field = _resolve_field_name(rule.target_field, valid_fields)
        formula = _rewrite_expression(rule.formula, alias_fields)
        when_expression = _rewrite_expression(rule.when, alias_fields)
        assignment = (
            f"    target.{target_field} = "
            f"static_cast<decltype(target.{target_field})>({formula});"
        )
        if when_expression:
            lines.append(f"    if ({when_expression}) {{")
            lines.append(assignment)
            if rule.default_value is not None:
                lines.append("    } else {")
                lines.append(
                    f"        target.{target_field} = "
                    f"static_cast<decltype(target.{target_field})>({rule.default_value});"
                )
            lines.append("    }")
        else:
            lines.append(assignment)
    return "\n".join(lines) + ("\n" if lines else "")


def _build_protocol_lookup(protocols: list[ProtocolSpec]) -> dict[str, ProtocolSpec]:
    """Builds a lookup table for protocols."""

    lookup: dict[str, ProtocolSpec] = {}
    for protocol in protocols:
        if protocol.type_name in lookup:
            raise ValueError(f"重复的协议类型名: {protocol.type_name}")
        lookup[protocol.type_name] = protocol
    return lookup


def _validate_field_references(conversion, protocol_lookup: dict[str, ProtocolSpec]) -> None:
    """Validates source-field references in one conversion."""

    alias_fields: dict[str, set[str]] = {}
    for source in conversion.sources:
        if source.protocol not in protocol_lookup:
            raise ValueError(f"转换 '{conversion.name}' 使用了未知源协议: {source.protocol}")
        alias_fields[source.alias] = {field.cpp_name for field in protocol_lookup[source.protocol].fields}

    for rule in conversion.rules:
        for field_ref in rule.source_fields:
            if "." not in field_ref:
                raise ValueError(f"转换 '{conversion.name}' 的源字段格式非法: {field_ref}")
            alias, field_name = field_ref.split(".", 1)
            if alias not in alias_fields:
                raise ValueError(f"转换 '{conversion.name}' 使用了未知别名: {alias}")
            try:
                _resolve_field_name(field_name, alias_fields[alias])
            except ValueError as exc:
                raise ValueError(f"转换 '{conversion.name}' 使用了未知字段: {field_ref}") from exc
        for expression in [_rewrite_expression(rule.formula, alias_fields), _rewrite_expression(rule.when, alias_fields)]:
            if expression is None:
                continue
            for alias, field_name in _FIELD_REF_RE.findall(expression):
                if alias not in alias_fields:
                    raise ValueError(f"转换 '{conversion.name}' 表达式使用了未知别名: {alias}")
                try:
                    _resolve_field_name(field_name, alias_fields[alias])
                except ValueError as exc:
                    raise ValueError(f"转换 '{conversion.name}' 表达式使用了未知字段: {alias}.{field_name}") from exc


def _build_runtime_maps(
    choreography: ChoreographySpec | None,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Builds runtime lookup maps for message conversion templates."""

    source_cache_keys: dict[str, str] = {}
    source_protocol_names: dict[str, str] = {}
    target_protocol_names: dict[str, str] = {}
    if choreography is None:
        return source_cache_keys, source_protocol_names, target_protocol_names

    for source in choreography.sources:
        source_cache_keys[source.message_type] = source.cache_key
        source_protocol_names[source.message_type] = source.protocol
    for target in choreography.targets:
        target_protocol_names[target.message_type] = target.protocol
    return source_cache_keys, source_protocol_names, target_protocol_names


def _build_default_endpoints(
    mappings: MappingSpec,
    source_cache_keys: dict[str, str],
    target_protocol_names: dict[str, str],
):
    """Builds fallback config.xml endpoints."""

    if mappings.runtime.endpoints:
        return mappings.runtime.endpoints
    endpoints = []
    recv_port = 4100
    send_port = 5100
    source_names: list[str] = []
    target_names: list[str] = []
    for conversion in mappings.conversions:
        for source in conversion.sources:
            source_name = source_cache_keys.get(source.protocol, source.protocol)
            if source_name not in source_names:
                source_names.append(source_name)
        target_name = target_protocol_names.get(conversion.target_protocol, conversion.target_protocol)
        if target_name not in target_names:
            target_names.append(target_name)
    from project_generator.models import EndpointSpec

    for index, name in enumerate(source_names):
        endpoints.append(
            EndpointSpec(
                ip="127.0.0.1",
                port=recv_port + index,
                net_type="udp",
                recv=True,
                feedback_port=recv_port + index,
                name=name,
            )
        )
    for index, name in enumerate(target_names):
        endpoints.append(
            EndpointSpec(
                ip="127.0.0.1",
                port=send_port + index,
                net_type="udp",
                recv=False,
                feedback_port=recv_port + index,
                name=name,
            )
        )
    return endpoints


def _serialize_nodes(nodes):
    """Serializes AST nodes into JSON-friendly dictionaries."""

    payload = []
    for node in nodes:
        if isinstance(node, ScalarNode):
            payload.append(
                {
                    "kind": "scalar",
                    "label": node.label,
                    "cpp_name": node.cpp_name,
                    "path": node.path,
                    "bit_length": node.bit_length,
                    "default_value": node.default_value,
                    "source_tag": node.source_tag,
                }
            )
        elif isinstance(node, BranchNode):
            payload.append(
                {
                    "kind": "branch",
                    "label": node.label,
                    "path": node.path,
                    "corr": node.corr,
                    "value": node.value,
                    "control_fields": list(node.control_fields),
                    "children": _serialize_nodes(node.children),
                }
            )
        elif isinstance(node, GroupNode):
            payload.append(
                {
                    "kind": "group",
                    "label": node.label,
                    "path": node.path,
                    "corr": node.corr,
                    "condition": node.condition,
                    "max_repeat": node.max_repeat,
                    "repeat_count": node.repeat_count,
                    "control_fields": list(node.control_fields),
                    "children": _serialize_nodes(node.children),
                }
            )
    return payload


def _build_manifest(
    protocols: list[ProtocolSpec],
    mappings: MappingSpec,
    choreography: ChoreographySpec | None,
) -> dict:
    """Builds protocol_manifest.json content."""

    payload = {
        "version": mappings.version,
        "project_name": mappings.project_name,
        "mode": "joint" if choreography else "simple",
        "runtime": {
            "loop_sleep_ms": mappings.runtime.loop_sleep_ms,
            "check_data_interval_ms": mappings.runtime.check_data_interval_ms,
            "reference_profile": mappings.runtime.reference_profile,
            "transport": (
                {
                    "message_type": mappings.runtime.transport.message_type,
                    "recv_ip": mappings.runtime.transport.recv_ip,
                    "recv_port": mappings.runtime.transport.recv_port,
                    "send_ip": mappings.runtime.transport.send_ip,
                    "send_port": mappings.runtime.transport.send_port,
                    "message_rules": [
                        {
                            "message_name": rule.message_name,
                            "delay_requirement": rule.delay_requirement,
                            "crc_check": {
                                "enabled": rule.crc_check.enabled,
                                "bind_element": rule.crc_check.bind_element,
                            },
                            "loop_config": {
                                "type": rule.loop_config.type,
                            },
                            "aggregation": {
                                "mode": rule.aggregation.mode,
                                "count": rule.aggregation.count,
                                "time_ms": rule.aggregation.time_ms,
                            },
                            "aggregation_type": {
                                "type": rule.aggregation_type.type,
                                "bind_element": rule.aggregation_type.bind_element,
                            },
                        }
                        for rule in mappings.runtime.transport.message_rules
                    ],
                }
                if mappings.runtime.transport is not None
                else None
            ),
            "protocol_verifies": [
                {
                    "protocol": item.protocol,
                    "constraints": [
                        {
                            "name": constraint.name,
                            "check": constraint.check,
                            "set": [
                                {"field": assignment.field, "value": assignment.value}
                                for assignment in constraint.assignments
                            ],
                        }
                        for constraint in item.constraints
                    ],
                    "verify_rules": [
                        {
                            "name": rule.name,
                            "when_seq": rule.when_seq,
                            "constraint": rule.constraint,
                        }
                        for rule in item.verify_rules
                    ],
                    "response_actions": [
                        {
                            "on_verify": action.on_verify,
                            "set_constraint": action.set_constraint,
                            "encode_seq": action.encode_seq,
                            "return_code": action.return_code,
                        }
                        for action in item.response_actions
                    ],
                    "default_verify": item.default_verify,
                    "default_return_code": item.default_return_code,
                }
                for item in mappings.runtime.protocol_verifies
            ],
            "endpoints": [
                {
                    "ip": item.ip,
                    "port": item.port,
                    "type": item.net_type,
                    "recv": item.recv,
                    "feedback_port": item.feedback_port,
                    "name": item.name,
                }
                for item in mappings.runtime.endpoints
            ],
        },
        "protocols": [],
        "conversions": [],
    }
    for protocol in protocols:
        payload["protocols"].append(
            {
                "type_name": protocol.type_name,
                "file_stem": protocol.file_stem,
                "source_path": str(protocol.source_path),
                "namespace": protocol.namespace,
                "dimen": {
                    "pack_head_length": protocol.dimen.pack_head_length,
                    "endian": protocol.dimen.endian,
                    "word_length": protocol.dimen.word_length,
                },
                "total_bits": protocol.total_bits,
                "structure_kind": protocol.structure_kind,
                "codec_supported": protocol.codec_supported,
                "unsupported_features": protocol.unsupported_features,
                "fields": [
                    {
                        "label": field.label,
                        "cpp_name": field.cpp_name,
                        "path": field.path,
                        "bit_length": field.bit_length,
                        "bit_offset": field.bit_offset,
                        "default_value": field.default_value,
                        "source_tag": field.source_tag,
                    }
                    for field in protocol.fields
                ],
                "nodes": _serialize_nodes(protocol.nodes),
                "sections": [
                    {
                        "name": section.name,
                        "cpp_name": section.cpp_name,
                        "tag_name": section.tag_name,
                        "path": section.path,
                        "nodes": _serialize_nodes(section.nodes),
                    }
                    for section in protocol.sections
                ],
                "sequences": [
                    {
                        "name": seq.name,
                        "cycle": seq.cycle,
                        "times": seq.times,
                        "members": [
                            {
                                "corr": member.corr,
                                "value": member.value,
                                "control_fields": list(member.control_fields),
                            }
                            for member in seq.members
                        ],
                    }
                    for seq in protocol.sequences
                ],
                "routes": [
                    {
                        "corr": route.corr,
                        "value": route.value,
                        "target_protocol": route.target_protocol,
                        "control_fields": list(route.control_fields),
                    }
                    for route in protocol.routes
                ],
            }
        )
    for conversion in mappings.conversions:
        payload["conversions"].append(
            {
                "name": conversion.name,
                "mode": conversion.mode,
                "sources": [{"alias": source.alias, "protocol": source.protocol} for source in conversion.sources],
                "target_protocol": conversion.target_protocol,
                "runtime": {
                    "process_method": conversion.runtime.process_method,
                    "usage_key": conversion.runtime.usage_key,
                    "response_enabled": conversion.runtime.response_enabled,
                    "send_mode": conversion.runtime.send_mode,
                    "cache_name": conversion.runtime.cache_name,
                    "cache_num": conversion.runtime.cache_num,
                    "sources": [
                        {
                            "alias": item.alias,
                            "message_name": item.message_name,
                            "display_name": item.display_name,
                            "fetches": [
                                {"count": fetch.count, "cycle_ms": fetch.cycle_ms}
                                for fetch in item.fetches
                            ],
                        }
                        for item in conversion.runtime.sources
                    ],
                },
                "rules": [
                    {
                        "target_field": rule.target_field,
                        "formula": rule.formula,
                        "source_fields": rule.source_fields,
                        "rule_type": rule.rule_type,
                        "when": rule.when,
                        "default_value": rule.default_value,
                        "description": rule.description,
                    }
                    for rule in conversion.rules
                ],
            }
        )
    if choreography is not None:
        payload["choreography"] = {
            "version": choreography.version,
            "mode": choreography.mode,
            "project_name": choreography.project_name,
            "sources": [
                {
                    "id": source.source_id,
                    "protocol": source.protocol,
                    "message_type": source.message_type,
                    "cache_key": source.cache_key,
                    "required": source.required,
                }
                for source in choreography.sources
            ],
            "targets": [
                {
                    "id": target.target_id,
                    "protocol": target.protocol,
                    "message_type": target.message_type,
                    "template_name": target.template_name,
                    "receive_window_ms": target.receive_window_ms,
                    "initial_status": target.initial_status,
                }
                for target in choreography.targets
            ],
            "joint_groups": [
                {
                    "group_id": group.group_id,
                    "target_id": group.target_id,
                    "sources": group.sources,
                    "trigger_policy": group.trigger_policy,
                    "matrix": {
                        "unit": group.matrix.unit,
                        "rows": group.matrix.rows,
                        "cols": group.matrix.cols,
                        "values": group.matrix.values,
                    },
                }
                for group in choreography.joint_groups
            ],
        }
    return payload


def _validate_project_inputs(
    protocol_lookup: dict[str, ProtocolSpec],
    mappings: MappingSpec,
    choreography: ChoreographySpec | None,
) -> None:
    """Validates the input combination before rendering."""

    if choreography is not None and choreography.project_name != mappings.project_name:
        raise ValueError("mappings.json 与 choreography.json 的 project_name 不一致")
    if choreography is None:
        for conversion in mappings.conversions:
            if conversion.mode == "joint":
                raise ValueError(f"转换 '{conversion.name}' 标记为 joint，但未提供 choreography.json")
    for conversion in mappings.conversions:
        if conversion.target_protocol not in protocol_lookup:
            raise ValueError(f"转换 '{conversion.name}' 使用了未知目标协议: {conversion.target_protocol}")
        _validate_field_references(conversion, protocol_lookup)


def render_project(
    output_dir: Path,
    protocols: list[ProtocolSpec],
    mappings: MappingSpec,
    choreography: ChoreographySpec | None,
) -> None:
    """Renders a complete Qt/C++ protocol conversion project."""

    ensure_directory(output_dir)
    joint = choreography is not None
    requested_profile = mappings.runtime.reference_profile
    selected_profile = requested_profile
    protocol_lookup = _build_protocol_lookup(protocols)
    protocol_verify_lookup = {
        item.protocol: item
        for item in mappings.runtime.protocol_verifies
    }
    if selected_profile is not None:
        render_reference_project(output_dir, selected_profile)
        write_text(output_dir / "README_GENERATOR.md", render_generator_readme())
        dump_json(
            output_dir / "protocol_manifest.json",
            _build_manifest(protocols=protocols, mappings=mappings, choreography=choreography),
        )
        return

    _validate_project_inputs(protocol_lookup, mappings, choreography)
    source_cache_keys, source_protocol_names, target_protocol_names = _build_runtime_maps(choreography)
    endpoints = _build_default_endpoints(mappings, source_cache_keys, target_protocol_names)

    protocol_header_names = []
    for protocol in protocols:
        header_name = f"{protocol.file_stem}_def.h"
        protocol_header_names.append(header_name)
        write_text(output_dir / header_name, render_protocol_header(protocol))

    mapping_header_names: list[str] = []
    mapping_source_names: list[str] = []
    process_methods = [_process_method_name(conversion) for conversion in mappings.conversions]
    for conversion in mappings.conversions:
        target_protocol = protocol_lookup[conversion.target_protocol]
        alias_fields = _build_alias_field_lookup(conversion, protocol_lookup)
        base_name = mapping_file_base(conversion)
        header_name = f"{base_name}.h"
        source_name = f"{base_name}.cpp"
        mapping_header_names.append(header_name)
        mapping_source_names.append(source_name)

        source_pairs = [
            (source.alias, protocol_lookup[source.protocol].type_name)
            for source in conversion.sources
        ]
        signature = _mapping_signature(conversion.name, target_protocol.type_name, source_pairs)
        include_names = [
            *(f"{protocol_lookup[source.protocol].file_stem}_def.h" for source in conversion.sources),
            f"{target_protocol.file_stem}_def.h",
        ]
        unique_includes = list(dict.fromkeys(include_names))
        write_text(
            output_dir / header_name,
            render_mapping_header(
                file_guard=f"{base_name.upper()}_H",
                function_signature=signature,
                includes=unique_includes,
            ),
        )
        write_text(
            output_dir / source_name,
            render_mapping_cpp(
                header_name=header_name,
                function_signature=signature,
                target_protocol=target_protocol.type_name,
                body=_mapping_body(conversion, target_protocol, alias_fields),
            ),
        )

    write_text(output_dir / "main.cpp", render_main_cpp())
    write_text(output_dir / "config.xml", render_config_xml(endpoints, mappings.runtime.transport))
    write_text(output_dir / "codec.h", render_codec_header(protocols=protocols, mapping_headers=mapping_header_names))
    write_text(output_dir / "codec.cpp", render_codec_cpp(protocols, protocol_verifies=protocol_verify_lookup))
    write_text(
        output_dir / "messageconvert.h",
        render_messageconvert_header(process_methods=process_methods, joint=joint),
    )
    write_text(
        output_dir / "messageconvert.cpp",
        render_messageconvert_cpp(
            conversions=mappings.conversions,
            protocol_lookup=protocol_lookup,
            source_cache_keys=source_cache_keys,
            source_protocol_names=source_protocol_names,
            target_protocol_names=target_protocol_names,
            joint=joint,
            loop_sleep_ms=mappings.runtime.loop_sleep_ms,
            check_data_interval_ms=mappings.runtime.check_data_interval_ms,
            transport=mappings.runtime.transport,
        ),
    )
    if choreography is not None:
        write_text(output_dir / "to_code_Choreography.h", render_choreography_header())
        write_text(output_dir / "to_code_Choreography.cpp", render_choreography_cpp(choreography))

    write_text(
        output_dir / "peach.pro",
        render_pro_file(
            project_name=mappings.project_name,
            headers=[*protocol_header_names, *mapping_header_names],
            sources=mapping_source_names,
            joint=joint,
        ),
    )
    write_text(output_dir / "README_GENERATOR.md", render_generator_readme())
    dump_json(
        output_dir / "protocol_manifest.json",
        _build_manifest(protocols=protocols, mappings=mappings, choreography=choreography),
    )
