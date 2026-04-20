"""XML parsing support for protocol definition files."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from project_generator.models import (
    BranchNode,
    DimenSpec,
    FieldSpec,
    GroupNode,
    ProtocolNode,
    ProtocolSpec,
    RouteSpec,
    ScalarNode,
    SectionSpec,
    SequenceMember,
    SequenceSpec,
)
from project_generator.utils import normalize_token, to_snake_name, to_type_name


def _local_name(tag: str) -> str:
    """Extracts the local tag name from one XML element tag."""

    if "}" in tag:
        return tag.split("}", 1)[1]
    if ":" in tag:
        return tag.split(":", 1)[1]
    return tag


def _tag_prefix(tag: str) -> str:
    """Returns the tag prefix when present."""

    if "}" in tag:
        raw = tag.split("}", 1)[1]
    else:
        raw = tag
    if ":" in raw:
        return raw.split(":", 1)[0]
    return raw


def _namespace_uri(tag: str) -> str:
    """Returns the XML namespace URI when present."""

    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def _normalize_path_parts(parts: list[str]) -> tuple[str, ...]:
    """Builds a stable normalized path tuple."""

    return tuple(part for part in parts if part)


def _build_field_name(path_parts: tuple[str, ...]) -> str:
    """Builds a stable C++ field identifier from one nested XML path."""

    tokens = [normalize_token(part) for part in path_parts if part]
    return "_".join(token for token in tokens if token) or "field"


def _parse_int(raw: str | None, fallback: int | None = None) -> int | None:
    """Parses an integer when possible."""

    if raw is None:
        return fallback
    text = raw.strip()
    if not text:
        return fallback
    try:
        return int(text)
    except ValueError:
        return fallback


def _parse_bit_length(node: ET.Element) -> int | None:
    """Parses a bit-length value from one XML node."""

    candidates = [
        (node.text or "").strip(),
        node.attrib.get("bitLength", "").strip(),
        node.attrib.get("length", "").strip(),
    ]
    for candidate in candidates:
        value = _parse_int(candidate)
        if value is not None:
            return value
    return None


def _append_feature(features: list[str], feature: str) -> None:
    """Appends one feature once."""

    if feature not in features:
        features.append(feature)


def _corr_fields(raw_corr: str | None) -> tuple[str, ...]:
    """Extracts referenced control-field names from one corr expression."""

    if not raw_corr:
        return ()
    parts: list[str] = []
    for chunk in raw_corr.split(","):
        token = chunk.strip()
        if not token:
            continue
        parts.append(token.rsplit(".", 1)[-1].strip())
    return tuple(parts)


def _match_control_default(
    control_fields: tuple[str, ...],
    label_defaults: dict[str, str | None],
) -> int | None:
    """Returns a numeric default value from referenced control fields."""

    for label in control_fields:
        default_value = label_defaults.get(label)
        parsed = _parse_int(default_value)
        if parsed is not None:
            return parsed
    return None


def _flatten_nodes(
    nodes: list[ProtocolNode],
    label_defaults: dict[str, str | None],
    features: list[str],
    path_parts: tuple[str, ...] = (),
    bit_offset: int = 0,
) -> tuple[list[FieldSpec], int]:
    """Flattens AST nodes into field manifest entries."""

    fields: list[FieldSpec] = []
    current_offset = bit_offset
    for node in nodes:
        if isinstance(node, ScalarNode):
            fields.append(
                FieldSpec(
                    label=node.label,
                    cpp_name=_build_field_name(path_parts + (node.label,)),
                    path="/".join(path_parts + (node.label,)),
                    path_parts=path_parts + (node.label,),
                    bit_length=node.bit_length,
                    bit_offset=current_offset,
                    default_value=node.default_value,
                    source_tag=node.source_tag,
                )
            )
            if node.bit_length is not None:
                current_offset += node.bit_length
            continue
        if isinstance(node, BranchNode):
            _append_feature(features, "branch")
            nested_fields, current_offset = _flatten_nodes(
                node.children,
                label_defaults,
                features,
                path_parts + (node.label,),
                current_offset,
            )
            fields.extend(nested_fields)
            continue

        _append_feature(features, "loop")
        repeat_count = node.repeat_count
        if node.max_repeat is not None and node.repeat_count > 1:
            _append_feature(features, "group_max_repeat")
        if node.max_repeat is None and repeat_count > 1:
            _append_feature(features, "group_default_repeat")
        for index in range(repeat_count):
            group_label = f"{node.label}_{index + 1}" if repeat_count > 1 else node.label
            nested_fields, current_offset = _flatten_nodes(
                node.children,
                label_defaults,
                features,
                path_parts + (group_label,),
                current_offset,
            )
            fields.extend(nested_fields)
    return fields, current_offset


def _parse_children(
    container: ET.Element,
    label_defaults: dict[str, str | None],
    features: list[str],
    path_parts: tuple[str, ...] = (),
) -> list[ProtocolNode]:
    """Parses one XML subtree into protocol nodes."""

    nodes: list[ProtocolNode] = []
    for child in list(container):
        local = _local_name(child.tag)
        label = child.attrib.get("name", local)
        node_path = path_parts + (label,)
        if local in {"Item", "StructMess"}:
            default_value = child.attrib.get("defaultValue")
            label_defaults[label] = default_value
            nodes.append(
                ScalarNode(
                    label=label,
                    cpp_name=_build_field_name(node_path),
                    path="/".join(node_path),
                    path_parts=node_path,
                    bit_length=_parse_bit_length(child),
                    default_value=default_value,
                    source_tag=local,
                )
            )
            continue
        if local == "Field":
            nodes.append(
                BranchNode(
                    label=label,
                    path="/".join(node_path),
                    path_parts=node_path,
                    corr=child.attrib.get("corr"),
                    value=child.attrib.get("value"),
                    control_fields=_corr_fields(child.attrib.get("corr")),
                    children=_parse_children(child, label_defaults, features, node_path),
                )
            )
            continue
        if local == "Group":
            control_fields = _corr_fields(child.attrib.get("corr"))
            max_repeat = _parse_int(child.attrib.get("max"))
            repeat_count = (
                max_repeat
                if max_repeat is not None
                else (_match_control_default(control_fields, label_defaults) or 1)
            )
            nodes.append(
                GroupNode(
                    label=label,
                    path="/".join(node_path),
                    path_parts=node_path,
                    corr=child.attrib.get("corr"),
                    condition=child.attrib.get("condition"),
                    max_repeat=max_repeat,
                    repeat_count=max(1, repeat_count),
                    control_fields=control_fields,
                    children=_parse_children(child, label_defaults, features, node_path),
                )
            )
            continue
        if local == "MessCode":
            _append_feature(features, "sequence")
            continue
        if local == "Dimen":
            continue
        nodes.extend(_parse_children(child, label_defaults, features, path_parts))
    return nodes


def _section_name_from_element(node: ET.Element) -> str:
    """Builds one section name from a top-level XML element."""

    raw_name = node.attrib.get("name", "").strip()
    if raw_name:
        return raw_name
    namespace_uri = _namespace_uri(node.tag).strip().rstrip("/")
    if namespace_uri:
        tail = namespace_uri.split("/")[-1].strip()
        if tail:
            return tail
    prefix = _tag_prefix(node.tag)
    if "_" in prefix:
        return prefix.split("_")[-1]
    return prefix or _local_name(node.tag)


def _parse_sections(
    root: ET.Element,
    label_defaults: dict[str, str | None],
    features: list[str],
) -> list[SectionSpec]:
    """Parses top-level section containers."""

    sections: list[SectionSpec] = []
    for child in list(root):
        local = _local_name(child.tag)
        if local != "NameSpace":
            continue
        section_name = _section_name_from_element(child)
        sections.append(
            SectionSpec(
                name=section_name,
                cpp_name=normalize_token(section_name),
                tag_name=_tag_prefix(child.tag),
                path=section_name,
                nodes=_parse_children(child, label_defaults, features),
            )
        )
    return sections


def _parse_dimen(root: ET.Element) -> DimenSpec:
    """Parses the optional Dimen node."""

    for child in list(root):
        if _local_name(child.tag) != "Dimen":
            continue
        endian_value = child.attrib.get("endian", "").strip()
        endian = "little" if endian_value in {"0", "little", "Little", "LE", "le"} else "big"
        return DimenSpec(
            pack_head_length=_parse_int(child.attrib.get("packHeadLength"), 0) or 0,
            endian=endian,
            word_length=_parse_int(child.attrib.get("wordLength"), -1) or -1,
        )
    return DimenSpec()


def _parse_sequences(root: ET.Element) -> list[SequenceSpec]:
    """Parses MessCode/PreSeq definitions."""

    sequences: list[SequenceSpec] = []
    for child in list(root):
        if _local_name(child.tag) != "MessCode":
            continue
        for pre_seq in list(child):
            if _local_name(pre_seq.tag) != "PreSeq":
                continue
            members: list[SequenceMember] = []
            for member in list(pre_seq):
                if _local_name(member.tag) != "Member":
                    continue
                members.append(
                    SequenceMember(
                        corr=member.attrib.get("corr"),
                        value=(member.text or "").strip() or member.attrib.get("value"),
                        control_fields=_corr_fields(member.attrib.get("corr")),
                    )
                )
            sequences.append(
                SequenceSpec(
                    name=pre_seq.attrib.get("name", f"Seq_{len(sequences) + 1}"),
                    cycle=_parse_int(pre_seq.attrib.get("cycle"), 0) or 0,
                    times=_parse_int(pre_seq.attrib.get("times"), 1) or 1,
                    members=members,
                )
            )
    return sequences


def _parse_routes(root: ET.Element) -> list[RouteSpec]:
    """Parses root-level route selectors such as k.xml."""

    routes: list[RouteSpec] = []
    for child in list(root):
        if _local_name(child.tag) != "Field":
            continue
        target_protocol = (child.text or "").strip()
        if not target_protocol:
            continue
        routes.append(
            RouteSpec(
                corr=child.attrib.get("corr"),
                value=child.attrib.get("value"),
                target_protocol=target_protocol,
                control_fields=_corr_fields(child.attrib.get("corr")),
            )
        )
    return routes


def _determine_structure_kind(features: list[str]) -> str:
    """Determines the XML structure kind from collected features."""

    if "loop" in features:
        return "loop"
    if "branch" in features:
        return "branch"
    return "fixed_length"


def parse_protocol_file(path: Path) -> ProtocolSpec:
    """Parses one protocol XML file into a protocol specification."""

    root = ET.fromstring(path.read_text(encoding="utf-8"))
    namespace = root.attrib.get("xmlns", "")
    raw_name = path.stem.replace(".", "_")
    type_name = to_type_name(raw_name)
    label_defaults: dict[str, str | None] = {}
    features: list[str] = []
    sections = _parse_sections(root, label_defaults, features)
    nodes = [node for section in sections for node in section.nodes]
    if not sections:
        nodes = _parse_children(root, label_defaults, features)
        sections = [
            SectionSpec(
                name="Origin",
                cpp_name="origin",
                tag_name="Origin",
                path="Origin",
                nodes=nodes,
            )
        ]
    fields, total_bits = _flatten_nodes(nodes, label_defaults, features)
    if not fields:
        fields.append(
            FieldSpec(
                label="payload",
                cpp_name="payload",
                path="payload",
                path_parts=("payload",),
                bit_length=None,
                bit_offset=0,
                default_value="0",
                source_tag="Generated",
            )
        )
    label_to_cpp = {field.label: field.cpp_name for field in fields}
    return ProtocolSpec(
        type_name=type_name,
        file_stem=to_snake_name(raw_name),
        source_path=path,
        namespace=namespace,
        dimen=_parse_dimen(root),
        total_bits=total_bits,
        structure_kind=_determine_structure_kind(features),
        codec_supported=True,
        unsupported_features=features,
        fields=fields,
        nodes=nodes,
        sections=sections,
        sequences=_parse_sequences(root),
        routes=_parse_routes(root),
        label_to_cpp=label_to_cpp,
    )


def load_protocols(protocol_dir: Path) -> list[ProtocolSpec]:
    """Loads all XML protocol definitions from a directory."""

    files = sorted(protocol_dir.glob("*.xml"))
    if not files:
        raise ValueError(f"未在目录中找到 XML 协议文件: {protocol_dir}")
    return [parse_protocol_file(file_path) for file_path in files]
