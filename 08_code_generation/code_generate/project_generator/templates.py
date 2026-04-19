"""Text template helpers for generated Qt/C++ projects."""

from __future__ import annotations

from project_generator.models import (
    MessageRuleDetailSpec,
    BranchNode,
    ChoreographySpec,
    ConversionSpec,
    EndpointSpec,
    GroupNode,
    ProtocolVerifySpec,
    ProtocolNode,
    ProtocolSpec,
    ScalarNode,
    TransportSpec,
)
from project_generator.utils import normalize_token, to_snake_name


def _cpp_field_name(path_parts: tuple[str, ...]) -> str:
    """Builds one flattened C++ field name."""

    tokens = [normalize_token(part) for part in path_parts if part]
    return "_".join(token for token in tokens if token) or "field"


def _indent(level: int, lines: list[str]) -> list[str]:
    """Applies indentation to non-empty lines."""

    prefix = "    " * level
    return [f"{prefix}{line}" if line else "" for line in lines]


def _quoted(text: str) -> str:
    """Renders one QStringLiteral value."""

    return f'QStringLiteral("{text}")'


def _xml_attr(text: str | None) -> str:
    """Escapes one XML attribute value."""

    raw = str(text or "")
    return (
        raw.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def render_main_cpp() -> str:
    """Renders the shared main.cpp file."""

    return """#include <QCoreApplication>
#include <QDebug>
#include <QDomDocument>
#include <QFile>
#include <memory>
#include "messageconvert.h"

int readMessageXML(
    QString path,
    QVector<std::shared_ptr<messageConvert::NetInfo>>& netlist,
    QVector<std::shared_ptr<messageConvert::MessageRuleInfo>>& messageRuleList)
{
    QFile file(path);
    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qDebug() << "Cannot open file for reading:" << qPrintable(file.errorString());
        return 1;
    }
    QDomDocument doc;
    if (!doc.setContent(&file)) {
        qDebug() << "Failed to load document";
        file.close();
        return 2;
    }
    file.close();
    QDomElement root = doc.documentElement();
    QDomNodeList childNodes = root.childNodes();
    for (int index = 0; index < childNodes.count(); ++index) {
        QDomNode node = childNodes.at(index);
        if (!node.isElement()) continue;
        QDomElement element = node.toElement();
        if (element.tagName() == "Item") {
            auto ip = element.attributes().namedItem("ip");
            auto port = element.attributes().namedItem("port");
            auto type = element.attributes().namedItem("type");
            auto recv = element.attributes().namedItem("recv");
            auto name = element.attributes().namedItem("name");
            auto feedBackPort = element.attributes().namedItem("feedBackPort");
            std::shared_ptr<messageConvert::NetInfo> net(new messageConvert::NetInfo);
            net->ip = ip.nodeValue();
            net->name = name.nodeValue();
            net->port = port.nodeValue().toInt();
            net->feedBackPort = feedBackPort.nodeValue().toInt();
            net->bRecvTag = recv.nodeValue().toInt();
            if (type.nodeValue().toUpper() == "TCP") net->netType = messageConvert::emTCP;
            else if (type.nodeValue().toUpper() == "DDS") net->netType = messageConvert::emDDS;
            else net->netType = messageConvert::emUDP;
            netlist.push_back(net);
            continue;
        }
        if (element.tagName() == "Transport") {
            QDomNodeList messageRuleNodes = element.childNodes();
            for (int ruleIndex = 0; ruleIndex < messageRuleNodes.count(); ++ruleIndex) {
                QDomNode ruleNode = messageRuleNodes.at(ruleIndex);
                if (!ruleNode.isElement()) continue;
                QDomElement ruleElement = ruleNode.toElement();
                if (ruleElement.tagName() != "MessageRule") continue;
                std::shared_ptr<messageConvert::MessageRuleInfo> rule(new messageConvert::MessageRuleInfo);
                rule->messageName = ruleElement.attribute("messageName");
                rule->delayRequirement = ruleElement.attribute("delayRequirement").toInt();
                QDomNodeList filterNodes = ruleElement.childNodes();
                for (int filterIndex = 0; filterIndex < filterNodes.count(); ++filterIndex) {
                    QDomNode filterNode = filterNodes.at(filterIndex);
                    if (!filterNode.isElement()) continue;
                    QDomElement filterElement = filterNode.toElement();
                    if (filterElement.tagName() == "CrcCheck") {
                        rule->crcCheck.enabled = filterElement.attribute("enabled").toInt() != 0;
                        rule->crcCheck.bindElement = filterElement.attribute("bindElement");
                    } else if (filterElement.tagName() == "LoopConfig") {
                        rule->loopConfig.type = filterElement.attribute("type");
                    } else if (filterElement.tagName() == "Aggregation") {
                        rule->aggregation.mode = filterElement.attribute("mode");
                        rule->aggregation.count = filterElement.attribute("count").isEmpty() ? -1 : filterElement.attribute("count").toInt();
                        rule->aggregation.timeMs = filterElement.attribute("timeMs").isEmpty() ? -1 : filterElement.attribute("timeMs").toInt();
                    } else if (filterElement.tagName() == "AggregationType") {
                        rule->aggregationType.type = filterElement.attribute("type");
                        rule->aggregationType.bindElement = filterElement.attribute("bindElement");
                    }
                }
                messageRuleList.push_back(rule);
            }
        }
    }
    return 0;
}

int main(int argc, char* argv[])
{
    QCoreApplication application(argc, argv);
    QVector<std::shared_ptr<messageConvert::NetInfo>> netlist;
    QVector<std::shared_ptr<messageConvert::MessageRuleInfo>> messageRuleList;
    const QString configPath = QCoreApplication::applicationDirPath() + "/config.xml";
    readMessageXML(configPath, netlist, messageRuleList);
    messageConvert converter;
    converter.start(netlist, messageRuleList);
    return application.exec();
}
"""


def render_config_xml(endpoints: list[EndpointSpec], transport: TransportSpec | None = None) -> str:
    """Renders config.xml."""

    if not endpoints:
        endpoints = [
            EndpointSpec(
                ip="127.0.0.1",
                port=3333,
                net_type="udp",
                recv=True,
                feedback_port=3333,
                name="INPUT",
            ),
            EndpointSpec(
                ip="127.0.0.1",
                port=3336,
                net_type="udp",
                recv=False,
                feedback_port=3333,
                name="OUTPUT",
            ),
        ]
    items = []
    for endpoint in endpoints:
        items.append(
            "    "
            f'<Item ip="{endpoint.ip}" port="{endpoint.port}" type="{endpoint.net_type}" '
            f'recv="{1 if endpoint.recv else 0}" feedBackPort="{endpoint.feedback_port}" '
            f'name="{endpoint.name}" />'
        )
    if transport is not None:
        items.append(
            "    "
            f'<Transport messageType="{_xml_attr(transport.message_type)}" '
            f'recvIp="{_xml_attr(transport.recv_ip)}" recvPort="{transport.recv_port}" '
            f'sendIp="{_xml_attr(transport.send_ip)}" sendPort="{transport.send_port}">'
        )
        for rule in transport.message_rules:
            items.append(
                "        "
                f'<MessageRule messageName="{_xml_attr(rule.message_name)}" '
                f'delayRequirement="{rule.delay_requirement}">'
            )
            items.append(
                "            "
                f'<CrcCheck enabled="{1 if rule.crc_check.enabled else 0}" '
                f'bindElement="{_xml_attr(rule.crc_check.bind_element)}" />'
            )
            items.append(
                "            "
                f'<LoopConfig type="{_xml_attr(rule.loop_config.type)}" />'
            )
            items.append(
                "            "
                f'<Aggregation mode="{_xml_attr(rule.aggregation.mode)}" '
                f'count="{"" if rule.aggregation.count is None else rule.aggregation.count}" '
                f'timeMs="{"" if rule.aggregation.time_ms is None else rule.aggregation.time_ms}" />'
            )
            items.append(
                "            "
                f'<AggregationType type="{_xml_attr(rule.aggregation_type.type)}" '
                f'bindElement="{_xml_attr(rule.aggregation_type.bind_element)}" />'
            )
            items.append("        </MessageRule>")
        items.append("    </Transport>")
    return "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<NameSpace>\n" + "\n".join(items) + "\n</NameSpace>\n"


def render_protocol_header(protocol: ProtocolSpec) -> str:
    """Renders one protocol definition header."""

    guard = f"{protocol.type_name.upper()}_DEF_H"
    field_lines = []
    for field in protocol.fields:
        default_value = field.default_value if field.default_value is not None else "0"
        field_lines.append(f"    long {field.cpp_name} = {default_value};  // {field.path}")
    return f"""#ifndef {guard}
#define {guard}

class {protocol.type_name} {{
public:
{chr(10).join(field_lines)}
}};

#endif
"""


def mapping_file_base(conversion: ConversionSpec) -> str:
    """Returns the mapping file base name for one conversion."""

    source_part = "_".join(to_snake_name(source.protocol) for source in conversion.sources)
    target_part = to_snake_name(conversion.target_protocol)
    return f"{source_part}_to_{target_part}"


def render_codec_header(protocols: list[ProtocolSpec], mapping_headers: list[str]) -> str:
    """Renders codec.h."""

    includes = [f'#include "{header}"' for header in mapping_headers]
    declarations: list[str] = []
    for protocol in protocols:
        declarations.extend(
            [
                f"QString decodeMsg(uchar* pData, int len, {protocol.type_name}& value);",
                f"void encodeMsg(QByteArray& data, {protocol.type_name}& value);",
                f"int checkObjMaps(QString strVerify, QByteArray& data, {protocol.type_name}& value);",
                "",
            ]
        )
    return (
        "#ifndef CODEC_H\n#define CODEC_H\n\n"
        + "\n".join(includes)
        + "\n#include <QByteArray>\n#include <QString>\n\n"
        + "\n".join(declarations).rstrip()
        + "\n\n#endif\n"
    )


def _control_expr(control_fields: tuple[str, ...], values: str | None, protocol: ProtocolSpec) -> str:
    """Builds a branch-control expression."""

    if not control_fields or not values:
        return "true"
    targets = [part.strip() for part in values.split(",")]
    checks: list[str] = []
    for index, field_label in enumerate(control_fields):
        cpp_name = protocol.label_to_cpp.get(field_label)
        if cpp_name is None:
            continue
        expected = targets[min(index, len(targets) - 1)]
        checks.append(f"value.{cpp_name} == {expected}")
    return " && ".join(checks) or "true"


def _group_repeat_expr(node: GroupNode, protocol: ProtocolSpec, var_name: str) -> list[str]:
    """Builds repeat-count lines for one group."""

    control_cpp = protocol.label_to_cpp.get(node.control_fields[0], "") if node.control_fields else ""
    limit = node.repeat_count
    if control_cpp:
        return [
            f"int {var_name} = static_cast<int>(value.{control_cpp});",
            f"if ({var_name} < 0) {var_name} = 0;",
            f"if ({var_name} > {limit}) {var_name} = {limit};",
        ]
    return [f"const int {var_name} = {limit};"]


def _render_decode_nodes(
    nodes: list[ProtocolNode],
    protocol: ProtocolSpec,
    path_parts: tuple[str, ...],
    level: int,
    endian_func: str,
    loop_index: int = 0,
) -> tuple[list[str], int]:
    """Renders decode statements for one node list."""

    lines: list[str] = []
    current_loop_index = loop_index
    for node in nodes:
        if isinstance(node, ScalarNode):
            field_name = _cpp_field_name(path_parts + (node.label,))
            if node.bit_length is None:
                lines.extend(_indent(level, [f"Q_UNUSED(value.{field_name});"]))
                continue
            lines.extend(
                _indent(
                    level,
                    [
                        f"if (bitOffset + {node.bit_length} > len * 8) return;",
                        f"value.{field_name} = static_cast<long>({endian_func}(raw, bitOffset, {node.bit_length}));",
                    ],
                )
            )
            continue
        if isinstance(node, BranchNode):
            condition = _control_expr(node.control_fields, node.value, protocol)
            nested_lines, current_loop_index = _render_decode_nodes(
                node.children,
                protocol,
                path_parts + (node.label,),
                level + 1,
                endian_func,
                current_loop_index,
            )
            lines.extend(_indent(level, [f"if ({condition}) {{"]))
            lines.extend(nested_lines)
            lines.extend(_indent(level, ["}"]))
            continue

        repeat_var = f"repeatCount_{current_loop_index}"
        current_loop_index += 1
        lines.extend(_indent(level, _group_repeat_expr(node, protocol, repeat_var)))
        for index in range(node.repeat_count):
            lines.extend(_indent(level, [f"if ({repeat_var} > {index}) {{"]))
            nested_lines, current_loop_index = _render_decode_nodes(
                node.children,
                protocol,
                path_parts + (f"{node.label}_{index + 1}" if node.repeat_count > 1 else node.label,),
                level + 1,
                endian_func,
                current_loop_index,
            )
            lines.extend(nested_lines)
            lines.extend(_indent(level, ["}"]))
    return lines, current_loop_index


def _render_encode_nodes(
    nodes: list[ProtocolNode],
    protocol: ProtocolSpec,
    path_parts: tuple[str, ...],
    level: int,
    endian_func: str,
    loop_index: int = 0,
) -> tuple[list[str], int]:
    """Renders encode statements for one node list."""

    lines: list[str] = []
    current_loop_index = loop_index
    for node in nodes:
        if isinstance(node, ScalarNode):
            field_name = _cpp_field_name(path_parts + (node.label,))
            if node.bit_length is None:
                continue
            lines.extend(
                _indent(
                    level,
                    [f"{endian_func}(data, static_cast<quint64>(value.{field_name}), {node.bit_length});"],
                )
            )
            continue
        if isinstance(node, BranchNode):
            condition = _control_expr(node.control_fields, node.value, protocol)
            nested_lines, current_loop_index = _render_encode_nodes(
                node.children,
                protocol,
                path_parts + (node.label,),
                level + 1,
                endian_func,
                current_loop_index,
            )
            lines.extend(_indent(level, [f"if ({condition}) {{"]))
            lines.extend(nested_lines)
            lines.extend(_indent(level, ["}"]))
            continue

        repeat_var = f"repeatCount_{current_loop_index}"
        current_loop_index += 1
        lines.extend(_indent(level, _group_repeat_expr(node, protocol, repeat_var)))
        for index in range(node.repeat_count):
            lines.extend(_indent(level, [f"if ({repeat_var} > {index}) {{"]))
            nested_lines, current_loop_index = _render_encode_nodes(
                node.children,
                protocol,
                path_parts + (f"{node.label}_{index + 1}" if node.repeat_count > 1 else node.label,),
                level + 1,
                endian_func,
                current_loop_index,
            )
            lines.extend(nested_lines)
            lines.extend(_indent(level, ["}"]))
    return lines, current_loop_index


def _section_func_suffix(section_name: str) -> str:
    """Builds one function suffix from a section name."""

    token = normalize_token(section_name)
    parts = [part for part in token.split("_") if part]
    return "".join(part[:1].upper() + part[1:] for part in parts) or "Origin"


def _member_condition(protocol: ProtocolSpec, member, value_name: str) -> str:
    """Builds one sequence member condition expression."""

    if not member.control_fields:
        return "true"
    targets = [part.strip() for part in (member.value or "").split(",") if part.strip()]
    checks: list[str] = []
    for index, field_label in enumerate(member.control_fields):
        cpp_name = protocol.label_to_cpp.get(field_label)
        if cpp_name is None:
            continue
        expected = targets[min(index, len(targets) - 1)] if targets else "0"
        checks.append(f"{value_name}.{cpp_name} == {expected}")
    return " && ".join(checks) or "true"


def _resolve_protocol_field_name(protocol: ProtocolSpec, field_name: str) -> str:
    """Resolves one protocol field name for verify-state generation."""

    valid_fields = {field.cpp_name for field in protocol.fields}
    if field_name in valid_fields:
        return field_name
    if field_name.startswith("namespace_"):
        candidate = field_name[len("namespace_") :]
        if candidate in valid_fields:
            return candidate
    normalized = normalize_token(field_name)
    if normalized in valid_fields:
        return normalized
    return field_name


def _render_sequence_helpers(protocol: ProtocolSpec, include_verify: bool = True) -> str:
    """Renders sequence helper functions."""

    sequences = protocol.sequences or []
    if not sequences:
        lines = [
            f"static QString check{protocol.type_name}SeqNum(const QString& seqNum)",
            "{",
            '    return seqNum.isEmpty() ? QStringLiteral("Seq_1") : seqNum;',
            "}",
        ]
        if include_verify:
            lines.extend(
                [
                    "",
                    f"static QString Verify{protocol.type_name}Seq({protocol.type_name}& value, const QString& seq)",
                    "{",
                    "    Q_UNUSED(value);",
                    '    return seq.isEmpty() ? QStringLiteral("Seq_1") : seq;',
                    "}",
                ]
            )
        return "\n".join(lines) + "\n"

    lines: list[str] = []
    for sequence in sequences:
        condition = " && ".join(_member_condition(protocol, member, "value") for member in sequence.members) or "true"
        lines.append(f"static bool match{protocol.type_name}_{sequence.name}({protocol.type_name}& value)")
        lines.append("{")
        lines.append(f"    return {condition};")
        lines.append("}")
        lines.append("")
    lines.append(f"static QString check{protocol.type_name}SeqNum(const QString& seqNum)")
    lines.append("{")
    for sequence in sequences:
        lines.append(f"    if (seqNum == {_quoted(sequence.name)}) return {_quoted(sequence.name)};")
    lines.append("    return QString();")
    lines.append("}")
    if include_verify:
        lines.append("")
        lines.append(f"static QString Verify{protocol.type_name}Seq({protocol.type_name}& value, const QString& seq)")
        lines.append("{")
        for sequence in sequences:
            lines.append(f"    if (seq == {_quoted(sequence.name)} && match{protocol.type_name}_{sequence.name}(value)) return {_quoted(sequence.name)};")
        lines.append("    return QString();")
        lines.append("}")
    return "\n".join(lines) + "\n"


def _render_verify_state_machine(protocol: ProtocolSpec, verify_spec: ProtocolVerifySpec) -> str:
    """Renders verify and response-state helpers for one protocol."""

    lines: list[str] = []
    for constraint in verify_spec.constraints:
        lines.extend(
            [
                f"static bool checkConstraint_{constraint.name}({protocol.type_name}& value)",
                "{",
                f"    return {constraint.check or 'true'};",
                "}",
                "",
                f"static bool setConstraint_{constraint.name}({protocol.type_name}& value)",
                "{",
            ]
        )
        if constraint.assignments:
            for assignment in constraint.assignments:
                field_name = _resolve_protocol_field_name(protocol, assignment.field)
                lines.append(
                    f"    value.{field_name} = static_cast<decltype(value.{field_name})>({assignment.value});"
                )
            lines.append("    return true;")
        else:
            lines.extend(["    Q_UNUSED(value);", "    return true;"])
        lines.extend(["}", ""])

    for rule in verify_spec.verify_rules:
        condition = f"seq == {_quoted(rule.when_seq)}"
        if rule.constraint:
            condition = f"{condition} && checkConstraint_{rule.constraint}(value)"
        lines.extend(
            [
                f"static QString checkVerify_{rule.name}({protocol.type_name}& value, const QString& seq)",
                "{",
                f"    return ({condition}) ? {_quoted(rule.name)} : QString();",
                "}",
                "",
            ]
        )

    for index, action in enumerate(verify_spec.response_actions, start=1):
        lines.extend(
            [
                f"static bool applyResponse_{index}({protocol.type_name}& value, QByteArray& data)",
                "{",
            ]
        )
        if action.set_constraint:
            lines.append(f"    setConstraint_{action.set_constraint}(value);")
        if action.encode_seq:
            lines.append(f"    write{action.encode_seq}(value, data);")
        else:
            lines.append("    data.clear();")
        lines.extend(["    return true;", "}", ""])

    lines.extend(
        [
            f"static QString Verify{protocol.type_name}Seq({protocol.type_name}& value, const QString& seq)",
            "{",
        ]
    )
    for rule in verify_spec.verify_rules:
        lines.extend(
            [
                f"    QString verify_{rule.name} = checkVerify_{rule.name}(value, seq);",
                f"    if (verify_{rule.name}.isEmpty() == false) return verify_{rule.name};",
            ]
        )
    if verify_spec.default_verify is not None:
        lines.append(f"    return {_quoted(verify_spec.default_verify)};")
    else:
        lines.append("    return QString();")
    lines.extend(["}", "", f"int checkObjMaps(QString strVerify, QByteArray& data, {protocol.type_name}& value)", "{"])
    for index, action in enumerate(verify_spec.response_actions, start=1):
        lines.extend(
            [
                f"    if (strVerify == {_quoted(action.on_verify)}) {{",
                f"        applyResponse_{index}(value, data);",
                f"        return {action.return_code};",
                "    }",
            ]
        )
    lines.append("    data.clear();")
    lines.append(f"    return {verify_spec.default_return_code};")
    lines.append("}")
    return "\n".join(lines) + "\n"


def _render_codec_impl(protocol: ProtocolSpec, verify_spec: ProtocolVerifySpec | None = None) -> str:
    """Renders codec.cpp functions for one protocol."""

    read_func = "readBitsLE" if protocol.endian == "little" else "readBits"
    append_func = "appendBitsLE" if protocol.endian == "little" else "appendBits"
    section_helpers: list[str] = []
    decode_calls: list[str] = []
    encode_section_calls: list[str] = []
    for section in protocol.sections:
        suffix = _section_func_suffix(section.name)
        decode_lines, _ = _render_decode_nodes(section.nodes, protocol, (), 1, read_func)
        encode_lines, _ = _render_encode_nodes(section.nodes, protocol, (), 1, append_func)
        if not decode_lines:
            decode_lines = _indent(1, ["Q_UNUSED(value);", "Q_UNUSED(raw);", "Q_UNUSED(len);", "Q_UNUSED(bitOffset);"])
        if not encode_lines:
            encode_lines = _indent(1, ["Q_UNUSED(value);", "Q_UNUSED(data);"])
        section_helpers.append(
            "\n".join(
                [
                    f"static void read{suffix}({protocol.type_name}& value, const QByteArray& raw, int len, int& bitOffset)",
                    "{",
                    *decode_lines,
                    "}",
                    "",
                    f"static void write{suffix}({protocol.type_name}& value, QByteArray& data)",
                    "{",
                    *encode_lines,
                    "}",
                ]
            )
        )
        decode_calls.append(f"    read{suffix}(value, raw, len, bitOffset);")
        encode_section_calls.append(f"    write{suffix}(value, data);")

    sequences = protocol.sequences or []
    default_seq_name = sequences[0].name if sequences else "Seq_1"
    write_seq_names = [sequence.name for sequence in sequences] or [default_seq_name]

    seq_choose_lines = [
        f"    if (match{protocol.type_name}_{sequence.name}(value)) return {_quoted(sequence.name)};"
        for sequence in sequences
    ]
    encode_default_return_line = f"    return {_quoted(default_seq_name)};"

    decode_match_lines = [
        f"    if (match{protocol.type_name}_{sequence.name}(value)) return Verify{protocol.type_name}Seq(value, {_quoted(sequence.name)});"
        for sequence in sequences
    ]
    if not decode_match_lines:
        decode_match_lines.append(f"    return Verify{protocol.type_name}Seq(value, {_quoted(default_seq_name)});")
    else:
        decode_match_lines.append(f"    return Verify{protocol.type_name}Seq(value, {_quoted(default_seq_name)});")

    write_seq_blocks: list[str] = []
    for seq_name in write_seq_names:
        write_seq_blocks.append(
            "\n".join(
                [
                    f"static void write{seq_name}({protocol.type_name}& value, QByteArray& data)",
                    "{",
                    "    data.clear();",
                    *encode_section_calls,
                    "}",
                ]
            )
        )

    encode_dispatch_lines = [
        f"    if (seq == {_quoted(seq_name)}) {{ write{seq_name}(value, data); return; }}"
        for seq_name in write_seq_names
    ]
    if not encode_dispatch_lines:
        encode_dispatch_lines = [f"    write{default_seq_name}(value, data);", "    return;"]

    check_obj_dispatch_lines = [
        f"    if (seq == {_quoted(seq_name)}) {{ write{seq_name}(value, data); return 0; }}"
        for seq_name in write_seq_names
    ]

    sequence_helpers_text = _render_sequence_helpers(protocol, include_verify=verify_spec is None)
    verify_state_text = _render_verify_state_machine(protocol, verify_spec) if verify_spec is not None else ""
    section_helpers_text = "\n\n".join(section_helpers)
    write_seq_text = "\n\n".join(write_seq_blocks)
    decode_calls_text = "\n".join(decode_calls)
    seq_choose_text = "\n".join(seq_choose_lines)
    decode_match_text = "\n".join(decode_match_lines)
    encode_dispatch_text = "\n".join(encode_dispatch_lines)
    check_obj_dispatch_text = "\n".join(check_obj_dispatch_lines)

    generic_check_obj_text = f"""int checkObjMaps(QString strVerify, QByteArray& data, {protocol.type_name}& value)
{{
    const QString seq = check{protocol.type_name}SeqNum(strVerify);
    if (seq.isEmpty()) {{
        data.clear();
        return -1;
    }}
    if (Verify{protocol.type_name}Seq(value, seq).isEmpty()) {{
        data.clear();
        return -1;
    }}
{check_obj_dispatch_text}
    data.clear();
    return -1;
}}"""

    return f"""{sequence_helpers_text}
{verify_state_text}
{section_helpers_text}

{write_seq_text}

{generic_check_obj_text if verify_spec is None else ""}

QString decodeMsg(uchar* pData, int len, {protocol.type_name}& value)
{{
    QByteArray raw(reinterpret_cast<const char*>(pData), len);
    int bitOffset = 0;
{decode_calls_text}
{decode_match_text}
}}

static QString checkEncodeSeqNumber({protocol.type_name}& value)
{{
{seq_choose_text}
{encode_default_return_line}
}}

static void VerifyField({protocol.type_name}& value)
{{
    Q_UNUSED(value);
}}

static void updateFieldValue({protocol.type_name}& value)
{{
    Q_UNUSED(value);
}}

static void updateGroupFlag({protocol.type_name}& value)
{{
    Q_UNUSED(value);
}}

void encodeMsg(QByteArray& data, {protocol.type_name}& value)
{{
    const QString seq = checkEncodeSeqNumber(value);
    VerifyField(value);
    updateFieldValue(value);
    updateGroupFlag(value);
{encode_dispatch_text}
    data.clear();
    write{default_seq_name}(value, data);
}}
"""


def render_codec_cpp(
    protocols: list[ProtocolSpec],
    protocol_verifies: dict[str, ProtocolVerifySpec] | None = None,
) -> str:
    """Renders codec.cpp."""

    blocks = [
        '#include "codec.h"',
        "#include <QStringList>",
        "#include <QtGlobal>",
        "",
        "namespace {",
        "quint64 readBits(const QByteArray& data, int& bitOffset, int bitLength)",
        "{",
        "    quint64 value = 0;",
        "    for (int index = 0; index < bitLength; ++index) {",
        "        const int absoluteBit = bitOffset + index;",
        "        const int byteIndex = absoluteBit / 8;",
        "        const int bitIndex = 7 - (absoluteBit % 8);",
        "        if (byteIndex >= data.size()) return value;",
        "        const quint8 byteValue = static_cast<quint8>(data.at(byteIndex));",
        "        value = (value << 1) | ((byteValue >> bitIndex) & 0x01);",
        "    }",
        "    bitOffset += bitLength;",
        "    return value;",
        "}",
        "",
        "quint64 readBitsLE(const QByteArray& data, int& bitOffset, int bitLength)",
        "{",
        "    quint64 value = 0;",
        "    for (int index = 0; index < bitLength; ++index) {",
        "        const quint64 bitValue = readBits(data, bitOffset, 1);",
        "        value |= (bitValue << index);",
        "    }",
        "    return value;",
        "}",
        "",
        "void appendBits(QByteArray& data, quint64 value, int bitLength)",
        "{",
        "    const int startBit = data.size() * 8;",
        "    const int totalBits = startBit + bitLength;",
        "    const int requiredBytes = (totalBits + 7) / 8;",
        "    if (data.size() < requiredBytes) data.append(QByteArray(requiredBytes - data.size(), '\\0'));",
        "    for (int index = 0; index < bitLength; ++index) {",
        "        const int absoluteBit = startBit + index;",
        "        const int byteIndex = absoluteBit / 8;",
        "        const int bitIndex = 7 - (absoluteBit % 8);",
        "        const quint64 bitValue = (value >> (bitLength - index - 1)) & 0x01ULL;",
        "        char byteValue = data[byteIndex];",
        "        if (bitValue != 0) byteValue = static_cast<char>(byteValue | (1 << bitIndex));",
        "        else byteValue = static_cast<char>(byteValue & ~(1 << bitIndex));",
        "        data[byteIndex] = byteValue;",
        "    }",
        "}",
        "",
        "void appendBitsLE(QByteArray& data, quint64 value, int bitLength)",
        "{",
        "    for (int index = 0; index < bitLength; ++index) appendBits(data, (value >> index) & 0x01ULL, 1);",
        "}",
        "}  // namespace",
        "",
    ]
    verify_lookup = protocol_verifies or {}
    blocks.extend(_render_codec_impl(protocol, verify_lookup.get(protocol.type_name)) for protocol in protocols)
    return "\n".join(blocks).rstrip() + "\n"


def render_mapping_header(file_guard: str, function_signature: str, includes: list[str]) -> str:
    """Renders one mapping header."""

    include_lines = "\n".join(f'#include "{header}"' for header in includes)
    return f"""#ifndef {file_guard}
#define {file_guard}

{include_lines}

{function_signature};

#endif
"""


def render_mapping_cpp(header_name: str, function_signature: str, target_protocol: str, body: str) -> str:
    """Renders one mapping source file."""

    return f"""#include "{header_name}"
#include <algorithm>
#include <cmath>

namespace {{
inline double clamp(double value, double low, double high)
{{
    return std::max(low, std::min(value, high));
}}
}}

{function_signature}
{{
    {target_protocol} target;
{body}    return target;
}}
"""


def render_choreography_header() -> str:
    """Renders the choreography header."""

    return """#ifndef TO_CODE_CHOREOGRAPHY_H
#define TO_CODE_CHOREOGRAPHY_H

#include <QMap>
#include <QObject>

class code_test {
public:
    static qulonglong getDstMsg_41(QString name);
    static qulonglong getSrcTime_41(QString s1, QString s2);
    QMap<QString, uint> getAllSrcTime_41();
    QMap<QString, uint> getAllDstTime_41();
    int getStatus_41(QString s1);
};

#endif
"""


def render_choreography_cpp(spec: ChoreographySpec) -> str:
    """Renders the choreography implementation."""

    dest_proto_list = ",".join(_quoted(target.protocol) for target in spec.targets)
    template_list = ",".join(_quoted(target.template_name) for target in spec.targets)
    status_list = ",".join("true" if target.initial_status == "cache" else "false" for target in spec.targets)
    src_list = ",".join(_quoted(source.protocol) for source in spec.sources)
    receive_windows = ",".join(str(target.receive_window_ms) for target in spec.targets)
    matrix_rows = spec.joint_groups[0].matrix.values if spec.joint_groups else []
    matrix_cpp_rows = []
    for row in matrix_rows:
        rendered = ",".join("-1" if value is None else str(value) for value in row)
        matrix_cpp_rows.append("{" + rendered + "}")
    matrix_cpp = ",".join(matrix_cpp_rows) if matrix_cpp_rows else "{0}"
    return f"""#include "to_code_Choreography.h"

QVector<QString> destProtoList_41 = {{{dest_proto_list}}};
QVector<QString> templateList_41 = {{{template_list}}};
QVector<bool> statusList41 = {{{status_list}}};
QVector<QString> src_list_41 = {{{src_list}}};
QVector<qulonglong> src_receive_time_list_41 = {{{receive_windows}}};
QVector<QVector<int>> target_send_martix_41 = {{{matrix_cpp}}};

qulonglong code_test::getDstMsg_41(QString name)
{{
    int pos = -1;
    for (int index = 0; index < destProtoList_41.size(); ++index) {{
        if (destProtoList_41[index] == name) {{
            pos = index;
            break;
        }}
    }}
    if (pos == -1) return pos;
    return src_receive_time_list_41[pos];
}}

qulonglong code_test::getSrcTime_41(QString s1, QString s2)
{{
    int left = -1;
    int right = -1;
    for (int index = 0; index < src_list_41.size(); ++index) {{
        if (src_list_41[index] == s1) left = index;
        if (src_list_41[index] == s2) right = index;
    }}
    if (left == -1 || right == -1) return -1;
    return target_send_martix_41[left][right];
}}

QMap<QString, uint> code_test::getAllSrcTime_41()
{{
    QMap<QString, uint> result;
    for (int index = 0; index < templateList_41.size(); ++index) result[templateList_41[index]] = src_receive_time_list_41[index];
    return result;
}}

QMap<QString, uint> code_test::getAllDstTime_41()
{{
    QMap<QString, uint> result;
    for (int left = 0; left < src_list_41.size(); ++left) {{
        for (int right = 0; right < src_list_41.size(); ++right) result[src_list_41[left] + ":" + src_list_41[right]] = target_send_martix_41[left][right];
    }}
    return result;
}}

int code_test::getStatus_41(QString s1)
{{
    int pos = -1;
    for (int index = 0; index < destProtoList_41.size(); ++index) {{
        if (destProtoList_41[index] == s1) pos = index;
    }}
    if (pos == -1) return -1;
    return statusList41[pos];
}}
"""


def _render_qmake_block(items: list[str]) -> str:
    """Renders one qmake item block."""

    lines = []
    last_index = len(items) - 1
    for index, item in enumerate(items):
        suffix = " \\" if index != last_index else ""
        lines.append(f"\t{item}{suffix}")
    return "\n".join(lines)


def render_pro_file(project_name: str, headers: list[str], sources: list[str], joint: bool) -> str:
    """Renders peach.pro."""

    all_sources = ["main.cpp", "messageconvert.cpp", *sources, "codec.cpp"]
    if joint:
        all_sources.append("to_code_Choreography.cpp")
    all_headers = ["messageconvert.h", *headers, "codec.h"]
    if joint:
        all_headers.append("to_code_Choreography.h")
    return f"""QT = core xml network concurrent

CONFIG += c++17 cmdline
TARGET = {project_name}
SOURCES += \\
{_render_qmake_block(all_sources)}

HEADERS += \\
{_render_qmake_block(all_headers)}
"""


def render_messageconvert_header(process_methods: list[str], joint: bool) -> str:
    """Renders messageconvert.h."""

    extra_slot = "    void onCheckDataTimer();\n" if joint else ""
    extra_member = "    QTimer checkDataTimer;\n" if joint else ""
    extra_check = "    void checkData(QString name, int time);\n" if joint else ""
    state_decl = "QStringList state = {};" if joint else "int state = 0;"
    process_decls = "\n".join(f"    void {method}();" for method in process_methods)
    return f"""#ifndef MESSAGECONVERT_H
#define MESSAGECONVERT_H

#include <QObject>
#include <QHostAddress>
#include <QMap>
#include <QMutex>
#include <QStringList>
#include <QTimer>
#include <QUdpSocket>
#include <QVector>
#include <memory>

class messageConvert : public QObject
{{
    Q_OBJECT
public:
    explicit messageConvert(QObject* parent = nullptr);
    enum NetType {{ emTCP, emUDP, emDDS }};
    class NetInfo {{ public: QString name; QString ip; int port = 0; quint16 feedBackPort = 0; int netType = emUDP; bool bRecvTag = true; }};
    class CrcCheckInfo {{ public: bool enabled = false; QString bindElement; }};
    class LoopConfigInfo {{ public: QString type = QStringLiteral("NONE"); }};
    class AggregationInfo {{ public: QString mode = QStringLiteral("SINGLE"); int count = -1; int timeMs = -1; }};
    class AggregationTypeInfo {{ public: QString type = QStringLiteral("TIME"); QString bindElement; }};
    class MessageRuleInfo {{
    public:
        QString messageName;
        int delayRequirement = 0;
        CrcCheckInfo crcCheck;
        LoopConfigInfo loopConfig;
        AggregationInfo aggregation;
        AggregationTypeInfo aggregationType;
    }};
    class msgDataInfo {{ public: QByteArray data; QVector<qulonglong> time; QString name; QString ip; quint16 port = 0; {state_decl} int num = 0; }};

signals:
    void showMessage(QString msg);

public slots:
    void readPendingDatagrams(QString name, QHostAddress ip, quint16 port, QByteArray data);
{extra_slot}private:
    int _maxThread = 5;
    int _threadExit = 0;
    std::shared_ptr<QUdpSocket> udpSend;
    QVector<std::shared_ptr<NetInfo>> udpSendList;
    QVector<std::shared_ptr<QUdpSocket>> udpRecvList;
    QVector<std::shared_ptr<MessageRuleInfo>> messageRuleList;
    QMap<QString, QString> crcValueMap;
    QVector<std::shared_ptr<msgDataInfo>> dataInfo;
    QMutex dataMutex;
{extra_member}    void pushData(std::shared_ptr<msgDataInfo> data);
    void getData(QString name, int time, int num, QByteArray& data, QString& ip, int& port, int& outTime);
{extra_check}    void msgConvertThread();
    void onSendMessage(QByteArray msg);
    QString computeCrc16Hex(const QString& raw) const;
    void cacheCrcValue(const QString& messageName, const QString& bindElement, const QString& rawValue);
    void cacheGeneratedTarget(const QString& targetName, int num, const QByteArray& data);
{process_decls}

public:
    int start(QVector<std::shared_ptr<NetInfo>> netlist, QVector<std::shared_ptr<MessageRuleInfo>> ruleList, int maxThread = 5);
    int stop();
}};

#endif
"""


def _fetch_runtime_source(conversion: ConversionSpec, alias: str):
    """Returns runtime metadata for one alias."""

    for item in conversion.runtime.sources:
        if item.alias == alias:
            return item
    return None


def _match_message_rules(transport: TransportSpec | None, protocol: ProtocolSpec) -> list[MessageRuleDetailSpec]:
    """Returns runtime message rules applicable to one protocol."""

    if transport is None:
        return []
    protocol_keys = {
        normalize_token(protocol.type_name),
        normalize_token(protocol.file_stem),
    }
    return [
        rule
        for rule in transport.message_rules
        if normalize_token(rule.message_name) in protocol_keys
    ]


def _resolve_bind_field(protocol: ProtocolSpec, bind_element: str | None) -> str | None:
    """Resolves one bind-element display name to a flattened C++ field name."""

    candidate = str(bind_element or "").strip()
    if not candidate:
        return None
    candidate_key = normalize_token(candidate)
    mapped = protocol.label_to_cpp.get(candidate)
    if mapped:
        return mapped
    for field in protocol.fields:
        keys = {
            normalize_token(field.cpp_name),
            normalize_token(field.label),
            normalize_token(field.path_parts[-1] if field.path_parts else field.cpp_name),
        }
        if candidate_key in keys:
            return field.cpp_name
    return None


def _render_crc_capture_lines(
    protocol: ProtocolSpec,
    value_var: str,
    transport: TransportSpec | None,
) -> list[str]:
    """Renders CRC calculation lines for one decoded/encoded protocol object."""

    lines: list[str] = []
    for rule in _match_message_rules(transport, protocol):
        if not rule.crc_check.enabled:
            continue
        field_name = _resolve_bind_field(protocol, rule.crc_check.bind_element)
        if not field_name:
            continue
        lines.append(
            f'cacheCrcValue({_quoted(rule.message_name)}, {_quoted(rule.crc_check.bind_element or "")}, '
            f'QString::number(static_cast<qlonglong>({value_var}.{field_name})));'
        )
    return lines


def _method_name(conversion: ConversionSpec) -> str:
    """Returns one generated process method name."""

    if conversion.runtime.process_method:
        return conversion.runtime.process_method
    if len(conversion.sources) == 1:
        return f"{conversion.sources[0].protocol}dataPro"
    source_part = "_".join(source.protocol for source in conversion.sources)
    return f"{source_part}dataPro"


def _render_process_function(
    conversion: ConversionSpec,
    protocol_lookup: dict[str, ProtocolSpec],
    source_cache_keys: dict[str, str],
    source_protocol_names: dict[str, str],
    target_protocol_names: dict[str, str],
    joint: bool,
    transport: TransportSpec | None,
) -> str:
    """Renders one conversion process method."""

    method_name = _method_name(conversion)
    lines = [f"void messageConvert::{method_name}()", "{"]
    if joint:
        lines.extend(_indent(1, ["QStringList msgNameList;", "QVector<int> msgTimeList;"]))
    for source in conversion.sources:
        runtime_source = _fetch_runtime_source(conversion, source.alias)
        protocol = protocol_lookup[source.protocol]
        message_name = (
            runtime_source.message_name
            if runtime_source and runtime_source.message_name
            else source_cache_keys.get(source.protocol, source.protocol)
        )
        display_name = (
            runtime_source.display_name
            if runtime_source and runtime_source.display_name
            else source_protocol_names.get(source.protocol, source.protocol)
        )
        fetches = runtime_source.fetches if runtime_source else []
        counts = ", ".join(str(item.count) for item in fetches)
        cycles = ", ".join(str(item.cycle_ms) for item in fetches)
        count_size = len(fetches)
        base = source.alias
        lines.extend(
            _indent(
                1,
                [
                    f"QByteArray {base}Data;",
                    f"{protocol.type_name} {base} = {{0}};",
                    f"int {base}Flag = 0;",
                    f"QString {base}Ip;",
                    f"int {base}Port = 0;",
                    f"int {base}Time = 0;",
                    f"int count_{base}[{count_size}] = {{ {counts} }};",
                    f"int cycle_{base}[{count_size}] = {{ {cycles} }};",
                    f"int num_{base} = {count_size};",
                    f"while (num_{base}-- > 0) {{",
                    f"    getData({_quoted(message_name)}, cycle_{base}[num_{base}], count_{base}[num_{base}], {base}Data, {base}Ip, {base}Port, {base}Time);",
                    f"    if ({base}Data.isEmpty() == false) {{",
                    f"        QString ret = decodeMsg((uchar*){base}Data.data(), {base}Data.size(), {base});",
                    "        if (ret.isEmpty() == false) {",
                    *_render_crc_capture_lines(protocol, base, transport),
                    "            QByteArray sdata;",
                    f"            int iret = checkObjMaps(ret, sdata, {base});",
                    f"            if (iret == 0) {base}Flag = 1;",
                ],
            )
        )
        if conversion.runtime.response_enabled:
            lines.extend(
                _indent(
                    3,
                    [
                        f"if (iret != -1 && {base}Port > 0) {{",
                        "    QUdpSocket soc;",
                        f"    soc.writeDatagram(sdata, QHostAddress({base}Ip), {base}Port);",
                        "}",
                    ],
                )
            )
        lines.extend(
            _indent(
                3,
                [
                    "}",
                    "break;",
                    "    }",
                    "}",
                    f"if (1 != {base}Flag) return;",
                ],
            )
        )
        if joint:
            lines.extend(_indent(1, [f"msgNameList.append({_quoted(display_name)});", f"msgTimeList.append({base}Time);"]))
    if joint:
        lines.extend(
            _indent(
                1,
                [
                    "if (msgNameList.size() >= 2) {",
                    "    int state = 0;",
                    "    for (int i = 0; i < msgNameList.size() - 1; ++i) {",
                    "        for (int j = i + 1; j < msgNameList.size(); ++j) {",
                    "            int s = code_test::getSrcTime_41(msgNameList[i], msgNameList[j]);",
                    "            if (-1 == s || (s + (msgTimeList[i] - msgTimeList[j])) > 0) state += 1;",
                    "        }",
                    "    }",
                    "    if (msgNameList.size() != state + 1) return;",
                    "}",
                ],
            )
        )
    args = ", ".join(source.alias for source in conversion.sources)
    target_protocol = protocol_lookup[conversion.target_protocol]
    target_name = target_protocol_names.get(conversion.target_protocol, conversion.target_protocol)
    cache_name = conversion.runtime.cache_name or target_protocol.type_name
    send_mode = conversion.runtime.send_mode or "direct"
    lines.extend(
        _indent(
            1,
            [
                f"{target_protocol.type_name} target = convert_{to_snake_name(conversion.name)}({args});",
                *_render_crc_capture_lines(target_protocol, "target", transport),
                "QByteArray sendData;",
                "encodeMsg(sendData, target);",
            ],
        )
    )
    if joint:
        lines.extend(
            _indent(
                1,
                [
                    "code_test check;",
                    f"int sflag = check.getStatus_41({_quoted(target_name)});",
                    "if (0 == sflag) onSendMessage(sendData);",
                    f"else cacheGeneratedTarget({_quoted(cache_name)}, {conversion.runtime.cache_num}, sendData);",
                ],
            )
        )
    else:
        if send_mode == "cache":
            lines.extend(
                _indent(
                    1,
                    [f"cacheGeneratedTarget({_quoted(cache_name)}, {conversion.runtime.cache_num}, sendData);"],
                )
            )
        else:
            lines.extend(_indent(1, ["onSendMessage(sendData);"]))
    lines.append("}")
    return "\n".join(lines)


def render_messageconvert_cpp(
    conversions: list[ConversionSpec],
    protocol_lookup: dict[str, ProtocolSpec],
    source_cache_keys: dict[str, str],
    source_protocol_names: dict[str, str],
    target_protocol_names: dict[str, str],
    joint: bool,
    loop_sleep_ms: int,
    check_data_interval_ms: int,
    transport: TransportSpec | None,
) -> str:
    """Renders messageconvert.cpp."""

    process_methods = [_method_name(conversion) for conversion in conversions]
    process_blocks = [
        _render_process_function(
            conversion,
            protocol_lookup,
            source_cache_keys,
            source_protocol_names,
            target_protocol_names,
            joint,
            transport,
        )
        for conversion in conversions
    ]
    timer_block = ""
    if joint:
        checks = [
            f"    checkData({_quoted(name)}, static_cast<int>(code_test::getDstMsg_41({_quoted(target_protocol_names[name])})));"
            for name in target_protocol_names
        ]
        timer_block = "\n".join(
            [
                "void messageConvert::onCheckDataTimer()",
                "{",
                *checks,
                "}",
                "",
                "void messageConvert::checkData(QString name, int time)",
                "{",
                "    QMutexLocker lock(&dataMutex);",
                "    for (int i = 0; i < dataInfo.size(); ++i) {",
                "        int ll = static_cast<int>(QDateTime::currentMSecsSinceEpoch() - dataInfo[i]->time.last());",
                "        if (ll > time && name == dataInfo[i]->name) {",
                "            dataInfo.remove(i);",
                "            return;",
                "        }",
                "    }",
                "}",
                "",
            ]
        )
    joint_include = '#include "to_code_Choreography.h"\n' if joint else ""
    joint_start = ""
    joint_stop = ""
    if joint:
        joint_start = (
            "    connect(&checkDataTimer, &QTimer::timeout, this, &messageConvert::onCheckDataTimer);\n"
            f"    checkDataTimer.start({check_data_interval_ms});\n"
        )
        joint_stop = "    checkDataTimer.stop();\n"
        push_data_reset_line = "                dataInfo[i]->state.clear();"
        push_data_duplicate_lines = "\n".join(
            [
                "                dataInfo[i]->num++;",
                "                dataInfo[i]->time.append(data->time.last());",
                "                dataInfo[i]->state.clear();",
            ]
        )
        get_data_condition = 'if (name == item->name && (num == item->num) && item->state.indexOf(name) == -1) {'
        get_data_mark = "            item->state.append(name);"
    else:
        push_data_reset_line = "                dataInfo[i]->state = 0;"
        push_data_duplicate_lines = "\n".join(
            [
                "                dataInfo[i]->num++;",
                "                dataInfo[i]->state = 0;",
            ]
        )
        get_data_condition = "if (name == item->name && (num <= item->num) && item->state == 0) {"
        get_data_mark = "            item->state = 1;"
    process_calls = "\n".join(f"        {method}();" for method in process_methods)
    return f"""#include "messageconvert.h"
#include "codec.h"
#include <QDateTime>
#include <QDebug>
#include <QMutexLocker>
#include <QtConcurrent>
{joint_include}
messageConvert::messageConvert(QObject* parent)
    : QObject(parent)
{{
}}

int messageConvert::start(QVector<std::shared_ptr<NetInfo>> netlist, QVector<std::shared_ptr<MessageRuleInfo>> ruleList, int maxThread)
{{
    _maxThread = maxThread;
    messageRuleList = ruleList;
    udpSend.reset(new QUdpSocket());
    for (auto serv : netlist) {{
        if (serv->bRecvTag == false) {{
            udpSendList.push_back(serv);
        }} else {{
            std::shared_ptr<QUdpSocket> soc(new QUdpSocket);
            connect(soc.get(), &QUdpSocket::readyRead, [serv, soc, this]() {{
                while (soc->hasPendingDatagrams()) {{
                    QHostAddress sender;
                    quint16 senderPort = 0;
                    qint64 size = soc->pendingDatagramSize();
                    QByteArray buffer(size, 0);
                    soc->readDatagram(buffer.data(), size, &sender, &senderPort);
                    readPendingDatagrams(serv->name, sender, serv->feedBackPort, buffer);
                }}
            }});
            if (!soc->bind(QHostAddress::Any, serv->port)) return -1;
            udpRecvList.push_back(soc);
        }}
    }}
    QtConcurrent::run([this]() {{ this->msgConvertThread(); }});
{joint_start}    return 0;
}}

QString messageConvert::computeCrc16Hex(const QString& raw) const
{{
    QByteArray bytes = raw.toUtf8();
    quint16 crc = 0xFFFF;
    for (unsigned char byte : bytes) {{
        crc ^= static_cast<quint16>(byte);
        for (int i = 0; i < 8; ++i) {{
            if (crc & 0x0001) crc = static_cast<quint16>((crc >> 1) ^ 0xA001);
            else crc = static_cast<quint16>(crc >> 1);
        }}
    }}
    return QStringLiteral("%1").arg(crc, 4, 16, QChar('0')).toUpper();
}}

void messageConvert::cacheCrcValue(const QString& messageName, const QString& bindElement, const QString& rawValue)
{{
    const QString crc = computeCrc16Hex(rawValue);
    const QString key = messageName + QStringLiteral(":") + bindElement;
    crcValueMap.insert(key, crc);
    qDebug() << "CRC_VALUE" << key << crc;
}}

int messageConvert::stop()
{{
    _threadExit = 1;
{joint_stop}    for (auto var : udpRecvList) {{
        if (var->isOpen()) var->close();
    }}
    if (udpSend && udpSend->isOpen()) udpSend->close();
    udpRecvList.clear();
    return 0;
}}

void messageConvert::onSendMessage(QByteArray msg)
{{
    for (auto var : udpSendList) udpSend->writeDatagram(msg, QHostAddress(var->ip), var->port);
}}

void messageConvert::readPendingDatagrams(QString name, QHostAddress ip, quint16 port, QByteArray data)
{{
    std::shared_ptr<msgDataInfo> d(new msgDataInfo);
    d->time.append(QDateTime::currentMSecsSinceEpoch());
    d->name = name;
    d->num = 1;
    d->data = data;
    d->ip = ip.toString();
    d->port = port;
    pushData(d);
}}

void messageConvert::pushData(std::shared_ptr<msgDataInfo> data)
{{
    QMutexLocker lock(&dataMutex);
    for (int i = 0; i < dataInfo.size(); ++i) {{
        if (data->name == dataInfo[i]->name) {{
            if (data->data != dataInfo[i]->data) {{
                dataInfo[i] = data;
                dataInfo[i]->time = data->time;
{push_data_reset_line}
            }} else {{
{push_data_duplicate_lines}
            }}
            return;
        }}
    }}
    dataInfo.push_back(data);
}}

void messageConvert::getData(QString name, int time, int num, QByteArray& data, QString& ip, int& port, int& outTime)
{{
    QMutexLocker lock(&dataMutex);
    for (auto item : dataInfo) {{
        {get_data_condition}
            for (int i = item->time.size() - 1; i >= 1; --i) {{
                if (item->time[i] - item->time[i - 1] <= time) return;
            }}
            ip = item->ip;
            port = item->port;
            data = item->data;
            outTime = static_cast<int>(item->time.first());
{get_data_mark}
            return;
        }}
    }}
}}

void messageConvert::cacheGeneratedTarget(const QString& targetName, int num, const QByteArray& data)
{{
    std::shared_ptr<msgDataInfo> d(new msgDataInfo);
    d->time.append(QDateTime::currentMSecsSinceEpoch());
    d->name = targetName;
    d->num = num;
    d->data = data;
    d->ip = QStringLiteral("127.0.0.1");
    d->port = 0;
    pushData(d);
}}

{timer_block}{chr(10).join(process_blocks)}

void messageConvert::msgConvertThread()
{{
    while (0 == _threadExit) {{
{process_calls}
        _sleep({loop_sleep_ms});
    }}
}}
"""


def render_generator_readme() -> str:
    """Renders generator usage documentation."""

    return """# Python 协议转换项目生成器

## 用法

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --output output/demo_project
```

联合转换模式：

```bash
python -m project_generator build --protocol-dir input/protocols --mappings input/mappings.json --choreography input/choreography.json --output output/demo_project
```

## 输入说明

- XML: 协议结构与 `MessCode` 序列
- `mappings.json`: 字段公式、运行时抓取策略、端口配置
- `choreography.json`: 联合转换目标窗口、时序矩阵、缓存发送策略

## 当前能力

- 解析 `Item/StructMess/Field/Group/MessCode`
- 生成 `*_def.h`、`codec.*`、`messageconvert.*`、映射文件、`main.cpp`、`config.xml`、`peach.pro`
- 联合模式生成 `to_code_Choreography.*`
- `codec.cpp` 按 AST 递归生成分支和循环读写逻辑
- `process_method / message_name / display_name / cache_name / cache_num` 支持自动推导或默认补齐
"""
