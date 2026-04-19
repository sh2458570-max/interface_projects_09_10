"""JSON loading and validation for generator inputs."""

from __future__ import annotations

import json
import re
from pathlib import Path

from project_generator.models import (
    ChoreographySource,
    ChoreographySpec,
    ChoreographyTarget,
    ConstraintAssignment,
    ConstraintSpec,
    ConversionRuntime,
    ConversionSpec,
    EndpointSpec,
    FetchAttempt,
    JointGroup,
    MappingRule,
    MappingSpec,
    MatrixSpec,
    ProtocolVerifySpec,
    ResponseActionSpec,
    RuntimeSpec,
    SourceAlias,
    SourceRuntime,
    VerifyRuleSpec,
)


_FORMULA_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_\s\.\+\-\*\/%\(\),<>=!&\|\?:]+$")
_FUNCTION_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_ALLOWED_FUNCTIONS = {
    "abs",
    "min",
    "max",
    "pow",
    "round",
    "floor",
    "ceil",
    "clamp",
}
_RESERVED_TOKENS = {"if", "for", "while", "return", "include"}


def _load_json(path: Path) -> dict:
    """Loads a JSON object from disk."""

    return json.loads(path.read_text(encoding="utf-8-sig"))


def _validate_expression(expression: str | None, aliases: set[str]) -> None:
    """Validates one formula or condition expression."""

    if expression is None:
        return
    if not _FORMULA_ALLOWED_RE.fullmatch(expression):
        raise ValueError(f"非法表达式字符: {expression}")
    lowered = expression.lower()
    for token in _RESERVED_TOKENS:
        if token in lowered:
            raise ValueError(f"表达式包含禁止关键字 '{token}': {expression}")
    for function_name in _FUNCTION_RE.findall(expression):
        if function_name not in _ALLOWED_FUNCTIONS and function_name not in aliases:
            raise ValueError(f"未授权函数或标识符 '{function_name}': {expression}")


def _load_fetches(payload: list[dict] | None) -> list[FetchAttempt]:
    """Loads one fetch-attempt list."""

    fetches = payload or []
    result = [FetchAttempt(count=int(item["count"]), cycle_ms=int(item["cycle_ms"])) for item in fetches]
    return result or [FetchAttempt(count=1, cycle_ms=0)]


def _load_runtime(payload: dict | None, source_aliases: list[SourceAlias]) -> ConversionRuntime:
    """Loads conversion runtime metadata."""

    runtime_payload = payload or {}
    runtime_sources: list[SourceRuntime] = []
    source_lookup = {source.alias: source for source in source_aliases}
    for alias, source in source_lookup.items():
        runtime_sources.append(
            SourceRuntime(
                alias=alias,
                message_name=None,
                display_name=None,
                fetches=[FetchAttempt(count=1, cycle_ms=0)],
            )
        )
    configured_sources = {item["alias"]: item for item in runtime_payload.get("sources", [])}
    for index, runtime_source in enumerate(runtime_sources):
        configured = configured_sources.get(runtime_source.alias)
        if configured is None:
            continue
        runtime_sources[index] = SourceRuntime(
            alias=runtime_source.alias,
            message_name=configured.get("message_name", runtime_source.message_name),
            display_name=configured.get("display_name", runtime_source.display_name),
            fetches=_load_fetches(configured.get("fetches")),
        )
    return ConversionRuntime(
        process_method=runtime_payload.get("process_method"),
        usage_key=runtime_payload.get("usage_key"),
        sources=runtime_sources,
        response_enabled=runtime_payload.get("response_enabled", True),
        send_mode=runtime_payload.get("send_mode", "direct"),
        cache_name=runtime_payload.get("cache_name"),
        cache_num=int(runtime_payload.get("cache_num", 3)),
    )


def _load_endpoints(payload: list[dict] | None) -> list[EndpointSpec]:
    """Loads config.xml endpoint settings."""

    result: list[EndpointSpec] = []
    for item in payload or []:
        result.append(
            EndpointSpec(
                ip=item.get("ip", "127.0.0.1"),
                port=int(item["port"]),
                net_type=item.get("type", "udp"),
                recv=bool(int(item["recv"])) if isinstance(item.get("recv"), str) else bool(item["recv"]),
                feedback_port=int(item.get("feed_back_port", item.get("feedBackPort", item["port"]))),
                name=item["name"],
            )
        )
    return result


def _load_protocol_verifies(payload: dict | None) -> list[ProtocolVerifySpec]:
    """Loads protocol-level verify/response state-machine settings."""

    if not payload:
        return []
    result: list[ProtocolVerifySpec] = []
    for protocol_name, item in payload.items():
        constraints: list[ConstraintSpec] = []
        for index, constraint in enumerate(item.get("constraints", []), start=1):
            constraints.append(
                ConstraintSpec(
                    name=constraint.get("name", f"Constraint{index}"),
                    check=constraint.get("check"),
                    assignments=[
                        ConstraintAssignment(field=assignment["field"], value=str(assignment["value"]))
                        for assignment in constraint.get("set", [])
                    ],
                )
            )
        verify_rules: list[VerifyRuleSpec] = []
        for index, rule in enumerate(item.get("verify_rules", []), start=1):
            default_constraint = constraints[index - 1].name if index - 1 < len(constraints) else None
            verify_rules.append(
                VerifyRuleSpec(
                    name=rule.get("name", f"verify{index}"),
                    when_seq=rule["when_seq"],
                    constraint=rule.get("constraint", default_constraint),
                )
            )
        response_actions: list[ResponseActionSpec] = []
        for index, action in enumerate(item.get("response_actions", []), start=1):
            default_verify = verify_rules[index - 1].name if index - 1 < len(verify_rules) else None
            if action.get("on_verify", default_verify) is None:
                raise ValueError(f"协议 '{protocol_name}' 的 response_actions[{index}] 缺少 on_verify，且无法按顺序推导")
            response_actions.append(
                ResponseActionSpec(
                    on_verify=action.get("on_verify", default_verify),
                    set_constraint=action.get("set_constraint"),
                    encode_seq=action.get("encode_seq"),
                    return_code=int(action.get("return_code", 0)),
                )
            )
        result.append(
            ProtocolVerifySpec(
                protocol=protocol_name,
                constraints=constraints,
                verify_rules=verify_rules,
                response_actions=response_actions,
                default_verify=item.get("default_verify"),
                default_return_code=int(item.get("default_return_code", -1)),
            )
        )
    return result


def load_mappings(path: Path) -> MappingSpec:
    """Loads and validates mappings.json."""

    payload = _load_json(path)
    conversions: list[ConversionSpec] = []
    seen_names: set[str] = set()
    for item in payload.get("conversions", []):
        name = item["name"]
        if name in seen_names:
            raise ValueError(f"重复的转换名称: {name}")
        seen_names.add(name)
        sources = [
            SourceAlias(alias=src["alias"], protocol=src["protocol"])
            for src in item.get("sources", [])
        ]
        alias_names = {source.alias for source in sources}
        if len(alias_names) != len(sources):
            raise ValueError(f"转换 '{name}' 中存在重复别名")
        rules: list[MappingRule] = []
        seen_target_fields: set[str] = set()
        for rule in item.get("rules", []):
            target_field = rule["target_field"]
            if target_field in seen_target_fields:
                raise ValueError(f"转换 '{name}' 中重复赋值字段: {target_field}")
            seen_target_fields.add(target_field)
            _validate_expression(rule["formula"], alias_names)
            _validate_expression(rule.get("when"), alias_names)
            rules.append(
                MappingRule(
                    target_field=target_field,
                    formula=rule["formula"],
                    source_fields=rule.get("source_fields", []),
                    rule_type=rule["rule_type"],
                    when=rule.get("when"),
                    default_value=(
                        None if rule.get("default_value") is None else str(rule.get("default_value"))
                    ),
                    description=rule.get("description"),
                )
            )
        conversions.append(
            ConversionSpec(
                name=name,
                mode=item["mode"],
                sources=sources,
                target_protocol=item["target"]["protocol"],
                rules=rules,
                runtime=_load_runtime(item.get("runtime"), sources),
            )
        )
    runtime_payload = payload.get("runtime", {})
    return MappingSpec(
        version=payload.get("version", "1.0"),
        project_name=payload["project_name"],
        conversions=conversions,
        runtime=RuntimeSpec(
            endpoints=_load_endpoints(runtime_payload.get("endpoints")),
            loop_sleep_ms=int(runtime_payload.get("loop_sleep_ms", 2)),
            check_data_interval_ms=int(runtime_payload.get("check_data_interval_ms", 5000)),
            protocol_verifies=_load_protocol_verifies(runtime_payload.get("protocol_verifies")),
            reference_profile=runtime_payload.get("reference_profile"),
        ),
    )


def load_choreography(path: Path) -> ChoreographySpec:
    """Loads and validates choreography.json."""

    payload = _load_json(path)
    sources = [
        ChoreographySource(
            source_id=item["id"],
            protocol=item["protocol"],
            message_type=item["message_type"],
            cache_key=item["cache_key"],
            required=item.get("required", True),
        )
        for item in payload.get("sources", [])
    ]
    targets = [
        ChoreographyTarget(
            target_id=item["id"],
            protocol=item["protocol"],
            message_type=item["message_type"],
            template_name=item["template_name"],
            receive_window_ms=int(item["receive_window_ms"]),
            initial_status=item.get("initial_status", "direct"),
        )
        for item in payload.get("targets", [])
    ]
    source_ids = {item.source_id for item in sources}
    target_ids = {item.target_id for item in targets}
    joint_groups: list[JointGroup] = []
    for group in payload.get("joint_groups", []):
        if group["target_id"] not in target_ids:
            raise ValueError(f"编排组引用了未知目标: {group['target_id']}")
        for source_name in group.get("sources", []):
            if source_name not in source_ids:
                raise ValueError(f"编排组引用了未知源消息: {source_name}")
        matrix_payload = group["matrix"]
        rows = matrix_payload["rows"]
        cols = matrix_payload["cols"]
        values = matrix_payload["values"]
        if len(values) != len(rows):
            raise ValueError(f"编排组 '{group['group_id']}' 的矩阵行数不匹配")
        for row in values:
            if len(row) != len(cols):
                raise ValueError(f"编排组 '{group['group_id']}' 的矩阵列数不匹配")
        for row_index, row in enumerate(values):
            for col_index, value in enumerate(row):
                if row_index == col_index and value != 0:
                    raise ValueError("时序矩阵对角线必须为 0")
                if value is not None and int(value) < 0:
                    raise ValueError("时序矩阵值必须为非负数或 null")
        joint_groups.append(
            JointGroup(
                group_id=group["group_id"],
                target_id=group["target_id"],
                sources=group["sources"],
                trigger_policy=group["trigger_policy"],
                matrix=MatrixSpec(
                    unit=matrix_payload["unit"],
                    rows=rows,
                    cols=cols,
                    values=values,
                ),
            )
        )
    return ChoreographySpec(
        version=payload.get("version", "1.0"),
        mode=payload["mode"],
        project_name=payload["project_name"],
        sources=sources,
        targets=targets,
        joint_groups=joint_groups,
    )
