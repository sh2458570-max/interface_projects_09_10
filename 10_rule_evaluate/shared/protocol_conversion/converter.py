from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

from .knowledge_base import KnowledgeRule, ProtocolConversionKnowledgeBase


JSON_FENCE_PATTERN = re.compile(r"```(?:json)?\s*(.*?)```", flags=re.DOTALL | re.IGNORECASE)
FIELD_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_./\-]{1,}$")
FORMULA_LABEL_PATTERN = re.compile(
    r"^(?:answer|rule|rules|formula|公式|转换公式|映射规则|规则|输出|结果|response)\s*[:：=\-]\s*",
    re.IGNORECASE,
)
TRAILING_EXPLANATION_PATTERN = re.compile(
    r"\s*(?:#|//|(?:说明|解释|备注|note|because|therefore|其中|表示|对应|即可|即为)\s*[:：]).*$",
    re.IGNORECASE,
)
MAPPING_PAIR_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:=|->|→)\s*([^,;\n]+)")
MAPPING_RANGE_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:-|~|～|—|–)\s*(-?\d+(?:\.\d+)?)\s*(?:=|->|→)\s*([^,;\n]+)")
BLOCK_FORMULA_PATTERN = re.compile(r"(?:\n|^)(?:if\s+|for\s+|while\s+|result\s*=)", re.IGNORECASE)
ARITHMETIC_SIGNAL_PATTERN = re.compile(
    r"\b(?:value|raw|bits)\b|\b(?:signed|unsigned|scale|clip|round|int|float|min|max|abs|sum|len|range)\s*\(",
    re.IGNORECASE,
)
EXPLANATORY_TEXT_PATTERN = re.compile(
    r"根据|首先|然后|用户|文档|说明|需要|这里|问题|表示|对应|现在|查看|分析|解释|because|therefore|step|first|then",
    re.IGNORECASE,
)


@dataclass
class ConversionRule:
    """Normalized conversion rule used by the execution engine."""

    field_name: str
    source_fields: List[str]
    conversion_mode: str
    formula: str
    target_field: Optional[str] = None
    unit: Optional[str] = None
    bit_length: Optional[int] = None
    source: str = "llm"
    description: Optional[str] = None
    concept_name: Optional[str] = None
    target_protocol_type: Optional[str] = None
    target_message_code: Optional[str] = None
    formula_kind: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field_name": self.field_name,
            "source_fields": list(self.source_fields),
            "conversion_mode": self.conversion_mode,
            "formula": self.formula,
            "target_field": self.target_field,
            "unit": self.unit,
            "bit_length": self.bit_length,
            "source": self.source,
            "description": self.description,
            "concept_name": self.concept_name,
            "target_protocol_type": self.target_protocol_type,
            "target_message_code": self.target_message_code,
            "formula_kind": self.formula_kind,
        }


def signed(value: Any, bits: int) -> int:
    bits = int(bits)
    raw = int(value)
    mask = (1 << bits) - 1
    raw &= mask
    sign_bit = 1 << (bits - 1)
    return raw - (1 << bits) if raw & sign_bit else raw


def unsigned(value: Any, bits: int) -> int:
    bits = int(bits)
    raw = int(value)
    mask = (1 << bits) - 1
    return raw & mask


def clip(value: Any, min_value: float, max_value: float) -> float:
    numeric = float(value)
    return max(float(min_value), min(float(max_value), numeric))


def scale(value: Any, factor: float, offset: float = 0.0) -> float:
    return float(value) * float(factor) + float(offset)


def _strip_wrappers(text: str) -> str:
    cleaned = str(text or "").strip()
    match = JSON_FENCE_PATTERN.search(cleaned)
    if match:
        return match.group(1).strip()
    return cleaned


def _clean_formula_text(text: str) -> str:
    cleaned = _strip_wrappers(str(text or "")).replace("```", "").strip().strip("`")
    cleaned = re.sub(r"^\s*(?:[-*•]+|\d+[.)])\s*", "", cleaned)
    cleaned = FORMULA_LABEL_PATTERN.sub("", cleaned)
    return cleaned.strip()


def _normalize_field_name(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_mode(value: Optional[str], formula: str) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"transcoding", "转义"}:
        return "transcoding"
    if raw in {"mapping", "转换"}:
        return "mapping"
    if _looks_like_mapping_table(formula):
        return "mapping"
    return "transcoding"


def _dedupe_preserve_order(items: Iterable[str]) -> List[str]:
    ordered: List[str] = []
    seen = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return ordered


def _strip_assignment_prefix(candidate: str, source_field: str = "") -> str:
    field_pattern = r"[A-Za-z][A-Za-z0-9_./\-]*"
    if source_field:
        field_pattern = rf"(?:{re.escape(source_field)}|{field_pattern})"
    match = re.match(rf"({field_pattern})\s*[:：=]\s*(.+)$", candidate)
    if not match:
        return candidate
    right = match.group(2).strip()
    if right:
        return right
    return candidate


def _maybe_json_load(text: str) -> Optional[Any]:
    try:
        return json.loads(_strip_wrappers(text))
    except Exception:
        return None


def _maybe_yaml_load(text: str) -> Optional[Any]:
    try:
        loaded = yaml.safe_load(_strip_wrappers(text))
    except Exception:
        return None
    if loaded is None or isinstance(loaded, (dict, list)):
        return loaded
    return None


def _looks_like_mapping_table(formula: str) -> bool:
    text = _clean_formula_text(formula)
    if not text:
        return False
    if BLOCK_FORMULA_PATTERN.search(text):
        return False
    return bool(MAPPING_RANGE_PATTERN.search(text) or MAPPING_PAIR_PATTERN.search(text))


def _looks_like_block_formula(formula: str) -> bool:
    return bool(BLOCK_FORMULA_PATTERN.search(str(formula or "").strip()))


def _looks_like_expression_formula(formula: str) -> bool:
    text = _clean_formula_text(formula)
    if not text or _looks_like_mapping_table(text):
        return False
    if EXPLANATORY_TEXT_PATTERN.search(text) and not ARITHMETIC_SIGNAL_PATTERN.search(text):
        return False
    try:
        ast.parse(text, mode="eval")
        return True
    except SyntaxError:
        return False


def _looks_like_executable_formula(formula: str) -> bool:
    text = _clean_formula_text(formula)
    if not text:
        return False
    if _looks_like_mapping_table(text):
        return True
    if _looks_like_block_formula(text):
        try:
            ast.parse(_normalize_block_formula(text), mode="exec")
            return True
        except SyntaxError:
            return False
    return _looks_like_expression_formula(text)


def _extract_transcoding_candidates(text: str, source_field: str = "") -> List[str]:
    cleaned = _clean_formula_text(text)
    if _looks_like_block_formula(cleaned):
        return [cleaned]
    candidates: List[str] = []
    patterns = [
        r"(?:formula[:：]|公式(?:是|为)?|转换公式(?:是|为)?|规则(?:是|为)?)([^。；\n]+)",
        r"((?:signed|unsigned|scale|clip)\([^。；\n]+\)(?:\s*[+\-*/%]\s*[^。；\n]+)*)",
        r"((?:value|raw)[^。；\n]*(?:[*/+\-][^。；\n]+)+)",
        r"([A-Za-z][A-Za-z0-9_./\-]*\s*=\s*(?:signed|unsigned|scale|clip|value|raw)[^。；\n]+)",
    ]
    line_candidates = [cleaned]
    line_candidates.extend(line.strip() for line in cleaned.splitlines() if line.strip())
    for raw_candidate in line_candidates:
        candidate = _strip_assignment_prefix(_clean_formula_text(raw_candidate), source_field=source_field)
        candidate = TRAILING_EXPLANATION_PATTERN.sub("", candidate).strip(" ：:，,。；;'\"")
        if candidate:
            candidates.append(candidate)
        for pattern in patterns:
            for match in re.finditer(pattern, raw_candidate, flags=re.IGNORECASE):
                extracted = _strip_assignment_prefix(match.group(1).strip(), source_field=source_field)
                extracted = TRAILING_EXPLANATION_PATTERN.sub("", extracted).strip(" ：:，,。；;'\"")
                if extracted:
                    candidates.append(extracted)
    return _dedupe_preserve_order(candidates)


def extract_executable_formula(text: str, mode: str, source_field: str = "") -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    cleaned = _clean_formula_text(raw)
    if _looks_like_block_formula(cleaned):
        return cleaned
    if mode == "mapping" and _looks_like_mapping_table(cleaned):
        return _normalize_mapping_formula(cleaned)
    for candidate in _extract_transcoding_candidates(cleaned, source_field=source_field):
        if _looks_like_executable_formula(candidate):
            return candidate
    if _looks_like_executable_formula(cleaned):
        return cleaned
    return ""


def _normalize_mapping_formula(text: str) -> str:
    normalized = _clean_formula_text(text)
    normalized = (
        normalized.replace("，", ",")
        .replace("；", ";")
        .replace("、", ",")
        .replace("以及", ",")
        .replace("和", ",")
    )
    items: List[str] = []
    for left, right, label in MAPPING_RANGE_PATTERN.findall(normalized):
        cleaned_label = TRAILING_EXPLANATION_PATTERN.sub("", label).strip(" ：:，,。；;'\"")
        items.append(f"{left}-{right}={cleaned_label}")
    for raw_value, label in MAPPING_PAIR_PATTERN.findall(normalized):
        cleaned_label = TRAILING_EXPLANATION_PATTERN.sub("", label).strip(" ：:，,。；;'\"")
        items.append(f"{raw_value}={cleaned_label}")
    return ", ".join(_dedupe_preserve_order(items))


def _extract_rule_items(parsed: Any) -> List[Dict[str, Any]]:
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("target_field_rules", "generated_rules", "rules", "items"):
            value = parsed.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _build_rule(item: Dict[str, Any], source: str = "llm") -> Optional[ConversionRule]:
    source_fields_value = item.get("source_fields")
    source_fields: List[str] = []
    if isinstance(source_fields_value, list):
        source_fields = [_normalize_field_name(value) for value in source_fields_value if _normalize_field_name(value)]
    elif isinstance(source_fields_value, str):
        source_fields = [_normalize_field_name(value) for value in source_fields_value.split(",") if _normalize_field_name(value)]

    field_name = _normalize_field_name(item.get("field_name") or item.get("source_field"))
    if not field_name and source_fields:
        field_name = source_fields[0]

    raw_formula = str(
        item.get("formula")
        or item.get("rule")
        or item.get("conversion_formula")
        or item.get("expression")
        or ""
    ).strip()
    if not field_name or not raw_formula:
        return None
    if not source_fields:
        source_fields = [field_name]
    provisional_mode = _normalize_mode(item.get("conversion_mode") or item.get("mode"), raw_formula)
    formula = extract_executable_formula(raw_formula, provisional_mode, source_field=field_name) or _clean_formula_text(raw_formula)
    conversion_mode = _normalize_mode(item.get("conversion_mode") or item.get("mode"), formula or raw_formula)
    if source != "knowledge_base" and not _looks_like_executable_formula(formula):
        return None
    formula_kind = str(item.get("formula_kind") or "").strip() or infer_formula_kind(formula)
    return ConversionRule(
        field_name=field_name,
        source_fields=source_fields,
        conversion_mode=conversion_mode,
        formula=formula,
        target_field=str(item.get("target_field") or "").strip().upper() or None,
        unit=str(item.get("unit") or "").strip() or None,
        bit_length=item.get("bit_length"),
        source=source,
        description=str(item.get("description") or item.get("evidence") or "").strip() or None,
        concept_name=str(item.get("concept_name") or "").strip() or None,
        target_protocol_type=str(item.get("target_protocol_type") or "").strip() or None,
        target_message_code=str(item.get("target_message_code") or "").strip().upper() or None,
        formula_kind=formula_kind,
    )


def parse_llm_formula_output(llm_formula_output: Any, source_fields: Optional[Iterable[str]] = None) -> List[ConversionRule]:
    normalized_source_fields = [_normalize_field_name(item) for item in (source_fields or []) if _normalize_field_name(item)]
    if isinstance(llm_formula_output, list):
        rules = [_build_rule(item) for item in llm_formula_output if isinstance(item, dict)]
        return [rule for rule in rules if rule]

    if isinstance(llm_formula_output, dict):
        items = _extract_rule_items(llm_formula_output)
        if not items:
            reserved_keys = {"protocol_type", "message_code", "embedding_model", "version", "description"}
            for key, value in llm_formula_output.items():
                if key in reserved_keys:
                    continue
                if isinstance(value, str):
                    items.append({"field_name": key, "formula": value})
                elif isinstance(value, dict):
                    item = dict(value)
                    item.setdefault("field_name", key)
                    items.append(item)
            if not items:
                items = [llm_formula_output]
        rules = [_build_rule(item) for item in items if isinstance(item, dict)]
        return [rule for rule in rules if rule]

    text = _strip_wrappers(str(llm_formula_output or ""))
    if not text:
        return []

    as_json = _maybe_json_load(text)
    if as_json is not None:
        return parse_llm_formula_output(as_json, source_fields=normalized_source_fields)

    as_yaml = _maybe_yaml_load(text)
    if as_yaml is not None:
        return parse_llm_formula_output(as_yaml, source_fields=normalized_source_fields)

    rules: List[ConversionRule] = []
    for raw_line in text.splitlines():
        line = raw_line.strip().strip(",")
        if not line:
            continue
        match = re.match(r"([A-Za-z][A-Za-z0-9_./\-]*)\s*[:：]\s*(.+)", line)
        if not match:
            match = re.match(r"([A-Za-z][A-Za-z0-9_./\-]*)\s*=>\s*(.+)", line)
        if match:
            field_name = _normalize_field_name(match.group(1))
            formula = match.group(2).strip()
            if FIELD_NAME_PATTERN.fullmatch(field_name):
                rule = _build_rule({"field_name": field_name, "formula": formula})
                if rule is not None:
                    rules.append(rule)

    if not rules and len(normalized_source_fields) == 1:
        field_name = normalized_source_fields[0]
        conversion_mode = _normalize_mode(None, text)
        formula = extract_executable_formula(text, conversion_mode, source_field=field_name)
        if formula and _looks_like_executable_formula(formula):
            rules.append(
                ConversionRule(
                    field_name=field_name,
                    source_fields=[field_name],
                    conversion_mode=conversion_mode,
                    formula=formula,
                    formula_kind=infer_formula_kind(formula),
                )
            )
    return rules


def normalize_source_message(source_message: Any) -> Dict[str, Any]:
    if isinstance(source_message, dict):
        return {_normalize_field_name(key): value for key, value in source_message.items() if _normalize_field_name(key)}
    normalized: Dict[str, Any] = {}
    if isinstance(source_message, list):
        for item in source_message:
            if not isinstance(item, dict):
                continue
            field_name = _normalize_field_name(item.get("field_name") or item.get("name"))
            if field_name:
                normalized[field_name] = item.get("value")
    return normalized


def _parse_mapping_formula(formula: str) -> Tuple[List[Tuple[float, float, str]], Dict[str, str]]:
    range_rules: List[Tuple[float, float, str]] = []
    exact_rules: Dict[str, str] = {}
    for left, right, label in MAPPING_RANGE_PATTERN.findall(formula or ""):
        range_rules.append((float(left), float(right), label.strip()))
    for raw_value, label in MAPPING_PAIR_PATTERN.findall(formula or ""):
        exact_rules[str(raw_value).strip()] = label.strip()
    return range_rules, exact_rules


def _normalize_block_formula(formula: str) -> str:
    lines: List[str] = []
    for raw_line in _clean_formula_text(formula).splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("return "):
            indent = line[: len(line) - len(line.lstrip())]
            line = f"{indent}result = {stripped[7:].strip()}"
        lines.append(line)
    return "\n".join(lines)


def infer_formula_kind(formula: str) -> str:
    cleaned = _clean_formula_text(formula)
    if _looks_like_mapping_table(cleaned):
        return "mapping_table"
    if _looks_like_block_formula(cleaned):
        return "python_block"
    return "python_expr"


class SafeFormulaValidator(ast.NodeVisitor):
    ALLOWED_NODES = (
        ast.Module,
        ast.Expression,
        ast.Expr,
        ast.Assign,
        ast.AugAssign,
        ast.If,
        ast.For,
        ast.Load,
        ast.Store,
        ast.Name,
        ast.Constant,
        ast.BinOp,
        ast.UnaryOp,
        ast.BoolOp,
        ast.Compare,
        ast.Call,
        ast.keyword,
        ast.List,
        ast.Tuple,
        ast.Dict,
        ast.Subscript,
        ast.Slice,
        ast.Index,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Pow,
        ast.Mod,
        ast.FloorDiv,
        ast.UAdd,
        ast.USub,
        ast.And,
        ast.Or,
        ast.Not,
        ast.Eq,
        ast.NotEq,
        ast.Gt,
        ast.GtE,
        ast.Lt,
        ast.LtE,
        ast.In,
        ast.NotIn,
        ast.Pass,
        ast.Break,
        ast.Continue,
    )
    DISALLOWED_NAMES = {"__import__", "eval", "exec", "open", "compile", "globals", "locals", "vars", "getattr", "setattr", "delattr"}

    def __init__(self, allowed_functions: Iterable[str]):
        self.allowed_functions = set(allowed_functions)

    def visit_Call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name) or node.func.id not in self.allowed_functions:
            raise ValueError("只允许调用白名单函数")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        raise ValueError("不允许属性访问")

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id in self.DISALLOWED_NAMES or node.id.startswith("__"):
            raise ValueError(f"禁止使用名称: {node.id}")

    def generic_visit(self, node: ast.AST) -> Any:
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"不支持的表达式节点: {type(node).__name__}")
        super().generic_visit(node)


def _build_formula_variables(normalized_source: Dict[str, Any], rule: ConversionRule) -> Dict[str, Any]:
    variables: Dict[str, Any] = {"bits": int(rule.bit_length or 0)}
    for field_name, field_value in normalized_source.items():
        if not isinstance(field_name, str) or not field_name.isidentifier():
            continue
        variables[field_name] = field_value
    if rule.field_name in normalized_source:
        variables["value"] = normalized_source[rule.field_name]
        variables["raw"] = normalized_source[rule.field_name]
    return variables


def _callable_env() -> Dict[str, Any]:
    return {
        "abs": abs,
        "round": round,
        "int": int,
        "float": float,
        "min": min,
        "max": max,
        "len": len,
        "sum": sum,
        "range": range,
        "enumerate": enumerate,
        "list": list,
        "dict": dict,
        "signed": signed,
        "unsigned": unsigned,
        "clip": clip,
        "scale": scale,
    }


def _execute_python_expression(formula: str, variables: Dict[str, Any]) -> Any:
    functions = _callable_env()
    tree = ast.parse(formula, mode="eval")
    SafeFormulaValidator(functions.keys()).visit(tree)
    return eval(compile(tree, "<formula>", "eval"), {"__builtins__": {}}, {**functions, **variables})


def _execute_python_block(formula: str, variables: Dict[str, Any]) -> Any:
    functions = _callable_env()
    normalized = _normalize_block_formula(formula)
    tree = ast.parse(normalized, mode="exec")
    SafeFormulaValidator(functions.keys()).visit(tree)
    scope = {**functions, **variables, "result": None}
    exec(compile(tree, "<formula>", "exec"), {"__builtins__": {}}, scope)
    return scope.get("result")


def _evaluate_mapping_formula(formula: str, value: Any) -> Any:
    range_rules, exact_rules = _parse_mapping_formula(formula)
    value_key = str(value).strip()
    if value_key in exact_rules:
        return exact_rules[value_key]
    try:
        numeric = float(value)
    except Exception:
        numeric = None
    if numeric is not None:
        for left, right, label in range_rules:
            if left <= numeric <= right:
                return label
    raise ValueError("未命中映射规则")


def _evaluate_arithmetic_formula(
    formula: str,
    value: Any,
    rule: ConversionRule,
    normalized_source: Dict[str, Any],
) -> Any:
    preview_rule = ConversionRule(
        field_name=rule.field_name,
        source_fields=list(rule.source_fields),
        conversion_mode=rule.conversion_mode,
        formula=formula,
        target_field=rule.target_field,
        unit=rule.unit,
        bit_length=rule.bit_length,
        source=rule.source,
        description=rule.description,
        concept_name=rule.concept_name,
        target_protocol_type=rule.target_protocol_type,
        target_message_code=rule.target_message_code,
        formula_kind=rule.formula_kind or infer_formula_kind(formula),
    )
    variables = _build_formula_variables(normalized_source, preview_rule)
    if preview_rule.formula_kind == "python_block":
        return _execute_python_block(formula, variables)
    return _execute_python_expression(formula, variables)


def _evaluate_formula(rule: ConversionRule, normalized_source: Dict[str, Any]) -> Any:
    variables = _build_formula_variables(normalized_source, rule)
    formula_kind = rule.formula_kind or infer_formula_kind(rule.formula)
    if formula_kind == "mapping_table":
        return _evaluate_mapping_formula(rule.formula, normalized_source.get(rule.field_name))
    if formula_kind == "python_block":
        return _execute_python_block(rule.formula, variables)
    return _execute_python_expression(rule.formula, variables)


def _rule_from_kb(kb_rule: KnowledgeRule) -> ConversionRule:
    return ConversionRule(
        field_name=kb_rule.field_name,
        source_fields=kb_rule.source_fields or [kb_rule.field_name],
        conversion_mode=_normalize_mode(kb_rule.conversion_mode, kb_rule.formula),
        formula=kb_rule.formula,
        target_field=kb_rule.target_field,
        unit=kb_rule.unit,
        bit_length=kb_rule.bit_length,
        source="knowledge_base",
        description=kb_rule.description,
        concept_name=kb_rule.concept_name,
        target_protocol_type=kb_rule.target_protocol_type,
        target_message_code=kb_rule.target_message_code,
        formula_kind=kb_rule.formula_kind or infer_formula_kind(kb_rule.formula),
    )


def execute_protocol_conversion(
    source_message: Any,
    llm_formula_output: Any,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
    use_knowledge_base: bool = True,
    target_protocol_type: Optional[str] = None,
    target_message_code: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_source = normalize_source_message(source_message)
    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    rules = parse_llm_formula_output(llm_formula_output, source_fields=normalized_source.keys())

    existing_signatures = {
        (rule.target_field or rule.field_name, tuple(rule.source_fields or [rule.field_name]), rule.formula)
        for rule in rules
    }
    if use_knowledge_base:
        if target_protocol_type or target_message_code:
            for kb_rule in knowledge_base.find_rules_for_source_fields(
                source_fields=normalized_source.keys(),
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
            ):
                converted_rule = _rule_from_kb(kb_rule)
                signature = (
                    converted_rule.target_field or converted_rule.field_name,
                    tuple(converted_rule.source_fields or [converted_rule.field_name]),
                    converted_rule.formula,
                )
                if signature not in existing_signatures:
                    rules.append(converted_rule)
                    existing_signatures.add(signature)
        missing_fields = [field_name for field_name in normalized_source if field_name not in {rule.field_name for rule in rules}]
        for field_name in missing_fields:
            kb_rule = knowledge_base.find_rule(field_name=field_name, message_code=message_code)
            if kb_rule is None:
                continue
            converted_rule = _rule_from_kb(kb_rule)
            signature = (
                converted_rule.target_field or converted_rule.field_name,
                tuple(converted_rule.source_fields or [converted_rule.field_name]),
                converted_rule.formula,
            )
            if signature not in existing_signatures:
                rules.append(converted_rule)
                existing_signatures.add(signature)

    converted_fields: List[Dict[str, Any]] = []
    for rule in rules:
        source_values = {field: normalized_source.get(field) for field in rule.source_fields}
        record = rule.to_dict()
        record.update(
            {
                "source_value": normalized_source.get(rule.field_name),
                "source_values": source_values,
                "target_field": rule.target_field or rule.field_name,
                "success": False,
            }
        )
        missing_inputs = [field for field in rule.source_fields if field not in normalized_source]
        if missing_inputs:
            record["error"] = f"源协议中未找到对应字段: {', '.join(missing_inputs)}"
            converted_fields.append(record)
            continue
        try:
            converted_value = _evaluate_formula(rule, normalized_source)
            record["converted_value"] = converted_value
            record["success"] = True
        except Exception as exc:
            record["error"] = str(exc)
        converted_fields.append(record)

    success_count = sum(1 for item in converted_fields if item.get("success"))
    converted_message = {
        item.get("target_field") or item.get("field_name"): item.get("converted_value")
        for item in converted_fields
        if item.get("success")
    }
    return {
        "protocol_type": protocol_type,
        "message_code": str(message_code or "").strip().upper() or None,
        "target_protocol_type": str(target_protocol_type or "").strip() or None,
        "target_message_code": str(target_message_code or "").strip().upper() or None,
        "embedding_model": knowledge_base.embedding_model,
        "knowledge_base": knowledge_base.to_summary(),
        "normalized_source_message": normalized_source,
        "normalized_rules": [rule.to_dict() for rule in rules],
        "converted_fields": converted_fields,
        "converted_message": converted_message,
        "summary": {
            "total_rules": len(converted_fields),
            "success_count": success_count,
            "failed_count": len(converted_fields) - success_count,
            "knowledge_graph_backend": knowledge_base.to_summary().get("backend"),
        },
    }
