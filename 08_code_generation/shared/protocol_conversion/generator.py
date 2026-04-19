from __future__ import annotations

import ast
import json
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shared.llm.local_llm import LocalLLM, get_llm

from .converter import execute_protocol_conversion, normalize_source_message
from .knowledge_base import ProtocolConversionKnowledgeBase
from .pageindex_adapter import get_pageindex_evidence_provider
from .trained_doc_index import get_trained_doc_evidence_provider


DEFAULT_PROTOCOL_TYPE = "Link16"
DEFAULT_EMPTY_RULE_RETRIES = 3
ALLOWED_FORMULA_FUNCTIONS = {
    "abs",
    "round",
    "int",
    "float",
    "min",
    "max",
    "len",
    "sum",
    "range",
    "enumerate",
    "list",
    "dict",
    "signed",
    "unsigned",
    "clip",
    "scale",
}
ALLOWED_FORMULA_VARS = {"value", "raw", "bits", "result", "True", "False", "None"}
STRICT_SEMANTIC_GROUPS = {
    "latitude",
    "longitude",
    "altitude",
    "pitch",
    "roll",
    "yaw",
    "time",
    "threat",
    "info",
}


def _normalize_protocol_spec(spec: Any, role: str, allow_empty_content: bool = False) -> Dict[str, Optional[str]]:
    if spec is None and allow_empty_content:
        return {
            "name": None,
            "protocol_type": None,
            "message_code": None,
            "content": None,
        }

    if isinstance(spec, str):
        content = spec.strip()
        if not content and not allow_empty_content:
            raise ValueError(f"{role}协议内容不能为空")
        return {
            "name": None,
            "protocol_type": None,
            "message_code": None,
            "content": content or None,
        }

    if not isinstance(spec, dict):
        raise ValueError(f"{role}协议定义必须是对象或字符串")

    name = str(spec.get("name") or spec.get("protocol_name") or spec.get("title") or "").strip() or None
    protocol_type = str(spec.get("protocol_type") or spec.get("type") or "").strip() or None
    message_code = str(spec.get("message_code") or spec.get("messageType") or "").strip() or None
    content = str(
        spec.get("content")
        or spec.get("document_text")
        or spec.get("definition")
        or spec.get("text")
        or ""
    ).strip() or None

    if not content and not allow_empty_content:
        raise ValueError(f"{role}协议内容不能为空")

    return {
        "name": name,
        "protocol_type": protocol_type,
        "message_code": message_code,
        "content": content,
    }


def _resolve_source_protocol_content(source_protocol: Dict[str, Optional[str]], use_trained_docs: bool) -> str:
    content = str(source_protocol.get("content") or "").strip()
    if content:
        return content
    if use_trained_docs:
        return (
            "训练阶段已上传并建立索引的协议文档将作为原协议证据来源；"
            "请优先依据 PageIndex 检索到的证据片段生成转换规则。"
        )
    return ""


def _extract_rule_items(parsed: Any) -> List[Dict[str, Any]]:
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        for key in ("target_field_rules", "generated_rules", "rules", "items"):
            value = parsed.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _normalize_source_fields(item: Dict[str, Any]) -> List[str]:
    source_fields = item.get("source_fields")
    normalized: List[str] = []
    if isinstance(source_fields, list):
        normalized = [str(value).strip().upper() for value in source_fields if str(value).strip()]
    elif isinstance(source_fields, str):
        normalized = [value.strip().upper() for value in source_fields.split(",") if value.strip()]

    if normalized:
        return normalized

    fallback = str(item.get("field_name") or item.get("source_field") or "").strip().upper()
    return [fallback] if fallback else []


def _infer_formula_kind(rule: str) -> str:
    text = str(rule or "").strip()
    if "\n" in text or any(text.startswith(prefix) for prefix in ("if ", "for ", "while ", "result =")):
        return "python_block"
    if any(token in text for token in ("->", "→")) or ("=" in text and any(ch.isdigit() for ch in text)):
        return "mapping_table"
    return "python_expr"


def normalize_generated_rules(rule_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized_rules: List[Dict[str, Any]] = []
    for item in rule_items:
        source_fields = _normalize_source_fields(item)
        target_field = str(item.get("target_field") or item.get("field_name") or "").strip().upper()
        rule = str(
            item.get("rule")
            or item.get("formula")
            or item.get("conversion_formula")
            or item.get("expression")
            or ""
        ).strip()
        conversion_mode = str(item.get("conversion_mode") or item.get("mode") or "").strip().lower() or None
        if not target_field or not rule:
            continue
        formula_kind = str(item.get("formula_kind") or "").strip() or _infer_formula_kind(rule)
        normalized_rules.append(
            {
                "target_field": target_field,
                "source_fields": source_fields,
                "conversion_mode": conversion_mode,
                "formula_kind": formula_kind,
                "rule": rule,
                "concept_name": str(item.get("concept_name") or item.get("concept") or target_field).strip() or None,
                "condition": item.get("condition"),
                "default_value": item.get("default_value"),
                "unit": item.get("unit"),
                "bit_length": item.get("bit_length"),
                "description": item.get("description"),
                "evidence": item.get("evidence"),
            }
        )
    return normalized_rules


def _dedupe_rules_by_target_field(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for rule in rules:
        target_field = str(rule.get("target_field") or "").strip().upper()
        if not target_field or target_field in seen:
            continue
        seen.add(target_field)
        deduped.append(rule)
    return deduped


def _format_pageindex_evidence(evidence_result: Optional[Dict[str, Any]]) -> Optional[str]:
    if not evidence_result:
        return None
    snippets = evidence_result.get("evidence_snippets") or []
    if not snippets:
        return None
    lines = [
        "PageIndex证据摘要（仅可依据以下证据生成规则；没有明确证据支持的字段必须跳过，不要猜测）："
    ]
    for index, snippet in enumerate(snippets, start=1):
        lines.append(
            "\n".join(
                [
                    f"[证据{index}] role={snippet.get('role') or 'unknown'}",
                    f"query={snippet.get('query') or 'N/A'}",
                    f"title={snippet.get('title') or 'N/A'}",
                    f"content={snippet.get('content') or ''}",
                ]
            )
        )
    return "\n\n".join(lines)


def _normalize_required_target_fields(required_target_fields: Optional[Iterable[Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen = set()
    for item in required_target_fields or []:
        if isinstance(item, dict):
            field_name = str(item.get("field_name") or item.get("name") or "").strip()
            if not field_name:
                continue
            key = field_name.upper()
            if key in seen:
                continue
            seen.add(key)
            normalized.append(
                {
                    "field_name": key,
                    "protocol": str(item.get("protocol") or "").strip() or None,
                    "default_value": item.get("default_value"),
                    "bit_length": item.get("bit_length"),
                    "label": str(item.get("label") or "").strip() or None,
                    "path_parts": list(item.get("path_parts") or []) if isinstance(item.get("path_parts"), list) else None,
                    "description": str(item.get("description") or "").strip() or None,
                }
            )
            continue

        field_name = str(item or "").strip()
        if not field_name:
            continue
        key = field_name.upper()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(
            {
                "field_name": key,
                "protocol": None,
                "default_value": None,
                "bit_length": None,
                "label": None,
                "path_parts": None,
                "description": None,
            }
        )
    return normalized


def _required_target_field_names(required_target_fields: Optional[Iterable[Any]]) -> List[str]:
    return [item["field_name"] for item in _normalize_required_target_fields(required_target_fields)]


def _missing_target_fields(
    generated_rules: List[Dict[str, Any]],
    required_target_fields: Optional[Iterable[Any]],
) -> List[str]:
    required_names = _required_target_field_names(required_target_fields)
    if not required_names:
        return []
    generated_names = {
        str(item.get("target_field") or "").strip().upper()
        for item in generated_rules
        if str(item.get("target_field") or "").strip()
    }
    return [field_name for field_name in required_names if field_name not in generated_names]


def _split_field_tokens(field_name: str) -> List[str]:
    text = str(field_name or "").strip().upper()
    if not text:
        return []
    tokens = [token for token in re.split(r"[^A-Z0-9]+", text) if token]
    if len(tokens) <= 1:
        tokens.extend(token for token in re.findall(r"[A-Z]+|\d+", text) if token and token not in tokens)
    return tokens or [text]


def _decode_field_text(value: Any) -> str:
    raw = str(value or "")
    if not raw:
        return ""

    def repl(match: re.Match[str]) -> str:
        try:
            return chr(int(match.group(1), 16))
        except Exception:
            return match.group(0)

    decoded = re.sub(r"U([0-9A-F]{4,6})", repl, raw.upper())
    return decoded.replace("_", " ")


def _semantic_groups_for_text(*values: Any) -> set[str]:
    raw_text = " ".join([str(value or "") for value in values if str(value or "").strip()])
    decoded_text = " ".join([_decode_field_text(value) for value in values if str(value or "").strip()])
    text = f"{raw_text} {decoded_text}".strip().lower()
    groups: set[str] = set()
    keyword_groups = {
        "latitude": ("latitude", "lat", "纬度"),
        "longitude": ("longitude", "lon", "经度"),
        "altitude": ("altitude", "height", "elevation", "高度"),
        "pitch": ("pitch", "俯仰"),
        "roll": ("roll", "翻滚"),
        "yaw": ("yaw", "heading", "偏航"),
        "time": ("time", "hour", "minute", "second", "timestamp", "小时", "分钟", "秒", "时间"),
        "threat": ("threat", "威胁"),
        "info": ("info", "信息"),
        "id": (" id ", "编号", "标识", "identifier"),
        "name": ("name", "名称"),
        "count": ("count", "quantity", "数量"),
        "target": ("target", "目标"),
    }
    padded = f" {text} "
    for group, keywords in keyword_groups.items():
        if any(keyword in padded for keyword in keywords):
            groups.add(group)
    return groups


def _is_direct_copy_rule(rule: Dict[str, Any]) -> bool:
    source_fields = [str(item or "").strip().upper() for item in (rule.get("source_fields") or []) if str(item or "").strip()]
    formula = str(rule.get("rule") or "").strip().upper()
    if not formula:
        return False
    if formula in source_fields:
        return True
    return formula in {"VALUE", "RESULT = VALUE"}


def _score_source_candidate(target_field: str, source_field: str) -> float:
    target = str(target_field or "").strip().upper()
    source = str(source_field or "").strip().upper()
    if not target or not source:
        return 0.0
    if target == source:
        return 100.0
    score = SequenceMatcher(a=target, b=source).ratio() * 60.0
    target_tokens = set(_split_field_tokens(target))
    source_tokens = set(_split_field_tokens(source))
    overlap = target_tokens & source_tokens
    score += float(len(overlap)) * 8.0
    if target.startswith(source) or source.startswith(target):
        score += 10.0
    if target.endswith(source) or source.endswith(target):
        score += 6.0
    return round(score, 4)


def _build_source_field_candidates(
    target_field_spec: Dict[str, Any],
    normalized_source_message: Dict[str, Any],
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    target_field = str(target_field_spec.get("field_name") or "").strip().upper()
    for source_field in normalized_source_message.keys():
        score = _score_source_candidate(target_field, source_field)
        if score <= 0:
            continue
        ranked.append(
            {
                "field_name": str(source_field).strip().upper(),
                "score": score,
                "sample_value": normalized_source_message.get(source_field),
            }
        )
    ranked.sort(key=lambda item: (-float(item["score"]), item["field_name"]))
    return ranked[:top_k]


def _build_target_generation_tasks(
    required_target_fields: Optional[Iterable[Any]],
    existing_rules: List[Dict[str, Any]],
    normalized_source_message: Dict[str, Any],
) -> List[Dict[str, Any]]:
    existing_targets = {
        str(rule.get("target_field") or "").strip().upper()
        for rule in existing_rules
        if str(rule.get("target_field") or "").strip()
    }
    tasks: List[Dict[str, Any]] = []
    for item in _normalize_required_target_fields(required_target_fields):
        field_name = item["field_name"]
        if field_name in existing_targets:
            continue
        tasks.append(
            {
                **item,
                "candidate_source_fields": _build_source_field_candidates(item, normalized_source_message),
            }
        )
    return tasks


def _resolve_pageindex_status(evidence_result: Optional[Dict[str, Any]]) -> Tuple[str, Optional[str]]:
    if not evidence_result:
        return "miss", None
    status = str(evidence_result.get("status") or "").strip().lower()
    reason = str(evidence_result.get("reason") or "").strip() or None
    if status == "unavailable":
        return "unavailable", reason
    snippets = evidence_result.get("evidence_snippets") or []
    if snippets:
        return "used", reason
    return "miss", reason


def _summarize_candidate_source_fields(candidates: List[Dict[str, Any]]) -> str:
    if not candidates:
        return "无明显候选源字段。"
    lines = ["候选源字段（只能从这些字段中选择；若都不可靠可返回 []）:"]
    for item in candidates:
        lines.append(
            f"- {item['field_name']} (score={item['score']}, sample_value={item.get('sample_value')})"
        )
    return "\n".join(lines)


def _format_target_field_requirements(required_target_fields: Optional[Iterable[Any]]) -> Optional[str]:
    normalized = _normalize_required_target_fields(required_target_fields)
    if not normalized:
        return None
    lines = ["目标字段清单（必须尽量覆盖；若无源字段依赖，可输出常量数值公式）:"]
    for item in normalized:
        suffix_parts = []
        if item.get("bit_length") is not None:
            suffix_parts.append(f"bit_length={item['bit_length']}")
        if item.get("default_value") not in (None, ""):
            suffix_parts.append(f"default_value={item['default_value']}")
        suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
        lines.append(f"- {item['field_name']}{suffix}")
    return "\n".join(lines)


def _format_target_task_requirements(target_tasks: List[Dict[str, Any]]) -> Optional[str]:
    if not target_tasks:
        return None
    lines = ["本轮仅为以下目标字段生成规则:"]
    for item in target_tasks:
        suffix_parts = []
        if item.get("bit_length") is not None:
            suffix_parts.append(f"bit_length={item['bit_length']}")
        if item.get("default_value") not in (None, ""):
            suffix_parts.append(f"default_value={item['default_value']}")
        if item.get("label"):
            suffix_parts.append(f"label={item['label']}")
        if item.get("description"):
            suffix_parts.append(f"description={item['description']}")
        if item.get("path_parts"):
            suffix_parts.append(f"path={'/'.join(str(part) for part in item['path_parts'])}")
        suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
        lines.append(f"- {item['field_name']}{suffix}")
        lines.append(_summarize_candidate_source_fields(item.get("candidate_source_fields") or []))
    return "\n".join(lines)


def _knowledge_rule_to_generated_rule(rule: Any) -> Dict[str, Any]:
    return {
        "target_field": str(rule.target_field or "").strip().upper(),
        "source_fields": [str(item).strip().upper() for item in (rule.source_fields or []) if str(item).strip()],
        "conversion_mode": str(rule.conversion_mode or "").strip().lower() or None,
        "formula_kind": str(rule.formula_kind or "").strip() or _infer_formula_kind(str(rule.formula or "")),
        "rule": str(rule.formula or "").strip(),
        "concept_name": str(rule.concept_name or rule.target_field or "").strip() or None,
        "condition": None,
        "default_value": None,
        "unit": rule.unit,
        "bit_length": rule.bit_length,
        "description": rule.description,
        "evidence": rule.description,
        "source": str(rule.source or "knowledge_graph"),
        "status": getattr(rule, "status", None),
    }


def _build_default_zero_rules(
    required_target_fields: Optional[Iterable[Any]],
    existing_rules: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    zero_rules: List[Dict[str, Any]] = []
    for item in _normalize_required_target_fields(required_target_fields):
        target_field = item["field_name"]
        if any(str(rule.get("target_field") or "").strip().upper() == target_field for rule in existing_rules):
            continue
        zero_rules.append(
            {
                "target_field": target_field,
                "source_fields": [],
                "conversion_mode": "transcoding",
                "formula_kind": "python_expr",
                "rule": "0",
                "concept_name": target_field,
                "condition": None,
                "default_value": item.get("default_value", 0),
                "unit": None,
                "bit_length": item.get("bit_length"),
                "description": "无法可靠转换，默认置 0",
                "evidence": None,
            }
        )
    return zero_rules


def _build_executable_rules(
    generated_rules: List[Dict[str, Any]],
    normalized_target_protocol: Dict[str, Optional[str]],
) -> List[Dict[str, Any]]:
    return [
        {
            "field_name": rule["source_fields"][0] if rule.get("source_fields") else "",
            "source_fields": list(rule.get("source_fields") or []),
            "target_field": rule["target_field"],
            "conversion_mode": rule["conversion_mode"],
            "formula_kind": rule["formula_kind"],
            "formula": rule["rule"],
            "rule": rule["rule"],
            "unit": rule.get("unit"),
            "bit_length": rule.get("bit_length"),
            "description": rule.get("description") or rule.get("evidence"),
            "concept_name": rule.get("concept_name"),
            "target_protocol_type": normalized_target_protocol.get("protocol_type") or normalized_target_protocol.get("name"),
            "target_message_code": normalized_target_protocol.get("message_code"),
        }
        for rule in generated_rules
    ]


def _build_kg_writeback_payload(
    generated_rules: List[Dict[str, Any]],
    source_protocol: Dict[str, Optional[str]],
    target_protocol: Dict[str, Optional[str]],
    excluded_target_fields: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    excluded = {
        str(item or "").strip().upper()
        for item in (excluded_target_fields or [])
        if str(item or "").strip()
    }
    rules: List[Dict[str, Any]] = []
    for rule in generated_rules:
        target_field = str(rule.get("target_field") or "").strip().upper()
        if not target_field or target_field in excluded:
            continue
        source_fields = [str(item).strip().upper() for item in (rule.get("source_fields") or []) if str(item).strip()]
        formula = str(rule.get("rule") or "").strip()
        if not formula or formula == "0" or not source_fields:
            continue
        evidence_items: List[Dict[str, Any]] = []
        evidence_text = str(rule.get("evidence") or "").strip()
        description_text = str(rule.get("description") or "").strip()
        if evidence_text:
            evidence_items.append({"type": "evidence", "content": evidence_text})
        if description_text and description_text != evidence_text:
            evidence_items.append({"type": "description", "content": description_text})
        rules.append(
            {
                "concept_name": str(rule.get("concept_name") or target_field).strip() or target_field,
                "source_fields": source_fields,
                "target_field": target_field,
                "conversion_mode": str(rule.get("conversion_mode") or "").strip().lower() or None,
                "formula_kind": str(rule.get("formula_kind") or "").strip() or _infer_formula_kind(formula),
                "formula": formula,
                "evidence": evidence_items,
                "confidence": rule.get("confidence"),
                "status": str(rule.get("status") or "candidate").strip().lower() or "candidate",
                "source": str(rule.get("source") or "llm_generated").strip() or "llm_generated",
                "target_protocol_type": str(
                    rule.get("target_protocol_type")
                    or target_protocol.get("protocol_type")
                    or target_protocol.get("name")
                    or ""
                ).strip() or None,
                "target_message_code": str(
                    rule.get("target_message_code")
                    or target_protocol.get("message_code")
                    or ""
                ).strip().upper() or None,
            }
        )
    return {
        "protocol_type": source_protocol.get("protocol_type") or source_protocol.get("name") or DEFAULT_PROTOCOL_TYPE,
        "source_message_code": source_protocol.get("message_code"),
        "target_protocol_type": target_protocol.get("protocol_type") or target_protocol.get("name"),
        "target_message_code": target_protocol.get("message_code"),
        "rules": rules,
    }


def build_protocol_rule_generation_prompt(
    source_protocol: Dict[str, Optional[str]],
    target_protocol: Dict[str, Optional[str]],
    source_message: Optional[Any] = None,
    pageindex_evidence: Optional[Dict[str, Any]] = None,
    use_trained_docs: bool = False,
    required_target_fields: Optional[Iterable[Any]] = None,
    target_tasks: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, str]:
    evidence_text = _format_pageindex_evidence(pageindex_evidence)
    target_field_requirements = _format_target_task_requirements(target_tasks or []) or _format_target_field_requirements(required_target_fields)
    source_protocol_content = _resolve_source_protocol_content(source_protocol, use_trained_docs)
    system_prompt = (
        "你是一个协议值转换公式生成器。"
        "你的任务不是直接输出目标报文，而是生成‘原协议字段值 -> 目标协议字段值’的转换公式。"
        "必须只输出 JSON。"
        "禁止输出<think>、解释文字、Markdown 代码块和额外前后缀。"
        "输出格式为 JSON 数组，每个元素必须包含："
        "concept_name, target_field, source_fields, conversion_mode, formula_kind, rule。"
        "conversion_mode 只能是 transcoding 或 mapping。"
        "formula_kind 只能是 python_expr、python_block、mapping_table。"
        "python_expr 表示单行表达式，可直接引用 source_fields 中的字段名。"
        "python_block 表示多行公式，允许 if/else/for，且最终必须把目标值赋给 result。"
        "mapping_table 表示离散值映射表，例如 1=10, 2=20。"
        "如果规则是常量赋值，source_fields 可以为空数组。"
        "所有 rule 都必须是值到值公式，不要输出标签解释文本；"
        "如果目标协议使用枚举数值，就输出数值映射，不要输出语义标签。"
        "若字段语义一致且表达方式一致，可使用 identity 公式 value 或 result = value。"
        "如果提供了PageIndex证据摘要，你必须只基于证据摘要中明确支持的字段生成规则；"
        "没有证据支持的字段不要补全、不要猜测、不要输出占位规则。"
    )
    source_message_text = json.dumps(source_message, ensure_ascii=False, indent=2) if source_message is not None else "null"
    user_prompt = [
        "请根据以下信息生成目标协议字段的值到值转换规则。\n\n"
        f"原协议名称: {source_protocol.get('name') or source_protocol.get('protocol_type') or '未提供'}\n"
        f"原协议类型: {source_protocol.get('protocol_type') or '未提供'}\n"
        f"原协议消息码: {source_protocol.get('message_code') or '未提供'}\n"
        f"原协议内容:\n{source_protocol_content or '未直接提供原协议全文，请严格依据证据摘要。'}\n\n"
        f"目标协议名称: {target_protocol.get('name') or target_protocol.get('protocol_type') or '未提供'}\n"
        f"目标协议类型: {target_protocol.get('protocol_type') or '未提供'}\n"
        f"目标协议消息码: {target_protocol.get('message_code') or '未提供'}\n"
        f"目标协议内容:\n{target_protocol['content']}\n\n"
        f"原始整包报文示例:\n{source_message_text}\n\n"
    ]
    if target_field_requirements:
        user_prompt.append(f"{target_field_requirements}\n\n")
    if evidence_text:
        user_prompt.append(f"{evidence_text}\n\n")
    user_prompt.extend(
        [
            "要求:\n"
            "1. 每条规则面向一个 target_field。\n"
            "2. source_fields 必须列出该目标字段依赖的原字段；如果是常量规则，可输出空数组。\n"
            "3. conversion_mode 只能是 transcoding 或 mapping。\n"
            "4. rule 必须能被程序直接执行，并产出目标字段的值。\n"
            "5. 如需条件或循环，请使用 python_block，并把最终值赋给 result。\n"
            "6. 输出必须是 JSON 数组。\n"
            "7. 不允许输出任何 JSON 之外的文本。\n"
            "8. 若某个目标字段没有源字段依赖，但目标协议要求必须输出，可生成常量数值公式。\n"
            "9. 若缺少明确证据支持某个目标字段，且无法给出可执行的值到值/常量公式，请直接跳过该字段，不要生成猜测性规则。\n"
            "10. 如果提供了候选源字段列表，只能从候选列表中选择 source_fields；不要自行发明新的源字段名。\n\n"
            "11. 禁止把明显不同物理量直接对应，例如高度<->经纬度、姿态角<->时间、信息字段<->威胁字段；不确定时直接跳过该字段。\n\n"
            "输出示例:\n"
            "[\n"
            "  {\n"
            "    \"concept_name\": \"LATITUDE\",\n"
            "    \"target_field\": \"LATITUDE_DEG\",\n"
            "    \"source_fields\": [\"LATITUDE\"],\n"
            "    \"conversion_mode\": \"transcoding\",\n"
            "    \"formula_kind\": \"python_expr\",\n"
            "    \"rule\": \"signed(LATITUDE, bits) * 0.0013 / 60\"\n"
            "  },\n"
            "  {\n"
            "    \"concept_name\": \"MISSION_ASSIGNMENT\",\n"
            "    \"target_field\": \"MISSION_ASSIGNMENT_CODE\",\n"
            "    \"source_fields\": [\"MISSION_ASSIGNMENT_DISCRETE\"],\n"
            "    \"conversion_mode\": \"mapping\",\n"
            "    \"formula_kind\": \"mapping_table\",\n"
            "    \"rule\": \"1=10, 5=30, 6=40\"\n"
            "  }\n"
            "]"
        ]
    )
    return system_prompt, "".join(user_prompt)


def _build_empty_rule_retry_prompt(base_prompt: str, attempt: int, max_attempts: int) -> str:
    return (
        f"{base_prompt}\n\n"
        f"重试提示：你上一轮输出未形成可用规则。当前是第 {attempt} / {max_attempts} 次尝试。"
        "请至少输出 1 条可执行规则；如果确实没有明确证据支持任何目标字段，"
        "也必须返回空 JSON 数组 []，不要输出解释文字。"
    )


def _build_missing_target_retry_prompt(
    base_prompt: str,
    attempt: int,
    max_attempts: int,
    missing_fields: List[str],
) -> str:
    missing_text = ", ".join(missing_fields)
    return (
        f"{base_prompt}\n\n"
        f"重试提示：你上一轮遗漏了以下目标字段的规则：{missing_text}。"
        f"当前是第 {attempt} / {max_attempts} 次尝试。"
        "请补齐这些目标字段；如果某字段不依赖源字段，可以输出常量数值公式。"
        "仍然只允许输出 JSON 数组。"
    )


class _FormulaReferenceCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_Name(self, node: ast.Name) -> Any:
        self.names.add(node.id)


def _validate_rule_formula_fields(
    rule: Dict[str, Any],
    available_source_fields: Iterable[str],
) -> Tuple[bool, Optional[str]]:
    source_fields = [str(item).strip().upper() for item in (rule.get("source_fields") or []) if str(item).strip()]
    available = {str(item).strip().upper() for item in available_source_fields if str(item).strip()}
    if any(field not in available for field in source_fields):
        invalid = [field for field in source_fields if field not in available]
        return False, f"source_fields 引用了不存在的字段: {', '.join(invalid)}"

    formula_kind = str(rule.get("formula_kind") or "").strip() or _infer_formula_kind(str(rule.get("rule") or ""))
    formula = str(rule.get("rule") or "").strip()
    if not formula:
        return False, "rule 为空"
    if formula_kind == "mapping_table":
        return True, None
    if formula == "0":
        return True, None

    try:
        parse_mode = "exec" if formula_kind == "python_block" else "eval"
        tree = ast.parse(formula, mode=parse_mode)
    except SyntaxError as exc:
        return False, f"公式语法错误: {exc.msg}"

    collector = _FormulaReferenceCollector()
    collector.visit(tree)
    allowed_names = set(source_fields) | ALLOWED_FORMULA_FUNCTIONS | ALLOWED_FORMULA_VARS
    invalid_names = [
        name for name in collector.names
        if name not in allowed_names and not name.startswith("__")
    ]
    if invalid_names:
        return False, f"公式引用了未声明字段: {', '.join(sorted(set(invalid_names)))}"
    return True, None


def _build_target_task_maps(target_tasks: Optional[Iterable[Dict[str, Any]]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, set[str]]]:
    target_spec_map: Dict[str, Dict[str, Any]] = {}
    candidate_map: Dict[str, set[str]] = {}
    for item in target_tasks or []:
        target_field = str(item.get("field_name") or item.get("target_field") or "").strip().upper()
        if not target_field:
            continue
        target_spec_map[target_field] = dict(item)
        candidates = {
            str(candidate.get("field_name") or "").strip().upper()
            for candidate in (item.get("candidate_source_fields") or [])
            if str(candidate.get("field_name") or "").strip()
        }
        if candidates:
            candidate_map[target_field] = candidates
    return target_spec_map, candidate_map


def _validate_rule_semantic_alignment(
    rule: Dict[str, Any],
    target_spec_map: Dict[str, Dict[str, Any]],
    candidate_map: Dict[str, set[str]],
) -> Tuple[bool, Optional[str]]:
    target_field = str(rule.get("target_field") or "").strip().upper()
    source_fields = [str(item or "").strip().upper() for item in (rule.get("source_fields") or []) if str(item or "").strip()]
    if not target_field or not source_fields:
        return True, None

    candidate_fields = candidate_map.get(target_field)
    if candidate_fields and any(source_field not in candidate_fields for source_field in source_fields):
        invalid = [source_field for source_field in source_fields if source_field not in candidate_fields]
        return False, f"source_fields 未命中候选源字段: {', '.join(invalid)}"

    target_spec = target_spec_map.get(target_field) or {}
    target_groups = _semantic_groups_for_text(
        target_field,
        target_spec.get("label"),
        target_spec.get("description"),
        " ".join(str(part) for part in (target_spec.get("path_parts") or [])),
    )
    source_groups = _semantic_groups_for_text(" ".join(source_fields))
    if target_groups and source_groups and not (target_groups & source_groups):
        if (target_groups & STRICT_SEMANTIC_GROUPS) and (source_groups & STRICT_SEMANTIC_GROUPS):
            return False, (
                "目标字段与源字段语义组冲突: "
                f"target={','.join(sorted(target_groups))} "
                f"source={','.join(sorted(source_groups))}"
            )
        if _is_direct_copy_rule(rule):
            return False, (
                "直拷贝规则缺少语义一致性: "
                f"target={','.join(sorted(target_groups))} "
                f"source={','.join(sorted(source_groups))}"
            )
    return True, None


def _filter_valid_generated_rules(
    generated_rules: List[Dict[str, Any]],
    available_source_fields: Iterable[str],
    target_tasks: Optional[Iterable[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    valid_rules: List[Dict[str, Any]] = []
    filtered_rules: List[Dict[str, Any]] = []
    target_spec_map, candidate_map = _build_target_task_maps(target_tasks)
    for rule in generated_rules:
        is_valid, reason = _validate_rule_formula_fields(rule, available_source_fields)
        if is_valid:
            is_valid, reason = _validate_rule_semantic_alignment(rule, target_spec_map, candidate_map)
        if is_valid:
            valid_rules.append(rule)
            continue
        filtered_rule = dict(rule)
        filtered_rule["filtered_reason"] = reason
        filtered_rules.append(filtered_rule)
    return valid_rules, filtered_rules


def _generate_rule_items_with_retry(
    llm_client: LocalLLM,
    system_prompt: str,
    user_prompt: str,
    max_new_tokens: int,
    max_empty_rule_retries: int,
    required_target_fields: Optional[Iterable[Any]] = None,
) -> Tuple[str, List[Dict[str, Any]], int, List[str]]:
    last_raw_output = ""
    last_missing_fields: List[str] = []
    last_generated_rules: List[Dict[str, Any]] = []
    attempts = max(1, int(max_empty_rule_retries) + 1)
    for attempt in range(1, attempts + 1):
        if attempt == 1:
            prompt = user_prompt
        elif last_missing_fields:
            prompt = _build_missing_target_retry_prompt(user_prompt, attempt, attempts, last_missing_fields)
        else:
            prompt = _build_empty_rule_retry_prompt(user_prompt, attempt, attempts)
        raw_output = llm_client.generate(
            prompt=prompt,
            system_prompt=system_prompt,
            max_new_tokens=max_new_tokens,
            temperature=0.0,
            top_p=1.0,
            enable_thinking=False,
        )
        last_raw_output = raw_output
        parsed = LocalLLM.parse_json_from_response(raw_output, prefer=list)
        if parsed is None:
            parsed = LocalLLM.parse_json_from_response(raw_output, prefer=dict)
        rule_items = _extract_rule_items(parsed)
        generated_rules = normalize_generated_rules(rule_items)
        last_generated_rules = generated_rules
        if generated_rules:
            missing_fields = _missing_target_fields(generated_rules, required_target_fields)
            if not missing_fields:
                return raw_output, generated_rules, attempt, []
            last_missing_fields = missing_fields
            continue
        last_missing_fields = []

    if last_generated_rules:
        return last_raw_output, last_generated_rules, attempts, last_missing_fields

    if _normalize_required_target_fields(required_target_fields):
        return last_raw_output, [], attempts, _required_target_field_names(required_target_fields)

    snippet = " ".join(str(last_raw_output or "").strip().split())[:240] or "EMPTY"
    raise ValueError(
        f"协议规则生成失败：连续 {attempts} 次生成空规则，请检查提示词、模型输出或证据输入。最后一次输出片段: {snippet}"
    )


def _generate_rules_for_target_tasks(
    llm_client: LocalLLM,
    source_protocol: Dict[str, Optional[str]],
    target_protocol: Dict[str, Optional[str]],
    source_message: Optional[Any],
    pageindex_evidence: Optional[Dict[str, Any]],
    use_trained_docs: bool,
    target_tasks: List[Dict[str, Any]],
    max_new_tokens: int,
    max_empty_rule_retries: int,
    batch_size: int = 5,
) -> Tuple[str, List[Dict[str, Any]], int, int, List[Dict[str, Any]]]:
    raw_outputs: List[str] = []
    all_valid_rules: List[Dict[str, Any]] = []
    all_filtered_rules: List[Dict[str, Any]] = []
    total_attempt_count = 0
    for start in range(0, len(target_tasks), max(1, batch_size)):
        batch = target_tasks[start : start + max(1, batch_size)]
        system_prompt, user_prompt = build_protocol_rule_generation_prompt(
            source_protocol,
            target_protocol,
            source_message=source_message,
            pageindex_evidence=pageindex_evidence,
            use_trained_docs=use_trained_docs,
            required_target_fields=batch,
            target_tasks=batch,
        )
        raw_output, generated_rules, attempt_count, _remaining_missing_fields = _generate_rule_items_with_retry(
            llm_client=llm_client,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_new_tokens=max_new_tokens,
            max_empty_rule_retries=max_empty_rule_retries,
            required_target_fields=batch,
        )
        total_attempt_count += attempt_count
        raw_outputs.append(raw_output)
        available_source_fields = normalize_source_message(source_message).keys() if isinstance(source_message, dict) else []
        valid_rules, filtered_rules = _filter_valid_generated_rules(
            generated_rules,
            available_source_fields,
            target_tasks=batch,
        )
        all_valid_rules.extend(valid_rules)
        all_filtered_rules.extend(filtered_rules)
    return "\n\n".join(text for text in raw_outputs if text), all_valid_rules, total_attempt_count, max(0, total_attempt_count - len(list(range(0, len(target_tasks), max(1, batch_size))))), all_filtered_rules


def generate_protocol_field_rules(
    source_protocol: Any,
    target_protocol: Any,
    source_message: Optional[Any] = None,
    required_target_fields: Optional[Iterable[Any]] = None,
    use_knowledge_base: bool = True,
    use_page_index: bool = False,
    use_trained_docs: bool = False,
    project_id: str = "",
    dataset_id: str = "",
    doc_set_id: str = "",
    index_ref: str = "",
    evidence_provider: Optional[Any] = None,
    llm: Optional[LocalLLM] = None,
    max_new_tokens: int = 8192,
    max_empty_rule_retries: int = DEFAULT_EMPTY_RULE_RETRIES,
) -> Dict[str, Any]:
    normalized_source_protocol = _normalize_protocol_spec(
        source_protocol,
        "原",
        allow_empty_content=bool(use_page_index and use_trained_docs),
    )
    normalized_target_protocol = _normalize_protocol_spec(target_protocol, "目标")
    normalized_required_target_fields = _normalize_required_target_fields(required_target_fields)
    pageindex_evidence = None
    page_index_status = "disabled"
    rag_status = "disabled"
    rag_reason = None
    source_protocol_type = (
        normalized_source_protocol.get("protocol_type")
        or normalized_source_protocol.get("name")
        or DEFAULT_PROTOCOL_TYPE
    )
    target_protocol_type = (
        normalized_target_protocol.get("protocol_type")
        or normalized_target_protocol.get("name")
        or source_protocol_type
    )
    normalized_source_message = normalize_source_message(source_message) if isinstance(source_message, dict) else {}
    knowledge_base = ProtocolConversionKnowledgeBase.load(source_protocol_type) if use_knowledge_base else None
    graph_generated_rules: List[Dict[str, Any]] = []
    knowledge_graph_hit = False

    if knowledge_base and normalized_source_message:
        graph_rules = knowledge_base.find_rules_for_source_fields(
            source_fields=list(normalized_source_message.keys()),
            message_code=normalized_source_protocol.get("message_code"),
            target_protocol_type=target_protocol_type,
            target_message_code=normalized_target_protocol.get("message_code"),
        )
        graph_generated_rules = _dedupe_rules_by_target_field(
            [_knowledge_rule_to_generated_rule(rule) for rule in graph_rules]
        )
        knowledge_graph_hit = bool(graph_generated_rules)
        if graph_generated_rules and not _missing_target_fields(graph_generated_rules, normalized_required_target_fields):
            executable_rules = _build_executable_rules(graph_generated_rules, normalized_target_protocol)
            return {
                "source_protocol": normalized_source_protocol,
                "target_protocol": normalized_target_protocol,
                "raw_output": None,
                "generated_rules": graph_generated_rules,
                "normalized_rules": executable_rules,
                "kg_writeback_payload": _build_kg_writeback_payload(
                    generated_rules=[],
                    source_protocol=normalized_source_protocol,
                    target_protocol=normalized_target_protocol,
                ),
                "summary": {
                    "total_rules": len(graph_generated_rules),
                    "target_fields": [rule["target_field"] for rule in graph_generated_rules],
                    "page_index_status": "knowledge_graph_skipped" if use_page_index else "disabled",
                    "rag_status": "knowledge_graph_skipped" if use_page_index else "disabled",
                    "rag_reason": None,
                    "evidence_snippet_count": 0,
                    "doc_set_id": doc_set_id or None,
                    "index_ref": index_ref or None,
                    "attempt_count": 0,
                    "empty_rule_retry_count": 0,
                    "knowledge_graph_hit": True,
                    "knowledge_graph_backend": knowledge_base.to_summary().get("backend"),
                    "knowledge_graph_rule_count": len(graph_generated_rules),
                    "llm_rule_count": 0,
                    "validated_rule_count": len(graph_generated_rules),
                    "filtered_rule_count": 0,
                    "target_task_count": 0,
                    "llm_attempted_target_count": 0,
                    "default_zero_rule_count": 0,
                    "kg_writeback_rule_count": 0,
                    "missing_target_fields": [],
                },
            }

    llm_required_target_fields = (
        [
            item
            for item in normalized_required_target_fields
            if item["field_name"] in _missing_target_fields(graph_generated_rules, normalized_required_target_fields)
        ]
        if normalized_required_target_fields
        else normalized_required_target_fields
    )

    if use_page_index:
        if evidence_provider is not None:
            provider = evidence_provider
        elif use_trained_docs:
            provider = get_trained_doc_evidence_provider(
                project_id=project_id,
                dataset_id=dataset_id,
                doc_set_id=doc_set_id,
                index_ref=index_ref,
            )
        else:
            provider = get_pageindex_evidence_provider()
        evidence_target_protocol = dict(normalized_target_protocol)
        evidence_target_protocol["field_queries"] = [
            item["field_name"] for item in llm_required_target_fields or []
        ]
        pageindex_evidence = provider.collect_evidence(
            source_protocol=normalized_source_protocol,
            target_protocol=evidence_target_protocol,
            source_message=source_message,
        )
        rag_status, rag_reason = _resolve_pageindex_status(pageindex_evidence)
        page_index_status = rag_status

    target_tasks = _build_target_generation_tasks(
        required_target_fields=llm_required_target_fields,
        existing_rules=graph_generated_rules,
        normalized_source_message=normalized_source_message,
    )

    generated_rules: List[Dict[str, Any]] = []
    filtered_rules: List[Dict[str, Any]] = []
    raw_output = ""
    attempt_count = 0
    empty_rule_retry_count = 0
    if target_tasks:
        llm_client = llm or get_llm()
        raw_output, generated_rules, attempt_count, empty_rule_retry_count, filtered_rules = _generate_rules_for_target_tasks(
            llm_client=llm_client,
            source_protocol=normalized_source_protocol,
            target_protocol=normalized_target_protocol,
            source_message=source_message,
            pageindex_evidence=pageindex_evidence,
            use_trained_docs=use_trained_docs,
            target_tasks=target_tasks,
            max_new_tokens=max_new_tokens,
            max_empty_rule_retries=max_empty_rule_retries,
        )
    elif not normalized_required_target_fields:
        llm_client = llm or get_llm()
        system_prompt, user_prompt = build_protocol_rule_generation_prompt(
            normalized_source_protocol,
            normalized_target_protocol,
            source_message=source_message,
            pageindex_evidence=pageindex_evidence,
            use_trained_docs=use_trained_docs,
            required_target_fields=llm_required_target_fields,
        )
        raw_output, raw_generated_rules, attempt_count, _remaining_missing_fields = _generate_rule_items_with_retry(
            llm_client=llm_client,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            max_new_tokens=max_new_tokens,
            max_empty_rule_retries=max_empty_rule_retries,
            required_target_fields=llm_required_target_fields,
        )
        generated_rules, filtered_rules = _filter_valid_generated_rules(
            raw_generated_rules,
            normalized_source_message.keys(),
            target_tasks=llm_required_target_fields,
        )
        empty_rule_retry_count = max(0, attempt_count - 1)
    combined_generated_rules = _dedupe_rules_by_target_field(graph_generated_rules + generated_rules)
    default_zero_rules = _build_default_zero_rules(normalized_required_target_fields, combined_generated_rules)
    combined_generated_rules = _dedupe_rules_by_target_field(combined_generated_rules + default_zero_rules)
    executable_rules = _build_executable_rules(combined_generated_rules, normalized_target_protocol)
    llm_executable_rules = _build_executable_rules(generated_rules, normalized_target_protocol)
    kg_writeback_payload = _build_kg_writeback_payload(
        generated_rules=generated_rules,
        source_protocol=normalized_source_protocol,
        target_protocol=normalized_target_protocol,
        excluded_target_fields={str(rule.get("target_field") or "").strip().upper() for rule in default_zero_rules},
    )
    if knowledge_base and llm_executable_rules:
        knowledge_base.upsert_generated_rules(
            llm_executable_rules,
            protocol_type=source_protocol_type,
            message_code=normalized_source_protocol.get("message_code"),
            target_protocol_type=target_protocol_type,
            target_message_code=normalized_target_protocol.get("message_code"),
            source="llm_generated",
        )
    return {
        "source_protocol": normalized_source_protocol,
        "target_protocol": normalized_target_protocol,
        "raw_output": raw_output,
        "generated_rules": combined_generated_rules,
        "normalized_rules": executable_rules,
        "kg_writeback_payload": kg_writeback_payload,
        "summary": {
            "total_rules": len(combined_generated_rules),
            "target_fields": [rule["target_field"] for rule in combined_generated_rules],
            "page_index_status": page_index_status,
            "rag_status": rag_status,
            "rag_reason": rag_reason,
            "evidence_snippet_count": int((pageindex_evidence or {}).get("evidence_snippet_count") or 0),
            "doc_set_id": (pageindex_evidence or {}).get("doc_set_id") or (doc_set_id or None),
            "index_ref": (pageindex_evidence or {}).get("index_ref") or (index_ref or None),
            "attempt_count": attempt_count,
            "empty_rule_retry_count": empty_rule_retry_count,
            "knowledge_graph_hit": knowledge_graph_hit,
            "knowledge_graph_backend": knowledge_base.to_summary().get("backend") if knowledge_base else None,
            "knowledge_graph_rule_count": len(graph_generated_rules),
            "llm_rule_count": len(generated_rules),
            "validated_rule_count": len(generated_rules),
            "filtered_rule_count": len(filtered_rules),
            "target_task_count": len(target_tasks),
            "llm_attempted_target_count": len(target_tasks),
            "default_zero_rule_count": len(default_zero_rules),
            "kg_writeback_rule_count": len(kg_writeback_payload.get("rules") or []),
            "missing_target_fields": _missing_target_fields(combined_generated_rules, normalized_required_target_fields),
        },
    }


def generate_and_convert_protocol_bundle(
    source_protocol: Any,
    target_protocol: Any,
    source_message: Any,
    use_knowledge_base: bool = True,
    use_page_index: bool = False,
    use_trained_docs: bool = False,
    project_id: str = "",
    dataset_id: str = "",
    doc_set_id: str = "",
    index_ref: str = "",
    evidence_provider: Optional[Any] = None,
    llm: Optional[LocalLLM] = None,
) -> Dict[str, Any]:
    normalized_source_protocol = _normalize_protocol_spec(
        source_protocol,
        "原",
        allow_empty_content=bool(use_page_index and use_trained_docs),
    )
    normalized_target_protocol = _normalize_protocol_spec(target_protocol, "目标")
    source_protocol_type = (
        normalized_source_protocol.get("protocol_type")
        or normalized_source_protocol.get("name")
        or DEFAULT_PROTOCOL_TYPE
    )
    target_protocol_type = (
        normalized_target_protocol.get("protocol_type")
        or normalized_target_protocol.get("name")
        or source_protocol_type
    )
    source_fields = list(normalize_source_message(source_message).keys())

    knowledge_base = ProtocolConversionKnowledgeBase.load(source_protocol_type)
    knowledge_base_summary = knowledge_base.to_summary()
    graph_rules: List[Dict[str, Any]] = []
    if use_knowledge_base:
        graph_rules = [
            {
                "field_name": rule.field_name,
                "source_fields": list(rule.source_fields or [rule.field_name]),
                "target_field": rule.target_field,
                "conversion_mode": rule.conversion_mode,
                "formula_kind": rule.formula_kind,
                "formula": rule.formula,
                "rule": rule.formula,
                "unit": rule.unit,
                "bit_length": rule.bit_length,
                "description": rule.description,
                "concept_name": rule.concept_name,
                "target_protocol_type": rule.target_protocol_type,
                "target_message_code": rule.target_message_code,
                "source": "knowledge_base",
            }
            for rule in knowledge_base.find_rules_for_source_fields(
                source_fields=source_fields,
                message_code=normalized_source_protocol.get("message_code"),
                target_protocol_type=target_protocol_type,
                target_message_code=normalized_target_protocol.get("message_code"),
            )
        ]

    if graph_rules:
        rule_generation = {
            "source_protocol": normalized_source_protocol,
            "target_protocol": normalized_target_protocol,
            "raw_output": None,
            "generated_rules": graph_rules,
            "normalized_rules": graph_rules,
            "summary": {
                "total_rules": len(graph_rules),
                "target_fields": [rule.get("target_field") for rule in graph_rules],
                "knowledge_graph_hit": True,
                "knowledge_graph_backend": knowledge_base_summary.get("backend"),
                "page_index_status": "knowledge_graph_skipped" if use_page_index else "disabled",
                "evidence_snippet_count": 0,
            },
        }
    else:
        rule_generation = generate_protocol_field_rules(
            source_protocol=normalized_source_protocol,
            target_protocol=normalized_target_protocol,
            source_message=source_message,
            use_knowledge_base=use_knowledge_base,
            use_page_index=use_page_index,
            use_trained_docs=use_trained_docs,
            project_id=project_id,
            dataset_id=dataset_id,
            doc_set_id=doc_set_id,
            index_ref=index_ref,
            evidence_provider=evidence_provider,
            llm=llm,
        )

    conversion_result = execute_protocol_conversion(
        source_message=source_message,
        llm_formula_output=rule_generation["normalized_rules"],
        protocol_type=source_protocol_type,
        message_code=normalized_source_protocol.get("message_code"),
        target_protocol_type=target_protocol_type,
        target_message_code=normalized_target_protocol.get("message_code"),
        use_knowledge_base=use_knowledge_base,
    )
    return {
        "rule_generation": rule_generation,
        "conversion_result": conversion_result,
        "converted_message": conversion_result["converted_message"],
        "summary": {
            "generated_rule_count": rule_generation["summary"]["total_rules"],
            "knowledge_graph_hit": bool(graph_rules),
            "converted_field_count": len(conversion_result["converted_message"]),
            "conversion_success_count": conversion_result["summary"]["success_count"],
            "conversion_failed_count": conversion_result["summary"]["failed_count"],
            "knowledge_graph_backend": conversion_result["knowledge_base"].get("backend"),
            "page_index_status": rule_generation["summary"].get("page_index_status", "disabled"),
            "evidence_snippet_count": rule_generation["summary"].get("evidence_snippet_count", 0),
            "doc_set_id": rule_generation["summary"].get("doc_set_id"),
            "index_ref": rule_generation["summary"].get("index_ref"),
        },
    }
