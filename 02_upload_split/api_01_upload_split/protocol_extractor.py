"""PDF协议字段提取与LLM后处理工具。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from shared.llm.local_llm import LocalLLM, get_llm

_llm: Optional[LocalLLM] = None

PROSE_FIELD_PATTERN = re.compile(
    r"((?:J\d+\.\d+[A-Z]?\d*|[A-Z][A-Z0-9_]{2,})(?:\s+[A-Za-z0-9./\-+,]+){0,4})\s+field\s*\((\d+)\s*bits?\)",
    flags=re.IGNORECASE,
)


def get_llm_client() -> LocalLLM:
    """延迟加载LLM实例"""
    global _llm
    if _llm is None:
        _llm = get_llm()
    return _llm


def _to_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_formula(text: str) -> Optional[str]:
    patterns = [
        r"(?:公式|formula|conversion|convert)\s*[:：=]\s*([^\n;，。]+)",
        r"([A-Za-z_][A-Za-z0-9_\s]*\s*=\s*[^;\n]+)",
        r"(?:value|x|val)\s*(?:\s*[\*\/\+\-]\s*[0-9A-Za-z_().]+)+",
        r"((?:-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*)(?:\s*(?:,|，|;|；|and|AND)\s*-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*)*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        if match.lastindex:
            formula = match.group(1).strip()
        else:
            formula = match.group(0).strip()
        formula = re.sub(r"\s+(?:and|AND)\s+", ", ", formula)
        return formula
    return None


def _extract_range(text: str) -> Dict[str, Optional[float]]:
    patterns = [
        r"(?:range|范围)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)\s*(?:to|TO|~|～|—|–|-)\s*([+\-]?\d+(?:\.\d+)?)",
        r"(?:range|范围)\s*[:：]?\s*\[\s*([+\-]?\d+(?:\.\d+)?)\s*,\s*([+\-]?\d+(?:\.\d+)?)\s*\]",
        r"(?:min|最小值?)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)\D+(?:max|最大值?)\s*[:：]?\s*([+\-]?\d+(?:\.\d+)?)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        return {
            "range_min": _to_float(match.group(1)),
            "range_max": _to_float(match.group(2)),
        }
    return {"range_min": None, "range_max": None}


def _extract_bit_info(text: str) -> Dict[str, Optional[int]]:
    for pattern in [r"(\d+)\s*[-~～—–]\s*(\d+)", r"\bbit\s*(\d+)\s*[-~～—–]\s*(\d+)"]:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            left = int(match.group(1))
            right = int(match.group(2))
            return {"bit_start": min(left, right), "bit_length": abs(left - right) + 1}

    width_match = re.search(r"(\d+)\s*(?:bits?|位)", text, flags=re.IGNORECASE)
    if width_match:
        return {"bit_start": None, "bit_length": int(width_match.group(1))}
    return {"bit_start": None, "bit_length": None}


def _clean_field_name(raw_name: str) -> str:
    name = (raw_name or "").strip()
    name = re.sub(r"^[,;:.\-\s]+|[,;:.\-\s]+$", "", name)
    name = re.sub(r"^(?:the|The)\s+", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def _is_valid_field_name(field_name: str) -> bool:
    """过滤明显噪声字段名。"""
    name = _clean_field_name(field_name)
    if len(name) < 3:
        return False
    lowered = name.lower()
    if lowered in {
        "field",
        "fields",
        "value",
        "values",
        "message",
        "message use",
        "descriptor position bits resolution",
    }:
        return False
    if name.startswith("."):
        return False

    # 结构化协议字段优先放行
    if re.search(r"J\d+\.\d+[A-Z]?\d*", name, flags=re.IGNORECASE):
        return True
    if "_" in name and re.search(r"[A-Z]", name):
        return True
    if re.fullmatch(r"[A-Z0-9./\-\s]{3,}", name):
        return True

    alpha_chars = [ch for ch in name if ch.isalpha()]
    if not alpha_chars:
        return False
    lower_ratio = sum(1 for ch in alpha_chars if ch.islower()) / len(alpha_chars)
    # 自然语句片段（小写占比高）直接过滤
    if lower_ratio > 0.35:
        return False
    return bool(re.search(r"[A-Z]", name))


def _extract_prose_fields(line: str) -> List[Dict[str, Any]]:
    """从自然语句中提取“X field (N bits)”类字段。"""
    fields: List[Dict[str, Any]] = []
    for match in PROSE_FIELD_PATTERN.finditer(line):
        raw_name = match.group(1)
        bit_length = int(match.group(2))
        field_name = _clean_field_name(raw_name)
        if not _is_valid_field_name(field_name):
            continue
        range_info = _extract_range(line)
        formula = _extract_formula(line)
        fields.append(
            {
                "field_name": field_name,
                "bit_start": None,
                "bit_length": bit_length,
                "meaning": line[:240].strip(),
                "formula": formula,
                "range_min": range_info["range_min"],
                "range_max": range_info["range_max"],
            }
        )
    return fields


def _extract_pipe_row_field(line: str) -> Optional[Dict[str, Any]]:
    """从管道分隔行中提取字段。"""
    parts = [p.strip() for p in line.split("|")]
    if len(parts) < 2:
        return None
    field_name = _clean_field_name(parts[0])
    if not _is_valid_field_name(field_name):
        return None

    candidate = " ".join(parts)
    bit_info = _extract_bit_info(parts[1] if len(parts) > 1 else candidate)
    range_info = _extract_range(candidate)
    formula = _extract_formula(candidate)
    has_mapping = bool(re.search(r"-?\d+\s*(?:=|->|→)\s*[A-Za-z_]", candidate))
    has_signal = (
        bit_info["bit_start"] is not None
        or bit_info["bit_length"] is not None
        or range_info["range_min"] is not None
        or range_info["range_max"] is not None
        or formula is not None
        or has_mapping
    )
    if not has_signal:
        return None

    if len(parts) >= 3:
        meaning = " | ".join([p for p in parts[2:] if p]).strip()
    elif len(parts) > 1:
        meaning = parts[-1]
    else:
        meaning = candidate[:240]

    return {
        "field_name": field_name,
        "bit_start": bit_info["bit_start"],
        "bit_length": bit_info["bit_length"],
        "meaning": meaning,
        "formula": formula,
        "range_min": range_info["range_min"],
        "range_max": range_info["range_max"],
    }


def _normalize_mapping_label(label: str) -> str:
    cleaned = re.sub(r"\(.*?\)", "", label or "")
    cleaned = cleaned.strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = re.sub(r"[^A-Za-z0-9_./\-]", "", cleaned)
    return cleaned.upper()


def _extract_mapping_table_fields(content: str) -> List[Dict[str, Any]]:
    """从任务离散值映射表中提取字段映射关系。"""
    field_name = None
    if re.search(r"\bJ12\.0\s+MAD\b", content, flags=re.IGNORECASE):
        field_name = "MISSION_ASSIGNMENT_DISCRETE"
    elif re.search(r"Receipt/?Compliance", content, flags=re.IGNORECASE):
        field_name = "RECEIPT_COMPLIANCE"
    if not field_name:
        return []

    pairs: Dict[int, str] = {}
    line_patterns = [
        re.compile(r"^\s*([A-Za-z][A-Za-z0-9/() \-]{1,90})\s*\|\s*(\d{1,3})\s*\|", re.MULTILINE),
        re.compile(r"^\s*([A-Za-z][A-Za-z0-9/() \-]{1,90})\s+(\d{1,3})\s+Table\s+\d", re.MULTILINE),
    ]
    for pattern in line_patterns:
        for match in pattern.finditer(content):
            label = (match.group(1) or "").strip()
            code = int(match.group(2))
            if not label:
                continue
            lowered = label.lower()
            if lowered in {"destruction orders", "interception orders", "procedural orders", "value"}:
                continue
            normalized_label = _normalize_mapping_label(label)
            if not normalized_label:
                continue
            if code not in pairs:
                pairs[code] = normalized_label

    if not pairs:
        return []

    sorted_pairs = sorted(pairs.items(), key=lambda item: item[0])[:40]
    formula = ", ".join([f"{code}={label}" for code, label in sorted_pairs])

    return [
        {
            "field_name": field_name,
            "bit_start": None,
            "bit_length": None,
            "meaning": "离散值映射表",
            "formula": formula,
            "range_min": None,
            "range_max": None,
        }
    ]


def rule_extract_protocol_fields(content: str) -> List[Dict[str, Any]]:
    """规则提取：解析字段名、位信息、范围与公式"""
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    fields: List[Dict[str, Any]] = []

    for line in lines:
        # 管道分隔的结构化行
        if "|" in line:
            parsed = _extract_pipe_row_field(line)
            if parsed:
                fields.append(parsed)
            continue

        # 自然语句中的“field (N bits)”模式
        fields.extend(_extract_prose_fields(line))

    fields.extend(_extract_mapping_table_fields(content))

    unique = {}
    for item in fields:
        key = (item.get("field_name", "").lower(), item.get("bit_start"), item.get("bit_length"))
        if key not in unique:
            unique[key] = item
    return list(unique.values())[:80]


def llm_extract_protocol_fields(content: str, page_num: int, block_type: str) -> List[Dict[str, Any]]:
    """LLM后处理：提取规则难处理的字段定义"""
    llm = get_llm_client()
    system_prompt = (
        "你是协议字段提取专家。返回严格JSON。格式:"
        "{\"protocol_fields\":[{\"field_name\":str,\"bit_start\":int|null,"
        "\"bit_length\":int|null,\"meaning\":str|null,\"formula\":str|null,"
        "\"range_min\":number|null,\"range_max\":number|null}]}"
    )
    user_prompt = (
        f"页面={page_num}，块类型={block_type}。\n"
        "请提取字段定义、位信息、范围和转换公式，未知填null。\n"
        f"内容:\n{content[:2800]}"
    )

    parsed = llm.extract_json(user_prompt, system_prompt=system_prompt)
    if not parsed:
        return []

    if isinstance(parsed, list):
        candidates = parsed
    elif isinstance(parsed, dict):
        candidates = parsed.get("protocol_fields") or parsed.get("fields") or []
    else:
        candidates = []

    results: List[Dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        if not item.get("field_name"):
            continue
        results.append(
            {
                "field_name": str(item.get("field_name")).strip(),
                "bit_start": item.get("bit_start"),
                "bit_length": item.get("bit_length"),
                "meaning": item.get("meaning"),
                "formula": item.get("formula"),
                "range_min": item.get("range_min"),
                "range_max": item.get("range_max"),
            }
        )
    return results


def enrich_protocol_metadata(block: Dict[str, Any], enable_llm_postprocess: bool) -> Dict[str, Any]:
    """补充块级协议字段元数据，必要时启用LLM后处理"""
    content = block.get("content", "")
    metadata = block.setdefault("metadata", {})

    rule_fields = rule_extract_protocol_fields(content)
    if rule_fields:
        metadata["protocol_fields"] = rule_fields
        metadata["extract_method"] = "rule"

    should_use_llm = (
        enable_llm_postprocess
        and len(content) > 80
        and (
            not rule_fields
            or any(f.get("bit_start") is None and f.get("bit_length") is None for f in rule_fields)
        )
        and bool(re.search(r"(J\d+\.\d+|DUI|field|字段|bit|位|resolution|分辨率)", content, re.IGNORECASE))
    )

    if should_use_llm:
        llm_fields = llm_extract_protocol_fields(content, block.get("page_num", 0), block.get("type", "text"))
        if llm_fields:
            existing = {(f.get("field_name", "").lower(), f.get("bit_start"), f.get("bit_length")) for f in rule_fields}
            merged = list(rule_fields)
            for field in llm_fields:
                key = (field.get("field_name", "").lower(), field.get("bit_start"), field.get("bit_length"))
                if key in existing:
                    continue
                merged.append(field)
                existing.add(key)
            metadata["protocol_fields"] = merged
            metadata["llm_postprocessed"] = True
            metadata["llm_field_count"] = len(llm_fields)
            metadata["extract_method"] = "rule+llm" if rule_fields else "llm"

    return block
