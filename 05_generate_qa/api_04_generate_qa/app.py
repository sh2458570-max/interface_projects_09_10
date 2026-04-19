# 接口4: QA对生成
# POST /api/knowledge/generate_qa

import os
import sys
import json
import time
import uuid
import re
from typing import List, Dict, Any, Optional, Tuple, Set
from flask import Flask, request, jsonify

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from shared.database.mysql_client import MySQLClient
except Exception:
    MySQLClient = None
from shared.database.models import Block, Chunk, QAPair
from shared.llm.local_llm import LocalLLM, get_llm
from shared.llm.prompt_templates import PromptTemplates
from shared.protocol_conversion.knowledge_base import ProtocolConversionKnowledgeBase
from shared.utils.file_store import FileStore

app = Flask(__name__)


class _UnavailableDBClient:
    """测试或依赖缺失时的占位 DB 客户端。"""

    def connection(self):
        raise RuntimeError("数据库客户端不可用")

    def __getattr__(self, name: str):
        raise RuntimeError(f"数据库客户端不可用: {name}")


# 初始化客户端
db_client = MySQLClient() if MySQLClient is not None else _UnavailableDBClient()
llm_client: Optional[LocalLLM] = None
file_store = FileStore()
protocol_kb: Optional[ProtocolConversionKnowledgeBase] = None

TASK_TYPE_ALIASES = {
    "协议理解类": "protocol_understanding",
    "协议理解": "protocol_understanding",
    "understanding": "protocol_understanding",
    "protocol_understanding": "protocol_understanding",
    "协议转换类": "protocol_conversion",
    "协议转换": "protocol_conversion",
    "conversion": "protocol_conversion",
    "protocol_conversion": "protocol_conversion",
}

CONVERSION_MODE_ALIASES = {
    "转义": "transcoding",
    "transcoding": "transcoding",
    "转换": "mapping",
    "mapping": "mapping",
}

USE_LLM_QUALITY_CHECK = os.getenv("USE_LLM_QUALITY_CHECK", "false").lower() == "true"
QA_GENERATION_RETRY = max(0, int(os.getenv("QA_GENERATION_RETRY", "1")))
UNDERSTANDING_SEMANTIC_TYPES = {"field_definition", "protocol_description", "single_block", "general_content"}
CONVERSION_SEMANTIC_TYPES = {"conversion_rule", "table_data"}
RULE_SIGNAL_PATTERN = re.compile(
    r"(?:formula|公式|mapping|映射|range|范围|resolution|分辨率|bit|位|value\s*[\*\/\+\-]|->|→|=)",
    flags=re.IGNORECASE,
)
NOISE_HINT_PATTERN = re.compile(
    r"(?:^table\s+\d|^figure\s+\d|continued|page\s+\d+|页眉|页脚|保密|密级|^\W*$)",
    flags=re.IGNORECASE,
)


def get_llm_client() -> LocalLLM:
    """获取LLM客户端单例"""
    global llm_client
    if llm_client is None:
        llm_client = get_llm()
    return llm_client


def get_protocol_kb(protocol_type: str = "Link16") -> Optional[ProtocolConversionKnowledgeBase]:
    """获取协议转换知识库单例。"""
    global protocol_kb
    if protocol_kb is None:
        try:
            protocol_kb = ProtocolConversionKnowledgeBase.load(protocol_type)
        except Exception:
            protocol_kb = None
    return protocol_kb


def normalize_task_type(value: Optional[str]) -> str:
    if not value:
        return "protocol_understanding"
    raw = str(value).strip()
    return TASK_TYPE_ALIASES.get(raw) or TASK_TYPE_ALIASES.get(raw.lower(), "protocol_understanding")


def normalize_conversion_mode(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if raw == "" or raw.lower() in {"null", "none"}:
        return None
    normalized = CONVERSION_MODE_ALIASES.get(raw) or CONVERSION_MODE_ALIASES.get(raw.lower())
    return normalized


def looks_like_block_formula(text: str) -> bool:
    """判断文本是否为多行块公式。"""
    cleaned = LocalLLM._sanitize_response_text(str(text or "")).strip()
    if "\n" not in cleaned:
        return False
    return any(
        token in cleaned
        for token in ("\nif ", "\nfor ", "\nwhile ", "\nelse:", "result =", "\nresult =")
    ) or cleaned.startswith(("if ", "for ", "while ", "result ="))


def extract_formula_only(text: str) -> str:
    """从文本中抽取公式，若无明显公式则返回原文本首行"""
    if not text:
        return ""
    sanitized = LocalLLM._sanitize_response_text(str(text)).strip()
    if looks_like_block_formula(sanitized):
        return sanitized
    formula_patterns = [
        r"(?:公式|formula|conversion)\s*[:：=]\s*([^\n;，。]+)",
        r"((?:-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*)(?:\s*(?:,|，|;|；|and|AND)\s*-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*)*)",
        r"([A-Za-z_][A-Za-z0-9_\s]*\s*=\s*[^;\n]+)",
        r"((?:value|x|val)\s*(?:\s*[\*\/\+\-]\s*[0-9A-Za-z_().]+)+)",
    ]
    for pattern in formula_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            if match.lastindex:
                formula = match.group(1).strip()
            else:
                formula = match.group(0).strip()
            formula = re.sub(r"\s+(?:and|AND)\s+", ", ", formula)
            return formula
    fallback = text.splitlines()[0].strip()
    fallback = re.sub(r"\s+(?:and|AND)\s+", ", ", fallback)
    return fallback


def contains_arithmetic_expression(text: str) -> bool:
    """判断文本是否包含可计算算术表达式。"""
    if not text:
        return False
    if looks_like_block_formula(text):
        return bool(re.search(r"[\*\/\+\-]", text) or re.search(r"\b(?:signed|unsigned|scale|clip|int|float|min|max|sum)\s*\(", text))
    return bool(
        re.search(r"[A-Za-z_][A-Za-z0-9_]*\s*[\*\/\+\-]\s*[0-9A-Za-z_(]", text)
        or (
            re.search(r"\b(?:value|raw|physical|lat|lon|x|val)\b", text, flags=re.IGNORECASE)
            and re.search(r"[\*\/\+\-]", text)
        )
    )


def infer_conversion_mode(formula_text: str) -> str:
    """根据转换表达式内容推断conversion_mode。"""
    text = str(formula_text or "").strip()
    if not text:
        return "mapping"
    if looks_like_block_formula(text):
        if contains_arithmetic_expression(text):
            return "transcoding"
        return "mapping"
    if re.search(r"(?:->|→|=>)", text):
        return "mapping"
    if re.search(r"-?\d+\s*=\s*[A-Za-z_]", text):
        return "mapping"
    if re.search(r"[A-Za-z_]+\s*=\s*-?\d+", text):
        return "mapping"
    if contains_arithmetic_expression(text):
        return "transcoding"
    return "mapping"


def contains_mapping_relation(text: str) -> bool:
    """判断文本是否包含离散值映射关系。"""
    if not text:
        return False
    if looks_like_block_formula(text):
        return "result =" in text or "if " in text
    return bool(re.search(r"-?\d+\s*(?:=|->|→)\s*[A-Za-z_]", text))


def normalize_conversion_payload(
    answer: str,
    conversion_formula: Optional[str],
    conversion_mode: Optional[str],
) -> Dict[str, str]:
    """规范化协议转换类字段，确保公式与模式一致。"""
    normalized_answer = extract_formula_only(str(answer or "").strip())
    normalized_formula = extract_formula_only(str(conversion_formula or normalized_answer).strip())
    if not looks_like_block_formula(normalized_answer) and normalized_formula != normalized_answer:
        normalized_formula = normalized_answer

    inferred_mode = infer_conversion_mode(normalized_formula or normalized_answer)
    normalized_mode = normalize_conversion_mode(conversion_mode) or inferred_mode
    if normalized_mode != inferred_mode:
        normalized_mode = inferred_mode

    return {
        "answer": normalized_answer,
        "conversion_formula": normalized_formula,
        "conversion_mode": normalized_mode,
    }


def normalize_source_fields_value(value: Any, fallback: Optional[str] = None) -> List[str]:
    """规范化 source_fields。"""
    normalized: List[str] = []
    if isinstance(value, list):
        normalized = [str(item).strip().upper() for item in value if str(item).strip()]
    elif isinstance(value, str):
        normalized = [item.strip().upper() for item in value.split(",") if item.strip()]
    if normalized:
        return normalized
    fallback_name = str(fallback or "").strip().upper()
    return [fallback_name] if fallback_name else []


def build_field_context(content: str) -> Dict[str, Dict[str, str]]:
    """从原始协议文本构建字段上下文，供理解类答案增强。"""
    context: Dict[str, Dict[str, str]] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            continue
        field_name = parts[0].strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9_./\-]{2,}", field_name):
            continue
        bit_segment = parts[1].strip() if len(parts) > 1 else ""
        details = " | ".join([p for p in parts[2:] if p]).strip()
        context[field_name] = {
            "bit_segment": bit_segment,
            "details": details,
        }
    return context


def match_field_name(question: str, field_context: Dict[str, Dict[str, str]]) -> Optional[str]:
    """根据问题文本匹配字段名。"""
    question_upper = str(question or "").upper()
    for field_name in field_context:
        if field_name in question_upper:
            return field_name
    return None


def extract_field_name_from_question(question: str) -> Optional[str]:
    """从问题文本中提取疑似源字段名。"""
    matches = re.findall(r"[A-Z][A-Z0-9_./\-]{2,}", str(question or "").upper())
    if not matches:
        return None
    stop_words = {"WHAT", "HOW", "WHY", "THE", "AND", "FOR", "LINK16", "PROTOCOL"}
    for candidate in matches:
        if candidate not in stop_words:
            return candidate
    return matches[0]


def resolve_conversion_metadata(
    question: str,
    field_context: Dict[str, Dict[str, str]],
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """解析协议转换问答的源字段、目标字段与知识库规则。"""
    source_field = match_field_name(question, field_context) or extract_field_name_from_question(question)
    if not source_field:
        return {}

    metadata: Dict[str, Optional[str]] = {"source_field": source_field, "target_field": None, "kb_formula": None, "kb_mode": None}
    kb = get_protocol_kb(protocol_type)
    if kb is None:
        return metadata

    rule = kb.find_rule(field_name=source_field, message_code=message_code)
    if rule is None:
        return metadata

    metadata["target_field"] = rule.target_field
    metadata["kb_formula"] = rule.formula
    metadata["kb_mode"] = rule.conversion_mode
    return metadata


def enrich_conversion_payload_with_context(
    question: str,
    payload: Dict[str, str],
    field_context: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    """当转换答案不完整时，从字段上下文或知识库回填映射/公式与目标字段。"""
    metadata = resolve_conversion_metadata(question, field_context)
    if metadata.get("source_field") and not payload.get("source_field"):
        payload["source_field"] = metadata["source_field"]
    if metadata.get("target_field") and not payload.get("target_field"):
        payload["target_field"] = metadata["target_field"]

    mode = payload.get("conversion_mode") or metadata.get("kb_mode") or "mapping"
    payload["conversion_mode"] = mode
    answer = payload.get("answer", "")

    needs_mapping = mode == "mapping" and not contains_mapping_relation(answer)
    needs_formula = mode == "transcoding" and not contains_arithmetic_expression(answer)
    if not (needs_mapping or needs_formula):
        return payload

    field_name = metadata.get("source_field")
    details = field_context.get(field_name or "", {}).get("details", "") if field_name else ""
    fallback = extract_formula_only(details) if details else ""
    kb_formula = str(metadata.get("kb_formula") or "")

    if needs_mapping:
        if contains_mapping_relation(fallback):
            payload["answer"] = fallback
            payload["conversion_formula"] = fallback
        elif contains_mapping_relation(kb_formula):
            payload["answer"] = kb_formula
            payload["conversion_formula"] = kb_formula
    elif needs_formula:
        if contains_arithmetic_expression(fallback):
            payload["answer"] = fallback
            payload["conversion_formula"] = fallback
        elif contains_arithmetic_expression(kb_formula):
            payload["answer"] = kb_formula
            payload["conversion_formula"] = kb_formula

    return payload


def enhance_understanding_answer(
    question: str,
    answer: str,
    field_context: Dict[str, Dict[str, str]],
) -> str:
    """基于字段上下文增强过短的协议理解类答案。"""
    normalized_answer = str(answer or "").strip()
    if len(normalized_answer) >= 16 and re.search(r"\d", normalized_answer):
        return normalized_answer

    matched_field = match_field_name(question, field_context)
    if not matched_field:
        return normalized_answer

    info = field_context.get(matched_field, {})
    segments: List[str] = []

    bit_segment = info.get("bit_segment", "").strip()
    details = info.get("details", "").strip()
    if bit_segment:
        segments.append(f"{matched_field}位段{bit_segment}")

    resolution_match = re.search(
        r"(?:resolution|分辨率)\s*([+\-]?\d+(?:\.\d+)?\s*[A-Za-z%°/]+)",
        details,
        flags=re.IGNORECASE,
    )
    if resolution_match:
        segments.append(f"分辨率{resolution_match.group(1).strip()}")

    range_match = re.search(
        r"(?:range|范围)\s*([+\-]?\d+(?:\.\d+)?)\s*(?:to|TO|~|～|—|–|-)\s*([+\-]?\d+(?:\.\d+)?)",
        details,
        flags=re.IGNORECASE,
    )
    if range_match:
        segments.append(f"范围{range_match.group(1)}到{range_match.group(2)}")

    mapping_pairs = re.findall(
        r"-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*",
        details,
        flags=re.IGNORECASE,
    )
    if mapping_pairs:
        segments.append(f"映射{', '.join(mapping_pairs[:4])}")

    if not segments and details:
        segments.append(details)
    if not segments:
        return normalized_answer

    enhanced = "，".join(segments).strip("，")
    if not enhanced:
        return normalized_answer
    if not enhanced.endswith("。"):
        enhanced += "。"
    return enhanced


def build_task_spec(task_types: List[str], conversion_modes: List[str], count: int) -> str:
    readable_task_types = ", ".join(task_types)
    readable_modes = ", ".join(conversion_modes) if conversion_modes else "transcoding, mapping"
    understanding_min = 0
    conversion_min = 0
    if "protocol_understanding" in task_types and "protocol_conversion" in task_types:
        conversion_min = max(1, count // 3)
        understanding_min = max(1, count - conversion_min)
    elif "protocol_conversion" in task_types:
        conversion_min = count
    else:
        understanding_min = count

    coverage_rule = (
        f"至少生成{understanding_min}条protocol_understanding与{conversion_min}条protocol_conversion。"
        if understanding_min and conversion_min
        else (
            f"所有问答均为protocol_conversion，至少{conversion_min}条。"
            if conversion_min
            else f"所有问答均为protocol_understanding，至少{understanding_min}条。"
        )
    )
    return (
        f"必须覆盖任务类型: {readable_task_types}。\n"
        f"{coverage_rule}\n"
        f"协议转换类允许的conversion_mode: {readable_modes}。\n"
        "对协议转换类，answer 必须输出可执行的值到值公式，允许单行表达式、mapping_table，或多行 if/else/for 公式块。\n"
        "如使用多行公式块，必须把最终结果赋给 result，conversion_formula 与 answer 保持一致。\n"
        "若输出为离散映射表（如5=10, 1=20），conversion_mode必须为mapping。\n"
        "只输出JSON数组，不要输出<think>、解释、前后缀文本。"
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _chunk_quality_score(chunk: Chunk) -> float:
    metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
    quality_score = float(metadata.get("quality_score", 0) or 0)
    field_count = float(metadata.get("field_count", 0) or 0)
    formula_count = float(metadata.get("formula_count", 0) or 0)
    mapping_pair_count = float(metadata.get("mapping_pair_count", 0) or 0)
    bit_coverage_count = float(metadata.get("bit_coverage_count", 0) or 0)
    evidence_score = float(metadata.get("evidence_score", 0) or 0)
    noise_penalty = float(metadata.get("noise_penalty", 0) or 0)
    noisy_block_count = float(metadata.get("noisy_block_count", 0) or 0)
    return (
        quality_score
        + field_count * 1.8
        + formula_count * 1.5
        + mapping_pair_count * 1.2
        + bit_coverage_count
        + evidence_score * 0.45
        - noise_penalty * 1.1
        - noisy_block_count * 1.5
    )


def _normalize_target_protocol(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return raw


def _chunk_matches_target_protocol(chunk: Chunk, target_protocol: str) -> bool:
    target = _normalize_target_protocol(target_protocol)
    if not target:
        return False
    metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
    anchor = str(metadata.get("protocol_anchor") or "").strip().upper()
    if anchor.startswith(target):
        return True
    field_names = metadata.get("field_names") if isinstance(metadata.get("field_names"), list) else []
    for name in field_names:
        if str(name).strip().upper().startswith(target):
            return True
    return target in str(chunk.content_snapshot or "").upper()


def _estimate_text_noise_penalty(text: str) -> float:
    cleaned = str(text or "").strip()
    if not cleaned:
        return 12.0
    lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
    line_count = len(lines) or 1
    short_lines = sum(1 for line in lines if len(line) <= 6)
    noise_hits = sum(1 for line in lines if NOISE_HINT_PATTERN.search(line))
    punctuation_chars = sum(1 for ch in cleaned if not ch.isalnum() and not ch.isspace())
    punctuation_ratio = punctuation_chars / max(len(cleaned), 1)
    penalty = 0.0
    if line_count >= 4 and short_lines / line_count >= 0.55:
        penalty += 4.0
    if noise_hits:
        penalty += min(6.0, noise_hits * 1.5)
    if punctuation_ratio >= 0.32:
        penalty += 2.0
    if len(cleaned) < 36 and not RULE_SIGNAL_PATTERN.search(cleaned):
        penalty += 3.0
    return round(penalty, 4)


def _build_chunk_adjustment_map(chunks: List[Chunk], target_protocol: str) -> Dict[str, Dict[str, float]]:
    """Build target/evidence/noise adjustments for QA chunk selection."""
    target = _normalize_target_protocol(target_protocol)
    keywords = {target} if target else set()
    major_match = re.match(r"(J\d+)", target)
    if major_match:
        keywords.add(major_match.group(1))

    all_block_ids: List[int] = []
    for chunk in chunks:
        all_block_ids.extend([bid for bid in (chunk.source_block_ids or []) if isinstance(bid, int)])

    block_map: Dict[int, Block] = {}
    if all_block_ids:
        try:
            blocks = db_client.get_blocks_by_ids(list(dict.fromkeys(all_block_ids)))
            block_map = {block.block_id: block for block in blocks}
        except Exception:
            block_map = {}

    adjustment_map: Dict[str, Dict[str, float]] = {}
    for chunk in chunks:
        metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
        chunk_text = f"{chunk.content_snapshot or ''} {' '.join(str(v) for v in (metadata.get('field_names') or []))}".upper()
        anchor = str(metadata.get("protocol_anchor") or "").strip().upper()
        field_count = float(metadata.get("field_count", 0) or 0)
        formula_count = float(metadata.get("formula_count", 0) or 0)
        mapping_pair_count = float(metadata.get("mapping_pair_count", 0) or 0)
        evidence_score = float(metadata.get("evidence_score", 0) or 0)
        noise_penalty = float(metadata.get("noise_penalty", 0) or 0)

        target_bonus = 0.0
        if target:
            if anchor == target:
                target_bonus += 20.0
            elif anchor and anchor.startswith(target):
                target_bonus += 16.0
            elif _chunk_matches_target_protocol(chunk, target):
                target_bonus += 12.0

        for keyword in keywords:
            if keyword and keyword in chunk_text:
                target_bonus = max(target_bonus, 18.0 if keyword == target else 10.0)

        field_bonus = min(16.0, field_count * 1.6)
        formula_bonus = min(18.0, formula_count * 2.2 + mapping_pair_count * 1.8)
        selection_bonus = target_bonus + field_bonus + formula_bonus + min(18.0, evidence_score * 0.35)

        for bid in chunk.source_block_ids or []:
            block = block_map.get(bid)
            if not block:
                continue
            content = str((block.cleaned_content or block.content or "")).strip()
            upper_content = content.upper()
            if target:
                for keyword in keywords:
                    if keyword and keyword in upper_content:
                        target_bonus = max(target_bonus, 26.0 if keyword == target else 14.0)
                        selection_bonus = max(selection_bonus, target_bonus + field_bonus + formula_bonus)
            if RULE_SIGNAL_PATTERN.search(content):
                formula_bonus = max(formula_bonus, 8.0)
            noise_penalty = max(noise_penalty, _estimate_text_noise_penalty(content))

        adjustment_map[chunk.chunk_id] = {
            "target_bonus": round(target_bonus, 4),
            "field_bonus": round(field_bonus, 4),
            "formula_bonus": round(formula_bonus, 4),
            "selection_bonus": round(target_bonus + field_bonus + formula_bonus + min(18.0, evidence_score * 0.35), 4),
            "noise_penalty": round(min(24.0, noise_penalty), 4),
        }
    return adjustment_map


def _build_target_bonus_map(chunks: List[Chunk], target_protocol: str) -> Dict[str, float]:
    """基于chunk关联原始块内容计算目标协议相关性加权。"""
    adjustment_map = _build_chunk_adjustment_map(chunks, target_protocol)
    return {chunk_id: item.get("target_bonus", 0.0) for chunk_id, item in adjustment_map.items()}


def _score_chunk_for_understanding(chunk: Chunk, target_protocol: str = "") -> float:
    score = _chunk_quality_score(chunk)
    semantic_type = str(chunk.semantic_type or "").strip()
    metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
    score += 10.0 if semantic_type in UNDERSTANDING_SEMANTIC_TYPES else 0.0
    score += float(metadata.get("field_count", 0) or 0) * 1.3
    score += float(metadata.get("bit_coverage_count", 0) or 0) * 1.2
    score += float(metadata.get("range_coverage_count", 0) or 0) * 1.0
    if _chunk_matches_target_protocol(chunk, target_protocol):
        score += 12.0
    return score


def _score_chunk_for_conversion(chunk: Chunk, target_protocol: str = "") -> float:
    score = _chunk_quality_score(chunk)
    semantic_type = str(chunk.semantic_type or "").strip()
    metadata = chunk.metadata if isinstance(chunk.metadata, dict) else {}
    score += 12.0 if semantic_type in CONVERSION_SEMANTIC_TYPES else 0.0
    score += float(metadata.get("formula_count", 0) or 0) * 1.8
    score += float(metadata.get("mapping_pair_count", 0) or 0) * 1.6
    if _chunk_matches_target_protocol(chunk, target_protocol):
        score += 12.0
    return score


def llm_rerank_chunk_ids(
    candidate_chunks: List[Chunk],
    task_types: List[str],
    selection_limit: int,
    target_protocol: str = "",
) -> Optional[List[str]]:
    """对规则选出的候选chunks进行轻量LLM重排。"""
    if not candidate_chunks:
        return None
    try:
        llm = get_llm_client()
        payload = {
            "selection_limit": selection_limit,
            "task_types": task_types,
            "target_protocol": target_protocol,
            "chunks": [
                {
                    "chunk_id": chunk.chunk_id,
                    "semantic_type": chunk.semantic_type,
                    "quality_score": _chunk_quality_score(chunk),
                    "metadata": chunk.metadata or {},
                    "content_preview": (chunk.content_snapshot or "")[:260],
                }
                for chunk in candidate_chunks[:20]
            ],
        }
        system_prompt = (
            "你是协议问答选块助手。"
            "从给定chunks中挑选最适合生成高质量QA的chunk_id。"
            "返回JSON: {\"selected_chunk_ids\": [\"chk_xxx\", ...]}，顺序即优先级。"
        )
        result = llm.extract_json(json.dumps(payload, ensure_ascii=False), system_prompt=system_prompt)
        if isinstance(result, dict) and isinstance(result.get("selected_chunk_ids"), list):
            selected = []
            seen = set()
            valid_ids = {chunk.chunk_id for chunk in candidate_chunks}
            for chunk_id in result["selected_chunk_ids"]:
                cid = str(chunk_id).strip()
                if cid and cid in valid_ids and cid not in seen:
                    selected.append(cid)
                    seen.add(cid)
                if len(selected) >= selection_limit:
                    break
            if selected:
                return selected
    except Exception:
        return None
    return None


def select_chunks_for_qa(
    chunks: List[Chunk],
    task_types: List[str],
    count: int,
    selection_config: Optional[Dict[str, Any]] = None,
) -> List[Chunk]:
    """根据任务类型自动选择最优chunks，优先规则，必要时LLM重排。"""
    if not chunks:
        return []
    selection_config = selection_config or {}
    top_k = _safe_int(selection_config.get("top_k_chunks"), max(4, min(16, count * 2)))
    top_k = max(1, min(40, top_k))
    enable_llm_rerank = bool(selection_config.get("enable_llm_rerank", False))
    target_protocol = _normalize_target_protocol(
        selection_config.get("target_protocol")
        or selection_config.get("target_protocol_anchor")
        or selection_config.get("target_keyword")
    )
    adjustment_map = _build_chunk_adjustment_map(chunks, target_protocol)

    understanding_quota = 0
    conversion_quota = 0
    if "protocol_understanding" in task_types and "protocol_conversion" in task_types:
        conversion_quota = max(1, top_k // 3)
        understanding_quota = max(1, top_k - conversion_quota)
    elif "protocol_conversion" in task_types:
        conversion_quota = top_k
    else:
        understanding_quota = top_k

    by_understanding = sorted(
        chunks,
        key=lambda chunk: _score_chunk_for_understanding(chunk, target_protocol=target_protocol)
        + adjustment_map.get(chunk.chunk_id, {}).get("selection_bonus", 0.0)
        - adjustment_map.get(chunk.chunk_id, {}).get("noise_penalty", 0.0),
        reverse=True,
    )
    by_conversion = sorted(
        chunks,
        key=lambda chunk: _score_chunk_for_conversion(chunk, target_protocol=target_protocol)
        + adjustment_map.get(chunk.chunk_id, {}).get("selection_bonus", 0.0)
        - adjustment_map.get(chunk.chunk_id, {}).get("noise_penalty", 0.0),
        reverse=True,
    )
    by_overall = sorted(
        chunks,
        key=lambda chunk: _chunk_quality_score(chunk)
        + adjustment_map.get(chunk.chunk_id, {}).get("selection_bonus", 0.0)
        - adjustment_map.get(chunk.chunk_id, {}).get("noise_penalty", 0.0)
        + (8.0 if _chunk_matches_target_protocol(chunk, target_protocol) else 0.0),
        reverse=True,
    )

    selected_ids: List[str] = []
    selected_set: Set[str] = set()

    if understanding_quota > 0:
        for chunk in by_understanding:
            if chunk.chunk_id in selected_set:
                continue
            selected_ids.append(chunk.chunk_id)
            selected_set.add(chunk.chunk_id)
            if len(selected_ids) >= understanding_quota:
                break

    if conversion_quota > 0:
        conversion_added = 0
        for chunk in by_conversion:
            if chunk.chunk_id in selected_set:
                continue
            selected_ids.append(chunk.chunk_id)
            selected_set.add(chunk.chunk_id)
            conversion_added += 1
            if conversion_added >= conversion_quota:
                break

    for chunk in by_overall:
        if len(selected_ids) >= top_k:
            break
        if chunk.chunk_id in selected_set:
            continue
        selected_ids.append(chunk.chunk_id)
        selected_set.add(chunk.chunk_id)

    chunk_by_id = {chunk.chunk_id: chunk for chunk in chunks}
    selected_chunks = [chunk_by_id[chunk_id] for chunk_id in selected_ids if chunk_id in chunk_by_id]

    if enable_llm_rerank and selected_chunks:
        llm_ids = llm_rerank_chunk_ids(
            selected_chunks,
            task_types=task_types,
            selection_limit=top_k,
            target_protocol=target_protocol,
        )
        if llm_ids:
            selected_chunks = [chunk_by_id[chunk_id] for chunk_id in llm_ids if chunk_id in chunk_by_id]

    return selected_chunks[:top_k]


def get_chunks_by_ids(chunk_ids: List[str]) -> List[Chunk]:
    """根据chunk_id列表获取语义块"""
    if not chunk_ids:
        return []
    with db_client.connection() as conn:
        cursor = conn.cursor()
        placeholder = "?" if db_client.is_sqlite else "%s"
        placeholders = ",".join([placeholder] * len(chunk_ids))
        query = f"SELECT * FROM chunks WHERE chunk_id IN ({placeholders})"
        cursor.execute(query, tuple(chunk_ids))
        rows = cursor.fetchall()
        row_chunks = [
            Chunk(
                chunk_id=row["chunk_id"],
                project_id=row["project_id"],
                dataset_id=row["dataset_id"],
                source_block_ids=json.loads(row["source_block_ids"]) if row["source_block_ids"] else [],
                semantic_type=row["semantic_type"],
                content_snapshot=row["content_snapshot"],
                metadata=json.loads(row["metadata"]) if row["metadata"] else {},
                created_at=row["created_at"],
            )
            for row in rows
        ]
        chunk_map = {chunk.chunk_id: chunk for chunk in row_chunks}
        return [chunk_map[chunk_id] for chunk_id in chunk_ids if chunk_id in chunk_map]


def get_source_content(source_ids: List[str]) -> tuple:
    """
    根据source_ids获取源数据内容
    source_ids可以是chunk_id（如"chk_001"）或block_id（如"101"）

    返回: (内容文本, 来源block_id列表, dataset_id)
    """
    all_content = []
    all_block_ids = []
    dataset_id = None

    # 分类处理：区分chunk_id和block_id
    chunk_ids = []
    block_ids = []

    for sid in source_ids:
        if isinstance(sid, str) and sid.startswith("chk_"):
            chunk_ids.append(sid)
        else:
            # 尝试作为block_id处理
            try:
                block_ids.append(int(sid))
            except (ValueError, TypeError):
                chunk_ids.append(str(sid))

    chunk_block_ids: List[int] = []

    # 获取chunks
    if chunk_ids:
        chunks = get_chunks_by_ids(chunk_ids)
        for chunk in chunks:
            chunk_block_ids.extend(chunk.source_block_ids or [])
        chunk_block_ids = [bid for bid in chunk_block_ids if isinstance(bid, int)]

        block_map: Dict[int, Block] = {}
        if chunk_block_ids:
            try:
                chunk_blocks = db_client.get_blocks_by_ids(chunk_block_ids)
                block_map = {block.block_id: block for block in chunk_blocks}
            except Exception:
                block_map = {}

        for chunk in chunks:
            if dataset_id is None:
                dataset_id = chunk.dataset_id
            chunk_text_parts: List[str] = []
            for bid in chunk.source_block_ids or []:
                if bid in block_map:
                    block = block_map[bid]
                    chunk_text_parts.append((block.cleaned_content or block.content or "").strip())
            chunk_text = "\n".join([p for p in chunk_text_parts if p])
            if not chunk_text:
                chunk_text = chunk.content_snapshot or ""
            all_content.append(chunk_text)
            all_block_ids.extend(chunk.source_block_ids)

    # 获取blocks
    if block_ids:
        blocks = db_client.get_blocks_by_ids(block_ids)
        for block in blocks:
            content = block.cleaned_content or block.content
            all_content.append(content)
            all_block_ids.append(block.block_id)

    # 去重block_ids
    seen = set()
    unique_block_ids = []
    for bid in all_block_ids:
        if bid not in seen:
            seen.add(bid)
            unique_block_ids.append(bid)

    dedup_contents = []
    seen_content = set()
    for text in all_content:
        normalized = str(text or "").strip()
        if not normalized or normalized in seen_content:
            continue
        seen_content.add(normalized)
        dedup_contents.append(normalized)

    combined_content = "\n\n".join(dedup_contents)
    return combined_content, unique_block_ids, dataset_id


def generate_qa_pairs(
    content: str,
    count: int,
    system_prompt: str = None,
    user_instruction: str = None,
    task_spec: str = None,
) -> List[Dict[str, Any]]:
    """调用LLM生成QA对"""
    llm = get_llm_client()

    # 使用模板格式化prompt
    system, user = PromptTemplates.format_qa_generate(
        content=content,
        count=count,
        system_prompt=system_prompt,
        user_instruction=user_instruction,
        task_spec=task_spec,
    )

    # 调用LLM生成
    response = llm.generate(
        prompt=user,
        system_prompt=system,
        max_new_tokens=2048,
        temperature=0.7,
    )
    qa_pairs = parse_qa_response(response)
    if qa_pairs:
        return qa_pairs

    # 真实模型偶发输出非JSON时，进行一次低温重试提升稳定性
    for _ in range(QA_GENERATION_RETRY):
        retry_user = (
            f"{user}\n\n"
            "重试要求：只返回JSON数组，每个元素必须包含question和answer字段，不要任何解释文字。"
        )
        response = llm.generate(
            prompt=retry_user,
            system_prompt=system,
            max_new_tokens=2048,
            temperature=0.2,
        )
        qa_pairs = parse_qa_response(response)
        if qa_pairs:
            break
    return qa_pairs


def parse_qa_response(response: str) -> List[Dict[str, Any]]:
    """解析LLM响应中的QA对"""
    qa_pairs: List[Dict[str, Any]] = []
    cleaned_response = LocalLLM._sanitize_response_text(response)

    parsed = LocalLLM.parse_json_from_response(cleaned_response, prefer=list)
    if isinstance(parsed, list):
        qa_pairs = parsed
    elif isinstance(parsed, dict):
        for key in ("qa_pairs", "data", "items"):
            maybe_list = parsed.get(key)
            if isinstance(maybe_list, list):
                qa_pairs = maybe_list
                break

    # 验证和清理QA对格式
    valid_pairs = []
    for qa in qa_pairs:
        if isinstance(qa, dict) and "question" in qa and "answer" in qa:
            qa_task_type = normalize_task_type(qa.get("qa_task_type") or qa.get("task_type"))
            conversion_mode = normalize_conversion_mode(qa.get("conversion_mode"))
            conversion_formula = qa.get("conversion_formula")
            answer = str(qa.get("answer", "")).strip()

            if qa_task_type == "protocol_conversion":
                conversion_payload = normalize_conversion_payload(
                    answer=answer,
                    conversion_formula=conversion_formula,
                    conversion_mode=conversion_mode,
                )
                answer = conversion_payload["answer"]
                conversion_formula = conversion_payload["conversion_formula"]
                conversion_mode = conversion_payload["conversion_mode"]

            valid_pairs.append({
                "question": str(qa.get("question", "")).strip(),
                "answer": answer,
                "qa_task_type": qa_task_type,
                "conversion_mode": conversion_mode,
                "conversion_formula": conversion_formula,
                "source_fields": normalize_source_fields_value(qa.get("source_fields"), fallback=qa.get("source_field")),
                "concept_name": str(qa.get("concept_name") or qa.get("concept") or "").strip() or None,
                "formula_kind": str(qa.get("formula_kind") or "").strip() or None,
                "target_protocol_type": str(qa.get("target_protocol_type") or "").strip() or None,
                "target_message_code": str(qa.get("target_message_code") or "").strip().upper() or None,
                "target_field": str(qa.get("target_field") or "").strip() or None,
                "source_field": str(qa.get("source_field") or "").strip() or None,
                "extracted_info": qa.get("extracted_info") if isinstance(qa.get("extracted_info"), dict) else None,
            })

    return valid_pairs


def check_quality(question: str, answer: str, qa_task_type: str = "protocol_understanding") -> tuple:
    """检查QA对质量"""
    if USE_LLM_QUALITY_CHECK:
        llm = get_llm_client()

        # 使用模板格式化prompt
        system, user = PromptTemplates.format_quality_check(question, answer)

        # 调用LLM检测质量
        result = llm.extract_json(user, system_prompt=system)

        if result:
            is_low_quality = result.get("is_low_quality", False)
            reason = result.get("reason", "")
            return is_low_quality, reason

    # 基于规则的简单质量检查
    is_low_quality = False
    reasons = []

    # 问题过短
    if len(question.strip()) < 8:
        is_low_quality = True
        reasons.append("问题过短")

    if qa_task_type == "protocol_conversion":
        formula = extract_formula_only(answer)
        if not formula or len(formula) < 3:
            is_low_quality = True
            reasons.append("转换公式无效")
        mode = infer_conversion_mode(formula)
        if mode == "transcoding" and not contains_arithmetic_expression(formula):
            is_low_quality = True
            reasons.append("transcoding缺少可计算公式")
        if mode == "mapping" and not (re.search(r"(?:=|->|→)", formula) or looks_like_block_formula(formula)):
            is_low_quality = True
            reasons.append("mapping缺少映射关系")
        return is_low_quality, "; ".join(reasons) if reasons else "质量合格"

    answer_text = answer.strip()
    if len(answer_text) < 6:
        is_low_quality = True
        reasons.append("答案过短")

    has_numeric = bool(re.search(r"\d", answer_text))
    has_mapping = bool(re.search(r"-?\d+\s*(?:=|->|→)\s*[A-Za-z_]", answer_text))
    if not (has_numeric or has_mapping):
        is_low_quality = True
        reasons.append("答案缺乏具体数值")

    # 包含模糊表述
    vague_terms = ["可能", "也许", "大概", "不确定", "不清楚"]
    if any(term in answer for term in vague_terms):
        reasons.append("答案包含模糊表述")

    return is_low_quality, "; ".join(reasons) if reasons else "质量合格"


def save_qa_pairs(
    qa_pairs: List[Dict[str, Any]],
    source_block_ids: List[int],
    dataset_id: str = None,
    instruction: str = ""
) -> List[QAPair]:
    """保存QA对到数据库和文件存储"""
    saved_pairs = []
    timestamp = int(time.time())

    for i, qa in enumerate(qa_pairs):
        # 生成QA ID
        qa_id = f"qa_{timestamp}_{i}_{uuid.uuid4().hex[:6]}"

        # 质量检查
        is_low_quality, quality_reason = check_quality(
            qa["question"],
            qa["answer"],
            qa.get("qa_task_type", "protocol_understanding"),
        )

        # 创建QAPair对象
        extracted_info = qa.get("extracted_info") if isinstance(qa.get("extracted_info"), dict) else {}
        if qa.get("source_field") and not extracted_info.get("source_field"):
            extracted_info["source_field"] = qa.get("source_field")
        if qa.get("source_fields") and not extracted_info.get("source_fields"):
            extracted_info["source_fields"] = qa.get("source_fields")
        if qa.get("target_field") and not extracted_info.get("target_field"):
            extracted_info["target_field"] = qa.get("target_field")
        if qa.get("concept_name") and not extracted_info.get("concept_name"):
            extracted_info["concept_name"] = qa.get("concept_name")
        if qa.get("formula_kind") and not extracted_info.get("formula_kind"):
            extracted_info["formula_kind"] = qa.get("formula_kind")

        qa_pair = QAPair(
            qa_id=qa_id,
            source_block_ids=[str(bid) for bid in source_block_ids],
            question=qa["question"],
            answer=qa["answer"],
            qa_task_type=qa.get("qa_task_type", "protocol_understanding"),
            conversion_mode=qa.get("conversion_mode"),
            conversion_formula=qa.get("conversion_formula"),
            source_field=qa.get("source_field"),
            source_fields=qa.get("source_fields") or normalize_source_fields_value(qa.get("source_field")),
            target_field=qa.get("target_field"),
            concept_name=qa.get("concept_name"),
            formula_kind=qa.get("formula_kind") or ("python_block" if looks_like_block_formula(qa.get("conversion_formula") or qa.get("answer")) else None),
            target_protocol_type=qa.get("target_protocol_type"),
            target_message_code=qa.get("target_message_code"),
            instruction=instruction,
            is_low_quality=is_low_quality,
            quality_reason=quality_reason if is_low_quality else None,
            extracted_info=extracted_info or None,
            protocol_type="Link16",
        )

        # 保存到数据库
        try:
            db_client.insert_qa(qa_pair)
            saved_pairs.append(qa_pair)
        except Exception as e:
            print(f"保存QA对失败: {e}")
            continue

    # 保存到文件存储
    if dataset_id and saved_pairs:
        try:
            file_store.save_qa_pairs(dataset_id, [qa.to_dict() for qa in saved_pairs])
        except Exception as e:
            print(f"保存QA对到文件失败: {e}")

    return saved_pairs


@app.route("/api/knowledge/generate_qa", methods=["POST"])
def generate_qa():
    """
    QA对生成接口

    输入参数:
    {
        "source_block_ids": ["chk_001", "chk_002"],
        "task_config": {
            "task_types": ["协议理解类", "协议转换类"],
            "conversion_modes": ["转义", "转换"]
        },
        "prompt_config": {
            "system_prompt": "你是一个协议专家...",
            "user_instruction": "请重点生成关于字段物理含义的问答"
        },
        "count": 5
    }

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "task_id": "gen_888",
            "qa_pairs": [
                {
                    "qa_id": "qa_xxx",
                    "question": "...",
                    "answer": "...",
                    "is_low_quality": false,
                    "source_block_ids": ["101", "102"]
                }
            ]
        }
    }
    """
    data = request.json
    if not data:
        return jsonify({
            "code": 400,
            "message": "请求体不能为空",
            "data": None
        }), 400

    # 参数验证
    source_block_ids = data.get("source_block_ids", [])
    if isinstance(source_block_ids, (str, int)):
        source_block_ids = [source_block_ids]
    elif not isinstance(source_block_ids, list):
        source_block_ids = []
    dataset_id = data.get("dataset_id")
    selection_config = data.get("selection_config", {}) or {}
    if not isinstance(selection_config, dict):
        selection_config = {}

    prompt_config = data.get("prompt_config", {})
    task_config = data.get("task_config", {})
    count = data.get("count", 5)

    # 限制数量
    count = min(max(1, count), 20)

    raw_task_types = task_config.get("task_types", ["protocol_understanding", "protocol_conversion"])
    task_types = []
    for t in raw_task_types:
        normalized = normalize_task_type(t)
        if normalized not in task_types:
            task_types.append(normalized)
    if not task_types:
        task_types = ["protocol_understanding", "protocol_conversion"]

    raw_conversion_modes = task_config.get("conversion_modes", ["transcoding", "mapping"])
    conversion_modes = []
    for m in raw_conversion_modes:
        normalized_mode = normalize_conversion_mode(m)
        if normalized_mode and normalized_mode not in conversion_modes:
            conversion_modes.append(normalized_mode)
    if not conversion_modes:
        conversion_modes = ["transcoding", "mapping"]

    task_spec = build_task_spec(task_types, conversion_modes, count)
    auto_select = bool(selection_config.get("auto_select", False)) or (not source_block_ids and bool(dataset_id))

    if not source_block_ids and not dataset_id:
        return jsonify({
            "code": 400,
            "message": "source_block_ids不能为空",
            "data": None
        }), 400

    try:
        selected_chunk_ids: List[str] = []
        selection_mode = "manual"

        if auto_select:
            selection_mode = "auto"
            if not dataset_id:
                return jsonify({
                    "code": 400,
                    "message": "自动选块模式缺少dataset_id",
                    "data": None
                }), 400
            dataset_chunks = db_client.get_chunks_by_dataset(dataset_id)
            if not dataset_chunks:
                return jsonify({
                    "code": 404,
                    "message": f"未找到dataset_id={dataset_id}的语义块",
                    "data": None
                }), 404
            selected_chunks = select_chunks_for_qa(
                chunks=dataset_chunks,
                task_types=task_types,
                count=count,
                selection_config=selection_config,
            )
            if not selected_chunks:
                return jsonify({
                    "code": 400,
                    "message": "自动选块失败，未找到满足条件的chunk",
                    "data": None
                }), 400
            selected_chunk_ids = [chunk.chunk_id for chunk in selected_chunks]
            source_block_ids = selected_chunk_ids

        # 1. 获取源数据内容
        content, block_ids, resolved_dataset_id = get_source_content(source_block_ids)
        if resolved_dataset_id:
            dataset_id = resolved_dataset_id

        if not content:
            return jsonify({
                "code": 400,
                "message": "无法获取源数据内容",
                "data": None
            }), 400

        # 2. 构建prompt并调用LLM生成QA对
        system_prompt = prompt_config.get("system_prompt")
        user_instruction = prompt_config.get("user_instruction")

        qa_pairs_raw = generate_qa_pairs(
            content=content,
            count=count,
            system_prompt=system_prompt,
            user_instruction=user_instruction,
            task_spec=task_spec,
        )

        if not qa_pairs_raw:
            return jsonify({
                "code": 500,
                "message": "LLM未能生成有效的QA对",
                "data": None
            }), 500

        field_context = build_field_context(content)

        # 规范化任务类型和转换模式
        for qa in qa_pairs_raw:
            qa["qa_task_type"] = qa.get("qa_task_type", "protocol_understanding")
            if qa["qa_task_type"] not in task_types:
                qa["qa_task_type"] = task_types[0]

            if qa["qa_task_type"] == "protocol_conversion":
                conversion_payload = normalize_conversion_payload(
                    answer=qa.get("answer", ""),
                    conversion_formula=qa.get("conversion_formula"),
                    conversion_mode=qa.get("conversion_mode"),
                )
                conversion_payload = enrich_conversion_payload_with_context(
                    question=qa.get("question", ""),
                    payload=conversion_payload,
                    field_context=field_context,
                )
                qa["answer"] = conversion_payload["answer"]
                qa["conversion_formula"] = conversion_payload["conversion_formula"]
                qa["conversion_mode"] = conversion_payload["conversion_mode"]
                qa["target_field"] = conversion_payload.get("target_field")
                qa["source_field"] = conversion_payload.get("source_field")
                if qa.get("target_field") or qa.get("source_field"):
                    qa["extracted_info"] = {
                        "source_field": qa.get("source_field"),
                        "target_field": qa.get("target_field"),
                    }
                if qa["conversion_mode"] not in conversion_modes:
                    qa["conversion_mode"] = conversion_modes[0]
            else:
                qa["answer"] = enhance_understanding_answer(
                    question=qa.get("question", ""),
                    answer=qa.get("answer", ""),
                    field_context=field_context,
                )
                qa["conversion_mode"] = None
                qa["conversion_formula"] = None

        # 3. 保存QA对到数据库和文件存储
        instruction = prompt_config.get("system_prompt", "")
        saved_pairs = save_qa_pairs(
            qa_pairs=qa_pairs_raw,
            source_block_ids=block_ids,
            dataset_id=dataset_id,
            instruction=instruction,
        )

        # 4. 构建响应
        task_id = f"gen_{int(time.time())}"
        qa_pairs_response = []
        for qa in saved_pairs:
            primary_source_id = None
            if qa.source_block_ids:
                first_source = qa.source_block_ids[0]
                try:
                    primary_source_id = int(first_source)
                except (TypeError, ValueError):
                    primary_source_id = first_source

            qa_pairs_response.append(
                {
                    "qa_id": qa.qa_id,
                    "insturctor": qa.instruction,
                    "question": qa.question,
                    "answer": qa.answer,
                    "qa_task_type": qa.qa_task_type,
                    "conversion_mode": qa.conversion_mode,
                    "conversion_formula": qa.conversion_formula,
                    "source_field": (qa.extracted_info or {}).get("source_field") if qa.extracted_info else None,
                    "target_field": (qa.extracted_info or {}).get("target_field") if qa.extracted_info else None,
                    "is_low_quality": qa.is_low_quality,
                    "reason": qa.quality_reason,
                    "source_block_id": primary_source_id,
                    # 兼容字段（保留）
                    "quality_reason": qa.quality_reason,
                    "source_block_ids": qa.source_block_ids,
                }
            )

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "task_id": task_id,
                "total_count": len(saved_pairs),
                "high_quality_count": sum(1 for qa in saved_pairs if not qa.is_low_quality),
                "low_quality_count": sum(1 for qa in saved_pairs if qa.is_low_quality),
                "selection_mode": selection_mode,
                "selected_chunk_ids": selected_chunk_ids,
                "qa_pairs": qa_pairs_response,
            }
        })

    except Exception as e:
        return jsonify({
            "code": 500,
            "message": f"生成QA对失败: {str(e)}",
            "data": None
        }), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5004, debug=True)
