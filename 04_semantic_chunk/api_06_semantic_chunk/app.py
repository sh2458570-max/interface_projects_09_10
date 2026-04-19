# 接口6: 语义单元智能划分与重组
# POST /api/data/semantic_chunk

import os
import sys
import json
import uuid
import time
import logging
import re
import inspect
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from flask import Flask, request, jsonify

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database.mysql_client import MySQLClient
from shared.database.milvus_client import MilvusClient
from shared.database.models import Block, Chunk
from shared.config import EMBED_MODEL_NAME
from shared.llm.local_llm import LocalLLM, get_llm
from shared.protocol_conversion import build_protocol_doc_index

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 初始化客户端
mysql_client = MySQLClient()
milvus_client = MilvusClient()
llm_client: Optional[LocalLLM] = None
embedding_model = None
resolved_embedding_model_name: Optional[str] = None
resolved_embedding_device: Optional[str] = None

# 集合名称
SEMANTIC_COLLECTION_NAME = "semantic_chunks"
EMBEDDING_DIM = 1024
CANONICAL_EMBED_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"
EMBED_MODEL_ALIASES = {
    "qwen3-0.6b-embedding": CANONICAL_EMBED_MODEL_NAME,
    "qwen/qwen3-embedding-0.6b": CANONICAL_EMBED_MODEL_NAME,
    "qwen3-embedding-0.6b": CANONICAL_EMBED_MODEL_NAME,
}
DEFAULT_LOCAL_EMBED_MODEL = "/home/hks/sxy/model_cache/Qwen/Qwen3-Embedding-0___6B"
PROTOCOL_ANCHOR_PATTERN = re.compile(r"\b(J\d+\.\d+[A-Z]?\d*)\b", flags=re.IGNORECASE)
FIELD_NAME_PATTERN = re.compile(r"\b[A-Z][A-Z0-9_./\-]{2,}\b")
MAPPING_PAIR_PATTERN = re.compile(r"-?\d+\s*(?:=|->|→)\s*[A-Za-z_][A-Za-z0-9_./\-]*")
FORMULA_HINT_PATTERN = re.compile(r"(?:formula|公式|convert|转换|resolution|分辨率|value\s*[\*\/\+\-])", flags=re.IGNORECASE)
RULE_EVIDENCE_PATTERN = re.compile(
    r"(?:formula|公式|mapping|映射|range|范围|resolution|分辨率|bit|位|value\s*[\*\/\+\-]|->|→|=)",
    flags=re.IGNORECASE,
)
HEADER_FOOTER_PATTERN = re.compile(
    r"(?:^table\s+\d|^figure\s+\d|^note\b|^附注|continued|page\s+\d+|保密|密级|页眉|页脚)",
    flags=re.IGNORECASE,
)
SEPARATOR_LINE_PATTERN = re.compile(r"^[=\-_.|/\\\s]{4,}$")


def get_llm_client() -> LocalLLM:
    """获取LLM客户端（延迟初始化）"""
    global llm_client
    if llm_client is None:
        llm_client = get_llm()
    return llm_client


def resolve_embedding_model_name() -> str:
    """解析可用的本地/远端 embedding 模型标识。"""
    explicit_path = os.getenv("EMBED_MODEL_PATH")
    env_name = os.getenv("EMBED_MODEL_NAME")

    candidates = []
    for candidate in [explicit_path, env_name, EMBED_MODEL_NAME, "qwen3-0.6b-embedding"]:
        if not candidate:
            continue
        raw = str(candidate).strip()
        alias_key = raw.lower()
        normalized_candidate = EMBED_MODEL_ALIASES.get(alias_key, raw)
        for value in [raw, normalized_candidate]:
            if value and value not in candidates:
                candidates.append(value)

    for candidate in candidates:
        normalized = os.path.expanduser(candidate)
        if os.path.exists(normalized):
            return normalized
        if candidate == CANONICAL_EMBED_MODEL_NAME and os.path.exists(DEFAULT_LOCAL_EMBED_MODEL):
            return DEFAULT_LOCAL_EMBED_MODEL

    if os.path.exists(DEFAULT_LOCAL_EMBED_MODEL):
        return DEFAULT_LOCAL_EMBED_MODEL

    return CANONICAL_EMBED_MODEL_NAME


def estimate_tokens(text: str) -> int:
    """
    估算文本的token数量
    简单估算：中文约1.5字符/token，英文约4字符/token
    """
    if not text:
        return 0
    # 粗略估算：平均3字符/token
    return len(text) // 3


def get_block_content(block: Block) -> str:
    """获取块的清洗后内容，如果没有则使用原始内容"""
    content = block.cleaned_content or block.content
    return content.strip() if content else ""


def merge_block_contents(blocks: List[Block]) -> Tuple[str, int]:
    """
    合并多个块的内容
    返回: (合并后的内容, 总token数)
    """
    contents = []
    total_tokens = 0
    for block in blocks:
        content = get_block_content(block)
        if content:
            contents.append(content)
            total_tokens += estimate_tokens(content)
    return "\n\n".join(contents), total_tokens


def extract_protocol_anchor(content: str, metadata: Dict[str, Any]) -> str:
    """提取协议锚点（如 J12.0C3），用于跨块聚合。"""
    if isinstance(metadata, dict):
        for key in ("protocol", "word_number"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                match = PROTOCOL_ANCHOR_PATTERN.search(value.strip())
                if match:
                    return match.group(1).upper()
    match = PROTOCOL_ANCHOR_PATTERN.search(content or "")
    return match.group(1).upper() if match else ""


def _is_plausible_field_name(token: str) -> bool:
    cleaned = str(token or "").strip().upper()
    if len(cleaned) < 3:
        return False
    if cleaned in {"FIELD", "FIELDS", "VALUE", "VALUES", "TABLE", "TABLES"}:
        return False
    if "_" in cleaned:
        return True
    if PROTOCOL_ANCHOR_PATTERN.fullmatch(cleaned):
        return True
    return bool(re.fullmatch(r"[A-Z0-9./\-]{4,}", cleaned))


def extract_field_names(content: str, metadata: Dict[str, Any]) -> Set[str]:
    """从块内容和协议提取元数据中抽取字段名集合。"""
    field_names: Set[str] = set()
    protocol_fields = metadata.get("protocol_fields") if isinstance(metadata, dict) else None
    if isinstance(protocol_fields, list):
        for field in protocol_fields:
            if not isinstance(field, dict):
                continue
            raw_name = str(field.get("field_name", "")).strip().upper()
            if _is_plausible_field_name(raw_name):
                field_names.add(raw_name)
    for token in FIELD_NAME_PATTERN.findall((content or "").upper()):
        if _is_plausible_field_name(token):
            field_names.add(token)
    return field_names


def count_mapping_pairs(text: str) -> int:
    if not text:
        return 0
    return len(MAPPING_PAIR_PATTERN.findall(text))


def _iter_nonempty_lines(text: str) -> List[str]:
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def estimate_noise_penalty(content: str, block_type: str, metadata: Dict[str, Any]) -> float:
    """Estimate how noisy one block looks for downstream rule extraction."""
    text = str(content or "").strip()
    if not text:
        return 18.0

    lines = _iter_nonempty_lines(text)
    line_count = len(lines) or 1
    short_lines = sum(1 for line in lines if len(line) <= 6)
    separator_lines = sum(1 for line in lines if SEPARATOR_LINE_PATTERN.fullmatch(line))
    header_footer_hits = sum(1 for line in lines if HEADER_FOOTER_PATTERN.search(line))
    punctuation_chars = sum(1 for ch in text if not ch.isalnum() and not ch.isspace())
    punctuation_ratio = punctuation_chars / max(len(text), 1)
    alpha_numeric_ratio = sum(1 for ch in text if ch.isalnum()) / max(len(text), 1)

    penalty = 0.0
    if block_type in {"image", "figure"}:
        penalty += 6.0
    if line_count >= 4 and short_lines / line_count >= 0.55:
        penalty += 5.0
    if separator_lines:
        penalty += min(4.0, separator_lines * 1.5)
    if header_footer_hits:
        penalty += min(6.0, header_footer_hits * 2.0)
    if punctuation_ratio >= 0.32:
        penalty += 3.0
    if alpha_numeric_ratio <= 0.38:
        penalty += 2.0
    if len(text) < 36 and not RULE_EVIDENCE_PATTERN.search(text):
        penalty += 4.0
    if metadata.get("protocol_fields") in (None, []) and count_mapping_pairs(text) == 0 and not FORMULA_HINT_PATTERN.search(text):
        penalty += 2.0

    return round(penalty, 4)


def estimate_evidence_score(
    content: str,
    block_type: str,
    protocol_anchor: str,
    field_names: Set[str],
    formula_count: int,
    mapping_pair_count: int,
    bit_coverage_count: int,
    range_coverage_count: int,
) -> float:
    """Estimate how much protocol conversion evidence one block contains."""
    score = 0.0
    if protocol_anchor:
        score += 12.0
    score += min(18.0, len(field_names) * 1.8)
    score += min(16.0, formula_count * 3.0)
    score += min(16.0, mapping_pair_count * 2.0)
    score += min(10.0, (bit_coverage_count + range_coverage_count) * 1.5)
    if RULE_EVIDENCE_PATTERN.search(content):
        score += 6.0
    if block_type == "table" and (mapping_pair_count > 0 or formula_count > 0):
        score += 4.0
    return round(score, 4)


def collect_block_features(block: Block) -> Dict[str, Any]:
    """聚合单个块的结构化特征，用于规则分块和质量评分。"""
    content = get_block_content(block)
    metadata = block.metadata if isinstance(block.metadata, dict) else {}
    protocol_fields = metadata.get("protocol_fields") if isinstance(metadata.get("protocol_fields"), list) else []

    formula_count = 0
    bit_coverage_count = 0
    range_coverage_count = 0
    mapping_pair_count = count_mapping_pairs(content)

    for field in protocol_fields:
        if not isinstance(field, dict):
            continue
        formula_text = str(field.get("formula") or "").strip()
        meaning_text = str(field.get("meaning") or "").strip()
        if formula_text:
            formula_count += 1
        if field.get("bit_start") is not None or field.get("bit_length") is not None:
            bit_coverage_count += 1
        if field.get("range_min") is not None or field.get("range_max") is not None:
            range_coverage_count += 1
        mapping_pair_count += count_mapping_pairs(formula_text) + count_mapping_pairs(meaning_text)

    content_has_formula = bool(FORMULA_HINT_PATTERN.search(content)) or formula_count > 0
    semantic_hint = determine_semantic_type([block])
    protocol_anchor = extract_protocol_anchor(content, metadata)
    field_names = extract_field_names(content, metadata)
    noise_penalty = estimate_noise_penalty(content, str(getattr(block, "block_type", "") or "text").lower(), metadata)
    evidence_score = estimate_evidence_score(
        content=content,
        block_type=str(getattr(block, "block_type", "") or "text").lower(),
        protocol_anchor=protocol_anchor,
        field_names=field_names,
        formula_count=formula_count + (1 if content_has_formula else 0),
        mapping_pair_count=mapping_pair_count,
        bit_coverage_count=bit_coverage_count,
        range_coverage_count=range_coverage_count,
    )

    return {
        "block": block,
        "block_id": block.block_id,
        "page_num": int(getattr(block, "page_num", 0) or 0),
        "block_type": str(getattr(block, "block_type", "") or "text").lower(),
        "content": content,
        "token_count": estimate_tokens(content),
        "protocol_anchor": protocol_anchor,
        "field_names": field_names,
        "formula_count": formula_count + (1 if content_has_formula else 0),
        "mapping_pair_count": mapping_pair_count,
        "bit_coverage_count": bit_coverage_count,
        "range_coverage_count": range_coverage_count,
        "semantic_hint": semantic_hint,
        "noise_penalty": noise_penalty,
        "evidence_score": evidence_score,
    }


def _score_merge_candidate(
    current_group: List[Dict[str, Any]],
    next_feature: Dict[str, Any],
    max_token_size: int,
) -> Tuple[int, List[str], int]:
    """评估下一个块是否应并入当前组。"""
    prev_feature = current_group[-1]
    reasons: List[str] = []
    score = 0

    projected_tokens = sum(item["token_count"] for item in current_group) + next_feature["token_count"]
    if projected_tokens > int(max_token_size * 1.35):
        return -99, ["超过最大token预算"], projected_tokens
    if projected_tokens > max_token_size:
        score -= 2
        reasons.append("接近token上限")

    prev_anchor = prev_feature["protocol_anchor"]
    next_anchor = next_feature["protocol_anchor"]
    if prev_anchor and next_anchor:
        if prev_anchor == next_anchor:
            score += 4
            reasons.append("同协议锚点")
        else:
            score -= 3
            reasons.append("协议锚点变化")

    shared_fields = prev_feature["field_names"] & next_feature["field_names"]
    if shared_fields:
        score += 3
        reasons.append("字段重叠")

    page_gap = max(0, next_feature["page_num"] - prev_feature["page_num"])
    if page_gap <= 1:
        score += 1
        reasons.append("页码连续")
    elif page_gap >= 3:
        score -= 2
        reasons.append("页码跨度大")

    if prev_feature["block_type"] == next_feature["block_type"]:
        score += 1
        reasons.append("块类型一致")
    elif {"table", "code"} & {prev_feature["block_type"], next_feature["block_type"]}:
        score -= 1
        reasons.append("结构类型差异")

    if prev_feature["semantic_hint"] == next_feature["semantic_hint"]:
        score += 1
        reasons.append("语义类型一致")

    if prev_feature["formula_count"] > 0 and next_feature["formula_count"] > 0:
        score += 1
        reasons.append("转换信息连续")

    if prev_feature.get("evidence_score", 0.0) >= 18.0 and next_feature.get("evidence_score", 0.0) >= 18.0:
        score += 1
        reasons.append("规则证据密集")

    next_noise = float(next_feature.get("noise_penalty", 0.0) or 0.0)
    next_evidence = float(next_feature.get("evidence_score", 0.0) or 0.0)
    if next_noise >= 8.0 and next_evidence < 12.0:
        score -= 2
        reasons.append("候选块噪声偏高")

    return score, reasons, projected_tokens


def llm_should_merge_blocks(
    current_group: List[Dict[str, Any]],
    next_feature: Dict[str, Any],
    max_token_size: int,
) -> Tuple[bool, str]:
    """
    在规则分数不确定时，使用LLM做一次边界判断。
    仅返回是否合并和简短原因。
    """
    try:
        llm = get_llm_client()
        prev = current_group[-1]
        payload = {
            "max_token_size": max_token_size,
            "current_group": {
                "block_ids": [item["block_id"] for item in current_group],
                "protocol_anchor": prev.get("protocol_anchor", ""),
                "semantic_hint": prev.get("semantic_hint", ""),
                "token_estimate": sum(item["token_count"] for item in current_group),
                "content_preview": (prev.get("content") or "")[:420],
            },
            "candidate_block": {
                "block_id": next_feature.get("block_id"),
                "protocol_anchor": next_feature.get("protocol_anchor", ""),
                "semantic_hint": next_feature.get("semantic_hint", ""),
                "token_estimate": next_feature.get("token_count", 0),
                "content_preview": (next_feature.get("content") or "")[:420],
            },
        }
        system_prompt = (
            "你是协议文档分块助手。"
            "判断candidate_block是否应与current_group合并。"
            "只输出JSON: {\"merge\": true/false, \"reason\": \"<=20字\"}"
        )
        user_prompt = json.dumps(payload, ensure_ascii=False)
        result = llm.extract_json(user_prompt, system_prompt=system_prompt)
        if isinstance(result, dict) and "merge" in result:
            return bool(result.get("merge")), str(result.get("reason", "")).strip()
    except Exception as exc:
        logger.warning("LLM边界判断失败，回退规则: %s", exc)
    return False, ""


def determine_group_semantic_type(features: List[Dict[str, Any]]) -> str:
    """根据组内统计特征确定chunk语义类型。"""
    if not features:
        return "general_content"
    table_count = sum(1 for item in features if item["block_type"] == "table")
    mapping_count = sum(item["mapping_pair_count"] for item in features)
    formula_count = sum(item["formula_count"] for item in features)
    field_names = set().union(*(item["field_names"] for item in features))
    bit_or_range_count = sum(item["bit_coverage_count"] + item["range_coverage_count"] for item in features)

    if mapping_count > 0:
        return "table_data" if table_count >= max(1, len(features) // 2) else "conversion_rule"
    if formula_count >= 2:
        return "conversion_rule"
    if field_names and bit_or_range_count > 0:
        return "field_definition"
    if table_count > 0:
        return "table_data"
    if any(item["protocol_anchor"] for item in features):
        return "protocol_description"
    return "general_content"


def build_chunk_metadata(features: List[Dict[str, Any]], reason: str, method: str) -> Dict[str, Any]:
    """生成chunk级质量统计，供后续QA选块使用。"""
    block_count = len(features)
    token_estimate = sum(item["token_count"] for item in features)
    field_names = sorted(set().union(*(item["field_names"] for item in features)))
    formula_count = sum(item["formula_count"] for item in features)
    mapping_pair_count = sum(item["mapping_pair_count"] for item in features)
    bit_coverage_count = sum(item["bit_coverage_count"] for item in features)
    range_coverage_count = sum(item["range_coverage_count"] for item in features)
    evidence_score = round(sum(float(item.get("evidence_score", 0.0) or 0.0) for item in features), 4)
    noise_penalty = round(sum(float(item.get("noise_penalty", 0.0) or 0.0) for item in features), 4)
    noisy_block_count = sum(1 for item in features if float(item.get("noise_penalty", 0.0) or 0.0) >= 8.0)

    protocol_anchor = ""
    for item in features:
        if item["protocol_anchor"]:
            protocol_anchor = item["protocol_anchor"]
            break

    base_quality_score = (
        min(40, len(field_names) * 3)
        + min(20, formula_count * 2)
        + min(15, mapping_pair_count)
        + min(15, bit_coverage_count + range_coverage_count)
        + (10 if protocol_anchor else 0)
    )
    quality_score = max(
        0.0,
        round(
            base_quality_score
            + min(24.0, evidence_score * 0.35)
            - min(24.0, noise_penalty * 0.8)
            - noisy_block_count * 1.5,
            4,
        ),
    )

    return {
        "protocol_anchor": protocol_anchor,
        "field_names": field_names[:24],
        "field_count": len(field_names),
        "formula_count": formula_count,
        "mapping_pair_count": mapping_pair_count,
        "bit_coverage_count": bit_coverage_count,
        "range_coverage_count": range_coverage_count,
        "token_estimate": token_estimate,
        "quality_score": quality_score,
        "evidence_score": evidence_score,
        "noise_penalty": noise_penalty,
        "noisy_block_count": noisy_block_count,
        "block_count": block_count,
        "merge_method": method,
        "reason": reason,
    }


def normalize_target_protocol(value: Any) -> str:
    return str(value or "").strip().upper()


def block_matches_target_protocol(block: Block, target_protocol: str) -> bool:
    """判断块是否与目标协议相关。"""
    target = normalize_target_protocol(target_protocol)
    if not target:
        return False

    content = get_block_content(block).upper()
    metadata = block.metadata if isinstance(block.metadata, dict) else {}
    anchor = extract_protocol_anchor(content, metadata).upper()

    major_match = re.match(r"(J\d+)", target)
    major = major_match.group(1) if major_match else target

    if target in content or target in anchor:
        return True
    if major and (major in content or anchor.startswith(major)):
        return True

    field_names = extract_field_names(content, metadata)
    for field_name in field_names:
        normalized = str(field_name).upper()
        if target in normalized or (major and major in normalized):
            return True
    return False


def filter_blocks_by_target_protocol(
    blocks: List[Block],
    target_protocol: str,
    page_window: int = 0,
) -> List[Block]:
    """
    按目标协议筛选块，并可扩展邻近页上下文。
    例如 target_protocol=J12.0 时，优先保留J12相关块。
    """
    target = normalize_target_protocol(target_protocol)
    if not target:
        return blocks

    matched_pages: Set[int] = set()
    matched_block_ids: Set[int] = set()
    for block in blocks:
        if block_matches_target_protocol(block, target):
            matched_pages.add(int(getattr(block, "page_num", 0) or 0))
            matched_block_ids.add(block.block_id)

    if not matched_pages:
        return blocks

    selected_pages: Set[int] = set()
    for page in matched_pages:
        selected_pages.add(page)
        for offset in range(1, max(0, page_window) + 1):
            selected_pages.add(page - offset)
            selected_pages.add(page + offset)

    filtered = [block for block in blocks if int(getattr(block, "page_num", 0) or 0) in selected_pages]
    logger.info(
        "按目标协议筛选块: target=%s, 原始=%d, 筛选后=%d, 覆盖页=%d",
        target,
        len(blocks),
        len(filtered),
        len(selected_pages),
    )
    return filtered if filtered else blocks


def rule_semantic_chunking(
    blocks: List[Block],
    max_token_size: int,
    use_llm_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    规则优先的语义chunk生成：
    - 先按协议锚点/字段重叠/页连续性聚合
    - 规则不确定时再用LLM做边界判定
    """
    if not blocks:
        return []

    ordered_blocks = sorted(blocks, key=lambda item: (item.page_num, item.block_id))
    features = [collect_block_features(block) for block in ordered_blocks]
    groups: List[List[Dict[str, Any]]] = []
    current_group: List[Dict[str, Any]] = [features[0]]
    llm_calls = 0
    max_llm_calls = int(os.getenv("SEMANTIC_CHUNK_MAX_LLM_BOUNDARY_CALLS", "8"))

    for next_feature in features[1:]:
        score, reasons, projected_tokens = _score_merge_candidate(current_group, next_feature, max_token_size)
        should_merge = score >= 3
        reason = ";".join(reasons) if reasons else "规则聚合"

        uncertain = 1 <= score < 3 and projected_tokens <= int(max_token_size * 1.2)
        if uncertain and use_llm_fallback and llm_calls < max_llm_calls:
            llm_merge, llm_reason = llm_should_merge_blocks(current_group, next_feature, max_token_size)
            llm_calls += 1
            if llm_merge:
                should_merge = True
                reason = f"{reason};LLM边界判定:{llm_reason or 'merge'}"

        if should_merge:
            current_group.append(next_feature)
            current_group[-1]["merge_reason"] = reason
        else:
            groups.append(current_group)
            current_group = [next_feature]

    if current_group:
        groups.append(current_group)

    chunk_suggestions: List[Dict[str, Any]] = []
    for group in groups:
        semantic_type = determine_group_semantic_type(group)
        reason = group[-1].get("merge_reason") or "规则聚合"
        chunk_suggestions.append(
            {
                "block_ids": [item["block_id"] for item in group],
                "semantic_type": semantic_type,
                "reason": reason,
                "metadata": build_chunk_metadata(group, reason=reason, method="rule+llm_boundary"),
            }
        )
    return chunk_suggestions


def analyze_semantic_relations(
    blocks: List[Block],
    max_token_size: int = 1024,
    use_llm_fallback: bool = True,
) -> List[Dict[str, Any]]:
    """
    规则优先分析块之间的语义关联性，必要时用LLM做边界兜底。

    Args:
        blocks: 文档块列表
        max_token_size: 最大token大小限制
        use_llm_fallback: 是否启用LLM边界判定兜底

    Returns:
        语义分块建议列表
    """
    if not blocks:
        return []

    # 如果只有一个块，直接返回
    if len(blocks) == 1:
        return [{
            "block_ids": [blocks[0].block_id],
            "semantic_type": "single_block",
            "reason": "单块内容",
            "metadata": build_chunk_metadata([collect_block_features(blocks[0])], reason="单块内容", method="single"),
        }]

    try:
        rule_chunks = rule_semantic_chunking(
            blocks=blocks,
            max_token_size=max_token_size,
            use_llm_fallback=use_llm_fallback,
        )
        if rule_chunks:
            return rule_chunks
    except Exception as e:
        logger.error(f"规则分块失败，回退基础策略: {e}")

    # 降级方案：按块类型和顺序简单分组
    return fallback_chunking(blocks, max_token_size)


def fallback_chunking(
    blocks: List[Block],
    max_token_size: int = 1024
) -> List[Dict[str, Any]]:
    """
    降级分块方案：按顺序和token限制简单分组
    """
    chunks = []
    current_group = []
    current_tokens = 0
    current_type = None

    for block in blocks:
        content = get_block_content(block)
        tokens = estimate_tokens(content)

        # 检查是否需要开始新组
        should_start_new = False

        # 1. token超限
        if current_tokens + tokens > max_token_size and current_group:
            should_start_new = True

        # 2. 块类型变化（表格和代码通常独立成块）
        if block.block_type in ["table", "code"] and current_group:
            should_start_new = True

        if should_start_new:
            # 保存当前组
            if current_group:
                group_features = [collect_block_features(item) for item in current_group]
                chunks.append({
                    "block_ids": [b.block_id for b in current_group],
                    "semantic_type": determine_semantic_type(current_group),
                    "reason": "按token限制和类型分组",
                    "metadata": build_chunk_metadata(
                        group_features,
                        reason="按token限制和类型分组",
                        method="fallback",
                    ),
                })
            current_group = []
            current_tokens = 0

        current_group.append(block)
        current_tokens += tokens

    # 保存最后一组
    if current_group:
        group_features = [collect_block_features(item) for item in current_group]
        chunks.append({
            "block_ids": [b.block_id for b in current_group],
            "semantic_type": determine_semantic_type(current_group),
            "reason": "最后一块",
            "metadata": build_chunk_metadata(group_features, reason="最后一块", method="fallback"),
        })

    return chunks


def determine_semantic_type(blocks: List[Block]) -> str:
    """
    根据块内容确定语义类型
    """
    if not blocks:
        return "unknown"

    # 检查块类型
    block_types = [b.block_type for b in blocks]

    if "table" in block_types:
        return "table_data"
    if "code" in block_types:
        return "code_example"

    # 检查内容特征
    all_content = " ".join(get_block_content(b) for b in blocks)
    all_content_lower = all_content.lower()

    # 字段定义特征
    field_keywords = ["字段", "field", "位宽", "bit", "范围", "range", "单位", "unit"]
    if any(kw in all_content_lower for kw in field_keywords):
        return "field_definition"

    # 转换规则特征
    conversion_keywords = ["公式", "formula", "计算", "calculate", "转换", "convert", "映射", "map"]
    if any(kw in all_content_lower for kw in conversion_keywords):
        return "conversion_rule"

    # 协议描述特征
    protocol_keywords = ["协议", "protocol", "概述", "overview", "用途", "purpose", "介绍", "introduction"]
    if any(kw in all_content_lower for kw in protocol_keywords):
        return "protocol_description"

    return "general_content"


def refine_chunks_by_token_limit(
    chunk_suggestions: List[Dict[str, Any]],
    blocks_dict: Dict[int, Block],
    max_token_size: int = 1024
) -> List[Dict[str, Any]]:
    """
    根据token限制细化分块
    """
    refined_chunks = []

    for suggestion in chunk_suggestions:
        block_ids = suggestion.get("block_ids", [])
        semantic_type = suggestion.get("semantic_type", "general_content")
        metadata = suggestion.get("metadata") if isinstance(suggestion.get("metadata"), dict) else {}

        # 获取对应的块
        blocks = [blocks_dict[bid] for bid in block_ids if bid in blocks_dict]
        if not blocks:
            continue

        # 计算总token
        total_tokens = sum(estimate_tokens(get_block_content(b)) for b in blocks)

        if total_tokens <= max_token_size:
            # 不超限，直接保留
            refined_chunks.append(suggestion)
        else:
            # 超限，需要拆分
            sub_chunks = split_chunk_by_tokens(blocks, max_token_size, semantic_type, metadata)
            refined_chunks.extend(sub_chunks)

    return refined_chunks


def split_chunk_by_tokens(
    blocks: List[Block],
    max_token_size: int,
    semantic_type: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    按token限制拆分块
    """
    chunks = []
    current_group = []
    current_tokens = 0

    for block in blocks:
        content = get_block_content(block)
        tokens = estimate_tokens(content)

        # 如果单个块就超限，需要切分内容
        if tokens > max_token_size:
            # 先保存当前组
            if current_group:
                group_features = [collect_block_features(item) for item in current_group]
                merged_metadata = dict(metadata or {})
                merged_metadata.update(
                    build_chunk_metadata(group_features, reason="token限制拆分", method="token_split")
                )
                chunks.append({
                    "block_ids": [b.block_id for b in current_group],
                    "semantic_type": semantic_type,
                    "reason": "token限制拆分",
                    "metadata": merged_metadata,
                })
                current_group = []
                current_tokens = 0

            # 大块单独成组（后续可在内容层面切分）
            single_feature = collect_block_features(block)
            single_metadata = dict(metadata or {})
            single_metadata.update(
                build_chunk_metadata([single_feature], reason="大块单独处理", method="token_split")
            )
            chunks.append({
                "block_ids": [block.block_id],
                "semantic_type": semantic_type,
                "reason": "大块单独处理",
                "metadata": single_metadata,
            })
            continue

        # 检查是否需要开始新组
        if current_tokens + tokens > max_token_size and current_group:
            group_features = [collect_block_features(item) for item in current_group]
            merged_metadata = dict(metadata or {})
            merged_metadata.update(
                build_chunk_metadata(group_features, reason="token限制拆分", method="token_split")
            )
            chunks.append({
                "block_ids": [b.block_id for b in current_group],
                "semantic_type": semantic_type,
                "reason": "token限制拆分",
                "metadata": merged_metadata,
            })
            current_group = []
            current_tokens = 0

        current_group.append(block)
        current_tokens += tokens

    # 保存最后一组
    if current_group:
        group_features = [collect_block_features(item) for item in current_group]
        merged_metadata = dict(metadata or {})
        merged_metadata.update(
            build_chunk_metadata(group_features, reason="token限制拆分", method="token_split")
        )
        chunks.append({
            "block_ids": [b.block_id for b in current_group],
            "semantic_type": semantic_type,
            "reason": "token限制拆分",
            "metadata": merged_metadata,
        })

    return chunks


def generate_content_snapshot(blocks: List[Block], max_length: int = 500) -> str:
    """
    生成内容快照
    """
    contents = []
    total_length = 0

    for block in blocks:
        content = get_block_content(block)
        if total_length + len(content) > max_length:
            # 截断
            remaining = max_length - total_length
            if remaining > 0:
                contents.append(content[:remaining] + "...")
            break
        contents.append(content)
        total_length += len(content)

    return "\n\n".join(contents)


def generate_embedding(text: str) -> List[float]:
    """
    生成文本向量嵌入
    默认优先在CPU上加载本地模型，避免与推理服务抢占GPU显存。
    """
    global embedding_model, resolved_embedding_model_name, resolved_embedding_device
    try:
        # 尝试使用嵌入模型（单例缓存，避免重复加载）
        if embedding_model is None:
            from sentence_transformers import SentenceTransformer
            model_name = resolve_embedding_model_name()
            device = os.getenv("EMBED_DEVICE", "cpu")
            resolved_embedding_model_name = model_name
            resolved_embedding_device = device
            logger.info(f"加载Embedding模型: {model_name} (device={device})")
            embedding_model = SentenceTransformer(
                model_name,
                local_files_only=True,
                device=device,
            )
        embedding = embedding_model.encode(text)
        vector = embedding.tolist()
        if len(vector) != EMBEDDING_DIM:
            # 保障维度一致性
            if len(vector) > EMBEDDING_DIM:
                vector = vector[:EMBEDDING_DIM]
            else:
                vector.extend([0.0] * (EMBEDDING_DIM - len(vector)))
        return vector
    except ImportError:
        logger.warning("sentence-transformers未安装，使用占位符向量")
        return [0.0] * EMBEDDING_DIM
    except Exception as e:
        logger.error(f"生成向量失败: {e}")
        return [0.0] * EMBEDDING_DIM


def save_chunks_to_db(
    chunks: List[Dict[str, Any]],
    blocks_dict: Dict[int, Block],
    project_id: str,
    dataset_id: str
) -> List[Chunk]:
    """
    保存语义块到MySQL和向量库
    """
    saved_chunks = []
    timestamp = int(time.time())

    for idx, chunk_data in enumerate(chunks):
        block_ids = chunk_data.get("block_ids", [])
        semantic_type = chunk_data.get("semantic_type", "general_content")
        chunk_metadata = chunk_data.get("metadata") if isinstance(chunk_data.get("metadata"), dict) else {}

        # 获取对应的块
        blocks = [blocks_dict[bid] for bid in block_ids if bid in blocks_dict]
        if not blocks:
            continue

        # 生成chunk_id
        chunk_id = f"chk_{timestamp}_{idx}_{uuid.uuid4().hex[:8]}"

        # 生成内容快照
        content_snapshot = generate_content_snapshot(blocks)

        if not chunk_metadata:
            features = [collect_block_features(block) for block in blocks]
            chunk_metadata = build_chunk_metadata(features, reason=chunk_data.get("reason", ""), method="save_fallback")

        # 创建Chunk对象
        chunk = Chunk(
            chunk_id=chunk_id,
            project_id=project_id,
            dataset_id=dataset_id,
            source_block_ids=block_ids,
            semantic_type=semantic_type,
            content_snapshot=content_snapshot,
            metadata={
                **chunk_metadata,
                "reason": chunk_data.get("reason", ""),
                "block_count": len(blocks),
                "created_method": "semantic_chunk_api",
            }
        )

        # 保存到MySQL
        try:
            mysql_client.insert_chunk(chunk)
            logger.info(f"保存语义块到MySQL: {chunk_id}")
        except Exception as e:
            logger.error(f"保存MySQL失败: {e}")
            continue

        # 生成并保存向量
        try:
            # 合并所有块内容用于生成向量
            full_content, _ = merge_block_contents(blocks)
            embedding = generate_embedding(full_content)

            # 保存到Milvus
            milvus_client.insert_vectors(
                collection_name=SEMANTIC_COLLECTION_NAME,
                ids=[str(uuid.uuid4())],
                chunk_ids=[chunk_id],
                project_ids=[project_id],
                dataset_ids=[dataset_id],
                semantic_types=[semantic_type],
                contents=[content_snapshot],
                embeddings=[embedding]
            )
            logger.info(f"保存向量到Milvus: {chunk_id}")

        except Exception as e:
            logger.error(f"保存向量失败: {e}")

        saved_chunks.append(chunk)

    return saved_chunks


@app.route("/api/data/semantic_chunk", methods=["POST"])
def semantic_chunk():
    """
    语义单元智能划分与重组接口

    输入参数:
    {
        "project_id": "proj_001",
        "dataset_id": "ds_001",
        "config": {
            "max_token_size": 1024
        },
        "source_block_ids": [101, 102, 103, 201]
    }

    响应格式:
    {
        "code": 200,
        "message": "success",
        "data": {
            "task_id": "chunk_task_001",
            "total_chunks": 3,
            "chunks": [...]
        }
    }
    """
    try:
        data = request.json
        if not data:
            return jsonify({
                "code": 400,
                "message": "请求体不能为空",
                "data": None
            }), 400

        # 参数验证
        project_id = data.get("project_id")
        dataset_id = data.get("dataset_id")
        source_block_ids = data.get("source_block_ids")
        config = data.get("config", {})
        max_token_size = config.get("max_token_size", 1024)
        use_llm_boundary_fallback = bool(config.get("use_llm_boundary_fallback", True))
        target_protocol = normalize_target_protocol(config.get("target_protocol"))
        build_doc_index_enabled = bool(config.get("build_doc_index", True))
        try:
            target_page_window = max(0, int(config.get("target_page_window", 0) or 0))
        except (TypeError, ValueError):
            target_page_window = 0

        if not project_id:
            return jsonify({
                "code": 400,
                "message": "缺少project_id参数",
                "data": None
            }), 400
        if not dataset_id:
            return jsonify({
                "code": 400,
                "message": "缺少dataset_id参数",
                "data": None
            }), 400

        logger.info(
            "开始语义分块: project_id=%s, source_block_ids=%s",
            project_id,
            "all" if not source_block_ids else len(source_block_ids),
        )

        # 1. 获取数据块
        if source_block_ids:
            blocks = mysql_client.get_blocks_by_ids(source_block_ids)
        else:
            blocks = mysql_client.get_blocks_by_project(project_id)
        if not blocks:
            return jsonify({
                "code": 404,
                "message": "未找到可用于语义分块的数据块",
                "data": None
            }), 404

        if target_protocol:
            blocks = filter_blocks_by_target_protocol(
                blocks,
                target_protocol=target_protocol,
                page_window=target_page_window,
            )
            if not blocks:
                return jsonify({
                    "code": 404,
                    "message": f"未找到目标协议{target_protocol}相关块",
                    "data": None
                }), 404

        logger.info(f"获取到 {len(blocks)} 个数据块")

        # 构建块ID到块的映射
        blocks_dict = {b.block_id: b for b in blocks}

        # 2. 分析语义关联性
        analyze_params = inspect.signature(analyze_semantic_relations).parameters
        if "use_llm_fallback" in analyze_params:
            chunk_suggestions = analyze_semantic_relations(
                blocks,
                max_token_size=max_token_size,
                use_llm_fallback=use_llm_boundary_fallback,
            )
        else:
            chunk_suggestions = analyze_semantic_relations(blocks, max_token_size)
        logger.info(f"LLM分析完成，建议分块数: {len(chunk_suggestions)}")

        # 3. 根据token限制细化分块
        refined_chunks = refine_chunks_by_token_limit(
            chunk_suggestions, blocks_dict, max_token_size
        )
        logger.info(f"Token限制细化完成，最终分块数: {len(refined_chunks)}")

        # 4. 生成task_id
        task_id = f"chunk_task_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        # 5. 保存到数据库
        try:
            # 确保Milvus集合存在
            milvus_client.create_collection(
                SEMANTIC_COLLECTION_NAME,
                dim=EMBEDDING_DIM,
                description="语义分块向量存储"
            )
            saved_chunks = save_chunks_to_db(
                refined_chunks, blocks_dict, project_id, dataset_id
            )
            logger.info(f"保存 {len(saved_chunks)} 个语义块到数据库")
        except Exception as e:
            logger.exception(f"保存数据库失败: {e}")
            return jsonify({
                "code": 500,
                "message": f"语义分块保存失败: {str(e)}",
                "data": None
            }), 500

        # 6. 构建响应
        chunks_response = []
        for chunk in saved_chunks:
            chunks_response.append({
                "chunk_id": chunk.chunk_id,
                "source_block_ids": chunk.source_block_ids,
                "semantic_type": chunk.semantic_type,
                "content_snapshot": chunk.content_snapshot[:200] + "..." if len(chunk.content_snapshot) > 200 else chunk.content_snapshot,
                "metadata": chunk.metadata
            })

        doc_index = None
        if build_doc_index_enabled:
            doc_index_result = build_protocol_doc_index(
                project_id=project_id,
                dataset_id=dataset_id,
                blocks=blocks,
                protocol_type=target_protocol or "",
                source_block_ids=source_block_ids or [],
            )
            compatibility_dir = Path(".index") / "pageindex" / project_id / str(doc_index_result["doc_set_id"])
            compatibility_dir.mkdir(parents=True, exist_ok=True)
            (compatibility_dir / "registry.json").write_text(
                json.dumps(doc_index_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            doc_index = {
                "doc_set_id": doc_index_result["doc_set_id"],
                "index_ref": doc_index_result["index_ref"],
                "status": doc_index_result["status"],
                "document_count": doc_index_result["document_count"],
                "storage_path": f".index/pageindex/{project_id}/{doc_index_result['doc_set_id']}/",
            }

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "task_id": task_id,
                "total_chunks": len(chunks_response),
                "target_protocol": target_protocol or None,
                "chunks": chunks_response,
                "doc_index": doc_index,
            }
        })

    except Exception as e:
        logger.exception(f"语义分块处理失败: {e}")
        return jsonify({
            "code": 500,
            "message": f"处理失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/data/semantic_chunk/preview", methods=["POST"])
def preview_semantic_chunk():
    """
    语义分块预览接口（不保存到数据库）

    输入参数同上，但不保存结果
    """
    try:
        data = request.json
        if not data:
            return jsonify({
                "code": 400,
                "message": "请求体不能为空",
                "data": None
            }), 400

        project_id = data.get("project_id")
        source_block_ids = data.get("source_block_ids")
        config = data.get("config", {})
        max_token_size = config.get("max_token_size", 1024)
        use_llm_boundary_fallback = bool(config.get("use_llm_boundary_fallback", True))
        target_protocol = normalize_target_protocol(config.get("target_protocol"))
        try:
            target_page_window = max(0, int(config.get("target_page_window", 0) or 0))
        except (TypeError, ValueError):
            target_page_window = 0

        if not project_id:
            return jsonify({
                "code": 400,
                "message": "缺少project_id参数",
                "data": None
            }), 400

        # 获取数据块
        if source_block_ids:
            blocks = mysql_client.get_blocks_by_ids(source_block_ids)
        else:
            blocks = mysql_client.get_blocks_by_project(project_id)
        if not blocks:
            return jsonify({
                "code": 404,
                "message": "未找到可用于语义分块的数据块",
                "data": None
            }), 404

        if target_protocol:
            blocks = filter_blocks_by_target_protocol(
                blocks,
                target_protocol=target_protocol,
                page_window=target_page_window,
            )
            if not blocks:
                return jsonify({
                    "code": 404,
                    "message": f"未找到目标协议{target_protocol}相关块",
                    "data": None
                }), 404

        blocks_dict = {b.block_id: b for b in blocks}

        # 分析语义
        analyze_params = inspect.signature(analyze_semantic_relations).parameters
        if "use_llm_fallback" in analyze_params:
            chunk_suggestions = analyze_semantic_relations(
                blocks,
                max_token_size=max_token_size,
                use_llm_fallback=use_llm_boundary_fallback,
            )
        else:
            chunk_suggestions = analyze_semantic_relations(blocks, max_token_size)
        refined_chunks = refine_chunks_by_token_limit(
            chunk_suggestions, blocks_dict, max_token_size
        )

        # 构建预览结果
        preview_chunks = []
        for idx, chunk_data in enumerate(refined_chunks):
            block_ids = chunk_data.get("block_ids", [])
            blocks_in_chunk = [blocks_dict[bid] for bid in block_ids if bid in blocks_dict]
            content_snapshot = generate_content_snapshot(blocks_in_chunk, max_length=200)
            full_content, tokens = merge_block_contents(blocks_in_chunk)

            preview_chunks.append({
                "chunk_index": idx,
                "source_block_ids": block_ids,
                "semantic_type": chunk_data.get("semantic_type", "general_content"),
                "content_preview": content_snapshot,
                "estimated_tokens": tokens,
                "reason": chunk_data.get("reason", ""),
                "metadata": chunk_data.get("metadata", {}),
            })

        return jsonify({
            "code": 200,
            "message": "success",
            "data": {
                "total_blocks": len(blocks),
                "total_chunks": len(preview_chunks),
                "max_token_size": max_token_size,
                "target_protocol": target_protocol or None,
                "chunks": preview_chunks
            }
        })

    except Exception as e:
        logger.exception(f"预览失败: {e}")
        return jsonify({
            "code": 500,
            "message": f"预览失败: {str(e)}",
            "data": None
        }), 500


@app.route("/api/data/semantic_chunk/status/<task_id>", methods=["GET"])
def get_chunk_status(task_id: str):
    """
    获取分块任务状态

    注意：当前实现是同步的，此接口保留用于未来异步任务支持
    """
    return jsonify({
        "code": 200,
        "message": "success",
        "data": {
            "task_id": task_id,
            "status": "completed",
            "message": "分块任务已完成"
        }
    })


@app.route("/health", methods=["GET"])
def health():
    """健康检查接口"""
    return jsonify({"status": "healthy", "service": "semantic_chunk"})


if __name__ == "__main__":
    # 初始化数据库表
    try:
        mysql_client.init_tables()
        logger.info("数据库表初始化完成")
    except Exception as e:
        logger.warning(f"数据库表初始化失败: {e}")

    app.run(host="0.0.0.0", port=5006, debug=True)
