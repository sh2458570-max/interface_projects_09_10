from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

MAPPING_PAIR_REGEX = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:=|->|→)\s*([^,;，；。\n]+)")
RANGE_PAIR_REGEX = re.compile(r"(-?\d+(?:\.\d+)?)\s*(?:-|~|～|—|–)\s*(-?\d+(?:\.\d+)?)\s*(?:=|->|→)\s*([^,;，；。\n]+)")

from .converter import execute_protocol_conversion, extract_executable_formula, parse_llm_formula_output
from .evaluation import evaluate_protocol_conversion
from .rate_evaluation import evaluate_protocol_conversion_rate

TRAINING_STYLE_SYSTEM_PROMPT = (
    "你是一个专业的网络消息协议转换助手，严格按照指令将原始协议消息转换为指定目标协议消息。"
)


def load_case_bundle(bundle_path: str | Path) -> Dict[str, Any]:
    """Load and validate a JSON case bundle for end-to-end evaluation."""
    path = Path(bundle_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("评测样例文件必须是 JSON 对象")
    cases = payload.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("评测样例文件缺少非空 cases 列表")
    validated_cases = [validate_case_definition(case) for case in cases]
    payload["cases"] = validated_cases
    payload["_bundle_dir"] = str(path.parent)
    return payload


def validate_case_definition(case: Dict[str, Any]) -> Dict[str, Any]:
    """Validate one case definition and return a normalized copy."""
    required_keys = [
        "case_id",
        "protocol_type",
        "message_code",
        "block_file",
        "field_name",
        "target_field",
        "conversion_mode",
        "question",
        "source_message",
        "reference_message",
    ]
    missing = [key for key in required_keys if key not in case]
    if missing:
        raise ValueError(f"评测样例缺少字段: {', '.join(missing)}")
    normalized = dict(case)
    normalized["field_name"] = str(case["field_name"]).strip().upper()
    normalized["target_field"] = str(case["target_field"]).strip().upper()
    normalized["conversion_mode"] = str(case["conversion_mode"]).strip().lower()
    if normalized["conversion_mode"] not in {"transcoding", "mapping"}:
        raise ValueError(f"不支持的 conversion_mode: {normalized['conversion_mode']}")
    source_evidence_fragments = case.get("source_evidence_fragments") or []
    if source_evidence_fragments and not isinstance(source_evidence_fragments, list):
        raise ValueError("source_evidence_fragments 必须是字符串列表")
    normalized["source_evidence_fragments"] = [
        str(item).strip() for item in source_evidence_fragments if str(item).strip()
    ]
    return normalized


def resolve_case_path(case: Dict[str, Any], bundle_dir: str | Path, key: str) -> Path:
    """Resolve a case-relative path against likely project roots."""
    raw = Path(str(case[key]))
    if raw.is_absolute():
        return raw

    bundle_path = Path(bundle_dir)
    candidates = [
        bundle_path / raw,
        bundle_path.parent / raw,
        bundle_path.parent.parent / raw,
        Path.cwd() / raw,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def extract_document_text(block_path: str | Path) -> str:
    """Extract plain text from a stored block JSON file."""
    payload = json.loads(Path(block_path).read_text(encoding="utf-8"))
    blocks = payload.get("blocks") if isinstance(payload, dict) else None
    if not isinstance(blocks, list):
        raise ValueError(f"块文件格式不合法: {block_path}")
    parts: List[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        content = str(block.get("content") or block.get("text") or "").strip()
        if content:
            parts.append(content)
    return "\n".join(parts).strip()


def build_case_prompt(case: Dict[str, Any], document_text: str) -> Dict[str, str]:
    """Build a training-style prompt for the current real-document case."""
    instruction = (
        "根据真实协议文档内容回答协议转换问题。"
        "第一行必须只输出唯一可执行的转换公式或映射规则。"
        "如果必须补充说明，只能从第二行开始并以“说明:”开头。"
    )
    user_prompt = (
        f"{instruction}\n\n"
        f"文档内容：\n{document_text}\n\n"
        f"问题：{case['question']}"
    )
    return {
        "system_prompt": TRAINING_STYLE_SYSTEM_PROMPT,
        "user_prompt": user_prompt,
    }


def sanitize_model_output(text: Any) -> str:
    """Remove common wrapper noise and keep the model answer body."""
    cleaned = str(text or "").strip()
    cleaned = re.sub(r"<think>[\s\S]*?</think>", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^<think>\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"```(?:json)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("```", "")
    cleaned = re.sub(r"<\|im_start\|>assistant\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"<\|im_end\|>\s*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def _nonempty_lines(text: str) -> List[str]:
    return [line.strip() for line in str(text or "").splitlines() if line.strip()]


def canonicalize_formula(formula: str, mode: str) -> str:
    """Canonicalize one formula string for exact-string diagnostics."""
    cleaned = sanitize_model_output(formula)
    if mode == "mapping":
        normalized = cleaned.replace("→", "=").replace("->", "=")
        parts = [re.sub(r"\s+", "", item) for item in re.split(r"[,;\n]", normalized) if item.strip()]
        return ",".join(sorted(parts))
    return re.sub(r"\s+", "", cleaned)


def extract_formula_from_text(text: str, mode: str) -> str:
    """Extract the most likely executable formula from a verbose answer."""
    return extract_executable_formula(sanitize_model_output(text), mode)


def _is_verbose_output(raw_output: str, formula: str, mode: str) -> bool:
    cleaned = sanitize_model_output(raw_output)
    normalized_formula = canonicalize_formula(formula, mode)
    if not cleaned or not normalized_formula:
        return False

    cleaned_lines = _nonempty_lines(cleaned)
    if len(cleaned_lines) > 1:
        first_line = cleaned_lines[0]
        if canonicalize_formula(first_line, mode) == normalized_formula:
            return True
        if any(marker in " ".join(cleaned_lines[1:]).lower() for marker in ("说明", "解释", "because", "therefore")):
            return True

    normalized_cleaned = canonicalize_formula(cleaned, mode)
    if normalized_cleaned != normalized_formula and (
        "\n" in cleaned
        or len(cleaned) > len(formula) + 8
        or any(marker in cleaned.lower() for marker in ("说明", "解释", "because", "therefore", "根据文档"))
    ):
        return True
    return False


def _normalize_fragment_text(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip()).lower()


def _expected_mapping_fragment(case: Dict[str, Any]) -> Optional[str]:
    if case.get("conversion_mode") != "mapping":
        return None
    source_message = case.get("source_message")
    if not isinstance(source_message, dict):
        return None
    field_value = source_message.get(case["field_name"])
    if field_value is None:
        return None
    expected_formula = str(case.get("expected_formula") or "")
    expected_value = _normalize_fragment_text(field_value)
    for left, label in MAPPING_PAIR_REGEX.findall(expected_formula):
        if _normalize_fragment_text(left) != expected_value:
            continue
        return f"{left}={label.strip()}"
    return None


def derive_source_evidence_fragments(case: Dict[str, Any]) -> List[str]:
    """Derive source-side evidence fragments used for completeness tagging."""
    explicit_fragments = [
        str(item).strip() for item in case.get("source_evidence_fragments") or [] if str(item).strip()
    ]
    if explicit_fragments:
        return explicit_fragments
    mapping_fragment = _expected_mapping_fragment(case)
    if mapping_fragment:
        return [mapping_fragment]
    return []


def analyze_case_source_completeness(case: Dict[str, Any], document_text: str) -> Dict[str, Any]:
    """Analyze whether the source document contains the minimum evidence for the case."""
    evidence_fragments = derive_source_evidence_fragments(case)
    normalized_document = _normalize_fragment_text(document_text)
    matched_fragments = [
        fragment for fragment in evidence_fragments if _normalize_fragment_text(fragment) in normalized_document
    ]
    missing_fragments = [fragment for fragment in evidence_fragments if fragment not in matched_fragments]
    source_incomplete = bool(evidence_fragments) and bool(missing_fragments)
    if source_incomplete:
        reason = f"源块缺少关键证据片段: {', '.join(missing_fragments)}"
    elif evidence_fragments:
        reason = "源块已包含当前样例所需的关键证据片段"
    else:
        reason = "未配置可自动判定的源证据片段，跳过 source_incomplete 自动标记"
    return {
        "source_incomplete": source_incomplete,
        "evidence_fragments": evidence_fragments,
        "matched_fragments": matched_fragments,
        "missing_fragments": missing_fragments,
        "reason": reason,
    }


def build_structured_rule(case: Dict[str, Any], raw_output: Any) -> Dict[str, Any]:
    """Turn raw model output into one executable structured conversion rule."""
    cleaned = sanitize_model_output(raw_output)
    parsed_rules = parse_llm_formula_output(cleaned, source_fields=[case["field_name"]])
    parsed_rule = parsed_rules[0].to_dict() if parsed_rules else {}
    parsed_formula = str(parsed_rule.get("formula") or "").strip()
    heuristic_formula = extract_executable_formula(cleaned, case["conversion_mode"], source_field=case["field_name"])
    formula = str(parsed_formula or heuristic_formula or "").strip()
    if not formula:
        raise ValueError("未从模型输出中提取到可执行规则")
    validated_rules = parse_llm_formula_output(formula, source_fields=[case["field_name"]])
    if not validated_rules:
        raise ValueError("抽取到的规则仍不可执行")
    validated_rule = validated_rules[0].to_dict()
    conversion_mode = str(validated_rule.get("conversion_mode") or parsed_rule.get("conversion_mode") or case["conversion_mode"]).strip()
    heuristic_extraction_used = bool(not parsed_formula and heuristic_formula)
    verbose_output = _is_verbose_output(cleaned, formula, conversion_mode)
    return {
        "field_name": case["field_name"],
        "target_field": case["target_field"],
        "conversion_mode": conversion_mode,
        "formula": validated_rule.get("formula") or formula,
        "bit_length": case.get("bit_length"),
        "unit": case.get("unit"),
        "description": case.get("description"),
        "valid": True,
        "verbose_output": verbose_output,
        "heuristic_extraction_used": heuristic_extraction_used,
        "extraction_method": "heuristic" if heuristic_extraction_used else "parsed",
    }


def _build_failed_case_result(
    case: Dict[str, Any],
    raw_output: Any,
    error_message: str,
    use_model_inference: bool = False,
    allow_modelscope_download: bool = False,
) -> Dict[str, Any]:
    cleaned_output = sanitize_model_output(raw_output)
    conversion_result = execute_protocol_conversion(
        source_message=case["source_message"],
        llm_formula_output=[],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        use_knowledge_base=False,
    )
    correctness_result = evaluate_protocol_conversion(
        converted_message=conversion_result["converted_message"],
        reference_message=case["reference_message"],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        source_message=case["source_message"],
        trace_id=case["case_id"],
        use_model_inference=use_model_inference,
        allow_modelscope_download=allow_modelscope_download,
    )
    rate_result = evaluate_protocol_conversion_rate(
        converted_message=conversion_result["converted_message"],
        reference_message=case["reference_message"],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        source_message=case["source_message"],
        correctness_result=correctness_result,
        trace_id=case["case_id"],
        use_model_inference=use_model_inference,
        allow_modelscope_download=allow_modelscope_download,
    )
    rate_result.update({
        "conversion_rate": 0.0,
        "field_coverage_rate": 0.0,
        "exact_match_rate": 0.0,
        "conversion_success_rate": 0.0,
        "high_confidence_rate": 0.0,
        "low_loss_rate": 0.0,
        "rule_hit_rate": 0.0,
    })
    if isinstance(rate_result.get("summary"), dict):
        rate_result["summary"].update({
            "covered_field_count": 0,
            "exact_match_count": 0,
            "conversion_success_count": 0,
            "high_confidence_count": 0,
            "low_loss_count": 0,
            "rule_hit_count": 0,
        })
    if isinstance(rate_result.get("trace_summary"), dict):
        rate_result["trace_summary"].update({
            "covered_field_count": 0,
            "conversion_success_count": 0,
            "high_confidence_count": 0,
            "low_loss_count": 0,
            "rule_hit_count": 0,
            "field_coverage_rate": 0.0,
            "conversion_success_rate": 0.0,
            "conversion_rate": 0.0,
        })
    expected_formula = str(case.get("expected_formula") or "").strip()
    required_fragments = [str(item).strip() for item in case.get("required_fragments") or [] if str(item).strip()]
    return {
        "case_id": case["case_id"],
        "question": case["question"],
        "protocol_type": case["protocol_type"],
        "message_code": case.get("message_code"),
        "raw_output": cleaned_output,
        "structured_rule": {
            "field_name": case["field_name"],
            "target_field": case["target_field"],
            "conversion_mode": case["conversion_mode"],
            "formula": "",
            "bit_length": case.get("bit_length"),
            "unit": case.get("unit"),
            "description": case.get("description"),
            "valid": False,
            "verbose_output": False,
            "heuristic_extraction_used": False,
            "extraction_method": None,
            "extraction_error": error_message,
        },
        "expected_formula": expected_formula or None,
        "exact_formula_match": False if expected_formula else None,
        "required_fragments": required_fragments,
        "matched_fragments": [],
        "fragment_hit_rate": 0.0 if required_fragments else 100.0,
        "conversion_result": conversion_result,
        "correctness_result": correctness_result,
        "rate_result": rate_result,
        "rule_extraction_error": error_message,
        "source_diagnostics": case.get("source_diagnostics") or analyze_case_source_completeness(case, ""),
        "source_incomplete": bool((case.get("source_diagnostics") or {}).get("source_incomplete")),
    }


def evaluate_case_output(
    case: Dict[str, Any],
    raw_output: Any,
    document_text: str = "",
    use_model_inference: bool = False,
    allow_modelscope_download: bool = False,
) -> Dict[str, Any]:
    """Execute and score one case using raw model output as the rule source."""
    source_diagnostics = analyze_case_source_completeness(case, document_text)
    case = dict(case)
    case["source_diagnostics"] = source_diagnostics
    try:
        structured_rule = build_structured_rule(case, raw_output)
    except ValueError as exc:
        return _build_failed_case_result(
            case,
            raw_output,
            str(exc),
            use_model_inference=use_model_inference,
            allow_modelscope_download=allow_modelscope_download,
        )
    conversion_result = execute_protocol_conversion(
        source_message=case["source_message"],
        llm_formula_output=[structured_rule],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        use_knowledge_base=False,
    )
    correctness_result = evaluate_protocol_conversion(
        converted_message=conversion_result["converted_message"],
        reference_message=case["reference_message"],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        source_message=case["source_message"],
        trace_id=case["case_id"],
        use_model_inference=use_model_inference,
        allow_modelscope_download=allow_modelscope_download,
    )
    rate_result = evaluate_protocol_conversion_rate(
        converted_message=conversion_result["converted_message"],
        reference_message=case["reference_message"],
        protocol_type=case["protocol_type"],
        message_code=case.get("message_code"),
        source_message=case["source_message"],
        correctness_result=correctness_result,
        trace_id=case["case_id"],
        use_model_inference=use_model_inference,
        allow_modelscope_download=allow_modelscope_download,
    )

    expected_formula = str(case.get("expected_formula") or "").strip()
    exact_formula_match = None
    if expected_formula:
        exact_formula_match = (
            canonicalize_formula(structured_rule["formula"], structured_rule["conversion_mode"])
            == canonicalize_formula(expected_formula, structured_rule["conversion_mode"])
        )

    required_fragments = [str(item).strip() for item in case.get("required_fragments") or [] if str(item).strip()]
    predicted_formula_text = sanitize_model_output(structured_rule["formula"]).lower()
    fragment_hits = [fragment for fragment in required_fragments if fragment.lower() in predicted_formula_text]
    fragment_hit_rate = round(len(fragment_hits) / max(len(required_fragments), 1) * 100.0, 4) if required_fragments else 100.0

    return {
        "case_id": case["case_id"],
        "question": case["question"],
        "protocol_type": case["protocol_type"],
        "message_code": case.get("message_code"),
        "raw_output": sanitize_model_output(raw_output),
        "structured_rule": structured_rule,
        "verbose_output": bool(structured_rule.get("verbose_output")),
        "heuristic_extraction_used": bool(structured_rule.get("heuristic_extraction_used")),
        "expected_formula": expected_formula or None,
        "exact_formula_match": exact_formula_match,
        "required_fragments": required_fragments,
        "matched_fragments": fragment_hits,
        "fragment_hit_rate": fragment_hit_rate,
        "conversion_result": conversion_result,
        "correctness_result": correctness_result,
        "rate_result": rate_result,
        "source_diagnostics": source_diagnostics,
        "source_incomplete": bool(source_diagnostics.get("source_incomplete")),
    }


def aggregate_case_results(case_results: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate multiple per-case evaluation results into one summary."""
    results = list(case_results)
    count = len(results)
    if count == 0:
        return {
            "total_cases": 0,
            "perfect_conversion_cases": 0,
            "perfect_conversion_complete_source_cases": 0,
            "avg_correctness_score": 0.0,
            "avg_conversion_rate": 0.0,
            "avg_correctness_score_complete_source_cases": 0.0,
            "avg_conversion_rate_complete_source_cases": 0.0,
            "avg_fragment_hit_rate": 0.0,
            "exact_formula_match_cases": 0,
            "source_incomplete_cases": 0,
            "source_complete_cases": 0,
            "model_failure_cases": 0,
            "rule_extraction_failed_cases": 0,
            "executable_rule_cases": 0,
            "executable_rule_rate": 0.0,
            "verbose_output_cases": 0,
            "verbose_output_rate": 0.0,
            "heuristic_extraction_used_cases": 0,
            "heuristic_extraction_rate": 0.0,
        }

    complete_source_results = [item for item in results if not item.get("source_incomplete")]
    perfect_conversion_cases = sum(
        1
        for item in results
        if float(item["rate_result"].get("conversion_rate", 0.0)) >= 100.0
        and float(item["correctness_result"].get("correctness_score", 0.0)) >= 99.0
    )
    perfect_conversion_complete_source_cases = sum(
        1
        for item in complete_source_results
        if float(item["rate_result"].get("conversion_rate", 0.0)) >= 100.0
        and float(item["correctness_result"].get("correctness_score", 0.0)) >= 99.0
    )
    exact_formula_match_cases = sum(1 for item in results if item.get("exact_formula_match") is True)
    avg_correctness_score = round(
        sum(float(item["correctness_result"].get("correctness_score", 0.0)) for item in results) / count,
        4,
    )
    avg_conversion_rate = round(
        sum(float(item["rate_result"].get("conversion_rate", 0.0)) for item in results) / count,
        4,
    )
    avg_fragment_hit_rate = round(
        sum(float(item.get("fragment_hit_rate", 0.0)) for item in results) / count,
        4,
    )
    complete_source_count = len(complete_source_results)
    avg_correctness_score_complete_source_cases = round(
        sum(float(item["correctness_result"].get("correctness_score", 0.0)) for item in complete_source_results)
        / max(complete_source_count, 1),
        4,
    )
    avg_conversion_rate_complete_source_cases = round(
        sum(float(item["rate_result"].get("conversion_rate", 0.0)) for item in complete_source_results)
        / max(complete_source_count, 1),
        4,
    )
    source_incomplete_cases = sum(1 for item in results if item.get("source_incomplete"))
    executable_rule_cases = sum(1 for item in results if bool((item.get("structured_rule") or {}).get("valid")))
    verbose_output_cases = sum(1 for item in results if bool((item.get("structured_rule") or {}).get("verbose_output")))
    heuristic_extraction_used_cases = sum(
        1 for item in results if bool((item.get("structured_rule") or {}).get("heuristic_extraction_used"))
    )
    model_failure_cases = sum(
        1 for item in complete_source_results if float(item["rate_result"].get("conversion_rate", 0.0)) < 100.0
    )
    rule_extraction_failed_cases = sum(
        1 for item in complete_source_results if bool(item.get("rule_extraction_error"))
    )
    return {
        "total_cases": count,
        "perfect_conversion_cases": perfect_conversion_cases,
        "perfect_conversion_complete_source_cases": perfect_conversion_complete_source_cases,
        "avg_correctness_score": avg_correctness_score,
        "avg_conversion_rate": avg_conversion_rate,
        "avg_correctness_score_complete_source_cases": avg_correctness_score_complete_source_cases,
        "avg_conversion_rate_complete_source_cases": avg_conversion_rate_complete_source_cases,
        "avg_fragment_hit_rate": avg_fragment_hit_rate,
        "exact_formula_match_cases": exact_formula_match_cases,
        "source_incomplete_cases": source_incomplete_cases,
        "source_complete_cases": complete_source_count,
        "model_failure_cases": model_failure_cases,
        "rule_extraction_failed_cases": rule_extraction_failed_cases,
        "executable_rule_cases": executable_rule_cases,
        "executable_rule_rate": round(executable_rule_cases / count * 100.0, 4),
        "verbose_output_cases": verbose_output_cases,
        "verbose_output_rate": round(verbose_output_cases / count * 100.0, 4),
        "heuristic_extraction_used_cases": heuristic_extraction_used_cases,
        "heuristic_extraction_rate": round(heuristic_extraction_used_cases / count * 100.0, 4),
    }
