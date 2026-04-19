"""Helpers for building ORPO preference data from evaluation reports."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from .e2e_eval import (
    build_case_prompt,
    canonicalize_formula,
    extract_document_text,
    resolve_case_path,
    sanitize_model_output,
)


def score_case_result(case_result: Dict[str, Any]) -> float:
    """Score one evaluated case result for pair selection.

    Args:
        case_result: One case result from the end-to-end evaluation report.

    Returns:
        A higher-is-better score used to rank candidate outputs.
    """
    correctness = float((case_result.get("correctness_result") or {}).get("correctness_score", 0.0))
    conversion_rate = float((case_result.get("rate_result") or {}).get("conversion_rate", 0.0))
    fragment_hit_rate = float(case_result.get("fragment_hit_rate", 0.0))
    penalty = 0.0
    if case_result.get("rule_extraction_error"):
        penalty += 20.0
    if case_result.get("source_incomplete"):
        penalty += 50.0
    return correctness * 0.6 + conversion_rate * 0.3 + fragment_hit_rate * 0.1 - penalty


def build_preference_prompt(case: Dict[str, Any], document_text: str) -> str:
    """Build one chat-style prompt prefix for ORPO training."""
    prompt = build_case_prompt(case, document_text)
    return (
        "<|im_start|>system\n"
        f"{prompt['system_prompt']}\n"
        "<|im_end|>\n"
        "<|im_start|>user\n"
        f"{prompt['user_prompt']}\n"
        "<|im_end|>\n"
        "<|im_start|>assistant\n"
    )


def _normalize_candidate_text(case_result: Dict[str, Any]) -> str:
    raw_output = sanitize_model_output(case_result.get("raw_output"))
    if raw_output:
        return raw_output
    structured_rule = case_result.get("structured_rule") or {}
    return sanitize_model_output(structured_rule.get("formula"))


def build_preference_pairs_from_evaluation_report(
    report_payload: Dict[str, Any],
    case_bundle: Dict[str, Any],
    *,
    include_source_incomplete: bool = False,
    min_score_gap: float = 1.0,
    max_rejected_per_case: int = 3,
) -> Dict[str, Any]:
    """Build ORPO preference pairs from one evaluation report.

    Args:
        report_payload: Output JSON loaded from ``evaluate_finetuned_protocol_e2e.py``.
        case_bundle: Case bundle loaded by ``load_case_bundle``.
        include_source_incomplete: Whether to keep source-incomplete cases.
        min_score_gap: Minimum gap between chosen and rejected scores.
        max_rejected_per_case: Maximum rejected samples emitted for each case.

    Returns:
        A payload containing ``pairs`` and a lightweight ``summary``.
    """
    cases_by_id = {case["case_id"]: case for case in case_bundle["cases"]}
    results_by_case: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for model_payload in report_payload.get("models") or []:
        model_name = str(model_payload.get("model_name") or "").strip() or "unknown"
        for case_result in model_payload.get("cases") or []:
            enriched = dict(case_result)
            enriched["model_name"] = model_name
            results_by_case[str(case_result.get("case_id") or "")].append(enriched)

    pairs: List[Dict[str, Any]] = []
    skipped_source_incomplete = 0
    skipped_missing_case = 0
    skipped_missing_chosen = 0
    skipped_missing_rejected = 0

    bundle_dir = case_bundle.get("_bundle_dir")
    if not bundle_dir and report_payload.get("cases_file"):
        bundle_dir = str(Path(str(report_payload["cases_file"])).resolve().parent)
    for case_id, case_results in results_by_case.items():
        case = cases_by_id.get(case_id)
        if not case:
            skipped_missing_case += 1
            continue
        source_incomplete = any(bool(item.get("source_incomplete")) for item in case_results)
        if source_incomplete and not include_source_incomplete:
            skipped_source_incomplete += 1
            continue

        expected_formula = sanitize_model_output(case.get("expected_formula"))
        ranked_results = sorted(case_results, key=score_case_result, reverse=True)
        chosen_result = ranked_results[0] if ranked_results else None
        chosen_text = expected_formula or (
            sanitize_model_output((chosen_result or {}).get("structured_rule", {}).get("formula"))
        )
        if not chosen_text:
            skipped_missing_chosen += 1
            continue

        chosen_score = 100.0 if expected_formula else score_case_result(chosen_result or {})
        canonical_chosen = canonicalize_formula(chosen_text, case["conversion_mode"])
        document_text = extract_document_text(resolve_case_path(case, bundle_dir, "block_file"))
        prompt = build_preference_prompt(case, document_text)

        seen_rejected = set()
        emitted = 0
        for rejected_result in sorted(case_results, key=score_case_result):
            rejected_text = _normalize_candidate_text(rejected_result)
            if not rejected_text:
                continue
            canonical_rejected = canonicalize_formula(rejected_text, case["conversion_mode"])
            if not canonical_rejected or canonical_rejected == canonical_chosen or canonical_rejected in seen_rejected:
                continue
            score_gap = chosen_score - score_case_result(rejected_result)
            if score_gap < min_score_gap and not rejected_result.get("rule_extraction_error"):
                continue
            seen_rejected.add(canonical_rejected)
            pairs.append(
                {
                    "case_id": case["case_id"],
                    "protocol_type": case["protocol_type"],
                    "message_code": case.get("message_code"),
                    "field_name": case["field_name"],
                    "target_field": case["target_field"],
                    "conversion_mode": case["conversion_mode"],
                    "source_complete": not source_incomplete,
                    "prompt": prompt,
                    "chosen": chosen_text,
                    "rejected": rejected_text,
                    "chosen_source": "expected_formula" if expected_formula else (chosen_result or {}).get("model_name"),
                    "rejected_source": rejected_result.get("model_name"),
                    "chosen_score": round(chosen_score, 4),
                    "rejected_score": round(score_case_result(rejected_result), 4),
                }
            )
            emitted += 1
            if emitted >= max_rejected_per_case:
                break
        if emitted == 0:
            skipped_missing_rejected += 1

    return {
        "pairs": pairs,
        "summary": {
            "total_cases": len(results_by_case),
            "pair_count": len(pairs),
            "skipped_source_incomplete_cases": skipped_source_incomplete,
            "skipped_missing_case": skipped_missing_case,
            "skipped_missing_chosen_cases": skipped_missing_chosen,
            "skipped_missing_rejected_cases": skipped_missing_rejected,
        },
    }
