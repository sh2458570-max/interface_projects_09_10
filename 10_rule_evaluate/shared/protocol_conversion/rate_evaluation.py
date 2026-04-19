from __future__ import annotations

from typing import Any, Dict, List, Optional

from .evaluation import evaluate_protocol_conversion


def _round_rate(value: float) -> float:
    return round(float(value), 4)


def _score_or_default(value: Any, default: float) -> float:
    """Return a numeric score while preserving explicit zero values."""
    if value is None:
        return float(default)
    return float(value)


def _expected_field_count(reference_message: Any, field_scores: List[Dict[str, Any]]) -> int:
    """Return the expected target field count for aggregation."""
    if isinstance(reference_message, dict):
        return len(reference_message)
    return len(field_scores)


def _build_trace_summary(
    trace_id: Optional[str],
    expected_count: int,
    covered_count: int,
    exact_match_count: int,
    high_confidence_count: int,
    low_loss_count: int,
    rule_hit_count: int,
    field_coverage_rate: float,
    conversion_success_rate: float,
    information_loss_rate: float,
    conversion_rate: float,
    field_score_source: str,
) -> Dict[str, Any]:
    """Build the request-level trace aggregation summary."""
    return {
        "trace_id": trace_id,
        "aggregation_scope": "request",
        "field_score_source": field_score_source,
        "expected_field_count": expected_count,
        "covered_field_count": covered_count,
        "conversion_success_count": exact_match_count,
        "high_confidence_count": high_confidence_count,
        "low_loss_count": low_loss_count,
        "rule_hit_count": rule_hit_count,
        "coverage_gap_count": max(expected_count - covered_count, 0),
        "field_coverage_rate": _round_rate(field_coverage_rate),
        "conversion_success_rate": _round_rate(conversion_success_rate),
        "information_loss_rate": _round_rate(information_loss_rate),
        "conversion_rate": _round_rate(conversion_rate),
    }


def evaluate_protocol_conversion_rate(
    converted_message: Any,
    reference_message: Any,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
    source_message: Optional[Any] = None,
    field_weights: Optional[Dict[str, Any]] = None,
    field_scores: Optional[List[Dict[str, Any]]] = None,
    correctness_result: Optional[Dict[str, Any]] = None,
    trace_id: Optional[str] = None,
    use_model_inference: bool = True,
    allow_modelscope_download: bool = True,
    confidence_threshold: float = 80.0,
    low_loss_threshold: float = 20.0,
) -> Dict[str, Any]:
    """Evaluate protocol conversion rate using field-level correctness results."""
    field_score_source = "request" if field_scores else "evaluation"
    if correctness_result is None:
        if field_scores is not None:
            correctness_result = {
                "protocol_type": protocol_type,
                "message_code": str(message_code or "").strip().upper() or None,
                "trace_id": trace_id,
                "normalized_source_message": source_message,
                "normalized_converted_message": converted_message,
                "normalized_reference_message": reference_message,
                "field_scores": field_scores,
                "information_loss_score": 0.0,
                "summary": {
                    "expected_field_count": _expected_field_count(reference_message, field_scores),
                    "matched_field_count": sum(1 for item in field_scores if item.get("exact_match")),
                    "missing_field_count": sum(1 for item in field_scores if not item.get("present")),
                    "unexpected_field_count": 0,
                    "missing_fields": [],
                    "unexpected_fields": [],
                },
                "strategy": {
                    "model_inference_enabled": use_model_inference,
                    "allow_modelscope_download": allow_modelscope_download,
                    "degraded": True,
                },
            }
        else:
            correctness_result = evaluate_protocol_conversion(
                converted_message=converted_message,
                reference_message=reference_message,
                protocol_type=protocol_type,
                message_code=message_code,
                source_message=source_message,
                field_weights=field_weights,
                trace_id=trace_id,
                use_model_inference=use_model_inference,
                allow_modelscope_download=allow_modelscope_download,
            )
    else:
        field_score_source = "request"

    field_scores = correctness_result.get("field_scores") or []
    expected_count = int(
        correctness_result.get("summary", {}).get("expected_field_count")
        or _expected_field_count(reference_message, field_scores)
        or 0
    )
    denominator = max(expected_count, 1)

    covered_count = sum(1 for item in field_scores if item.get("present"))
    exact_match_count = sum(1 for item in field_scores if item.get("exact_match"))
    high_confidence_count = sum(
        1 for item in field_scores if _score_or_default(item.get("field_correctness_score"), 0.0) >= float(confidence_threshold)
    )
    low_loss_count = sum(
        1 for item in field_scores if _score_or_default(item.get("information_loss_score"), 100.0) <= float(low_loss_threshold)
    )
    rule_hit_count = sum(1 for item in field_scores if item.get("formula") or item.get("source_field"))

    field_coverage_rate = covered_count / denominator * 100.0
    exact_match_rate = exact_match_count / denominator * 100.0
    conversion_success_rate = exact_match_rate
    high_confidence_rate = high_confidence_count / denominator * 100.0
    low_loss_rate = low_loss_count / denominator * 100.0
    rule_hit_rate = rule_hit_count / denominator * 100.0
    if field_scores:
        information_loss_rate = sum(_score_or_default(item.get("information_loss_score"), 0.0) for item in field_scores) / len(field_scores)
    else:
        information_loss_rate = float(correctness_result.get("information_loss_score") or 0.0)

    conversion_rate = (
        field_coverage_rate * 0.35
        + conversion_success_rate * 0.25
        + high_confidence_rate * 0.20
        + low_loss_rate * 0.10
        + rule_hit_rate * 0.10
    )

    strategy = dict(correctness_result.get("strategy") or {})
    strategy.update(
        {
            "aggregation_scope": "request",
            "field_score_source": field_score_source,
        }
    )

    return {
        "protocol_type": correctness_result.get("protocol_type"),
        "message_code": correctness_result.get("message_code"),
        "trace_id": correctness_result.get("trace_id"),
        "conversion_rate": _round_rate(conversion_rate),
        "field_coverage_rate": _round_rate(field_coverage_rate),
        "exact_match_rate": _round_rate(exact_match_rate),
        "conversion_success_rate": _round_rate(conversion_success_rate),
        "high_confidence_rate": _round_rate(high_confidence_rate),
        "low_loss_rate": _round_rate(low_loss_rate),
        "information_loss_rate": _round_rate(information_loss_rate),
        "rule_hit_rate": _round_rate(rule_hit_rate),
        "summary": {
            "expected_field_count": expected_count,
            "covered_field_count": covered_count,
            "exact_match_count": exact_match_count,
            "conversion_success_count": exact_match_count,
            "high_confidence_count": high_confidence_count,
            "low_loss_count": low_loss_count,
            "rule_hit_count": rule_hit_count,
            "missing_fields": correctness_result.get("summary", {}).get("missing_fields") or [],
            "unexpected_fields": correctness_result.get("summary", {}).get("unexpected_fields") or [],
        },
        "trace_summary": _build_trace_summary(
            trace_id=correctness_result.get("trace_id"),
            expected_count=expected_count,
            covered_count=covered_count,
            exact_match_count=exact_match_count,
            high_confidence_count=high_confidence_count,
            low_loss_count=low_loss_count,
            rule_hit_count=rule_hit_count,
            field_coverage_rate=field_coverage_rate,
            conversion_success_rate=conversion_success_rate,
            information_loss_rate=information_loss_rate,
            conversion_rate=conversion_rate,
            field_score_source=field_score_source,
        ),
        "thresholds": {
            "confidence_threshold": float(confidence_threshold),
            "low_loss_threshold": float(low_loss_threshold),
        },
        "strategy": strategy,
        "correctness_result": correctness_result,
    }
