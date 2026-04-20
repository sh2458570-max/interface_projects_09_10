from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.protocol_conversion.generator import (
    _build_kg_writeback_payload,
    _score_source_candidate,
    _validate_rule_semantic_alignment,
)


def test_kg_writeback_payload_assigns_default_confidence():
    payload = _build_kg_writeback_payload(
        generated_rules=[
            {
                "target_field": "威胁类型",
                "source_fields": ["威胁类型"],
                "conversion_mode": "transcoding",
                "formula_kind": "python_expr",
                "rule": "威胁类型",
                "source": "llm_generated",
                "status": "candidate",
            }
        ],
        source_protocol={"protocol_type": "K1.6", "message_code": "K1.6"},
        target_protocol={"protocol_type": "K1.7", "message_code": "K1.7"},
    )

    rules = payload["rules"]
    assert len(rules) == 1
    assert isinstance(rules[0]["confidence"], float)
    assert 0.0 < rules[0]["confidence"] <= 1.0


def test_control_field_cannot_map_to_business_field():
    target_spec = {
        "field_name": "威胁类型",
        "label": "威胁类型",
        "description": "业务字段",
        "path_parts": ["消息", "威胁类型"],
    }
    assert _score_source_candidate(target_spec, "FPI11") == 0.0

    valid, reason = _validate_rule_semantic_alignment(
        {
            "target_field": "威胁类型",
            "source_fields": ["FPI11"],
            "rule": "FPI11",
            "formula_kind": "python_expr",
        },
        target_spec_map={"威胁类型": target_spec},
        candidate_map={"威胁类型": {"FPI11"}},
    )

    assert valid is False
    assert "控制位字段" in str(reason)
