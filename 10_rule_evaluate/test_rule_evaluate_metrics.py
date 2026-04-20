from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.protocol_conversion.rule_evaluation import evaluate_protocol_rules
from shared.retrieval.reranker import inspect_reranker_model_dir


def _write_xml(directory: Path, filename: str, field_names: list[str]) -> None:
    items = "\n".join(f'    <Item name="{field_name}">8</Item>' for field_name in field_names)
    xml = f"<Root>\n  <NameSpace name=\"消息\">\n{items}\n  </NameSpace>\n</Root>\n"
    (directory / filename).write_text(xml, encoding="utf-8")


def test_inspect_reranker_model_dir_accepts_qwen3_causallm_reranker(tmp_path):
    model_dir = tmp_path / "Qwen3-Reranker-0___6B"
    model_dir.mkdir()
    (model_dir / "config.json").write_text(
        json.dumps(
            {
                "architectures": ["Qwen3ForCausalLM"],
                "model_type": "qwen3",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (model_dir / "README.md").write_text(
        "# Qwen3-Reranker-0.6B\n\nThis is a text reranking model.\n",
        encoding="utf-8",
    )

    inspection = inspect_reranker_model_dir(model_dir)

    assert inspection["compatible"] is True
    assert inspection["loader_type"] == "causal_lm_reranker"


def test_rule_metrics_accuracy_only_counts_convertible_fields(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    _write_xml(source_dir, "source.xml", ["FIELD_A"])
    _write_xml(target_dir, "target.xml", ["FIELD_A", "FIELD_B"])

    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        json.dumps(
            [
                {
                    "target_field": "FIELD_A",
                    "source_fields": ["FIELD_A"],
                    "formula": "FIELD_A",
                    "rule_type": "direct",
                },
                {
                    "target_field": "FIELD_B",
                    "source_fields": [],
                    "formula": "0",
                    "rule_type": "const",
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = evaluate_protocol_rules(
        source_protocol_dirs=[str(source_dir)],
        target_protocol_dir=str(target_dir),
        conversion_rules=str(rules_path),
        coarse_top_k=5,
        coarse_similarity_threshold=0.0,
        fine_similarity_threshold=0.0,
        use_model_inference=False,
        allow_modelscope_download=False,
    )

    assert result["summary"]["target_field_count"] == 2
    assert result["summary"]["convertible_field_count"] == 1
    assert result["summary"]["non_zero_rule_count"] == 1
    assert result["scores"]["field_match_accuracy"] == 100.0
    assert result["scores"]["field_coverage_rate"] == 100.0
    assert result["scores"]["final_conversion_rate"] == 50.0
