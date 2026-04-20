from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
API_APP_PATH = PROJECT_ROOT / "api_03_extract_validate" / "app.py"


def _load_api_module():
    spec = importlib.util.spec_from_file_location("interface_project_07_api_app", API_APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {API_APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _prepare_protocol_dirs(tmp_path: Path) -> tuple[Path, Path]:
    examples_dir = (
        PROJECT_ROOT
        / "code_generate"
        / "project_generator"
        / "examples"
        / "simple"
        / "protocols"
    )
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    shutil.copy(examples_dir / "temp_sensor.xml", source_dir / "temp_sensor.xml")
    shutil.copy(examples_dir / "temp_report.xml", target_dir / "temp_report.xml")
    return source_dir, target_dir


def test_protocol_generate_rules_returns_four_boolean_checks(tmp_path):
    module = _load_api_module()
    source_dir, target_dir = _prepare_protocol_dirs(tmp_path)

    module.generate_protocol_field_rules = lambda **_: {
        "generated_rules": [
            {
                "target_field": "TEMPERATURE_C",
                "source_fields": ["TEMPERATURE"],
                "formula": "TEMPERATURE",
                "rule": "TEMPERATURE",
                "conversion_mode": "transcoding",
                "description": "温度直传",
            }
        ],
        "normalized_rules": [
            {
                "target_field": "TEMPERATURE_C",
                "source_fields": ["TEMPERATURE"],
                "formula": "TEMPERATURE",
                "rule": "TEMPERATURE",
                "conversion_mode": "transcoding",
                "description": "温度直传",
            }
        ],
        "kg_writeback_payload": {},
        "summary": {"total_rules": 1},
        "raw_output": "mocked",
    }

    client = module.app.test_client()
    response = client.post(
        "/api/knowledge/protocol_generate_rules",
        json={
            "source_protocol_dirs": [str(source_dir)],
            "target_protocol_dir": str(target_dir),
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    rules_json_path = payload["data"]["conversion_rules_json"]
    assert isinstance(rules_json_path, str)
    assert Path(rules_json_path).exists()
    saved_rules = json.loads(Path(rules_json_path).read_text(encoding="utf-8"))
    assert saved_rules["conversions"][0]["rules"][0]["target_field"] == "TEMPERATURE_C"
    assert payload["data"]["summary"] == {
        "knowledge_graph_field_count": 0,
        "llm_converted_field_count": 0,
    }
    assert "_meta" not in payload["data"]
    assert payload["data"]["validation_result"] == {
        "field_legality": True,
        "position_accuracy": True,
        "conversion_logic": True,
        "protocol_compliance": True,
    }


def test_displayize_rule_records_preserves_rule_alignment_for_writeback(tmp_path):
    module = _load_api_module()
    source_dir, target_dir = _prepare_protocol_dirs(tmp_path)

    protocol_dir = module._merge_protocol_dirs([str(source_dir)], str(target_dir))
    try:
        display_rules = module._displayize_rule_records(
            [
                {
                    "concept_name": "温度",
                    "source_fields": ["TEMP_SENSOR.TEMPERATURE"],
                    "target_field": "TEMPERATURE_C",
                    "conversion_mode": "transcoding",
                    "formula": "temp_sensor.temperature",
                    "source": "llm_generated",
                    "status": "candidate",
                },
                {
                    "concept_name": "无效候选",
                    "source_fields": [],
                    "target_field": "STATUS",
                    "conversion_mode": "transcoding",
                    "formula": "0",
                    "source": "llm_generated",
                    "status": "candidate",
                },
            ],
            protocol_dir=protocol_dir,
            target_protocol_name="Temp_Report",
            source_protocol_name="Temp_Sensor",
        )
    finally:
        if protocol_dir and protocol_dir.exists():
            shutil.rmtree(protocol_dir, ignore_errors=True)

    assert len(display_rules) == 2
    assert display_rules[0]["target_field"] == "temperature_c"
    assert display_rules[0]["formula"] == "temp_sensor.temperature"
    assert display_rules[1]["target_field"] == "status"
    assert display_rules[1]["formula"] == "0"
