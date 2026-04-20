from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DIR = ROOT_DIR / "test"
DATA_DIR = TEST_DIR / "data" / "codegen"
OUTPUT_DIR = TEST_DIR / "output"
REPORT_PATH = OUTPUT_DIR / "smoke_report.json"

INTERFACES = {
    "01": {"name": "01_validate_protocol_files", "port": 6101, "health_project": "01_validate_protocol_files"},
    "02": {"name": "02_upload_split", "port": 6102, "health_project": "02_upload_split"},
    "03": {"name": "03_clean", "port": 6103, "health_project": "03_clean"},
    "04": {"name": "04_semantic_chunk", "port": 6104, "health_project": "04_semantic_chunk"},
    "05": {"name": "05_generate_qa", "port": 6105, "health_project": "05_generate_qa"},
    "06": {"name": "06_extract_validate_qa", "port": 6106, "health_project": "06_extract_validate_qa"},
    "07": {"name": "07_protocol_generate_rules", "port": 6107, "health_project": "07_protocol_generate_rules"},
    "08": {"name": "08_code_generation", "port": 6108, "health_project": "08_code_generation"},
    "09": {"name": "09_finetune_runtime", "port": 6109, "health_project": "09_finetune_runtime"},
    "10": {"name": "10_rule_evaluate", "port": 6110, "health_project": "10_rule_evaluate"},
}


def _normalize_interfaces(raw: str | None) -> List[str]:
    if not raw:
        return list(INTERFACES.keys())
    values = []
    for item in raw.split(","):
        text = item.strip()
        if not text:
            continue
        if text in INTERFACES:
            values.append(text)
            continue
        for key, meta in INTERFACES.items():
            if text == meta["name"]:
                values.append(key)
                break
        else:
            raise ValueError(f"未知接口标识: {text}")
    if not values:
        raise ValueError("interfaces 不能为空")
    return values


def _normalize_suites(raw: str) -> List[str]:
    suites = [item.strip().lower() for item in raw.split(",") if item.strip()]
    allowed = {"health", "contract", "codegen", "rule-eval"}
    invalid = [item for item in suites if item not in allowed]
    if invalid:
        raise ValueError(f"未知测试套件: {', '.join(invalid)}")
    return suites


def _base_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
    return requests.request(method=method, url=url, timeout=30, **kwargs)


def _record(results: List[Dict[str, Any]], name: str, passed: bool, details: Dict[str, Any]) -> None:
    results.append({"name": name, "passed": passed, "details": details})


def run_health_suite(host: str, interface_ids: List[str], results: List[Dict[str, Any]]) -> None:
    for interface_id in interface_ids:
        meta = INTERFACES[interface_id]
        url = f"{_base_url(host, meta['port'])}/health"
        response = _request("GET", url)
        payload = response.json()
        passed = response.status_code == 200 and payload.get("project") == meta["health_project"]
        _record(
            results,
            f"health:{interface_id}",
            passed,
            {
                "url": url,
                "status_code": response.status_code,
                "payload": payload,
            },
        )


def run_contract_suite(host: str, interface_ids: List[str], results: List[Dict[str, Any]]) -> None:
    contract_cases = [
        ("06", "/api/knowledge/extract_validate_qa", "qa_id"),
        ("07", "/api/knowledge/protocol_generate_rules", "source_protocol_dirs"),
        ("08", "/api/code_generation/generate", "source_protocol_dirs"),
        ("10", "/api/knowledge/rule_evaluate", "source_protocol_dirs"),
    ]
    for interface_id, path, expected_text in contract_cases:
        if interface_id not in interface_ids:
            continue
        meta = INTERFACES[interface_id]
        url = f"{_base_url(host, meta['port'])}{path}"
        response = _request("POST", url, json={})
        try:
            payload = response.json()
        except Exception:
            payload = {"raw": response.text}
        message = str(payload.get("message") or payload)
        passed = response.status_code == 400 and expected_text in message
        _record(
            results,
            f"contract:{interface_id}",
            passed,
            {
                "url": url,
                "status_code": response.status_code,
                "payload": payload,
            },
        )


def run_codegen_suite(host: str, interface_ids: List[str], results: List[Dict[str, Any]]) -> None:
    if "08" not in interface_ids:
        return
    output_dir = OUTPUT_DIR / "generated_project"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "source_protocol_dirs": [str(DATA_DIR / "source_protocols")],
        "target_protocol_dir": str(DATA_DIR / "target_protocols"),
        "conversion_rules_json": str(DATA_DIR / "rules" / "07_protocol_generate_rules.json"),
        "conversion_matrix_json": str(DATA_DIR / "conversion_matrix.json"),
        "port_config_json": str(DATA_DIR / "port_config.json"),
        "output_dir": str(output_dir),
        "project_name": "temp_sensor_codegen_smoke",
    }
    url = f"{_base_url(host, INTERFACES['08']['port'])}/api/code_generation/generate"
    response = _request("POST", url, json=payload)
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text}
    generated_manifest = output_dir / "protocol_manifest.json"
    passed = response.status_code == 200 and generated_manifest.exists()
    _record(
        results,
        "codegen:08",
        passed,
        {
            "url": url,
            "status_code": response.status_code,
            "response": body,
            "output_dir": str(output_dir),
            "manifest_exists": generated_manifest.exists(),
        },
    )


def run_rule_eval_suite(host: str, interface_ids: List[str], results: List[Dict[str, Any]]) -> None:
    if "10" not in interface_ids:
        return
    payload = {
        "source_protocol_dirs": [str(DATA_DIR / "source_protocols")],
        "target_protocol_dir": str(DATA_DIR / "target_protocols"),
        "conversion_rules": str(DATA_DIR / "rules" / "07_protocol_generate_rules.json"),
        "coarse_top_k": 5,
        "coarse_similarity_threshold": 0.1,
        "fine_similarity_threshold": 0.1,
        "use_model_inference": False,
    }
    url = f"{_base_url(host, INTERFACES['10']['port'])}/api/knowledge/rule_evaluate"
    response = _request("POST", url, json=payload)
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text}
    passed = response.status_code == 200 and isinstance(body.get("data"), dict)
    _record(
        results,
        "rule-eval:10",
        passed,
        {
            "url": url,
            "status_code": response.status_code,
            "response": body,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="interface_projects 部署后冒烟测试")
    parser.add_argument("--host", default="127.0.0.1", help="服务主机，默认 127.0.0.1")
    parser.add_argument("--interfaces", default=None, help="接口列表，逗号分隔，例如 08,10")
    parser.add_argument(
        "--suites",
        default="health,contract,codegen,rule-eval",
        help="测试套件，逗号分隔，可选 health,contract,codegen,rule-eval",
    )
    args = parser.parse_args()

    interface_ids = _normalize_interfaces(args.interfaces)
    suites = _normalize_suites(args.suites)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    results: List[Dict[str, Any]] = []

    try:
        if "health" in suites:
            run_health_suite(args.host, interface_ids, results)
        if "contract" in suites:
            run_contract_suite(args.host, interface_ids, results)
        if "codegen" in suites:
            run_codegen_suite(args.host, interface_ids, results)
        if "rule-eval" in suites:
            run_rule_eval_suite(args.host, interface_ids, results)
    except requests.RequestException as exc:
        _record(results, "request-error", False, {"error": str(exc)})
    except Exception as exc:  # pragma: no cover - script level fallback
        _record(results, "unexpected-error", False, {"error": str(exc)})

    passed = all(item["passed"] for item in results) if results else False
    report = {
        "host": args.host,
        "interfaces": interface_ids,
        "suites": suites,
        "passed": passed,
        "results": results,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
