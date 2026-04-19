from __future__ import annotations

import sys
from pathlib import Path

from flask import Flask, jsonify, request


PROJECT_DIR = Path(__file__).resolve().parent


def _purge_local_conflicts() -> None:
    for name in list(sys.modules):
        if name == "shared" or name.startswith("shared."):
            sys.modules.pop(name, None)


_purge_local_conflicts()
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from shared.protocol_conversion import evaluate_protocol_rules


impl_evaluate_protocol_rules = evaluate_protocol_rules

app = Flask(__name__)


@app.route("/api/knowledge/rule_evaluate", methods=["POST"])
def rule_evaluate():
    try:
        data = request.json
        if not isinstance(data, dict):
            return jsonify({"code": 400, "message": "请求体必须是JSON对象", "data": None}), 400

        source_protocol_dirs = data.get("source_protocol_dirs")
        if source_protocol_dirs is None:
            source_protocol_dirs = data.get("source_protocol_dir")
        target_protocol_dir = data.get("target_protocol_dir")
        conversion_rules = data.get("conversion_rules")

        if not source_protocol_dirs:
            return jsonify({"code": 400, "message": "source_protocol_dirs不能为空", "data": None}), 400
        if not target_protocol_dir:
            return jsonify({"code": 400, "message": "target_protocol_dir不能为空", "data": None}), 400
        if conversion_rules is None:
            return jsonify({"code": 400, "message": "conversion_rules不能为空", "data": None}), 400

        result = impl_evaluate_protocol_rules(
            source_protocol_dirs=source_protocol_dirs,
            target_protocol_dir=target_protocol_dir,
            conversion_rules=conversion_rules,
            coarse_top_k=int(data.get("coarse_top_k", 10)),
            coarse_similarity_threshold=float(data.get("coarse_similarity_threshold", 0.55)),
            fine_similarity_threshold=float(data.get("fine_similarity_threshold", 0.75)),
            use_model_inference=True if data.get("use_model_inference") is None else bool(data.get("use_model_inference")),
            allow_modelscope_download=False,
            trace_id=data.get("trace_id"),
        )
        return jsonify({"code": 200, "message": "success", "data": result})
    except FileNotFoundError as exc:
        return jsonify({"code": 404, "message": f"评估模型文件不存在: {str(exc)}", "data": None}), 404
    except ValueError as exc:
        return jsonify({"code": 400, "message": str(exc), "data": None}), 400
    except Exception as exc:
        return jsonify({"code": 500, "message": f"规则级评估失败: {str(exc)}", "data": None}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "project": "10_rule_evaluate"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6110, debug=True, threaded=True)
