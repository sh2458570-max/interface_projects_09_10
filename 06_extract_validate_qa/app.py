from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from flask import Flask, jsonify


PROJECT_DIR = Path(__file__).resolve().parent
LOCAL_API_DIR = PROJECT_DIR / "api_03_extract_validate"
LOCAL_APP_PATH = LOCAL_API_DIR / "app.py"


def _purge_local_conflicts() -> None:
    for name in list(sys.modules):
        if name == "shared" or name.startswith("shared.") or name == "api_03_extract_validate" or name.startswith("api_03_extract_validate."):
            sys.modules.pop(name, None)


def _load_local_extract_validate_qa():
    _purge_local_conflicts()
    for path in (str(PROJECT_DIR), str(LOCAL_API_DIR)):
        if path not in sys.path:
            sys.path.insert(0, path)
    spec = importlib.util.spec_from_file_location(
        "interface_project_06_local_api",
        LOCAL_APP_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {LOCAL_APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.extract_validate_qa


impl_extract_validate_qa = _load_local_extract_validate_qa()


app = Flask(__name__)


@app.route("/api/knowledge/extract_validate_qa", methods=["POST"])
def extract_validate_qa():
    return impl_extract_validate_qa()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "project": "06_extract_validate_qa"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6106, debug=True)
