from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from flask import Flask, jsonify


PROJECT_DIR = Path(__file__).resolve().parent
LOCAL_API_DIR = PROJECT_DIR / "api_05_finetune"
LOCAL_APP_PATH = LOCAL_API_DIR / "app.py"


def _purge_local_conflicts() -> None:
    for name in list(sys.modules):
        if (
            name == "shared"
            or name.startswith("shared.")
            or name == "api_05_finetune"
            or name.startswith("api_05_finetune.")
            or name == "finetune_service"
        ):
            sys.modules.pop(name, None)


def _load_local_finetune_module():
    _purge_local_conflicts()
    for path in (str(PROJECT_DIR), str(LOCAL_API_DIR)):
        if path not in sys.path:
            sys.path.insert(0, path)
    spec = importlib.util.spec_from_file_location(
        "interface_project_09_local_api",
        LOCAL_APP_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {LOCAL_APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_local_module = _load_local_finetune_module()
impl_finetune_action = _local_module.finetune_action
impl_finetune_stream = _local_module.finetune_stream
impl_download_model = _local_module.download_model
impl_legacy_submit = _local_module.legacy_submit
impl_legacy_stream = _local_module.legacy_stream
impl_legacy_download = _local_module.legacy_download
impl_job_status = _local_module.job_status

app = Flask(__name__)


@app.route("/api/model/finetune/action", methods=["POST"])
def finetune_action():
    return impl_finetune_action()


@app.route("/api/model/finetune/stream", methods=["GET"])
def finetune_stream():
    return impl_finetune_stream()


@app.route("/api/model/finetune/model/download", methods=["GET"])
def download_model():
    return impl_download_model()


@app.route("/api/finetune/job/submit", methods=["POST"])
def legacy_submit():
    return impl_legacy_submit()


@app.route("/api/finetune/job/stream", methods=["GET"])
def legacy_stream():
    return impl_legacy_stream()


@app.route("/api/finetune/model/download", methods=["GET"])
def legacy_download():
    return impl_legacy_download()


@app.route("/api/finetune/job/status", methods=["GET"])
def job_status():
    return impl_job_status()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "project": "09_finetune_runtime"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6109, debug=True, threaded=True)
