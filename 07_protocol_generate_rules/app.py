from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

from flask import Flask, jsonify


PROJECT_DIR = Path(__file__).resolve().parent
LOCAL_API_DIR = PROJECT_DIR / "api_03_extract_validate"
LOCAL_APP_PATH = LOCAL_API_DIR / "app.py"


def _purge_local_conflicts() -> None:
    for name in list(sys.modules):
        if (
            name == "shared"
            or name.startswith("shared.")
            or name == "api_03_extract_validate"
            or name.startswith("api_03_extract_validate.")
            or name == "project_generator"
            or name.startswith("project_generator.")
        ):
            sys.modules.pop(name, None)


def _install_database_stubs_if_needed() -> None:
    try:
        import sqlite3  # noqa: F401
        return
    except Exception:
        pass

    fake_mysql_module = types.ModuleType("shared.database.mysql_client")
    fake_models_module = types.ModuleType("shared.database.models")

    class FakeMySQLClient:
        pass

    class FakeQAPair:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def to_dict(self):
            return dict(self.__dict__)

    fake_mysql_module.MySQLClient = FakeMySQLClient
    fake_models_module.QAPair = FakeQAPair
    sys.modules["shared.database.mysql_client"] = fake_mysql_module
    sys.modules["shared.database.models"] = fake_models_module


def _load_local_api_module():
    _purge_local_conflicts()
    _install_database_stubs_if_needed()
    for path in (str(PROJECT_DIR), str(LOCAL_API_DIR)):
        if path not in sys.path:
            sys.path.insert(0, path)
    spec = importlib.util.spec_from_file_location(
        "interface_project_07_local_api",
        LOCAL_APP_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {LOCAL_APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_local_api_module = _load_local_api_module()
impl_protocol_generate_rules = _local_api_module.protocol_generate_rules
impl_protocol_rules_manual_writeback = _local_api_module.protocol_rules_manual_writeback

app = Flask(__name__)


@app.route("/api/knowledge/protocol_generate_rules", methods=["POST"])
def protocol_generate_rules():
    return impl_protocol_generate_rules()


@app.route("/api/knowledge/protocol_rules/manual_writeback", methods=["POST"])
def protocol_rules_manual_writeback():
    return impl_protocol_rules_manual_writeback()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "project": "07_protocol_generate_rules"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6107, debug=True)
