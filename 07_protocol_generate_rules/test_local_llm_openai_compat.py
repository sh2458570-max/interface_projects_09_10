from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.llm.local_llm import LLMConfig, LocalLLM


def test_openai_base_url_with_v1_is_not_duplicated(monkeypatch):
    monkeypatch.setenv("openai_api_key", "local-qwen")
    monkeypatch.setenv("openai_base_url", "http://127.0.0.1:8000/v1")
    monkeypatch.setenv("openai_model", "qwen-local")
    monkeypatch.delenv("LLM_MODEL_NAME", raising=False)
    monkeypatch.delenv("USE_VLLM", raising=False)
    monkeypatch.delenv("VLLM_URL", raising=False)

    llm = LocalLLM(LLMConfig())

    assert llm.config.use_vllm is True
    assert llm.config.model_name == "qwen-local"
    assert llm.config.api_key == "local-qwen"
    assert llm._api_base_url == "http://127.0.0.1:8000/v1"
    assert llm._service_root_url == "http://127.0.0.1:8000"


def test_openai_base_url_without_v1_is_normalized_once(monkeypatch):
    monkeypatch.setenv("openai_base_url", "http://127.0.0.1:8000")
    monkeypatch.setenv("openai_model", "qwen-local")
    monkeypatch.delenv("USE_VLLM", raising=False)
    monkeypatch.delenv("VLLM_URL", raising=False)

    llm = LocalLLM(LLMConfig())

    assert llm.config.use_vllm is True
    assert llm._api_base_url == "http://127.0.0.1:8000/v1"
    assert llm._service_root_url == "http://127.0.0.1:8000"
