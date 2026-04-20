from __future__ import annotations

import math
import os
import uuid
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import torch
import torch.nn.functional as F

from shared import config
from shared.retrieval.reranker import (
    InvalidRerankerModelError,
    Qwen3Reranker,
    inspect_reranker_model_dir,
)

from .converter import normalize_source_message
from .knowledge_base import ProtocolConversionKnowledgeBase


DEFAULT_EMBED_REPO = os.getenv("EMBED_MODEL_REPO", "Qwen/Qwen3-Embedding-0.6B")
DEFAULT_RERANK_REPO = os.getenv("RERANK_MODEL_REPO", "Qwen/Qwen3-Reranker-0.6B")


@dataclass
class ModelLoadResult:
    backend_name: str
    available: bool
    model_dir: Optional[str]
    downloaded: bool
    reason: Optional[str] = None


class EmbeddingEncoder:
    """Lightweight embedding encoder used for conversion evaluation."""

    def __init__(self, model_dir: str):
        from transformers import AutoModel, AutoTokenizer

        self.model_dir = model_dir
        self.tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=True)
        requested_device = os.getenv("EMBED_DEVICE", "").strip()
        self.device = requested_device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.model = AutoModel.from_pretrained(
            model_dir,
            trust_remote_code=True,
            torch_dtype=torch.float16 if self.device.startswith("cuda") else torch.float32,
            device_map=None,
        )
        self.model = self.model.to(self.device)
        self.model.eval()

    @torch.no_grad()
    def encode(self, texts: Sequence[str], max_length: int = 512) -> torch.Tensor:
        inputs = self.tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        ).to(self.model.device)
        outputs = self.model(**inputs)
        attention_mask = inputs["attention_mask"].unsqueeze(-1)
        hidden = outputs.last_hidden_state * attention_mask
        pooled = hidden.sum(dim=1) / attention_mask.sum(dim=1).clamp(min=1)
        return F.normalize(pooled, dim=1).float().cpu()


@dataclass
class EvaluationBackend:
    embedding: Optional[EmbeddingEncoder]
    embedding_info: ModelLoadResult
    reranker: Optional[Qwen3Reranker]
    reranker_info: ModelLoadResult


def _candidate_paths(model_kind: str) -> List[Path]:
    home = Path.home()
    configured_root = Path(os.path.expanduser(config.MODEL_CACHE_DIR))
    env_dir = os.getenv(f"{model_kind.upper()}_MODEL_DIR")
    paths: List[Path] = []
    if env_dir:
        paths.append(Path(os.path.expanduser(env_dir)))
    if model_kind == "embed":
        paths.extend(
            [
                configured_root / "Qwen" / "Qwen3-Embedding-0.6B",
                configured_root / "Qwen3-Embedding-0.6B",
                home / "sxy" / "model_cache" / "Qwen" / "Qwen3-Embedding-0___6B",
                home / "model_cache" / "Qwen" / "Qwen3-Embedding-0.6B",
            ]
        )
    else:
        paths.extend(
            [
                configured_root / "Qwen" / "Qwen3-Reranker-0.6B",
                configured_root / "Qwen3-Reranker-0.6B",
                home / "sxy" / "model_cache" / "Qwen3-Reranker-0___6B",
                home / "model_cache" / "Qwen3-Reranker-0.6B",
            ]
        )
    deduped: List[Path] = []
    seen = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _existing_model_dir(model_kind: str) -> Optional[Path]:
    for candidate in _candidate_paths(model_kind):
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def _download_from_modelscope(repo_id: str, cache_dir: Path) -> Tuple[Optional[str], Optional[str]]:
    try:
        from modelscope import snapshot_download
    except Exception as exc:
        return None, f"modelscope不可用: {exc}"
    try:
        model_dir = snapshot_download(repo_id, cache_dir=str(cache_dir))
        return model_dir, None
    except Exception as exc:
        return None, f"ModelScope下载失败: {exc}"


def _load_embedding_backend(allow_modelscope_download: bool) -> Tuple[Optional[EmbeddingEncoder], ModelLoadResult]:
    existing = _existing_model_dir("embed")
    if existing is not None:
        try:
            encoder = EmbeddingEncoder(str(existing))
            return encoder, ModelLoadResult("qwen3_embedding", True, str(existing), False)
        except Exception as exc:
            load_error = f"本地embedding模型加载失败: {exc}"
        else:
            load_error = None
    else:
        load_error = "未找到本地embedding模型目录"

    if allow_modelscope_download:
        cache_dir = Path(os.path.expanduser(config.MODEL_CACHE_DIR)) / "modelscope"
        model_dir, error = _download_from_modelscope(DEFAULT_EMBED_REPO, cache_dir)
        if model_dir:
            try:
                encoder = EmbeddingEncoder(model_dir)
                return encoder, ModelLoadResult("qwen3_embedding", True, model_dir, True)
            except Exception as exc:
                load_error = f"ModelScope embedding 模型加载失败: {exc}"
        elif error:
            load_error = error
    return None, ModelLoadResult("fallback_text_similarity", False, None, False, load_error)


def _load_reranker_backend(allow_modelscope_download: bool) -> Tuple[Optional[Qwen3Reranker], ModelLoadResult]:
    requested_device = os.getenv("RERANK_DEVICE", "").strip()
    requested_gpu = os.getenv("RERANK_GPU", "").strip()
    existing = _existing_model_dir("rerank")
    if existing is not None:
        inspection = inspect_reranker_model_dir(existing)
        if not inspection.get("compatible"):
            load_error = str(inspection.get("reason") or "本地reranker模型结构不兼容")
            return None, ModelLoadResult("fallback_text_similarity", False, str(existing), False, load_error)
        try:
            reranker = Qwen3Reranker(
                model_dir=str(existing),
                gpu=requested_gpu or None,
                device=requested_device or None,
                fp16=torch.cuda.is_available(),
            )
            return reranker, ModelLoadResult("qwen3_reranker", True, str(existing), False)
        except InvalidRerankerModelError as exc:
            load_error = f"本地reranker模型结构不兼容: {exc}"
        except Exception as exc:
            load_error = f"本地reranker模型加载失败: {exc}"
        else:
            load_error = None
    else:
        load_error = "未找到本地reranker模型目录"

    if allow_modelscope_download:
        cache_dir = Path(os.path.expanduser(config.MODEL_CACHE_DIR)) / "modelscope"
        model_dir, error = _download_from_modelscope(DEFAULT_RERANK_REPO, cache_dir)
        if model_dir:
            inspection = inspect_reranker_model_dir(model_dir)
            if not inspection.get("compatible"):
                load_error = str(inspection.get("reason") or "ModelScope reranker 模型结构不兼容")
                return None, ModelLoadResult("fallback_text_similarity", False, str(model_dir), True, load_error)
            try:
                reranker = Qwen3Reranker(
                    model_dir=str(model_dir),
                    gpu=requested_gpu or None,
                    device=requested_device or None,
                    fp16=torch.cuda.is_available(),
                )
                return reranker, ModelLoadResult("qwen3_reranker", True, str(model_dir), True)
            except InvalidRerankerModelError as exc:
                load_error = f"ModelScope reranker 模型结构不兼容: {exc}"
            except Exception as exc:
                load_error = f"ModelScope reranker 模型加载失败: {exc}"
        elif error:
            load_error = error
    return None, ModelLoadResult("fallback_text_similarity", False, None, False, load_error)


def _load_backends(use_model_inference: bool, allow_modelscope_download: bool) -> EvaluationBackend:
    if not use_model_inference:
        disabled = ModelLoadResult("fallback_text_similarity", False, None, False, "已禁用模型推理")
        return EvaluationBackend(None, disabled, None, disabled)
    embedding, embedding_info = _load_embedding_backend(allow_modelscope_download)
    reranker, reranker_info = _load_reranker_backend(allow_modelscope_download)
    return EvaluationBackend(embedding, embedding_info, reranker, reranker_info)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value).strip()


def _field_text(field_name: str, value: Any) -> str:
    return f"{field_name}={_normalize_text(value)}"


def _safe_float(value: Any) -> Optional[float]:
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except Exception:
        return None


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def _numeric_similarity(left: Any, right: Any) -> Optional[float]:
    left_num = _safe_float(left)
    right_num = _safe_float(right)
    if left_num is None or right_num is None:
        return None
    denominator = max(abs(right_num), 1.0)
    relative_error = abs(left_num - right_num) / denominator
    return max(0.0, 1.0 - min(relative_error, 1.0))


def _fallback_similarity(left_text: str, right_text: str) -> float:
    if not left_text and not right_text:
        return 1.0
    return SequenceMatcher(None, left_text, right_text).ratio()


def _cosine_similarity(embedding: EmbeddingEncoder, left_text: str, right_text: str) -> float:
    vectors = embedding.encode([left_text, right_text])
    return float(torch.matmul(vectors[0], vectors[1]).item())


def _normalize_cosine(score: float) -> float:
    return max(0.0, min(1.0, (score + 1.0) / 2.0))


def _rerank_similarity(reranker: Qwen3Reranker, left_text: str, right_text: str) -> float:
    results = reranker.rerank_pairs(left_text, [{"content": right_text}], top_k=1, text_key="content")
    if not results:
        return 0.0
    first = results[0]
    probability = first.get("rerank_probability")
    if probability is not None:
        try:
            return max(0.0, min(1.0, float(probability)))
        except Exception:
            pass
    return float(_sigmoid(float(first.get("rerank_score", 0.0))))


def _metadata_map(protocol_type: str, message_code: Optional[str]) -> Dict[str, Dict[str, Any]]:
    knowledge_base = ProtocolConversionKnowledgeBase.load(protocol_type)
    metadata: Dict[str, Dict[str, Any]] = {}
    for rule in knowledge_base.list_rules(message_code=message_code):
        payload = {
            "unit": rule.unit,
            "source_field": rule.field_name,
            "target_field": rule.target_field or rule.field_name,
            "bit_length": rule.bit_length,
            "formula": rule.formula,
        }
        metadata[rule.field_name] = payload
        if rule.target_field:
            metadata[rule.target_field] = payload
    return metadata


def _expected_fields(reference_message: Dict[str, Any], field_weights: Optional[Dict[str, Any]]) -> List[str]:
    if field_weights:
        weighted_fields = [str(key).strip().upper() for key in field_weights if str(key).strip()]
        if weighted_fields:
            return weighted_fields
    return list(reference_message.keys())


def evaluate_protocol_conversion(
    converted_message: Any,
    reference_message: Any,
    protocol_type: str = "Link16",
    message_code: Optional[str] = None,
    source_message: Optional[Any] = None,
    field_weights: Optional[Dict[str, Any]] = None,
    trace_id: Optional[str] = None,
    use_model_inference: bool = True,
    allow_modelscope_download: bool = True,
) -> Dict[str, Any]:
    """Evaluate protocol conversion correctness with embedding/reranker or fallback scoring."""
    normalized_converted = normalize_source_message(converted_message)
    normalized_reference = normalize_source_message(reference_message)
    normalized_source = normalize_source_message(source_message or {})
    if not normalized_reference:
        raise ValueError("reference_message不能为空")

    evaluation_trace_id = str(trace_id or uuid.uuid4())
    field_weights = {str(key).strip().upper(): float(value) for key, value in (field_weights or {}).items() if str(key).strip()}
    expected_fields = _expected_fields(normalized_reference, field_weights)
    backend = _load_backends(use_model_inference=use_model_inference, allow_modelscope_download=allow_modelscope_download)
    metadata = _metadata_map(protocol_type, message_code)

    field_scores: List[Dict[str, Any]] = []
    total_weight = 0.0
    weighted_correctness = 0.0
    weighted_semantic = 0.0
    weighted_rerank = 0.0
    weighted_loss = 0.0
    matched_fields = 0

    for field_name in expected_fields:
        reference_value = normalized_reference.get(field_name)
        converted_value = normalized_converted.get(field_name)
        field_weight = field_weights.get(field_name, 1.0)
        total_weight += field_weight

        present = converted_value is not None
        presence_score = 1.0 if present else 0.0
        exact_match = present and _normalize_text(converted_value).upper() == _normalize_text(reference_value).upper()
        exact_score = 1.0 if exact_match else 0.0
        numeric_similarity = _numeric_similarity(converted_value, reference_value) if present else None

        reference_text = _field_text(field_name, reference_value)
        converted_text = _field_text(field_name, converted_value) if present else f"{field_name}=<missing>"
        if backend.embedding is not None:
            semantic_similarity = _normalize_cosine(_cosine_similarity(backend.embedding, converted_text, reference_text))
            semantic_source = "embedding"
        else:
            semantic_similarity = _fallback_similarity(converted_text, reference_text)
            semantic_source = "fallback"
        if numeric_similarity is not None:
            semantic_similarity = (semantic_similarity * 0.4) + (numeric_similarity * 0.6)

        if backend.reranker is not None and present:
            rerank_score = _rerank_similarity(backend.reranker, converted_text, reference_text)
            rerank_source = "reranker"
        else:
            rerank_score = semantic_similarity
            rerank_source = "fallback"

        field_correctness = (
            0.20 * presence_score
            + 0.20 * exact_score
            + 0.35 * semantic_similarity
            + 0.25 * rerank_score
        )
        field_loss = 1.0 - field_correctness
        if numeric_similarity is not None:
            field_loss = max(field_loss, 1.0 - numeric_similarity)
        if not present:
            field_loss = 1.0

        if exact_match:
            matched_fields += 1
        weighted_correctness += field_correctness * field_weight
        weighted_semantic += semantic_similarity * field_weight
        weighted_rerank += rerank_score * field_weight
        weighted_loss += field_loss * field_weight

        field_scores.append(
            {
                "field_name": field_name,
                "source_field": metadata.get(field_name, {}).get("source_field"),
                "target_field": metadata.get(field_name, {}).get("target_field", field_name),
                "reference_value": reference_value,
                "converted_value": converted_value,
                "source_value": normalized_source.get(metadata.get(field_name, {}).get("source_field", field_name)),
                "weight": field_weight,
                "present": present,
                "exact_match": exact_match,
                "presence_score": round(presence_score * 100, 4),
                "semantic_similarity": round(semantic_similarity * 100, 4),
                "rerank_score": round(rerank_score * 100, 4),
                "numeric_similarity": round((numeric_similarity or 0.0) * 100, 4) if numeric_similarity is not None else None,
                "field_correctness_score": round(field_correctness * 100, 4),
                "information_loss_score": round(field_loss * 100, 4),
                "unit": metadata.get(field_name, {}).get("unit"),
                "formula": metadata.get(field_name, {}).get("formula"),
                "scoring_backend": {
                    "semantic": semantic_source,
                    "rerank": rerank_source,
                },
            }
        )

    denominator = max(total_weight, 1.0)
    unexpected_fields = sorted(field for field in normalized_converted.keys() if field not in expected_fields)
    missing_fields = sorted(field for field in expected_fields if field not in normalized_converted)
    correctness_score = weighted_correctness / denominator * 100.0
    semantic_score = weighted_semantic / denominator * 100.0
    rerank_score = weighted_rerank / denominator * 100.0
    information_loss_score = weighted_loss / denominator * 100.0
    conversion_rate = matched_fields / max(len(expected_fields), 1) * 100.0

    return {
        "protocol_type": protocol_type,
        "message_code": str(message_code or "").strip().upper() or None,
        "trace_id": evaluation_trace_id,
        "embedding_model": config.EMBED_MODEL_NAME,
        "rerank_model": config.RERANK_MODEL_NAME,
        "normalized_source_message": normalized_source,
        "normalized_converted_message": normalized_converted,
        "normalized_reference_message": normalized_reference,
        "field_scores": field_scores,
        "correctness_score": round(correctness_score, 4),
        "semantic_similarity": round(semantic_score, 4),
        "rerank_score": round(rerank_score, 4),
        "information_loss_score": round(information_loss_score, 4),
        "conversion_rate": round(conversion_rate, 4),
        "summary": {
            "expected_field_count": len(expected_fields),
            "matched_field_count": matched_fields,
            "missing_field_count": len(missing_fields),
            "unexpected_field_count": len(unexpected_fields),
            "missing_fields": missing_fields,
            "unexpected_fields": unexpected_fields,
        },
        "strategy": {
            "model_inference_enabled": use_model_inference,
            "allow_modelscope_download": allow_modelscope_download,
            "embedding_backend": backend.embedding_info.__dict__,
            "reranker_backend": backend.reranker_info.__dict__,
            "degraded": not (backend.embedding_info.available and backend.reranker_info.available),
        },
    }
