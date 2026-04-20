import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import torch
from transformers import AutoModelForCausalLM, AutoModelForSequenceClassification, AutoTokenizer


DEFAULT_RERANK_INSTRUCTION = (
    "Given a protocol target field, retrieve relevant source fields that can correctly convert to the target field"
)
DEFAULT_SYSTEM_PROMPT = (
    'Judge whether the Document meets the requirements based on the Query and the Instruct provided. '
    'Note that the answer can only be "yes" or "no".'
)


class InvalidRerankerModelError(RuntimeError):
    """Raised when the configured reranker model directory is not a reranker."""


def _read_readme_text(directory: Path) -> str:
    readme_path = directory / "README.md"
    if not readme_path.exists():
        return ""
    try:
        return readme_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def inspect_reranker_model_dir(model_dir: Union[str, Path]) -> Dict[str, Any]:
    """Inspects local files and returns whether the directory is reranker-compatible."""

    directory = Path(model_dir).expanduser().resolve()
    config_path = directory / "config.json"
    if not config_path.exists():
        return {
            "compatible": False,
            "reason": f"缺少 config.json: {config_path}",
            "architectures": [],
            "model_type": None,
            "loader_type": None,
        }

    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "compatible": False,
            "reason": f"读取 config.json 失败: {exc}",
            "architectures": [],
            "model_type": None,
            "loader_type": None,
        }

    architectures = [
        str(item).strip()
        for item in (config.get("architectures") or [])
        if str(item).strip()
    ]
    model_type = str(config.get("model_type") or "").strip() or None
    readme_text = _read_readme_text(directory).lower()
    rerank_signal = any(
        token in readme_text
        for token in ("text reranking", "text-ranking", "qwen3-reranker", "rerank")
    ) or "reranker" in directory.name.lower()

    if any("SequenceClassification" in item for item in architectures):
        return {
            "compatible": True,
            "reason": None,
            "architectures": architectures,
            "model_type": model_type,
            "loader_type": "sequence_classification",
        }
    if model_type and "sequence" in model_type.lower():
        return {
            "compatible": True,
            "reason": None,
            "architectures": architectures,
            "model_type": model_type,
            "loader_type": "sequence_classification",
        }
    if any("CausalLM" in item for item in architectures) and rerank_signal:
        return {
            "compatible": True,
            "reason": None,
            "architectures": architectures,
            "model_type": model_type,
            "loader_type": "causal_lm_reranker",
        }
    if any("CausalLM" in item for item in architectures):
        return {
            "compatible": False,
            "reason": (
                "当前模型是因果语言模型，但未检测到 reranker 专用标识: "
                f"architectures={architectures}, dir={directory.name}"
            ),
            "architectures": architectures,
            "model_type": model_type,
            "loader_type": None,
        }
    return {
        "compatible": False,
        "reason": (
            "当前模型缺少可识别的 reranker 架构: "
            f"architectures={architectures or ['<empty>']}, model_type={model_type or '<empty>'}"
        ),
        "architectures": architectures,
        "model_type": model_type,
        "loader_type": None,
    }


class Qwen3Reranker:
    def __init__(
        self,
        model_dir: Optional[str] = None,
        gpu: Optional[str] = None,
        device: Optional[str] = None,
        fp16: bool = True,
    ):
        home_dir = os.path.expanduser("~")
        self.model_dir = model_dir or str(Path(home_dir) / "sxy/model_cache/Qwen3-Reranker-0___6B")
        if device:
            self.device = device
        elif gpu is not None:
            self.device = f"cuda:{gpu}"
        else:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

        inspection = inspect_reranker_model_dir(self.model_dir)
        if not inspection.get("compatible"):
            raise InvalidRerankerModelError(str(inspection.get("reason") or "reranker模型结构不兼容"))
        self.loader_type = str(inspection.get("loader_type") or "sequence_classification").strip()

        tokenizer_kwargs = {"trust_remote_code": True}
        if self.loader_type == "causal_lm_reranker":
            tokenizer_kwargs["padding_side"] = "left"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_dir, **tokenizer_kwargs)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        dtype = torch.float16 if fp16 and self.device.startswith("cuda") else torch.float32
        if self.loader_type == "causal_lm_reranker":
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_dir,
                trust_remote_code=True,
                torch_dtype=dtype,
                device_map=None,
            )
            self.token_false_id = self.tokenizer.convert_tokens_to_ids("no")
            self.token_true_id = self.tokenizer.convert_tokens_to_ids("yes")
            if self.token_false_id is None or self.token_true_id is None:
                raise InvalidRerankerModelError("reranker 模型缺少 yes/no 打分 token")
            prefix = (
                "<|im_start|>system\n"
                f"{DEFAULT_SYSTEM_PROMPT}"
                "<|im_end|>\n<|im_start|>user\n"
            )
            suffix = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
            self.prefix_tokens = self.tokenizer.encode(prefix, add_special_tokens=False)
            self.suffix_tokens = self.tokenizer.encode(suffix, add_special_tokens=False)
            self.max_model_length = min(int(getattr(self.tokenizer, "model_max_length", 8192) or 8192), 8192)
        else:
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_dir,
                trust_remote_code=True,
                torch_dtype=dtype,
                device_map=None,
            )
            self.model.config.pad_token_id = self.tokenizer.pad_token_id
        self.model = self.model.to(self.device)
        self.model.eval()

    def _format_instruction(self, instruction: Optional[str], query: str, doc: str) -> str:
        task_instruction = instruction or DEFAULT_RERANK_INSTRUCTION
        return f"<Instruct>: {task_instruction}\n<Query>: {query}\n<Document>: {doc}"

    def _process_causal_inputs(
        self,
        pairs: Sequence[str],
        max_length: int,
    ) -> Dict[str, torch.Tensor]:
        body_max_length = max(max_length - len(self.prefix_tokens) - len(self.suffix_tokens), 16)
        inputs = self.tokenizer(
            list(pairs),
            padding=False,
            truncation="longest_first",
            return_attention_mask=False,
            max_length=body_max_length,
        )
        for index, item in enumerate(inputs["input_ids"]):
            inputs["input_ids"][index] = self.prefix_tokens + item + self.suffix_tokens
        padded = self.tokenizer.pad(inputs, padding=True, return_tensors="pt", max_length=max_length)
        return {key: value.to(self.model.device) for key, value in padded.items()}

    @torch.no_grad()
    def rerank_pairs(
        self,
        query: str,
        docs: Sequence[Dict[str, Any]],
        *,
        top_k: Optional[int] = 5,
        max_length: int = 512,
        text_key: str = "content",
        instruction: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not docs:
            return []

        if self.loader_type == "causal_lm_reranker":
            pairs = []
            for item in docs:
                text = item.get(text_key, "")
                if not isinstance(text, str) or not text.strip():
                    text = ""
                pairs.append(self._format_instruction(instruction, query, text))
            inputs = self._process_causal_inputs(pairs, min(max_length, self.max_model_length))
            logits = self.model(**inputs).logits[:, -1, :]
            true_logits = logits[:, self.token_true_id]
            false_logits = logits[:, self.token_false_id]
            stacked = torch.stack([false_logits, true_logits], dim=1)
            log_probs = torch.nn.functional.log_softmax(stacked, dim=1)
            probabilities = log_probs[:, 1].exp().detach().cpu().tolist()
            raw_scores = [math.log(max(min(prob, 1.0 - 1e-6), 1e-6) / max(1.0 - min(max(prob, 1e-6), 1.0 - 1e-6), 1e-6)) for prob in probabilities]
        else:
            pairs = []
            for item in docs:
                text = item.get(text_key, "")
                if not isinstance(text, str) or not text.strip():
                    text = ""
                pairs.append((query, text))
            inputs = self.tokenizer(
                pairs,
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            ).to(self.model.device)
            outputs = self.model(**inputs)
            raw_scores = outputs.logits.squeeze(-1).float().cpu().tolist()
            probabilities = [1.0 / (1.0 + math.exp(-float(score))) for score in raw_scores]

        reranked: List[Dict[str, Any]] = []
        for raw_score, probability, item in zip(raw_scores, probabilities, docs):
            normalized_score = raw_score[0] if isinstance(raw_score, list) else raw_score
            ranked_item = dict(item)
            ranked_item["rerank_score"] = float(normalized_score)
            ranked_item["rerank_probability"] = float(probability)
            reranked.append(ranked_item)

        reranked.sort(key=lambda item: float(item["rerank_score"]), reverse=True)
        if top_k is not None:
            reranked = reranked[:top_k]
        return reranked

    @torch.no_grad()
    def rerank_milvus(
        self,
        query: str,
        milvus_results: Any,
        *,
        top_k: int = 5,
        max_length: int = 512,
        content_field: str = "content",
        source_field: str = "source",
        instruction: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        hits = milvus_results[0]
        docs: List[Dict[str, Any]] = []
        for hit in hits:
            entity = getattr(hit, "entity", None)
            if entity is None:
                continue
            docs.append(
                {
                    "content": entity.get(content_field),
                    "source": entity.get(source_field),
                    "vector_score": float(getattr(hit, "score", 0.0)),
                }
            )
        return self.rerank_pairs(
            query,
            docs,
            top_k=top_k,
            max_length=max_length,
            text_key="content",
            instruction=instruction,
        )
