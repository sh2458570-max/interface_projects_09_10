import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class Qwen3Reranker:
    def __init__(
        self,
        model_dir: Optional[str] = None,
        gpu: Optional[str] = None,
        device: Optional[str] = None,
        fp16: bool = True,
    ):
        home_dir = os.path.expanduser("~")
        self.model_dir = model_dir or str(
            Path(home_dir) / "sxy/model_cache/Qwen3-Reranker-0___6B"
        )
        if device:
            self.device = device
        elif gpu is not None:
            self.device = f"cuda:{gpu}"
        else:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_dir, trust_remote_code=True
        )
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        dtype = torch.float16 if fp16 else torch.float32
        self.model = AutoModelForSequenceClassification.from_pretrained(
            self.model_dir,
            trust_remote_code=True,
            torch_dtype=dtype,
            device_map=None,
        )
        self.model = self.model.to(self.device)
        self.model.config.pad_token_id = self.tokenizer.pad_token_id
        self.model.eval()

    @torch.no_grad()
    def rerank_pairs(
        self,
        query: str,
        docs: Sequence[Dict[str, Any]],
        *,
        top_k: Optional[int] = 5,
        max_length: int = 512,
        text_key: str = "content",
    ) -> List[Dict[str, Any]]:
        """
        通用 rerank：输入 docs=[{"content":..., "source":..., ...}, ...]
        返回：每条多一个 "rerank_score"，并按 rerank_score 降序排序
        """
        if not docs:
            return []

        pairs = []
        for d in docs:
            text = d.get(text_key, "")
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
        scores = outputs.logits.squeeze(-1).float().cpu().tolist()

        reranked: List[Dict[str, Any]] = []
        for s, d in zip(scores, docs):
            if isinstance(s, list):
                while isinstance(s, list) and s:
                    s = s[0]
            item = dict(d)
            item["rerank_score"] = float(s)
            reranked.append(item)

        reranked.sort(key=lambda x: x["rerank_score"], reverse=True)
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
    ) -> List[Dict[str, Any]]:
        """
        专门处理 Milvus collection.search 的返回（results）
        要求 search 时 output_fields 至少包含 content/source
        """
        hits = milvus_results[0]  # 标准：results[0] 是 hits
        docs: List[Dict[str, Any]] = []

        for hit in hits:
            entity = getattr(hit, "entity", None)
            if entity is None:
                continue
            content = entity.get(content_field)
            source = entity.get(source_field)
            docs.append(
                {
                    "content": content,
                    "source": source,
                    "vector_score": float(getattr(hit, "score", 0.0)),
                }
            )

        return self.rerank_pairs(
            query, docs, top_k=top_k, max_length=max_length, text_key="content"
        )
