# pipeline_demo.py
# RAG Two-stage retrieval pipeline
# 1) Embedding + Milvus recall
# 2) Reranker fine ranking

import os
from pathlib import Path

import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel, AutoModelForSequenceClassification
from pymilvus import connections, Collection


#  0) 配置 
HOME = os.path.expanduser("~")

EMBED_MODEL_DIR = str(Path(HOME) / "sxy/model_cache/Qwen/Qwen3-Embedding-0___6B")
RERANK_MODEL_DIR = str(Path(HOME) / "sxy/model_cache/Qwen3-Reranker-0___6B")

MILVUS_DB_PATH = "/home/hks/sxy/milvus_lite.db"
COLLECTION_NAME = "rag_pdf_6016d"

TOPK = 20
TOPN = 5
MAX_LEN = 512



# 1) Embedding 
@torch.no_grad()
def embed_mean_pool(tokenizer, model, texts, batch_size=32, max_length=512):
    vecs = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        inputs = tokenizer(
            batch,
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        ).to(model.device)

        outputs = model(**inputs)
        emb = outputs.last_hidden_state.mean(dim=1)
        emb = F.normalize(emb, dim=1).float().cpu()
        vecs.append(emb)

    return torch.cat(vecs, dim=0)


# 2) Rerank
@torch.no_grad()
def rerank_pairs(tokenizer, model, query: str, docs: list[str], max_length=512):
    pairs = [(query, d) for d in docs]

    inputs = tokenizer(
        pairs,
        padding=True,
        truncation=True,
        max_length=max_length,
        return_tensors="pt",
    ).to(model.device)

    outputs = model(**inputs)
    scores = outputs.logits.squeeze(-1).float().cpu().tolist()
    return scores


def _to_float_score(s):
    return s[0] if isinstance(s, list) else float(s)


# 全局初始化（接口模式） 
device = "cuda" if torch.cuda.is_available() else "cpu"

print("Loading embedding model:", EMBED_MODEL_DIR)
e_tok = AutoTokenizer.from_pretrained(EMBED_MODEL_DIR, trust_remote_code=True)
e_model = AutoModel.from_pretrained(
    EMBED_MODEL_DIR,
    trust_remote_code=True,
    torch_dtype=torch.float16 if device == "cuda" else torch.float32,
    device_map="cuda" if device == "cuda" else None,
)
if device != "cuda":
    e_model = e_model.to(device)
e_model.eval()

print("Loading reranker model:", RERANK_MODEL_DIR)
r_tok = AutoTokenizer.from_pretrained(RERANK_MODEL_DIR, trust_remote_code=True)
if r_tok.pad_token is None:
    r_tok.pad_token = r_tok.eos_token

r_model = AutoModelForSequenceClassification.from_pretrained(
    RERANK_MODEL_DIR,
    trust_remote_code=True,
    torch_dtype=torch.float16 if device == "cuda" else torch.float32,
    device_map="cuda" if device == "cuda" else None,
)
r_model.config.pad_token_id = r_tok.pad_token_id
if device != "cuda":
    r_model = r_model.to(device)
r_model.eval()

print("Connecting Milvus Lite:", MILVUS_DB_PATH)
connections.connect(alias="default", uri=MILVUS_DB_PATH)
col = Collection(COLLECTION_NAME)
col.load()


#  接口调用函数（ api_server ）
def run_pipeline(query: str, topk: int = TOPK, topn: int = TOPN):

    # 1向量化
    q_vec = embed_mean_pool(e_tok, e_model, [query], batch_size=1, max_length=MAX_LEN)
    q_vec_np = q_vec.numpy()

    # 2Milvus 搜索
    results = col.search(
        data=[q_vec_np[0]],
        anns_field="embedding",
        param={"metric_type": "COSINE"},
        limit=topk,
        output_fields=["content", "source", "chunk_id"],
    )

    hits = results[0]

    cand_docs = []
    cand_meta = []

    for hit in hits:
        content = hit.entity.get("content") or ""
        source = hit.entity.get("source")
        chunk_id = hit.entity.get("chunk_id")

        cand_docs.append(content)
        cand_meta.append({
            "source": source,
            "chunk_id": chunk_id,
            "vec_score": float(hit.score)
        })

    # 3rerank
    rerank_sc = rerank_pairs(r_tok, r_model, query, cand_docs, max_length=MAX_LEN)
    order = sorted(
        range(len(rerank_sc)),
        key=lambda i: _to_float_score(rerank_sc[i]),
        reverse=True
    )

    # 4 输出
    outputs = []
    for i in order[:topn]:
        outputs.append({
            "content": cand_docs[i],
            "source": cand_meta[i]["source"],
            "chunk_id": cand_meta[i]["chunk_id"],
            "vec_score": cand_meta[i]["vec_score"],
            "rerank_score": _to_float_score(rerank_sc[i])
        })

    return {
        "query": query,
        "results": outputs
    }


#本地测试模式
def main():
    while True:
        query = input("\n请输入 query（q 退出）：")
        if query.lower() == "q":
            break

        result = run_pipeline(query)

        for i, r in enumerate(result["results"], 1):
            print(f"\n[{i}] source={r['source']} chunk={r['chunk_id']}")
            print(r["content"][:200])


if __name__ == "__main__":
    main()
