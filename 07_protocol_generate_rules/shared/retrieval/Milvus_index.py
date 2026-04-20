# Milvus_index.py
# Build Milvus Lite collection for MIL-STD-6016D PDF using Qwen3-Embedding-0.6B
# Improvements:
# 1) Clean noisy "bit index number lines" (e.g., "69 68 67 ...")
# 2) Split by "WORD NUMBER:" blocks first, then (optional) secondary split via DocumentSplitter

import os
import re
from pathlib import Path

import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel

import pdfplumber
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

# your splitter.py
from splitter import DocumentSplitter


# ================== 0) 配置 ==================
HOME = os.path.expanduser("~")

# Embedding model dir (make sure this path exists)
EMBED_MODEL_DIR = str(Path(HOME) / "sxy/model_cache/Qwen/Qwen3-Embedding-0___6B")

# PDF path (relative or absolute)
PDF_PATH = "data/MIL-STD-6016D(无水印)(1)-2119-8838.pdf"

# Milvus Lite DB path (local file)
MILVUS_DB_PATH = os.path.join(os.getcwd(), "milvus_lite.db")

# Collection name
COLLECTION_NAME = "rag_pdf_6016d"

# Use which GPU (optional)
os.environ["CUDA_VISIBLE_DEVICES"] = "3"

# Chunk filtering thresholds
MIN_BLOCK_CHARS = 200      # blocks shorter than this are dropped
SECOND_SPLIT = True        # keep using DocumentSplitter for secondary splitting
EMBED_BATCH = 8            # smaller is safer for GPU memory
MAX_LEN = 512
# =============================================


def load_pdf_text(pdf_path: str) -> str:
    """Extract text from PDF by pages and concatenate."""
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            try:
                # a bit more robust than default
                t = page.extract_text(x_tolerance=2, y_tolerance=2)
                if t and t.strip():
                    pages.append(t)
            except Exception as e:
                print(f"[WARN] Skip page {i}: {e}")
    return "\n".join(pages)


def clean_lines(s: str) -> str:
    """
    Remove lines that harm semantic search:
    - pure numeric/symbol lines (e.g., "1 : 3 : 16 :" or "----")
    - long bit-index rows (e.g., "69 68 67 ... 50")
    """
    out = []
    for line in s.splitlines():
        t = line.strip()
        if not t:
            continue

        # 1) only digits / colons / dashes / spaces
        if re.fullmatch(r"[\d:\-\s]+", t):
            continue

        # 2) long sequences of numbers separated by spaces
        # e.g. "69 68 67 66 ... 50"
        if len(t) > 30 and re.fullmatch(r"(\d+\s+){10,}\d+", t):
            continue

        out.append(t)
    return "\n".join(out)


def split_by_word_number(full_text: str):
    """
    Split the whole text by 'WORD NUMBER:' boundaries.
    Each block tends to contain WORD TITLE / DESCRIPTION / FIELD CODING etc.
    """
    parts = re.split(r"(?=WORD NUMBER:\s*[A-Z0-9\.]+)", full_text)
    blocks = []
    for p in parts:
        p = clean_lines(p)
        if len(p) < MIN_BLOCK_CHARS:
            continue
        blocks.append(p)
    return blocks


def build_embed_model():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(EMBED_MODEL_DIR, trust_remote_code=True)
    model = AutoModel.from_pretrained(
        EMBED_MODEL_DIR,
        trust_remote_code=True,
        torch_dtype=torch.float16 if device == "cuda" else torch.float32,
        device_map="cuda" if device == "cuda" else None,
    )
    if device != "cuda":
        model = model.to(device)
    model.eval()
    return tokenizer, model


@torch.no_grad()
def embed_texts(tokenizer, model, texts, batch_size=32, max_length=512):
    """Convert texts -> embeddings, mean pooling + normalize, return numpy (N, dim)."""
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

    return torch.cat(vecs, dim=0).numpy()


def connect_milvus_lite(db_path: str):
    # If db_path has no dir (just filename), dirname is ""
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    connections.connect(alias="default", uri=db_path)


def create_collection(name: str, dim: int):
    """Drop and recreate collection for clean testing."""
    if utility.has_collection(name):
        utility.drop_collection(name)

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
        FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="chunk_id", dtype=DataType.INT64),
    ]
    schema = CollectionSchema(fields, description="RAG chunks with Qwen3-Embedding-0.6B")
    col = Collection(name=name, schema=schema)

    # FLAT + COSINE for simplicity
    col.create_index(
        field_name="embedding",
        index_params={"index_type": "FLAT", "metric_type": "COSINE"},
    )
    return col


def main():
    print("=== 1) 读取 PDF ===")
    print("PDF:", PDF_PATH)
    full_text = load_pdf_text(PDF_PATH)
    if not full_text.strip():
        raise RuntimeError("PDF 提取文本为空：可能路径不对，或 PDF 文本层不可提取。")

    print("=== 2) 清洗 + 按 WORD NUMBER 分块 ===")
    blocks = split_by_word_number(full_text)
    print(f"word-blocks: {len(blocks)}")
    if len(blocks) == 0:
        raise RuntimeError("split_by_word_number 得到 0 个块：可能 PDF 中没有 'WORD NUMBER:' 或抽取失败。")

    print("=== 3) 二次切分（可选） ===")
    splitter = DocumentSplitter()
    docs = []

    if SECOND_SPLIT:
        for bi, blk in enumerate(blocks):
            sub_docs = splitter.split_to_documents(blk, file_path=PDF_PATH)
            for dj, d in enumerate(sub_docs):
                d.metadata["chunk_id"] = bi * 1000 + dj
                docs.append(d)
    else:
        # no secondary split
        for bi, blk in enumerate(blocks):
            class _Doc:
                def __init__(self, page_content, metadata):
                    self.page_content = page_content
                    self.metadata = metadata
            docs.append(_Doc(blk, {"source": PDF_PATH, "chunk_id": bi}))

    texts = [d.page_content for d in docs]
    sources = [d.metadata.get("source", PDF_PATH) for d in docs]
    chunk_ids = [int(d.metadata.get("chunk_id", i)) for i, d in enumerate(docs)]

    print(f"chunks: {len(texts)}")
    if len(texts) == 0:
        raise RuntimeError("分块后 chunks=0：检查 splitter.py 或过滤条件是否过严。")

    # quick sanity check
    print("\n[Preview 1 chunk]")
    print(texts[0][:400].replace("\n", " "))
    print("...")

    print("\n=== 4) 向量化 ===")
    tokenizer, model = build_embed_model()
    vectors = embed_texts(tokenizer, model, texts, batch_size=EMBED_BATCH, max_length=MAX_LEN)
    dim = vectors.shape[1]
    print("embedding shape:", vectors.shape)

    print("\n=== 5) 写入 Milvus Lite ===")
    connect_milvus_lite(MILVUS_DB_PATH)
    col = create_collection(COLLECTION_NAME, dim=dim)

    col.insert(
        data=[vectors, texts, sources, chunk_ids],
        fields=["embedding", "content", "source", "chunk_id"],
    )
    col.flush()
    col.load()

    print("\n✅ Done.")
    print("Milvus DB:", MILVUS_DB_PATH)
    print("Collection:", COLLECTION_NAME)
    print("Entities:", col.num_entities)


if __name__ == "__main__":
    main()
