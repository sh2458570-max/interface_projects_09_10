# 全局配置

import os

# 项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ==================== 模型配置 ====================

MODEL_CACHE_DIR = os.path.expanduser(os.getenv("MODEL_CACHE_DIR", "~/model_cache"))
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "qwen3-0.6b-embedding")
RERANK_MODEL_NAME = os.getenv("RERANK_MODEL_NAME", "Qwen3-Reranker-0.6B")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME", "Qwen/Qwen3-4B")

# LLM服务配置
USE_VLLM = os.getenv("USE_VLLM", "false").lower() == "true"
VLLM_URL = os.getenv("VLLM_URL", "http://localhost:8000")

# ==================== 数据库配置 ====================

# MySQL配置
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "protocol_db")

# Milvus配置
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
MILVUS_DB = os.getenv("MILVUS_DB", "protocol_db")

# 数据目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ==================== 服务端口配置 ====================

SERVICE_PORTS = {
    "api_01_upload_split": 5001,
    "api_02_clean": 5002,
    "api_03_extract_validate": 5003,
    "api_04_generate_qa": 5004,
    "api_05_finetune": 5005,
    "api_06_semantic_chunk": 5006,
}

# ==================== API接口路径 ====================

API_ENDPOINTS = {
    "upload_split": "/api/data/upload_split",
    "clean": "/api/data/clean",
    "extract_validate_qa": "/api/knowledge/extract_validate_qa",
    "generate_qa": "/api/knowledge/generate_qa",
    "finetune_action": "/api/model/finetune/action",
    "finetune_stream": "/api/model/finetune/stream",
    "semantic_chunk": "/api/data/semantic_chunk",
}

# ==================== 训练配置 ====================

TRAINING_CONFIG = {
    "default_base_model": LLM_MODEL_NAME,
    "default_epochs": 3,
    "default_learning_rate": 2e-4,
    "default_batch_size": 4,
    "default_lora_rank": 16,
    "default_lora_alpha": 32,
    "default_lora_dropout": 0.05,
    "default_max_length": 2048,
    "checkpoint_dir": os.path.join(DATA_DIR, "checkpoints"),
    "output_dir": os.path.join(DATA_DIR, "models"),
}

# 创建必要目录
for key in ["checkpoint_dir", "output_dir"]:
    os.makedirs(TRAINING_CONFIG[key], exist_ok=True)
