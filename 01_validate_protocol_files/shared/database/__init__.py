# shared/database/__init__.py
try:
    from .mysql_client import MySQLClient
except Exception:
    MySQLClient = None

try:
    from .milvus_client import MilvusClient
except Exception:
    MilvusClient = None

from .models import Block, Chunk, QAPair, FinetuneJob, JobStatus

__all__ = [
    "MySQLClient",
    "MilvusClient",
    "Block",
    "Chunk",
    "QAPair",
    "FinetuneJob",
    "JobStatus",
]
