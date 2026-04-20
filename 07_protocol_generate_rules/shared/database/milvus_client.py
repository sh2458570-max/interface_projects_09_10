# shared/database/milvus_client.py
# Milvus向量数据库客户端

import os
from typing import List, Optional, Dict, Any
from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
)


class MilvusClient:
    """Milvus向量数据库客户端"""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        db_name: str = None,
    ):
        self.host = host or os.getenv("MILVUS_HOST", "localhost")
        self.port = port or int(os.getenv("MILVUS_PORT", "19530"))
        self.db_name = db_name or os.getenv("MILVUS_DB", "protocol_db")
        default_lite = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "data",
            "milvus_lite.db",
        )
        self.uri = os.getenv("MILVUS_URI")
        self.lite_uri = os.path.expanduser(os.getenv("MILVUS_LITE_URI", default_lite))
        self.auto_fallback_lite = os.getenv("MILVUS_AUTO_FALLBACK_LITE", "true").lower() == "true"
        self._connected = False
        self._collections = {}

    def connect(self):
        """连接Milvus"""
        if not self._connected:
            if self.uri:
                connections.connect(alias="default", uri=self.uri)
                self._connected = True
                return
            try:
                connections.connect(
                    alias="default",
                    host=self.host,
                    port=self.port,
                    db_name=self.db_name,
                )
            except Exception:
                if not self.auto_fallback_lite:
                    raise
                try:
                    import milvus_lite  # noqa: F401
                except ImportError as exc:
                    raise RuntimeError(
                        "Milvus服务不可用，且未安装milvus-lite。请先安装: pip install milvus-lite"
                    ) from exc
                os.makedirs(os.path.dirname(self.lite_uri), exist_ok=True)
                self.uri = self.lite_uri
                connections.connect(alias="default", uri=self.uri)
            self._connected = True

    def disconnect(self):
        """断开连接"""
        if self._connected:
            connections.disconnect("default")
            self._connected = False

    def create_collection(self, collection_name: str, dim: int = 1024, description: str = ""):
        """创建向量集合"""
        self.connect()

        if utility.has_collection(collection_name):
            return Collection(collection_name)

        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, max_length=64, is_primary=True, auto_id=False),
            FieldSchema(name="chunk_id", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="project_id", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="dataset_id", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="semantic_type", dtype=DataType.VARCHAR, max_length=64),
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=8192),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]

        schema = CollectionSchema(fields=fields, description=description)
        collection = Collection(name=collection_name, schema=schema)

        # 创建索引
        index_params = {
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024},
        }
        collection.create_index(field_name="embedding", index_params=index_params)

        return collection

    def get_collection(self, collection_name: str) -> Collection:
        """获取集合"""
        self.connect()
        if collection_name not in self._collections:
            if utility.has_collection(collection_name):
                self._collections[collection_name] = Collection(collection_name)
            else:
                self._collections[collection_name] = self.create_collection(collection_name)
        return self._collections[collection_name]

    def insert_vectors(
        self,
        collection_name: str,
        ids: List[str],
        chunk_ids: List[str],
        project_ids: List[str],
        dataset_ids: List[str],
        semantic_types: List[str],
        contents: List[str],
        embeddings: List[List[float]],
    ) -> int:
        """插入向量数据"""
        collection = self.get_collection(collection_name)

        data = [
            ids,
            chunk_ids,
            project_ids,
            dataset_ids,
            semantic_types,
            contents,
            embeddings,
        ]

        result = collection.insert(data)
        collection.flush()
        return result.insert_count

    def search(
        self,
        collection_name: str,
        query_vector: List[float],
        top_k: int = 10,
        filter_expr: str = None,
    ) -> List[Dict[str, Any]]:
        """向量相似度搜索"""
        collection = self.get_collection(collection_name)
        collection.load()

        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}

        results = collection.search(
            data=[query_vector],
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            expr=filter_expr,
            output_fields=["chunk_id", "project_id", "dataset_id", "semantic_type", "content"],
        )

        output = []
        for hits in results:
            for hit in hits:
                output.append({
                    "id": hit.id,
                    "distance": hit.distance,
                    "chunk_id": hit.entity.get("chunk_id"),
                    "project_id": hit.entity.get("project_id"),
                    "dataset_id": hit.entity.get("dataset_id"),
                    "semantic_type": hit.entity.get("semantic_type"),
                    "content": hit.entity.get("content"),
                })
        return output

    def delete_by_ids(self, collection_name: str, ids: List[str]):
        """根据ID删除向量"""
        collection = self.get_collection(collection_name)
        expr = f'id in {ids}'
        collection.delete(expr)
        collection.flush()

    def delete_by_dataset(self, collection_name: str, dataset_id: str):
        """根据数据集ID删除向量"""
        collection = self.get_collection(collection_name)
        expr = f'dataset_id == "{dataset_id}"'
        collection.delete(expr)
        collection.flush()

    def count(self, collection_name: str) -> int:
        """获取集合中的向量数量"""
        collection = self.get_collection(collection_name)
        collection.flush()
        return collection.num_entities

    def drop_collection(self, collection_name: str):
        """删除集合"""
        self.connect()
        if utility.has_collection(collection_name):
            utility.drop_collection(collection_name)
        if collection_name in self._collections:
            del self._collections[collection_name]
