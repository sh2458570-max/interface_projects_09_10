# shared/utils/file_store.py
# 文件存储工具

import os
import json
import time
from typing import List, Dict, Any, Optional
from datetime import datetime


class FileStore:
    """文件存储工具类，用于管理训练数据和中间结果"""

    def __init__(self, base_dir: str = None):
        self.base_dir = base_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "data"
        )
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_project_dir(self, project_id: str) -> str:
        """获取项目目录"""
        project_dir = os.path.join(self.base_dir, "projects", project_id)
        os.makedirs(project_dir, exist_ok=True)
        return project_dir

    def _get_dataset_dir(self, dataset_id: str) -> str:
        """获取数据集目录"""
        dataset_dir = os.path.join(self.base_dir, "datasets", dataset_id)
        os.makedirs(dataset_dir, exist_ok=True)
        return dataset_dir

    def _get_job_dir(self, job_id: str) -> str:
        """获取任务目录"""
        job_dir = os.path.join(self.base_dir, "jobs", job_id)
        os.makedirs(job_dir, exist_ok=True)
        return job_dir

    def _get_project_doc_set_dir(self, project_id: str) -> str:
        """获取项目级文档集目录。"""
        doc_set_dir = os.path.join(self._get_project_dir(project_id), "doc_sets")
        os.makedirs(doc_set_dir, exist_ok=True)
        return doc_set_dir

    def _get_pageindex_registry_dir(self, project_id: str) -> str:
        """获取项目级 PageIndex registry 目录。"""
        registry_dir = os.path.join(self.base_dir, "pageindex_registry", project_id)
        os.makedirs(registry_dir, exist_ok=True)
        return registry_dir

    # ==================== 项目数据存储 ====================

    def save_blocks(self, project_id: str, blocks: List[Dict[str, Any]], file_name: str = None) -> str:
        """保存文档块数据"""
        project_dir = self._get_project_dir(project_id)
        if file_name is None:
            file_name = f"blocks_{int(time.time())}.json"
        file_path = os.path.join(project_dir, file_name)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "project_id": project_id,
                "total_blocks": len(blocks),
                "created_at": datetime.now().isoformat(),
                "blocks": blocks,
            }, f, ensure_ascii=False, indent=2)

        return file_path

    def load_blocks(self, project_id: str, file_name: str = None) -> List[Dict[str, Any]]:
        """加载文档块数据"""
        project_dir = self._get_project_dir(project_id)
        if file_name is None:
            # 查找最新的blocks文件
            files = [f for f in os.listdir(project_dir) if f.startswith("blocks_")]
            if not files:
                return []
            file_name = sorted(files)[-1]

        file_path = os.path.join(project_dir, file_name)
        if not os.path.exists(file_path):
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("blocks", [])

    def save_cleaned_blocks(self, project_id: str, blocks: List[Dict[str, Any]]) -> str:
        """保存清洗后的数据块"""
        return self.save_blocks(project_id, blocks, file_name=f"cleaned_{int(time.time())}.json")

    # ==================== 数据集存储 ====================

    def create_dataset(self, dataset_id: str, project_id: str, name: str = "", description: str = "") -> str:
        """创建数据集目录"""
        dataset_dir = self._get_dataset_dir(dataset_id)

        meta = {
            "dataset_id": dataset_id,
            "project_id": project_id,
            "name": name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "block_count": 0,
            "chunk_count": 0,
            "qa_count": 0,
            "preference_pair_count": 0,
        }

        with open(os.path.join(dataset_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        return dataset_dir

    def load_dataset_meta(self, dataset_id: str) -> Dict[str, Any]:
        """读取数据集元信息。"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        meta_path = os.path.join(dataset_dir, "meta.json")
        if not os.path.exists(meta_path):
            return {}
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}

    def update_dataset_meta(self, dataset_id: str, updates: Dict[str, Any]):
        """公开的数据集元信息更新接口。"""
        self._update_dataset_meta(dataset_id, updates)

    # ==================== 文档索引存储 ====================

    def save_project_doc_set(self, project_id: str, doc_set_id: str, payload: Dict[str, Any]) -> str:
        """保存项目级文档集清单。"""
        doc_set_dir = self._get_project_doc_set_dir(project_id)
        path = os.path.join(doc_set_dir, f"{doc_set_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path

    def load_project_doc_set(self, project_id: str, doc_set_id: str) -> Dict[str, Any]:
        """读取项目级文档集清单。"""
        path = os.path.join(self._get_project_doc_set_dir(project_id), f"{doc_set_id}.json")
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}

    def save_pageindex_registry(self, project_id: str, doc_set_id: str, payload: Dict[str, Any]) -> str:
        """保存项目级 PageIndex registry。"""
        registry_dir = self._get_pageindex_registry_dir(project_id)
        path = os.path.join(registry_dir, f"{doc_set_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path

    def load_pageindex_registry(self, project_id: str, doc_set_id: str) -> Dict[str, Any]:
        """读取项目级 PageIndex registry。"""
        path = os.path.join(self._get_pageindex_registry_dir(project_id), f"{doc_set_id}.json")
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}

    def list_pageindex_registries(self, project_id: str) -> List[Dict[str, Any]]:
        """列出项目下所有 PageIndex registry。"""
        registry_dir = self._get_pageindex_registry_dir(project_id)
        items: List[Dict[str, Any]] = []
        for name in sorted(os.listdir(registry_dir)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(registry_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                items.append(payload)
        return items

    def resolve_pageindex_registry(
        self,
        project_id: str = "",
        dataset_id: str = "",
        doc_set_id: str = "",
        index_ref: str = "",
    ) -> Dict[str, Any]:
        """根据 project/dataset/doc_set/index_ref 解析 registry。"""
        resolved_project_id = str(project_id or "").strip()
        resolved_doc_set_id = str(doc_set_id or "").strip()
        resolved_index_ref = str(index_ref or "").strip()

        if dataset_id:
            dataset_meta = self.load_dataset_meta(dataset_id)
            if not resolved_project_id:
                resolved_project_id = str(dataset_meta.get("project_id") or "").strip()
            if not resolved_doc_set_id:
                resolved_doc_set_id = str(dataset_meta.get("doc_set_id") or "").strip()
            if not resolved_index_ref:
                resolved_index_ref = str(dataset_meta.get("index_ref") or "").strip()

        if resolved_project_id and resolved_doc_set_id:
            registry = self.load_pageindex_registry(resolved_project_id, resolved_doc_set_id)
            if registry and (not resolved_index_ref or str(registry.get("index_ref") or "").strip() == resolved_index_ref):
                return registry

        if not resolved_project_id:
            return {}

        for registry in self.list_pageindex_registries(resolved_project_id):
            if resolved_doc_set_id and str(registry.get("doc_set_id") or "").strip() != resolved_doc_set_id:
                continue
            if resolved_index_ref and str(registry.get("index_ref") or "").strip() != resolved_index_ref:
                continue
            if dataset_id and str(registry.get("dataset_id") or "").strip() != str(dataset_id).strip():
                continue
            return registry
        return {}

    def save_chunks(self, dataset_id: str, chunks: List[Dict[str, Any]]) -> str:
        """保存语义块数据"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "chunks.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "dataset_id": dataset_id,
                "total_chunks": len(chunks),
                "created_at": datetime.now().isoformat(),
                "chunks": chunks,
            }, f, ensure_ascii=False, indent=2)

        # 更新meta
        self._update_dataset_meta(dataset_id, {"chunk_count": len(chunks)})

        return file_path

    def load_chunks(self, dataset_id: str) -> List[Dict[str, Any]]:
        """加载语义块数据"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "chunks.json")

        if not os.path.exists(file_path):
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("chunks", [])

    # ==================== QA数据存储 ====================

    def save_qa_pairs(self, dataset_id: str, qa_pairs: List[Dict[str, Any]]) -> str:
        """保存QA对数据"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "qa_pairs.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "dataset_id": dataset_id,
                "total_qa": len(qa_pairs),
                "created_at": datetime.now().isoformat(),
                "qa_pairs": qa_pairs,
            }, f, ensure_ascii=False, indent=2)

        # 更新meta
        self._update_dataset_meta(dataset_id, {"qa_count": len(qa_pairs)})

        return file_path

    def load_qa_pairs(self, dataset_id: str) -> List[Dict[str, Any]]:
        """加载QA对数据"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "qa_pairs.json")

        if not os.path.exists(file_path):
            return []

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("qa_pairs", [])

    def save_preference_pairs(self, dataset_id: str, preference_pairs: List[Dict[str, Any]]) -> str:
        """保存偏好对训练数据(JSONL格式)。"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "preference_pairs.jsonl")

        with open(file_path, "w", encoding="utf-8") as f:
            for pair in preference_pairs:
                f.write(json.dumps(pair, ensure_ascii=False) + "\n")

        self._update_dataset_meta(dataset_id, {"preference_pair_count": len(preference_pairs)})
        return file_path

    def load_preference_pairs(self, dataset_id: str) -> List[Dict[str, Any]]:
        """加载偏好对训练数据。"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        file_path = os.path.join(dataset_dir, "preference_pairs.jsonl")

        if not os.path.exists(file_path):
            return []

        pairs: List[Dict[str, Any]] = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                pairs.append(json.loads(line))
        return pairs

    def export_training_data(self, dataset_id: str, output_path: str = None) -> str:
        """导出训练数据（JSONL格式）"""
        qa_pairs = self.load_qa_pairs(dataset_id)

        if output_path is None:
            dataset_dir = self._get_dataset_dir(dataset_id)
            output_path = os.path.join(dataset_dir, "train.jsonl")

        with open(output_path, "w", encoding="utf-8") as f:
            for qa in qa_pairs:
                if not qa.get("is_low_quality", False):
                    if qa.get("qa_task_type") == "protocol_conversion":
                        input_payload = {
                            "question": qa.get("question", ""),
                            "concept_name": qa.get("concept_name"),
                            "source_field": qa.get("source_field"),
                            "source_fields": qa.get("source_fields") or ([qa.get("source_field")] if qa.get("source_field") else []),
                            "target_field": qa.get("target_field"),
                            "target_protocol_type": qa.get("target_protocol_type"),
                            "target_message_code": qa.get("target_message_code"),
                            "conversion_mode": qa.get("conversion_mode"),
                            "formula_kind": qa.get("formula_kind"),
                        }
                        train_item = {
                            "instruction": qa.get("instruction", "你是一个协议转换公式生成器，请输出原协议字段值到目标协议字段值的转换公式。"),
                            "input": json.dumps(input_payload, ensure_ascii=False),
                            "output": qa.get("conversion_formula") or qa.get("answer", ""),
                        }
                    else:
                        train_item = {
                            "instruction": qa.get("instruction", "你是一个协议文档专家，请根据文档内容回答问题。"),
                            "input": qa.get("question", ""),
                            "output": qa.get("answer", ""),
                        }
                    f.write(json.dumps(train_item, ensure_ascii=False) + "\n")

        return output_path

    def export_preference_data(self, dataset_id: str, output_path: str = None) -> str:
        """导出偏好训练数据(JSONL格式)。"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        source_path = os.path.join(dataset_dir, "preference_pairs.jsonl")
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"偏好训练数据不存在: {source_path}")

        if output_path is None or os.path.abspath(output_path) == os.path.abspath(source_path):
            return source_path

        with open(source_path, "r", encoding="utf-8") as src, open(output_path, "w", encoding="utf-8") as dst:
            dst.write(src.read())
        return output_path

    def _update_dataset_meta(self, dataset_id: str, updates: Dict[str, Any]):
        """更新数据集元信息"""
        dataset_dir = self._get_dataset_dir(dataset_id)
        meta_path = os.path.join(dataset_dir, "meta.json")

        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            meta.update(updates)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)

    # ==================== 模型检查点存储 ====================

    def save_checkpoint(self, job_id: str, step: int, checkpoint_path: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """保存检查点信息"""
        job_dir = self._get_job_dir(job_id)

        checkpoint_info = {
            "step": step,
            "path": checkpoint_path,
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics,
        }

        # 更新最新检查点
        checkpoint_file = os.path.join(job_dir, "latest_checkpoint.json")
        with open(checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(checkpoint_info, f, ensure_ascii=False, indent=2)

        return checkpoint_info

    def load_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        """加载最新检查点"""
        job_dir = self._get_job_dir(job_id)
        checkpoint_file = os.path.join(job_dir, "latest_checkpoint.json")

        if not os.path.exists(checkpoint_file):
            return None

        with open(checkpoint_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_training_history(self, job_id: str, history: List[Dict[str, Any]]):
        """保存训练历史"""
        job_dir = self._get_job_dir(job_id)
        history_file = os.path.join(job_dir, "training_history.json")

        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def load_training_history(self, job_id: str) -> List[Dict[str, Any]]:
        """加载训练历史"""
        job_dir = self._get_job_dir(job_id)
        history_file = os.path.join(job_dir, "training_history.json")

        if not os.path.exists(history_file):
            return []

        with open(history_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # ==================== 工具方法 ====================

    def get_data_path(self, *parts) -> str:
        """获取数据目录下的路径"""
        return os.path.join(self.base_dir, *parts)

    def cleanup_old_files(self, days: int = 7):
        """清理旧文件"""
        import time
        cutoff = time.time() - days * 24 * 60 * 60

        for root, dirs, files in os.walk(self.base_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.getmtime(file_path) < cutoff:
                    os.remove(file_path)
                    print(f"已清理: {file_path}")
