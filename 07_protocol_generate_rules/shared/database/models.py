# shared/database/models.py
# 数据模型定义

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum


class JobStatus(str, Enum):
    """微调任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class BlockType(str, Enum):
    """块类型"""
    TEXT = "text"
    TABLE = "table"
    IMAGE = "image"
    CODE = "code"


@dataclass
class Block:
    """文档块模型"""
    block_id: int
    project_id: str
    file_name: str
    page_num: int
    content: str
    block_type: str = "text"
    cleaned_content: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "block_id": self.block_id,
            "project_id": self.project_id,
            "file_name": self.file_name,
            "page_num": self.page_num,
            "content": self.content,
            "block_type": self.block_type,
            "cleaned_content": self.cleaned_content,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


@dataclass
class Chunk:
    """语义块模型"""
    chunk_id: str
    project_id: str
    dataset_id: str
    source_block_ids: List[int]
    semantic_type: str
    content_snapshot: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    embedding: Optional[List[float]] = None
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "source_block_ids": self.source_block_ids,
            "semantic_type": self.semantic_type,
            "content_snapshot": self.content_snapshot,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


@dataclass
class QAPair:
    """问答对模型"""
    qa_id: str
    source_block_ids: List[str]
    question: str
    answer: str
    qa_task_type: str = "protocol_understanding"
    conversion_mode: Optional[str] = None
    conversion_formula: Optional[str] = None
    source_field: Optional[str] = None
    source_fields: List[str] = field(default_factory=list)
    target_field: Optional[str] = None
    concept_name: Optional[str] = None
    formula_kind: Optional[str] = None
    target_protocol_type: Optional[str] = None
    target_message_code: Optional[str] = None
    instruction: str = ""
    is_low_quality: bool = False
    quality_reason: Optional[str] = None
    extracted_info: Optional[Dict[str, Any]] = None
    validation_result: Optional[Dict[str, Any]] = None
    protocol_type: str = "Link16"
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "qa_id": self.qa_id,
            "source_block_ids": self.source_block_ids,
            "question": self.question,
            "answer": self.answer,
            "qa_task_type": self.qa_task_type,
            "conversion_mode": self.conversion_mode,
            "conversion_formula": self.conversion_formula,
            "source_field": self.source_field,
            "source_fields": self.source_fields,
            "target_field": self.target_field,
            "concept_name": self.concept_name,
            "formula_kind": self.formula_kind,
            "target_protocol_type": self.target_protocol_type,
            "target_message_code": self.target_message_code,
            "instruction": self.instruction,
            "is_low_quality": self.is_low_quality,
            "quality_reason": self.quality_reason,
            "extracted_info": self.extracted_info,
            "validation_result": self.validation_result,
            "protocol_type": self.protocol_type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    def to_jsonl(self) -> Dict[str, str]:
        """转换为训练用的JSONL格式"""
        if self.qa_task_type == "protocol_conversion":
            conversion_input = {
                "question": self.question,
                "concept_name": self.concept_name,
                "source_field": self.source_field,
                "source_fields": self.source_fields or ([self.source_field] if self.source_field else []),
                "target_field": self.target_field,
                "target_protocol_type": self.target_protocol_type,
                "target_message_code": self.target_message_code,
                "conversion_mode": self.conversion_mode,
                "formula_kind": self.formula_kind,
            }
            return {
                "instruction": self.instruction or "你是一个协议转换公式生成器，请输出原协议字段值到目标协议字段值的转换公式。",
                "input": json.dumps(conversion_input, ensure_ascii=False),
                "output": self.conversion_formula or self.answer,
            }
        return {
            "instruction": self.instruction or "你是一个协议文档专家，请根据文档内容回答问题。",
            "input": self.question,
            "output": self.answer,
        }


@dataclass
class FinetuneJob:
    """微调任务模型"""
    job_id: str
    status: JobStatus = JobStatus.PENDING
    base_model: str = ""
    dataset_id: str = ""
    config: Dict[str, Any] = field(default_factory=dict)
    progress: Dict[str, Any] = field(default_factory=dict)
    last_checkpoint: Optional[Dict[str, Any]] = None
    model_path: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "base_model": self.base_model,
            "dataset_id": self.dataset_id,
            "config": self.config,
            "progress": self.progress,
            "last_checkpoint": self.last_checkpoint,
            "model_path": self.model_path,
            "metrics": self.metrics,
            "error_message": self.error_message,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class CleaningIssue:
    """清洗问题记录"""
    block_id: int
    issue_type: str
    description: str
    original: str
    cleaned: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "block_id": self.block_id,
            "issue_type": self.issue_type,
            "description": self.description,
            "original": self.original,
            "cleaned": self.cleaned,
        }
