# shared/database/mysql_client.py
# MySQL数据库客户端（支持SQLite本地回退）

import os
import json
import sqlite3
from typing import Optional, List, Dict, Any, Sequence
from contextlib import contextmanager

import pymysql
from pymysql.cursors import DictCursor

from .models import Block, Chunk, QAPair, FinetuneJob, JobStatus, CleaningIssue


class MySQLClient:
    """MySQL数据库客户端（无服务时可自动回退SQLite）"""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        self.host = host or os.getenv("MYSQL_HOST", "localhost")
        self.port = port or int(os.getenv("MYSQL_PORT", "3306"))
        self.user = user or os.getenv("MYSQL_USER", "root")
        self.password = password or os.getenv("MYSQL_PASSWORD", "password")
        self.database = database or os.getenv("MYSQL_DATABASE", "protocol_db")

        default_sqlite = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "data",
            f"{self.database}.sqlite3",
        )
        self.sqlite_path = os.path.expanduser(os.getenv("SQLITE_DB_PATH", default_sqlite))
        self.backend = "sqlite" if os.getenv("MYSQL_USE_SQLITE", "false").lower() == "true" else "mysql"
        self.auto_fallback_sqlite = os.getenv("MYSQL_AUTO_FALLBACK_SQLITE", "true").lower() == "true"
        self._pool = []

    @property
    def is_sqlite(self) -> bool:
        return self.backend == "sqlite"

    def _mysql_connection(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            cursorclass=DictCursor,
        )

    def _sqlite_connection(self):
        os.makedirs(os.path.dirname(self.sqlite_path), exist_ok=True)
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _get_connection(self):
        """获取数据库连接。MySQL不可用时按配置自动回退SQLite。"""
        if self.is_sqlite:
            return self._sqlite_connection()
        try:
            return self._mysql_connection()
        except Exception:
            if not self.auto_fallback_sqlite:
                raise
            self.backend = "sqlite"
            return self._sqlite_connection()

    @contextmanager
    def connection(self):
        """连接上下文管理器"""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _normalize_sqlite_query(self, query: str) -> str:
        q = query.replace("%s", "?")
        q = q.replace("NOW()", "CURRENT_TIMESTAMP")
        q = q.replace("BOOLEAN", "INTEGER")
        q = q.replace("TRUE", "1").replace("FALSE", "0")
        return q

    def _execute(self, cursor, query: str, params: Optional[Sequence[Any]] = None):
        if self.is_sqlite:
            query = self._normalize_sqlite_query(query)
        if params is None:
            return cursor.execute(query)
        return cursor.execute(query, params)

    def _fetchall_dict(self, cursor) -> List[Dict[str, Any]]:
        rows = cursor.fetchall()
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return rows
        return [dict(row) for row in rows]

    def _fetchone_dict(self, cursor) -> Optional[Dict[str, Any]]:
        row = cursor.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return row
        return dict(row)

    def _column_exists(self, cursor, table_name: str, column_name: str) -> bool:
        """检查列是否存在（兼容MySQL/SQLite）"""
        if self.is_sqlite:
            self._execute(cursor, f"PRAGMA table_info({table_name})")
            columns = self._fetchall_dict(cursor)
            return any(col.get("name") == column_name for col in columns)

        self._execute(
            cursor,
            """
                SELECT 1
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND COLUMN_NAME = %s
                LIMIT 1
            """,
            (self.database, table_name, column_name),
        )
        return self._fetchone_dict(cursor) is not None

    def _placeholders(self, count: int) -> str:
        mark = "?" if self.is_sqlite else "%s"
        return ",".join([mark] * count)

    def _init_tables_mysql(self, cursor):
        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS blocks (
                    block_id INT PRIMARY KEY AUTO_INCREMENT,
                    project_id VARCHAR(64) NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    page_num INT DEFAULT 1,
                    content TEXT,
                    block_type VARCHAR(32) DEFAULT 'text',
                    cleaned_content TEXT,
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_project (project_id),
                    INDEX idx_file (file_name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        )

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id VARCHAR(64) PRIMARY KEY,
                    project_id VARCHAR(64) NOT NULL,
                    dataset_id VARCHAR(64) NOT NULL,
                    source_block_ids JSON,
                    semantic_type VARCHAR(64),
                    content_snapshot TEXT,
                    metadata JSON,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_project (project_id),
                    INDEX idx_dataset (dataset_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        )

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS qa_pairs (
                    qa_id VARCHAR(64) PRIMARY KEY,
                    source_block_ids JSON,
                    question TEXT,
                    answer TEXT,
                    qa_task_type VARCHAR(64) DEFAULT 'protocol_understanding',
                    conversion_mode VARCHAR(32),
                    conversion_formula TEXT,
                    source_field VARCHAR(128),
                    source_fields JSON,
                    target_field VARCHAR(128),
                    concept_name VARCHAR(128),
                    formula_kind VARCHAR(32),
                    target_protocol_type VARCHAR(64),
                    target_message_code VARCHAR(64),
                    instruction TEXT,
                    is_low_quality BOOLEAN DEFAULT FALSE,
                    quality_reason VARCHAR(255),
                    extracted_info JSON,
                    validation_result JSON,
                    protocol_type VARCHAR(32) DEFAULT 'Link16',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_protocol (protocol_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        )

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS finetune_jobs (
                    job_id VARCHAR(64) PRIMARY KEY,
                    status VARCHAR(32) DEFAULT 'pending',
                    base_model VARCHAR(128),
                    dataset_id VARCHAR(64),
                    config JSON,
                    progress JSON,
                    last_checkpoint JSON,
                    model_path VARCHAR(512),
                    metrics JSON,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP NULL,
                    completed_at TIMESTAMP NULL,
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        )

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id VARCHAR(64) PRIMARY KEY,
                    project_id VARCHAR(64) NOT NULL,
                    name VARCHAR(128),
                    description TEXT,
                    block_count INT DEFAULT 0,
                    chunk_count INT DEFAULT 0,
                    qa_count INT DEFAULT 0,
                    file_path VARCHAR(512),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_project (project_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        )

    def _init_tables_sqlite(self, cursor):
        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS blocks (
                    block_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    page_num INTEGER DEFAULT 1,
                    content TEXT,
                    block_type TEXT DEFAULT 'text',
                    cleaned_content TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """,
        )
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_blocks_project ON blocks(project_id)")
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_blocks_file ON blocks(file_name)")

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    source_block_ids TEXT,
                    semantic_type TEXT,
                    content_snapshot TEXT,
                    metadata TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """,
        )
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_chunks_project ON chunks(project_id)")
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_chunks_dataset ON chunks(dataset_id)")

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS qa_pairs (
                    qa_id TEXT PRIMARY KEY,
                    source_block_ids TEXT,
                    question TEXT,
                    answer TEXT,
                    qa_task_type TEXT DEFAULT 'protocol_understanding',
                    conversion_mode TEXT,
                    conversion_formula TEXT,
                    source_field TEXT,
                    source_fields TEXT,
                    target_field TEXT,
                    concept_name TEXT,
                    formula_kind TEXT,
                    target_protocol_type TEXT,
                    target_message_code TEXT,
                    instruction TEXT,
                    is_low_quality INTEGER DEFAULT 0,
                    quality_reason TEXT,
                    extracted_info TEXT,
                    validation_result TEXT,
                    protocol_type TEXT DEFAULT 'Link16',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """,
        )
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_qa_protocol ON qa_pairs(protocol_type)")

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS finetune_jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'pending',
                    base_model TEXT,
                    dataset_id TEXT,
                    config TEXT,
                    progress TEXT,
                    last_checkpoint TEXT,
                    model_path TEXT,
                    metrics TEXT,
                    error_message TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    completed_at TEXT
                )
            """,
        )
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_finetune_status ON finetune_jobs(status)")

        self._execute(
            cursor,
            """
                CREATE TABLE IF NOT EXISTS datasets (
                    dataset_id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    name TEXT,
                    description TEXT,
                    block_count INTEGER DEFAULT 0,
                    chunk_count INTEGER DEFAULT 0,
                    qa_count INTEGER DEFAULT 0,
                    file_path TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """,
        )
        self._execute(cursor, "CREATE INDEX IF NOT EXISTS idx_datasets_project ON datasets(project_id)")

    def init_tables(self):
        """初始化数据表"""
        with self.connection() as conn:
            cursor = conn.cursor()
            if self.is_sqlite:
                self._init_tables_sqlite(cursor)
            else:
                self._init_tables_mysql(cursor)

            qa_column_migrations = [
                ("qa_task_type", "qa_task_type TEXT DEFAULT 'protocol_understanding'" if self.is_sqlite else "qa_task_type VARCHAR(64) DEFAULT 'protocol_understanding'"),
                ("conversion_mode", "conversion_mode TEXT" if self.is_sqlite else "conversion_mode VARCHAR(32)"),
                ("conversion_formula", "conversion_formula TEXT"),
                ("source_field", "source_field TEXT" if self.is_sqlite else "source_field VARCHAR(128)"),
                ("source_fields", "source_fields TEXT" if self.is_sqlite else "source_fields JSON"),
                ("target_field", "target_field TEXT" if self.is_sqlite else "target_field VARCHAR(128)"),
                ("concept_name", "concept_name TEXT" if self.is_sqlite else "concept_name VARCHAR(128)"),
                ("formula_kind", "formula_kind TEXT" if self.is_sqlite else "formula_kind VARCHAR(32)"),
                ("target_protocol_type", "target_protocol_type TEXT" if self.is_sqlite else "target_protocol_type VARCHAR(64)"),
                ("target_message_code", "target_message_code TEXT" if self.is_sqlite else "target_message_code VARCHAR(64)"),
            ]
            for column_name, column_ddl in qa_column_migrations:
                try:
                    if self._column_exists(cursor, "qa_pairs", column_name):
                        continue
                    self._execute(cursor, f"ALTER TABLE qa_pairs ADD COLUMN {column_ddl}")
                except Exception:
                    pass

            print(f"数据库表初始化完成 (backend={self.backend})")

    # ==================== Block 操作 ====================

    def insert_block(self, block: Block) -> int:
        """插入文档块"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                """
                    INSERT INTO blocks (project_id, file_name, page_num, content, block_type, cleaned_content, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    block.project_id,
                    block.file_name,
                    block.page_num,
                    block.content,
                    block.block_type,
                    block.cleaned_content,
                    json.dumps(block.metadata, ensure_ascii=False) if block.metadata else None,
                ),
            )
            return cursor.lastrowid

    def insert_blocks(self, blocks: List[Block]) -> List[int]:
        """批量插入文档块"""
        ids = []
        for block in blocks:
            ids.append(self.insert_block(block))
        return ids

    def get_blocks_by_ids(self, block_ids: List[int]) -> List[Block]:
        """根据ID列表获取文档块"""
        if not block_ids:
            return []
        with self.connection() as conn:
            cursor = conn.cursor()
            placeholders = self._placeholders(len(block_ids))
            self._execute(
                cursor,
                f"SELECT * FROM blocks WHERE block_id IN ({placeholders})",
                block_ids,
            )
            rows = self._fetchall_dict(cursor)
            return [self._row_to_block(row) for row in rows]

    def get_blocks_by_project(self, project_id: str) -> List[Block]:
        """根据项目ID获取所有文档块"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                "SELECT * FROM blocks WHERE project_id = %s ORDER BY block_id",
                (project_id,),
            )
            rows = self._fetchall_dict(cursor)
            return [self._row_to_block(row) for row in rows]

    def update_block_content(self, block_id: int, cleaned_content: str):
        """更新文档块清洗后的内容"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                """
                    UPDATE blocks SET cleaned_content = %s, updated_at = NOW()
                    WHERE block_id = %s
                """,
                (cleaned_content, block_id),
            )

    def _row_to_block(self, row: Dict[str, Any]) -> Block:
        """数据库行转Block对象"""
        return Block(
            block_id=row["block_id"],
            project_id=row["project_id"],
            file_name=row["file_name"],
            page_num=row["page_num"],
            content=row["content"],
            block_type=row["block_type"],
            cleaned_content=row["cleaned_content"],
            metadata=json.loads(row["metadata"]) if row.get("metadata") else {},
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
        )

    # ==================== Chunk 操作 ====================

    def insert_chunk(self, chunk: Chunk) -> str:
        """插入语义块"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                """
                    INSERT INTO chunks (chunk_id, project_id, dataset_id, source_block_ids, semantic_type, content_snapshot, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chunk.chunk_id,
                    chunk.project_id,
                    chunk.dataset_id,
                    json.dumps(chunk.source_block_ids, ensure_ascii=False),
                    chunk.semantic_type,
                    chunk.content_snapshot,
                    json.dumps(chunk.metadata, ensure_ascii=False) if chunk.metadata else None,
                ),
            )
            return chunk.chunk_id

    def get_chunks_by_dataset(self, dataset_id: str) -> List[Chunk]:
        """根据数据集ID获取语义块"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT * FROM chunks WHERE dataset_id = %s", (dataset_id,))
            rows = self._fetchall_dict(cursor)
            return [self._row_to_chunk(row) for row in rows]

    def _row_to_chunk(self, row: Dict[str, Any]) -> Chunk:
        return Chunk(
            chunk_id=row["chunk_id"],
            project_id=row["project_id"],
            dataset_id=row["dataset_id"],
            source_block_ids=json.loads(row["source_block_ids"]) if row.get("source_block_ids") else [],
            semantic_type=row["semantic_type"],
            content_snapshot=row["content_snapshot"],
            metadata=json.loads(row["metadata"]) if row.get("metadata") else {},
            created_at=row.get("created_at"),
        )

    # ==================== QA 操作 ====================

    def insert_qa(self, qa: QAPair) -> str:
        """插入QA对"""
        values = (
            qa.qa_id,
            json.dumps(qa.source_block_ids, ensure_ascii=False),
            qa.question,
            qa.answer,
            qa.qa_task_type,
            qa.conversion_mode,
            qa.conversion_formula,
            qa.source_field,
            json.dumps(qa.source_fields, ensure_ascii=False) if qa.source_fields else None,
            qa.target_field,
            qa.concept_name,
            qa.formula_kind,
            qa.target_protocol_type,
            qa.target_message_code,
            qa.instruction,
            int(bool(qa.is_low_quality)),
            qa.quality_reason,
            json.dumps(qa.extracted_info, ensure_ascii=False) if qa.extracted_info else None,
            json.dumps(qa.validation_result, ensure_ascii=False) if qa.validation_result else None,
            qa.protocol_type,
        )
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                f"""
                    INSERT INTO qa_pairs (qa_id, source_block_ids, question, answer,
                        qa_task_type, conversion_mode, conversion_formula,
                        source_field, source_fields, target_field, concept_name,
                        formula_kind, target_protocol_type, target_message_code,
                        instruction,
                        is_low_quality, quality_reason, extracted_info, validation_result, protocol_type)
                    VALUES ({self._placeholders(len(values))})
                """,
                values,
            )
            return qa.qa_id

    def get_qa_by_id(self, qa_id: str) -> Optional[QAPair]:
        """根据ID获取QA对"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT * FROM qa_pairs WHERE qa_id = %s", (qa_id,))
            row = self._fetchone_dict(cursor)
            return self._row_to_qa(row) if row else None

    def _row_to_qa(self, row: Dict[str, Any]) -> QAPair:
        return QAPair(
            qa_id=row["qa_id"],
            source_block_ids=json.loads(row["source_block_ids"]) if row.get("source_block_ids") else [],
            question=row["question"],
            answer=row["answer"],
            qa_task_type=row.get("qa_task_type") or "protocol_understanding",
            conversion_mode=row.get("conversion_mode"),
            conversion_formula=row.get("conversion_formula"),
            source_field=row.get("source_field"),
            source_fields=json.loads(row["source_fields"]) if row.get("source_fields") else [],
            target_field=row.get("target_field"),
            concept_name=row.get("concept_name"),
            formula_kind=row.get("formula_kind"),
            target_protocol_type=row.get("target_protocol_type"),
            target_message_code=row.get("target_message_code"),
            instruction=row.get("instruction") or "",
            is_low_quality=bool(row.get("is_low_quality")),
            quality_reason=row.get("quality_reason"),
            extracted_info=json.loads(row["extracted_info"]) if row.get("extracted_info") else None,
            validation_result=json.loads(row["validation_result"]) if row.get("validation_result") else None,
            protocol_type=row.get("protocol_type") or "Link16",
            created_at=row.get("created_at"),
        )

    # ==================== FinetuneJob 操作 ====================

    def insert_job(self, job: FinetuneJob) -> str:
        """插入微调任务"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                """
                    INSERT INTO finetune_jobs (job_id, status, base_model, dataset_id, config, progress, last_checkpoint, model_path, metrics, error_message, started_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job.job_id,
                    job.status.value if isinstance(job.status, JobStatus) else job.status,
                    job.base_model,
                    job.dataset_id,
                    json.dumps(job.config, ensure_ascii=False) if job.config else None,
                    json.dumps(job.progress, ensure_ascii=False) if job.progress else None,
                    json.dumps(job.last_checkpoint, ensure_ascii=False) if job.last_checkpoint else None,
                    job.model_path,
                    json.dumps(job.metrics, ensure_ascii=False) if job.metrics else None,
                    job.error_message,
                    job.started_at,
                ),
            )
            return job.job_id

    def get_job(self, job_id: str) -> Optional[FinetuneJob]:
        """获取微调任务"""
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(cursor, "SELECT * FROM finetune_jobs WHERE job_id = %s", (job_id,))
            row = self._fetchone_dict(cursor)
            return self._row_to_job(row) if row else None

    def update_job(self, job_id: str, **kwargs):
        """更新微调任务"""
        allowed_fields = ["status", "progress", "last_checkpoint", "model_path", "metrics", "error_message", "started_at", "completed_at"]
        updates = []
        values = []
        for key, value in kwargs.items():
            if key in allowed_fields:
                if key in ["progress", "last_checkpoint", "metrics"]:
                    value = json.dumps(value, ensure_ascii=False) if value else None
                updates.append(f"{key} = %s")
                values.append(value)
        if not updates:
            return
        values.append(job_id)
        with self.connection() as conn:
            cursor = conn.cursor()
            self._execute(
                cursor,
                f"UPDATE finetune_jobs SET {', '.join(updates)} WHERE job_id = %s",
                values,
            )

    def get_latest_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取最新检查点"""
        job = self.get_job(job_id)
        return job.last_checkpoint if job else None

    def _row_to_job(self, row: Dict[str, Any]) -> FinetuneJob:
        status_value = row.get("status") or JobStatus.PENDING.value
        status = JobStatus(status_value) if status_value in JobStatus._value2member_map_ else JobStatus.PENDING
        return FinetuneJob(
            job_id=row["job_id"],
            status=status,
            base_model=row.get("base_model") or "",
            dataset_id=row.get("dataset_id") or "",
            config=json.loads(row["config"]) if row.get("config") else {},
            progress=json.loads(row["progress"]) if row.get("progress") else {},
            last_checkpoint=json.loads(row["last_checkpoint"]) if row.get("last_checkpoint") else None,
            model_path=row.get("model_path"),
            metrics=json.loads(row["metrics"]) if row.get("metrics") else {},
            error_message=row.get("error_message"),
            created_at=row.get("created_at"),
            started_at=row.get("started_at"),
            completed_at=row.get("completed_at"),
        )
