from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from shared.utils.file_store import FileStore

from .pageindex_adapter import PageIndexEvidenceProvider, _default_pageindex_client_factory


ROOT_DIR = Path(__file__).resolve().parents[2]
PAGEINDEX_WORKSPACE_ROOT = ROOT_DIR / "data" / "pageindex_workspace"
PAGEINDEX_DOC_ROOT = ROOT_DIR / "data" / "pageindex_docs"


def _slugify(value: Any, default: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z._-]+", "-", str(value or "").strip()).strip("-").lower()
    return slug or default


def _compact_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _stable_hash(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def _normalize_message_codes(raw_codes: Any) -> List[str]:
    if isinstance(raw_codes, str):
        values = [raw_codes]
    elif isinstance(raw_codes, list):
        values = raw_codes
    else:
        values = []
    result: List[str] = []
    seen = set()
    for item in values:
        code = str(item or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def _normalize_tags(raw_tags: Any) -> List[str]:
    if isinstance(raw_tags, str):
        values = [raw_tags]
    elif isinstance(raw_tags, list):
        values = raw_tags
    else:
        values = []
    tags: List[str] = []
    seen = set()
    for item in values:
        tag = str(item or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tags


def _format_protocol_field_item(raw_field: Any) -> str:
    """Render one protocol-field metadata entry into stable markdown text."""
    if isinstance(raw_field, str):
        return _compact_text(raw_field)
    if isinstance(raw_field, dict):
        field_name = _compact_text(raw_field.get("field_name"))
        meaning = _compact_text(raw_field.get("meaning"))
        formula = _compact_text(raw_field.get("formula"))
        parts = [part for part in [field_name, meaning, formula] if part]
        if parts:
            return " | ".join(parts)
        return _compact_text(json.dumps(raw_field, ensure_ascii=False, sort_keys=True))
    return _compact_text(raw_field)


def _format_protocol_fields(raw_fields: Any) -> str:
    """Normalize protocol_fields metadata into a readable single-line string."""
    if not isinstance(raw_fields, list) or not raw_fields:
        return "N/A"
    rendered: List[str] = []
    seen = set()
    for item in raw_fields:
        text = _format_protocol_field_item(item)
        if not text or text in seen:
            continue
        seen.add(text)
        rendered.append(text)
    return ", ".join(rendered) if rendered else "N/A"


def _filter_blocks(
    blocks: Iterable[Any],
    file_names: Optional[List[str]] = None,
    source_block_ids: Optional[List[int]] = None,
) -> List[Any]:
    allowed_files = {str(item).strip() for item in (file_names or []) if str(item).strip()}
    allowed_block_ids = {int(item) for item in (source_block_ids or [])}
    filtered: List[Any] = []
    for block in blocks:
        if allowed_files and str(getattr(block, "file_name", "") or "").strip() not in allowed_files:
            continue
        block_id = getattr(block, "block_id", None)
        if allowed_block_ids and int(block_id or 0) not in allowed_block_ids:
            continue
        filtered.append(block)
    filtered.sort(
        key=lambda item: (
            str(getattr(item, "file_name", "") or ""),
            int(getattr(item, "page_num", 0) or 0),
            int(getattr(item, "block_id", 0) or 0),
        )
    )
    return filtered


def _group_blocks_by_file(blocks: Iterable[Any]) -> Dict[str, List[Any]]:
    grouped: Dict[str, List[Any]] = {}
    for block in blocks:
        file_name = str(getattr(block, "file_name", "") or "unnamed").strip() or "unnamed"
        grouped.setdefault(file_name, []).append(block)
    return grouped


def _render_document_markdown(
    file_name: str,
    blocks: List[Any],
    protocol_type: str,
    message_codes: List[str],
    tags: List[str],
) -> str:
    body: List[str] = [
        f"# {file_name}",
        "",
        f"- protocol_type: {protocol_type or 'N/A'}",
        f"- message_codes: {', '.join(message_codes) if message_codes else 'N/A'}",
        f"- tags: {', '.join(tags) if tags else 'N/A'}",
        "",
    ]
    for block in blocks:
        page_num = int(getattr(block, "page_num", 0) or 0)
        block_type = str(getattr(block, "block_type", "") or "text").strip() or "text"
        block_id = int(getattr(block, "block_id", 0) or 0)
        metadata = getattr(block, "metadata", {}) or {}
        protocol_fields = metadata.get("protocol_fields") if isinstance(metadata, dict) else None
        content = str(getattr(block, "cleaned_content", None) or getattr(block, "content", "") or "").strip()
        if not content:
            continue
        body.extend(
            [
                f"## Page {page_num or 'N/A'} / Block {block_id or 'N/A'}",
                "",
                f"- block_type: {block_type}",
                f"- protocol_fields: {_format_protocol_fields(protocol_fields)}",
                "",
                content,
                "",
            ]
        )
    return "\n".join(body).strip() + "\n"


def build_protocol_doc_index(
    project_id: str,
    blocks: Iterable[Any],
    dataset_id: str = "",
    protocol_type: str = "",
    message_codes: Optional[List[str]] = None,
    file_names: Optional[List[str]] = None,
    source_block_ids: Optional[List[int]] = None,
    doc_set_id: str = "",
    index_ref: str = "",
    tags: Optional[List[str]] = None,
    rebuild: bool = False,
    file_store: Optional[FileStore] = None,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> Dict[str, Any]:
    """Build and persist a reusable PageIndex registry from project blocks."""
    resolved_project_id = str(project_id or "").strip()
    if not resolved_project_id:
        raise ValueError("project_id不能为空")

    store = file_store or FileStore()
    filtered_blocks = _filter_blocks(blocks, file_names=file_names, source_block_ids=source_block_ids)
    if not filtered_blocks:
        raise ValueError("未找到可用于建立协议文档索引的数据块")

    resolved_doc_set_id = str(doc_set_id or "").strip() or f"docset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    resolved_index_ref = str(index_ref or "").strip() or f"idx_{int(time.time())}"
    resolved_protocol_type = str(protocol_type or "").strip()
    resolved_message_codes = _normalize_message_codes(message_codes)
    resolved_tags = _normalize_tags(tags)

    existing_registry = store.load_pageindex_registry(resolved_project_id, resolved_doc_set_id)
    if existing_registry and not rebuild:
        return existing_registry

    workspace_dir = PAGEINDEX_WORKSPACE_ROOT / resolved_project_id / resolved_doc_set_id
    docs_dir = PAGEINDEX_DOC_ROOT / resolved_project_id / resolved_doc_set_id
    workspace_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    client = (client_factory or _default_pageindex_client_factory)(workspace_dir)
    grouped_blocks = _group_blocks_by_file(filtered_blocks)
    documents: List[Dict[str, Any]] = []

    for file_name, file_blocks in grouped_blocks.items():
        markdown = _render_document_markdown(
            file_name=file_name,
            blocks=file_blocks,
            protocol_type=resolved_protocol_type,
            message_codes=resolved_message_codes,
            tags=resolved_tags,
        )
        file_hash = _stable_hash(markdown)
        normalized_name = f"{_slugify(file_name, 'document')}_{file_hash[:12]}.md"
        normalized_path = docs_dir / normalized_name
        normalized_path.write_text(markdown, encoding="utf-8")
        doc_id = client.index(str(normalized_path), mode="md")
        documents.append(
            {
                "doc_id": str(doc_id),
                "file_name": file_name,
                "normalized_path": str(normalized_path),
                "file_hash": file_hash,
                "protocol_type": resolved_protocol_type or None,
                "message_codes": list(resolved_message_codes),
                "tags": list(resolved_tags),
                "source_block_ids": [int(getattr(block, "block_id", 0) or 0) for block in file_blocks],
                "page_range": sorted({int(getattr(block, "page_num", 0) or 0) for block in file_blocks}),
                "status": "indexed",
            }
        )

    doc_set_payload = {
        "project_id": resolved_project_id,
        "dataset_id": str(dataset_id or "").strip() or None,
        "doc_set_id": resolved_doc_set_id,
        "index_ref": resolved_index_ref,
        "protocol_type": resolved_protocol_type or None,
        "message_codes": list(resolved_message_codes),
        "tags": list(resolved_tags),
        "document_count": len(documents),
        "documents": [
            {
                "doc_id": item["doc_id"],
                "file_name": item["file_name"],
                "file_hash": item["file_hash"],
                "source_block_ids": item["source_block_ids"],
            }
            for item in documents
        ],
        "created_at": datetime_now_iso(),
    }
    store.save_project_doc_set(resolved_project_id, resolved_doc_set_id, doc_set_payload)

    registry = {
        "project_id": resolved_project_id,
        "dataset_id": str(dataset_id or "").strip() or None,
        "doc_set_id": resolved_doc_set_id,
        "index_ref": resolved_index_ref,
        "status": "ready",
        "protocol_type": resolved_protocol_type or None,
        "message_codes": list(resolved_message_codes),
        "tags": list(resolved_tags),
        "workspace_dir": str(workspace_dir),
        "docs_dir": str(docs_dir),
        "document_count": len(documents),
        "documents": documents,
        "created_at": datetime_now_iso(),
        "updated_at": datetime_now_iso(),
    }
    store.save_pageindex_registry(resolved_project_id, resolved_doc_set_id, registry)

    if dataset_id:
        store.update_dataset_meta(
            str(dataset_id).strip(),
            {
                "doc_set_id": resolved_doc_set_id,
                "index_ref": resolved_index_ref,
                "protocol_type": resolved_protocol_type or None,
                "message_codes": list(resolved_message_codes),
            },
        )
    return registry


def datetime_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


class TrainedDocEvidenceProvider(PageIndexEvidenceProvider):
    """Reuse training-stage PageIndex registries during rule generation."""

    def __init__(
        self,
        project_id: str = "",
        dataset_id: str = "",
        doc_set_id: str = "",
        index_ref: str = "",
        file_store: Optional[FileStore] = None,
        client_factory: Optional[Callable[[Path], Any]] = None,
    ):
        self.file_store = file_store or FileStore()
        self.registry = self.file_store.resolve_pageindex_registry(
            project_id=project_id,
            dataset_id=dataset_id,
            doc_set_id=doc_set_id,
            index_ref=index_ref,
        )
        workspace_dir = self.registry.get("workspace_dir") if isinstance(self.registry, dict) else None
        docs_dir = self.registry.get("docs_dir") if isinstance(self.registry, dict) else None
        super().__init__(
            workspace_dir=Path(workspace_dir) if workspace_dir else None,
            docs_dir=Path(docs_dir) if docs_dir else None,
            client_factory=client_factory,
        )

    def collect_evidence(
        self,
        source_protocol: Dict[str, Any],
        target_protocol: Dict[str, Any],
        source_message: Optional[Any] = None,
        max_snippets_per_role: int = 3,
    ) -> Dict[str, Any]:
        if not self.registry:
            return {
                "status": "unavailable",
                "reason": "trained_doc_registry_not_found",
                "evidence_snippets": [],
                "evidence_snippet_count": 0,
            }
        try:
            client = self._get_client()
        except Exception as exc:
            return {
                "status": "unavailable",
                "reason": str(exc),
                "evidence_snippets": [],
                "evidence_snippet_count": 0,
            }

        source_queries = self._extract_source_queries(source_protocol, source_message)
        target_queries = self._extract_target_queries(target_protocol)
        snippets = self._collect_source_registry_snippets(
            client=client,
            source_protocol=source_protocol,
            queries=source_queries,
            top_k=max_snippets_per_role,
        )

        if target_queries:
            target_doc_id = self._get_or_create_document(client, "target", target_protocol)
            if target_doc_id:
                snippets.extend(
                    self._collect_role_snippets(
                        client=client,
                        doc_id=target_doc_id,
                        role="target",
                        protocol=target_protocol,
                        queries=target_queries,
                        top_k=max_snippets_per_role,
                    )
                )

        snippets.sort(key=lambda item: (-float(item.get("score") or 0), str(item.get("title") or "")))
        limited_snippets = snippets[: max(1, max_snippets_per_role * 2)]
        return {
            "status": "used" if limited_snippets else "fallback",
            "reason": None,
            "evidence_snippets": limited_snippets,
            "evidence_snippet_count": len(limited_snippets),
            "candidate_doc_count": len(self._filter_registry_documents(source_protocol)),
            "matched_doc_ids": sorted({str(item.get("doc_id") or "") for item in limited_snippets if item.get("doc_id")}),
            "doc_set_id": self.registry.get("doc_set_id"),
            "index_ref": self.registry.get("index_ref"),
        }

    def _filter_registry_documents(self, source_protocol: Dict[str, Any]) -> List[Dict[str, Any]]:
        documents = list((self.registry.get("documents") or [])) if isinstance(self.registry, dict) else []
        protocol_type = str(source_protocol.get("protocol_type") or "").strip()
        message_code = str(source_protocol.get("message_code") or "").strip()
        filtered = documents
        if protocol_type:
            protocol_filtered = [
                doc for doc in filtered if str(doc.get("protocol_type") or "").strip() in {"", protocol_type}
            ]
            if protocol_filtered:
                filtered = protocol_filtered
        if message_code:
            message_filtered = []
            for doc in filtered:
                codes = _normalize_message_codes(doc.get("message_codes"))
                if not codes or message_code in codes:
                    message_filtered.append(doc)
            if message_filtered:
                filtered = message_filtered
        return filtered or documents

    def _collect_source_registry_snippets(
        self,
        client: Any,
        source_protocol: Dict[str, Any],
        queries: List[str],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        for document in self._filter_registry_documents(source_protocol):
            doc_id = str(document.get("doc_id") or "").strip()
            if not doc_id:
                continue
            role_snippets = self._collect_role_snippets(
                client=client,
                doc_id=doc_id,
                role="source",
                protocol=source_protocol,
                queries=queries,
                top_k=top_k,
            )
            for item in role_snippets:
                item["doc_id"] = doc_id
                item["file_name"] = document.get("file_name")
            ranked.extend(role_snippets)
        ranked.sort(key=lambda item: (-float(item.get("score") or 0), str(item.get("title") or "")))
        return ranked[:top_k]


def get_trained_doc_evidence_provider(
    project_id: str = "",
    dataset_id: str = "",
    doc_set_id: str = "",
    index_ref: str = "",
    file_store: Optional[FileStore] = None,
    client_factory: Optional[Callable[[Path], Any]] = None,
) -> TrainedDocEvidenceProvider:
    return TrainedDocEvidenceProvider(
        project_id=project_id,
        dataset_id=dataset_id,
        doc_set_id=doc_set_id,
        index_ref=index_ref,
        file_store=file_store,
        client_factory=client_factory,
    )
