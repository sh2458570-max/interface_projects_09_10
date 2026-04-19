from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import textwrap
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
PAGEINDEX_WORKSPACE_DIR = ROOT_DIR / "data" / "pageindex_workspace"
PAGEINDEX_DOC_DIR = ROOT_DIR / "data" / "pageindex_docs"
FIELD_PATTERN = re.compile(r"\b[A-Z][A-Z0-9_]{2,}\b")


def _normalize_identifier(value: Any) -> str:
    return str(value or "").strip().upper()


def _split_identifier_tokens(value: str) -> List[str]:
    return [token for token in re.split(r"[_\W]+", value.upper()) if len(token) >= 2]


def _truncate(text: str, limit: int = 500) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _safe_json_loads(raw: Any, fallback: Any) -> Any:
    if raw is None:
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _extract_field_candidates(text: str, limit: int = 20) -> List[str]:
    fields: List[str] = []
    seen = set()
    for match in FIELD_PATTERN.finditer(str(text or "")):
        token = match.group(0).upper()
        if token in seen:
            continue
        seen.add(token)
        fields.append(token)
        if len(fields) >= limit:
            break
    return fields


def _normalize_query_list(raw_queries: Any, limit: int = 20) -> List[str]:
    if not isinstance(raw_queries, list):
        return []
    queries: List[str] = []
    seen = set()
    for item in raw_queries:
        if isinstance(item, dict):
            candidate = str(item.get("field_name") or item.get("name") or "").strip()
        else:
            candidate = str(item or "").strip()
        normalized = _normalize_identifier(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        queries.append(normalized)
        if len(queries) >= limit:
            break
    return queries


def _iter_paragraphs(text: str, max_chars: int = 800) -> Iterable[str]:
    normalized = str(text or "").replace("\r\n", "\n")
    blocks = [block.strip() for block in re.split(r"\n\s*\n", normalized) if block.strip()]
    if not blocks:
        blocks = [normalized.strip()] if normalized.strip() else []
    for block in blocks:
        if len(block) <= max_chars:
            yield block
            continue
        for chunk in textwrap.wrap(block, width=max_chars, break_long_words=False, break_on_hyphens=False):
            if chunk.strip():
                yield chunk.strip()


def _extract_markdown_title(text: str, fallback: str) -> str:
    for line in str(text or "").splitlines():
        match = re.match(r"^\s*#\s+(.+?)\s*$", line)
        if match:
            return match.group(1).strip()
    return str(fallback or "Untitled").strip() or "Untitled"


def _build_doc_description(text: str, fallback: str) -> str:
    cleaned = re.sub(r"<think>.*?</think>", "", str(text or ""), flags=re.DOTALL).strip()
    for block in _iter_paragraphs(cleaned, max_chars=240):
        normalized = re.sub(r"^\s*#+\s*", "", block).strip()
        if normalized:
            return _truncate(normalized, 240)
    return _truncate(fallback, 240)


def _flatten_workspace_nodes(nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for node in nodes:
        flattened.append(node)
        children = node.get("nodes") or []
        if isinstance(children, list) and children:
            flattened.extend(_flatten_workspace_nodes(children))
    return flattened


def _build_markdown_structure(text: str, fallback_title: str) -> List[Dict[str, Any]]:
    lines = str(text or "").splitlines()
    doc_title = _extract_markdown_title(text, fallback_title)
    heading_positions: List[Dict[str, Any]] = []
    for index, line in enumerate(lines):
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if not match:
            continue
        heading_positions.append(
            {
                "level": len(match.group(1)),
                "title": match.group(2).strip(),
                "start": index,
            }
        )

    root_text = "\n".join(lines[: min(len(lines), 12)]).strip() or str(text or "").strip()
    root: Dict[str, Any] = {
        "title": doc_title,
        "node_id": "0000",
        "line_num": 1,
        "physical_index": 1,
        "text": root_text,
        "summary": _truncate(root_text or doc_title, 400),
        "nodes": [],
        "prefix_summary": root_text,
    }

    if not heading_positions:
        root["text"] = str(text or "").strip()
        root["summary"] = _truncate(root["text"] or doc_title, 400)
        return [root]

    stack: List[tuple[int, Dict[str, Any]]] = [(0, root)]
    for node_index, heading in enumerate(heading_positions, start=1):
        next_start = heading_positions[node_index]["start"] if node_index < len(heading_positions) else len(lines)
        section_text = "\n".join(lines[heading["start"] : next_start]).strip()
        node = {
            "title": heading["title"] or f"Section {node_index}",
            "node_id": f"{node_index:04d}",
            "line_num": heading["start"] + 1,
            "physical_index": heading["start"] + 1,
            "text": section_text,
            "summary": _truncate(section_text or heading["title"], 600),
            "nodes": [],
        }
        while stack and stack[-1][0] >= int(heading["level"]):
            stack.pop()
        parent = stack[-1][1] if stack else root
        parent.setdefault("nodes", []).append(node)
        stack.append((int(heading["level"]), node))
    return [root]


class LocalPageIndexClient:
    """Local fallback PageIndex-compatible client backed by Markdown files."""

    def __init__(self, workspace: str = "", **kwargs):
        self.workspace = Path(workspace or PAGEINDEX_WORKSPACE_DIR).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.meta_file = self.workspace / "_meta.json"
        self.kwargs = kwargs

    def index(self, file_path: str, mode: str = "md") -> str:
        resolved_path = Path(file_path).resolve()
        text = resolved_path.read_text(encoding="utf-8")
        doc_id = hashlib.sha256(f"{resolved_path}::{text}".encode("utf-8")).hexdigest()[:32]
        structure = _build_markdown_structure(text, fallback_title=resolved_path.stem)
        payload = {
            "id": doc_id,
            "type": str(mode or "md"),
            "path": str(resolved_path),
            "doc_name": resolved_path.stem,
            "doc_description": _build_doc_description(text, resolved_path.stem),
            "line_count": max(len(text.splitlines()), 1),
            "structure": structure,
        }
        (self.workspace / f"{doc_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        meta = self._load_meta()
        meta[doc_id] = {
            "type": payload["type"],
            "doc_name": payload["doc_name"],
            "doc_description": payload["doc_description"],
            "path": payload["path"],
            "line_count": payload["line_count"],
        }
        self.meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return doc_id

    def get_document_structure(self, doc_id: str) -> str:
        payload = self._load_document(doc_id)
        return json.dumps(payload.get("structure") or [], ensure_ascii=False)

    def get_page_content(self, doc_id: str, pages: str) -> str:
        payload = self._load_document(doc_id)
        refs = [item.strip() for item in str(pages or "").split(",") if item.strip()]
        nodes = _flatten_workspace_nodes(payload.get("structure") or [])
        results: List[Dict[str, Any]] = []
        seen = set()

        for ref in refs:
            matched = False
            for node in nodes:
                page_ref = str(node.get("physical_index") or node.get("line_num") or "").strip()
                if not page_ref or page_ref != ref:
                    continue
                content = str(node.get("text") or node.get("summary") or "").strip()
                if not content:
                    continue
                signature = (page_ref, content)
                if signature in seen:
                    continue
                seen.add(signature)
                results.append({"page": page_ref, "content": content})
                matched = True
            if matched:
                continue

            fallback_content = self._slice_content_by_line(payload, ref)
            if fallback_content:
                signature = (ref, fallback_content)
                if signature not in seen:
                    seen.add(signature)
                    results.append({"page": ref, "content": fallback_content})
        return json.dumps(results, ensure_ascii=False)

    def _load_meta(self) -> Dict[str, Any]:
        if not self.meta_file.exists():
            return {}
        try:
            return json.loads(self.meta_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _load_document(self, doc_id: str) -> Dict[str, Any]:
        payload_path = self.workspace / f"{doc_id}.json"
        if not payload_path.exists():
            raise KeyError(f"pageindex document not found: {doc_id}")
        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"pageindex document is invalid: {doc_id}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"pageindex document is invalid: {doc_id}")
        return payload

    def _slice_content_by_line(self, payload: Dict[str, Any], line_ref: str) -> str:
        try:
            line_num = max(int(str(line_ref).strip()), 1)
        except (TypeError, ValueError):
            return ""
        path = Path(str(payload.get("path") or "").strip())
        if not path.exists():
            return ""
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return ""
        start = max(line_num - 1, 0)
        snippet = "\n".join(lines[start : min(start + 20, len(lines))]).strip()
        return snippet


def _default_pageindex_client_factory(workspace: Path):
    client_cls = _import_pageindex_client()
    client_kwargs = {"workspace": str(workspace)}
    api_key = str(os.getenv("PAGEINDEX_API_KEY") or "").strip()
    model = str(os.getenv("PAGEINDEX_MODEL") or "").strip()
    retrieve_model = str(os.getenv("PAGEINDEX_RETRIEVE_MODEL") or "").strip()
    if api_key:
        client_kwargs["api_key"] = api_key
    if model:
        client_kwargs["model"] = model
    if retrieve_model:
        client_kwargs["retrieve_model"] = retrieve_model
    if client_cls is None:
        return LocalPageIndexClient(**client_kwargs)
    return client_cls(**client_kwargs)


def _import_pageindex_client():
    try:
        from pageindex.client import PageIndexClient

        return PageIndexClient
    except ImportError:
        pass

    pageindex_root = os.getenv("PAGEINDEX_ROOT")
    if not pageindex_root:
        return None
    root_path = os.path.abspath(os.path.expanduser(pageindex_root))
    if root_path not in sys.path:
        sys.path.insert(0, root_path)
    try:
        from pageindex.client import PageIndexClient

        return PageIndexClient
    except ImportError:
        return None


class PageIndexEvidenceProvider:
    """Collect field-oriented evidence snippets using an optional PageIndex workspace."""

    def __init__(
        self,
        workspace_dir: Optional[Path] = None,
        docs_dir: Optional[Path] = None,
        client_factory: Optional[Callable[[Path], Any]] = None,
    ):
        self.workspace_dir = Path(workspace_dir or PAGEINDEX_WORKSPACE_DIR)
        self.docs_dir = Path(docs_dir or PAGEINDEX_DOC_DIR)
        self.meta_file = self.workspace_dir / "_protocol_meta.json"
        self.client_factory = client_factory or _default_pageindex_client_factory
        self._client = None
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        self.docs_dir.mkdir(parents=True, exist_ok=True)

    def is_available(self) -> bool:
        try:
            self._get_client()
        except Exception:
            return False
        return True

    def collect_evidence(
        self,
        source_protocol: Dict[str, Any],
        target_protocol: Dict[str, Any],
        source_message: Optional[Any] = None,
        max_snippets_per_role: int = 3,
    ) -> Dict[str, Any]:
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
        snippets: List[Dict[str, Any]] = []

        for role, protocol, queries in (
            ("source", source_protocol, source_queries),
            ("target", target_protocol, target_queries),
        ):
            if not queries:
                continue
            doc_id = self._get_or_create_document(client, role, protocol)
            if not doc_id:
                continue
            snippets.extend(
                self._collect_role_snippets(
                    client=client,
                    doc_id=doc_id,
                    role=role,
                    protocol=protocol,
                    queries=queries,
                    top_k=max_snippets_per_role,
                )
            )

        status = "used" if snippets else "miss"
        return {
            "status": status,
            "reason": None,
            "evidence_snippets": snippets,
            "evidence_snippet_count": len(snippets),
            "source_query_count": len(source_queries),
            "target_query_count": len(target_queries),
        }

    def _get_client(self):
        if self._client is None:
            self._client = self.client_factory(self.workspace_dir)
        return self._client

    def _load_meta(self) -> Dict[str, Any]:
        if not self.meta_file.exists():
            return {}
        try:
            return json.loads(self.meta_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_meta(self, payload: Dict[str, Any]) -> None:
        self.meta_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _protocol_key(self, role: str, protocol: Dict[str, Any]) -> str:
        content = protocol.get("content") or ""
        raw = "||".join(
            [
                role,
                str(protocol.get("protocol_type") or ""),
                str(protocol.get("message_code") or ""),
                str(protocol.get("name") or ""),
                str(content),
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _materialize_protocol_markdown(self, role: str, protocol: Dict[str, Any], key: str) -> Path:
        title = protocol.get("name") or protocol.get("protocol_type") or f"{role} protocol"
        content = str(protocol.get("content") or "").strip()
        body = [
            f"# {title}",
            "",
            f"- role: {role}",
            f"- protocol_type: {protocol.get('protocol_type') or 'N/A'}",
            f"- message_code: {protocol.get('message_code') or 'N/A'}",
            "",
            "## Protocol Definition",
            "",
            content,
            "",
        ]
        path = self.docs_dir / f"{role}_{key}.md"
        path.write_text("\n".join(body), encoding="utf-8")
        return path

    def _get_or_create_document(self, client: Any, role: str, protocol: Dict[str, Any]) -> Optional[str]:
        key = self._protocol_key(role, protocol)
        meta = self._load_meta()
        entry = meta.get(key)
        if isinstance(entry, dict) and entry.get("doc_id"):
            return str(entry["doc_id"])

        file_path = self._materialize_protocol_markdown(role, protocol, key)
        try:
            doc_id = client.index(str(file_path), mode="md")
        except Exception:
            return None
        meta[key] = {
            "doc_id": doc_id,
            "role": role,
            "file_path": str(file_path),
            "protocol_type": protocol.get("protocol_type"),
            "message_code": protocol.get("message_code"),
            "name": protocol.get("name"),
        }
        self._save_meta(meta)
        return doc_id

    def _extract_source_queries(self, source_protocol: Dict[str, Any], source_message: Optional[Any]) -> List[str]:
        explicit_queries = _normalize_query_list(source_protocol.get("field_queries"))
        if explicit_queries:
            return explicit_queries
        if isinstance(source_message, dict) and source_message:
            return [_normalize_identifier(key) for key in source_message.keys() if _normalize_identifier(key)]
        return _extract_field_candidates(str(source_protocol.get("content") or ""), limit=12)

    def _extract_target_queries(self, target_protocol: Dict[str, Any]) -> List[str]:
        explicit_queries = _normalize_query_list(
            target_protocol.get("field_queries") or target_protocol.get("required_target_fields")
        )
        if explicit_queries:
            return explicit_queries
        return _extract_field_candidates(str(target_protocol.get("content") or ""), limit=16)

    def _flatten_structure_nodes(self, nodes: List[Dict[str, Any]], path: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        flattened: List[Dict[str, Any]] = []
        breadcrumbs = list(path or [])
        for node in nodes:
            title = str(node.get("title") or "").strip()
            current_path = breadcrumbs + ([title] if title else [])
            flattened.append(
                {
                    "title": title or "Untitled",
                    "summary": _truncate(node.get("summary") or "", 300),
                    "page_ref": node.get("physical_index") or node.get("line_num"),
                    "section_path": " / ".join(item for item in current_path if item),
                }
            )
            child_nodes = node.get("nodes") or []
            if isinstance(child_nodes, list) and child_nodes:
                flattened.extend(self._flatten_structure_nodes(child_nodes, current_path))
        return flattened

    def _get_page_content(self, client: Any, doc_id: str, page_ref: Any) -> str:
        if page_ref in (None, ""):
            return ""
        raw = client.get_page_content(doc_id, str(page_ref))
        payload = _safe_json_loads(raw, [])
        if not isinstance(payload, list):
            return ""
        texts = [str(item.get("content") or "").strip() for item in payload if isinstance(item, dict)]
        return "\n".join(text for text in texts if text).strip()

    def _collect_candidates(self, client: Any, doc_id: str, protocol: Dict[str, Any]) -> List[Dict[str, Any]]:
        structure_payload = _safe_json_loads(client.get_document_structure(doc_id), [])
        candidates: List[Dict[str, Any]] = []
        seen = set()

        if isinstance(structure_payload, list):
            for node in self._flatten_structure_nodes(structure_payload):
                content = self._get_page_content(client, doc_id, node.get("page_ref"))
                snippet = _truncate(content or node.get("summary") or node.get("title") or "", 600)
                signature = (node.get("section_path"), snippet)
                if not snippet or signature in seen:
                    continue
                seen.add(signature)
                candidates.append(
                    {
                        "title": node.get("section_path") or node.get("title") or "Untitled",
                        "content": snippet,
                    }
                )

        for index, paragraph in enumerate(_iter_paragraphs(str(protocol.get("content") or "")), start=1):
            signature = ("raw", paragraph)
            if signature in seen:
                continue
            seen.add(signature)
            candidates.append(
                {
                    "title": f"Raw Paragraph {index}",
                    "content": _truncate(paragraph, 600),
                }
            )
        return candidates

    def _score_candidate(self, candidate: Dict[str, Any], queries: List[str]) -> float:
        haystack = f"{candidate.get('title')}\n{candidate.get('content')}".upper()
        title_upper = str(candidate.get("title") or "").upper()
        score = 0.0
        for query in queries:
            query_upper = _normalize_identifier(query)
            if not query_upper:
                continue
            if query_upper in title_upper:
                score += 4.0
            if query_upper in haystack:
                score += 2.0
            token_hits = sum(1 for token in _split_identifier_tokens(query_upper) if token and token in haystack)
            score += min(token_hits, 4) * 0.4
        return score

    def _collect_role_snippets(
        self,
        client: Any,
        doc_id: str,
        role: str,
        protocol: Dict[str, Any],
        queries: List[str],
        top_k: int,
    ) -> List[Dict[str, Any]]:
        ranked: List[Dict[str, Any]] = []
        for candidate in self._collect_candidates(client, doc_id, protocol):
            score = self._score_candidate(candidate, queries)
            if score <= 0:
                continue
            matched_query = next(
                (query for query in queries if _normalize_identifier(query) in str(candidate.get("content") or "").upper() or _normalize_identifier(query) in str(candidate.get("title") or "").upper()),
                queries[0],
            )
            ranked.append(
                {
                    "role": role,
                    "query": matched_query,
                    "title": candidate.get("title"),
                    "content": candidate.get("content"),
                    "score": round(score, 2),
                    "protocol_name": protocol.get("name") or protocol.get("protocol_type"),
                }
            )
        ranked.sort(key=lambda item: (-float(item.get("score") or 0), str(item.get("title") or "")))
        return ranked[:top_k]


def get_pageindex_evidence_provider() -> PageIndexEvidenceProvider:
    return PageIndexEvidenceProvider()
