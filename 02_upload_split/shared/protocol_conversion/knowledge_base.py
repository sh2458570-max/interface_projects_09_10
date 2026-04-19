from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from neo4j import GraphDatabase
except ImportError:  # pragma: no cover - optional dependency
    GraphDatabase = None


KB_DIR = Path(__file__).resolve().parents[2] / "data" / "protocol_conversion_kb"
GRAPH_FILE_MAP = {
    "link16": "link16_value_graph.json",
}
LEGACY_FILE_MAP = {
    "link16": "link16_conversion_rules.json",
}
CONCEPT_SUFFIXES = (
    "_LABEL",
    "_CODE",
    "_RAW",
    "_VALUE",
    "_FT",
    "_M",
    "_KM",
    "_DEG",
    "_RAD",
    "_MPS",
    "_KTS",
    "_HZ",
    "_SEC",
    "_MS",
    "_MIN",
)
FORMULA_BLOCK_PATTERN = re.compile(r"(?:\n|^)(?:if\s+|for\s+|while\s+|result\s*=)", re.IGNORECASE)
MAPPING_TABLE_PATTERN = re.compile(r"-?\d+(?:\.\d+)?\s*(?:=|->|→)\s*[^,;\n]+")
TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in TRUTHY_VALUES


def _csv_env(name: str, default: List[str]) -> List[str]:
    value = str(os.getenv(name) or "").strip()
    if not value:
        return list(default)
    items = [str(item).strip().lower() for item in value.split(",")]
    return [item for item in items if item]


@dataclass
class KnowledgeGraphSettings:
    """Runtime configuration for protocol-conversion knowledge graph backends."""

    protocol_type: str = "Link16"
    backend: str = "local_json_graph"
    enabled: bool = False
    uri: str = ""
    username: str = "neo4j"
    password: str = ""
    database: str = "neo4j"
    timeout_seconds: float = 5.0
    auto_init: bool = True
    json_fallback: bool = True
    write_fallback_json: bool = True
    read_statuses: List[str] = field(default_factory=lambda: ["approved", "verified"])
    default_write_status: str = "candidate"

    @classmethod
    def from_env(cls, protocol_type: str = "Link16") -> "KnowledgeGraphSettings":
        """Build graph settings from environment variables."""
        backend = str(os.getenv("PROTOCOL_CONVERSION_GRAPH_BACKEND") or "auto").strip().lower() or "auto"
        uri = str(os.getenv("PROTOCOL_CONVERSION_NEO4J_URI") or os.getenv("NEO4J_URI") or "").strip()
        enabled = _env_flag("PROTOCOL_CONVERSION_NEO4J_ENABLED", default=backend in {"auto", "neo4j"} and bool(uri))
        if backend in {"local_json", "json"}:
            enabled = False
        elif backend == "neo4j":
            enabled = bool(uri)

        return cls(
            protocol_type=str(protocol_type or "Link16").strip() or "Link16",
            backend="neo4j_graph" if enabled else "local_json_graph",
            enabled=enabled,
            uri=uri,
            username=str(os.getenv("PROTOCOL_CONVERSION_NEO4J_USERNAME") or os.getenv("NEO4J_USERNAME") or "neo4j").strip() or "neo4j",
            password=str(os.getenv("PROTOCOL_CONVERSION_NEO4J_PASSWORD") or os.getenv("NEO4J_PASSWORD") or "").strip(),
            database=str(os.getenv("PROTOCOL_CONVERSION_NEO4J_DATABASE") or os.getenv("NEO4J_DATABASE") or "neo4j").strip() or "neo4j",
            timeout_seconds=float(os.getenv("PROTOCOL_CONVERSION_NEO4J_TIMEOUT_SECONDS") or "5.0"),
            auto_init=_env_flag("PROTOCOL_CONVERSION_NEO4J_AUTO_INIT", default=True),
            json_fallback=_env_flag("PROTOCOL_CONVERSION_JSON_FALLBACK", default=True),
            write_fallback_json=_env_flag("PROTOCOL_CONVERSION_WRITE_FALLBACK_JSON", default=True),
            read_statuses=_csv_env("PROTOCOL_CONVERSION_NEO4J_READ_STATUSES", ["approved", "verified"]),
            default_write_status=str(os.getenv("PROTOCOL_CONVERSION_NEO4J_WRITE_STATUS") or "candidate").strip().lower() or "candidate",
        )


@dataclass
class KnowledgeRule:
    """One value-to-value conversion rule resolved from the knowledge graph."""

    protocol_type: str
    message_code: Optional[str]
    field_name: str
    conversion_mode: str
    formula: str
    target_field: Optional[str]
    unit: Optional[str]
    aliases: List[str]
    source: str
    description: Optional[str] = None
    bit_length: Optional[int] = None
    source_fields: List[str] = field(default_factory=list)
    target_protocol_type: Optional[str] = None
    target_message_code: Optional[str] = None
    concept_name: Optional[str] = None
    edge_id: Optional[str] = None
    formula_kind: Optional[str] = None
    confidence: Optional[float] = None
    status: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "protocol_type": self.protocol_type,
            "message_code": self.message_code,
            "field_name": self.field_name,
            "source_fields": list(self.source_fields or [self.field_name]),
            "conversion_mode": self.conversion_mode,
            "formula": self.formula,
            "target_field": self.target_field,
            "target_protocol_type": self.target_protocol_type,
            "target_message_code": self.target_message_code,
            "unit": self.unit,
            "aliases": list(self.aliases),
            "source": self.source,
            "description": self.description,
            "bit_length": self.bit_length,
            "concept_name": self.concept_name,
            "edge_id": self.edge_id,
            "formula_kind": self.formula_kind,
            "confidence": self.confidence,
            "status": self.status,
        }


class ProtocolConversionKnowledgeBase:
    """Protocol conversion knowledge graph backed by a local JSON store."""

    def __init__(self, protocol_type: str, payload: Dict[str, Any], file_path: Path):
        self.protocol_type = str(protocol_type or payload.get("protocol_type") or "Link16")
        self.payload = payload
        self.file_path = file_path
        self.embedding_model = str(payload.get("embedding_model") or "qwen3-0.6b-embedding")
        self.version = str(payload.get("version") or "graph-v1")
        self.backend = str(payload.get("backend") or "local_json_graph")
        self.description = str(payload.get("description") or "")
        self._concepts = payload.get("concepts") or []
        self._field_nodes = payload.get("field_nodes") or []
        self._edges = payload.get("edges") or []
        self._concept_by_id = {
            str(item.get("concept_id") or ""): item for item in self._concepts if item.get("concept_id")
        }
        self._field_by_id = {
            str(item.get("node_id") or ""): item for item in self._field_nodes if item.get("node_id")
        }
        self._field_index = self._build_field_index(self._field_nodes)

    @classmethod
    def load(cls, protocol_type: str) -> "ProtocolConversionKnowledgeBase":
        """Load the preferred knowledge-base backend with safe fallback."""
        local_backend = cls.load_local(protocol_type)
        settings = KnowledgeGraphSettings.from_env(protocol_type=protocol_type)
        if not settings.enabled:
            return local_backend
        if GraphDatabase is None or not settings.uri or not settings.password:
            return local_backend

        try:
            neo4j_backend = Neo4jProtocolConversionKnowledgeBase(protocol_type=protocol_type, settings=settings)
        except Exception:
            return local_backend

        if settings.json_fallback:
            return CompositeProtocolConversionKnowledgeBase(
                protocol_type=protocol_type,
                primary=neo4j_backend,
                fallback=local_backend,
            )
        return neo4j_backend

    @classmethod
    def load_local(cls, protocol_type: str) -> "ProtocolConversionKnowledgeBase":
        """Load the legacy local JSON graph backend only."""
        normalized = str(protocol_type or "link16").strip().lower()
        graph_file = KB_DIR / GRAPH_FILE_MAP.get(normalized, GRAPH_FILE_MAP["link16"])
        if graph_file.exists():
            payload = json.loads(graph_file.read_text(encoding="utf-8"))
            return cls(protocol_type=str(payload.get("protocol_type") or protocol_type), payload=payload, file_path=graph_file)

        legacy_file = KB_DIR / LEGACY_FILE_MAP.get(normalized, LEGACY_FILE_MAP["link16"])
        payload = cls._bootstrap_graph_payload(protocol_type=protocol_type, legacy_file=legacy_file)
        graph_file.parent.mkdir(parents=True, exist_ok=True)
        graph_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return cls(protocol_type=str(payload.get("protocol_type") or protocol_type), payload=payload, file_path=graph_file)

    @classmethod
    def load_neo4j(
        cls,
        protocol_type: str,
        settings: Optional[KnowledgeGraphSettings] = None,
    ) -> "Neo4jProtocolConversionKnowledgeBase":
        """Load the Neo4j graph backend explicitly."""
        return Neo4jProtocolConversionKnowledgeBase(
            protocol_type=protocol_type,
            settings=settings or KnowledgeGraphSettings.from_env(protocol_type=protocol_type),
        )

    @staticmethod
    def _normalize_field_name(value: Any) -> str:
        return str(value or "").strip().upper()

    @staticmethod
    def _normalize_message_code(value: Any) -> Optional[str]:
        cleaned = str(value or "").strip().upper()
        return cleaned or None

    @staticmethod
    def _normalize_protocol_type(value: Any) -> str:
        return str(value or "Link16").strip() or "Link16"

    @staticmethod
    def _normalize_status(value: Any, default: str = "approved") -> str:
        cleaned = str(value or "").strip().lower()
        return cleaned or default

    @staticmethod
    def _infer_formula_kind(formula: str) -> str:
        text = str(formula or "").strip()
        if FORMULA_BLOCK_PATTERN.search(text):
            return "python_block"
        if MAPPING_TABLE_PATTERN.search(text) and not any(token in text for token in ("if ", "for ", "result =", "+", "*", "/")):
            return "mapping_table"
        return "python_expr"

    @classmethod
    def _infer_concept_name(cls, source_field: str, target_field: Optional[str]) -> str:
        base = cls._normalize_field_name(target_field) or cls._normalize_field_name(source_field)
        for suffix in CONCEPT_SUFFIXES:
            if base.endswith(suffix) and len(base) > len(suffix) + 2:
                return base[: -len(suffix)]
        if base.endswith("_DISCRETE") and len(base) > 11:
            return base[:-9]
        return base

    @staticmethod
    def _concept_id(name: str) -> str:
        normalized = re.sub(r"[^A-Z0-9]+", "_", str(name or "").strip().upper()).strip("_") or "UNKNOWN"
        return f"concept::{normalized}"

    @classmethod
    def _field_node_id(cls, protocol_type: str, message_code: Optional[str], field_name: str) -> str:
        protocol = cls._normalize_protocol_type(protocol_type).upper()
        message = cls._normalize_message_code(message_code) or "ANY"
        field = cls._normalize_field_name(field_name)
        return f"field::{protocol}::{message}::{field}"

    @staticmethod
    def _edge_id(source_node_id: str, target_node_id: str, formula: str, conversion_mode: str) -> str:
        digest = hashlib.md5(f"{source_node_id}|{target_node_id}|{conversion_mode}|{formula}".encode("utf-8")).hexdigest()[:12]
        return f"edge::{digest}"

    @classmethod
    def _rule_node_id(
        cls,
        source_protocol_type: str,
        source_message_code: Optional[str],
        target_protocol_type: str,
        target_message_code: Optional[str],
        target_field: str,
        source_fields: Iterable[str],
        formula: str,
        conversion_mode: str,
    ) -> str:
        fingerprint = {
            "source_protocol_type": cls._normalize_protocol_type(source_protocol_type),
            "source_message_code": cls._normalize_message_code(source_message_code),
            "target_protocol_type": cls._normalize_protocol_type(target_protocol_type),
            "target_message_code": cls._normalize_message_code(target_message_code),
            "target_field": cls._normalize_field_name(target_field),
            "source_fields": sorted(cls._normalize_field_name(item) for item in source_fields if cls._normalize_field_name(item)),
            "formula": str(formula or "").strip(),
            "conversion_mode": str(conversion_mode or "").strip().lower(),
        }
        digest = hashlib.md5(json.dumps(fingerprint, sort_keys=True).encode("utf-8")).hexdigest()[:16]
        return f"rule::{digest}"

    @staticmethod
    def _evidence_id(rule_id: str, description: str, source: str) -> str:
        digest = hashlib.md5(f"{rule_id}|{source}|{description}".encode("utf-8")).hexdigest()[:16]
        return f"evidence::{digest}"

    @classmethod
    def _default_write_status(cls, source: str, explicit_status: Optional[str], fallback: str = "candidate") -> str:
        if explicit_status:
            return cls._normalize_status(explicit_status, default=fallback)
        if str(source or "").strip().lower() in {"llm", "llm_generated", "candidate", "llm_candidate"}:
            return fallback
        return "approved"

    @classmethod
    def _bootstrap_graph_payload(cls, protocol_type: str, legacy_file: Path) -> Dict[str, Any]:
        payload = json.loads(legacy_file.read_text(encoding="utf-8")) if legacy_file.exists() else {}
        concepts: List[Dict[str, Any]] = []
        concept_ids = set()
        field_nodes: List[Dict[str, Any]] = []
        field_node_ids = set()
        edges: List[Dict[str, Any]] = []

        for item in payload.get("rules") or []:
            source_protocol = cls._normalize_protocol_type(item.get("protocol_type") or protocol_type)
            message_code = cls._normalize_message_code(item.get("message_code"))
            field_name = cls._normalize_field_name(item.get("field_name"))
            if not field_name:
                continue
            target_field = cls._normalize_field_name(item.get("target_field")) or field_name
            concept_name = cls._infer_concept_name(field_name, target_field)
            concept_id = cls._concept_id(concept_name)
            if concept_id not in concept_ids:
                concepts.append(
                    {
                        "concept_id": concept_id,
                        "name": concept_name,
                        "aliases": [],
                        "description": f"Bootstrapped concept for {concept_name}.",
                    }
                )
                concept_ids.add(concept_id)

            source_node_id = cls._field_node_id(source_protocol, message_code, field_name)
            if source_node_id not in field_node_ids:
                field_nodes.append(
                    {
                        "node_id": source_node_id,
                        "protocol_type": source_protocol,
                        "message_code": message_code,
                        "field_name": field_name,
                        "aliases": [cls._normalize_field_name(alias) for alias in item.get("aliases") or [] if cls._normalize_field_name(alias)],
                        "unit": str(item.get("unit") or "").strip() or None,
                        "bit_length": item.get("bit_length"),
                        "concept_id": concept_id,
                        "role": "source",
                    }
                )
                field_node_ids.add(source_node_id)

            target_node_id = cls._field_node_id(
                item.get("target_protocol_type") or source_protocol,
                item.get("target_message_code") or message_code,
                target_field,
            )
            if target_node_id not in field_node_ids:
                field_nodes.append(
                    {
                        "node_id": target_node_id,
                        "protocol_type": cls._normalize_protocol_type(item.get("target_protocol_type") or source_protocol),
                        "message_code": cls._normalize_message_code(item.get("target_message_code") or message_code),
                        "field_name": target_field,
                        "aliases": [],
                        "unit": str(item.get("unit") or "").strip() or None,
                        "bit_length": item.get("bit_length"),
                        "concept_id": concept_id,
                        "role": "target",
                    }
                )
                field_node_ids.add(target_node_id)

            formula = str(item.get("formula") or "").strip()
            conversion_mode = str(item.get("conversion_mode") or "mapping").strip().lower() or "mapping"
            edges.append(
                {
                    "edge_id": cls._edge_id(source_node_id, target_node_id, formula, conversion_mode),
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "source_fields": list(item.get("source_fields") or [field_name]),
                    "conversion_mode": conversion_mode,
                    "formula": formula,
                    "formula_kind": cls._infer_formula_kind(formula),
                    "description": str(item.get("description") or "").strip() or None,
                    "source": str(item.get("source") or "legacy_bootstrap"),
                    "confidence": item.get("confidence", 1.0),
                    "status": cls._normalize_status(item.get("status"), default="approved"),
                }
            )

        return {
            "protocol_type": payload.get("protocol_type") or protocol_type,
            "version": f"graph-{payload.get('version') or 'v1'}",
            "embedding_model": payload.get("embedding_model") or "qwen3-0.6b-embedding",
            "description": payload.get("description") or "Bootstrapped local protocol conversion knowledge graph.",
            "backend": "local_json_graph",
            "concepts": concepts,
            "field_nodes": field_nodes,
            "edges": edges,
        }

    @classmethod
    def _build_field_index(cls, field_nodes: List[Dict[str, Any]]) -> Dict[Tuple[str, Optional[str], str], List[Dict[str, Any]]]:
        index: Dict[Tuple[str, Optional[str], str], List[Dict[str, Any]]] = {}
        for item in field_nodes:
            protocol = cls._normalize_protocol_type(item.get("protocol_type"))
            message_code = cls._normalize_message_code(item.get("message_code"))
            names = [cls._normalize_field_name(item.get("field_name"))]
            names.extend(cls._normalize_field_name(alias) for alias in item.get("aliases") or [])
            for name in names:
                if not name:
                    continue
                index.setdefault((protocol, message_code, name), []).append(item)
        return index

    def _save(self) -> None:
        self.payload["concepts"] = self._concepts
        self.payload["field_nodes"] = self._field_nodes
        self.payload["edges"] = self._edges
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.write_text(json.dumps(self.payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _iter_source_nodes(self, field_name: str, message_code: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        normalized_field = self._normalize_field_name(field_name)
        normalized_message = self._normalize_message_code(message_code)
        seen = set()
        if normalized_message is None:
            for (protocol, _, name), items in self._field_index.items():
                if protocol != self.protocol_type or name != normalized_field:
                    continue
                for item in items:
                    node_id = item.get("node_id")
                    if node_id and node_id not in seen:
                        seen.add(node_id)
                        yield item
            return
        for key in (
            (self.protocol_type, normalized_message, normalized_field),
            (self.protocol_type, None, normalized_field),
        ):
            for item in self._field_index.get(key, []):
                node_id = item.get("node_id")
                if node_id and node_id not in seen:
                    seen.add(node_id)
                    yield item

    def _edge_to_rule(self, edge: Dict[str, Any]) -> Optional[KnowledgeRule]:
        source_node = self._field_by_id.get(str(edge.get("source_node_id") or ""))
        target_node = self._field_by_id.get(str(edge.get("target_node_id") or ""))
        if not source_node:
            return None
        concept = self._concept_by_id.get(str(source_node.get("concept_id") or (target_node or {}).get("concept_id") or ""), {})
        return KnowledgeRule(
            protocol_type=self._normalize_protocol_type(source_node.get("protocol_type") or self.protocol_type),
            message_code=self._normalize_message_code(source_node.get("message_code")),
            field_name=self._normalize_field_name(source_node.get("field_name")),
            source_fields=[self._normalize_field_name(item) for item in edge.get("source_fields") or [] if self._normalize_field_name(item)] or [self._normalize_field_name(source_node.get("field_name"))],
            conversion_mode=str(edge.get("conversion_mode") or "mapping").strip().lower() or "mapping",
            formula=str(edge.get("formula") or "").strip(),
            target_field=self._normalize_field_name((target_node or {}).get("field_name")),
            target_protocol_type=self._normalize_protocol_type((target_node or {}).get("protocol_type")) if target_node and target_node.get("protocol_type") else None,
            target_message_code=self._normalize_message_code((target_node or {}).get("message_code")) if target_node else None,
            unit=str((target_node or {}).get("unit") or source_node.get("unit") or "").strip() or None,
            aliases=[self._normalize_field_name(alias) for alias in source_node.get("aliases") or [] if self._normalize_field_name(alias)],
            source=str(edge.get("source") or "knowledge_graph"),
            description=str(edge.get("description") or "").strip() or None,
            bit_length=(target_node or {}).get("bit_length") if target_node and target_node.get("bit_length") is not None else source_node.get("bit_length"),
            concept_name=str(concept.get("name") or "").strip() or None,
            edge_id=str(edge.get("edge_id") or "").strip() or None,
            formula_kind=str(edge.get("formula_kind") or "").strip() or None,
            confidence=edge.get("confidence"),
            status=self._normalize_status(edge.get("status"), default="approved"),
        )

    @staticmethod
    def _rule_signature(rule: KnowledgeRule) -> Tuple[str, Tuple[str, ...], str]:
        return (
            rule.target_field or rule.field_name,
            tuple(rule.source_fields or [rule.field_name]),
            rule.formula,
        )

    def find_rule(
        self,
        field_name: str,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_field: Optional[str] = None,
    ) -> Optional[KnowledgeRule]:
        rules = self.list_rules(
            message_code=message_code,
            field_names=[field_name],
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
            target_fields=[target_field] if target_field else None,
        )
        return rules[0] if rules else None

    def list_rules(
        self,
        message_code: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_fields: Optional[List[str]] = None,
    ) -> List[KnowledgeRule]:
        normalized_fields = [self._normalize_field_name(item) for item in (field_names or []) if self._normalize_field_name(item)]
        normalized_targets = {self._normalize_field_name(item) for item in (target_fields or []) if self._normalize_field_name(item)}
        normalized_message = self._normalize_message_code(message_code)
        normalized_target_protocol = str(target_protocol_type or "").strip() or None
        normalized_target_message = self._normalize_message_code(target_message_code)

        candidates: List[Tuple[int, KnowledgeRule]] = []
        seen = set()

        for field_name in normalized_fields or [""]:
            nodes = list(self._iter_source_nodes(field_name, message_code=normalized_message)) if field_name else list(self._field_nodes)
            for node in nodes:
                node_id = node.get("node_id")
                for edge in self._edges:
                    if field_name and edge.get("source_node_id") != node_id:
                        continue
                    rule = self._edge_to_rule(edge)
                    if rule is None:
                        continue
                    if normalized_message and rule.message_code not in {None, normalized_message}:
                        continue
                    if normalized_target_protocol and rule.target_protocol_type not in {None, normalized_target_protocol}:
                        continue
                    if normalized_target_message and rule.target_message_code not in {None, normalized_target_message}:
                        continue
                    if normalized_targets and (rule.target_field or "") not in normalized_targets:
                        continue
                    signature = self._rule_signature(rule)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    score = 0
                    if normalized_message and rule.message_code == normalized_message:
                        score += 30
                    elif rule.message_code is None:
                        score += 8
                    if field_name and rule.field_name == field_name:
                        score += 12
                    elif field_name and field_name in set(rule.aliases):
                        score += 6
                    if normalized_target_protocol and rule.target_protocol_type == normalized_target_protocol:
                        score += 16
                    if normalized_target_message and rule.target_message_code == normalized_target_message:
                        score += 12
                    if normalized_targets and rule.target_field in normalized_targets:
                        score += 10
                    score += int(float(rule.confidence or 0.0) * 5)
                    candidates.append((score, rule))

        candidates.sort(key=lambda item: (-item[0], item[1].target_field or item[1].field_name, item[1].formula))
        return [rule for _, rule in candidates]

    def find_rules_for_source_fields(
        self,
        source_fields: Iterable[str],
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
    ) -> List[KnowledgeRule]:
        wanted = {self._normalize_field_name(item) for item in source_fields if self._normalize_field_name(item)}
        if not wanted:
            return []
        candidates = self.list_rules(
            message_code=message_code,
            field_names=list(wanted),
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
        )
        matched: List[KnowledgeRule] = []
        seen = set()
        for rule in candidates:
            if not set(rule.source_fields or [rule.field_name]).issubset(wanted):
                continue
            signature = self._rule_signature(rule)
            if signature in seen:
                continue
            seen.add(signature)
            matched.append(rule)
        return matched

    def _ensure_concept(self, concept_name: str) -> str:
        normalized = str(concept_name or "").strip() or "UNKNOWN"
        concept_id = self._concept_id(normalized)
        if concept_id in self._concept_by_id:
            return concept_id
        record = {
            "concept_id": concept_id,
            "name": normalized,
            "aliases": [],
            "description": f"LLM discovered concept {normalized}.",
        }
        self._concepts.append(record)
        self._concept_by_id[concept_id] = record
        return concept_id

    def _ensure_field_node(
        self,
        protocol_type: str,
        message_code: Optional[str],
        field_name: str,
        concept_id: str,
        role: str,
        aliases: Optional[Iterable[str]] = None,
        unit: Optional[str] = None,
        bit_length: Optional[int] = None,
    ) -> str:
        node_id = self._field_node_id(protocol_type, message_code, field_name)
        existing = self._field_by_id.get(node_id)
        normalized_aliases = [self._normalize_field_name(item) for item in (aliases or []) if self._normalize_field_name(item)]
        if existing is not None:
            current_aliases = {self._normalize_field_name(item) for item in existing.get("aliases") or [] if self._normalize_field_name(item)}
            for alias in normalized_aliases:
                if alias not in current_aliases:
                    existing.setdefault("aliases", []).append(alias)
            if unit and not existing.get("unit"):
                existing["unit"] = unit
            if bit_length is not None and existing.get("bit_length") is None:
                existing["bit_length"] = bit_length
            if concept_id and not existing.get("concept_id"):
                existing["concept_id"] = concept_id
            return node_id

        record = {
            "node_id": node_id,
            "protocol_type": self._normalize_protocol_type(protocol_type),
            "message_code": self._normalize_message_code(message_code),
            "field_name": self._normalize_field_name(field_name),
            "aliases": normalized_aliases,
            "unit": str(unit or "").strip() or None,
            "bit_length": bit_length,
            "concept_id": concept_id,
            "role": role,
        }
        self._field_nodes.append(record)
        self._field_by_id[node_id] = record
        for name in [record["field_name"], *record["aliases"]]:
            self._field_index.setdefault((record["protocol_type"], record["message_code"], name), []).append(record)
        return node_id

    def _normalize_rule_input(
        self,
        item: Any,
        protocol_type: Optional[str] = None,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        source: str = "llm",
        default_status: str = "candidate",
    ) -> Optional[KnowledgeRule]:
        if isinstance(item, KnowledgeRule):
            rule = item
            if not rule.status:
                rule.status = self._default_write_status(rule.source or source, None, fallback=default_status)
            return rule
        if not isinstance(item, dict):
            return None

        field_name = self._normalize_field_name(item.get("field_name") or item.get("source_field"))
        source_fields = [
            self._normalize_field_name(value)
            for value in item.get("source_fields") or ([] if field_name else [])
            if self._normalize_field_name(value)
        ]
        if not source_fields and field_name:
            source_fields = [field_name]
        if not field_name and source_fields:
            field_name = source_fields[0]
        if not field_name:
            return None

        formula = str(
            item.get("formula")
            or item.get("rule")
            or item.get("conversion_formula")
            or item.get("expression")
            or ""
        ).strip()
        if not formula:
            return None

        target_field = self._normalize_field_name(item.get("target_field")) or field_name
        return KnowledgeRule(
            protocol_type=self._normalize_protocol_type(item.get("protocol_type") or protocol_type or self.protocol_type),
            message_code=self._normalize_message_code(item.get("message_code") or message_code),
            field_name=field_name,
            source_fields=source_fields,
            conversion_mode=str(item.get("conversion_mode") or item.get("mode") or "mapping").strip().lower() or "mapping",
            formula=formula,
            target_field=target_field,
            target_protocol_type=str(item.get("target_protocol_type") or target_protocol_type or protocol_type or self.protocol_type).strip() or None,
            target_message_code=self._normalize_message_code(item.get("target_message_code") or target_message_code),
            unit=str(item.get("unit") or "").strip() or None,
            aliases=[self._normalize_field_name(alias) for alias in item.get("aliases") or [] if self._normalize_field_name(alias)],
            source=str(item.get("source") or source),
            description=str(item.get("description") or item.get("evidence") or "").strip() or None,
            bit_length=item.get("bit_length"),
            concept_name=str(item.get("concept_name") or self._infer_concept_name(field_name, target_field)).strip() or None,
            edge_id=str(item.get("edge_id") or "").strip() or None,
            formula_kind=str(item.get("formula_kind") or self._infer_formula_kind(formula)).strip() or None,
            confidence=float(item.get("confidence")) if item.get("confidence") is not None else None,
            status=self._default_write_status(item.get("source") or source, item.get("status"), fallback=default_status),
        )

    def upsert_generated_rules(
        self,
        rules: Iterable[Any],
        protocol_type: Optional[str] = None,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        source: str = "llm",
    ) -> List[KnowledgeRule]:
        written_rules: List[KnowledgeRule] = []
        for item in rules:
            rule = self._normalize_rule_input(
                item,
                protocol_type=protocol_type,
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
                source=source,
                default_status="candidate",
            )
            if rule is None:
                continue

            concept_name = rule.concept_name or self._infer_concept_name(rule.field_name, rule.target_field)
            concept_id = self._ensure_concept(concept_name)
            source_protocol = rule.protocol_type or protocol_type or self.protocol_type
            target_protocol = rule.target_protocol_type or target_protocol_type or source_protocol
            target_field = rule.target_field or rule.field_name
            source_fields = rule.source_fields or [rule.field_name]
            target_node_id = self._ensure_field_node(
                protocol_type=target_protocol,
                message_code=rule.target_message_code or target_message_code,
                field_name=target_field,
                concept_id=concept_id,
                role="target",
                unit=rule.unit,
                bit_length=rule.bit_length,
            )
            for source_field in source_fields:
                source_node_id = self._ensure_field_node(
                    protocol_type=source_protocol,
                    message_code=rule.message_code or message_code,
                    field_name=source_field,
                    concept_id=concept_id,
                    role="source",
                    aliases=rule.aliases,
                    unit=rule.unit,
                    bit_length=rule.bit_length,
                )
                edge_id = rule.edge_id or self._edge_id(source_node_id, target_node_id, rule.formula, rule.conversion_mode)
                edge_payload = {
                    "edge_id": edge_id,
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "source_fields": source_fields,
                    "conversion_mode": rule.conversion_mode,
                    "formula": rule.formula,
                    "formula_kind": rule.formula_kind or self._infer_formula_kind(rule.formula),
                    "description": rule.description,
                    "source": rule.source or source,
                    "confidence": rule.confidence,
                    "status": self._normalize_status(rule.status, default="candidate"),
                }
                replaced = False
                for idx, existing in enumerate(self._edges):
                    if existing.get("edge_id") == edge_id:
                        self._edges[idx] = edge_payload
                        replaced = True
                        break
                if not replaced:
                    self._edges.append(edge_payload)
                edge_rule = self._edge_to_rule(edge_payload)
                if edge_rule is not None:
                    written_rules.append(edge_rule)

        if written_rules:
            self._save()
        return written_rules

    def to_summary(self) -> Dict[str, Any]:
        return {
            "protocol_type": self.protocol_type,
            "version": self.version,
            "embedding_model": self.embedding_model,
            "backend": self.backend,
            "concept_count": len(self._concepts),
            "field_node_count": len(self._field_nodes),
            "rule_count": len(self._edges),
            "file_path": str(self.file_path),
        }


class Neo4jProtocolConversionKnowledgeBase(ProtocolConversionKnowledgeBase):
    """Protocol conversion knowledge graph backed by Neo4j."""

    def __init__(self, protocol_type: str, settings: KnowledgeGraphSettings):
        if GraphDatabase is None:
            raise RuntimeError("neo4j driver is not installed")
        if not settings.uri:
            raise RuntimeError("Neo4j URI is not configured")

        self.protocol_type = self._normalize_protocol_type(protocol_type)
        self.settings = settings
        self.embedding_model = "qwen3-0.6b-embedding"
        self.version = "graph-v2-neo4j"
        self.backend = "neo4j_graph"
        self.description = "Neo4j-backed protocol conversion knowledge graph."
        self.file_path = None
        self.driver = GraphDatabase.driver(
            settings.uri,
            auth=(settings.username, settings.password),
            connection_timeout=settings.timeout_seconds,
        )
        self.driver.verify_connectivity()
        if settings.auto_init:
            self.ensure_schema()

    def _run_cypher(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        with self.driver.session(database=self.settings.database) as session:
            result = session.run(query, parameters or {})
            records: List[Dict[str, Any]] = []
            for record in result:
                if hasattr(record, "data"):
                    records.append(record.data())
                else:  # pragma: no cover - fallback for lightweight fakes
                    records.append(dict(record))
            return records

    def ensure_schema(self) -> None:
        """Create required constraints and indexes for the graph model."""
        statements = [
            "CREATE CONSTRAINT concept_id_unique IF NOT EXISTS FOR (c:Concept) REQUIRE c.concept_id IS UNIQUE",
            "CREATE CONSTRAINT field_node_id_unique IF NOT EXISTS FOR (f:Field) REQUIRE f.node_id IS UNIQUE",
            "CREATE CONSTRAINT rule_id_unique IF NOT EXISTS FOR (r:Rule) REQUIRE r.rule_id IS UNIQUE",
            "CREATE CONSTRAINT evidence_id_unique IF NOT EXISTS FOR (e:Evidence) REQUIRE e.evidence_id IS UNIQUE",
            "CREATE INDEX field_lookup IF NOT EXISTS FOR (f:Field) ON (f.protocol_type, f.message_code, f.field_name)",
            "CREATE INDEX rule_status_lookup IF NOT EXISTS FOR (r:Rule) ON (r.status, r.target_protocol_type, r.target_message_code, r.target_field)",
        ]
        for statement in statements:
            self._run_cypher(statement)

    def _query_rule_records(self) -> List[Dict[str, Any]]:
        query = """
        MATCH (r:Rule)-[:USES_SOURCE]->(src:Field)
        WHERE src.protocol_type = $protocol_type
        WITH r, collect(DISTINCT src{.*}) AS sources
        MATCH (r)-[:PRODUCES_TARGET]->(target:Field)
        OPTIONAL MATCH (r)-[:ABOUT_CONCEPT]->(concept:Concept)
        RETURN r{.*} AS rule, sources, target{.*} AS target, concept{.*} AS concept
        """
        return self._run_cypher(query, {"protocol_type": self.protocol_type})

    def _record_to_rule(self, record: Dict[str, Any]) -> Optional[KnowledgeRule]:
        rule_props = record.get("rule") or {}
        target = record.get("target") or {}
        concept = record.get("concept") or {}
        sources = record.get("sources") or []
        if not rule_props or not sources:
            return None

        source_fields = [
            self._normalize_field_name(item)
            for item in rule_props.get("source_fields") or [source.get("field_name") for source in sources]
            if self._normalize_field_name(item)
        ]
        first_source = sources[0]
        aliases: List[str] = []
        for source in sources:
            for alias in source.get("aliases") or []:
                normalized = self._normalize_field_name(alias)
                if normalized and normalized not in aliases:
                    aliases.append(normalized)

        return KnowledgeRule(
            protocol_type=self._normalize_protocol_type(rule_props.get("source_protocol_type") or first_source.get("protocol_type") or self.protocol_type),
            message_code=self._normalize_message_code(rule_props.get("source_message_code") or first_source.get("message_code")),
            field_name=self._normalize_field_name(first_source.get("field_name")),
            source_fields=source_fields or [self._normalize_field_name(first_source.get("field_name"))],
            conversion_mode=str(rule_props.get("conversion_mode") or "mapping").strip().lower() or "mapping",
            formula=str(rule_props.get("formula") or "").strip(),
            target_field=self._normalize_field_name(rule_props.get("target_field") or target.get("field_name")),
            target_protocol_type=self._normalize_protocol_type(rule_props.get("target_protocol_type") or target.get("protocol_type")) if (rule_props.get("target_protocol_type") or target.get("protocol_type")) else None,
            target_message_code=self._normalize_message_code(rule_props.get("target_message_code") or target.get("message_code")),
            unit=str(target.get("unit") or first_source.get("unit") or "").strip() or None,
            aliases=aliases,
            source=str(rule_props.get("source") or "knowledge_graph"),
            description=str(rule_props.get("description") or "").strip() or None,
            bit_length=target.get("bit_length") if target.get("bit_length") is not None else first_source.get("bit_length"),
            concept_name=str(rule_props.get("concept_name") or concept.get("name") or "").strip() or None,
            edge_id=str(rule_props.get("rule_id") or "").strip() or None,
            formula_kind=str(rule_props.get("formula_kind") or "").strip() or None,
            confidence=rule_props.get("confidence"),
            status=self._normalize_status(rule_props.get("status"), default="approved"),
        )

    def find_rule(
        self,
        field_name: str,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_field: Optional[str] = None,
    ) -> Optional[KnowledgeRule]:
        rules = self.list_rules(
            message_code=message_code,
            field_names=[field_name],
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
            target_fields=[target_field] if target_field else None,
        )
        return rules[0] if rules else None

    def list_rules(
        self,
        message_code: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_fields: Optional[List[str]] = None,
    ) -> List[KnowledgeRule]:
        normalized_fields = {self._normalize_field_name(item) for item in (field_names or []) if self._normalize_field_name(item)}
        normalized_targets = {self._normalize_field_name(item) for item in (target_fields or []) if self._normalize_field_name(item)}
        normalized_message = self._normalize_message_code(message_code)
        normalized_target_protocol = str(target_protocol_type or "").strip() or None
        normalized_target_message = self._normalize_message_code(target_message_code)

        candidates: List[Tuple[int, KnowledgeRule]] = []
        seen = set()
        for record in self._query_rule_records():
            rule = self._record_to_rule(record)
            if rule is None:
                continue
            if rule.status and self.settings.read_statuses and rule.status not in set(self.settings.read_statuses):
                continue

            source_names = set(rule.source_fields or [rule.field_name])
            source_names.add(rule.field_name)
            source_names.update(rule.aliases)
            if normalized_fields and source_names.isdisjoint(normalized_fields):
                continue
            if normalized_message and rule.message_code not in {None, normalized_message}:
                continue
            if normalized_target_protocol and rule.target_protocol_type not in {None, normalized_target_protocol}:
                continue
            if normalized_target_message and rule.target_message_code not in {None, normalized_target_message}:
                continue
            if normalized_targets and (rule.target_field or "") not in normalized_targets:
                continue

            signature = self._rule_signature(rule)
            if signature in seen:
                continue
            seen.add(signature)
            score = 0
            if normalized_message and rule.message_code == normalized_message:
                score += 30
            elif rule.message_code is None:
                score += 8
            if normalized_fields and rule.field_name in normalized_fields:
                score += 12
            elif normalized_fields and not set(rule.aliases).isdisjoint(normalized_fields):
                score += 6
            if normalized_target_protocol and rule.target_protocol_type == normalized_target_protocol:
                score += 16
            if normalized_target_message and rule.target_message_code == normalized_target_message:
                score += 12
            if normalized_targets and rule.target_field in normalized_targets:
                score += 10
            score += int(float(rule.confidence or 0.0) * 5)
            candidates.append((score, rule))

        candidates.sort(key=lambda item: (-item[0], item[1].target_field or item[1].field_name, item[1].formula))
        return [rule for _, rule in candidates]

    def find_rules_for_source_fields(
        self,
        source_fields: Iterable[str],
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
    ) -> List[KnowledgeRule]:
        wanted = {self._normalize_field_name(item) for item in source_fields if self._normalize_field_name(item)}
        if not wanted:
            return []
        candidates = self.list_rules(
            message_code=message_code,
            field_names=list(wanted),
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
        )
        matched: List[KnowledgeRule] = []
        seen = set()
        for rule in candidates:
            if not set(rule.source_fields or [rule.field_name]).issubset(wanted):
                continue
            signature = self._rule_signature(rule)
            if signature in seen:
                continue
            seen.add(signature)
            matched.append(rule)
        return matched

    def upsert_generated_rules(
        self,
        rules: Iterable[Any],
        protocol_type: Optional[str] = None,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        source: str = "llm",
    ) -> List[KnowledgeRule]:
        written_rules: List[KnowledgeRule] = []
        for item in rules:
            rule = self._normalize_rule_input(
                item,
                protocol_type=protocol_type,
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
                source=source,
                default_status=self.settings.default_write_status,
            )
            if rule is None:
                continue

            source_protocol = rule.protocol_type or protocol_type or self.protocol_type
            source_message = rule.message_code or message_code
            target_protocol = rule.target_protocol_type or target_protocol_type or source_protocol
            target_message = rule.target_message_code or target_message_code
            source_fields = rule.source_fields or [rule.field_name]
            target_field = rule.target_field or rule.field_name
            concept_name = rule.concept_name or self._infer_concept_name(rule.field_name, target_field)
            concept_id = self._concept_id(concept_name)
            target_node_id = self._field_node_id(target_protocol, target_message, target_field)
            rule_id = rule.edge_id or self._rule_node_id(
                source_protocol_type=source_protocol,
                source_message_code=source_message,
                target_protocol_type=target_protocol,
                target_message_code=target_message,
                target_field=target_field,
                source_fields=source_fields,
                formula=rule.formula,
                conversion_mode=rule.conversion_mode,
            )

            query = """
            MERGE (c:Concept {concept_id: $concept_id})
            SET c.name = $concept_name,
                c.description = coalesce(c.description, $concept_description)
            MERGE (target:Field {node_id: $target_node_id})
            SET target.protocol_type = $target_protocol_type,
                target.message_code = $target_message_code,
                target.field_name = $target_field,
                target.unit = $unit,
                target.bit_length = $bit_length
            MERGE (target)-[:EXPRESSES]->(c)
            MERGE (r:Rule {rule_id: $rule_id})
            SET r.source_protocol_type = $source_protocol_type,
                r.source_message_code = $source_message_code,
                r.target_protocol_type = $target_protocol_type,
                r.target_message_code = $target_message_code,
                r.target_field = $target_field,
                r.source_fields = $source_fields,
                r.conversion_mode = $conversion_mode,
                r.formula = $formula,
                r.formula_kind = $formula_kind,
                r.source = $source,
                r.description = $description,
                r.confidence = $confidence,
                r.status = $status,
                r.concept_name = $concept_name
            MERGE (r)-[:PRODUCES_TARGET]->(target)
            MERGE (r)-[:ABOUT_CONCEPT]->(c)
            WITH r, c
            UNWIND $source_nodes AS source_node
            MERGE (src:Field {node_id: source_node.node_id})
            SET src.protocol_type = source_node.protocol_type,
                src.message_code = source_node.message_code,
                src.field_name = source_node.field_name,
                src.aliases = source_node.aliases,
                src.unit = source_node.unit,
                src.bit_length = source_node.bit_length
            MERGE (src)-[:EXPRESSES]->(c)
            MERGE (r)-[:USES_SOURCE]->(src)
            """
            parameters = {
                "concept_id": concept_id,
                "concept_name": concept_name,
                "concept_description": f"Protocol conversion concept for {concept_name}.",
                "rule_id": rule_id,
                "source_protocol_type": source_protocol,
                "source_message_code": source_message,
                "target_protocol_type": target_protocol,
                "target_message_code": target_message,
                "target_field": target_field,
                "target_node_id": target_node_id,
                "source_fields": source_fields,
                "conversion_mode": rule.conversion_mode,
                "formula": rule.formula,
                "formula_kind": rule.formula_kind or self._infer_formula_kind(rule.formula),
                "source": rule.source or source,
                "description": rule.description,
                "confidence": rule.confidence,
                "status": self._normalize_status(rule.status, default=self.settings.default_write_status),
                "unit": rule.unit,
                "bit_length": rule.bit_length,
                "source_nodes": [
                    {
                        "node_id": self._field_node_id(source_protocol, source_message, source_field),
                        "protocol_type": source_protocol,
                        "message_code": source_message,
                        "field_name": source_field,
                        "aliases": list(rule.aliases),
                        "unit": rule.unit,
                        "bit_length": rule.bit_length,
                    }
                    for source_field in source_fields
                ],
            }
            self._run_cypher(query, parameters)

            if rule.description:
                evidence_query = """
                MERGE (e:Evidence {evidence_id: $evidence_id})
                SET e.source_type = $source,
                    e.snippet = $snippet
                WITH e
                MATCH (r:Rule {rule_id: $rule_id})
                MERGE (r)-[:SUPPORTED_BY]->(e)
                """
                self._run_cypher(
                    evidence_query,
                    {
                        "evidence_id": self._evidence_id(rule_id, rule.description, rule.source or source),
                        "source": rule.source or source,
                        "snippet": rule.description,
                        "rule_id": rule_id,
                    },
                )

            written_rules.append(
                KnowledgeRule(
                    protocol_type=source_protocol,
                    message_code=self._normalize_message_code(source_message),
                    field_name=self._normalize_field_name(source_fields[0]),
                    source_fields=[self._normalize_field_name(item) for item in source_fields],
                    conversion_mode=rule.conversion_mode,
                    formula=rule.formula,
                    target_field=self._normalize_field_name(target_field),
                    target_protocol_type=self._normalize_protocol_type(target_protocol),
                    target_message_code=self._normalize_message_code(target_message),
                    unit=rule.unit,
                    aliases=[self._normalize_field_name(alias) for alias in rule.aliases],
                    source=rule.source or source,
                    description=rule.description,
                    bit_length=rule.bit_length,
                    concept_name=concept_name,
                    edge_id=rule_id,
                    formula_kind=rule.formula_kind or self._infer_formula_kind(rule.formula),
                    confidence=rule.confidence,
                    status=self._normalize_status(rule.status, default=self.settings.default_write_status),
                )
            )
        return written_rules

    def to_summary(self) -> Dict[str, Any]:
        return {
            "protocol_type": self.protocol_type,
            "version": self.version,
            "embedding_model": self.embedding_model,
            "backend": self.backend,
            "uri": self.settings.uri,
            "database": self.settings.database,
            "read_statuses": list(self.settings.read_statuses),
            "default_write_status": self.settings.default_write_status,
        }


class CompositeProtocolConversionKnowledgeBase(ProtocolConversionKnowledgeBase):
    """Composite repository that prefers Neo4j and falls back to local JSON."""

    def __init__(
        self,
        protocol_type: str,
        primary: Neo4jProtocolConversionKnowledgeBase,
        fallback: ProtocolConversionKnowledgeBase,
    ):
        self.protocol_type = self._normalize_protocol_type(protocol_type)
        self.primary = primary
        self.fallback = fallback
        self.embedding_model = getattr(primary, "embedding_model", fallback.embedding_model)
        self.version = f"{getattr(primary, 'version', 'neo4j_graph')}+{fallback.version}"
        self.backend = "neo4j_graph+local_json_graph"
        self.description = "Composite Neo4j-first protocol conversion knowledge graph."
        self.file_path = fallback.file_path

    def find_rule(
        self,
        field_name: str,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_field: Optional[str] = None,
    ) -> Optional[KnowledgeRule]:
        rules = self.list_rules(
            message_code=message_code,
            field_names=[field_name],
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
            target_fields=[target_field] if target_field else None,
        )
        return rules[0] if rules else None

    def list_rules(
        self,
        message_code: Optional[str] = None,
        field_names: Optional[List[str]] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        target_fields: Optional[List[str]] = None,
    ) -> List[KnowledgeRule]:
        try:
            primary_rules = self.primary.list_rules(
                message_code=message_code,
                field_names=field_names,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
                target_fields=target_fields,
            )
            if primary_rules:
                return primary_rules
        except Exception:
            pass

        return self.fallback.list_rules(
            message_code=message_code,
            field_names=field_names,
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
            target_fields=target_fields,
        )

    def find_rules_for_source_fields(
        self,
        source_fields: Iterable[str],
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
    ) -> List[KnowledgeRule]:
        try:
            primary_rules = self.primary.find_rules_for_source_fields(
                source_fields=source_fields,
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
            )
            if primary_rules:
                return primary_rules
        except Exception:
            pass

        return self.fallback.find_rules_for_source_fields(
            source_fields=source_fields,
            message_code=message_code,
            target_protocol_type=target_protocol_type,
            target_message_code=target_message_code,
        )

    def upsert_generated_rules(
        self,
        rules: Iterable[Any],
        protocol_type: Optional[str] = None,
        message_code: Optional[str] = None,
        target_protocol_type: Optional[str] = None,
        target_message_code: Optional[str] = None,
        source: str = "llm",
    ) -> List[KnowledgeRule]:
        materialized = list(rules)
        primary_rules: List[KnowledgeRule] = []
        fallback_rules: List[KnowledgeRule] = []

        try:
            primary_rules = self.primary.upsert_generated_rules(
                materialized,
                protocol_type=protocol_type,
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
                source=source,
            )
        except Exception:
            primary_rules = []

        if self.primary.settings.write_fallback_json or not primary_rules:
            fallback_rules = self.fallback.upsert_generated_rules(
                materialized,
                protocol_type=protocol_type,
                message_code=message_code,
                target_protocol_type=target_protocol_type,
                target_message_code=target_message_code,
                source=source,
            )

        return primary_rules or fallback_rules

    def to_summary(self) -> Dict[str, Any]:
        primary_summary = self.primary.to_summary()
        fallback_summary = self.fallback.to_summary()
        return {
            "protocol_type": self.protocol_type,
            "version": self.version,
            "embedding_model": self.embedding_model,
            "backend": self.backend,
            "primary": primary_summary,
            "fallback": fallback_summary,
            "file_path": str(self.file_path),
        }
