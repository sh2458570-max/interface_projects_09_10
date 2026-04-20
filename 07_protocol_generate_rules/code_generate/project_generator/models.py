"""Data models used by the protocol project generator."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class FieldSpec:
    """Represents one flattened protocol field."""

    label: str
    cpp_name: str
    path: str
    path_parts: tuple[str, ...]
    bit_length: int | None
    bit_offset: int
    default_value: str | None
    source_tag: str


@dataclass(slots=True)
class DimenSpec:
    """Represents protocol-wide dimension metadata."""

    pack_head_length: int = 0
    endian: str = "big"
    word_length: int = -1


@dataclass(slots=True)
class ScalarNode:
    """Represents one scalar XML node."""

    label: str
    cpp_name: str
    path: str
    path_parts: tuple[str, ...]
    bit_length: int | None
    default_value: str | None
    source_tag: str


@dataclass(slots=True)
class BranchNode:
    """Represents one XML Field branch node."""

    label: str
    path: str
    path_parts: tuple[str, ...]
    corr: str | None
    value: str | None
    control_fields: tuple[str, ...]
    children: list["ProtocolNode"] = field(default_factory=list)


@dataclass(slots=True)
class GroupNode:
    """Represents one XML Group loop node."""

    label: str
    path: str
    path_parts: tuple[str, ...]
    corr: str | None
    condition: str | None
    max_repeat: int | None
    repeat_count: int
    control_fields: tuple[str, ...]
    children: list["ProtocolNode"] = field(default_factory=list)


ProtocolNode = ScalarNode | BranchNode | GroupNode


@dataclass(slots=True)
class SequenceMember:
    """Represents one sequence member selector."""

    corr: str | None
    value: str | None
    control_fields: tuple[str, ...]


@dataclass(slots=True)
class SequenceSpec:
    """Represents one MessCode/PreSeq sequence."""

    name: str
    cycle: int
    times: int
    members: list[SequenceMember] = field(default_factory=list)


@dataclass(slots=True)
class SectionSpec:
    """Represents one top-level protocol section."""

    name: str
    cpp_name: str
    tag_name: str
    path: str
    nodes: list[ProtocolNode] = field(default_factory=list)


@dataclass(slots=True)
class RouteSpec:
    """Represents one root-level route mapping."""

    corr: str | None
    value: str | None
    target_protocol: str
    control_fields: tuple[str, ...]


@dataclass(slots=True)
class ProtocolSpec:
    """Represents one protocol definition file."""

    type_name: str
    file_stem: str
    source_path: Path
    namespace: str
    dimen: DimenSpec = field(default_factory=DimenSpec)
    total_bits: int = 0
    structure_kind: str = "fixed_length"
    codec_supported: bool = True
    unsupported_features: list[str] = field(default_factory=list)
    fields: list[FieldSpec] = field(default_factory=list)
    nodes: list[ProtocolNode] = field(default_factory=list)
    sections: list[SectionSpec] = field(default_factory=list)
    sequences: list[SequenceSpec] = field(default_factory=list)
    routes: list[RouteSpec] = field(default_factory=list)
    label_to_cpp: dict[str, str] = field(default_factory=dict)

    @property
    def endian(self) -> str:
        """Returns the protocol endian value."""

        return self.dimen.endian


@dataclass(slots=True)
class SourceAlias:
    """Represents one mapping source alias."""

    alias: str
    protocol: str


@dataclass(slots=True)
class MappingRule:
    """Represents one target-field mapping rule."""

    target_field: str
    formula: str
    source_fields: list[str]
    rule_type: str
    when: str | None
    default_value: str | None
    description: str | None


@dataclass(slots=True)
class EndpointSpec:
    """Represents one config.xml endpoint item."""

    ip: str
    port: int
    net_type: str
    recv: bool
    feedback_port: int
    name: str


@dataclass(slots=True)
class FetchAttempt:
    """Represents one source-fetch retry rule."""

    count: int
    cycle_ms: int


@dataclass(slots=True)
class SourceRuntime:
    """Represents runtime fetch metadata for one source alias."""

    alias: str
    message_name: str | None = None
    display_name: str | None = None
    fetches: list[FetchAttempt] = field(default_factory=list)


@dataclass(slots=True)
class ConstraintAssignment:
    """Represents one field assignment inside a response constraint."""

    field: str
    value: str


@dataclass(slots=True)
class ConstraintSpec:
    """Represents one named verify constraint."""

    name: str
    check: str | None = None
    assignments: list[ConstraintAssignment] = field(default_factory=list)


@dataclass(slots=True)
class VerifyRuleSpec:
    """Represents one verify-tag recognition rule."""

    name: str
    when_seq: str
    constraint: str | None = None


@dataclass(slots=True)
class ResponseActionSpec:
    """Represents one response action triggered by a verify tag."""

    on_verify: str
    set_constraint: str | None = None
    encode_seq: str | None = None
    return_code: int = 0


@dataclass(slots=True)
class ProtocolVerifySpec:
    """Represents protocol-level verify/response state-machine settings."""

    protocol: str
    constraints: list[ConstraintSpec] = field(default_factory=list)
    verify_rules: list[VerifyRuleSpec] = field(default_factory=list)
    response_actions: list[ResponseActionSpec] = field(default_factory=list)
    default_verify: str | None = None
    default_return_code: int = -1


@dataclass(slots=True)
class ConversionRuntime:
    """Represents runtime generation metadata for one conversion."""

    process_method: str | None = None
    usage_key: str | None = None
    sources: list[SourceRuntime] = field(default_factory=list)
    response_enabled: bool = True
    send_mode: str = "direct"
    cache_name: str | None = None
    cache_num: int = 3


@dataclass(slots=True)
class ConversionSpec:
    """Represents one conversion chain."""

    name: str
    mode: str
    sources: list[SourceAlias]
    target_protocol: str
    rules: list[MappingRule]
    runtime: ConversionRuntime = field(default_factory=ConversionRuntime)


@dataclass(slots=True)
class RuntimeSpec:
    """Represents top-level runtime project settings."""

    endpoints: list[EndpointSpec] = field(default_factory=list)
    loop_sleep_ms: int = 2
    check_data_interval_ms: int = 5000
    protocol_verifies: list[ProtocolVerifySpec] = field(default_factory=list)
    reference_profile: str | None = None


@dataclass(slots=True)
class MappingSpec:
    """Represents the mappings.json file."""

    version: str
    project_name: str
    conversions: list[ConversionSpec]
    runtime: RuntimeSpec = field(default_factory=RuntimeSpec)


@dataclass(slots=True)
class ChoreographySource:
    """Represents one choreography source entry."""

    source_id: str
    protocol: str
    message_type: str
    cache_key: str
    required: bool


@dataclass(slots=True)
class ChoreographyTarget:
    """Represents one choreography target entry."""

    target_id: str
    protocol: str
    message_type: str
    template_name: str
    receive_window_ms: int
    initial_status: str


@dataclass(slots=True)
class MatrixSpec:
    """Represents one timing matrix."""

    unit: str
    rows: list[str]
    cols: list[str]
    values: list[list[int | None]]


@dataclass(slots=True)
class JointGroup:
    """Represents one choreography joint group."""

    group_id: str
    target_id: str
    sources: list[str]
    trigger_policy: str
    matrix: MatrixSpec


@dataclass(slots=True)
class ChoreographySpec:
    """Represents the choreography.json file."""

    version: str
    mode: str
    project_name: str
    sources: list[ChoreographySource]
    targets: list[ChoreographyTarget]
    joint_groups: list[JointGroup]
