"""Reference profile support for fully implemented legacy projects."""

from __future__ import annotations

from pathlib import Path

from project_generator.utils import ensure_directory, write_text


ROOT_DIR = Path(__file__).resolve().parents[1]


REFERENCE_PROFILES: dict[str, dict[str, object]] = {
    "newB": {
        "source_dir": ROOT_DIR / "newB",
        "files": [
            "codec.cpp",
            "codec.h",
            "messageconvert.cpp",
            "messageconvert.h",
            "main.cpp",
            "config.xml",
            "peach.pro",
            "to_code_Choreography.cpp",
            "to_code_Choreography.h",
            "s0_1_to_w304.cpp",
            "s0_1_to_w304.h",
            "s106_to_w204.cpp",
            "s106_to_w204.h",
            "s0_1_def.h",
            "s106_def.h",
            "w204_def.h",
            "w304_def.h",
        ],
    },
    "newC": {
        "source_dir": ROOT_DIR / "newB" / "newC",
        "files": [
            "codec.cpp",
            "codec.h",
            "messageconvert.cpp",
            "messageconvert.h",
            "main.cpp",
            "config.xml",
            "peach.pro",
            "w304_to_iCD304.cpp",
            "w304_to_iCD304.h",
            "w304_def.h",
            "iCD304_def.h",
        ],
    },
    "newD": {
        "source_dir": ROOT_DIR / "newB" / "newD",
        "files": [
            "codec.cpp",
            "codec.h",
            "messageconvert.cpp",
            "messageconvert.h",
            "main.cpp",
            "config.xml",
            "peach.pro",
            "w304_to_iCD304.cpp",
            "w304_to_iCD304.h",
            "w304_def.h",
            "iCD304_def.h",
        ],
    },
}


def detect_reference_profile(protocol_types: set[str], conversion_pairs: set[tuple[tuple[str, ...], str]], joint: bool) -> str | None:
    """Detects one built-in legacy profile from protocol and conversion sets."""

    if joint and protocol_types == {"S0_1", "S106", "W304", "W204"}:
        if conversion_pairs == {(("S0_1",), "W304"), (("S106",), "W204")}:
            return "newB"
    if not joint and protocol_types == {"W304", "ICD304"}:
        if conversion_pairs == {(("W304",), "ICD304")}:
            return "newC"
    return None


def render_reference_project(output_dir: Path, profile_name: str) -> None:
    """Copies one reference project into the output directory."""

    profile = REFERENCE_PROFILES[profile_name]
    source_dir = Path(profile["source_dir"])
    ensure_directory(output_dir)
    for relative_name in profile["files"]:
        source_path = source_dir / relative_name
        write_text(output_dir / relative_name, source_path.read_text(encoding="utf-8"))
