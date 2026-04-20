"""Unit tests for generated project static validation."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1] / "code_generate"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from project_generator.models import FieldSpec, ProtocolSpec  # noqa: E402
from project_generator.renderer import (  # noqa: E402
    _validate_codec_content,
    _validate_protocol_header_content,
)
from project_generator.templates import render_codec_cpp, render_codec_header, render_protocol_header  # noqa: E402


def _build_protocol(type_name: str, file_stem: str, fields: list[str]) -> ProtocolSpec:
    """Builds one minimal protocol spec for validation tests."""

    return ProtocolSpec(
        type_name=type_name,
        file_stem=file_stem,
        source_path=Path(f"/tmp/{file_stem}.xml"),
        namespace="",
        fields=[
            FieldSpec(
                label=name,
                cpp_name=name,
                path=name,
                path_parts=(name,),
                bit_length=1,
                bit_offset=index,
                default_value="0",
                source_tag="Item",
            )
            for index, name in enumerate(fields)
        ],
    )


class RendererValidationTest(unittest.TestCase):
    """Covers post-render static validation."""

    def test_protocol_header_validation_accepts_normal_render(self) -> None:
        protocol = _build_protocol("K1_6", "k1_6", ["gpi4", "field_a"])
        content = render_protocol_header(protocol)
        _validate_protocol_header_content(protocol, "k1_6_def.h", content)

    def test_protocol_header_validation_rejects_joined_fields(self) -> None:
        protocol = _build_protocol("K1_6", "k1_6", ["gpi4", "field_a"])
        content = """#ifndef K1_6_DEF_H
#define K1_6_DEF_H

class K1_6 {
public:
    long gpi4 = 0; long field_a = 0;
};

#endif
"""
        with self.assertRaisesRegex(ValueError, "字段声明格式异常"):
            _validate_protocol_header_content(protocol, "k1_6_def.h", content)

    def test_codec_validation_rejects_missing_field_reference(self) -> None:
        protocol = _build_protocol("K1_6", "k1_6", ["field_a"])
        codec = """#include \"codec.h\"

QString decodeMsg(uchar* pData, int len, K1_6& value)
{
    Q_UNUSED(pData);
    Q_UNUSED(len);
    value.gpi4 = 1;
    return QString();
}
"""
        with self.assertRaisesRegex(ValueError, "未声明字段"):
            _validate_codec_content([protocol], "codec.cpp", codec)

    def test_codec_validation_rejects_bad_append_arity(self) -> None:
        protocol = _build_protocol("K1_6", "k1_6", ["field_a"])
        codec = """#include \"codec.h\"

void appendBits(QByteArray& data, quint64 value, int bitLength)
{
    Q_UNUSED(data);
    Q_UNUSED(value);
    Q_UNUSED(bitLength);
}

void encodeMsg(QByteArray& data, K1_6& value)
{
    appendBits(data, value.field_a);
}
        """
        with self.assertRaisesRegex(ValueError, "参数数量异常"):
            _validate_codec_content([protocol], "codec.cpp", codec)

    def test_codec_header_includes_qtglobal(self) -> None:
        protocol = _build_protocol("K1_6", "k1_6", ["field_a"])
        header = render_codec_header([protocol], ["k1_6_to_k1_7.h"])
        self.assertIn("#include <QtGlobal>", header)

    def test_codec_helpers_are_protocol_scoped(self) -> None:
        protocol_a = _build_protocol("K1_6", "k1_6", ["field_a"])
        protocol_b = _build_protocol("K1_7", "k1_7", ["field_b"])
        codec = render_codec_cpp([protocol_a, protocol_b])
        self.assertIn("static QString checkEncodeSeqNumberK1_6", codec)
        self.assertIn("static QString checkEncodeSeqNumberK1_7", codec)
        self.assertIn("static void writeK1_6Seq1", codec)
        self.assertIn("static void writeK1_7Seq1", codec)


if __name__ == "__main__":
    unittest.main()
