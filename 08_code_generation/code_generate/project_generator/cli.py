"""Command line interface for the protocol project generator."""

from __future__ import annotations

import argparse
from pathlib import Path

from project_generator.loaders import load_choreography, load_mappings
from project_generator.renderer import render_project
from project_generator.xml_parser import load_protocols


def build_project(args: argparse.Namespace) -> None:
    """Builds one generated Qt/C++ project."""

    protocol_dir = Path(args.protocol_dir).resolve()
    mappings_path = Path(args.mappings).resolve()
    output_dir = Path(args.output).resolve()
    choreography_path = Path(args.choreography).resolve() if args.choreography else None

    protocols = load_protocols(protocol_dir)
    mappings = load_mappings(mappings_path)
    choreography = load_choreography(choreography_path) if choreography_path else None
    render_project(output_dir, protocols, mappings, choreography)
    print(f"生成完成: {output_dir}")


def create_parser() -> argparse.ArgumentParser:
    """Creates the CLI argument parser."""

    parser = argparse.ArgumentParser(description="Qt/C++ 协议转换项目生成器")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="根据输入生成 C++ 项目")
    build_parser.add_argument("--protocol-dir", required=True, help="协议 XML 目录")
    build_parser.add_argument("--mappings", required=True, help="转换公式 JSON")
    build_parser.add_argument("--choreography", help="联合转换时序矩阵 JSON")
    build_parser.add_argument("--output", required=True, help="输出目录")
    build_parser.set_defaults(func=build_project)
    return parser


def main() -> None:
    """Runs the generator CLI."""

    parser = create_parser()
    args = parser.parse_args()
    args.func(args)

