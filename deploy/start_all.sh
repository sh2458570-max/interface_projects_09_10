#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START_ONE="$ROOT_DIR/deploy/start_one.sh"

PROJECTS=(
  "01_validate_protocol_files"
  "02_upload_split"
  "03_clean"
  "04_semantic_chunk"
  "05_generate_qa"
  "06_extract_validate_qa"
  "07_protocol_generate_rules"
  "08_code_generation"
  "09_finetune_runtime"
  "10_rule_evaluate"
)

for project in "${PROJECTS[@]}"; do
  "$START_ONE" "$project"
done

echo "全部接口启动命令已执行完成。"
