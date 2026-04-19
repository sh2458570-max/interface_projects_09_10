#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/deploy/runtime/pids"

if [[ ! -d "$PID_DIR" ]]; then
  echo "未找到 PID 目录: $PID_DIR"
  exit 0
fi

shopt -s nullglob
for pid_file in "$PID_DIR"/*.pid; do
  pid="$(cat "$pid_file")"
  name="$(basename "$pid_file" .pid)"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    echo "已停止 $name (PID=$pid)"
  else
    echo "$name 的 PID 不存在: $pid"
  fi
  rm -f "$pid_file"
done
