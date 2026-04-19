#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/deploy/runtime"
LOG_DIR="$RUNTIME_DIR/logs"
PID_DIR="$RUNTIME_DIR/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "未找到可执行 Python: $PYTHON_BIN" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "用法: $0 <接口目录名>" >&2
  exit 1
fi

PROJECT_NAME="$1"
PROJECT_DIR="$ROOT_DIR/$PROJECT_NAME"
APP_FILE="$PROJECT_DIR/app.py"
PID_FILE="$PID_DIR/$PROJECT_NAME.pid"
LOG_FILE="$LOG_DIR/$PROJECT_NAME.log"

if [[ ! -f "$APP_FILE" ]]; then
  echo "接口目录不存在或缺少 app.py: $PROJECT_DIR" >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "$PROJECT_NAME 已在运行，PID=$(cat "$PID_FILE")"
  exit 0
fi

(
  cd "$PROJECT_DIR"
  nohup "$PYTHON_BIN" app.py >"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
)

echo "已启动 $PROJECT_NAME"
echo "PID: $(cat "$PID_FILE")"
echo "日志: $LOG_FILE"
