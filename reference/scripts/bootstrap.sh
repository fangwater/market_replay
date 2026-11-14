#!/bin/sh
set -eu

PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VENV_DIR=${VENV_DIR:-"$PROJECT_ROOT/.venv"}

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/python" -m pip install -e "$PROJECT_ROOT"

echo "Python environment created: $VENV_DIR"
echo "Set PYTHON_BIN=$VENV_DIR/bin/python in .env before starting PM2."
