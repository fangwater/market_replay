#!/bin/sh
set -eu
PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_ROOT"
exec "${EXCHANGE_MCP_PYTHON:-$PROJECT_ROOT/.venv-mcp/bin/python}" -m exchange_mcp.server
