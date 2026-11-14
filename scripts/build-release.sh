#!/usr/bin/env bash
# Release build; profile copied from mkt_signal (thin LTO, codegen-units=4, stripped).
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 0 ]]; then
  echo "Usage: scripts/build-release.sh" >&2
  exit 2
fi
cd "$ROOT_DIR"
cargo build --release --locked -p mdl_replay --bin mdl-replay
BIN="$ROOT_DIR/target/release/mdl-replay"
echo "[INFO] built $BIN"
sha256sum "$BIN"
