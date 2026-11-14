#!/usr/bin/env bash
# Starts (or restarts) the mdl-replay server under PM2, mkt_signal style.
#
# Env overrides:
#   MDL_REPLAY_CONFIG   config path      (default: config/mdl_replay.toml)
#   MDL_REPLAY_CORES    taskset CPU list (default: 0-31)
#   MDL_REPLAY_PM2_NAME / PM2_NAMESPACE
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 0 ]]; then
  echo "Usage: scripts/start_mdl_replay.sh   (configure via env, see header)" >&2
  exit 2
fi
CONFIG="${MDL_REPLAY_CONFIG:-$ROOT_DIR/config/mdl_replay.toml}"
CORES="${MDL_REPLAY_CORES:-0-31}"
PROC_NAME="${MDL_REPLAY_PM2_NAME:-mdl_replay}"
NAMESPACE="${PM2_NAMESPACE:-cn_futures_db}"
BIN="$ROOT_DIR/target/release/mdl-replay"

[[ -x "$BIN" ]] || { echo "[ERROR] $BIN missing; run scripts/build-release.sh" >&2; exit 1; }
[[ -f "$CONFIG" ]] || { echo "[ERROR] config $CONFIG not found" >&2; exit 1; }
[[ "$CORES" =~ ^[0-9,-]+$ ]] || { echo "[ERROR] MDL_REPLAY_CORES must be a CPU list (got $CORES)" >&2; exit 1; }
if command -v pm2 >/dev/null 2>&1; then PM2=(pm2); elif command -v npx >/dev/null 2>&1; then PM2=(npx pm2); else
  echo "[ERROR] neither pm2 nor npx is available" >&2; exit 1; fi

"$BIN" serve --help >/dev/null
"${PM2[@]}" delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
"${PM2[@]}" start /usr/bin/taskset \
  --name "$PROC_NAME" --namespace "$NAMESPACE" --cwd "$ROOT_DIR" --interpreter none \
  -- -c "$CORES" "$BIN" serve --config "$CONFIG"
CONTROL="$(awk -F'"' '/^control_endpoint/ {print $2}' "$CONFIG")"
CONTROL="${CONTROL:-tcp://127.0.0.1:13100}"
for _ in $(seq 1 50); do
  if "$BIN" req --endpoint "$CONTROL" --timeout-ms 200 '{"cmd":"ping"}' >/dev/null 2>&1; then
    echo "[INFO] $PROC_NAME up on $CONTROL (cores $CORES)"
    echo "Logs: ${PM2[*]} logs --namespace $NAMESPACE $PROC_NAME"
    exit 0
  fi
  sleep 0.2
done
echo "[ERROR] $PROC_NAME did not answer ping on $CONTROL" >&2
"${PM2[@]}" logs --namespace "$NAMESPACE" "$PROC_NAME" --lines 30 --nostream >&2 || true
exit 1
