#!/usr/bin/env bash
# Builds the production binaries from a clean checkout of one commit and
# switches market_replay_deploy/current to them atomically.
#
#   scripts/deploy-release.sh [<rev>]      (default: HEAD; must be committed)
#
# Production (market-replay-server, its jobs and services) only ever runs
# binaries from $DEPLOY/current/bin, never from target/release: worktree and
# dev builds share target/ and would otherwise replace production binaries.
# Running processes keep their old inode; the next job start uses the new
# release. Restart the server afterwards with scripts/market_replay_server.sh.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY=/mnt/nvme-raid0-28t/fanghaizhou/market_replay_deploy
TARGET=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay_deploy
KEEP=5
rev="${1:-HEAD}"
sha="$(git -C "$ROOT_DIR" rev-parse --verify "$rev^{commit}")"
short="${sha:0:12}"
src="$DEPLOY/src/$short"
release="$DEPLOY/releases/$short"
mkdir -p "$DEPLOY/src" "$DEPLOY/releases"
if [[ ! -x "$release/bin/market-replay-server" ]]; then
  rm -rf "$src"
  git -C "$ROOT_DIR" worktree add --detach "$src" "$sha" >/dev/null
  trap 'git -C "$ROOT_DIR" worktree remove --force "$src" >/dev/null 2>&1 || true' EXIT
  export LIBCLANG_PATH="${LIBCLANG_PATH:-$HOME/libclang-shim}" CARGO_TARGET_DIR="$TARGET"
  (cd "$src" && nice cargo build --release --locked --workspace --bins)
  rm -rf "$release.partial"
  mkdir -p "$release.partial/bin"
  find "$TARGET/release" -maxdepth 1 -type f -executable -exec cp -p {} "$release.partial/bin/" \;
  {
    echo "commit=$sha"
    echo "built_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "rustc=$(rustc --version)"
  } > "$release.partial/REVISION"
  mv "$release.partial" "$release"
fi
ln -sfn "releases/$short" "$DEPLOY/current.new"
mv -T "$DEPLOY/current.new" "$DEPLOY/current"
echo "[INFO] $DEPLOY/current -> releases/$short ($(ls "$release/bin" | wc -l) binaries)"
# Keep the newest $KEEP releases (never the current one).
ls -1t "$DEPLOY/releases" | grep -v '\.partial$' | tail -n +$((KEEP + 1)) | while read -r old; do
  [[ "$old" == "$short" ]] || rm -rf "$DEPLOY/releases/$old"
done
