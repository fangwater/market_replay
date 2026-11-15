#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$project_dir/logs"
cd "$project_dir"
pm2 startOrReload ecosystem.config.cjs --update-env
pm2 save
