#!/usr/bin/env bash
set -euo pipefail
task_repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$task_repo"
export LIBCLANG_PATH="${LIBCLANG_PATH:-/home/fanghaizhou/libclang-shim}"
# Reuse this machine's matching RocksDB 11.8.1 archive when available.
# Cargo still builds this project's Rust objects in its own NVMe target directory.
if [[ -z ${ROCKSDB_LIB_DIR:-} ]]; then
    for task_native in /mnt/nvme-raid0-28t/fanghaizhou/cargo-target/mkt_signal/release/build/librocksdb-sys-*; do
        if [[ -f $task_native/out/librocksdb.a && -f $task_native/output ]] &&
            rg -Fq 'librocksdb-sys-0.19.0+11.8.1' "$task_native/output"; then
            export ROCKSDB_LIB_DIR="$task_native/out"
            export ROCKSDB_STATIC=1
            break
        fi
    done
fi
task_release=/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release
mkdir -p "$task_release"
cc -shared -fPIC -O2 "$task_repo/src/native_cpu_budget.c" -o "$task_release/libly_cme_cpu_budget.so.partial"
mv "$task_release/libly_cme_cpu_budget.so.partial" "$task_release/libly_cme_cpu_budget.so"
exec cargo build --release --locked "$@"
