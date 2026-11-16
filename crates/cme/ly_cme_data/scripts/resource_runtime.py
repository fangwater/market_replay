"""Program budgets for ordinary data CLIs; no cgroup/admin prerequisite."""
import fcntl
import json
import os
from pathlib import Path
import sys

MEMORY_BYTES = 64 * 1024**3
MAX_WORKERS = 2
IO_THREADS = 2
RUNTIME_LOCK = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/runtime/resource.lock')
LAUNCHER = Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/ly_cme_run')


def bounded_workers(value):
    value = int(value)
    if not 1 <= value <= MAX_WORKERS:
        raise ValueError(f'workers must be between 1 and {MAX_WORKERS}; one shared 64 GiB / 32-thread budget')
    return value


def process_identity(pid):
    text = Path(f'/proc/{pid}/stat').read_bytes()
    fields = text.rsplit(b')', 1)[1].split()
    return int(fields[1]), int(fields[2]), int(fields[19])


def supervised():
    """Verify a live locked supervisor and actual ancestry, not an env flag."""
    try:
        with RUNTIME_LOCK.open() as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
                return False
            except BlockingIOError:
                pass
            root = json.load(stream)
        parent, group, start = process_identity(root['pid'])
        if start != root['start_ticks'] or group != root['pid']:
            return False
        pid = os.getpid()
        for _ in range(1024):
            parent, group, start = process_identity(pid)
            if pid == root['pid']:
                return start == root['start_ticks']
            if parent == 0 or parent == pid:
                return False
            pid = parent
    except (OSError, ValueError, KeyError, IndexError, TypeError):
        return False
    return False


def enter():
    """Re-exec under the Rust supervisor before NumPy/Arrow imports."""
    if not supervised():
        if not LAUNCHER.is_file():
            raise RuntimeError(f'build the resource launcher first: {LAUNCHER}')
        os.execv(str(LAUNCHER), [str(LAUNCHER), '--', sys.executable, *sys.argv])
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:32])
    for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS',
                'NUMEXPR_MAX_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS', 'UV_THREADPOOL_SIZE',
                'ARROW_NUM_THREADS', 'OMP_THREAD_LIMIT'):
        os.environ[key] = '1'
    os.environ.update(LY_CME_DUCKDB_THREADS='1', LY_CME_DUCKDB_MEMORY='2GB',
                      LY_CME_IO_THREADS='2', LY_CME_WORKERS='2')


def configure_arrow(arrow):
    arrow.set_cpu_count(1)
    arrow.set_io_thread_count(1)
