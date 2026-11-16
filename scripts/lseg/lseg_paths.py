"""Shared locations for the LSEG / IB delivery scripts.

Code lives in the market_replay repo (scripts/lseg, config/lseg); run state,
evidence and reports live on NVMe under DATA (artifacts/, reports/).
Historical artifact JSON written before 2026-11-16 still names the old
`/home/fanghaizhou/ly_cme_data/artifacts/...` prefix, which is now
`DATA/artifacts/...`.
"""
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parents[1]
CONFIG = REPO / 'config' / 'lseg'
DATA = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data')
RELEASE = Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release')
# IB MySQL credentials (IB_MYSQL_*), mode 600, never committed.
IB_ENV = Path.home() / '.config' / 'market_replay' / 'ib_mysql.env'
