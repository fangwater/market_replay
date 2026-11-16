"""Shared locations for the LSEG / IB delivery scripts.

Code lives in the market_replay repo (scripts/lseg, config/lseg); run state,
evidence and reports live on NVMe under DATA (artifacts/, reports/).
Old ly_cme_data paths in artifacts were rewritten on 2026-11-16, except
artifacts/lseg_full_rocksdb_20261112/plan.json (a kline source-identity
record, kept byte-identical).
"""
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
REPO = SCRIPTS.parents[1]
CONFIG = REPO / 'config' / 'lseg'
DATA = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data')
RELEASE = Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release')
# IB MySQL credentials (IB_MYSQL_*), mode 600, never committed.
IB_ENV = Path.home() / '.config' / 'market_replay' / 'ib_mysql.env'
