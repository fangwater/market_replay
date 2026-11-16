#!/usr/bin/env python3
"""Publish 2026 persisted TAS states and the exact reviewed baseline status input.

Success state stays local for restart. Only source/mapping/execution problems
are published under meta; Trade, Quote and minute bars are never modified.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq

from group_lseg_by_exchange import references, classify

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
NVME = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data')
RUN = ROOT/'artifacts/tas_status_20261109'
DEST = Path('/mnt/nfs/30.100_cta_data/lseg')
BIN = Path('/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/market_replay/release/tas_status_parquet')
INPUT = NVME/'cme_causal_roll_inputs_20261023/ten_root_admission_catalog/session_status.json'


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.partial')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')
    temp.replace(path)


def prepare():
    refs = references([NVME/'cme_causal_roll_inputs_20261023/hrd.csv',
                       ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv'])
    exchanges = {ric: classify(ric, 'futures', ref, {})['exchange'].lower()
                 for ric, ref in refs.items()}
    old = json.loads(Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/tas_tick_parquet/2026/_audit/run.json').read_text())
    jobs = [dict(id='original_2026', db=old['source'], period=old['source_period'],
                 cfs={cf: None for cf in old['products']})]
    merge = json.loads((ROOT/'artifacts/tas_supplement_merge_20261103/completion.json').read_text())
    supplement = json.loads((ROOT/'artifacts/tas_supplement_ingest_20261103/db_audit.json').read_text())
    jobs.append(dict(id='supplement_2026', db=supplement['db'], period='2026-01-01_2026-05-30',
                     cfs={cf: supplement['products'][cf]['kinds'].get('13', 0)
                          for cf in merge['new_product_cfs']}))
    # The original and supplemental sets are disjoint; never read the merged CFs twice.
    assert set(jobs[0]['cfs']).isdisjoint(jobs[1]['cfs'])
    for path in sorted((ROOT/'artifacts/tas_daily_ingest_20261109/products').glob('*/completion.json')):
        record = json.loads(path.read_text())
        if not record.get('db'):
            continue  # Empty upstream source placeholders are already listed in source_gaps.csv.
        audit = json.loads(Path(record['db_audit']).read_text())
        jobs.append(dict(id='daily_'+record['product'], db=record['db'], period=record['source_period'],
                         cfs={cf: rec['kinds'].get('13', 0) for cf, rec in audit['products'].items()}))
    assert len({j['db'] for j in jobs}) == len(jobs)
    # Use the same actual RIC classifications as the existing Trade/Quote delivery.
    for path in [ROOT/'artifacts/tas_supplement_parquet_20261109/exchange_map.json',
                 *sorted((ROOT/'artifacts/tas_daily_ingest_20261109/products').glob('*/exchange_map.json'))]:
        for ric, venue in json.loads(path.read_text()).items():
            if ric in exchanges:
                assert exchanges[ric] == venue, (ric, exchanges[ric], venue)
            exchanges[ric] = venue
    plan = dict(year=2026, exchanges=exchanges, jobs=jobs)
    path = RUN/'plan.json'
    if path.exists():
        assert json.loads(path.read_text()) == plan, 'Source plan changed; review before resuming.'
    else:
        save(path, plan)
    return plan


def publish_baseline_input():
    raw = INPUT.read_bytes()
    receipt = json.loads(Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d/_audit/cme_ten_causal_20261025/contract_build/build_receipt.json').read_text())
    assert receipt['exit_code'] == 0
    assert receipt['input_sha256'][str(INPUT)] == hashlib.sha256(raw).hexdigest()
    events = json.loads(raw)
    assert len(events) == 145
    assert sum(e['state'] == 'halted' for e in events) == 135
    assert sum(e['state'] == 'open' for e in events) == 10
    schema = pa.schema([
        pa.field('product', pa.string(), False), pa.field('ric', pa.string(), True),
        pa.field('known_at_ns', pa.uint64(), False), pa.field('effective_ns', pa.uint64(), False),
        pa.field('state', pa.string(), False), pa.field('source', pa.string(), False),
    ], metadata={
        'source_kind': 'reviewed_baseline_session_status',
        'scope': '2025-11-28 outage; 135 TAS RIC halts plus 10 product-scope CME announcement reopenings; not a full historical or 2026 status feed',
        'ric_null_scope': 'all contracts of this product',
        'time_basis': 'UTC nanoseconds; known_at_ns is when evidence became available; effective_ns is its action time; preserve both',
        'baseline_input_json_sha256': hashlib.sha256(raw).hexdigest(),
    })
    expected = pa.Table.from_pylist(events, schema=schema)
    path = DEST/'meta/session_status.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temp = path.with_suffix('.parquet.partial')
        pq.write_table(expected, temp, compression='zstd')
        with temp.open('rb') as stream:
            os.fsync(stream.fileno())
        actual = pq.ParquetFile(temp).read()
        assert actual.equals(expected, check_metadata=True)
        os.link(temp, path)
        temp.unlink()
    else:
        assert pq.ParquetFile(path).read().equals(expected, check_metadata=True)


def record_mapping_problems():
    records = {}
    for path in (RUN/'checkpoints').glob('*.txt'):
        lines = path.read_text().splitlines()
        origin = lines[0]
        ric_line = next(s for s in lines if s.startswith('review_rics='))
        for ric in json.loads(ric_line.split('=', 1)[1]):
            records.setdefault(ric, set()).add(origin.split('|')[2])
    if records:
        import csv
        path = DEST/'meta/problems/status_mapping.csv'
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix('.csv.partial')
        with temp.open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=['ric', 'product_cfs', 'problem'])
            writer.writeheader()
            for ric, cfs in sorted(records.items()):
                writer.writerow(dict(ric=ric, product_cfs='|'.join(sorted(cfs)),
                    problem='参考缺失或交易所冲突；原始状态保留于review目录，未凭前缀推定交易所'))
        temp.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--only-job', help='Bounded first run using the same resumable delivery, e.g. daily_ES')
    parser.add_argument('--workers', type=int, default=3)
    args = parser.parse_args()
    RUN.mkdir(exist_ok=True)
    with (RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan = prepare()
        path = RUN/'plan.json'
        if args.only_job:
            plan = dict(plan, jobs=[j for j in plan['jobs'] if j['id'] == args.only_job])
            assert plan['jobs'], args.only_job
            path = RUN/'bounded_plan.json'
            save(path, plan)
        command = [str(BIN), '--plan', str(path), '--output', str(DEST),
                   '--checkpoint', str(RUN/'checkpoints'), '--workers', str(args.workers)]
        subprocess.run(command, check=True, capture_output=True, text=True)
        record_mapping_problems()
        publish_baseline_input()
        failure = DEST/'meta/problems/status_export_failure.json'
        if failure.exists() and json.loads(failure.read_text()).get('script') == str(Path(__file__).resolve()):
            failure.unlink()


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        save(DEST/'meta/problems/status_export_failure.json', dict(problem=str(error), stderr=getattr(error, 'stderr', None), script=str(Path(__file__).resolve())))
        raise
