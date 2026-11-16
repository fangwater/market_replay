#!/usr/bin/env python3
"""Rewrite only delivered TAS minutes to the compact public schema.

Use the original local rich minute cache. Convert OHLC from Int64 e9 to actual
Float64 prices. Empty-minute rules, volume, timestamps, trade counts and rows
are untouched; no tick or status re-aggregation is needed.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import fcntl
import json
from pathlib import Path
from urllib.parse import quote

import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as pipeline

STATE=pipeline.ROOT/f'artifacts/kline_schema_v{pipeline.KLINE_SCHEMA_VERSION}_20261110'
PROBLEMS=pipeline.DEST/'meta/problems'/f'kline_schema_v{pipeline.KLINE_SCHEMA_VERSION}'


def migrate(contract,pool):
    ric=contract['ric']
    checkpoint=STATE/(quote(ric,safe='_-.^')+'.json')
    if checkpoint.exists():return json.loads(checkpoint.read_text())
    original_checkpoint=pipeline.RUN/'contracts'/(quote(ric,safe='_-.^')+'.json')
    original=json.loads(original_checkpoint.read_text())
    if not original['kline_rows']:
        return None
    source=pipeline.WORK/'contracts'/quote(ric,safe='_-.^')/'kline.parquet'
    old=pq.ParquetFile(source).read()
    if int(pipeline.KLINE_SCHEMA_VERSION)>=4:
        assert (old.schema.metadata or {}).get(b'fill_policy_version')==pipeline.FILL_POLICY_VERSION.encode(),'Run rebuild_kline_fill_policy.py before compacting old minute caches'
    assert len(old)==original['kline_rows'],ric
    table=pipeline.public_kline(old)
    assert table.select(pipeline.KLINE_CORE).equals(pipeline.kline_float_prices(old.select(pipeline.KLINE_CORE)),check_metadata=False),ric
    counts=Counter(table['status'].to_pylist())
    files=pipeline.publish_daily(table,contract,'kline',pool,replace_schema=True)
    result=dict(ric=ric,rows=len(table),files=files,is_ffill_rows=int(pc.sum(table['is_ffill']).as_py() or 0),
                status_counts=dict(counts),schema_version=pipeline.KLINE_SCHEMA_VERSION)
    # Machine restart state only. No successful-processing receipt goes to NAS.
    original['public_kline_schema_version']=pipeline.KLINE_SCHEMA_VERSION
    pipeline.save(original_checkpoint,original)
    pipeline.save(checkpoint,result)
    issue=PROBLEMS/(quote(ric,safe='_-.^')+'.json')
    issue.unlink(missing_ok=True)
    return result


def checked(contract,pool):
    try:return migrate(contract,pool)
    except Exception as error:
        import traceback
        pipeline.save(PROBLEMS/(quote(contract['ric'],safe='_-.^')+'.json'),
            dict(ric=contract['ric'],problem=str(error),traceback=traceback.format_exc()))
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ric')
    parser.add_argument('--workers',type=int,default=4)
    args=parser.parse_args()
    STATE.mkdir(parents=True,exist_ok=True)
    with (pipeline.RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        contracts=json.loads((pipeline.RUN/'contracts.json').read_text())
        selected=[]
        for ric,contract in contracts.items():
            if args.ric and ric!=args.ric:continue
            receipt=pipeline.RUN/'contracts'/(quote(ric,safe='_-.^')+'.json')
            if json.loads(receipt.read_text())['kline_rows']:selected.append(contract)
        selected.sort(key=lambda c:(c['ric']!='ESM26',c['ric']))
        with ThreadPoolExecutor(max_workers=16) as publisher,ThreadPoolExecutor(max_workers=args.workers) as workers:
            futures=[workers.submit(checked,c,publisher) for c in selected]
            for future in as_completed(futures):
                row=future.result()
                if row:print(json.dumps(dict(ric=row['ric'],rows=row['rows'],files=row['files'])),flush=True)
        if not args.ric:
            assert len(list(STATE.glob('*.json')))==len(selected)
        problems=PROBLEMS
        if problems.exists() and not any(problems.iterdir()):problems.rmdir()


if __name__=='__main__':main()
