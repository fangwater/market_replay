#!/usr/bin/env python3
"""Add temporary-halt minutes and carry the own close across session breaks.

Reuses exact actual-trade minutes already computed locally. No tick scan or
production writes; only the minute grid, price padding and comparison change.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import fcntl
import json
import os
from urllib.parse import quote

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as pipeline

STATE=pipeline.ROOT/'artifacts/kline_policy_v4_20261110'
PROBLEMS=pipeline.DEST/'meta/problems/kline_policy_v4'
DIAGNOSTICS=['outside_status_trade_count','unknown_status_trade_count',
    'missing_exchange_time_trade_count','late_trade_count']


def local_parquet(table,path):
    temp=path.with_suffix(path.suffix+'.partial')
    pq.write_table(table,temp,compression='zstd')
    with temp.open('rb') as stream:os.fsync(stream.fileno())
    temp.replace(path)


def rebuild(contract,status,pool):
    ric=contract['ric'];name=quote(ric,safe='_-.^')
    checkpoint=STATE/(name+'.json')
    if checkpoint.exists():return json.loads(checkpoint.read_text())
    original_path=pipeline.RUN/'contracts'/(name+'.json')
    original=json.loads(original_path.read_text())
    previous=json.loads((pipeline.ROOT/'artifacts/kline_schema_v3_20261110'/(name+'.json')).read_text())
    folder=pipeline.WORK/'contracts'/name
    old=pq.ParquetFile(folder/'kline.parquet').read()
    actual=old.filter(pc.greater(old['trade_count'],0))
    start=contract['start_ns'];end=contract['end_ns']//pipeline.MINUTE*pipeline.MINUTE
    intervals=pipeline.state_intervals(status,start,end)
    c=pipeline.connection()
    try:
        c.register('states',intervals)
        c.register('cached_minutes',actual.select(pipeline.KLINE_CORE[1:]+DIAGNOSTICS))
        c.execute('CREATE TEMP VIEW minutes AS SELECT * FROM cached_minutes')
        bars=pipeline.minute_grid(c,ric,start,end,pipeline.previous_close_seed(ric,start))
        new_actual=bars.filter(pc.greater(bars['trade_count'],0))
        columns=pipeline.KLINE_CORE+DIAGNOSTICS
        assert actual.select(columns).equals(new_actual.select(columns),check_metadata=False),ric
        if (old.schema.metadata or {}).get(b'fill_policy_version')!=b'4':
            assert pc.all(pc.is_in(old['ts_utc_ns'],value_set=bars['ts_utc_ns'].combine_chunks())).as_py(),ric
        assert int(pc.sum(bars['trade_count']).as_py() or 0)==original['source_trade_rows'],ric
        assert int(pc.sum(bars['volume']).as_py() or 0)==original['source_volume'],ric
        issues=c.from_arrow(bars).filter('outside_status_trade_count>0 OR unknown_status_trade_count>0 OR missing_exchange_time_trade_count>0 OR NOT status_known').arrow().read_all()
        differences=None;stats=None
        ib_path=folder/'kline_ib.parquet'
        if original['ib_rows']:
            ib=pq.ParquetFile(ib_path).read()
            stats,differences=pipeline.compare_ib(c,ric,bars,ib,contract['tick_size'])
    finally:c.close()
    local_parquet(intervals,folder/'intervals.parquet')
    local_parquet(bars,folder/'kline.parquet')
    for filename,table in [('state_issues',issues),('differences',differences)]:
        path=folder/(filename+'.parquet')
        if table is not None and len(table):local_parquet(table,path)
        else:path.unlink(missing_ok=True)
    public=pipeline.public_kline(bars)
    files=pipeline.publish_daily(public,contract,'kline',pool,replace_schema=True)
    result=dict(ric=ric,rows=len(bars),files=files,old_rows=previous['rows'],
        is_ffill_rows=int(pc.sum(public['is_ffill']).as_py() or 0),
        status_counts=dict(Counter(public['status'].to_pylist())),schema_version=pipeline.KLINE_SCHEMA_VERSION,
        fill_policy_version=pipeline.FILL_POLICY_VERSION)
    original.update(kline_rows=len(bars),state_issue_minutes=len(issues),comparison=stats,
        files=original['files']-previous['files']+files,
        public_kline_schema_version=pipeline.KLINE_SCHEMA_VERSION)
    pipeline.save(original_path,original)
    pipeline.save(checkpoint,result)
    (PROBLEMS/(name+'.json')).unlink(missing_ok=True)
    return result


def checked(contract,status,pool):
    try:return rebuild(contract,status,pool)
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
        status=pq.ParquetFile(pipeline.WORK/'status.parquet').read()
        selected=[]
        priority={'ESM26','NQM26','YMM26','GCM26','CLN26'}
        for ric,contract in contracts.items():
            if args.ric and ric!=args.ric:continue
            original=json.loads((pipeline.RUN/'contracts'/(quote(ric,safe='_-.^')+'.json')).read_text())
            if original['kline_rows']:selected.append(contract)
        selected.sort(key=lambda c:(c['ric'] not in priority,c['ric']))
        with ThreadPoolExecutor(max_workers=16) as publisher,ThreadPoolExecutor(max_workers=args.workers) as workers:
            futures=[workers.submit(checked,contract,status.filter(pc.equal(status['ric'],contract['ric'])),publisher) for contract in selected]
            for future in as_completed(futures):
                row=future.result()
                print(json.dumps(row,ensure_ascii=False),flush=True)
        if not args.ric:
            assert len(list(STATE.glob('*.json')))==len(selected)
            pipeline.finalize(contracts)
        if PROBLEMS.exists() and not any(PROBLEMS.iterdir()):PROBLEMS.rmdir()


if __name__=='__main__':main()
