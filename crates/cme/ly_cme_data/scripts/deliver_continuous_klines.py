#!/usr/bin/env python3
"""Deliver continuous, lifetime-bounded TAS minutes; native IB stays unchanged."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
import csv
import fcntl
import json
from pathlib import Path
from urllib.parse import quote

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as pipeline
from rebuild_kline_fill_policy import local_parquet,DIAGNOSTICS
from kline_lifetimes import prepare_lifetimes,window

STATE=pipeline.ROOT/'artifacts/kline_continuous_v5_20261110'
PROBLEMS=pipeline.DEST/'meta/problems/continuous_kline'



def remove_stale(contract,days,keep):
    for day_number in sorted(set(days)-set(keep)):
        day=pd.Timestamp(int(day_number)*pipeline.DAY,unit='ns',tz='UTC').strftime('%Y%m%d')
        path=pipeline.DEST/contract['exchange']/'2026'/quote(contract['ric'],safe='_-.^')/day/'kline.parquet'
        if not path.exists():continue
        footer=pq.ParquetFile(path)
        names=footer.schema_arrow.names
        for index in range(footer.num_row_groups):
            group=footer.metadata.row_group(index)
            fields=['volume','trade_count']
            for field in fields:
                stats=group.column(names.index(field)).statistics
                assert stats and stats.has_min_max and stats.min==stats.max==0,path
        path.unlink()


def unknown_runs(public):
    times=public['ts_utc_ns'].to_numpy()
    mask=pc.equal(public['status'],'UNKNOWN').to_numpy(zero_copy_only=False)
    positions=np.flatnonzero(mask)
    if not len(positions):return []
    edges=np.r_[0,np.flatnonzero(np.diff(positions)>1)+1,len(positions)]
    result=[]
    for lo,hi in zip(edges[:-1],edges[1:]):
        indexes=positions[lo:hi];section=public.take(pa.array(indexes))
        result.append(dict(start_utc=pd.Timestamp(int(times[indexes[0]]),unit='ns',tz='UTC').isoformat(),
            end_utc_exclusive=pd.Timestamp(int(times[indexes[-1]])+pipeline.MINUTE,unit='ns',tz='UTC').isoformat(),
            minutes=len(indexes),trade_count=int(pc.sum(section['trade_count']).as_py() or 0),volume=int(pc.sum(section['volume']).as_py() or 0)))
    return result


def migrate(contract,status,lifetime,pool):
    ric=contract['ric'];name=quote(ric,safe='_-.^');checkpoint=STATE/'contracts'/(name+'.json')
    if checkpoint.exists():return json.loads(checkpoint.read_text())
    folder=pipeline.WORK/'contracts'/name
    original_path=pipeline.RUN/'contracts'/(name+'.json');original=json.loads(original_path.read_text())
    old=pq.ParquetFile(folder/'kline.parquet').read()
    start,end=window(contract,status,old,lifetime)
    actual=old.filter(pc.greater(old['trade_count'],0))
    native=pq.ParquetFile(folder/'kline_ib.parquet').read() if original['ib_rows'] else None
    stage=STATE/'working'/(name+'.json')
    if stage.exists():old_days=json.loads(stage.read_text())
    else:
        old_days=dict(tas=sorted(set((old['ts_utc_ns'].to_numpy()//pipeline.DAY).tolist())))
        pipeline.save(stage,old_days)
    intervals=pipeline.state_intervals(status,start,end)
    c=pipeline.connection()
    try:
        c.register('states',intervals)
        c.register('cached_minutes',actual.select(pipeline.KLINE_CORE[1:]+DIAGNOSTICS))
        candidates=c.execute('''SELECT DISTINCT m.ts_utc_ns FROM cached_minutes m JOIN states s
            ON s.start_ns<m.ts_utc_ns+60000000000 AND s.end_ns>m.ts_utc_ns WHERE s.phase='UNKNOWN' ''').fetchall()
        if candidates:
            # Recover true event-level counts only for the small ambiguous range.
            lo=min(int(row[0]) for row in candidates);hi=max(int(row[0]) for row in candidates)+pipeline.MINUTE
            probe=pipeline.connection()
            try:
                pipeline.sparse_minutes(probe,contract['trade_files'],intervals,lo,hi)
                precise=probe.execute('SELECT ts_utc_ns,unknown_status_trade_count FROM minutes').fetch_arrow_table()
            finally:probe.close()
            c.register('precise_unknown',precise)
            c.execute('''CREATE TEMP VIEW minutes AS SELECT m.* EXCLUDE(unknown_status_trade_count),
                coalesce(p.unknown_status_trade_count,m.unknown_status_trade_count)::UBIGINT unknown_status_trade_count
                FROM cached_minutes m LEFT JOIN precise_unknown p USING(ts_utc_ns)''')
        else:c.execute('CREATE TEMP VIEW minutes AS SELECT * FROM cached_minutes')
        bars=pipeline.minute_grid(c,ric,start,end,pipeline.previous_close_seed(ric,start))
        new_actual=bars.filter(pc.greater(bars['trade_count'],0))
        assert actual.select(pipeline.KLINE_CORE).equals(new_actual.select(pipeline.KLINE_CORE),check_metadata=False),ric
        assert int(pc.sum(bars['trade_count']).as_py() or 0)==original['source_trade_rows'],ric
        assert int(pc.sum(bars['volume']).as_py() or 0)==original['source_volume'],ric
        issues=c.from_arrow(bars).filter('outside_status_trade_count>0 OR unknown_status_trade_count>0 OR missing_exchange_time_trade_count>0 OR (NOT status_known AND trade_count>0)').arrow().read_all()
        stats=differences=None
        if native is not None:stats,differences=pipeline.compare_ib(c,ric,bars,native,contract['tick_size'])
    finally:c.close()
    public=pipeline.public_kline(bars)
    local_parquet(intervals,folder/'intervals.parquet');local_parquet(bars,folder/'kline.parquet')
    for kind,table in [('state_issues',issues),('differences',differences)]:
        path=folder/(kind+'.parquet')
        if table is not None and len(table):local_parquet(table,path)
        else:path.unlink(missing_ok=True)
    tas_files=pipeline.publish_daily(public,contract,'kline',pool,replace_schema=True)
    ib_files=len(set((native['ts_utc_ns'].to_numpy()//pipeline.DAY).tolist())) if native is not None else 0
    keep=sorted(set((public['ts_utc_ns'].to_numpy()//pipeline.DAY).tolist()))
    remove_stale(contract,old_days['tas'],keep)
    result=dict(ric=ric,rows=len(public),files=tas_files,ib_native_rows=original['ib_rows'],
        status_counts=dict(Counter(public['status'].to_pylist())),unknown_intervals=unknown_runs(public),
        unknown_trade_count=int(pc.sum(bars['unknown_status_trade_count']).as_py() or 0),
        unknown_minutes_with_trades=int(pc.sum(pc.and_(pc.equal(public['status'],'UNKNOWN'),pc.greater(public['trade_count'],0))).as_py() or 0),
        start_ns=start,end_ns=end,schema_version=pipeline.KLINE_SCHEMA_VERSION)
    original.update(kline_rows=len(public),state_issue_minutes=len(issues),comparison=stats,
        files=len(set((status['ts_utc_ns'].to_numpy()//pipeline.DAY).tolist()))+tas_files+ib_files,
        public_kline_schema_version=pipeline.KLINE_SCHEMA_VERSION,start_ns=start,end_ns=end)
    pipeline.save(original_path,original);pipeline.save(checkpoint,result)
    (PROBLEMS/(name+'.json')).unlink(missing_ok=True)
    return result


def checked(contract,status,lifetime,pool):
    try:return migrate(contract,status,lifetime,pool)
    except Exception as error:
        import traceback
        pipeline.save(PROBLEMS/(quote(contract['ric'],safe='_-.^')+'.json'),dict(ric=contract['ric'],problem=str(error),traceback=traceback.format_exc()))
        raise


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--ric');parser.add_argument('--workers',type=int,default=4)
    args=parser.parse_args();STATE.mkdir(parents=True,exist_ok=True)
    with (pipeline.RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        contracts=json.loads((pipeline.RUN/'contracts.json').read_text());lifetimes=prepare_lifetimes(contracts,pipeline.REF,pipeline.ROOT,STATE/'lifetimes.json')
        status=pq.ParquetFile(pipeline.WORK/'status.parquet').read();selected=[]
        for ric,contract in contracts.items():
            if args.ric and ric!=args.ric:continue
            if json.loads((pipeline.RUN/'contracts'/(quote(ric,safe='_-.^')+'.json')).read_text())['kline_rows']:selected.append(contract)
        selected.sort(key=lambda c:(c['ric'] not in {'ESM26','GCM26','NQH26','YMH26','GCJ28'},c['ric']))
        with ThreadPoolExecutor(max_workers=16) as publisher,ThreadPoolExecutor(max_workers=args.workers) as workers:
            futures=[workers.submit(checked,c,status.filter(pc.equal(status['ric'],c['ric'])),lifetimes[c['ric']],publisher) for c in selected]
            for future in as_completed(futures):
                row=future.result();print(json.dumps({k:v for k,v in row.items() if k!='unknown_intervals'},ensure_ascii=False),flush=True)
        if not args.ric:
            assert len(list((STATE/'contracts').glob('*.json')))==len(selected)
            unknown=[]
            for path in (STATE/'contracts').glob('*.json'):
                row=json.loads(path.read_text());unknown.extend(dict(ric=row['ric'],**item) for item in row['unknown_intervals'])
            target=pipeline.DEST/'meta/problems/unknown_status_intervals.csv'
            if unknown:pipeline.write_csv(target,unknown,['ric','start_utc','end_utc_exclusive','minutes','trade_count','volume'])
            else:target.unlink(missing_ok=True)
            pipeline.finalize(contracts)
        if PROBLEMS.exists() and not any(PROBLEMS.iterdir()):PROBLEMS.rmdir()


if __name__=='__main__':main()
