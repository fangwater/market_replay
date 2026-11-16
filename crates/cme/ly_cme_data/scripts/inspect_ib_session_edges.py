#!/usr/bin/env python3
"""Inspect five existing IB snapshots against Databento trading intervals.

Read-only inputs; findings and bounded analysis remain in project artifacts.
This observes the user's IB snapshot, not undocumented IB server internals.
"""
import argparse
import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as pipeline


def utc(value):
    return pd.Timestamp(int(value),unit='ns',tz='UTC').isoformat()


def inspect(ric, reasons):
    directory=pipeline.WORK/'contracts'/ric
    ib=pq.ParquetFile(directory/'kline_ib.parquet').read(
        columns=['ts_utc_ns','open','high','low','close','volume']).to_pandas()
    ib=ib.sort_values('ts_utc_ns',kind='stable').reset_index(drop=True)
    times=ib.ts_utc_ns.to_numpy()
    assert len(times) and np.all(times[1:]>times[:-1]),ric
    states=pq.ParquetFile(directory/'intervals.parquet').read().to_pylist()
    bars=pq.ParquetFile(directory/'kline.parquet').read(
        columns=['ts_utc_ns','close','trade_count']).to_pandas().set_index('ts_utc_ns')
    minute=pipeline.MINUTE
    closed=[];reopened=[]
    counts=dict(ric=ric,ib_rows=len(ib),first_utc=utc(times[0]),last_utc=utc(times[-1]),
        closed_intervals=0,scheduled_closed_intervals=0,scheduled_closed_rows=0,
        other_closed_rows=0,closed_rows=0,closed_zero_rows=0,
        aligned_reopen_intervals=0,reopen_first_minute_missing=0,
        first_available_zero_runs=0,zero_runs_equal_prior_close=0,
        zero_run_rows=0,tas_unpriced_rows_in_ib_zero_runs=0)
    for index,state in enumerate(states):
        start=state['start_ns'];end=state['end_ns']
        if state['is_trading'] is False:
            # Only whole minutes contained in an explicitly non-tradable state.
            lo=max((start+minute-1)//minute*minute,int(times[0]))
            hi=min(end-minute+1,int(times[-1])+1)
            if hi>lo:
                counts['closed_intervals']+=1
                reason=reasons.get((ric,start),'UNSPECIFIED')
                inside=ib.iloc[np.searchsorted(times,lo):np.searchsorted(times,hi)]
                scheduled=reason=='SCHEDULED'
                counts['scheduled_closed_intervals']+=int(scheduled)
                counts['scheduled_closed_rows' if scheduled else 'other_closed_rows']+=len(inside)
                counts['closed_rows']+=len(inside)
                counts['closed_zero_rows']+=int((inside.volume==0).sum())
                for row in inside.itertuples(index=False):
                    closed.append(dict(ric=ric,ts_utc=utc(row.ts_utc_ns),
                        closed_start_utc=utc(start),reopen_utc=utc(end),reason=reason,
                        open=row.open,high=row.high,low=row.low,close=row.close,volume=row.volume))
        if state['is_trading'] is not True or not index:continue
        previous=states[index-1]
        if previous['is_trading'] is not False:continue
        if start-previous['start_ns']<minute or start%minute:continue
        if start<int(times[0]) or start>int(times[-1]):continue
        counts['aligned_reopen_intervals']+=1
        k=int(np.searchsorted(times,start));stop=int(np.searchsorted(times,end))
        first_present=k<stop and int(times[k])==start
        counts['reopen_first_minute_missing']+=int(not first_present)
        row=dict(ric=ric,reopen_utc=utc(start),closed_start_utc=utc(previous['start_ns']),
            closed_seconds=(start-previous['start_ns'])/1e9,
            reason=reasons.get((ric,previous['start_ns']),'UNSPECIFIED'),
            first_minute_present=first_present,first_available_utc=None,zero_run_rows=0,
            all_zero_ohlc_equal_prior_close=None,prior_ib_close=None,prior_ib_utc=None,
            tas_unpriced_rows=0)
        if k<stop:
            row['first_available_utc']=utc(times[k])
            if k:
                row['prior_ib_close']=float(ib.iloc[k-1]['close'])
                row['prior_ib_utc']=utc(times[k-1])
            if ib.iloc[k].volume==0:
                values=ib.iloc[k:stop].volume.to_numpy()
                nonzero=np.flatnonzero(values!=0)
                n=int(nonzero[0]) if len(nonzero) else len(values)
                zero=ib.iloc[k:k+n]
                equal=bool(k and zero[pipeline.KLINE_PRICES].sub(row['prior_ib_close']).abs().le(1e-9).all(axis=1).all())
                row['zero_run_rows']=n
                row['all_zero_ohlc_equal_prior_close']=equal
                counts['first_available_zero_runs']+=1
                counts['zero_runs_equal_prior_close']+=int(equal)
                counts['zero_run_rows']+=n
                tas=bars.reindex(zero.ts_utc_ns)
                row['tas_unpriced_rows']=int(((tas.trade_count==0)&tas.close.isna()).sum())
                counts['tas_unpriced_rows_in_ib_zero_runs']+=row['tas_unpriced_rows']
        reopened.append(row)
    return counts,closed,reopened


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rics',nargs='+',default=['ESM26','NQM26','YMM26','GCM26','CLN26'])
    args=parser.parse_args()
    status=pq.ParquetFile(pipeline.WORK/'status.parquet').read(columns=[
        'ric','ts_event_ns','ts_recv_ns','is_midnight_snapshot','is_trading','reason_name'])
    status=status.filter(pc.is_in(status['ric'],value_set=pa.array(args.rics)))
    reasons={}
    for row in status.to_pylist():
        if row['is_trading']!='N':continue
        at=row['ts_recv_ns'] if row['is_midnight_snapshot'] or row['ts_event_ns']==2**64-1 else row['ts_event_ns']
        reasons.setdefault((row['ric'],at),row['reason_name'])
    summaries=[];closed=[];reopened=[]
    for ric in args.rics:
        stats,halt,opens=inspect(ric,reasons)
        summaries.append(stats);closed.extend(halt);reopened.extend(opens)
        print(json.dumps(stats,ensure_ascii=False),flush=True)
    out=pipeline.ROOT/'artifacts/ib_session_edges_20261110'
    pipeline.save(out/'summary.json',dict(
        scope='Five contract snapshots within existing 2026 status/IB windows; not all IB instruments',
        time_basis='UTC minute starts; whole-minute non-trading rows; reopen analysis requires a minute-aligned boundary after at least 60s non-trading',
        contracts=summaries))
    for name,rows in [('closed_ib_rows',closed),('reopen_boundaries',reopened)]:
        if rows:pipeline.write_csv(out/(name+'.csv'),rows,list(rows[0]))


if __name__=='__main__':main()
