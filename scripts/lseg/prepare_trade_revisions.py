#!/usr/bin/env python3
"""Find exact source-print candidates for archived TAS corrections, read-only."""
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import build_market_status_klines as pipeline

RUN=pipeline.ROOT/'artifacts/tas_trade_corrections_20261110'
WORK=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/trade_correction_work')


def candidates(ric,rows,job):
    target=WORK/'candidates'/(ric+'.parquet')
    if target.exists():return dict(ric=ric,rows=pq.ParquetFile(target).metadata.num_rows)
    pairs=set()
    for row in rows:
        for p,v in [('original_price','original_volume'),('price','volume')]:
            if row[p] is not None and row[v] is not None:pairs.add((row[p],row[v]))
    keys=pa.table({'price':pa.array([p for p,v in pairs],type=pa.int64()),'volume':pa.array([v for p,v in pairs],type=pa.uint64())})
    c=pipeline.connection()
    try:
        c.register('keys',keys)
        table=c.execute('''WITH raw AS (
            SELECT ts_utc_ns::BIGINT published_ns,exch_hms_ns,price,volume,filename,file_row_number,
                (ts_utc_ns::HUGEINT // 86400000000000)*86400000000000+exch_hms_ns::HUGEINT provisional_ns
            FROM read_parquet(?,hive_partitioning=false,filename=true,file_row_number=true)
            WHERE ts_utc_ns<=?), wanted AS (SELECT r.* FROM raw r SEMI JOIN keys k USING(price,volume))
            SELECT *, (CASE WHEN exch_hms_ns=18446744073709551615 THEN published_ns
                WHEN provisional_ns>published_ns+43200000000000 THEN provisional_ns-86400000000000
                WHEN provisional_ns<published_ns-43200000000000 THEN provisional_ns+86400000000000
                ELSE provisional_ns END)::BIGINT event_ns
            FROM wanted ORDER BY published_ns,filename,file_row_number''',
            [job['trade_files'],max(r['published_ns'] for r in rows)]).fetch_arrow_table()
    finally:c.close()
    target.parent.mkdir(parents=True,exist_ok=True);temporary=target.with_suffix('.parquet.partial')
    pq.write_table(table,temporary,compression='zstd');temporary.replace(target)
    return dict(ric=ric,rows=len(table))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--workers',type=int,default=4);args=parser.parse_args()
    corrections=json.loads((RUN/'corrections.json').read_text())['corrections']
    jobs=json.loads((pipeline.RUN/'contracts.json').read_text());groups={}
    for row in corrections:groups.setdefault(row['ric'],[]).append(row)
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures=[pool.submit(candidates,ric,rows,jobs[ric]) for ric,rows in sorted(groups.items())]
        for future in as_completed(futures):print(json.dumps(future.result()),flush=True)


if __name__=='__main__':main()
