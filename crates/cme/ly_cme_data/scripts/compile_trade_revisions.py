#!/usr/bin/env python3
"""Compile source-witnessed TAS revisions; require completed identity recovery."""
import argparse
from collections import Counter,defaultdict
import hashlib
import json

import pyarrow.parquet as pq

import build_market_status_klines as pipeline
from prepare_trade_revisions import RUN,WORK
from trade_revision_ledger import source_record,resolve
from tas_trade_revisions import CATALOG,VERSION


def digest(plan):
    return hashlib.sha256(json.dumps(plan,sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def compile_plans(preview=False,allow_pending_sources=False):
    jobs=[]
    for label in ['daily','historical']:
        jobs+=json.loads((RUN/f'identity_jobs_{label}.json').read_text())
    missing=[j['id'] for j in jobs if not (WORK/'identities'/(j['id']+'.done.json')).exists()]
    if missing and not preview and not allow_pending_sources:raise RuntimeError(f'Identity recovery incomplete: {len(missing)} source jobs remain')
    by_ric=defaultdict(list);seen=set();counts=Counter()
    for job in sorted(jobs,key=lambda j:j['id']):
        path=WORK/'identities'/(job['id']+'.jsonl')
        if not path.exists():continue
        for line in path.open():
            record=json.loads(line);key=(record['source'],record['line'])
            if key in seen:continue
            seen.add(key)
            row=source_record(record);by_ric[row['ric']].append(row);counts[row['kind']]+=1
    archived=json.loads((RUN/'corrections.json').read_text())['corrections']
    def key(row):return tuple(row.get(k) for k in ('ric','published_ns','original_price','original_volume'))
    raw_counts=Counter(key(r) for rows in by_ric.values() for r in rows if r['kind']=='Correction')
    absent=Counter(key(r) for r in archived)-raw_counts
    if absent and not preview and not allow_pending_sources:raise RuntimeError(f'{sum(absent.values())} archived Correction rows lack source witnesses: {list(absent.items())[:3]}')
    pending=defaultdict(list);remaining=absent.copy()
    for row in archived:
        if remaining[key(row)]>0:
            pending[row['ric']].append(dict(row,action='pending',reason='source_witness_pending',source=row['db'],line=None))
            remaining[key(row)]-=1
    allowed=json.loads((RUN/'db_plan.json').read_text())['rics']
    plans={};stats=Counter()
    for ric in sorted(allowed):
        path=WORK/'candidates'/(ric+'.parquet')
        candidates=[dict(t,ric=ric) for t in pq.ParquetFile(path).read().to_pylist()] if path.exists() else []
        result=resolve(candidates,by_ric[ric])
        result['unresolved']+=pending[ric]
        plan=dict(version=VERSION,ric=ric,source_scope='local archived 2026 TAS sources as of 2026-11-10; historical netting, not a causal replay',
            source_corrections=sum(r['kind']=='Correction' for r in by_ric[ric]),**result)
        plans[ric]=plan;stats.update({k:len(v) for k,v in result.items()})
        if not preview:
            pipeline.save(RUN/'plan_history'/(digest(plan)+'.json'),plan)
            pipeline.save(CATALOG/(ric+'.json'),plan)
    summary=dict(source_jobs=len(jobs),missing_jobs=len(missing),archived_rows=len(archived),
        archived_without_source=sum(absent.values()),pending_sources_explicit=bool(allow_pending_sources),source_rows=dict(counts),totals=dict(stats),
        affected_rics=sum(bool(p['patches'] or p['insertions']) for p in plans.values()),
        unresolved_reasons=dict(Counter(r['reason'] for p in plans.values() for r in p['unresolved'])))
    pipeline.save(RUN/('preview_summary.json' if preview else 'compiled_summary.json'),summary)
    if not preview:
        # Only unresolved, actual problems are published to the user disk.
        rows=[]
        for ric,plan in plans.items():
            for r in plan['unresolved']:
                rows.append(dict(ric=ric,published_utc=pipeline.pd.Timestamp(r['published_ns'],unit='ns',tz='UTC').isoformat(),
                    action=r['action'],reason=r['reason'],original_uid=r.get('original_uid'),original_seq=r.get('original_seq'),
                    original_price_e9=r.get('original_price'),original_volume=r.get('original_volume'),
                    exchange_hms_ns=r.get('hms'),candidates=r.get('candidates'),
                    source=r.get('source'),line=r.get('line')))
        path=pipeline.DEST/'meta/problems/tas_trade_revisions_unresolved.csv'
        if rows:pipeline.write_csv(path,rows,list(rows[0]))
        else:path.unlink(missing_ok=True)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--preview',action='store_true')
    parser.add_argument('--allow-pending-sources',action='store_true',help='apply only witnessed events; record every unscanned archived event as pending')
    args=parser.parse_args();print(json.dumps(compile_plans(args.preview,args.allow_pending_sources),ensure_ascii=False),flush=True)
