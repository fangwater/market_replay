#!/usr/bin/env python3
"""Daily Databento states, state-aware TAS minutes and IB comparison.

Run with reference_cme/.venv/bin/python (Databento and DuckDB).
Source databases, DBN, ticks and IB snapshots are read-only. Restart state and
working Parquet stay local; only requested data and actual problems go to NAS.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime, timezone
import csv
import fcntl
import json
import os
from pathlib import Path
import re
import sys
import uuid
from urllib.parse import quote

import databento as db
import databento_dbn as dbn
import duckdb
duckdb.close()  # Unused global connection; real connections are explicit below.
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from group_lseg_by_exchange import references, classify
from parquet_delivery_checks import check_footer
from kline_lifetimes import prepare_lifetimes,window as lifetime_window

ROOT = Path(__file__).resolve().parents[1]
REF = Path('/home/fanghaizhou/cn_futures_db/reference_cme')
DATA = Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data')
DEST = Path('/mnt/nfs/30.100_cta_data/lseg')
RUN = ROOT/'artifacts/market_status_kline_20261110'
WORK = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/market_status_kline_20261110')
IB = DATA/'ib_minute_parquet/covered_futures_20261103'
DAY = 86_400_000_000_000
MINUTE = 60_000_000_000
START = int(pd.Timestamp('2026-01-01', tz='UTC').value)
YEAR_END = int(pd.Timestamp('2027-01-01', tz='UTC').value)
MONTHS = 'FGHJKMNQUVXZ'
PRODUCTS = {'ES':'ES','NQ':'NQ','RTY':'RTY','YM':'YM','GC':'GC','CL':'CL',
            'SR3':'SRA','KRW':'KWR','6E':'URO','SR1':'S1R'}
PATTERN = re.compile(r'^(ES|NQ|RTY|YM|GC|CL|SR3|KRW|6E|SR1)([FGHJKMNQUVXZ])(\d{1,2})$')
KLINE_CORE = ['ric','ts_utc_ns','open','high','low','close','volume','trade_count']
KLINE_SCHEMA_VERSION = '5'
FILL_POLICY_VERSION = '5'
KLINE_PRICES = ['open', 'high', 'low', 'close']
SOURCES = [REF/'cme_equity_index_status_2024_2026.dbn',
           REF/'batch_downloads/GLBX-20260903-NHHKXUT6LW/glbx-mdp3-20200101-20260903.status.dbn.zst']


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.partial')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
    temp.replace(path)


def write_csv(path, rows, fields):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.partial')
    with temp.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
        writer.writeheader(); writer.writerows(rows)
    temp.replace(path)


def connection(threads=2):
    cap=os.environ.get('LY_CME_DUCKDB_THREADS')
    if cap is not None:
        if int(cap)!=1:raise ValueError('project DuckDB thread budget must be 1')
        threads=min(int(threads),int(cap))
    memory=os.environ.get('LY_CME_DUCKDB_MEMORY','3GB')
    if cap is not None and memory!='2GB':raise ValueError('project DuckDB memory budget must be 2GB')
    c = duckdb.connect(config={'threads':int(threads),'memory_limit':memory})
    temporary=WORK/'duckdb_temp'/f'{os.getpid()}_{uuid.uuid4().hex}'
    temporary.mkdir(parents=True,exist_ok=True)
    c.execute('SET temp_directory=?', [str(temporary)])
    c.execute('SET preserve_insertion_order=true')
    # Arrow scans can be estimated as one row: avoid an accidental quadratic
    # nested-loop ASOF plan for hundreds of thousands of status records.
    c.execute('SET asof_loop_join_threshold=0')
    return c


def source_inventory():
    """Use existing local tick indexes; never walk the entire NAS tree."""
    by_ric = defaultdict(list)
    old = DATA/'tas_tick_parquet/2026'
    with (old/'_audit/ric_inventory.csv').open() as stream:
        for row in csv.DictReader(stream):
            if row['data_type'] == 'trade':
                by_ric[row['ric']].extend(str(p) for p in sorted((old/'trade'/('ric='+quote(row['ric'], safe='_-.^'))).glob('*.parquet')))
    for path in sorted((ROOT/'artifacts/tas_daily_ingest_20261109/products').glob('*/completion.json')):
        record = json.loads(path.read_text())
        for item in record.get('published_files', []):
            if item['data_type'] == 'trade':
                by_ric[item['ric']].append(item['source_export_file'])
    supplement = json.loads((ROOT/'artifacts/tas_supplement_parquet_20261109/completion.json').read_text())
    for item in supplement['files']:
        if item['data_type'] == 'trade':
            by_ric[item['ric']].append(item['source_export_file'])
    for ric, files in by_ric.items():
        assert len(files) == len(set(files)), ric
    return dict(by_ric)


def publish_source_quality():
    path=REF/'batch_downloads/GLBX-20260903-NHHKXUT6LW/condition.json'
    rows=[dict(utc_date=r['date'],condition=r['condition'],
        detail='GLBX数据集日级质量标记，非status专属缺失证明；保留状态及成交，比较时结合本项复核')
        for r in json.loads(path.read_text()) if '2026-01-01'<=r['date']<'2026-09-04' and r['condition']!='available']
    if rows:write_csv(DEST/'meta/problems/databento_source_quality.csv',rows,['utc_date','condition','detail'])


def extract_status():
    """Stream DBN as packed arrays; discard non-2026/spreads before pandas."""
    cache = WORK/'mapped_status.parquet'
    if cache.exists():
        return pq.ParquetFile(cache).read(), json.loads((RUN/'status_inputs.json').read_text())
    WORK.mkdir(parents=True, exist_ok=True)
    parts = []; mappings = []; inputs = []
    for source_index, path in enumerate(SOURCES):
        store = db.DBNStore.from_file(path)
        metadata = store.metadata
        local_mappings = []
        for symbol, intervals in metadata.mappings.items():
            if not PATTERN.fullmatch(symbol):
                continue
            for interval in intervals:
                lo = int(pd.Timestamp(interval['start_date'], tz='UTC').value)
                hi = int(pd.Timestamp(interval['end_date'], tz='UTC').value)
                if lo < YEAR_END and hi > START:
                    local_mappings.append(dict(source_index=source_index, instrument_id=int(interval['symbol']),
                        raw_symbol=symbol, map_start_ns=lo, map_end_ns=hi))
        ids = np.array(sorted({m['instrument_id'] for m in local_mappings}), dtype=np.uint32)
        mappings.extend(local_mappings)
        input_info = dict(path=str(path), bytes=path.stat().st_size, start=int(metadata.start), end=int(metadata.end),
                          symbols=list(metadata.symbols))
        inputs.append(input_info)
        offset = 0
        for a in store.to_ndarray(count=1_000_000):
            mask = (a['ts_recv'] >= START) & (a['ts_recv'] < YEAR_END)
            indexes = np.flatnonzero(mask)
            if indexes.size:
                indexes = indexes[np.isin(a['instrument_id'][indexes], ids)]
            if indexes.size:
                b = a[indexes]
                frame = pd.DataFrame({name:b[name] for name in b.dtype.names
                    if name not in ['length','rtype','_reserved']})
                for name in ['is_trading','is_quoting','is_short_sell_restricted']:
                    frame[name] = frame[name].str.decode('ascii')
                frame['source_index'] = source_index
                frame['source_order'] = offset + indexes
                parts.append(pa.Table.from_pandas(frame, preserve_index=False))
            offset += len(a)
    raw = pa.concat_tables(parts)
    mapping_table = pa.Table.from_pylist(mappings)
    c = connection()
    c.register('raw', raw); c.register('maps', mapping_table)
    # Require one unique time-valid raw symbol per source record.
    mapped = c.execute('''SELECT r.*,m.raw_symbol FROM raw r JOIN maps m
        ON r.source_index=m.source_index AND r.instrument_id=m.instrument_id
        AND r.ts_recv>=m.map_start_ns AND r.ts_recv<m.map_end_ns
        ORDER BY r.source_index,r.source_order''').fetch_arrow_table()
    assert len(mapped) == len(raw), 'DBN symbology mapping is missing or ambiguous'
    pq.write_table(mapped, cache, compression='zstd')
    save(RUN/'status_inputs.json', inputs)
    return mapped, inputs


def normalized_status():
    path = WORK/'status.parquet'
    contracts_path = RUN/'contracts.json'
    if path.exists() and contracts_path.exists():
        return pq.ParquetFile(path).read(), json.loads(contracts_path.read_text())
    mapped, inputs = extract_status()
    c = connection()
    c.register('s', mapped)
    definition_file = REF/'parsed/core_six_definition_2020_present/definition_outrights.parquet'
    definitions=pq.ParquetFile(definition_file).read(columns=['instrument_id','raw_symbol','ts_recv',
        'maturity_year','maturity_month','min_price_increment','exchange'])
    definitions=definitions.append_column('known_ns',definitions['ts_recv'].cast(pa.int64()))
    c.register('definition_input',definitions)
    c.execute('''CREATE TABLE definitions AS SELECT instrument_id,raw_symbol,
        known_ns::UBIGINT known_ns,maturity_year::INTEGER maturity_year,
        maturity_month::INTEGER maturity_month,min_price_increment,exchange
        FROM definition_input''')
    frame = c.execute('''SELECT s.*, d.maturity_year,d.maturity_month,d.min_price_increment,d.exchange AS definition_exchange
        FROM s ASOF LEFT JOIN definitions d ON s.instrument_id=d.instrument_id
        AND s.raw_symbol=d.raw_symbol AND s.ts_recv>=d.known_ns''').fetchdf()
    source_files = source_inventory()
    refs = references([Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
                       ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv'])
    ib_plan = json.loads((IB/'_audit/contract_plan.json').read_text())
    ib_jobs = {j['ric']:j for j in ib_plan['jobs']}
    known_rics = set(refs) | set(source_files) | set(ib_jobs)
    prior_plan=ROOT/'artifacts/tas_status_20261109/plan.json'
    if prior_plan.exists():known_rics.update(json.loads(prior_plan.read_text())['exchanges'])
    prior_problems=DEST/'meta/problems/status_mapping.csv'
    if prior_problems.exists():
        with prior_problems.open(encoding='utf-8-sig',newline='') as stream:
            known_rics.update(r['ric'] for r in csv.DictReader(stream))
    extra_candidates = defaultdict(set)
    # One-digit RICs (notably KRW) use actual dated HRD, never a decade guess.
    hrd_paths = [Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
                 ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']
    def hrd_records():
        for hrd_path in hrd_paths:
            with hrd_path.open(encoding='utf-8-sig',newline='') as stream:yield from csv.DictReader(stream)
    for record in hrd_records():
            ric = record['RIC']
            m = re.fullmatch(r'(URO|SRA|S1R|KWR)([FGHJKMNQUVXZ])(\d{1,2})', ric)
            if not m or not record.get('Expiration Date'):
                continue
            try: expiry = pd.to_datetime(record['Expiration Date'], format='%m/%d/%Y', utc=True)
            except ValueError: continue
            if expiry.value < START - 31*DAY:
                continue
            root, month, suffix = m.groups()
            year = 2000+int(suffix) if len(suffix)==2 else expiry.year + int(month=='F' and expiry.month==12)
            if year % (100 if len(suffix)==2 else 10) != int(suffix):
                continue
            for db_root, lseg_root in PRODUCTS.items():
                if lseg_root == root:
                    extra_candidates[(db_root, month, year % 10)].add((ric, year))
    identities = {}; rejected = []
    for row in frame[['source_index','instrument_id','raw_symbol','maturity_year','maturity_month','min_price_increment','definition_exchange']].drop_duplicates().itertuples(index=False):
        db_root, month, suffix = PATTERN.fullmatch(row.raw_symbol).groups()
        product = PRODUCTS[db_root]
        if pd.notna(row.maturity_year) and pd.notna(row.maturity_month):
            year = int(row.maturity_year)
            assert int(row.maturity_month)==MONTHS.index(month)+1, row
            canonical = f'{product}{month}{year%100:02d}'
            aliases = [r for r in [canonical, f'{product}{month}{year%10}'] if r in known_rics]
            ric = aliases[0] if aliases else None
        else:
            candidates = extra_candidates.get((db_root,month,int(suffix)%10),set())
            if len(suffix)==2:
                candidates = {(ric,y) for ric,y in candidates if y%100==int(suffix)}
            candidates = {(ric,y) for ric,y in candidates if ric in known_rics}
            # Prefer the actual two-digit RIC if the same HRD identity has aliases.
            years = {y for _,y in candidates}
            if len(years)==1:
                year=next(iter(years)); ric=sorted((r for r,y in candidates),key=lambda r:(-len(r),r))[0]
            else:
                ric=None; year=None
        key = (int(row.source_index),int(row.instrument_id),row.raw_symbol)
        if ric is None:
            rejected.append(dict(source_index=key[0],instrument_id=key[1],raw_symbol=key[2],
                problem='缺少唯一的LSEG RIC／交割年月证据，未凭一位年份猜测身份'))
            continue
        venue = classify(ric,'futures',refs.get(ric,{}),{})['exchange'].lower()
        if venue not in ['cme','cbot','comex','nymex','review']:
            rejected.append(dict(source_index=key[0],instrument_id=key[1],raw_symbol=key[2],
                problem=f'GLBX合约与同名LSEG RIC交易所不符：{ric}/{venue}，不合并'))
            continue
        # A vetted definition MIC can classify a new far contract without guessing its prefix.
        if venue=='review' and row.definition_exchange in ['XCME','XCBT','XCEC','XNYM']:
            venue={'XCME':'cme','XCBT':'cbot','XCEC':'comex','XNYM':'nymex'}[row.definition_exchange]
        value = dict(ric=ric,product=product,databento_product=db_root,exchange=venue,
                     delivery_year=year,delivery_month=MONTHS.index(month)+1,
                     tick_size=float(row.min_price_increment) if pd.notna(row.min_price_increment) else None)
        if key in identities:
            assert identities[key]==value, ('conflicting dated identity',key)
        identities[key]=value
    if rejected:
        unique = {tuple(r.values()):r for r in rejected}
        write_csv(DEST/'meta/problems/market_status_identity.csv', list(unique.values()),
            ['source_index','instrument_id','raw_symbol','problem'])
    mapping_rows=[dict(source_index=k[0],instrument_id=k[1],raw_symbol=k[2],**v) for k,v in identities.items()]
    c.register('ids',pa.Table.from_pylist(mapping_rows));c.register('events',mapped)
    unmapped=c.execute('''SELECT e.*, '缺少唯一的LSEG合约身份映射，保留原始Databento状态待核实'::VARCHAR issue
        FROM events e ANTI JOIN ids USING(source_index,instrument_id,raw_symbol)
        ORDER BY source_index,source_order''').fetch_arrow_table()
    if len(unmapped):
        problem=DEST/'meta/problems/unmapped_market_status.parquet'
        temp=problem.with_suffix('.parquet.partial')
        pq.write_table(unmapped,temp,compression='zstd');temp.replace(problem)
    result=c.execute('''SELECT ids.ric,ids.product,ids.databento_product,ids.exchange,
        e.ts_recv::UBIGINT ts_utc_ns,e.ts_event::UBIGINT ts_event_ns,e.ts_recv::UBIGINT ts_recv_ns,
        e.instrument_id,e.publisher_id,e.raw_symbol,e.action,e.reason,e.trading_event,
        e.is_trading,e.is_quoting,e.is_short_sell_restricted,
        (e.ts_recv % 86400000000000 = 0) is_midnight_snapshot,e.source_index,e.source_order
        FROM events e JOIN ids USING(source_index,instrument_id,raw_symbol)
        ORDER BY ric,ts_recv_ns,source_index,source_order''').fetch_arrow_table()
    enum_names=lambda cls:{int(getattr(cls,n)):n for n in dir(cls) if n.isupper()}
    for col,cls in [('action',dbn.StatusAction),('reason',dbn.StatusReason),('trading_event',dbn.TradingEvent)]:
        names=enum_names(cls)
        result=result.append_column(col+'_name',pa.array([names.get(x,'UNKNOWN') for x in result[col].to_pylist()]))
    metadata={b'source':b'Databento GLBX.MDP3 status',
        b'time_basis':b'UTC nanoseconds; directory date and ts_utc_ns use ts_recv; exchange time is ts_event_ns',
        b'is_midnight_snapshot':b'ts_recv at exact UTC midnight; initial state, not a new opening',
        b'flags':b'Y=yes, N=no, ~=unknown; original codes retained'}
    result=result.replace_schema_metadata(metadata)
    pq.write_table(result,path,compression='zstd')
    contracts={}
    for identity_key,v in identities.items():
        ric=v['ric']
        if ric not in contracts:
            contracts[ric]=dict(v,trade_files=source_files.get(ric,[]),ib_file=str(IB/'data'/f'{ric}.parquet') if ric in ib_jobs else None,
                ib_contract=ib_jobs.get(ric),start_ns=START,end_ns=min(YEAR_END,inputs[identity_key[0]]['end']))
    contracts={ric:v for ric,v in contracts.items() if ric in set(result['ric'].to_pylist())}
    save(contracts_path,contracts)
    return result,contracts


def publish_unmapped_status(status):
    target=DEST/'meta/problems/unmapped_market_status.parquet'
    if target.exists():return
    mapped=pq.ParquetFile(WORK/'mapped_status.parquet').read()
    c=connection();c.register('events',mapped);c.register('valid',status.select(['source_index','instrument_id','raw_symbol']))
    missing=c.execute('''SELECT e.*, '缺少唯一的LSEG合约身份映射，保留原始Databento状态待核实'::VARCHAR issue
        FROM events e ANTI JOIN (SELECT DISTINCT * FROM valid) v USING(source_index,instrument_id,raw_symbol)
        ORDER BY source_index,source_order''').fetch_arrow_table()
    if len(missing):
        temp=target.with_suffix('.parquet.partial');pq.write_table(missing,temp,compression='zstd');temp.replace(target)
    c.close()


def state_intervals(status, start, end):
    """Reconstruct reported history using event time; midnight rows seed state."""
    frame=status.to_pandas() if isinstance(status,pa.Table) else status.copy()
    snapshot=frame.is_midnight_snapshot.to_numpy(dtype=bool)
    event=frame.ts_event_ns.to_numpy(dtype=np.uint64)
    recv=frame.ts_recv_ns.to_numpy(dtype=np.uint64)
    invalid=(event==np.iinfo(np.uint64).max)
    frame['effective_ns']=np.where(snapshot|invalid,recv,event)
    frame=frame.sort_values(['effective_ns','ts_recv_ns','source_index','source_order'],kind='stable')
    cursor=start;state=None;phase='UNKNOWN';session=0;rows=[]
    for row in frame.itertuples(index=False):
        at=int(row.effective_ns)
        if at>=end:break
        if row.is_trading in ('Y','N'):
            new=row.is_trading=='Y'
        elif int(row.action)==7:
            new=True
        elif int(row.action) in [1,2,8,9,10,12,13,15]:
            new=False
        else:
            continue  # NONE/NO_CANCEL and unknown flags do not manufacture a phase change.
        reason=int(getattr(row,'reason',0))
        action=int(row.action)
        if new:
            next_phase='TRADING'
        elif reason==5:
            next_phase='EXPIRED'
        elif reason==1:
            # A halted instrument can go through scheduled PRE_OPEN on its way
            # back to trading; this is still inside that temporary interruption.
            next_phase='HALTED' if phase=='HALTED' and action in (0,1) else 'SCHEDULED_CLOSED'
        elif reason in (2,3,6) or action in (8,9,10):
            # Weekend startup also emits surveillance/recovery messages. Only
            # an interruption of a known trading session creates halt padding.
            next_phase='HALTED' if phase in ('TRADING','HALTED') else phase if phase in ('SCHEDULED_CLOSED','EXPIRED') else 'UNKNOWN'
        else:
            next_phase=phase if state is False else 'UNKNOWN'
        at=max(start,at)
        if new==state and next_phase==phase:continue
        if at>cursor:
            rows.append(dict(start_ns=cursor,end_ns=at,is_trading=state,session_id=session if state else None,phase=phase))
        if new and state is not True:session+=1
        cursor=at;state=new;phase=next_phase
    if cursor<end:
        rows.append(dict(start_ns=cursor,end_ns=end,is_trading=state,session_id=session if state else None,phase=phase))
    schema=pa.schema([('start_ns',pa.int64()),('end_ns',pa.int64()),('is_trading',pa.bool_()),('session_id',pa.int64()),('phase',pa.string())])
    return pa.Table.from_pylist(rows,schema=schema)


def sparse_minutes(c, files, intervals, start, end, revision_plan=None,event_minutes=None):
    """Aggregate prints after applying an audited historical correction ledger."""
    if 'phase' not in intervals.schema.names:
        intervals=intervals.append_column('phase',pa.array([
            'UNKNOWN' if value is None else 'TRADING' if value else 'SCHEDULED_CLOSED'
            for value in intervals['is_trading'].to_pylist()],type=pa.string()))
    c.register('states',intervals)
    if not files and revision_plan is None:
        return None
    selected=''
    if event_minutes is not None:
        selected=' AND event_ns // 60000000000 * 60000000000 IN ('+','.join(str(int(value)) for value in sorted(event_minutes))+')'
    if not files:
        c.execute('''CREATE TEMP TABLE trade_clock_gross(published_ns BIGINT,exch_hms_ns UBIGINT,
            price BIGINT,volume UINTEGER,filename VARCHAR,file_row_number BIGINT,event_ns BIGINT)''')
    else:c.execute('''CREATE TEMP TABLE trade_clock_gross AS
        WITH source AS (SELECT ts_utc_ns::BIGINT published_ns,exch_hms_ns,
            price,volume,filename,file_row_number,
            (ts_utc_ns::HUGEINT // 86400000000000)*86400000000000+exch_hms_ns::HUGEINT provisional_ns
            FROM read_parquet(?,hive_partitioning=false,filename=true,file_row_number=true)
            WHERE ts_utc_ns>=? AND ts_utc_ns<?),
        clock AS (SELECT *, (CASE WHEN exch_hms_ns=18446744073709551615 THEN published_ns
            WHEN exch_hms_ns>=86400000000000 THEN error('invalid exchange time')
            WHEN provisional_ns>published_ns+43200000000000 THEN provisional_ns-86400000000000
            WHEN provisional_ns<published_ns-43200000000000 THEN provisional_ns+86400000000000
            ELSE provisional_ns END)::BIGINT event_ns FROM source)
        SELECT published_ns,exch_hms_ns,price,volume,filename,file_row_number,event_ns
        FROM clock WHERE event_ns>=? AND event_ns<?'''+selected,[files,start-DAY//2,end+DAY//2,start,end])
    if revision_plan is not None:
        from tas_trade_revisions import apply
        apply(c,revision_plan,start,end,event_minutes)
    else:c.execute('CREATE TEMP VIEW trade_clock AS SELECT * FROM trade_clock_gross')
    c.execute('''CREATE TEMP TABLE minutes AS
        SELECT (e.event_ns // 60000000000)*60000000000 ts_utc_ns,
            first(price ORDER BY event_ns,published_ns,filename,file_row_number)::BIGINT AS "open",
            max(price)::BIGINT high,min(price)::BIGINT low,
            last(price ORDER BY event_ns,published_ns,filename,file_row_number)::BIGINT AS "close",
            sum(volume)::UBIGINT volume,count(*)::UBIGINT trade_count,
            count_if(s.is_trading=false)::UBIGINT outside_status_trade_count,
            count_if(s.is_trading IS NULL OR s.phase='UNKNOWN')::UBIGINT unknown_status_trade_count,
            count_if(e.exch_hms_ns=18446744073709551615)::UBIGINT missing_exchange_time_trade_count,
            count_if(e.published_ns // 60000000000 > e.event_ns // 60000000000)::UBIGINT late_trade_count
        FROM trade_clock e ASOF LEFT JOIN states s ON e.event_ns>=s.start_ns
        GROUP BY 1''')
    counts=c.execute('''SELECT count(*),coalesce(sum(volume),0),
        count_if(exch_hms_ns=18446744073709551615) FROM trade_clock''').fetchone()
    return tuple(0 if v is None else int(v) for v in counts)


def minute_grid(c, ric, start, end, previous_close=None):
    c.execute('''CREATE TEMP TABLE minute_status AS
        WITH indexes AS (SELECT unnest(generate_series(?::BIGINT,?::BIGINT-60000000000,60000000000)) ts)
        SELECT i.ts AS ts_utc_ns,
            coalesce(sum(CASE WHEN s.is_trading=true THEN least(s.end_ns,i.ts+60000000000)-greatest(s.start_ns,i.ts) ELSE 0 END),0)::UBIGINT trading_ns,
            coalesce(sum(CASE WHEN s.phase='HALTED' THEN least(s.end_ns,i.ts+60000000000)-greatest(s.start_ns,i.ts) ELSE 0 END),0)::UBIGINT halted_ns,
            coalesce(sum(CASE WHEN s.phase='SCHEDULED_CLOSED' THEN least(s.end_ns,i.ts+60000000000)-greatest(s.start_ns,i.ts) ELSE 0 END),0)::UBIGINT scheduled_closed_ns,
            coalesce(sum(CASE WHEN s.phase IN ('TRADING','HALTED','SCHEDULED_CLOSED') THEN least(s.end_ns,i.ts+60000000000)-greatest(s.start_ns,i.ts) ELSE 0 END),0)=60000000000 status_known,
            max(s.session_id) FILTER(WHERE s.is_trading=true) session_id
        FROM indexes i LEFT JOIN states s ON s.start_ns<i.ts+60000000000 AND s.end_ns>i.ts
        WHERE i.ts>=? AND i.ts+60000000000<=?
        GROUP BY i.ts''',[start,end,start,end])
    result=c.execute('''WITH joined AS (
        SELECT g.*,m.* EXCLUDE(ts_utc_ns),
            coalesce(last_value(m.close IGNORE NULLS) OVER(ORDER BY g.ts_utc_ns
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),?::BIGINT) prior_close
        FROM minute_status g LEFT JOIN minutes m USING(ts_utc_ns))
        SELECT ?::VARCHAR ric,ts_utc_ns::UBIGINT ts_utc_ns,
            coalesce("open",prior_close)::BIGINT AS "open",
            coalesce(high,prior_close)::BIGINT high,
            coalesce(low,prior_close)::BIGINT low,
            coalesce("close",prior_close)::BIGINT AS "close",
            coalesce(volume,0)::UBIGINT volume,coalesce(trade_count,0)::UBIGINT trade_count,
            (coalesce(trade_count,0)=0) is_empty,trading_ns,halted_ns,scheduled_closed_ns,status_known,
            coalesce(outside_status_trade_count,0)::UBIGINT outside_status_trade_count,
            coalesce(unknown_status_trade_count,0)::UBIGINT unknown_status_trade_count,
            coalesce(missing_exchange_time_trade_count,0)::UBIGINT missing_exchange_time_trade_count,
            coalesce(late_trade_count,0)::UBIGINT late_trade_count
        FROM joined ORDER BY ts_utc_ns''',[previous_close,ric]).fetch_arrow_table()
    metadata={b'source':b'LSEG TAS kind=1 with Databento GLBX.MDP3 status',b'frequency':b'1min; UTC minute-start labels',
        b'price_scale':b'1000000000; open/high/low/close are exact nullable Int64 e9',
        b'trade_time_basis':b'exchange time, nearest UTC date within 12h of publication; missing exchange time falls back to publication and is counted',
        b'empty_minutes':b'continuous UTC minutes in the valid instrument/covered window, including scheduled closures and unknown states; own previous close carries across all breaks; null until an own prior trade exists',
        b'fill_policy_version':FILL_POLICY_VERSION.encode(),
        b'exception_policy':b'all actual trades within the covered window preserved, including closed/unknown-state and late prints; annotated, never silently deleted',
        b'status_time_basis':b'historical exchange event time; midnight snapshot seeds state at ts_recv; no scheduled calendar filter'}
    return result.replace_schema_metadata(metadata)


def kline_float_prices(table):
    """Convert fixed-point work prices once; Float64 already uses price units."""
    for name in KLINE_PRICES:
        column=table[name]
        if column.type==pa.int64():
            prices=pc.divide(pc.cast(column,pa.float64(),safe=False),pa.scalar(1e9))
            table=table.set_column(table.schema.get_field_index(name),name,prices)
        elif column.type!=pa.float64():
            raise TypeError(f'{name}: expected Int64 e9 or Float64 price, got {column.type}')
    return table


def previous_close_seed(ric,start):
    """Use only this canonical RIC's own indexed pre-window trade."""
    seeds=json.loads((WORK/'previous_close.json').read_text())['seeds']
    seed=seeds.get(ric)
    if seed is None:return None
    assert seed['source_ric']==ric and seed['event_ns']<start,(ric,seed)
    return int(seed['price_e9'])


def public_kline(table):
    """User-facing minutes; diagnostic counters stay in local work/problem data."""
    if table is None:return None
    if table.schema.names==KLINE_CORE+['is_ffill','status']:
        result=table
    else:
        known=pc.and_(pc.fill_null(table['status_known'],False),pc.is_valid(table['trading_ns'])).to_numpy()
        duration=pc.fill_null(table['trading_ns'],0).to_numpy()
        halted=pc.fill_null(table['halted_ns'],0).to_numpy() if 'halted_ns' in table.schema.names else np.zeros(len(table),dtype=np.uint64)
        scheduled=pc.fill_null(table['scheduled_closed_ns'],0).to_numpy() if 'scheduled_closed_ns' in table.schema.names else np.zeros(len(table),dtype=np.uint64)
        labels=np.full(len(table),'UNKNOWN',dtype=object)
        labels[known & (duration==MINUTE)]='TRADING'
        labels[known & (scheduled==MINUTE)]='SCHEDULED_CLOSED'
        labels[known & (halted==MINUTE)]='HALTED'
        labels[known & (duration<MINUTE) & (halted<MINUTE) & (scheduled<MINUTE)]='PARTIAL'
        filled=pc.and_(pc.fill_null(table['is_empty'],False),pc.is_valid(table['close']))
        result=table.select(KLINE_CORE).append_column('is_ffill',filled).append_column('status',pa.array(labels,type=pa.string()))
    result=kline_float_prices(result)
    metadata=dict(table.schema.metadata or {})
    metadata[b'kline_schema_version']=KLINE_SCHEMA_VERSION.encode()
    metadata[b'price_scale']=b'1; open/high/low/close are nullable Float64 in actual price units, same units as baseline and IB'
    metadata[b'is_ffill']=b'true only when no trade occurred and OHLC was filled from the previous close; unpriced empty rows remain null and false; volume/trade_count are never filled'
    metadata[b'status']=b'TRADING=full minute tradable; SCHEDULED_CLOSED=full minute scheduled closure; HALTED=full minute temporary halt; PARTIAL=known phase change within minute; UNKNOWN=incomplete state or unclassified cause; detailed phases are in market_status.parquet'
    metadata[b'empty_minutes']=b'continuous UTC grid in valid covered instrument lifetime; all no-trade prices use own previous close, volume/trade_count=0; null if no own prior price; is_ffill indicates an actual fill'
    metadata[b'exception_policy']=(b'raw prints immutable; reliably linked historical cancels/corrections/inserts applied; all remaining prints retained including non-trading/unknown-state prints; unresolved links under meta/problems'
        if metadata.get(b'correction_policy_version') else b'raw prints aggregated without revision netting; non-trading/unknown-state prints retained; diagnostic details under meta')
    metadata[b'trade_time_basis']=b'exchange time, nearest UTC date within 12h of publication; missing exchange time falls back to publication'
    return result.replace_schema_metadata(metadata)


def publish_file(table, target, sampled, replace_schema=False,allow_trade_revisions=False):
    """Atomic data publication; footer checks everywhere, contents only on samples."""
    target.parent.mkdir(parents=True,exist_ok=True)
    part=dict(rows=len(table),ric=table['ric'][0].as_py(),utc_date=target.parent.name)
    replace=False
    readback_done=False
    if target.exists() and (replace_schema or allow_trade_revisions):
        rules_change=table.schema.metadata.get(b'fill_policy_version')==FILL_POLICY_VERSION.encode()
        revision_change=allow_trade_revisions and table.schema.metadata.get(b'correction_policy_version')==b'1'
        old=check_footer(target,{**part,'rows':None} if rules_change else part)
        old_metadata=dict(old.schema_arrow.metadata or {})
        replace=(old_metadata.get(b'kline_schema_version')!=KLINE_SCHEMA_VERSION.encode()
            or rules_change and old_metadata.get(b'fill_policy_version')!=FILL_POLICY_VERSION.encode()
            or revision_change and old_metadata.get(b'correction_plan_sha256')!=table.schema.metadata.get(b'correction_plan_sha256'))
        if replace:
            assert target.name=='kline.parquet' and set(KLINE_CORE).issubset(old.schema_arrow.names),target
            assert old_metadata.get(b'source')==table.schema.metadata.get(b'source'),target
            for name in KLINE_CORE:
                before=old.schema_arrow.field(name).type
                after=table.schema.field(name).type
                assert before==after or (name in KLINE_PRICES and before==pa.int64() and after==pa.float64()),target
            if sampled:
                if revision_change:
                    assert old.metadata.num_rows==len(table),target
                    stable=['ric','ts_utc_ns','status']
                    assert old.read(columns=stable).equals(table.select(stable),check_metadata=False),target
                elif rules_change and old_metadata.get(b'fill_policy_version')!=FILL_POLICY_VERSION.encode():
                    before=kline_float_prices(old.read())
                    common=table.filter(pc.is_in(table['ts_utc_ns'],value_set=before['ts_utc_ns'].combine_chunks()))
                    before_common=before.filter(pc.is_in(before['ts_utc_ns'],value_set=table['ts_utc_ns'].combine_chunks()))
                    unchanged=[name for name in KLINE_CORE if name not in KLINE_PRICES]
                    assert common.select(unchanged).equals(before_common.select(unchanged),check_metadata=False),target
                    assert before.filter(pc.greater(before['trade_count'],0)).select(KLINE_CORE).equals(
                        table.filter(pc.greater(table['trade_count'],0)).select(KLINE_CORE),check_metadata=False),target
                elif old.schema_arrow.names==KLINE_CORE+['is_ffill','status']:
                    assert kline_float_prices(old.read()).equals(table,check_metadata=False),target
                else:
                    assert kline_float_prices(old.read(columns=KLINE_CORE)).equals(table.select(KLINE_CORE),check_metadata=False),target
    if not target.exists() or replace:
        temp=target.with_name('.'+target.name+'.partial')
        pq.write_table(table,temp,compression='zstd',row_group_size=131072)
        with temp.open('rb') as stream:os.fsync(stream.fileno())
        if replace:
            footer=check_footer(temp,part)
            assert footer.schema_arrow.equals(table.schema,check_metadata=True),target
            if sampled:
                assert footer.read().equals(table,check_metadata=False),target
                readback_done=True
            temp.replace(target)
        else:
            try:os.link(temp,target)
            finally:temp.unlink(missing_ok=True)
    footer=check_footer(target,part)
    assert footer.schema_arrow.equals(table.schema,check_metadata=True),target
    if sampled and not readback_done:
        assert footer.read().equals(table,check_metadata=False),target
    target.with_name('.'+target.name+'.partial').unlink(missing_ok=True)


def publish_daily(table, contract, kind, pool, replace_schema=False,allow_trade_revisions=False):
    if table is None or not len(table):return 0
    times=table['ts_utc_ns'].to_numpy()
    days=times//DAY
    assert (times[1:]>=times[:-1]).all(),(contract['ric'],kind)
    edges=np.r_[0,np.flatnonzero(days[1:]!=days[:-1])+1,len(table)]
    selected={0,(len(edges)-2)//2,len(edges)-2}
    futures=[]
    for i,(lo,hi) in enumerate(zip(edges[:-1],edges[1:])):
        day=datetime.fromtimestamp(int(days[lo])*86400,tz=timezone.utc).strftime('%Y%m%d')
        target=DEST/contract['exchange']/'2026'/quote(contract['ric'],safe='_-.^')/day/(kind+'.parquet')
        futures.append(pool.submit(publish_file,table.slice(int(lo),int(hi-lo)),target,i in selected,replace_schema,allow_trade_revisions))
    for future in futures:future.result()
    return len(futures)


def compare_ib(c, ric, bars, ib, tick):
    # Planned/unknown padding is presentation, not evidence of missing IB bars.
    bars=bars.filter(pc.or_(pc.or_(pc.greater(bars['trading_ns'],0),pc.greater(bars['halted_ns'],0)),pc.greater(bars['trade_count'],0)))
    c.register('bars',bars);c.register('ib',ib)
    # Do not pick a favourable shift or rescale source IB volume.
    c.execute('''CREATE TEMP TABLE comparison AS SELECT coalesce(b.ts_utc_ns,i.ts_utc_ns)::UBIGINT ts_utc_ns,
        b.ts_utc_ns IS NOT NULL has_tas,i.ts_utc_ns IS NOT NULL has_ib,
        b.open/1e9 tas_open,b.high/1e9 tas_high,b.low/1e9 tas_low,b.close/1e9 tas_close,b.volume tas_volume,
        i.open ib_open,i.high ib_high,i.low ib_low,i.close ib_close,i.volume ib_volume,
        b.is_empty,b.trading_ns,b.status_known,b.outside_status_trade_count,b.unknown_status_trade_count,
        b.missing_exchange_time_trade_count,b.late_trade_count,i.time_quality_flags,i.ohlcv_quality_flags,
        abs(i.open-b.open/1e9) open_error,abs(i.high-b.high/1e9) high_error,
        abs(i.low-b.low/1e9) low_error,abs(i.close-b.close/1e9) close_error,
        i.volume-b.volume volume_difference
        FROM bars b FULL OUTER JOIN ib i USING(ts_utc_ns)''')
    stats=c.execute('''SELECT
        count_if(has_tas AND has_ib) matched_minutes,
        count_if(has_tas AND NOT has_ib) missing_ib_minutes,
        count_if(has_ib AND NOT has_tas) ib_only_minutes,
        count_if(has_tas AND has_ib AND tas_open IS NOT NULL AND tas_high IS NOT NULL AND tas_low IS NOT NULL AND tas_close IS NOT NULL
            AND isfinite(ib_open) AND isfinite(ib_high) AND isfinite(ib_low) AND isfinite(ib_close)) valid_price_minutes,
        count_if(has_tas AND has_ib AND greatest(open_error,high_error,low_error,close_error)<=1e-9
            AND tas_open IS NOT NULL AND ib_open IS NOT NULL) ohlc_equal_minutes,
        count_if(has_tas AND has_ib AND close_error<=1e-9) close_equal_minutes,
        count_if(has_tas AND has_ib AND close_error<=?) close_within_tick_minutes,
        count_if(has_tas AND has_ib AND abs(volume_difference)<=1e-9) volume_equal_minutes,
        sum(tas_volume) FILTER(WHERE has_tas AND has_ib) matched_tas_volume,
        sum(ib_volume) FILTER(WHERE has_tas AND has_ib) matched_ib_volume,
        max(greatest(open_error,high_error,low_error,close_error)) max_price_error,
        max(close_error) max_close_error,max(abs(volume_difference)) max_volume_error,
        sum(outside_status_trade_count) outside_status_trade_count,
        sum(unknown_status_trade_count) unknown_status_trade_count,
        sum(missing_exchange_time_trade_count) missing_exchange_time_trade_count,
        sum(late_trade_count) late_trade_count
        FROM comparison''',[tick if tick else 1e-9]).fetchdf().iloc[0].to_dict()
    for k,v in list(stats.items()):
        if pd.isna(v):stats[k]=None
        elif k.endswith('_minutes') or k.endswith('_count'):stats[k]=int(v)
        else:stats[k]=float(v)
    stats['ric']=ric;stats['tick_size']=tick
    n=stats['valid_price_minutes'];m=stats['matched_minutes']
    for name in ['ohlc_equal','close_equal','close_within_tick']:
        stats[name+'_pct']=100*stats[name+'_minutes']/n if n else None
    stats['volume_equal_pct']=100*stats['volume_equal_minutes']/m if m else None
    a=stats['matched_tas_volume'];b=stats['matched_ib_volume']
    stats['ib_volume_difference_pct']=100*(b-a)/a if a and b is not None else None
    differences=c.execute('''SELECT ?::VARCHAR ric,*,
        CASE WHEN NOT has_tas THEN 'IB有分钟，TAS无状态网格／成交'
             WHEN NOT has_ib THEN 'TAS有分钟，IB缺行'
             WHEN time_quality_flags<>0 OR ohlcv_quality_flags<>0 THEN 'IB源质量标记'
             WHEN tas_open IS NULL OR ib_open IS NULL THEN '价格缺失'
             ELSE 'OHLC或成交量不同' END issue
        FROM comparison WHERE NOT has_tas OR NOT has_ib OR
            tas_open IS NULL OR ib_open IS NULL OR
            greatest(open_error,high_error,low_error,close_error)>1e-9 OR abs(volume_difference)>1e-9
            OR time_quality_flags<>0 OR ohlcv_quality_flags<>0
        ORDER BY ts_utc_ns''',[ric]).fetch_arrow_table()
    probes=[]
    for shift in [-60,0,60]:
        row=c.execute('''SELECT count(*) AS matched_minutes,avg(abs(i.close-b.close/1e9)) close_mae,
            avg(CASE WHEN abs(i.close-b.close/1e9)<=1e-9 THEN 1.0 ELSE 0.0 END)*100 close_equal_pct
            FROM bars b JOIN ib i ON b.ts_utc_ns::HUGEINT=i.ts_utc_ns::HUGEINT+?''',[shift*1_000_000_000]).fetchone()
        probes.append(dict(shift_seconds=shift,matched_minutes=int(row[0]),close_mae=row[1],close_equal_pct=row[2]))
    stats['shift_probes']=probes
    return stats,differences


def process_contract(contract, status, publish_pool):
    ric=contract['ric'];folder=WORK/'contracts'/quote(ric,safe='_-.^');folder.mkdir(parents=True,exist_ok=True)
    checkpoint=RUN/'contracts'/f'{quote(ric,safe="_-.^")}.json'
    if checkpoint.exists():return json.loads(checkpoint.read_text())
    lives=prepare_lifetimes({ric:contract},REF,ROOT,ROOT/'artifacts/kline_continuous_v5_20261110/lifetimes.json')
    start,end=lifetime_window(contract,status,None,lives[ric])
    interval=state_intervals(status,start,end)
    pq.write_table(interval,folder/'intervals.parquet',compression='zstd')
    c=connection()
    bars=None;ib=None;stats=None;issues=None;gross=(0,0)
    try:
        from tas_trade_revisions import load_plan
        revisions=load_plan(ric) if contract['trade_files'] else None
        counts=sparse_minutes(c,contract['trade_files'],interval,start,end,revisions) if end>start else (0,0,0)
        if counts is not None and end>start:gross=c.execute('SELECT count(*),coalesce(sum(volume),0) FROM trade_clock_gross').fetchone()
        if counts is not None and (counts[0] or gross[0]):
            bars=minute_grid(c,ric,start,end,previous_close_seed(ric,start))
            from tas_trade_revisions import mark_corrected
            bars=mark_corrected(bars,revisions)
            assert int(pc.sum(bars['trade_count']).as_py() or 0)==counts[0]
            assert int(pc.sum(bars['volume']).as_py() or 0)==counts[1]
            pq.write_table(bars,folder/'kline.parquet',compression='zstd')
            c.register('bars',bars)
            issues=c.execute('''SELECT * FROM bars WHERE outside_status_trade_count>0 OR unknown_status_trade_count>0
                OR missing_exchange_time_trade_count>0 OR NOT status_known''').fetch_arrow_table()
            if len(issues):pq.write_table(issues,folder/'state_issues.parquet',compression='zstd')
        if contract['ib_file']:
            ib=pq.read_table(contract['ib_file'],filters=[('ts_utc_ns','>=',contract['start_ns']),('ts_utc_ns','<',contract['end_ns']//MINUTE*MINUTE)]).sort_by([('ts_utc_ns','ascending')])
            assert len(ib['ts_utc_ns'].unique())==len(ib),f'duplicate IB minute: {ric}'
            pq.write_table(ib,folder/'kline_ib.parquet',compression='zstd')
            if bars is not None and len(ib):
                stats,differences=compare_ib(c,ric,bars,ib,contract['tick_size'])
                if len(differences):pq.write_table(differences,folder/'differences.parquet',compression='zstd')
    finally:
        # Release large spillable trade tables before waiting on NAS writes,
        # including when an individual contract fails.
        c.close()
    # Publish status on its reception UTC day, independently of K-line coverage.
    files=publish_daily(status,contract,'market_status',publish_pool)
    files+=publish_daily(public_kline(bars),contract,'kline',publish_pool,allow_trade_revisions=True)
    files+=publish_daily(ib,contract,'kline_ib',publish_pool)
    result=dict(ric=ric,product=contract['product'],exchange=contract['exchange'],files=files,
        status_rows=len(status),kline_rows=len(bars) if bars is not None else 0,
        ib_rows=len(ib) if ib is not None else 0,source_trade_rows=int(gross[0]),source_volume=int(gross[1]),
        effective_trade_rows=counts[0] if counts else 0,effective_volume=counts[1] if counts else 0,
        state_issue_minutes=len(issues) if issues is not None else 0,
        comparison=stats,start_ns=start,end_ns=end)
    save(checkpoint,result)
    return result


def checked_contract(contract,status,publish_pool):
    issue=DEST/'meta/problems/market_status_kline'/f'{quote(contract["ric"],safe="_-.^")}.json'
    try:
        result=process_contract(contract,status,publish_pool)
        issue.unlink(missing_ok=True)
        return result
    except Exception as error:
        import traceback
        save(issue,dict(ric=contract['ric'],problem=str(error),traceback=traceback.format_exc()))
        raise


SUMMARY_NAMES={
    'ric':'RIC','product':'品种','exchange':'交易所','matched_minutes':'匹配分钟数',
    'missing_ib_minutes':'IB缺行分钟数','ib_only_minutes':'仅IB分钟数',
    'missing_ib_trade_minutes':'IB缺行_有TAS成交分钟数','missing_ib_empty_minutes':'IB缺行_TAS补空分钟数',
    'missing_ib_volume':'IB缺行分钟的TAS成交量','ib_only_nonzero_minutes':'仅IB且成交量非零分钟数',
    'valid_price_minutes':'双方有效价格分钟数','ohlc_equal_pct':'OHLC全一致比例_pct',
    'close_equal_pct':'收盘价一致比例_pct','close_within_tick_pct':'收盘价差不超过1跳比例_pct',
    'volume_equal_pct':'成交量一致比例_pct','matched_tas_volume':'匹配分钟TAS成交量',
    'matched_ib_volume':'匹配分钟IB成交量','ib_volume_difference_pct':'IB相对TAS成交量差_pct',
    'max_price_error':'最大OHLC价差','max_close_error':'最大收盘价差','max_volume_error':'最大分钟成交量差',
    'tick_size':'最小价位','outside_status_trade_count':'闭市状态成交笔数',
    'unknown_status_trade_count':'未知状态成交笔数','missing_exchange_time_trade_count':'缺交易所时间成交笔数',
    'late_trade_count':'跨分钟迟报成交笔数','start_utc':'比较开始UTC','end_utc_exclusive':'比较结束UTC_不含',
}


def finalize(contracts,destination=None,require_all_states=True):
    destination=destination or DEST
    results=[]
    for ric in contracts:
        p=RUN/'contracts'/f'{quote(ric,safe="_-.^")}.json'
        assert p.exists(),('unfinished contract',ric)
        results.append(json.loads(p.read_text()))
    if require_all_states:
        assert sum(r['status_rows'] for r in results)==pq.ParquetFile(WORK/'status.parquet').metadata.num_rows
    difference_files=[str(WORK/'contracts'/quote(r['ric'],safe='_-.^')/'differences.parquet') for r in results]
    difference_files=[p for p in difference_files if Path(p).exists()]
    missing_by_ric={}
    if difference_files:
        query=connection()
        missing=query.execute('''SELECT ric,
            count_if(has_tas AND NOT has_ib AND NOT is_empty)::UBIGINT missing_ib_trade_minutes,
            count_if(has_tas AND NOT has_ib AND is_empty)::UBIGINT missing_ib_empty_minutes,
            coalesce(sum(tas_volume) FILTER(WHERE has_tas AND NOT has_ib),0)::DOUBLE missing_ib_volume,
            count_if(has_ib AND NOT has_tas AND ib_volume>0)::UBIGINT ib_only_nonzero_minutes
            FROM read_parquet(?,union_by_name=true) GROUP BY ric''',[difference_files]).fetch_arrow_table()
        missing_by_ric={r['ric']:r for r in missing.to_pylist()}
        query.close()
    summaries=[];gaps=[]
    for result in results:
        contract=contracts[result['ric']]
        if result['comparison']:
            row=dict(result['comparison'],product=result['product'],exchange=result['exchange'],
                start_utc=pd.Timestamp(result['start_ns'],tz='UTC').isoformat(),
                end_utc_exclusive=pd.Timestamp(result['end_ns'],tz='UTC').isoformat())
            extra=missing_by_ric.get(result['ric'],{})
            for field in ['missing_ib_trade_minutes','missing_ib_empty_minutes','missing_ib_volume','ib_only_nonzero_minutes']:
                row[field]=extra.get(field) or 0
            assert row['missing_ib_trade_minutes']+row['missing_ib_empty_minutes']==row['missing_ib_minutes']
            assert (row['matched_tas_volume'] or 0)+row['missing_ib_volume']==result.get('effective_volume',result['source_volume'])
            summaries.append(row)
        if not result['source_trade_rows'] or not result['ib_rows']:
            missing=[]
            if not result['source_trade_rows']:missing.append('共同覆盖窗口无TAS成交快照，只有状态数据')
            if not result['ib_rows']:missing.append('现有IB快照在共同窗口无已核定同规格分钟')
            gaps.append(dict(ric=contract['ric'],product=contract['product'],problem='；'.join(missing)))
    plan=json.loads((IB/'_audit/contract_plan.json').read_text())
    for job in plan['jobs']:
        if job['ric'] not in contracts and str(job['ib_last_utc'])>='2026-01-01':
            gaps.append(dict(ric=job['ric'],product=job['tas_product'],
                problem='当前Databento状态没有该品种／合约覆盖；未用静态日历代替'))
    if gaps:
        write_csv(destination/'meta/problems/market_status_coverage.csv',gaps,['ric','product','problem'])
    path_conflicts=[]
    for ric,contract in contracts.items():
        if contract['exchange']!='review' and any('/lseg/review/' in p for p in contract['trade_files']):
            path_conflicts.append(dict(ric=ric,new_exchange=contract['exchange'],
                problem='新Databento定义已提供MIC；新状态／K线在已核定交易所目录，既有原始行情仍在review目录，未迁移原文件'))
    if path_conflicts:write_csv(destination/'meta/problems/market_status_exchange_paths.csv',path_conflicts,['ric','new_exchange','problem'])
    out=destination/'meta/comparison_ib/2026';out.mkdir(parents=True,exist_ok=True)
    rows=[{label:s.get(key) for key,label in SUMMARY_NAMES.items()} for s in summaries]
    write_csv(out/'合约比较.csv',rows,list(SUMMARY_NAMES.values()))
    product_rows=[]
    for product in sorted({s['product'] for s in summaries}):
        group=[s for s in summaries if s['product']==product]
        sums={k:sum(s.get(k) or 0 for s in group) for k in ['matched_minutes','missing_ib_minutes','ib_only_minutes',
            'valid_price_minutes','ohlc_equal_minutes','close_equal_minutes','close_within_tick_minutes','volume_equal_minutes',
            'matched_tas_volume','matched_ib_volume','outside_status_trade_count','unknown_status_trade_count',
            'missing_exchange_time_trade_count','late_trade_count','missing_ib_trade_minutes',
            'missing_ib_empty_minutes','missing_ib_volume','ib_only_nonzero_minutes']}
        sums.update(product=product,exchange=group[0]['exchange'],ric=f'{len(group)}个RIC',tick_size=group[0]['tick_size'])
        for name in ['ohlc_equal','close_equal','close_within_tick']:
            sums[name+'_pct']=100*sums[name+'_minutes']/sums['valid_price_minutes'] if sums['valid_price_minutes'] else None
        sums['volume_equal_pct']=100*sums['volume_equal_minutes']/sums['matched_minutes'] if sums['matched_minutes'] else None
        a=sums['matched_tas_volume'];b=sums['matched_ib_volume']
        sums['ib_volume_difference_pct']=100*(b-a)/a if a else None
        for k in ['max_price_error','max_close_error','max_volume_error']:
            values=[s[k] for s in group if s[k] is not None]
            sums[k]=max(values) if values else None
        product_rows.append({label:sums.get(key) for key,label in SUMMARY_NAMES.items()})
    write_csv(out/'品种比较.csv',product_rows,list(SUMMARY_NAMES.values()))
    # These are requested comparison findings, not successful-validation receipts.
    c=connection(4)
    for name,target in [('differences','分钟差异.parquet'),('state_issues','状态与成交差异.parquet')]:
        files=[str(WORK/'contracts'/quote(ric,safe='_-.^')/(name+'.parquet')) for ric in contracts]
        files=[p for p in files if Path(p).exists()]
        if files:
            temp=out/('.'+target+'.partial')
            # DuckDB COPY destinations are SQL literals; bind the file list in
            # a relation first and let its writer handle the destination path.
            c.read_parquet(files,union_by_name=True).order('ric,ts_utc_ns').write_parquet(str(temp),compression='zstd')
            temp.replace(out/target)
    probes=[dict(ric=s['ric'],**p) for s in summaries for p in s['shift_probes']]
    write_csv(out/'分钟标签偏移诊断.csv',probes,['ric','shift_seconds','matched_minutes','close_mae','close_equal_pct'])
    sys.path.insert(0,str(ROOT/'artifacts/python_deps'))
    from openpyxl import Workbook
    from openpyxl.styles import Font
    wb=Workbook();wb.remove(wb.active)
    gap_display=[{'RIC':r['ric'],'品种':r['product'],'原因':r['problem']} for r in gaps]
    for title,data in [('品种比较',product_rows),('合约比较',rows),('覆盖缺口',gap_display)]:
        sheet=wb.create_sheet(title)
        if not data:continue
        columns=list(data[0]);sheet.append(columns)
        for row in data:sheet.append([row.get(k) for k in columns])
        sheet.freeze_panes='A2';sheet.auto_filter.ref=sheet.dimensions
        for cell in sheet[1]:cell.font=Font(bold=True)
        for col in sheet.columns:sheet.column_dimensions[col[0].column_letter].width=min(55,max(16,len(str(col[0].value))*2))
    notes=wb.create_sheet('比较口径')
    for text in [
        '2026年；共同状态覆盖窗口截至2026-09-03凌晨；各RIC有无行情不同。',
        'TAS按交易所成交时间归桶，缺时间回退发布时间并标记；不复权，不删迟报、不滤异常价。',
        'TAS在合约有效覆盖期内连续输出UTC分钟；状态为TRADING/SCHEDULED_CLOSED/HALTED/PARTIAL/UNKNOWN。',
        '计划休市与UNKNOWN的纯补行不参与IB缺行统计；所有真实TAS成交保留。',
        '复市沿用本RIC最后已知成交收盘，跨休市不重置；窗口起点用本RIC只读历史成交预热，无历史价才留null。',
        '状态显示闭市或未知时的实际成交仍保留，并在状态与成交差异中列出。',
        'IB保留全部原始行、字段、DOUBLE价量和标签，不补行／补价／加状态／按TAS有效期删行；主结果不平移。',
        'IB缺行区分TAS有实际成交与TAS补空分钟；补空分钟缺行不直接等于漏成交。',
        'OHLC一致容差1e-9；成交量比较不换算；总量差仅针对双方匹配分钟。',
        '观察到差异不等于认定IB或TAS错误；成交资格、迟报、更正及来源过滤需结合源记录核查。',
    ]:notes.append([text])
    notes.column_dimensions['A'].width=110
    differences=out/'分钟差异.parquet'
    if differences.exists():
        largest=c.execute('''SELECT ric,ts_utc_ns,tas_open,ib_open,tas_high,ib_high,tas_low,ib_low,
            tas_close,ib_close,tas_volume,ib_volume,close_error,volume_difference,issue
            FROM read_parquet(?) WHERE has_tas AND has_ib
            ORDER BY greatest(open_error,high_error,low_error,close_error) DESC NULLS LAST LIMIT 200''',[str(differences)]).fetchdf()
        largest['UTC分钟']=pd.to_datetime(largest.pop('ts_utc_ns'),unit='ns',utc=True).astype(str)
        largest=largest.rename(columns={'ric':'RIC','tas_open':'TAS开盘','ib_open':'IB开盘',
            'tas_high':'TAS最高','ib_high':'IB最高','tas_low':'TAS最低','ib_low':'IB最低',
            'tas_close':'TAS收盘','ib_close':'IB收盘','tas_volume':'TAS成交量','ib_volume':'IB成交量',
            'close_error':'收盘绝对价差','volume_difference':'IB减TAS成交量差','issue':'问题'})
        sheet=wb.create_sheet('最大价差分钟');sheet.append(list(largest.columns))
        for row in largest.itertuples(index=False,name=None):
            sheet.append([None if pd.isna(v) else v for v in row])
        sheet.freeze_panes='A2';sheet.auto_filter.ref=sheet.dimensions
        for column in sheet.columns:sheet.column_dimensions[column[0].column_letter].width=30 if column[0].value=='UTC分钟' else 18
    temp=out/'.IB与LSEG分钟线比较_2026.xlsx.partial';wb.save(temp);temp.replace(out/'IB与LSEG分钟线比较_2026.xlsx')
    c.close()
    (destination/'meta/problems/market_status_kline_failure.json').unlink(missing_ok=True)
    print(json.dumps(dict(stage='comparison',contracts=len(summaries),products=len(product_rows),
        matched_minutes=sum(s['matched_minutes'] for s in summaries),report=str(out/'IB与LSEG分钟线比较_2026.xlsx')),ensure_ascii=False),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--ric')
    parser.add_argument('--workers',type=int,default=4)
    args=parser.parse_args()
    RUN.mkdir(parents=True,exist_ok=True);WORK.mkdir(parents=True,exist_ok=True)
    with (RUN/'runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        publish_source_quality()
        status,contracts=normalized_status()
        prepare_lifetimes(contracts,REF,ROOT,ROOT/'artifacts/kline_continuous_v5_20261110/lifetimes.json')
        publish_unmapped_status(status)
        print(json.dumps(dict(stage='prepared',status_rows=len(status),rics=len(contracts),
            trade_rics=sum(bool(c['trade_files']) for c in contracts.values()),ib_rics=sum(bool(c['ib_file']) for c in contracts.values())),ensure_ascii=False),flush=True)
        if args.prepare_only:return
        selected=[v for k,v in contracts.items() if not args.ric or k==args.ric]
        priority={'ESM26','NQM26','YMM26','CLN26','GCM26'}
        selected.sort(key=lambda v:(v['ric'] not in priority,not bool(v['ib_file']),not bool(v['trade_files']),v['product'],v['ric']))
        assert selected,'RIC not covered by verified Databento symbology'
        groups=defaultdict(list)
        for i,ric in enumerate(status['ric'].to_pylist()):groups[ric].append(i)
        with ThreadPoolExecutor(max_workers=16) as publisher,ThreadPoolExecutor(max_workers=args.workers) as workers:
            tasks=[workers.submit(checked_contract,v,status.take(pa.array(groups[v['ric']],type=pa.int64())),publisher) for v in selected]
            for future in tasks:
                result=future.result()
                print(json.dumps(dict(stage='contract',ric=result['ric'],status_rows=result['status_rows'],kline_rows=result['kline_rows'],ib_rows=result['ib_rows'])),flush=True)
        if not args.ric:finalize(contracts)


if __name__=='__main__':
    try:main()
    except Exception as error:
        import traceback
        save(DEST/'meta/problems/market_status_kline_failure.json',dict(problem=str(error),traceback=traceback.format_exc()))
        raise
