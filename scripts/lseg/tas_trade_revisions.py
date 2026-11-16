"""Apply audited trade-ledger patches to a DuckDB source-clock relation."""
import json
import hashlib
from pathlib import Path

import pyarrow as pa

VERSION='1'
CATALOG=Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/tas_trade_corrections_20261110/compiled')
PATCH_SCHEMA=pa.schema([('filename',pa.string()),('file_row_number',pa.int64()),('action',pa.string()),
    ('price',pa.int64()),('volume',pa.uint32()),('original_price',pa.int64()),('original_volume',pa.uint32())])
INSERT_SCHEMA=pa.schema([('published_ns',pa.int64()),('exch_hms_ns',pa.uint64()),('price',pa.int64()),
    ('volume',pa.uint32()),('filename',pa.string()),('file_row_number',pa.int64()),('event_ns',pa.int64())])


def load_plan(ric):
    path=CATALOG/(ric+'.json')
    if not path.exists():raise RuntimeError(f'No audited correction plan for {ric}; run compile_trade_revisions.py first')
    result=json.loads(path.read_text());assert result['version']==VERSION
    return result


def mark_corrected(table,plan):
    metadata=dict(table.schema.metadata or {})
    digest=hashlib.sha256(json.dumps(plan,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    metadata.update({b'correction_policy_version':VERSION.encode(),b'correction_plan_sha256':digest.encode(),
        b'correction_pending_count':str(len(plan.get('unresolved',[]))).encode(),
        b'correction_pending_scope':b'pending at plan compilation; current unresolved issues are in meta/problems/tas_trade_revisions_unresolved.csv; plan witnesses retained locally by hash',
        b'trade_revisions':b'historical as-of-source netting of reliably linked cancellations/corrections/inserts; raw ticks immutable; unresolved links retained under meta/problems'})
    return table.replace_schema_metadata(metadata)


def scope(plan,start,end,event_minutes=None):
    def included(ns):
        return start<=ns<end and (event_minutes is None or ns//60_000_000_000*60_000_000_000 in event_minutes)
    patches=[row for row in plan.get('patches',[]) if included(row['event_ns'])]
    insertions=[row for row in plan.get('insertions',[]) if included(row['event_ns'])]
    return patches,insertions


def delta(plan,start,end):
    patches,insertions=scope(plan,start,end)
    count=len(insertions)-sum(row['action']=='cancel' for row in patches)
    volume=sum(row['volume'] for row in insertions)
    volume+=sum(-row['original_volume'] if row['action']=='cancel' else row['volume']-row['original_volume'] for row in patches)
    return count,volume


def apply(c,plan,start,end,event_minutes=None):
    patches,insertions=scope(plan,start,end,event_minutes)
    c.register('trade_revision_patches',pa.Table.from_pylist(patches,schema=PATCH_SCHEMA))
    inserted=[dict(row,filename='correction:'+row['event_id'],file_row_number=0) for row in insertions]
    c.register('trade_revision_insertions',pa.Table.from_pylist(inserted,schema=INSERT_SCHEMA))
    matched=c.execute('''SELECT count(*),count_if(g.price<>p.original_price OR g.volume<>p.original_volume)
        FROM trade_clock_gross g JOIN trade_revision_patches p USING(filename,file_row_number)''').fetchone()
    assert matched[0]==len(patches) and not matched[1],('source revision mismatch',matched,len(patches))
    c.execute('''CREATE TEMP TABLE trade_clock AS
        SELECT g.published_ns,g.exch_hms_ns,
            CASE WHEN p.action='replace' THEN p.price ELSE g.price END::BIGINT price,
            CASE WHEN p.action='replace' THEN p.volume ELSE g.volume END::UINTEGER volume,
            g.filename,g.file_row_number,g.event_ns
        FROM trade_clock_gross g LEFT JOIN trade_revision_patches p USING(filename,file_row_number)
        WHERE p.action IS NULL OR p.action='replace'
        UNION ALL SELECT * FROM trade_revision_insertions''')
    gross=c.execute('SELECT count(*),coalesce(sum(volume),0) FROM trade_clock_gross').fetchone()
    net=c.execute('SELECT count(*),coalesce(sum(volume),0) FROM trade_clock').fetchone()
    expected_count=len(insertions)-sum(row['action']=='cancel' for row in patches)
    expected_volume=sum(row['volume'] for row in insertions)+sum(
        -row['original_volume'] if row['action']=='cancel' else row['volume']-row['original_volume'] for row in patches)
    assert int(net[0])-int(gross[0])==expected_count
    assert int(net[1])-int(gross[1])==expected_volume
    return dict(gross_count=int(gross[0]),gross_volume=int(gross[1]),net_count=int(net[0]),net_volume=int(net[1]))
