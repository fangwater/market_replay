"""Identity-aware historical netting of TAS cancels, corrections and inserts.

Raw prints are immutable. CAN_COND price/size fields can be last-sale state,
so an explicit cancel never creates a replacement trade from those fields.
"""
from collections import defaultdict
from decimal import Decimal
import re

import pandas as pd

DAY=86_400_000_000_000
MISSING_CLOCK=2**64-1


def price(value):
    if value in (None,''):return None
    scaled=Decimal(str(value))*1_000_000_000
    if scaled!=scaled.to_integral_value():raise ValueError(('price precision',value))
    return int(scaled)


def quantity(value):
    if value in (None,''):return None
    parsed=Decimal(str(value))
    if parsed!=parsed.to_integral_value():raise ValueError(('fractional quantity',value))
    return int(parsed)


def clock(value):
    if value in (None,''):return None
    h,m,sec=str(value).split(':');seconds=Decimal(sec)
    return (int(h)*3600+int(m)*60)*1_000_000_000+int(seconds*1_000_000_000)


def event_time(published,hms):
    if hms is None or hms==MISSING_CLOCK:return int(published)
    provisional=int(published)//DAY*DAY+int(hms)
    if provisional>published+DAY//2:provisional-=DAY
    if provisional<published-DAY//2:provisional+=DAY
    return provisional


def source_record(record):
    f=record['fields']
    return dict(ric=f['#RIC'],published_ns=int(pd.Timestamp(f['Date-Time']).value),
        kind=f['Type'],price=price(f.get('Price')),volume=quantity(f.get('Volume')),
        original_price=price(f.get('Original Price')),original_volume=quantity(f.get('Original Volume')),
        uid=f.get('Unique Trade Identification'),original_uid=f.get('Original Unique Trade Identification'),
        seq=f.get('Seq. No.'),original_seq=f.get('Original Seq. No.'),
        trade_seq=f.get('Trade Sequence Number'),original_trade_seq=f.get('Original Trade Sequence Number'),
        hms=clock(f.get('Exch Time')),original_hms=clock(f.get('Original Exch Time')),
        date=f.get('Date'),original_date=f.get('Original Date'),qualifiers=f.get('Qualifiers',''),
        source=record.get('source'),line=record.get('line'))


def date_key(value):
    return pd.Timestamp(value).date().isoformat() if value else None


def combine_corrections(records):
    """Merge complementary projections, preserving all source witnesses."""
    groups=defaultdict(list)
    for row in records:
        if row['kind']!='Correction':continue
        key=tuple(row.get(k) for k in ('ric','published_ns','original_uid','original_seq','original_trade_seq',
            'original_price','original_volume','original_date','original_hms'))
        groups[key].append(row)
    result=[]
    for rows in groups.values():
        merged=dict(rows[0]);merged['witnesses']=[dict(source=r['source'],line=r['line']) for r in rows]
        conflicts=[]
        for name in ['price','volume','uid','seq','trade_seq','hms','date']:
            values={r.get(name) for r in rows if r.get(name) not in (None,'')}
            if len(values)>1:conflicts.append(name)
            merged[name]=next(iter(values)) if len(values)==1 else None
        merged['qualifiers']=';'.join(dict.fromkeys(t for r in rows for t in r['qualifiers'].split(';') if t))
        tags={fid:value.strip(' "') for value,fid in re.findall(r'([^;]*)\[([^\]]+)\]',merged['qualifiers'])}
        if conflicts:action='unresolved_conflicting_fragments'
        elif tags.get('CAN_COND')=='611' or tags.get('CAN_COND_N')=='2':action='cancel'
        elif tags.get('INSSALCOND')=='2':action='insert'
        elif (merged['original_price'] is not None or merged['original_volume'] is not None) and (merged['price'] is not None or merged['volume'] is not None):action='amend'
        else:action='unresolved_action'
        merged['action']=action;merged['conflicts']=conflicts
        result.append(merged)
    return sorted(result,key=lambda r:(r['published_ns'],r['ric'],str(r.get('seq') or ''),str(r.get('original_uid') or '')))


def attach_identities(candidates,source_trades):
    raw=defaultdict(list);physical=defaultdict(list)
    for row in source_trades:
        if row['kind']!='Trade' or row['price'] is None or row['volume'] is None:continue
        raw[(row['ric'],row['published_ns'],row['price'],row['volume'],row['hms'])].append(row)
    for row in candidates:
        hms=row.get('exch_hms_ns');hms=None if hms==MISSING_CLOCK else hms
        key=(row['ric'],row['published_ns'],row['price'],row['volume'],hms)
        physical[key].append(row)
    states=[]
    for key,rows in physical.items():
        witnesses=sorted(raw.get(key,[]),key=lambda r:(r.get('source') or '',r.get('line') or 0))
        rows=sorted(rows,key=lambda r:(r['filename'],r['file_row_number']))
        for index,row in enumerate(rows):
            attached=witnesses[index] if len(witnesses)==len(rows) else None
            state=dict(row,active=True,uid=attached.get('uid') if attached else None,
                seq=attached.get('seq') if attached else None,trade_seq=attached.get('trade_seq') if attached else None,
                qualifiers=attached.get('qualifiers','') if attached else '',
                date=attached.get('date') if attached else None,source_identity_recovered=attached is not None,revisions=[],history=[])
            state['original']=dict(row);states.append(state)
    return states


def resolve(candidates,source_rows):
    states=attach_identities(candidates,source_rows)
    corrections=combine_corrections(source_rows)
    applied=[];unresolved=[]
    for index,correction in enumerate(corrections):
        event_id=f"{correction['ric']}:{correction['published_ns']}:{index}"
        c=dict(correction,event_id=event_id)
        if c['action'].startswith('unresolved'):
            unresolved.append(dict(c,reason=c['action']));continue
        prior=[s for s in states if s['ric']==c['ric'] and s['published_ns']<=c['published_ns']]
        if c['action']=='insert':
            if c['price'] is None or c['volume'] is None:
                unresolved.append(dict(c,reason='insert_missing_price_or_volume'));continue
            already=[s for s in prior if s['published_ns']==c['published_ns'] and s['price']==c['price'] and s['volume']==c['volume']]
            if not already and c.get('uid'):
                already=[s for s in prior if s.get('uid')==c['uid'] and s['price']==c['price'] and s['volume']==c['volume']]
            if already:
                applied.append(dict(c,result='insert_already_in_trade_stream',matched=len(already)));continue
            at=event_time(c['published_ns'],c.get('hms'))
            inserted=dict(ric=c['ric'],event_id=event_id,published_ns=c['published_ns'],event_ns=at,
                filename='correction:'+event_id,file_row_number=0,
                exch_hms_ns=c.get('hms') if c.get('hms') is not None else MISSING_CLOCK,price=c['price'],volume=c['volume'])
            states.append(dict(inserted,active=True,is_insert=True,source_identity_recovered=True,uid=c.get('uid'),seq=c.get('seq'),trade_seq=c.get('trade_seq'),
                date=c.get('date'),original=dict(inserted),history=[],revisions=[event_id]))
            applied.append(dict(c,result='insert',target_event_ns=at));continue
        evidence=None;matches=[]
        for source_key,target_key in [('original_uid','uid'),('original_trade_seq','trade_seq'),('original_seq','seq')]:
            if c.get(source_key):
                matches=[s for s in prior if s.get(target_key)==str(c[source_key])
                    and (source_key=='original_uid' or c.get('original_date') or c['published_ns']-s['published_ns']<=DAY)]
                if matches:evidence=source_key;break
        def compatible(s):
            return any(all(c.get(a) is None or version.get(b)==c[a]
                for a,b in [('original_price','price'),('original_volume','volume')])
                for version in [s,s['original'],*s['history']])
        if matches:matches=[s for s in matches if compatible(s)]
        else:
            # ID absence may use a complete, unique original fingerprint. A
            # known conflicting identifier must never be overridden by prices.
            if c['original_price'] is None or c['original_volume'] is None:
                unresolved.append(dict(c,reason='no_usable_identity_or_original_payload'));continue
            matches=[s for s in prior if compatible(s)]
            for source_key,target_key in [('original_uid','uid'),('original_trade_seq','trade_seq'),('original_seq','seq')]:
                if c.get(source_key):matches=[s for s in matches if s.get(target_key) in (None,'',str(c[source_key]))]
            if c.get('original_hms') is not None:matches=[s for s in matches if s['original'].get('exch_hms_ns')==c['original_hms']]
            evidence='unique_original_payload'
        # Explicit references cannot be contradicted by a weaker matched key.
        for source_key,target_key in [('original_uid','uid'),('original_trade_seq','trade_seq'),('original_seq','seq')]:
            if c.get(source_key):matches=[s for s in matches if s.get(target_key) in (None,'',str(c[source_key]))]
        if c.get('original_date'):matches=[s for s in matches if date_key(s.get('date'))==date_key(c['original_date'])]
        if c.get('original_hms') is not None:matches=[s for s in matches if s['original'].get('exch_hms_ns')==c['original_hms']]
        if not matches:
            unresolved.append(dict(c,reason='original_trade_not_found'));continue
        if any(not s['source_identity_recovered'] for s in matches):
            unresolved.append(dict(c,reason='original_source_identity_unrecovered'));continue
        # Multiple physical rows are safe only when a strong ID proves an
        # identical logical print was repeated. Otherwise preserve ambiguity.
        fingerprint={(s['event_ns'],s['price'],s['volume']) for s in matches}
        if len(matches)>1 and (evidence=='unique_original_payload' or len(fingerprint)!=1):
            unresolved.append(dict(c,reason='multiple_original_candidates',candidates=len(matches)));continue
        if evidence=='unique_original_payload' and c.get('original_hms') is None:
            # Clock inherited from the current last sale is not an original
            # clock. Require exact clock confirmation for the ID-less path.
            if c.get('hms') is None or matches[0]['original'].get('exch_hms_ns')!=c['hms']:
                unresolved.append(dict(c,reason='original_clock_not_confirmed'));continue
        if c['action']=='amend' and c.get('hms') is not None and any(s['original'].get('exch_hms_ns')!=c['hms'] for s in matches):
            unresolved.append(dict(c,reason='amendment_clock_change_requires_review'));continue
        for state in matches:
            if c['action']=='cancel':state['active']=False
            else:
                if not state['active']:
                    unresolved.append(dict(c,reason='amendment_of_inactive_trade'));break
                state['history'].append(dict(price=state['price'],volume=state['volume'],event_ns=state['event_ns']))
                if c['price'] is not None:state['price']=c['price']
                if c['volume'] is not None:state['volume']=c['volume']
            state['revisions'].append(event_id)
        else:
            applied.append(dict(c,result=c['action'],basis=evidence,
                targets=[dict(filename=s['filename'],file_row_number=s['file_row_number'],event_ns=s['event_ns']) for s in matches]));continue
    patches=[];insertions=[]
    for s in states:
        original=s['original']
        if s.get('is_insert'):
            if s['active']:insertions.append({k:s[k] for k in ['ric','event_id','published_ns','event_ns','exch_hms_ns','price','volume']})
            continue
        if not s['active'] or s['price']!=original['price'] or s['volume']!=original['volume']:
            patches.append(dict(ric=s['ric'],filename=s['filename'],file_row_number=s['file_row_number'],
                published_ns=original['published_ns'],event_ns=original['event_ns'],original_price=original['price'],original_volume=original['volume'],
                action='replace' if s['active'] else 'cancel',price=s['price'],volume=s['volume'],revisions=s['revisions']))
    return dict(patches=patches,insertions=insertions,applied=applied,unresolved=unresolved)
