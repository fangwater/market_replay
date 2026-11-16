#!/usr/bin/env python3
"""Resolve actual IB futures to actual TAS RICs using expiry/reference evidence."""
import calendar
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'artifacts/python_deps'))
import pymysql
from match_ib_all_futures_tas import CURRENT, SUPPLEMENT, HRD

MONTHS = 'FGHJKMNQUVXZ'
# symbol -> venue, currency, full-size monetary point multiplier, IB local prefix
SPECS = {
    'BRR':('CME','USD','5','BTC'), 'CC':('NYBOT','USD','10','CC'),
    'CL':('NYMEX','USD','1000','CL'), 'COIL':('IPE','USD','1000','COIL'),
    'CT':('NYBOT','USD','50000','CT'), 'DAX':('EUREX','EUR','25','FDAX'),
    'DX':('NYBOT','USD','1000','DX'), 'ECF':('ENDEX','EUR','1000','ECF'),
    'ES':('CME','USD','50','ES'), 'GBL':('EUREX','EUR','1000','FGBL'),
    'GBM':('EUREX','EUR','1000','FGBM'), 'GBS':('EUREX','EUR','1000','FGBS'),
    'GC':('COMEX','USD','100','GC'), 'GF':('CME','USD','50000','GF'),
    'GOIL':('IPE','USD','100','GOIL'), 'HE':('CME','USD','40000','HE'),
    'HG':('COMEX','USD','25000','HG'), 'HO':('NYMEX','USD','42000','HO'),
    'JGB':('OSE.JPN','JPY','1000000',''), 'KC':('NYBOT','USD','37500','KC'),
    'LE':('CME','USD','40000','LE'), 'NG':('NYMEX','USD','10000','NG'),
    'NKD':('CME','USD','5','NKD'), 'NQ':('CME','USD','20','NQ'),
    'PA':('NYMEX','USD','100','PA'), 'PL':('NYMEX','USD','50','PL'),
    'RB':('NYMEX','USD','42000','RB'), 'RS':('NYBOT','CAD','20','RS'),
    'SB':('NYBOT','USD','112000','SB'), 'SCI':('SGX','USD','100','FEF'),
    'SI':('COMEX','USD','5000','SI'), 'TSR20':('SGX','USD','50','TF'),
    'VIX':('CFE','USD','1000','VX'), 'W':('ICEEUSOFT','USD','50','W'),
    'XINA50':('SGX','USD','1','CN'), 'YM':('CBOT','USD','5','YM'),
    'ZC':('CBOT','USD','5000','ZC'), 'ZF':('CBOT','USD','1000','ZF'),
    'ZL':('CBOT','USD','60000','ZL'), 'ZM':('CBOT','USD','100','ZM'),
    'ZN':('CBOT','USD','1000','ZN'), 'ZS':('CBOT','USD','5000','ZS'),
    'ZT':('CBOT','USD','2000','ZT'), 'ZW':('CBOT','USD','5000','ZW'),
}

def delivery(c):
    venue,currency,multiplier,prefix=SPECS[c['symbol']]
    if (c['exchange']!=venue or c['currency']!=currency or
            Decimal(c['multiplier'])!=Decimal(multiplier)):
        raise ValueError('different_venue_currency_or_size')
    expiry=datetime.strptime(c['maturity_date'],'%Y%m%d')
    if c['symbol']=='JGB':
        if not re.fullmatch(r'16\d{7}', c['local_symbol'] or ''):
            raise ValueError('unrecognized_JGB_local_symbol')
        return CURRENT['JGB'], MONTHS[expiry.month-1], expiry.year
    if venue=='EUREX':
        match=re.fullmatch(re.escape(prefix)+r' (\d{8}) M',c['local_symbol'] or '')
        if not match or match[1]!=c['maturity_date']:
            words=re.fullmatch(re.escape(prefix)+r' (JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC) (\d{2})',c['local_symbol'] or '')
            if not words or words[1]!=expiry.strftime('%b').upper() or words[2]!=expiry.strftime('%y'):
                raise ValueError('unrecognized_Eurex_local_symbol')
        return CURRENT[c['symbol']], MONTHS[expiry.month-1], expiry.year
    pattern=re.escape(prefix)+(r'(\d{2})?' if c['symbol']=='VIX' else '')+r'(['+MONTHS+r'])(\d{1,2})'
    match=re.fullmatch(pattern,c['local_symbol'] or '')
    if not match:
        raise ValueError('unrecognized_local_symbol')
    month,year_token=match.groups()[-2:]
    years=[y for y in range(expiry.year-1,expiry.year+2) if str(y).endswith(year_token)]
    if len(years)!=1:
        raise ValueError('ambiguous_delivery_year')
    root={**CURRENT,**SUPPLEMENT}[c['symbol']]
    if c['symbol']=='VIX' and match[1]:root+=match[1]
    return root,month,years[0]

def connect():
    cfg=dict(line.split('=',1) for line in (ROOT/'.env.local').read_text().splitlines()
             if line and not line.startswith('#'))
    return pymysql.connect(host=cfg['IB_MYSQL_HOST'],port=int(cfg['IB_MYSQL_PORT']),
        user=cfg['IB_MYSQL_USER'],password=cfg['IB_MYSQL_PASSWORD'],database=cfg['IB_MYSQL_DATABASE'],
        connect_timeout=10,read_timeout=30,autocommit=True,cursorclass=pymysql.cursors.DictCursor)

def main():
    out=ROOT/'artifacts/ib_ric_export_20261103'
    contracts=json.loads((out/'ib_contracts.json').read_text())
    observed=defaultdict(set)
    for r in csv.DictReader((ROOT/'artifacts/ric_inventory_20261102/source_ric_by_period.csv').open()):
        observed[r['ric']].add(int(r['period_year']))
    source=Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data/future_supplement/normalised/futures_11_time_and_sales_ric_list_0_tas_2026-01-01_2026-05-30/merged-Report.csv.gz')
    with gzip.open(source,'rt',newline='') as f:
        for r in csv.DictReader(f):
            if int(r['Count'])>0:observed[r['#RIC']].add(2026)
    candidates=[]; excluded=[]; wanted=set()
    for c in contracts:
        if c['symbol'] not in SPECS:continue
        try:
            root,month,year=delivery(c)
        except (ValueError,TypeError) as e:
            excluded.append(dict(c,status=str(e)));continue
        stems=[f'{root}{month}{year%100:02d}',f'{root}{month}{year%10}']
        possible=[ric for ric,periods in observed.items()
                  if ric.split('^')[0] in stems and any(year-9<=p<=year+1 for p in periods)]
        if c['symbol']=='VIX' and root!='VX':
            # Weekly RIC month codes can refer to the following monthly cycle;
            # the actual expiry date, not an IB month-letter substitution, resolves it.
            possible=[ric for ric,periods in observed.items()
                      if re.fullmatch(re.escape(root)+r'['+MONTHS+r']\d{1,2}(?:\^.*)?',ric)
                      and any(year-9<=p<=year+1 for p in periods)]
        wanted.update(possible)
        # HRD may contain listed contracts without source events yet.
        wanted.update(stems)
        candidates.append(dict(c,tas_product=root,delivery_year=year,delivery_month=MONTHS.index(month)+1,possible_rics=sorted(set(possible+stems))))
    refs=defaultdict(list)
    fields=['RIC','Expiration Date','Market MIC','Currency Code','Lot Size','Lot Units','Ticker']
    for file in [HRD,ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']:
        with file.open(newline='') as f:
            for r in csv.DictReader(f):
                if r['RIC'] in wanted:
                    v={k:r[k] for k in fields}
                    if v not in refs[r['RIC']]:refs[r['RIC']].append(v)
    jobs=[]
    for c in candidates:
        expiry=datetime.strptime(c['maturity_date'],'%Y%m%d')
        aliases=[]; evidence={}
        for ric in c['possible_rics']:
            versions=[]
            for v in refs[ric]:
                try:d=datetime.strptime(v['Expiration Date'],'%m/%d/%Y')
                except ValueError:continue
                same_expiry=abs((d-expiry).days)<=7
                if c['symbol']=='TSR20':
                    # IB records the previous month's last trading day; HRD
                    # records a delivery-month date. Match the delivery month,
                    # together with the actual SGX SICOM identity and units.
                    previous_month=datetime(c['delivery_year'],c['delivery_month'],1)
                    from datetime import timedelta
                    previous_month-=timedelta(days=1)
                    same_expiry=(d.year==c['delivery_year'] and d.month==c['delivery_month']
                                 and expiry.year==previous_month.year and expiry.month==previous_month.month
                                 and v['Market MIC']=='XSIM' and v['Currency Code']=='USc'
                                 and v['Lot Size']=='5,000' and v['Lot Units']=='KG' and v['Ticker']=='TF')
                if same_expiry:versions.append(v)
            if versions:
                aliases.append(ric);evidence[ric]=versions
        if not aliases:
            excluded.append(dict(c,status='no_matching_expiry_in_actual_HRD'));continue
        def rank(ric):
            valid=[p for p in observed[ric] if c['delivery_year']-9<=p<=c['delivery_year']+1]
            stem=ric.split('^')[0]
            return (max(valid,default=0),len(re.search(r'\d+$',stem)[0]),'^' not in ric,ric)
        canonical=max(aliases,key=rank)
        jobs.append({k:v for k,v in dict(c,ric=canonical,tas_aliases=sorted(aliases),hrd_evidence=evidence).items() if k!='possible_rics'})
    by_ric=defaultdict(list)
    for j in jobs:by_ric[j['ric']].append(j['con_id'])
    assert all(len(v)==1 for v in by_ric.values()), {k:v for k,v in by_ric.items() if len(v)>1}
    conn=connect()
    try:
        with conn.cursor() as x:
            x.execute('SET SESSION MAX_EXECUTION_TIME=20000')
            x.execute("SET SESSION time_zone='+00:00'")
            x.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY')
            x.execute('SELECT MAX(`id`) AS cutoff_id FROM `minute_data`')
            cutoff=x.fetchone()['cutoff_id']
            for i,j in enumerate(jobs,1):
                for label,direction in [('first','ASC'),('last','DESC')]:
                    x.execute('SELECT `utc_time`,`timestamp`,`id` FROM `minute_data` FORCE INDEX(uk_conid_utctime) '
                              "WHERE `con_id`=%s AND `utc_time` >= '1970-01-01' AND `id`<=%s "
                              f'ORDER BY `utc_time` {direction} LIMIT 1',(j['con_id'],cutoff))
                    value=x.fetchone()
                    j[f'ib_{label}_utc']=str(value['utc_time']) if value else None
                if i%100==0:print(f'Probed {i}/{len(jobs)} contracts',flush=True)
    finally:conn.rollback();conn.close()
    active=[j for j in jobs if j['ib_first_utc'] is not None]
    empty=[dict(j,status='no_valid_IB_minutes') for j in jobs if j['ib_first_utc'] is None]
    for j in active:j['cutoff_id']=cutoff
    plan=dict(status='ready',created_at=datetime.now(timezone.utc).isoformat(),cutoff_id=cutoff,
              covered_products=44,contracts_resolved=len(jobs),jobs=active,excluded=excluded+empty,
              scope='All available IB futures history for the 44 covered products, with actual HRD expiry evidence; no TAS export.',
              bar_timestamp_semantics='Original IB utc_time labels retained; this task compares first/last labels, not minute open/end labeling convention.')
    (out/'plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(dict(resolved=len(jobs),with_minutes=len(active),empty=len(empty),excluded_reasons=dict(Counter(x['status'] for x in excluded)),products=sorted({j['symbol'] for j in active})),ensure_ascii=False),flush=True)

if __name__=='__main__':main()
