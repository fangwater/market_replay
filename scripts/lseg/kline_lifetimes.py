"""Referenced activation/expiration bounds for continuous TAS minute grids."""
import csv
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.compute as pc


def prepare_lifetimes(contracts,reference_root,project_root,cache):
    target=cache
    if target.exists():
        prior=json.loads(target.read_text())
        if set(contracts).issubset(prior):return prior
    definitions=pq.ParquetFile(reference_root/'parsed/core_six_definition_2020_present/definition_outrights.parquet').read(
        columns=['raw_symbol','maturity_year','maturity_month','expiration','activation','ts_recv']).to_pandas()
    definitions=definitions.sort_values('ts_recv').drop_duplicates(['raw_symbol','maturity_year','maturity_month'],keep='last').set_index(['raw_symbol','maturity_year','maturity_month'])
    dates={ric:set() for ric in contracts}
    for path in [Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv'),
        project_root/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']:
        with path.open(encoding='utf-8-sig',newline='') as stream:
            for record in csv.DictReader(stream):
                ric=record['RIC']
                if ric not in contracts or not record.get('Expiration Date'):continue
                date=pd.to_datetime(record['Expiration Date'],format='%m/%d/%Y',utc=True,errors='coerce')
                if pd.notna(date) and abs(date.year-contracts[ric]['delivery_year'])<=1:
                    dates[ric].add(int((date+pd.Timedelta(days=1)).value))
    result={}
    for ric,contract in contracts.items():
        key=(contract['databento_product']+'FGHJKMNQUVXZ'[contract['delivery_month']-1]+str(contract['delivery_year']%10),contract['delivery_year'],contract['delivery_month'])
        activation=expiration=None;basis=None
        if key in definitions.index:
            row=definitions.loc[key]
            activation=int(row.activation.value) if pd.notna(row.activation) else None
            expiration=int(row.expiration.value) if pd.notna(row.expiration) else None
            basis='definition'
        if expiration is None and len(dates[ric])==1:expiration=next(iter(dates[ric]));basis='HRD_date'
        result[ric]=dict(activation_ns=activation,expiration_ns=expiration,source=basis)
    target.parent.mkdir(parents=True,exist_ok=True)
    temp=target.with_suffix('.json.partial')
    temp.write_text(json.dumps(result,indent=2)+'\n')
    temp.replace(target)
    return result


def window(contract,status,old,lifetime,minute=60000000000):
    activation=lifetime['activation_ns'];expiration=lifetime['expiration_ns']
    rows=status.to_pandas()
    effective=np.where(rows.is_midnight_snapshot,rows.ts_recv_ns,rows.ts_event_ns)
    if 'reason' in rows:
        expired=effective[(rows.reason.to_numpy()==5)&(effective<2**63)]
        if len(expired):expiration=min(int(expired.min()),expiration) if expiration is not None else int(expired.min())
    if activation is None:
        valid=effective[effective<2**63]
        activation=int(valid.min()) if len(valid) else contract['start_ns']
        if old is not None:activation=min(activation,int(old['ts_utc_ns'][0].as_py()))
    start=max(contract['start_ns'],int(activation)//minute*minute)
    end=contract['end_ns']//minute*minute
    if expiration is not None:end=min(end,(expiration+minute-1)//minute*minute)
    if old is not None:
        actual=old.filter(pc.greater(old['trade_count'],0))
        assert len(actual) and actual['ts_utc_ns'][0].as_py()>=start and actual['ts_utc_ns'][-1].as_py()<end,(contract['ric'],'actual trade outside referenced lifetime')
        assert end>start,(contract['ric'],start,end)
    return start,end

