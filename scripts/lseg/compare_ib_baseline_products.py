#!/usr/bin/env python3
"""Compare five more actual futures contracts, reusing the single-RIC entry point."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
DATA=Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data')
OUT=ROOT/'artifacts/ib_baseline_multi_comparison_20261103'
SPECS=[
    ('NQM26','纳斯达克100','CME','NQ',.25,'exchange','指数点'),
    ('CLN26','WTI原油','NYMEX','CL',.01,'exchange','美元/桶'),
    ('GCM26','黄金','COMEX','GC',.1,'exchange','美元/盎司'),
    ('HGN26','铜','COMEX','HG',.0005,'publication','美元/磅'),
    ('TYM26','10年期美债（IB ZN）','CBOT','TY',1/64,'publication','报价点'),
]
sys.path.insert(0,str(ROOT/'artifacts/python_deps'))
from openpyxl import Workbook,load_workbook
from openpyxl.styles import Font,PatternFill,Alignment
from openpyxl.utils import get_column_letter

def source_probe(spec):
    ric,name,venue,product,tick,clock,unit=spec
    folder=OUT/ric;s=json.loads((folder/'summary.json').read_text())
    diff=pd.read_parquet(folder/'minute_differences.parquet')
    target=diff.loc[diff.volume_ib_minus_base.abs().idxmax()]
    minute=int(target.ts)*10**9
    files=sorted((DATA/'tas_tick_parquet/2026/trade'/('ric='+ric)).glob('*.parquet'))
    source=ds.dataset([str(f) for f in files],format='parquet')
    # Nearest-day exchange recovery can differ from publication by up to 12h.
    left,right=(minute-43200*10**9,minute+43260*10**9) if clock=='exchange' else (minute,minute+60*10**9)
    raw=source.to_table(filter=(ds.field('ts_utc_ns')>=left)&(ds.field('ts_utc_ns')<right),columns=['ric','ts_utc_ns','exch_hms_ns','price','volume','aggressor']).to_pandas()
    pub=raw.ts_utc_ns.to_numpy(dtype=np.int64);exch=raw.exch_hms_ns.to_numpy(dtype=np.uint64)
    day=86400*10**9;event=(pub//day)*day+exch.astype(np.int64)
    missing=exch==np.iinfo(np.uint64).max;event[missing]=pub[missing]
    event=np.where(event>pub+day//2,event-day,event);event=np.where(pub>event+day//2,event+day,event)
    raw['exchange_event_ns']=event;raw['bucket_event_ns']=event if clock=='exchange' else pub
    raw=raw[(raw.bucket_event_ns>=minute)&(raw.bucket_event_ns<minute+60*10**9)].sort_values(['bucket_event_ns','ts_utc_ns'],kind='stable').copy()
    raw['price_points']=raw.price/1e9
    raw['published_utc']=pd.to_datetime(raw.ts_utc_ns,unit='ns',utc=True).astype(str)
    raw['exchange_utc']=pd.to_datetime(raw.exchange_event_ns,unit='ns',utc=True).astype(str)
    raw['delay_seconds']=(raw.ts_utc_ns.astype('int64')-raw.exchange_event_ns)/1e9
    source_agg=dict(open=float(raw.price_points.iloc[0]),high=float(raw.price_points.max()),low=float(raw.price_points.min()),close=float(raw.price_points.iloc[-1]),volume=int(raw.volume.sum())) if len(raw) else None
    reproduced=source_agg is not None and all(np.isclose(source_agg[k],target[k+'_base'],rtol=0,atol=1e-9) for k in source_agg)
    raw.to_parquet(folder/'largest_volume_gap_source_trades.parquet',index=False)
    trade_columns=['published_utc','exchange_utc','price_points','volume','aggressor','delay_seconds']
    large=raw.nlargest(10,'volume')[trade_columns].to_dict('records')
    delayed=raw[raw.delay_seconds>=60]
    probe=dict(ric=ric,clock=clock,minute_utc=str(target.utc),baseline={k:float(target[k+'_base']) for k in ['open','high','low','close','volume']},ib={k:float(target[k+'_ib']) for k in ['open','high','low','close','volume']},
               source_aggregate=source_agg,source_matches_baseline=bool(reproduced),source_records=len(raw),largest_source_trades=large,
               aggressor0_volume=int(raw.loc[raw.aggressor==0,'volume'].sum()),
               published_at_least_60s_later_records=delayed[trade_columns].to_dict('records'),
               published_at_least_60s_later_volume=int(delayed.volume.sum()),
               max_publication_lag_seconds=float(raw.delay_seconds.max()) if len(raw) else None,
               volume_ib_minus_base=float(target.volume_ib_minus_base),classification='observed_difference_cause_unverified',
               limitation='Volume difference alone does not establish IB error; trade conditions/IDs/corrections and provider eligibility remain unverified.')
    (folder/'source_probe.json').write_text(json.dumps(probe,ensure_ascii=False,indent=2)+'\n')
    return probe

def run_comparison(spec,refresh):
    ric,name,venue,product,tick,clock,unit=spec;folder=OUT/ric;folder.mkdir(exist_ok=True)
    if refresh or not (folder/'summary.json').exists():
        cmd=[sys.executable,str(SCRIPTS / 'compare_ib_baseline_one.py'),'--ric',ric,'--baseline-dir',str(DATA/'baseline_data_1min'/venue/product),'--ib-file',str(DATA/'ib_minute_parquet/covered_futures_20261103/data'/f'{ric}.parquet'),'--tick-size',str(tick),'--output',str(folder),'--skip-source-check']
        with (folder/'run.log').open('w') as log:subprocess.run(cmd,check=True,stdout=log,stderr=subprocess.STDOUT)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--refresh',action='store_true');a=p.parse_args()
    OUT.mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda s:run_comparison(s,a.refresh),SPECS))
    with ThreadPoolExecutor(max_workers=2) as pool:
        probes=list(pool.map(source_probe,SPECS))
    results=[];details=[]
    for spec,probe in zip(SPECS,probes):
        ric,name,venue,product,tick,clock,unit=spec;s=json.loads((OUT/ric/'summary.json').read_text())
        d=pd.read_parquet(OUT/ric/'minute_differences.parquet')
        # Coverage is relative to the published baseline grid, not every minute
        # in a continuous date span. Missing empty rows are not missing trades.
        missing=pd.DataFrame()
        if s['missing_ib_minutes']:
            parts=[]
            for f in sorted((DATA/'baseline_data_1min'/venue/product).glob('202605*.parquet')):
                t=pq.read_table(f,columns=['ric','ts','open','high','low','close','volume','count']).to_pandas();parts.append(t[t.ric==ric])
            baseline=pd.concat(parts,ignore_index=True)
            ib=pq.read_table(DATA/'ib_minute_parquet/covered_futures_20261103/data'/f'{ric}.parquet',columns=['ts_utc_ns']).to_pandas()
            missing=baseline[~baseline.ts.isin(ib.ts_utc_ns//10**9)].copy();missing['utc']=pd.to_datetime(missing.ts,unit='s',utc=True).astype(str)
            missing.to_csv(OUT/ric/'missing_ib_minutes.csv',index=False,encoding='utf-8-sig')
        price_empty=int(missing[['open','high','low','close']].isna().all(axis=1).sum()) if len(missing) else 0
        missing_positive=int((missing.volume>0).sum()) if len(missing) else 0
        rec=dict(ric=ric,name_cn=name,baseline_product=product,baseline_clock=clock,price_unit=unit,tick_size=tick,trade_days=s['trade_days'],matched_minutes=s['matched_minutes'],
                 missing_ib_grid_rows=s['missing_ib_minutes'],missing_ib_positive_volume_minutes=missing_positive,missing_ib_empty_ohlc_rows=price_empty,
                 close_equal_pct=s['metrics']['close']['equal_pct'],close_within_one_tick_pct=s['metrics']['close']['within_one_tick_pct'],close_mae=s['metrics']['close']['mean_absolute_error'],close_mae_ticks=s['metrics']['close']['mean_absolute_error']/tick,
                 ohlc_all_equal_pct=s['ohlc_all_equal_pct'],volume_equal_pct=s['metrics']['volume']['equal_pct'],volume_ib_minus_base_pct=s['volume_ib_minus_base_pct'],
                 baseline_volume_matched=s['baseline_volume'],ib_volume_matched=s['ib_volume'],missing_baseline_volume=float(missing.volume.sum()) if len(missing) else 0,
                 max_close_error=s['metrics']['close']['max_absolute_error'],max_ohlc_error=float(d.max_ohlc_abs_error.max()),
                 max_volume_gap=abs(probe['volume_ib_minus_base']),source_probe_reproduced=probe['source_matches_baseline'],
                 ib_time_flag_rows=s['ib_time_flag_rows'],ib_ohlcv_flag_rows=s['ib_ohlcv_flag_rows'])
        results.append(rec)
        assert min(s['time_shift_probes'],key=lambda p:p['close_mae_points'])['ib_shift_seconds']==0
        for x in ['open','high','low','close','volume']:
            details.append(dict(ric=ric,field=x,**s['metrics'][x]))
        assert len(d)==s['matched_minutes'] and not d.ts.duplicated().any()
        assert probe['source_matches_baseline'],f'{ric}: bounded TAS probe did not reproduce baseline'
    pd.DataFrame(results).to_csv(OUT/'product_summary.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(results).to_parquet(OUT/'product_summary.parquet',index=False)
    pd.DataFrame(details).to_csv(OUT/'field_metrics.csv',index=False,encoding='utf-8-sig')
    wb=Workbook();ws=wb.active;ws.title='五品种汇总'
    headers=['品种','RIC','匹配分钟','收盘完全一致%','收盘差≤1tick%','OHLC全相同%','量完全一致%','IB相对总量差%','收盘MAE原单位','收盘MAE ticks','baseline分桶时钟','IB缺网格行','缺有成交分钟','最大量差手数']
    ws.append(headers)
    for r in results:ws.append([r['name_cn'],r['ric'],r['matched_minutes'],r['close_equal_pct'],r['close_within_one_tick_pct'],r['ohlc_all_equal_pct'],r['volume_equal_pct'],r['volume_ib_minus_base_pct'],r['close_mae'],r['close_mae_ticks'],{'exchange':'交易所时间','publication':'源发布时间'}[r['baseline_clock']],r['missing_ib_grid_rows'],r['missing_ib_positive_volume_minutes'],r['max_volume_gap']])
    w=wb.create_sheet('各字段误差');w.append(['RIC','字段','比较分钟','相同%','MAE','P95','P99','最大差'])
    for d in details:w.append([d['ric'],d['field'],d['compared'],d['equal_pct'],d['mean_absolute_error'],d['p95_absolute_error'],d['p99_absolute_error'],d['max_absolute_error']])
    w=wb.create_sheet('最大量差源核对');w.append(['RIC','UTC分钟','baseline量','IB量','IB减baseline手数','源重聚合一致','源记录数','最大单条记录量','最大单条记录价格','aggressor','源发布时间','交易所时间','发布晚于交易所秒数','发布晚至少60秒量','本分钟最大发布时差秒'])
    for pr in probes:
        top=pr['largest_source_trades'][0]
        w.append([pr['ric'],pr['minute_utc'],pr['baseline']['volume'],pr['ib']['volume'],pr['volume_ib_minus_base'],pr['source_matches_baseline'],pr['source_records'],top['volume'],top['price_points'],top['aggressor'],top['published_utc'],top['exchange_utc'],top['delay_seconds'],pr['published_at_least_60s_later_volume'],pr['max_publication_lag_seconds']])
    w=wb.create_sheet('迟报源记录');w.append(['RIC','源发布时间UTC','交易所时间UTC','价格原单位','源记录手数','aggressor','发布晚于交易所秒数'])
    for pr in probes:
        for t in pr['published_at_least_60s_later_records']:
            w.append([pr['ric'],t['published_utc'],t['exchange_utc'],t['price_points'],t['volume'],t['aggressor'],t['delay_seconds']])
    w=wb.create_sheet('统计口径');w.append(['项目','说明'])
    for row in [
        ['范围','2026年5月1—29日，已有未复权逐合约baseline；原油选CLN26，避免CLM26在月中到期。'],
        ['价格单位','不缩放价格；tick为NQ 0.25、CL 0.01、GC 0.1、HG 0.0005、TY 1/64。'],
        ['时钟','NQ/CL/GC既有底表按交易所时间；HG/TY为发布时间口径，均保留其既有分钟，不重建底表。'],
        ['分钟偏移','五个样本都以原UTC分钟匹配最好；±1分钟仅作为错位诊断。'],
        ['量差分母','同一分钟匹配集合上IB总量减baseline总量，再除baseline总量；缺失网格行另列。'],
        ['黄金4行','4个缺失分钟均为零量且OHLC全空的baseline补行，未发现缺有成交分钟。'],
        ['源抽查','每个合约各抽一个最大量差分钟，依据底表时钟重聚合TAS逐笔；大单aggressor=0并不等同已证明特殊或异常。'],
        ['发布时间差','指LSEG源Date-Time减Exch Time，不是IB接收时间；迟报源记录页仅列本次五分钟抽查中发布晚至少60秒的记录。'],
        ['纳指例外','最大量差分钟TAS方向码全为1/2，最大发布时差不足0.06秒；不能用迟报大单解释全部差异。'],
        ['归因','所有量价差异均先列为观测差异；成交条件、成交ID、修正/撤销和IB纳入规则未核定，不登记为已确认IB错误。'],
    ]:w.append(row)
    for w in wb:
        w.freeze_panes='C2';w.auto_filter.ref=w.dimensions
        for c in w[1]:c.font=Font(color='FFFFFF',bold=True);c.fill=PatternFill('solid',fgColor='17365D')
        for k in range(1,w.max_column+1):w.column_dimensions[get_column_letter(k)].width=28 if k>1 else 25
        if w.title=='统计口径':w.column_dimensions['B'].width=125
        for row in w:
            for c in row:c.alignment=Alignment(wrap_text=True,vertical='top')
    report=ROOT/'reports/IB与Baseline_五品种对比_202605.xlsx';wb.save(report)
    read=load_workbook(report,data_only=True);assert read['五品种汇总'].max_row==6
    assert {r[1] for r in read['五品种汇总'].iter_rows(min_row=2,values_only=True)}=={s[0] for s in SPECS}
    result=dict(status='complete_verified',scope='five additional fixed futures contracts, existing May 2026 baseline and IB',products=results,source_probes=probes,workbook=str(report),
                source_data_modified=False,source_script_sha256=hashlib.sha256((SCRIPTS / 'compare_ib_baseline_one.py').read_bytes()).hexdigest())
    (OUT/'completion.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(results,ensure_ascii=False,indent=2))
    print('Workbook:',report)

if __name__=='__main__':main()
