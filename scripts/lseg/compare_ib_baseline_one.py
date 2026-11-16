#!/usr/bin/env python3
"""Small, reproducible IB vs existing unadjusted baseline comparison for one RIC."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
DATA=Path('/mnt/hdd-raid5-72t/liang_torch/lseg_data')
sys.path.insert(0,str(ROOT/'artifacts/python_deps'))
from openpyxl import Workbook,load_workbook
from openpyxl.styles import Font,PatternFill,Alignment

def metrics(frame):
    result={}
    for c in ['open','high','low','close','volume']:
        delta=frame[c+'_ib']-frame[c+'_base'];e=delta.abs();valid=e.notna()
        result[c]=dict(compared=int(valid.sum()),equal=int((e[valid]<=1e-9).sum()),equal_pct=float((e[valid]<=1e-9).mean()*100),
                       mean_absolute_error=float(e.mean()),median_absolute_error=float(e.median()),p95_absolute_error=float(e.quantile(.95)),p99_absolute_error=float(e.quantile(.99)),max_absolute_error=float(e.max()))
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ric',default='ESM26');p.add_argument('--start',default='20260501');p.add_argument('--end',default='20260529')
    p.add_argument('--baseline-dir',type=Path,default=DATA/'baseline_data_1min/CME/ES')
    p.add_argument('--ib-file',type=Path,default=DATA/'ib_minute_parquet/covered_futures_20261103/data/ESM26.parquet')
    p.add_argument('--output',type=Path,default=ROOT/'artifacts/ib_baseline_es_comparison_20261103')
    p.add_argument('--tick-size',type=float,default=.25)
    p.add_argument('--skip-source-check',action='store_true',help='Compare existing minute bars without reading raw TAS trade files')
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True)
    files=sorted(f for f in a.baseline_dir.glob('*.parquet') if a.start<=f.stem<=a.end)
    assert files
    inputs={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in files+[a.ib_file]}
    parts=[]
    for f in files:
        t=pq.read_table(f,columns=['ric','contract_id','ts','open','high','low','close','volume','count','special_volume','implied_volume']).to_pandas()
        t=t[t.ric==a.ric].copy();t['trade_day']=f.stem;parts.append(t)
    base=pd.concat(parts,ignore_index=True).sort_values('ts')
    assert not base.ts.duplicated().any() and base.contract_id.nunique()==1
    ib=pq.read_table(a.ib_file).to_pandas()
    assert set(ib.ric)=={a.ric} and ib.con_id.nunique()==1
    assert (ib.ts_utc_ns%60_000_000_000==0).all()
    ib['ts']=ib.ts_utc_ns//1_000_000_000
    assert not ib.ts.duplicated().any()
    ib=ib[(ib.ts>=base.ts.min()-60)&(ib.ts<=base.ts.max()+60)].copy()
    probes=[]
    for shift in [-60,0,60]:
        x=ib.copy();x['ts']+=shift
        j=base.merge(x,on='ts',suffixes=('_base','_ib'),validate='one_to_one')
        e=(j.close_ib-j.close_base).abs()
        probes.append(dict(ib_shift_seconds=shift,matched=len(j),close_equal_pct=float((e<=1e-9).mean()*100),close_mae_points=float(e.mean())))
    merged=base.merge(ib,on='ts',suffixes=('_base','_ib'),validate='one_to_one')
    merged['utc']=pd.to_datetime(merged.ts,unit='s',utc=True)
    for c in ['open','high','low','close','volume']:
        merged[c+'_ib_minus_base']=merged[c+'_ib']-merged[c+'_base']
    merged['max_ohlc_abs_error']=merged[[c+'_ib_minus_base' for c in ['open','high','low','close']]].abs().max(axis=1)
    merged['ohlc_all_equal']=merged.max_ohlc_abs_error<=1e-9
    merged.to_parquet(a.output/'minute_differences.parquet',index=False)
    merged.to_csv(a.output/'minute_differences.csv',index=False,encoding='utf-8-sig')
    daily=[]
    for day,g in merged.groupby('trade_day'):
        daily.append(dict(trade_day=day,minutes=len(g),close_equal_pct=float((g.close_ib_minus_base.abs()<=1e-9).mean()*100),
                          ohlc_all_equal_pct=float(g.ohlc_all_equal.mean()*100),volume_equal_pct=float((g.volume_ib_minus_base.abs()<=1e-9).mean()*100),
                          baseline_volume=float(g.volume_base.sum()),ib_volume=float(g.volume_ib.sum()),max_price_error=float(g.max_ohlc_abs_error.max())))
    pd.DataFrame(daily).to_csv(a.output/'daily_summary.csv',index=False,encoding='utf-8-sig')
    top=merged.nlargest(100,'max_ohlc_abs_error')
    top.to_csv(a.output/'top_price_differences.csv',index=False,encoding='utf-8-sig')
    # Bound one source check to the largest high discrepancy. This is an
    # explanation/sensitivity check, not a new price-filtering rule.
    worst=merged.loc[merged.high_ib_minus_base.abs().idxmax()]
    outside=pd.DataFrame(columns=['published_utc','event_utc','price_points','volume','aggressor','delay_seconds'])
    if not a.skip_source_check:
        m0=int(worst.ts)*10**9;hour=(m0//(3600*10**9))*(3600*10**9)
        source_dir=DATA/'tas_tick_parquet/2026/trade'/('ric='+a.ric)
        source=ds.dataset([str(f) for f in sorted(source_dir.glob('*.parquet'))],format='parquet')
        raw=source.to_table(filter=(ds.field('ts_utc_ns')>=hour)&(ds.field('ts_utc_ns')<hour+3600*10**9),columns=['ric','ts_utc_ns','exch_hms_ns','price','volume','aggressor']).to_pandas()
        day=86400*10**9;raw['event_ns']=(raw.ts_utc_ns//day)*day+raw.exch_hms_ns
        raw.loc[raw.event_ns>raw.ts_utc_ns+day//2,'event_ns']-=day
        raw.loc[raw.ts_utc_ns>raw.event_ns+day//2,'event_ns']+=day
        raw=raw[(raw.event_ns>=m0)&(raw.event_ns<m0+60*10**9)].sort_values(['event_ns','ts_utc_ns'],kind='stable').copy()
        raw['price_points']=raw.price/1e9;raw['published_utc']=pd.to_datetime(raw.ts_utc_ns,unit='ns',utc=True).astype(str)
        raw['event_utc']=pd.to_datetime(raw.event_ns,unit='ns',utc=True).astype(str)
        raw['delay_seconds']=(raw.ts_utc_ns.astype('int64')-raw.event_ns.astype('int64'))/1e9
        aggregate=lambda f:dict(open=float(f.price_points.iloc[0]),high=float(f.price_points.max()),low=float(f.price_points.min()),close=float(f.price_points.iloc[-1]),volume=int(f.volume.sum()))
        original=aggregate(raw)
        assert all(original[k]==worst[k+'_base'] for k in original),'bounded source range did not reproduce baseline'
        outside=raw[(raw.price_points<worst.low_ib)|(raw.price_points>worst.high_ib)]
        kept=raw[~raw.index.isin(outside.index)]
        raw.to_parquet(a.output/'spike_minute_tas_trades.parquet',index=False)
        outside.to_csv(a.output/'spike_outside_ib_range.csv',index=False,encoding='utf-8-sig')
        sensitivity=aggregate(kept)
    stat=metrics(merged)
    for c in ['open','high','low','close']:
        stat[c]['within_one_tick_pct']=float((merged[c+'_ib_minus_base'].abs()<=a.tick_size+1e-9).mean()*100)
    summary=dict(status='comparison_complete',ric=a.ric,contract_id=str(base.contract_id.iloc[0]),ib_con_id=str(ib.con_id.iloc[0]),
        date_basis='Chicago TradDay daily filenames',start_trade_day=a.start,end_trade_day=a.end,trade_days=len(files),
        first_utc=str(pd.to_datetime(base.ts.min(),unit='s',utc=True)),last_utc=str(pd.to_datetime(base.ts.max(),unit='s',utc=True)),
        baseline_minutes=len(base),ib_minutes_in_span=int(((ib.ts>=base.ts.min())&(ib.ts<=base.ts.max())).sum()),matched_minutes=len(merged),
        missing_ib_minutes=int((~base.ts.isin(ib.ts)).sum()),extra_ib_minutes_in_span=int((~ib[(ib.ts>=base.ts.min())&(ib.ts<=base.ts.max())].ts.isin(base.ts)).sum()),
        ib_time_flag_rows=int((merged.time_quality_flags!=0).sum()),ib_ohlcv_flag_rows=int((merged.ohlcv_quality_flags!=0).sum()),
        tick_size=a.tick_size,metrics=stat,ohlc_all_equal_pct=float(merged.ohlc_all_equal.mean()*100),
        baseline_volume=float(merged.volume_base.sum()),ib_volume=float(merged.volume_ib.sum()),volume_ib_minus_base=float(merged.volume_ib.sum()-merged.volume_base.sum()),
        volume_ib_minus_base_pct=float((merged.volume_ib.sum()/merged.volume_base.sum()-1)*100),
        baseline_positive_ib_zero_minutes=int(((merged.volume_base>0)&(merged.volume_ib==0)).sum()),
        time_shift_probes=probes,source_check=(dict(utc=str(worst.utc),raw_trade_count=len(raw),raw_aggregate=original,ib_aggregate={k:float(worst[k+'_ib']) for k in ['open','high','low','close','volume']},
        outside_ib_range_count=len(outside),outside_ib_range_volume=int(outside.volume.sum()),outside_ib_range_trades=outside[['published_utc','event_utc','price_points','volume','aggressor','delay_seconds']].to_dict('records'),
        illustrative_exclusion_aggregate=sensitivity,illustrative_exclusion_matches_ib=all(sensitivity[k]==worst[k+'_ib'] for k in sensitivity),
        limitation='Diagnostic exclusion selected using IB high/low; not an independently justified cleaning rule. Raw qualifiers/corrections were not audited.') if not a.skip_source_check else dict(status='not_run',limitation='Minute comparison only; individual raw trades and corrections were not checked.')),
        source_input_sha256=inputs)
    review_path=a.output/'review_findings.json'
    reviews=json.loads(review_path.read_text()) if review_path.exists() else []
    summary['review_findings']=reviews
    (a.output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    wb=Workbook();ws=wb.active;ws.title='汇总'
    ws.append(['项目','结果','说明'])
    for row in [
        ['样本',a.ric,f'{a.start}—{a.end}，{len(files)}个交易日，合约级未复权baseline'],
        ['匹配分钟',len(merged),f'baseline {len(base)}；同窗IB {summary["ib_minutes_in_span"]}'],
        ['OHLC全部一致%',summary['ohlc_all_equal_pct'],'同RIC同UTC分钟，零成交量分钟保留'],
        *[[f'{c} 完全一致%',stat[c]['equal_pct'],f'MAE={stat[c]["mean_absolute_error"]:.8f}；最大={stat[c]["max_absolute_error"]}'] for c in ['open','high','low','close','volume']],
        ['收盘误差≤1 tick%',stat['close']['within_one_tick_pct'],f'{a.ric} tick={a.tick_size}（原报价单位）'],
        ['baseline总量',summary['baseline_volume'],'原始手数'],['IB总量',summary['ib_volume'],'原始手数'],
        ['IB总量差%',summary['volume_ib_minus_base_pct'],'以baseline总量为分母'],
        ['尖刺分钟',str(worst.utc),f'baseline high={worst.high_base}，IB high={worst.high_ib}'],
        ['源记录复现','未执行' if a.skip_source_check else '通过','本次仅对比已有分钟底表' if a.skip_source_check else 'TAS逐笔重聚合，OHLCV与该分钟baseline完全一致'],
        ['差异解释','原始成交条件／修正仍待核对' if a.skip_source_check else f'{len(outside)}笔IB价格范围外打印，共{int(outside.volume.sum())}手','分钟线差异不直接作为数据源错误结论'],
    ]:ws.append(row)
    for name,frame in [('按日统计',pd.DataFrame(daily)),('最大价格差异100',top[['trade_day','utc','open_base','open_ib','high_base','high_ib','low_base','low_ib','close_base','close_ib','volume_base','volume_ib','max_ohlc_abs_error']].assign(utc=lambda f:f.utc.astype(str))),('尖刺源成交',outside[['published_utc','event_utc','price_points','volume','aggressor','delay_seconds']]),('分钟偏移诊断',pd.DataFrame(probes))]:
        w=wb.create_sheet(name);w.append(list(frame.columns))
        for row in frame.itertuples(index=False,name=None):w.append(list(row))
    if reviews:
        ws.append(['待核查事项',reviews[0]['title'],reviews[0]['status']])
        w=wb.create_sheet('待核查事项')
        w.append(['事项编号','标题','状态','RIC','UTC分钟','baseline手数','IB手数','相差手数','已确认事实','尚待核实','后续核查'])
        for r in reviews:
            w.append([r['id'],r['title'],r['status'],r['ric'],r['minute_utc'],r['baseline_volume'],r['ib_volume'],r['baseline_minus_ib_volume'],r['confirmed'],r['unresolved'],'；'.join(r['next_checks'])])
    for w in wb:
        w.freeze_panes='B2';w.auto_filter.ref=w.dimensions
        for cell in w[1]:cell.font=Font(color='FFFFFF',bold=True);cell.fill=PatternFill('solid',fgColor='17365D')
        for col in w.columns:
            w.column_dimensions[col[0].column_letter].width=38 if col[0].column in (1,2) else 26
        for row in w:
            for c in row:c.alignment=Alignment(wrap_text=True,vertical='top')
    report=ROOT/'reports'/f'IB与Baseline分钟线简单对比_{a.ric}_{a.start[:6]}.xlsx';wb.save(report)
    check=load_workbook(report,data_only=True);assert check['尖刺源成交'].max_row==len(outside)+1
    assert all(hashlib.sha256(Path(f).read_bytes()).hexdigest()==h for f,h in inputs.items())
    print(json.dumps({k:v for k,v in summary.items() if k not in ['source_input_sha256','source_check']},ensure_ascii=False,indent=2))
    print('Workbook:',report)

if __name__=='__main__':main()
