#!/usr/bin/env python3
"""Publish requested comparison findings after the LSEG-only rebuild finishes."""
import json
from pathlib import Path
import sys

import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

import build_market_status_klines as p
from rebuild_lseg_only_klines import RUN,WORK,PROBLEMS,problem_parquet
from rebuild_kline_fill_policy import local_parquet


def finalize():
    contracts=json.loads((RUN/'contracts.json').read_text())
    results=[json.loads((RUN/'contracts'/f'{ric}.json').read_text()) for ric in contracts]
    assert len(results)==len(contracts)
    for name,rows in [
        ('venue_pending.csv',[dict(ric=c['ric'],product=c['product'],problem='LSEG原始归属仍为review，保留原目录，不借Databento定义重分类') for c in contracts.values() if c['exchange']=='review']),
        ('tick_size_pending.csv',[dict(ric=c['ric'],product=c['product'],problem='LSEG HRD最小价位缺失或冲突，未输出1跳一致率') for c in contracts.values() if c['tick_size'] is None]),
    ]:
        if rows:p.write_csv(PROBLEMS/name,rows,list(rows[0]))
    p.WORK=WORK
    # A current TAS source extends beyond the Nov 3 IB snapshot. Those later
    # minutes are coverage, not evidence of IB loss. Refresh only comparisons
    # made before this common-source cutoff guard was added.
    snapshot=json.loads((p.IB/'_audit/delivery.json').read_text())
    ib_end=int(pd.Timestamp(snapshot['ib_last_utc']).value)+p.MINUTE
    for result in results:
        ric=result['ric'];end=min(result['end_ns'],ib_end);contract=contracts[ric]
        if not result['comparison'] or result.get('comparison_end_ns')==end:continue
        bars=pq.ParquetFile(WORK/'contracts'/ric/'kline.parquet').read()
        bars=bars.filter(pc.less(bars['ts_utc_ns'],end))
        ib=pq.ParquetFile(contract['ib_file']).read()
        ib=ib.filter(pc.and_(pc.greater_equal(ib['ts_utc_ns'],result['start_ns']),pc.less(ib['ts_utc_ns'],end)))
        query=p.connection()
        try:stats,delta=p.compare_ib(query,ric,bars,ib,contract['tick_size'])
        finally:query.close()
        local=WORK/'contracts'/ric/'ib_differences.parquet';remote=PROBLEMS/'ib_differences'/f'{ric}.parquet'
        if len(delta):local_parquet(delta,local);problem_parquet(delta,remote)
        else:local.unlink(missing_ok=True);remote.unlink(missing_ok=True)
        result.update(comparison=stats,comparison_end_ns=end,comparison_effective_volume=int(pc.sum(bars['volume']).as_py() or 0))
        p.save(RUN/'contracts'/f'{ric}.json',result)
    c=p.connection(4)
    differences=[str(WORK/'contracts'/r['ric']/'ib_differences.parquet') for r in results
        if r['comparison'] and (WORK/'contracts'/r['ric']/'ib_differences.parquet').exists()]
    missing={}
    if differences:
        missing={r['ric']:r for r in c.execute('''SELECT ric,
            count_if(has_tas AND NOT has_ib AND NOT is_empty)::UBIGINT missing_ib_trade_minutes,
            count_if(has_tas AND NOT has_ib AND is_empty)::UBIGINT missing_ib_empty_minutes,
            coalesce(sum(tas_volume) FILTER(WHERE has_tas AND NOT has_ib),0)::DOUBLE missing_ib_volume,
            count_if(has_ib AND NOT has_tas AND ib_volume>0)::UBIGINT ib_only_nonzero_minutes
            FROM read_parquet(?) GROUP BY ric''',[differences]).fetch_arrow_table().to_pylist()}
    summaries=[];gaps=[]
    for result in results:
        ric=result['ric'];stats=result['comparison']
        if not stats:
            gaps.append(dict(RIC=ric,品种=result['product'],原因='无有效TAS成交或现有同规格IB快照无共同分钟'))
            continue
        row=dict(stats,product=result['product'],exchange=result['exchange'],
            start_utc=pd.Timestamp(result['start_ns'],tz='UTC').isoformat(),
            end_utc_exclusive=pd.Timestamp(result.get('comparison_end_ns',result['end_ns']),tz='UTC').isoformat())
        if row['tick_size'] is None:row['close_within_tick_pct']=None
        extra=missing.get(ric,{})
        for field in ('missing_ib_trade_minutes','missing_ib_empty_minutes','missing_ib_volume','ib_only_nonzero_minutes'):
            row[field]=extra.get(field) or 0
        assert row['missing_ib_trade_minutes']+row['missing_ib_empty_minutes']==row['missing_ib_minutes'],ric
        assert (row['matched_tas_volume'] or 0)+row['missing_ib_volume']==result.get('comparison_effective_volume',result['effective_volume']),ric
        summaries.append(row)
    display=lambda row:{label:row.get(key) for key,label in p.SUMMARY_NAMES.items()}
    rows=list(map(display,summaries));products=[]
    summed=['matched_minutes','missing_ib_minutes','ib_only_minutes','valid_price_minutes','ohlc_equal_minutes',
        'close_equal_minutes','close_within_tick_minutes','volume_equal_minutes','matched_tas_volume','matched_ib_volume',
        'outside_status_trade_count','unknown_status_trade_count','missing_exchange_time_trade_count','late_trade_count',
        'missing_ib_trade_minutes','missing_ib_empty_minutes','missing_ib_volume','ib_only_nonzero_minutes']
    for product in sorted({s['product'] for s in summaries}):
        group=[s for s in summaries if s['product']==product]
        v={k:sum(s.get(k) or 0 for s in group) for k in summed}
        v.update(product=product,exchange=group[0]['exchange'],ric=f'{len(group)}个RIC')
        for name in ('ohlc_equal','close_equal','close_within_tick'):
            v[name+'_pct']=100*v[name+'_minutes']/v['valid_price_minutes'] if v['valid_price_minutes'] else None
        if any(s['tick_size'] is None for s in group):v['close_within_tick_pct']=None
        v['volume_equal_pct']=100*v['volume_equal_minutes']/v['matched_minutes'] if v['matched_minutes'] else None
        v['ib_volume_difference_pct']=100*(v['matched_ib_volume']-v['matched_tas_volume'])/v['matched_tas_volume'] if v['matched_tas_volume'] else None
        for name in ('max_price_error','max_close_error','max_volume_error'):
            v[name]=max((s[name] for s in group if s[name] is not None),default=None)
        products.append(display(v))
    out=p.DEST/'meta/comparison_ib/2026';out.mkdir(parents=True,exist_ok=True)
    p.write_csv(out/'合约比较.csv',rows,list(p.SUMMARY_NAMES.values()))
    p.write_csv(out/'品种比较.csv',products,list(p.SUMMARY_NAMES.values()))
    probes=[dict(ric=s['ric'],**probe) for s in summaries for probe in s['shift_probes']]
    p.write_csv(out/'分钟标签偏移诊断.csv',probes,['ric','shift_seconds','matched_minutes','close_mae','close_equal_pct'])
    for files,name in [(differences,'分钟差异.parquet'),([str(f) for f in (PROBLEMS/'trade_state').glob('*.parquet')],'状态与成交差异.parquet')]:
        target=out/name
        if not files:target.unlink(missing_ok=True);continue
        temporary=out/('.'+name+'.partial')
        c.read_parquet(files,union_by_name=True).order('ric,ts_utc_ns').write_parquet(str(temporary),compression='zstd')
        temporary.replace(target)
    sys.path.insert(0,str(p.ROOT/'artifacts/python_deps'))
    from openpyxl import Workbook
    from openpyxl.styles import Font
    wb=Workbook();wb.remove(wb.active)
    for name,data in [('品种比较',products),('合约比较',rows),('覆盖缺口',gaps)]:
        sheet=wb.create_sheet(name)
        if not data:continue
        fields=list(data[0]);sheet.append(fields)
        for row in data:sheet.append([row.get(k) for k in fields])
        sheet.freeze_panes='A2';sheet.auto_filter.ref=sheet.dimensions
        for cell in sheet[1]:cell.font=Font(bold=True)
        for column in sheet.columns:sheet.column_dimensions[column[0].column_letter].width=25
    sheet=wb.create_sheet('比较口径')
    for text in [
        '2026：TAS按各品种现有LSEG源截止，最晚11月8日；IB为11月3日原始快照，逐RIC窗口见合约比较。',
        '新版TAS K线只用LSEG成交、可信改撤、原始状态、HRD和本RIC历史价；不输入Databento状态或definition、静态日历、CME公告或IB价量。',
        '状态按LSEG发布时间生效，不取整、不照交易所时间回拨；边界可能落在一分钟内，按PARTIAL或UNKNOWN表达。',
        '缺源包使状态转UNKNOWN，下一明确阶段前不恢复；NQ 11月4日、KWR 5月30–31日源缺口不推算补齐。',
        'TAS按交易所成交时间归UTC分钟；缺时间回退发布时间。10列、Float64实际价格、UInt64量及成交记录数。',
        'TAS在有效覆盖期连续补分钟，无成交沿用本RIC旧收盘，零量零笔数；无历史价为空，只有实际补价才is_ffill=true。',
        '比较排除计划休市及UNKNOWN的纯补行，保留实际成交、HALTED和原始IB缺行；IB缺行不等于漏计真实成交。',
        '可靠关联改撤已净额化；未确认关联的原打印保留，问题清单仍适用，不能称全量最终有效成交。',
        'IB原文件只读，不补行、不补价、不增加状态、不裁剪或写回；报告读取原始完整快照。',
        '主比较不平移、不缩放成交量；±1分钟仅为标签诊断。OHLC一致容差1e-9，总量差只统计匹配分钟。',
        'LSEG HRD最小价位存在冲突或缺失时，不借Databento补规格；1跳比例留空。',
        '比较截止不晚于IB快照最后有效UTC分钟的下一分钟（2026-11-03 05:20 UTC）；之后的TAS仅交付、不算IB缺行。观察到价量差不直接认定任一供应商错误。',
    ]:sheet.append([text])
    sheet.column_dimensions['A'].width=120
    if differences:
        largest=c.execute('''SELECT ric,ts_utc_ns,tas_close,ib_close,tas_volume,ib_volume,close_error,volume_difference,issue
            FROM read_parquet(?) WHERE has_tas AND has_ib ORDER BY greatest(open_error,high_error,low_error,close_error) DESC NULLS LAST LIMIT 200''',
            [differences]).fetchdf()
        largest['UTC分钟']=pd.to_datetime(largest.pop('ts_utc_ns'),unit='ns',utc=True).astype(str)
        sheet=wb.create_sheet('最大价差分钟');sheet.append(list(largest.columns))
        for row in largest.itertuples(index=False,name=None):sheet.append([None if pd.isna(v) else v for v in row])
        sheet.freeze_panes='A2'
    temporary=out/'.IB与LSEG分钟线比较_2026.xlsx.partial';wb.save(temporary)
    temporary.replace(out/'IB与LSEG分钟线比较_2026.xlsx')
    c.close()
    p.save(RUN/'comparison.json',dict(contracts=len(summaries),products=len(products),matched_minutes=sum(s['matched_minutes'] for s in summaries)))


if __name__=='__main__':finalize()
