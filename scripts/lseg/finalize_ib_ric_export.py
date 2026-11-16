#!/usr/bin/env python3
"""Audit the complete per-RIC IB delivery and publish first/last time comparisons."""
import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
sys.path.insert(0,str(ROOT/'artifacts/python_deps'))
from openpyxl import Workbook,load_workbook
from openpyxl.styles import Font,PatternFill,Alignment
from openpyxl.worksheet.table import Table,TableStyleInfo
from openpyxl.utils import get_column_letter

def utc(ns):
    if ns is None:return None
    seconds,nanos=divmod(ns,10**9)
    return datetime.fromtimestamp(seconds,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+f'.{nanos:09d}Z'

def write_csv(path,rows):
    if not rows:return
    with path.open('w',newline='',encoding='utf-8-sig') as f:
        fields=list(dict.fromkeys(k for r in rows for k in r))
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)

def sheet(wb,name,headers,rows,widths):
    ws=wb.create_sheet(name);ws.append(headers)
    for row in rows:ws.append(row)
    ws.freeze_panes='C2'
    for i,h in enumerate(headers,1):
        cell=ws.cell(1,i);cell.font=Font(color='FFFFFF',bold=True);cell.fill=PatternFill('solid',fgColor='17365D')
        ws.column_dimensions[get_column_letter(i)].width=widths[i-1] if i<=len(widths) else 24
    for row in ws.iter_rows(min_row=2):
        for c in row:c.alignment=Alignment(vertical='top',wrap_text=True)
    if rows:
        tab=Table(displayName='T'+str(len(wb.worksheets)),ref=f'A1:{get_column_letter(len(headers))}{len(rows)+1}')
        tab.tableStyleInfo=TableStyleInfo(name='TableStyleMedium2',showRowStripes=True);ws.add_table(tab)
    return ws

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('output',type=Path)
    p.add_argument('--report',type=Path,default=ROOT/'reports/IB分钟线_逐RIC首末时间对照_20261103.xlsx')
    a=p.parse_args();out=a.output.resolve();audit=out/'_audit'
    complete=json.loads((out/'_SUCCESS.json').read_text())
    assert complete['status']=='complete_verified' and not complete['partial_validation']
    plan=json.loads((audit/'contract_plan.json').read_text())
    bounds=json.loads((audit/'tas_bounds.json').read_text())['bounds']
    expected={j['ric']:j for j in plan['jobs']}
    reports={f.stem:json.loads(f.read_text()) for f in (audit/'jobs').glob('*.json')}
    assert set(expected)==set(reports)
    assert {p.stem for p in (out/'data').glob('*.parquet')}==set(expected)
    assert not list(out.rglob('*.partial'))
    records=[];schema=None;rows=0;file_bytes=0;time_anomalies=[];ohlcv_anomalies=[]
    for ric,j in sorted(expected.items()):
        r=reports[ric];path=out/'data'/f'{ric}.parquet';pf=pq.ParquetFile(path)
        assert r['status']=='passed' and r['con_id']==j['con_id']
        assert r['cutoff_id']==plan['cutoff_id'] and pf.metadata.num_rows==r['rows']
        if schema is None:schema=pf.schema_arrow
        assert schema.equals(pf.schema_arrow,check_metadata=False)
        assert r['first_utc']==j['ib_first_utc'] and r['last_utc']==j['ib_last_utc']
        assert path.stat().st_size==r['bytes']
        endpoint_cols=['ric','ts_utc_ns','con_id','id','time_quality_flags']
        first=pf.read_row_group(0,columns=endpoint_cols).slice(0,1).to_pylist()[0]
        last_group=pf.read_row_group(pf.num_row_groups-1,columns=endpoint_cols)
        last=last_group.slice(len(last_group)-1,1).to_pylist()[0]
        assert first['ric']==last['ric']==ric and first['con_id']==last['con_id']==j['con_id']
        assert first['ts_utc_ns']==r['first_ts_utc_ns'] and last['ts_utc_ns']==r['last_ts_utc_ns']
        b=bounds[ric];tf,tl=b['trade_first_ns'],b['trade_last_ns'];qf,ql=b['quote_first_bucket_ns'],b['quote_last_bucket_ns']
        overlap=(max(r['first_ts_utc_ns']//60000000000,tf//60000000000)<=min(r['last_ts_utc_ns']//60000000000,tl//60000000000)) if tf is not None else None
        status=('成交分钟标签范围有交集' if overlap else '成交分钟标签范围无交集') if tf is not None else ('仅TAS报价可对照' if qf is not None else '该合约TAS无成交报价')
        rec=dict(ric=ric,ib_symbol=j['symbol'],con_id=j['con_id'],local_symbol=j['local_symbol'],exchange=j['exchange'],currency=j['currency'],multiplier=j['multiplier'],maturity_date=j['maturity_date'],delivery_year=j['delivery_year'],delivery_month=j['delivery_month'],
                 ib_rows=r['rows'],ib_first_utc=utc(r['first_ts_utc_ns']),ib_last_utc=utc(r['last_ts_utc_ns']),ib_first_ts_utc_ns=r['first_ts_utc_ns'],ib_last_ts_utc_ns=r['last_ts_utc_ns'],
                 ib_first_source_id=first['id'],ib_last_source_id=last['id'],ib_first_time_quality_flags=first['time_quality_flags'],ib_last_time_quality_flags=last['time_quality_flags'],
                 tas_trade_first_utc=utc(tf),tas_trade_last_utc=utc(tl),tas_trade_first_ns=tf,tas_trade_last_ns=tl,
                 tas_quote_first_bucket_utc=utc(qf),tas_quote_last_bucket_utc=utc(ql),tas_quote_first_bucket_ns=qf,tas_quote_last_bucket_ns=ql,
                 ib_minus_tas_trade_start_days=(r['first_ts_utc_ns']-tf)/86400000000000 if tf is not None else None,
                 ib_minus_tas_trade_end_days=(r['last_ts_utc_ns']-tl)/86400000000000 if tl is not None else None,
                 minute_range_overlap=overlap,coverage_status=status,tas_aliases='|'.join(j['tas_aliases']),
                 invalid_utc_quarantined=r['rejected_invalid_utc'],time_flag_rows=r['time_flag_rows'],ohlcv_flag_rows=r['ohlcv_flag_rows'],identity_mismatch_rows=r['identity_mismatch_rows'],zero_volume_rows=r['zero_volume_rows'],
                 path=str(path.relative_to(out)),bytes=r['bytes'],source_value_sha256=r['source_value_sha256'],file_sha256=r['file_sha256'])
        records.append(rec);rows+=r['rows'];file_bytes+=r['bytes']
        for flag,col_list,result in [
            ('time_flag_rows',['ric','con_id','id','utc_time','timestamp','time_cst','utc_time_raw','time_cst_raw','time_quality_flags'],time_anomalies),
            ('ohlcv_flag_rows',['ric','con_id','id','utc_time','open','high','low','close','volume','ohlcv_quality_flags'],ohlcv_anomalies)]:
            if r[flag]:
                col='time_quality_flags' if flag=='time_flag_rows' else 'ohlcv_quality_flags'
                for batch in pf.iter_batches(batch_size=131072,columns=col_list):
                    table=pa.Table.from_batches([batch]);bad=table.filter(pc.not_equal(table[col],0))
                    if len(bad):result.append(bad)
    assert rows==complete['rows'] and file_bytes==complete['bytes'] and len(records)==complete['files']
    rejected=0
    for path in (audit/'rejected').glob('*.jsonl'):
        with path.open() as f:
            for line in f:
                r=json.loads(line);assert r['utc_time'].startswith('0000-') or r['utc_time']<'1970-01-01';rejected+=1
    assert rejected==complete['rejected_invalid_utc']
    table=pa.Table.from_pylist(records)
    pq.write_table(table,audit/'ric_inventory.parquet',compression='zstd')
    write_csv(audit/'ric_inventory.csv',records)
    excluded=[]
    reasons={'different_venue_currency_or_size':'规格／交易所／币种不匹配（含微型／迷你）','no_matching_expiry_in_actual_HRD':'实际交付HRD未核定该到期合约','no_valid_IB_minutes':'IB无有效UTC分钟记录'}
    for e in plan['excluded']:
        excluded.append({k:e.get(k) for k in ['symbol','con_id','local_symbol','exchange','currency','multiplier','maturity_date','status'] }|{'reason_cn':reasons.get(e['status'],e['status'])})
    write_csv(audit/'contract_exclusions.csv',excluded)
    anomaly_counts={}
    for name,parts in [('time_anomalies',time_anomalies),('ohlcv_anomalies',ohlcv_anomalies)]:
        t=pa.concat_tables(parts) if parts else None
        if t is not None:pq.write_table(t,audit/(name+'.parquet'),compression='zstd')
        anomaly_counts[name]=len(t) if t is not None else 0
    assert anomaly_counts['time_anomalies']==complete['time_flag_rows']
    assert anomaly_counts['ohlcv_anomalies']==complete['ohlcv_flag_rows']
    coverage=Counter(r['coverage_status'] for r in records)
    wb=Workbook();wb.remove(wb.active)
    sheet(wb,'导出概览',['项目','数量／结果','说明'],[
        ['已覆盖IB品种',len({r['ib_symbol'] for r in records}),'本轮仅期货，不导出股票／ETF。'],
        ['单RIC文件数',len(records),'每个data/<RIC>.parquet只含该RIC及其唯一IB con_id。'],
        ['有效分钟行数',rows,'保留零成交量、原始价格、原始数量；不填缺失分钟。'],
        ['最早IB分钟',min(r['ib_first_utc'] for r in records),'UTC，按实际逐文件记录核对。'],
        ['最后IB分钟',max(r['ib_last_utc'] for r in records),'UTC，受固定源id水位约束。'],
        ['源id水位',plan['cutoff_id'],'每个只读工作连接使用一致性快照；只取id不超过此值的记录。'],
        ['无效UTC隔离行',rejected,'保存完整原始行在_audit/rejected/*.jsonl，未猜测修复UTC。'],
        ['时间异常标记行',complete['time_flag_rows'],'有效UTC保留，原timestamp不修改；明细见time_anomalies.parquet。'],
        ['OHLCV异常标记行',complete['ohlcv_flag_rows'],'原值仍保留；仅标记空值、非有限数、OHLC约束或负成交量。'],
        ['行身份异常',complete['identity_mismatch_rows'],'source symbol/exchange/maturity与con_id元数据不一致的记录。'],
        ['未导出合约',len(excluded),'按具体原因分列，不能将元数据存在当作实际有行情。'],
        *[[k,v,'仅首末分钟标签范围比较，不证明逐分钟完整或OHLC一致。'] for k,v in coverage.items()],
        ['数据目录',str(out),'本轮仅导出IB，TAS仅索引查询首末时间。'],
    ],[30,66,100])
    headers=['RIC','IB品种','IB合约ID','IB本地代码','交易所','币种','IB到期日','交割年月','IB分钟数','IB开始UTC','IB最后UTC','首条时间异常码','末条时间异常码','TAS成交开始UTC','TAS成交最后UTC','TAS报价开始UTC','TAS报价最后UTC','IB开始减TAS开始（天）','IB结束减TAS结束（天）','范围核对','TAS实际别名','时间异常行','OHLCV异常行','零成交量行','Parquet文件']
    sheet(wb,'逐RIC首末时间',headers,[[r['ric'],r['ib_symbol'],r['con_id'],r['local_symbol'],r['exchange'],r['currency'],r['maturity_date'],f"{r['delivery_year']}-{r['delivery_month']:02d}",r['ib_rows'],r['ib_first_utc'],r['ib_last_utc'],r['ib_first_time_quality_flags'],r['ib_last_time_quality_flags'],r['tas_trade_first_utc'],r['tas_trade_last_utc'],r['tas_quote_first_bucket_utc'],r['tas_quote_last_bucket_utc'],r['ib_minus_tas_trade_start_days'],r['ib_minus_tas_trade_end_days'],r['coverage_status'],r['tas_aliases'],r['time_flag_rows'],r['ohlcv_flag_rows'],r['zero_volume_rows'],r['path']] for r in records],[19,13,17,25,17,12,18,15,18,34,34,18,18,34,34,34,34,25,25,31,40,18,18,18,40])
    sheet(wb,'合约未导出明细',['IB品种','IB合约ID','IB本地代码','交易所','币种','乘数','到期日','原因'],[[r['symbol'],r['con_id'],r['local_symbol'],r['exchange'],r['currency'],r['multiplier'],r['maturity_date'],r['reason_cn']] for r in excluded],[15,18,29,18,13,18,19,65])
    if time_anomalies:
        endpoints={r['ric']:(r['ib_first_source_id'],r['ib_last_source_id']) for r in records}
        details=[]
        labels={1:'源epoch缺失',2:'源epoch与UTC不符',4:'CST无效',8:'CST与UTC+8不符',16:'UTC不是整分钟'}
        for table in time_anomalies:
            for r in table.to_pylist():
                first,last=endpoints[r['ric']]
                position='／'.join(x for i,x in [(first,'首条'),(last,'末条')] if r['id']==i) or '范围内部'
                details.append([r['ric'],r['con_id'],r['id'],r['utc_time_raw'],r['timestamp'],r['time_cst_raw'],r['time_quality_flags'],'；'.join(v for bit,v in labels.items() if r['time_quality_flags']&bit),position])
        sheet(wb,'时间异常明细',['RIC','IB合约ID','源行ID','源UTC时间','源timestamp毫秒','源CST时间','异常码','异常含义','所在位置'],details,[20,20,20,29,24,29,14,62,19])
    sheet(wb,'字段与口径',['项目','说明'],[
        ['行情范围','IB minute_data现有有效历史；没有截为2026年，也没有导出TAS Trade/Quote。'],
        ['RIC','以真实HRD到期信息核定，交割月取local_symbol；Eurex/JGB使用专门解析规则，周度VIX逐合约核定。'],
        ['TSR20日期','IB最后交易日在前月末，HRD为交割月日期；使用交割月+XSIM/TF/USc/5000KG核定，不能直接按到期月替换。'],
        ['TAS别名','同一实际合约历史一位／两位年码经HRD核定后列为别名；IB文件使用最新交付实际RIC，未改写TAS。'],
        ['首末时间','IB为有效utc_time标签；TAS成交为原Date-Time事件时间，报价为Exch Time秒桶。'],
        ['分钟范围','仅比较UTC标签分钟范围；IB标签代表分钟开始还是结束尚未独立核定，未移动标签。'],
        ['时间差符号','IB减TAS；正值为IB更晚，负值为IB更早。不能仅据时间范围判断哪方缺数据。'],
        ['价格数量','IB DOUBLE原值保留Float64，未做TAS定点缩放或单位转换，未前后复权。'],
        ['时间标准化','utc_time与ts_utc_ns一致；timestamp为源毫秒值，包含错误0值也不覆盖。time_cst为原墙上时间，未施加时区转换。'],
        ['时间异常码','0=未发现上述时间异常；1=源epoch缺失，2=源epoch与UTC不符，4=CST无效，8=CST与UTC+8不符，16=不是整分钟；组合码按位相加。'],
        ['源时间原值','utc_time_raw/time_cst_raw/created_at_raw保存源DATETIME/TIMESTAMP字符串；created_at按查询会话UTC读取。'],
        ['校验','每个文件逐行重建原始15列并核对SHA256、排序、首末时间、标准时间及质量标记；Python核对完整文件集合与footer。'],
        ['快照边界','只读连接各自一致性快照并固定源id上界；不是跨连接同一时刻的全库物理快照。'],
        ['质量比较范围','本次只比较首末时间并标准化导出，不构造TAS分钟线、不执行OHLC或成交量一致性验收。'],
    ],[30,145])
    a.report.parent.mkdir(exist_ok=True,parents=True);wb.save(a.report)
    read=load_workbook(a.report,data_only=True)
    assert read['逐RIC首末时间'].max_row-1==len(records)
    assert {r[0] for r in read['逐RIC首末时间'].iter_rows(min_row=2,values_only=True)}==set(expected)
    result={**complete,'delivery_status':'passed','covered_products':len({r['ib_symbol'] for r in records}),'coverage_status_counts':dict(coverage),'excluded_contracts':len(excluded),'excluded_reasons':dict(Counter(r['status'] for r in excluded)),
            'ib_first_utc':min(r['ib_first_utc'] for r in records),'ib_last_utc':max(r['ib_last_utc'] for r in records),'time_comparison_workbook':str(a.report),'anomaly_counts':anomaly_counts,'schema':str(schema),
            'ric_inventory_sha256':hashlib.sha256((audit/'ric_inventory.parquet').read_bytes()).hexdigest(),
            'flagged_first_ric_count':sum(r['ib_first_time_quality_flags']!=0 for r in records),
            'flagged_last_ric_count':sum(r['ib_last_time_quality_flags']!=0 for r in records)}
    (audit/'delivery.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    run=ROOT/'artifacts/ib_ric_export_20261103'
    shutil.copyfile(audit/'delivery.json',run/'completion.json')
    print(json.dumps({k:v for k,v in result.items() if k!='schema'},ensure_ascii=False,indent=2))

if __name__=='__main__':main()
