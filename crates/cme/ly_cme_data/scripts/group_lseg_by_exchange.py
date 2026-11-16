#!/usr/bin/env python3
"""Build exchange-grouped LSEG catalogues from Reports and reference metadata.

Only metadata is read. Market files and production databases are never changed.
One-digit year RICs remain isolated by source job/period in the detailed index.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys
import urllib.request
from urllib.parse import quote

import pandas as pd

from inventory_tas_rics import routed_product

ROOT = Path(__file__).resolve().parents[1]
PRE = Path('/home/fanghaizhou/preprocess')
HDD = Path('/mnt/hdd-raid5-72t/liang_torch')
NVME = Path('/mnt/nvme-raid0-28t/fanghaizhou/lseg_data')
ISO_URL = 'https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv'
DESTINATION = '172.16.30.100:/mnt/FuturePool/CTA_Data'

# Historical market identifiers are retained in separate columns. These are
# directory families, not claims about a venue's legal owner at every old date.
VENUES = {
    'CME': ('芝加哥商业交易所', 'CME Group', ['XCME']),
    'CBOT': ('芝加哥期货交易所（含历史KCBT）', 'CME Group', ['XCBT', 'XKBT']),
    'COMEX': ('纽约商品交易所', 'CME Group', ['XCEC']),
    'NYMEX': ('纽约商业交易所', 'CME Group', ['XNYM']),
    'ICE_EUROPE': ('ICE欧洲（含历史LIFFE）', 'ICE', ['IFEU', 'ICEU', 'IFLL', 'IFLO', 'IFLX', 'XLIF']),
    'ICE_US': ('ICE美国（含历史NYSE LIFFE US）', 'ICE', ['IFUS', 'ICUS', 'XNLI']),
    'ICE_CANADA': ('ICE加拿大（含历史WCE）', 'ICE', ['IFCA', 'XWCE']),
    'ICE_ENDEX': ('ICE Endex', 'ICE', ['NDEX']),
    'EUREX': ('欧洲期货交易所', 'Deutsche Boerse', ['XEUR']),
    'EURONEXT_PARIS': ('巴黎衍生品市场MONEP', 'Euronext', ['XMON']),
    'CFE': ('Cboe期货交易所', 'Cboe', ['XCBF']),
    'CBOE': ('芝加哥期权交易所', 'Cboe', ['XCBO']),
    'SGX': ('新加坡交易所（含历史SICOM）', 'SGX', ['XSIM', 'XSCE']),
    'HKEX_FUTURES': ('香港期货交易所', 'HKEX', ['XHKF']),
    'LME': ('伦敦金属交易所', 'HKEX', ['XLME']),
    'OSE': ('大阪交易所', 'JPX', ['XOSE']),
    'TSE': ('东京证券交易所', 'JPX', ['XTKS']),
    'ASX24': ('澳大利亚期货市场', 'ASX', ['XSFE']),
    'KRX': ('韩国交易所（含历史KOFEX）', 'KRX', ['XKRX', 'XKFE']),
    'MONTREAL': ('蒙特利尔交易所', 'TMX', ['XMOD']),
    'B3': ('巴西交易所（含历史BM&F）', 'B3', ['BVMF', 'XBMF']),
    'BURSA_MALAYSIA': ('马来西亚衍生品交易所', 'Bursa Malaysia', ['XKLS']),
    'MEXDER': ('墨西哥衍生品交易所', 'BMV Group', ['XEMD']),
    'BMV': ('墨西哥证券交易所', 'BMV Group', ['XMEX']),
    'NSE_IFSC': ('印度NSE国际交易所', 'NSE', ['INSE']),
    'INE': ('上海国际能源交易中心', 'SHFE', ['XINE']),
    'NASDAQ': ('纳斯达克RIC市场', 'Nasdaq', ['XNAS']),
    'NYSE': ('纽约证券交易所RIC市场', 'ICE', ['XNYS']),
    'NYSE_ARCA': ('纽约证券交易所Arca RIC市场', 'ICE', ['ARCX']),
    'CBOE_BZX': ('Cboe BZX RIC市场', 'Cboe', ['BATS']),
    'INDICES': ('指数（单独存放）', '', []),
    'REVIEW': ('交易所或历史身份待核实', '', []),
}
MIC_TO_VENUE = {mic: key for key, (_, _, mics) in VENUES.items() for mic in mics}
CODE_TO_VENUE = {
    'NYM': 'NYMEX', 'CMX': 'COMEX', 'CBT': 'CBOT', 'KBT': 'CBOT',
    'CME': 'CME', 'IMM': 'CME', 'IOM': 'CME', 'GEM': 'CME',
    'IEU': 'ICE_EUROPE', 'LIF': 'ICE_EUROPE', 'IUS': 'ICE_US',
    'ICA': 'ICE_CANADA', 'EDX': 'ICE_ENDEX', 'EUX': 'EUREX',
    'SFE': 'ASX24', 'MON': 'EURONEXT_PARIS', 'MNP': 'EURONEXT_PARIS',
    'SIM': 'SGX', 'SIC': 'SGX', 'CBF': 'CFE', 'KFE': 'KRX', 'KSC': 'KRX',
    'HFE': 'HKEX_FUTURES', 'BMF': 'B3', 'MDX': 'BURSA_MALAYSIA',
    'OSA': 'OSE', 'TYO': 'TSE', 'MDE': 'MEXDER', 'LME': 'LME',
}
STOCK_SUFFIX = {'O': 'NASDAQ', 'N': 'NYSE', 'P': 'NYSE_ARCA', 'BAT': 'CBOE_BZX'}


def read_csv(path):
    opener = gzip.open if path.name.endswith('.gz') else open
    with opener(path, 'rt', newline='', encoding='utf-8-sig') as f:
        yield from csv.DictReader(f)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def join(values):
    return '|'.join(sorted(set(str(v) for v in values if str(v))))


def dump(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def discover_reports():
    # Prefer reports co-located with market files; saved remote metadata fills
    # missing source jobs. A cached Report is not evidence of downloaded Data.
    roots = [
        HDD/'lseg_data/future', HDD/'lseg_data/future_supplement',
        HDD/'usstock_data/raw_data',
        PRE/'database/cme_level2_review_20261025/metadata',
        PRE/'database/lseg_future_supplement_20261014/remote_metadata',
        PRE/'database/lseg_future_supplement_20261012/remote_metadata',
        PRE/'database/usstock_level2_review_20261026/lseg_metadata',
    ]
    sources = {}
    duplicates = []
    for root in roots:
        if not root.exists():
            continue
        for p in sorted(root.rglob('merged-Report.csv.gz')):
            job = p.parent.name
            if not re.search(r'\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}$', job):
                continue
            asset = 'equity' if 'equities' in job else 'futures'
            key = (asset, job)
            if key in sources:
                duplicates.append(dict(job=job, chosen=str(sources[key]), duplicate=str(p),
                                       byte_identical=digest(p)==digest(sources[key])))
            else:
                sources[key] = p
    return sources, duplicates


def source_layer(job):
    return {'tas': 'tas', 'll2': 'normalisedLL2', 'rl2': 'rawLL2',
            'sum': 'summarised', 'raw': 'raw', 'mbp': 'mbp'}[
                re.search(r'_(tas|ll2|rl2|sum|raw|mbp)_\d{4}-', job)[1]]


def references(paths):
    refs = defaultdict(lambda: defaultdict(set))
    for path in paths:
        for row in read_csv(path):
            rec = refs[row['RIC']]
            for field in ['Market MIC', 'Exchange Code', 'RIC Root', 'Security Description']:
                if row.get(field):
                    rec[field].add(row[field])
    return refs


def venue_of_mic(mic):
    return MIC_TO_VENUE.get(mic, 'MIC_'+mic)


def classify(ric, asset, ref, stock_venues):
    mics = ref.get('Market MIC', set())
    codes = ref.get('Exchange Code', set())
    roots = ref.get('RIC Root', set())
    route = routed_product(ric) if asset == 'futures' else ric.rsplit('.', 1)[0]
    shape = ('指数' if ric.startswith('.') else '连续报价' if re.fullmatch(r'Cc\d+', ric)
             else '累计总量' if ric.endswith('TOT') else '原始RIC')
    candidates = {venue_of_mic(m) for m in mics}
    if asset == 'equity':
        # This identifies the RIC/subscription market, not each trade venue.
        venue = stock_venues.get(ric, STOCK_SUFFIX.get(ric.rsplit('.', 1)[-1], 'REVIEW'))
        method = 'published_stock_venue' if ric in stock_venues else 'ric_market_suffix'
        status = 'RIC市场分组' if venue != 'REVIEW' else '待核实'
        note = '股票可能包含跨场所成交；RIC市场不等同每笔成交场所。'
    elif shape == '指数':
        venue, method, status = 'INDICES', 'index_separate', '指数单列'
        note = '指数单列，不作为期货成交工具分入交易所。'
    elif len(candidates) > 1:
        venue, method, status = 'REVIEW', 'conflicting_hrd_venues', '待核实'
        note = '同一原始RIC参考记录涉及多个交易所，需结合源日期和具体合约身份核定。'
    elif len(candidates) == 1:
        venue, method, status = next(iter(candidates)), 'exact_ric_hrd_mic', '参考记录已分组'
        note = '按同RIC全部参考版本的交易所归并；保留历史MIC，不展开一位年份。'
    else:
        code_venues = {CODE_TO_VENUE[c] for c in codes if c in CODE_TO_VENUE}
        if len(code_venues) == 1 and all(c in CODE_TO_VENUE for c in codes):
            venue, method, status = next(iter(code_venues)), 'exact_ric_hrd_exchange_code', '参考记录已分组'
            note = '同RIC参考记录缺MIC，按明确的LSEG交易所代码归类。'
        else:
            venue, method, status = 'REVIEW', 'missing_or_ambiguous_reference', '待核实'
            note = '无可唯一归属交易所的同RIC参考记录；不按相似前缀强行归类。'
    return dict(asset_class=asset, ric=ric, exchange=venue, mapping_status=status,
                mapping_method=method, instrument_shape=shape, routed_product=route,
                reference_roots=join(roots), reference_mics=join(mics),
                reference_exchange_codes=join(codes), candidate_exchanges=join(candidates),
                reference_description=join(ref.get('Security Description', set()))[:2500], note=note)


def save_frame(out, name, frame):
    frame.to_csv(out/(name+'.csv'), index=False, encoding='utf-8-sig')
    frame.to_parquet(out/(name+'.parquet'), index=False)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT/'artifacts/lseg_exchange_groups_20261109')
    args = p.parse_args()
    out = args.output
    out.mkdir(parents=True, exist_ok=True)
    (out/'completion.json').unlink(missing_ok=True)
    iso = out/'ISO10383_MIC.csv'
    if not iso.exists():
        iso.write_bytes(urllib.request.urlopen(ISO_URL, timeout=30).read())
    iso_rows = {r['MIC']: r for r in read_csv(iso)}
    ref_paths = [NVME/'cme_causal_roll_inputs_20261023/hrd.csv',
                 ROOT/'artifacts/ib_tas_all_futures_20261103/supplement_hrd.csv']
    stock_path = PRE/'database/usstock_multifrequency_20261026/source_coverage.csv'
    stocks = {r['ric']: {'ARCA': 'NYSE_ARCA', 'BZX': 'CBOE_BZX', 'NASDAQ': 'NASDAQ',
                         'NYSE': 'NYSE'}[r['venue']] for r in read_csv(stock_path)}
    refs = references(ref_paths)
    reports, duplicate_reports = discover_reports()
    print(f'Discovered {len(reports)} source jobs; building active RIC index.', flush=True)
    jobs, detail = [], []
    for (asset, job), report in reports.items():
        rows = list(read_csv(report))
        active = [r for r in rows if int(r['Count']) > 0]
        files = list(report.parent.glob('merged-Data*.csv.gz'))
        layer = source_layer(job)
        info = dict(asset_class=asset, source_job=job, source_layer=layer, source_period=job[-21:],
                    report_path=str(report), report_sha256=digest(report),
                    local_data_files=len(files), local_compressed_bytes=sum(f.stat().st_size for f in files),
                    raw_file_presence='本机有Data文件（未作完整性验收）' if files else '仅源清单元数据',
                    report_rows=len(rows), active_ric_rows=len(active), source_count=sum(int(r['Count']) for r in rows))
        jobs.append(info)
        for r in active:
            detail.append(dict(asset_class=asset, source_job=job, source_layer=layer,
                               source_period=job[-21:], ric=r['#RIC'], source_count=int(r['Count']),
                               source_start=r['Start'], source_end=r['End'],
                               local_data_files=len(files), report_path=str(report)))
    job_frame = pd.DataFrame(jobs)
    frame = pd.DataFrame(detail)
    assert not frame.duplicated(['asset_class', 'source_job', 'ric']).any()
    keys = sorted(set(zip(frame.asset_class, frame.ric)))
    classified = pd.DataFrame([classify(ric, asset, refs.get(ric, {}), stocks) for asset, ric in keys])
    frame = frame.merge(classified, on=['asset_class', 'ric'], how='left', validate='many_to_one')
    # A dedicated COMEX source establishes exchange context for its missing
    # reference codes; this does not identify a matching composite contract.
    comex = (frame.mapping_method=='missing_or_ambiguous_reference') & frame.source_job.str.startswith('comex_')
    frame.loc[comex, ['exchange', 'mapping_method', 'mapping_status', 'note']] = [
        'COMEX', 'dedicated_comex_source', '专用源包已分组',
        'COMEX专用源包；保留前导1等原始RIC，不据此与复合报价合并。']
    frame['year'] = frame.source_period.str[:4].astype(int)
    grouped = []
    for (asset, exchange, ric), g in frame.groupby(['asset_class', 'exchange', 'ric'], sort=True):
        first = g.iloc[0]
        grouped.append(dict(asset_class=asset, exchange=exchange, ric=ric,
            routed_product=first.routed_product, reference_roots=first.reference_roots,
            instrument_shape=first.instrument_shape, mapping_status=join(g.mapping_status),
            mapping_method=join(g.mapping_method), reference_mics=first.reference_mics,
            reference_exchange_codes=first.reference_exchange_codes,
            candidate_exchanges=first.candidate_exchanges,
            source_layers=join(g.source_layer), source_jobs=len(g), first_source_utc=g.source_start.min(),
            last_source_utc=g.source_end.max(), source_years=join(g.year),
            jobs_with_local_data=int((g.local_data_files>0).sum()),
            reference_description=first.reference_description, note=join(g.note)))
    catalog = pd.DataFrame(grouped)
    groups = []
    for exchange, g in catalog.groupby('exchange', sort=True):
        example_mics = set('|'.join(g.reference_mics).split('|')) - {''}
        fallback = next((iso_rows[m]['MARKET NAME-INSTITUTION DESCRIPTION'] for m in sorted(example_mics)
                         if m in iso_rows), exchange)
        name, company, _ = VENUES.get(exchange, (fallback, '', []))
        groups.append(dict(exchange=exchange, exchange_name_cn=name, exchange_group=company,
            ric_count=len(g), futures_ric_count=int((g.asset_class=='futures').sum()),
            equity_ric_count=int((g.asset_class=='equity').sum()),
            routed_root_count=g[['asset_class', 'routed_product']].drop_duplicates().shape[0],
            rics_with_local_data=int((g.jobs_with_local_data>0).sum()),
            source_layers=join(g.source_layers.str.split('|').explode()),
            first_source_utc=g.first_source_utc.min(), last_source_utc=g.last_source_utc.max(),
            example_roots=join(g.routed_product)[:2500]))
    summary = pd.DataFrame(groups)
    catalog = catalog.merge(summary[['exchange', 'exchange_name_cn', 'exchange_group']], on='exchange', validate='many_to_one')
    frame = frame.merge(summary[['exchange', 'exchange_name_cn', 'exchange_group']], on='exchange', validate='many_to_one')
    product_rows = []
    for (asset, exchange, root), g in catalog.groupby(['asset_class', 'exchange', 'routed_product'], sort=True):
        product_rows.append(dict(asset_class=asset, exchange=exchange, routed_product=root,
            ric_count=len(g), example_ric=join(g.ric)[:120], reference_roots=join(g.reference_roots.str.split('|').explode()),
            source_layers=join(g.source_layers.str.split('|').explode()),
            first_source_utc=g.first_source_utc.min(), last_source_utc=g.last_source_utc.max(),
            mapping_status=join(g.mapping_status)))
    products = pd.DataFrame(product_rows)
    for name, data in [('source_jobs', job_frame), ('source_ric_details', frame),
                       ('ric_catalog', catalog), ('exchange_summary', summary), ('product_groups', products)]:
        save_frame(out, name, data)
    for exchange, g in catalog.groupby('exchange', sort=True):
        folder = out/'exchanges'/exchange
        folder.mkdir(parents=True, exist_ok=True)
        save_frame(folder, 'ric_catalog', g)
        save_frame(folder, 'products', products[products.exchange==exchange])

    # Historical one-file-to-one-file plan, retained for source lineage only.
    # The user's final RIC/day layout supersedes these legacy destinations.
    # No copy/move/link or remote write is performed here.
    inventory_path = HDD/'lseg_data/tas_tick_parquet/2026/_audit/ric_inventory.csv'
    inv = pd.read_csv(inventory_path, keep_default_na=False)
    ric_exchange = frame[(frame.asset_class=='futures') & (frame.source_layer=='tas') &
                         frame.source_job.str.startswith('shanghai_evolution_futures_') & (frame.year==2026)]
    assert not ric_exchange.ric.duplicated().any()
    paths = []
    lookup = ric_exchange.set_index('ric').to_dict('index')
    for r in inv.to_dict('records'):
        m = lookup.get(r['ric'])
        assert m is not None, f"Exported TAS RIC missing source classification: {r['ric']}"
        folder = HDD/'lseg_data/tas_tick_parquet/2026'/r['data_type']/('ric='+quote(r['ric'], safe='_-.^'))
        files = sorted(folder.glob('*.parquet'))
        assert files, folder
        for f in files:
            relative = f"LSEG/{m['exchange']}/tas/{r['data_type']}/{quote(r['ric'], safe='_-.^')}/2026/{f.name}"
            paths.append(dict(exchange=m['exchange'], ric=r['ric'], data_type=r['data_type'],
                              source_file=str(f), target_relative_path=relative,
                              target_address=DESTINATION+'/'+relative, bytes=f.stat().st_size,
                              mapping_status=m['mapping_status'],plan_status='superseded_legacy_layout',
                              current_path_template=f"lseg/{m['exchange'].lower()}/2026/{quote(r['ric'], safe='_-.^')}/<YYYYMMDD>/{r['data_type']}.parquet"))
    plan = pd.DataFrame(paths)
    assert plan.source_file.is_unique and plan.target_relative_path.is_unique
    save_frame(out, 'tas_2026_file_plan', plan)

    sys.path.insert(0, str(ROOT/'artifacts/python_deps'))
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    wb = Workbook()
    wb.remove(wb.active)
    field_labels = {
        'asset_class':'资产源', 'exchange':'交易所目录', 'exchange_name_cn':'交易所名称',
        'exchange_group':'集团参考', 'ric':'原始RIC', 'routed_product':'原路由根',
        'reference_roots':'HRD参考品种根', 'instrument_shape':'工具类型标记',
        'mapping_status':'分组状态', 'mapping_method':'分组依据代码',
        'reference_mics':'原始MIC', 'reference_exchange_codes':'LSEG交易所代码',
        'candidate_exchanges':'存在冲突的交易所候选', 'source_layers':'数据类型',
        'source_jobs':'源作业数', 'first_source_utc':'源最早UTC', 'last_source_utc':'源最晚UTC',
        'source_years':'源周期年份', 'jobs_with_local_data':'本机有Data的源作业数',
        'reference_description':'HRD工具描述', 'note':'说明', 'source_job':'源作业名称',
        'source_layer':'数据类型', 'source_period':'源周期', 'report_path':'源清单路径',
        'report_sha256':'源清单SHA256', 'local_data_files':'本机Data文件数',
        'local_compressed_bytes':'本机Data压缩字节数', 'raw_file_presence':'原始包位置状态',
        'report_rows':'清单RIC行数', 'active_ric_rows':'活跃RIC行数', 'source_count':'本视图源Count',
        'data_type':'成交或报价', 'source_file':'本机源文件', 'target_relative_path':'旧布局计划路径已失效',
        'target_address':'旧布局地址不适用', 'bytes':'源文件字节数',
        'plan_status':'历史计划状态','current_path_template':'当前按UTC日交付路径模板',
    }
    sheets = [
        ('交易所汇总', summary, {'exchange':'交易所目录', 'exchange_name_cn':'交易所名称', 'exchange_group':'集团参考',
            'ric_count':'RIC归属条目数', 'futures_ric_count':'期货源RIC条目数', 'equity_ric_count':'美股RIC条目数',
            'routed_root_count':'路由根数非标准品种数', 'rics_with_local_data':'所在源包本机有Data的RIC数',
            'source_layers':'数据类型', 'first_source_utc':'源最早UTC', 'last_source_utc':'源最晚UTC', 'example_roots':'路由根列表'}),
        ('期货路由根分组', products[products.asset_class=='futures'], {'asset_class':'资产源', 'exchange':'交易所目录',
            'routed_product':'原路由根', 'ric_count':'RIC数', 'example_ric':'示例RIC', 'reference_roots':'参考品种根',
            'source_layers':'数据类型', 'first_source_utc':'源最早UTC', 'last_source_utc':'源最晚UTC', 'mapping_status':'分组依据状态'}),
        ('美股RIC分组', catalog[catalog.asset_class=='equity'], {}),
        ('待核实RIC', catalog[catalog.exchange=='REVIEW'], {}),
        ('全部RIC索引', catalog, {}),
        ('源包覆盖', job_frame, {}),
        ('2026文件落盘计划', plan, {}),
    ]
    for title, data, labels in sheets:
        ws = wb.create_sheet(title)
        ws.append([labels.get(k,field_labels.get(k,k)) for k in data.columns])
        for row in data.itertuples(index=False, name=None):
            ws.append(list(row))
        ws.freeze_panes='C2'
        ws.auto_filter.ref=ws.dimensions
        for cell in ws[1]:
            cell.font=Font(color='FFFFFF', bold=True)
            cell.fill=PatternFill('solid', fgColor='17365D')
        for i in range(1, len(data.columns)+1):
            ws.column_dimensions[get_column_letter(i)].width=24
    notes = wb.create_sheet('分组口径')
    for row in [
        ['项目', '说明'],
        ['范围', '本机及preprocess已保存的原批次/补充批次/美股Report，按源作业去重。包含仅远端清单，无实时采购全量核查。'],
        ['数据分层', 'TAS、Normalized LL2、Raw LL2、Raw、MBP、Summary分别保留；同RIC跨视图重复出现不等于重复成交。'],
        ['MIC参考', '使用实际HRD同RIC参考记录；ISO MIC表仅用于市场代码释义，不决定某RIC属于何交易所。'],
        ['历史归并', 'ICE旧MIC、LIFFE、KCBT等按目录家族归并，原始MIC完整保留；集团字段是目录参考，不追溯每个历史日期所有权。'],
        ['COMEX', 'COMEX与NYMEX分目录；前导1等RIC仅在COMEX专用包可按包归类，不与复合RIC合并。'],
        ['历史RIC', '一位年份RIC可跨十年复用；同RIC所冲突进REVIEW；源日期和job字段保留，不合并具体合约身份。'],
        ['美股', '按既有发布venue或RIC后缀所属市场分组，非每笔成交场所；RAW可能含跨场所成交与NBBO。'],
        ['指数', '原始指数单列INDICES；连续报价、累计总量保留原RIC并标类型，不解释成普通到期成交。'],
        ['统计', 'RIC条目按资产源+交易所+原始RIC计；同RIC跨来源存在冲突时可在REVIEW和明确源包目录各占一条。'],
        ['Count', '源Count是供应商各视图计数，单位不同，不跨数据类型累加为成交笔数；清单不证明授权或文件完整。'],
        ['落盘目标', DESTINATION],
        ['输出路径标准', 'lseg/<交易所小写>/<年份>/<原始RIC转义>/<UTC年月日>/trade.parquet或quote.parquet；直接只读扫描RocksDB导出。'],
        ['历史文件计划', '文件计划表的一对一旧目标路径已经失效，仅保留本机旧文件来源；当前按日输出不能用移动或重命名旧part代替。'],
        ['当前动作', '仅生成分组索引、中文表与文件落盘计划；行情数据未迁移，未访问目标服务器。'],
        ['研究数据', 'baseline/HFQ/因子属于已有派生产物，不以源Report Count重复统计；不把Databento Nasdaq数据冒充LSEG。'],
    ]:
        notes.append(row)
    notes.column_dimensions['A'].width=24
    notes.column_dimensions['B'].width=125
    for row in notes:
        for cell in row:
            cell.alignment=Alignment(wrap_text=True, vertical='top')
    report = ROOT/'reports/LSEG数据按交易所分组_20261109.xlsx'
    wb.save(report)
    check = load_workbook(report, read_only=True, data_only=True)
    assert check['全部RIC索引'].max_row==len(catalog)+1
    assert check['交易所汇总'].max_row==len(summary)+1
    assert int(job_frame.active_ric_rows.sum())==len(frame)
    assert int(job_frame.source_count.sum())==int(frame.source_count.sum())
    assert int(summary.ric_count.sum())==len(catalog)
    assert not frame.exchange.isna().any()
    assert len(plan)==3493 and int(plan.bytes.sum())==53218483628
    inputs = {str(p): digest(p) for p in [*ref_paths, stock_path, iso, inventory_path]}
    input_reports = {r['report_path']: r['report_sha256'] for r in jobs}
    assert all(digest(Path(path))==sha for path, sha in input_reports.items())
    dump(out/'duplicate_reports.json', duplicate_reports)
    dump(out/'completion.json', dict(status='grouped_catalog_verified', created_at=datetime.now(timezone.utc).isoformat(),
        scope='Existing local and saved remote LSEG metadata; no live remote-universe discovery; no market-data migration.',
        source_jobs=len(jobs), active_source_ric_job_rows=len(frame),
        unique_asset_ric_pairs=len(keys), ric_exchange_entries=len(catalog),
        exchange_directories=summary.exchange.tolist(),
        mapping_methods=dict(Counter(frame.mapping_method)),
        review_ric_entries=int((catalog.exchange=='REVIEW').sum()),
        existing_tas_2026_files=len(plan), existing_tas_2026_bytes=int(plan.bytes.sum()),
        remote_target=DESTINATION, data_migrated=False, workbook=str(report), workbook_sha256=digest(report),
        inputs_sha256=inputs, report_sha256=input_reports, script_sha256=digest(Path(__file__))))
    print(summary[['exchange', 'ric_count', 'futures_ric_count', 'equity_ric_count']].to_string(index=False))
    print('Workbook:', report, flush=True)


if __name__=='__main__':
    main()
