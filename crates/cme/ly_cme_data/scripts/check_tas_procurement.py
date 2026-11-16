#!/usr/bin/env python3
"""Reconcile the 19 missing IB products against the saved official purchase attachments."""
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from make_futures_checklist_excel import PRODUCTS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/tas_procurement_check_20261103'
BASE = 'http://10.61.20.64:5201'
NEARBY = {
    'AH': ('ALI', '采购的是COMEX铝；不是LME原铝。'),
    'BZ': ('BRN', '采购的是ICE布伦特（LCO），不是NYMEX Brent Last Day。'),
    'CA': ('HG', '采购的是COMEX铜；不是LME铜，也未列家乐福单股期货。'),
    'M6E': ('6E', '采购的是标准Euro FX；未列微型欧元。'),
    'MES': ('ES', '采购的是E-mini标普500；未列Micro E-mini。'),
    'MGC': ('GC', '采购的是标准黄金；未列微型黄金。'),
    'MNK': ('NKD', '采购的是标准美元日经；未列微型美元日经。'),
    'MNQ': ('NQ', '采购的是E-mini纳斯达克100；未列Micro E-mini。'),
    'MYM': ('YM', '采购的是E-mini道琼斯；未列Micro E-mini。'),
    'NI': ('', '采购附件未列LME镍，未发现可对应的采购产品。'),
    'PB': ('', '采购附件未列LME铅，未发现可对应的采购产品。'),
    'QC': ('HG', '采购的是标准铜；未列12,500磅迷你铜。'),
    'QG': ('NG', '采购的是标准天然气；未列2,500 MMBtu迷你天然气。'),
    'QO': ('GC', '采购的是100盎司黄金；未列50盎司迷你黄金。'),
    'SIC': ('SI', '采购的是5,000盎司白银；未列100盎司白银。'),
    'TFM': ('TFM', 'TFM明确列入采购；供应商映射0#TFUBM:。HRD为IFEU、USD、10,000 MMBTU，IB为ENDEX、EUR/MWh，需核对映射和交付。'),
    'UC': ('6H', '采购的是CME CNH/USD；不是SGX USD/CNH。'),
    'XIN0I': ('CN', '采购的是SGX富时中国A50；不是中国H50。'),
    'ZSLME': ('', '采购附件未列LME锌，未发现可对应的采购产品。'),
}

def main():
    with (OUT / 'symbols-global-survey-liuyang.csv').open(newline='') as f:
        survey = list(csv.DictReader(f))
    mapping_sheet = json.loads((OUT / 'vendor_mapping_rows.json').read_text())['RIC LIST']
    header, rows = mapping_sheet[0], mapping_sheet[1:]
    mapping = [dict(zip(header, row)) for row in rows]
    s = {row['ticker']: row for row in survey}
    m = {row['ticker']: row for row in mapping}
    with (ROOT / 'artifacts/ib_tas_all_futures_20261103/missing_tas_exchange_groups.csv').open(newline='') as f:
        missing = list(csv.DictReader(f))
    assert len(missing) == 19 and {r['ib_symbol'] for r in missing} == set(NEARBY)
    results = []
    for row in missing:
        ib = row['ib_symbol']
        nearby, note = NEARBY[ib]
        direct_s = ib in s
        direct_m = ib in m
        assert direct_s == direct_m
        if nearby:
            assert nearby in s and nearby in m
        results.append(dict(
            ib_symbol=ib, chinese_name=PRODUCTS[ib][0], ib_exchanges=row['ib_exchanges'],
            listed_same_product=direct_s,
            procurement_status='采购已列入，交付规格待核查' if direct_s else '采购清单未列入同规格产品',
            nearby_purchase_ticker=nearby,
            nearby_purchase_name=s[nearby]['CN name'] if nearby else '',
            nearby_purchase_exchange=s[nearby]['exchange'] if nearby else '',
            nearby_vendor_chain=(m[nearby]['Chain RIC'] or '').strip() if nearby else '',
            note=note))
    purchased = [r['ib_symbol'] for r in results if r['listed_same_product']]
    assert purchased == ['TFM']
    assert len(survey) == 121 and len(mapping) == 120
    assert set(s) - set(m) == {'AFR'} and not (set(m) - set(s))
    refs = ROOT / 'artifacts/ib_tas_all_futures_20261103'
    hrd = json.loads((refs / 'raw_hrd_identity_witnesses.json').read_text())['TFUBMF0']
    assert {(r['Market MIC'], r['Currency Code'], r['Lot Size'], r['Lot Units']) for r in hrd} == {('IFEU', 'USD', '10,000', 'MMBTU')}
    contracts = json.loads((refs / 'ib_futures_snapshot.json').read_text())['contracts']
    ib_tfm = [r for r in contracts if r['symbol'] == 'TFM']
    assert {(r['exchange'], r['currency']) for r in ib_tfm} == {('ENDEX', 'EUR')}
    inputs = ['dataset_page.html', 'attachment_27_preview.html', 'symbols-global-survey-liuyang.csv',
              'Futues_List_RIC_Mapping.xlsx', 'vendor_mapping_rows.json']
    result = dict(status='verified_against_published_procurement_attachments',
                  checked_at=datetime.now(timezone.utc).isoformat(), dataset_url=BASE+'/market/dataset/lseg_waipan_data',
                  purchase_list_url=BASE+'/datasets/attachments/27/preview',
                  vendor_mapping_url=BASE+'/datasets/attachments/37/preview',
                  missing_ib_codes=19, absent_from_published_purchase_list=18,
                  purchased_but_mapping_or_spec_mismatch=['TFM'],
                  advertised_purchase_rows=120, actual_survey_rows=121, vendor_mapping_rows=120,
                  survey_only_ticker='AFR',
                  scope='Published purchase scope, not an invoice/payment audit; candidates checked by venue and specification, not ticker equality alone.',
                  inputs={name:hashlib.sha256((OUT/name).read_bytes()).hexdigest() for name in inputs},
                  tfm_evidence={'purchase_row':s['TFM'], 'vendor_mapping_row':m['TFM'],
                                'hrd_versions':hrd, 'ib_metadata_examples':ib_tfm[:3]}, results=results)
    (OUT/'summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    with (OUT/'missing19_procurement.csv').open('w',newline='',encoding='utf-8-sig') as f:
        writer=csv.DictWriter(f,fieldnames=list(results[0])); writer.writeheader(); writer.writerows(results)
    print(json.dumps({k:v for k,v in result.items() if k not in ('inputs','results','tfm_evidence')},ensure_ascii=False,indent=2))

if __name__ == '__main__':
    main()
