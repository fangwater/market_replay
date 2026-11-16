#!/usr/bin/env python3
"""Match all actual IB futures symbols to raw TAS, independently of processed baselines."""
import argparse
import csv
import gzip
import hashlib
import io
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
sys.path.insert(0, str(ROOT / "artifacts/python_deps"))
import pymysql

TAS_ROOT = Path("/mnt/hdd-raid5-72t/liang_torch/lseg_data")
HRD = Path("/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_causal_roll_inputs_20261023/hrd.csv")
# These source-root mappings were checked against actual HRD venue, description,
# quote currency / units and ticker. The purchase spreadsheet alone is insufficient.
CURRENT = {
    "BRR": "BTC", "CC": "CC", "CL": "CL", "COIL": "LCO", "CT": "CT",
    "DAX": "FDX", "DX": "DX", "ECF": "CFI2", "ES": "ES", "GBL": "FGBL",
    "GBM": "FGBM", "GBS": "FGBS", "GC": "GC", "GF": "FC", "HE": "LH",
    "HG": "HG", "HO": "HO", "JGB": "JGB", "KC": "KC", "LE": "LC",
    "NG": "NG", "NQ": "NQ", "PA": "PA", "PL": "PL", "RB": "RB", "RS": "RS",
    "SB": "SB", "SCI": "SZZF", "SI": "SI", "VIX": "VX", "W": "LSU",
    "XINA50": "SFC", "YM": "YM", "ZC": "C", "ZF": "FV", "ZL": "BO",
    "ZM": "SM", "ZN": "TY", "ZS": "S", "ZT": "TU", "ZW": "W",
}
SUPPLEMENT = {"GOIL": "LGO", "NKD": "NK", "TSR20": "STF"}
NOTES = {
    "DAX": "Only standard FDAX / multiplier 25 confirmed; IB also has mini/micro variants under DAX.",
    "SI": "Standard SI / multiplier 5000 confirmed; IB also has SIL / multiplier 1000 under SI.",
    "VIX": "VX monthly is explicitly confirmed; weekly VXnn contracts require their own RIC matching.",
    "GOIL": "Original G is GBP short gilt; supplement LGO is ICE USD gasoil, 100 tonnes.",
    "NKD": "Original JNI is OSE JPY Nikkei / multiplier 1000; supplement NK is CME USD Nikkei / multiplier 5.",
    "TSR20": "Original SINR is INE CNY NR rubber; supplement STF is SGX SICOM TSR20, 5000 kg quoted in US cents.",
    "TFM": "Original TFUBM is USD / 10000 MMBTU Dutch TTF; IB TFM is EUR / MWh. Different contracts.",
    "UC": "CNC is CME CNH; original CNHZ9 is KOFEX / KRW. Neither is IB SGX USD/CNH.",
    "BZ": "NYMEX Brent Last Day is not ICE Brent LCO / IB COIL.",
    "XIN0I": "SGX China H50 is not China A50 SFC, nor HKFE HCEI.",
    "W": "IB W is ICE white sugar -> LSU; TAS W is CBOT wheat -> IB ZW.",
}
REFERENCE_FIELDS = ("Market MIC", "Exchange Code", "Currency Code", "Lot Size", "Lot Units", "Security Description", "Ticker")


def csv_rows(path):
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Directory containing the saved supplemental Report and HRD evidence")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cfg = dict(line.split("=", 1) for line in IB_ENV.read_text().splitlines()
               if line and not line.startswith("#"))
    con = pymysql.connect(host=cfg["IB_MYSQL_HOST"], port=int(cfg["IB_MYSQL_PORT"]),
                          user=cfg["IB_MYSQL_USER"], password=cfg["IB_MYSQL_PASSWORD"],
                          database=cfg["IB_MYSQL_DATABASE"], connect_timeout=10, read_timeout=25,
                          autocommit=True, cursorclass=pymysql.cursors.DictCursor)
    try:
        with con.cursor() as cursor:
            cursor.execute("SET SESSION MAX_EXECUTION_TIME=20000")
            cursor.execute("START TRANSACTION READ ONLY")
            cursor.execute("SELECT DISTINCT `sec_type` FROM `minute_data` FORCE INDEX(idx_sec_type)")
            types = [r["sec_type"] for r in cursor.fetchall()]
            if {v.lower() for v in types} != {"fut"}:
                raise ValueError(f"unexpected security types: {types}")
            cursor.execute("SELECT DISTINCT `symbol` FROM `minute_data` FORCE INDEX(idx_symbol_utctime)")
            symbols = sorted(r["symbol"] for r in cursor.fetchall())
            cursor.execute("SELECT `con_id`,`symbol`,`exchange`,`multiplier`,`maturity_date`,"
                           "`instrument_type`,`local_symbol`,`currency`,`company_name` "
                           "FROM `contract_info` WHERE `instrument_type`='FUT' ORDER BY `id`")
            contracts = list(cursor.fetchall())
    finally:
        con.rollback()
        con.close()
    save_json(args.output_dir / "ib_futures_snapshot.json", dict(symbols=symbols, security_types=types, contracts=contracts))
    inventory_path = TAS_ROOT / "tas_tick_parquet/2026/_audit/ric_inventory.csv"
    inventory = csv_rows(inventory_path)
    tas = defaultdict(list)
    for row in inventory:
        tas[row["product_cf"].split(":", 2)[2]].append(row)
    by_symbol = defaultdict(list)
    for contract in contracts:
        by_symbol[contract["symbol"]].append(contract)

    # Preserve actual reference records for both matching and rejected lookalikes.
    candidates = {**CURRENT, **SUPPLEMENT}
    examples = {symbol: next(r["ric"] for r in tas[root] if r["data_type"] == "trade")
                for symbol, root in CURRENT.items()}
    examples.update(GOIL="LGOH6", NKD="NKH26", TSR20="STFF6")
    wanted = set(examples.values()) | {"GH6", "JNIH6", "SINRF6", "TFUBMF0", "CNHZ9"}
    references = defaultdict(list)
    for path in (HRD, args.output_dir / "supplement_hrd.csv"):
        with path.open(newline="") as file:
            for row in csv.DictReader(file):
                if row["RIC"] in wanted:
                    references[row["RIC"]].append({key: row[key] for key in
                                                   ("RIC", "Change Date", "Expiration Date", *REFERENCE_FIELDS)})
    with gzip.open(args.output_dir / "supplement_tas_2026_report.csv.gz", "rt", newline="") as file:
        supplement_report = list(csv.DictReader(file))
    results = []
    for symbol in symbols:
        ib = by_symbol[symbol]
        row = dict(ib_symbol=symbol, ib_metadata_contracts=len(ib),
                   ib_exchanges="|".join(sorted({r["exchange"] for r in ib})),
                   ib_names="|".join(sorted({r["company_name"] or "" for r in ib})),
                   tas_root=candidates.get(symbol, ""), status="not_found_same_instrument",
                   tas_example_ric="", tas_2026_trade_rics=0, tas_2026_quote_rics=0,
                   tas_2026_trade_rows=0, tas_2026_quote_rows=0,
                   supplement_report_rows=0, note=NOTES.get(symbol, ""))
        if symbol in CURRENT:
            root = CURRENT[symbol]
            if not tas[root] or not references[examples[symbol]]:
                raise ValueError(f"missing actual TAS/reference witness for {symbol}")
            row.update(status="present_current_tas", tas_example_ric=examples[symbol])
            for kind in ("trade", "quote"):
                values = [r for r in tas[root] if r["data_type"] == kind]
                row[f"tas_2026_{kind}_rics"] = len(values)
                row[f"tas_2026_{kind}_rows"] = sum(int(r["rows"]) for r in values)
        elif symbol in SUPPLEMENT:
            root = SUPPLEMENT[symbol]
            active = [r for r in supplement_report if int(r["Count"]) > 0
                      and re_full_root(r["#RIC"]) == root]
            if not active or not references[examples[symbol]]:
                raise ValueError(f"missing supplemental source/reference witness for {symbol}")
            if tas[root]:
                raise ValueError(f"supplement root unexpectedly already in current export: {root}")
            row.update(status="present_supplement_source_not_current_db", tas_example_ric=examples[symbol],
                       supplement_report_rows=sum(int(r["Count"]) for r in active))
        results.append(row)
    path = args.output_dir / "all_ib_futures_tas_matches.csv"
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)
    save_json(args.output_dir / "raw_hrd_identity_witnesses.json", references)
    inputs = [inventory_path, HRD, args.output_dir / "supplement_hrd.csv", args.output_dir / "supplement_tas_2026_report.csv.gz"]
    summary = dict(measured_at=datetime.now(timezone.utc).isoformat(), mysql_futures_symbols=len(symbols),
                   count_basis="distinct symbol with actual minute_data rows; all sec_type=fut",
                   status_counts=dict(Counter(r["status"] for r in results)),
                   symbols_by_status={status: [r["ib_symbol"] for r in results if r["status"] == status]
                                      for status in sorted({r["status"] for r in results})},
                   scope="raw all-product TAS; no preprocess processed-product allowlist",
                   current_tas_period="2026-01-01_2026-06-01",
                   supplement_period="2026-01-01_2026-05-30",
                   caution="Product presence is not full expiry/date/minute coverage; mixed-size IB symbols are not interchangeable.",
                   input_sha256={str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs})
    save_json(args.output_dir / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def re_full_root(ric):
    import re
    stem = ric.split("^", 1)[0]
    match = re.fullmatch(r"(.+)[FGHJKMNQUVXZ][0-9]+", stem)
    return match[1] if match else stem


if __name__ == "__main__":
    main()
