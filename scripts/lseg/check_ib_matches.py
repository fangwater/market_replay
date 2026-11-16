#!/usr/bin/env python3
"""Read-only IB connectivity and instrument/date matching against preprocess."""
import argparse
import csv
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lseg_paths import DATA as ROOT, SCRIPTS, CONFIG, IB_ENV, REPO
sys.path.insert(0, str(ROOT / "artifacts/python_deps"))
import pymysql

PREPROCESS = Path("/home/fanghaizhou/preprocess")
STOCK_ROOT = Path("/mnt/hdd-raid5-72t/liang_torch/usstock_data")
TAS_ROOT = Path("/mnt/hdd-raid5-72t/liang_torch/lseg_data")
MONTHS = "FGHJKMNQUVXZ"
# RIC root -> IB symbols, reference exchange, full-size multiplier.
FUTURES = {
    "ES": (["ES"], "CME", "50"), "NQ": (["NQ"], "CME", "20"),
    "RTY": (["RTY"], "CME", "50"), "YM": (["YM"], "CBOT", "5"),
    "GC": (["GC"], "COMEX", "100"), "CL": (["CL"], "NYMEX", "1000"),
    "URO": (["EUR", "6E"], "CME", "125000"), "JY": (["JPY", "6J"], "CME", "12500000"),
    "HG": (["HG"], "COMEX", "25000"), "SI": (["SI"], "COMEX", "5000"),
    "TU": (["ZT"], "CBOT", "2000"), "FV": (["ZF"], "CBOT", "1000"),
    "TY": (["ZN"], "CBOT", "1000"), "TN": (["TN"], "CBOT", "1000"),
    "US": (["ZB"], "CBOT", "1000"), "AUL": (["UB"], "CBOT", "1000"),
}


def read_csv(path):
    with path.open(newline="") as file:
        return list(csv.DictReader(file))


def write_csv(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def save_json(path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str) + "\n")


def naive_utc_ns(ns):
    return datetime.fromtimestamp(int(ns) / 1e9, timezone.utc).replace(tzinfo=None)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    cfg = dict(line.split("=", 1) for line in IB_ENV.read_text().splitlines()
               if line and not line.startswith("#"))
    conn = pymysql.connect(host=cfg["IB_MYSQL_HOST"], port=int(cfg["IB_MYSQL_PORT"]),
                           user=cfg["IB_MYSQL_USER"], password=cfg["IB_MYSQL_PASSWORD"],
                           database=cfg["IB_MYSQL_DATABASE"], connect_timeout=10, read_timeout=25,
                           write_timeout=10, autocommit=True, charset="utf8mb4",
                           cursorclass=pymysql.cursors.DictCursor)
    queries = []

    def query(sql, params=()):
        started = time.monotonic()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            rows = list(cursor.fetchall())
        queries.append(dict(sql=sql, params=params, rows=len(rows), seconds=time.monotonic() - started))
        return rows

    bounds_cache = {}

    def bounds(table, con_id):
        key = (table, con_id)
        if key not in bounds_cache:
            result = {}
            result["invalid_utc_examples"] = query(
                f"SELECT `symbol`,`con_id`,`time_cst`,`utc_time`,`timestamp` FROM `{table}` "
                "FORCE INDEX(uk_conid_utctime) WHERE `con_id`=%s AND `utc_time` < '1000-01-01' LIMIT 3", (con_id,))
            for label, direction in (("first", "ASC"), ("last", "DESC")):
                rows = query(f"SELECT `symbol`,`con_id`,`exchange`,`maturity_date`,`utc_time`,`timestamp`,`sec_type` "
                             f"FROM `{table}` FORCE INDEX(uk_conid_utctime) WHERE `con_id`=%s AND `utc_time` >= '1000-01-01' "
                             f"ORDER BY `utc_time` {direction} LIMIT 1", (con_id,))
                if rows and not isinstance(rows[0]["utc_time"], datetime):
                    raise TypeError("expected the quoted utc_time DATETIME column")
                result[label] = rows[0] if rows else None
            bounds_cache[key] = result
        return bounds_cache[key]

    def overlap(table, con_id, start, end):
        rows = query(f"SELECT `utc_time`,`timestamp` FROM `{table}` FORCE INDEX(uk_conid_utctime) "
                     "WHERE `con_id`=%s AND `utc_time` >= %s AND `utc_time` < %s ORDER BY `utc_time` LIMIT 1",
                     (con_id, start, end))
        return str(rows[0]["utc_time"]) if rows else ""

    try:
        query("SET SESSION MAX_EXECUTION_TIME=20000")
        query("START TRANSACTION READ ONLY")
        server = query("SELECT VERSION() AS version,DATABASE() AS database_name,CURRENT_USER() AS authenticated_user,"
                       "@@session.time_zone AS session_time_zone,@@system_time_zone AS system_time_zone")[0]
        contracts = query("SELECT con_id,symbol,exchange,multiplier,maturity_date,instrument_type,local_symbol,currency,company_name "
                          "FROM contract_info ORDER BY id")
        symbols = {table: {row["symbol"] for row in query(f"SELECT DISTINCT symbol FROM `{table}` FORCE INDEX(idx_symbol_utctime)")}
                   for table in ("minute_data", "minute_data_stock")}
        save_json(args.output_dir / "instruments_snapshot.json", dict(measured_at=datetime.now(timezone.utc).isoformat(),
                  contracts=contracts, minute_data_symbols=sorted(symbols["minute_data"]),
                  minute_data_stock_symbols=sorted(symbols["minute_data_stock"])))
        by_symbol = defaultdict(list)
        for contract in contracts:
            by_symbol[(contract["instrument_type"], contract["symbol"])].append(contract)
        raw = read_csv(PREPROCESS / "database/usstock_multifrequency_20261026/source_coverage.csv")
        stock_inputs = [dict(cohort="raw_77", **row) for row in raw]
        nasdaq_root = STOCK_ROOT / "baseline_data_1min/NASDAQ"
        for path in sorted(nasdaq_root.iterdir()):
            if not path.is_dir():
                continue
            dates = sorted(p.stem for p in path.glob("*.parquet"))
            if dates:
                stock_inputs.append(dict(cohort="nasdaq_26", venue="NASDAQ", ric=path.name,
                                         first_session=dates[0], last_session=dates[-1], files=len(dates)))
        stocks = []
        for source in stock_inputs:
            ticker = {"BRKb.N": "BRK B"}.get(source["ric"], source["ric"].rsplit(".", 1)[0])
            row = dict(source, ib_symbol=ticker, status="missing_ib_symbol", con_id="", ib_company="", ib_exchange="",
                       first_ib_utc="", last_ib_utc="", first_overlap_ib_utc="", note="")
            candidates = by_symbol[("STK", ticker)]
            if ticker not in symbols["minute_data_stock"]:
                stocks.append(row)
                continue
            if len(candidates) != 1:
                row.update(status="requires_contract_metadata", note=f"contract_info candidate count={len(candidates)}")
                stocks.append(row)
                continue
            candidate = candidates[0]
            row.update(con_id=candidate["con_id"], ib_company=candidate["company_name"], ib_exchange=candidate["exchange"])
            b = bounds("minute_data_stock", candidate["con_id"])
            row.update(first_ib_utc=str(b["first"]["utc_time"]) if b["first"] else "",
                       last_ib_utc=str(b["last"]["utc_time"]) if b["last"] else "")
            if source["ric"] == "SPCX.O":
                row.update(status="identity_conflict", note="preprocess historical SPAC ETF; IB metadata identifies SpaceX Class A; do not join")
                stocks.append(row)
                continue
            venue = {"BZX": "BATS"}.get(source["venue"], source["venue"])
            if candidate["exchange"] != venue or candidate["currency"] != "USD":
                row.update(status="venue_or_currency_requires_review")
                stocks.append(row)
                continue
            start = datetime.strptime(source["first_session"], "%Y%m%d")
            end = datetime.strptime(source["last_session"], "%Y%m%d") + timedelta(days=1)
            witness = overlap("minute_data_stock", candidate["con_id"], start, end)
            row.update(status="matched_with_date_overlap" if witness else "matched_no_date_overlap",
                       first_overlap_ib_utc=witness)
            if source["ric"] == "BRKb.N":
                row["note"] = "Explicit share-class alias BRKb.N -> BRK B"
            stocks.append(row)
        write_csv(args.output_dir / "stock_ric_matches.csv", stocks)

        inventory = read_csv(TAS_ROOT / "tas_tick_parquet/2026/_audit/ric_inventory.csv")
        tas_rows = {(r["ric"], r["data_type"]): r for r in inventory}
        tas_rics = {r["ric"] for r in inventory}
        futures, products = [], []
        for root, (ib_symbols, exchange, multiplier) in FUTURES.items():
            candidates = [c for symbol in ib_symbols for c in by_symbol[("FUT", symbol)]]
            product = dict(ric_root=root, ib_symbols="|".join(ib_symbols), exchange=exchange,
                           expected_multiplier=multiplier, metadata_contracts=len(candidates),
                           matching_full_size_contracts=0, matched_tas_2026_contracts=0,
                           matched_trade_with_overlap=0, matched_quote_only_with_overlap=0,
                           status="missing_ib_product" if not candidates else "metadata_found")
            for contract in candidates:
                row = dict(ric_root=root, ib_symbol=contract["symbol"], con_id=contract["con_id"],
                           ib_local_symbol=contract["local_symbol"], ib_exchange=contract["exchange"],
                           ib_multiplier=contract["multiplier"], ib_expiry=contract["maturity_date"],
                           tas_ric="", status="requires_identity_review", first_ib_utc="", last_ib_utc="",
                           tas_has_trade=False, tas_has_quote=False, first_overlap_ib_utc="")
                local = re.fullmatch(rf"{re.escape(contract['symbol'])}([{MONTHS}])([0-9]{{1,2}})", contract["local_symbol"] or "")
                if (not local or contract["exchange"] != exchange or contract["currency"] != "USD"
                        or contract["multiplier"] != multiplier):
                    row["status"] = "different_size_or_local_symbol_or_venue"
                    futures.append(row)
                    continue
                product["matching_full_size_contracts"] += 1
                expiry_year = int(contract["maturity_date"][:4])
                years = [y for y in range(expiry_year - 1, expiry_year + 2) if str(y).endswith(local[2])]
                if len(years) != 1:
                    futures.append(row)
                    continue
                # Delivery month comes from local_symbol, not expiration month (CL is a counterexample).
                ric = f"{root}{local[1]}{years[0] % 100:02d}"
                row["tas_ric"] = ric
                b = bounds("minute_data", contract["con_id"])
                row.update(first_ib_utc=str(b["first"]["utc_time"]) if b["first"] else "",
                           last_ib_utc=str(b["last"]["utc_time"]) if b["last"] else "")
                if ric not in tas_rics:
                    row["status"] = "identity_resolved_no_tas_2026_records"
                    futures.append(row)
                    continue
                product["matched_tas_2026_contracts"] += 1
                row.update(tas_has_trade=(ric, "trade") in tas_rows, tas_has_quote=(ric, "quote") in tas_rows)
                records = tas_rows.get((ric, "trade"), tas_rows.get((ric, "quote")))
                start = naive_utc_ns(records["first_record_ts_utc_ns"]).replace(second=0, microsecond=0)
                end = naive_utc_ns(records["last_record_ts_utc_ns"]).replace(second=0, microsecond=0) + timedelta(minutes=1)
                witness = overlap("minute_data", contract["con_id"], start, end)
                row.update(first_overlap_ib_utc=witness,
                           status="matched_with_date_overlap" if witness else "matched_no_date_overlap")
                if witness:
                    product["matched_trade_with_overlap" if row["tas_has_trade"] else "matched_quote_only_with_overlap"] += 1
                futures.append(row)
            if product["matched_trade_with_overlap"]:
                product["status"] = "matched_trade_with_date_overlap"
            products.append(product)
        write_csv(args.output_dir / "futures_product_matches.csv", products)
        write_csv(args.output_dir / "futures_contract_matches.csv", futures)
        save_json(args.output_dir / "ib_indexed_date_bounds.json", [{"table": key[0], "con_id": key[1], **value}
                   for key, value in bounds_cache.items()])
        summary = dict(measured_at=datetime.now(timezone.utc).isoformat(), connection="authenticated_and_readable", server=server,
                       contract_info_rows=len(contracts), ib_futures_symbols=len(symbols["minute_data"]),
                       ib_stock_symbols=len(symbols["minute_data_stock"]),
                       stocks={cohort: dict(Counter(r["status"] for r in stocks if r["cohort"] == cohort))
                               for cohort in ("raw_77", "nasdaq_26")},
                       futures=dict(Counter(r["status"] for r in products)),
                       futures_contract_statuses=dict(Counter(r["status"] for r in futures)),
                       futures_tas_2026_matched_contracts=sum(r["matched_tas_2026_contracts"] for r in products),
                       futures_tas_2026_trade_contracts_with_overlap=sum(r["matched_trade_with_overlap"] for r in products),
                       invalid_utc_probe_contracts={table: sum(key[0] == table and bool(value["invalid_utc_examples"])
                                                           for key, value in bounds_cache.items())
                                                    for table in ("minute_data", "minute_data_stock")},
                       queries=len(queries), scope="instrument metadata and actual date overlap only; no OHLC quality comparison")
        save_json(args.output_dir / "match_summary.json", summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print("stock exceptions", [(r["cohort"], r["ric"], r["status"]) for r in stocks if r["status"] != "matched_with_date_overlap"])
        print("future products", [(r["ric_root"], r["status"], r["matched_trade_with_overlap"]) for r in products])
    finally:
        conn.rollback()
        conn.close()
        save_json(args.output_dir / "readonly_queries.json", queries)


if __name__ == "__main__":
    main()
