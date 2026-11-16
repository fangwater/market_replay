#!/usr/bin/env python3
"""Check completed export manifests and Parquet footers; build a per-RIC index."""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from parquet_delivery_checks import check_footer


def summarize(root):
    success = json.loads((root / "_SUCCESS.json").read_text())
    run = json.loads((root / "_audit/run.json").read_text())
    reports = [json.loads(p.read_text()) for p in (root / "_audit").glob("p%3A*.json")]
    if success["status"] != "complete" or run["sample"]:
        raise ValueError("not a complete production export")
    if sorted(r["cf"] for r in reports) != sorted(run["products"]):
        raise ValueError("source CF / completed CF sets differ")
    expected, totals, rics = set(), defaultdict(int), {}
    schemas = {}
    for report in reports:
        if not report["verified"]:
            raise ValueError("unverified CF")
        cf_totals = defaultdict(int)
        for part in report["parts"]:
            name = part["path"]
            if name in expected:
                raise ValueError(f"duplicate file: {name}")
            expected.add(name)
            path = root / name
            footer = check_footer(path,part)
            kind = "trade" if part["kind"] == 1 else "quote"
            fields = run[f"schema_{kind}"]
            if footer.schema_arrow.names != fields or footer.metadata.num_rows != part["rows"]:
                raise ValueError(f"footer mismatch: {name}")
            if path.stat().st_size != part["bytes"]:
                raise ValueError(f"size mismatch: {name}")
            if kind in schemas and not footer.schema_arrow.equals(schemas[kind], check_metadata=True):
                raise ValueError(f"inconsistent schema: {name}")
            schemas[kind] = footer.schema_arrow
            cf_totals[kind] += part["rows"]
            totals[kind] += part["rows"]
            key = (kind, part["ric"])
            if key not in rics:
                rics[key] = dict(data_type=kind, ric=part["ric"], product_cf=report["cf"],
                                 rows=0, files=0, bytes=0,
                                 first_record_ts_utc_ns=part["first"]["ts_utc_ns"],
                                 last_record_ts_utc_ns=part["last"]["ts_utc_ns"])
            if rics[key]["product_cf"] != report["cf"]:
                raise ValueError("same original RIC routed to different CFs")
            item = rics[key]
            item["rows"] += part["rows"]
            item["files"] += 1
            item["bytes"] += part["bytes"]
            item["first_record_ts_utc_ns"] = min(item["first_record_ts_utc_ns"], part["first"]["ts_utc_ns"])
            item["last_record_ts_utc_ns"] = max(item["last_record_ts_utc_ns"], part["last"]["ts_utc_ns"])
        if (cf_totals["trade"], cf_totals["quote"]) != (report["trade_rows"], report["quote_rows"]):
            raise ValueError(f"CF counts mismatch: {report['cf']}")
    if success.get("layout") == "ric_utc_day_v1":
        actual = {str(p.relative_to(root)) for p in (root / "lseg").rglob("*.parquet")}
        for report in reports:
            for part in report['parts']:
                path = Path(part['path'])
                kind = 'trade' if part['kind'] == 1 else 'quote'
                if len(path.parts) != 6 or path.parts[0] != 'lseg' or path.name != kind+'.parquet' or path.parts[4] != part.get('utc_date'):
                    raise ValueError(f'invalid RIC/day output path: {path}')
    else:
        actual = {str(p.relative_to(root)) for kind in ("trade", "quote") for p in (root / kind).rglob("*.parquet")}
    if actual != expected or list(root.rglob("*.partial")):
        raise ValueError("unlisted, missing, or unfinished files")
    if (totals["trade"], totals["quote"], len(expected)) != (success["trade_rows"], success["quote_rows"], success["files"]):
        raise ValueError("dataset totals mismatch")
    rows = [rics[key] for key in sorted(rics)]
    with (root / "_audit/ric_inventory.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    pq.write_table(pa.Table.from_pylist(rows), root / "_audit/ric_inventory.parquet", compression="zstd")
    result = dict(status="passed", product_cfs=len(reports),
                  layout=success.get('layout','legacy'),date_basis=success.get('date_basis','source order'),
                  trade_rics=sum(k[0] == "trade" for k in rics),
                  quote_rics=sum(k[0] == "quote" for k in rics),
                  distinct_rics=len({k[1] for k in rics}),
                  trade_rows=totals["trade"], quote_rows=totals["quote"],
                  files=len(expected), bytes=sum(r["bytes"] for r in rows),
                  source_period=success["source_period"],
                  source_manifest_unchanged=success["source_current"] == success["source_current_after"],
                  readback_mode=success.get('readback_mode','full'),
                  full_readback_files=success.get('full_readback_files',len(expected)),
                  exact_value_readback=("all output rows verified by Rust against source SHA256" if success.get('readback_mode','full')=='full' else "at most three files per kind/CF fully read back; all files have footer/schema/row-count/RIC/date checks"),
                  independent_footer_check="all files and CF totals checked by Python")
    (root / "_audit/delivery.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    summarize(parser.parse_args().root)
