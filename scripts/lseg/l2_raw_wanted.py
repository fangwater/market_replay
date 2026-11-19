#!/usr/bin/env python3
"""RICs to wash from a Raw LL2 package: those with Raw data that the Normalized LL2 delivery lacks.

usage: l2_raw_wanted.py --year 2024 --report <merged-Report.csv.gz> --out <ric list>

wanted(Y) = Raw-active RICs (Report Count > 0) that have TAS directories in UTC year Y (catalog
`directories`) and either no Normalized L2 directory for Y (`l2_directories`) or fewer L2 days than TAS
days (`l2_gaps.sqlite l2_unfillable`, RIC rows). Raw is a different source: a RIC that Normalized covers
completely is not washed from Raw. Opens the catalogs read-only.
"""
import argparse, csv, gzip, sqlite3, sys, io

ap = argparse.ArgumentParser()
ap.add_argument("--year", type=int, required=True)
ap.add_argument("--report", required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--catalog", default="/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite")
ap.add_argument("--gaps", default="/mnt/nfs/30.100_cta_data/lseg/meta/l2_gaps.sqlite")
a = ap.parse_args()

ro = lambda p: sqlite3.connect("file:%s?mode=ro&immutable=1" % p, uri=True)
cat, gaps = ro(a.catalog), ro(a.gaps)
tas = {r[0] for r in cat.execute("select ric from directories where utc_year=?", (a.year,))}
l2 = {r[0] for r in cat.execute("select ric from l2_directories where utc_year=?", (a.year,))}
partial = {r[0] for r in gaps.execute(
    "select ric from l2_unfillable where utc_year=? and ric is not null and l2_days < tas_days", (a.year,))}

op = gzip.open if a.report.endswith(".gz") else open
with op(a.report, "rt", encoding="utf-8-sig") as f:
    raw = {r["#RIC"] for r in csv.DictReader(f) if int(r.get("Count") or 0) > 0}

wanted = sorted(r for r in raw & tas if r not in l2 or r in partial)
with open(a.out, "w") as f:
    f.write("# Raw LL2 RICs for UTC year %d: Raw-active, TAS present, Normalized L2 absent or partial\n" % a.year)
    f.write("".join(r + "\n" for r in wanted))
print("year %d: raw active %d, with TAS %d, wanted %d (no L2 %d, partial %d)" % (
    a.year, len(raw), len(raw & tas), len(wanted),
    sum(1 for r in wanted if r not in l2), sum(1 for r in wanted if r in l2)))
