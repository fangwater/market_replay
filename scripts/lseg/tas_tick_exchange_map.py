#!/usr/bin/env python3
"""Exchange map (raw RIC -> lowercase exchange directory) for `lseg_tas_parquet --layout ric-day` of one UTC year.

usage: tas_tick_exchange_map.py --year 2020 --out map_2020.json

Sources, first match wins (so the tick files land in the same RIC-year directory as kline / status):
  1. delivery catalog `directories` of that UTC year (the directory the RIC-year already has),
  2. any other year of the catalog (a RIC code keeps its exchange),
  3. the approved exchange plan of lseg-daily (tas_status_20261109/plan.json `exchanges`),
  4. the purchase catalog (ric_catalog.csv `exchange`, lowercased; REVIEW -> review).
A RIC in no source is missing from the map and the exporter stops on it (never guessed).
"""
import argparse, csv, json, sqlite3

ap = argparse.ArgumentParser()
ap.add_argument("--year", type=int, required=True)
ap.add_argument("--out", required=True)
ap.add_argument("--catalog", default="/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite")
ap.add_argument("--plan", default="/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/tas_status_20261109/plan.json")
ap.add_argument("--ric-catalog", default="/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_exchange_groups_20261109/ric_catalog.csv")
a = ap.parse_args()
cat = sqlite3.connect("file:%s?mode=ro&immutable=1" % a.catalog, uri=True)
out, src = {}, {1: 0, 2: 0, 3: 0, 4: 0}


def ex_of(directory):
    return directory.split("/lseg/")[1].split("/")[0]


for ric, d in cat.execute("select ric, directory from directories where utc_year=?", (a.year,)):
    out.setdefault(ric, ex_of(d)); src[1] += 1
for ric, d in cat.execute("select ric, directory from directories where utc_year<>? order by utc_year desc", (a.year,)):
    if ric not in out:
        out[ric] = ex_of(d); src[2] += 1
plan = json.load(open(a.plan)).get("exchanges", {})
for ric, ex in plan.items():
    if ric not in out:
        out[ric] = ex.lower(); src[3] += 1
with open(a.ric_catalog, encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        if r["ric"] not in out and r["exchange"]:
            out[r["ric"]] = r["exchange"].lower(); src[4] += 1
bad = [k for k, v in out.items() if not v or not all(c.islower() or c.isdigit() or c == "_" for c in v)]
for k in bad:
    del out[k]
json.dump(out, open(a.out, "w"))
print("year %d: %d RICs mapped (catalog this year %d, other year %d, plan %d, purchase catalog %d), %d with an invalid name dropped" % (
    a.year, len(out), src[1], src[2], src[3], src[4], len(bad)))
