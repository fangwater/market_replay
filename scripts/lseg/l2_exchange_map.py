#!/usr/bin/env python3
"""RIC -> delivery exchange directory map for lseg_l2_slice (read-only sources).

  l2_exchange_map.py <PRODUCT,PRODUCT,...|ALL> [--plan PLAN.json] [--conflicts OUT.tsv] > map.tsv

Lines `RIC<TAB>exchange-directory`. Sources, in this order:
  1. the delivery catalog `directories` table (a RIC is listed once TAS data was delivered for it);
  2. with --plan, the approved exchange plan of lseg-daily (`exchanges`: RIC -> delivery
     directory, e.g. /mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/tas_status_20261109/plan.json)
     for the RICs that are absent from the catalog (plan-only).
`review` in the plan or the purchase catalog means "unresolved" (docs/lseg/venue_review.md): it never
conflicts with a directory the delivery catalog knows; it is only used for a RIC the catalog lacks.
A RIC that both sources know with DIFFERENT directories is a conflict: it is left out of the map and
written to --conflicts (`RIC<TAB>detail`), so the deliver tool records it as a problem and does not
deliver it. A RIC in neither source has no line (the tool then reports it, never guesses).
The plan is keyed by RIC only (its `year` field names the 2026 TAS jobs it was made for, not a
property of the mapping), so it can resolve RICs of other years too, but it only lists RICs of
those jobs; RICs that never had TAS data and are not in the plan stay unmapped.
A RIC can sit in different exchange directories in different UTC years: one-digit RIC codes repeat every
ten years (e.g. CFI2Z0: ice_europe for the 2020 contract, ice_endex for the 2030 one; see
docs/lseg/venue_review.md). The map has one directory per RIC, so the directory of the RIC's latest UTC
year is used (daily runs deliver recent days), or with --year the directory of that UTC year when the
catalog has it. Two directories in the SAME UTC year is an error.
Counts of the groups go to stderr.
"""
import argparse
import json
import sqlite3
import sys

CATALOG = "file:/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite?mode=ro&immutable=1"

ap = argparse.ArgumentParser()
ap.add_argument("products")
ap.add_argument("--plan")
ap.add_argument("--conflicts")
ap.add_argument("--ric-catalog")
ap.add_argument("--third-out")
ap.add_argument("--year", type=int, help="prefer the catalog directory of this UTC year")
a = ap.parse_args()

want = None if a.products == "ALL" else set(a.products.split(","))
con = sqlite3.connect(CATALOG, uri=True)
by_year = {}
for product, ric, utc_year, directory in con.execute("select product, ric, utc_year, directory from directories"):
    if want is not None and product not in want:
        continue
    exchange = directory.split("/lseg/")[1].split("/")[0]
    if by_year.setdefault(ric, {}).setdefault(utc_year, exchange) != exchange:
        sys.exit(f"RIC {ric} is listed under two exchange directories in the catalog in {utc_year}")
seen = {}
multi_year = 0
for ric, years in by_year.items():
    if len(set(years.values())) > 1:
        multi_year += 1
    seen[ric] = years[a.year] if a.year in years else years[max(years)]

plan = {}
if a.plan:
    plan = json.load(open(a.plan))["exchanges"]

out = dict(seen)
plan_only = conflicts = 0
conflict_lines = {}
if want is None:
    for ric, ex in plan.items():
        if ric not in seen:
            out[ric] = ex
            plan_only += 1
        elif seen[ric] != ex and ex.lower() != "review":
            conflicts += 1
            conflict_lines[ric] = f"delivery catalog says {seen[ric]}, approved plan says {ex}"
            out.pop(ric, None)

third = {}
if a.ric_catalog and want is None:
    import csv

    with open(a.ric_catalog, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ric, ex = r["ric"], r["exchange"].strip().lower()
            if not ex:
                continue
            if ric in conflict_lines:
                continue
            if ric in out:
                if out[ric] != ex and ex != "review":
                    conflicts += 1
                    conflict_lines[ric] = f"delivery catalog/plan say {out[ric]}, purchase catalog says {ex}"
                    out.pop(ric, None)
            elif ric not in seen and ric not in plan:
                third[ric] = ex
                out[ric] = ex

for ric in sorted(out):
    print(f"{ric}\t{out[ric]}")
if a.third_out:
    with open(a.third_out, "w") as f:
        for ric in sorted(third):
            f.write(f"{ric}\t{third[ric]}\n")
if a.conflicts:
    with open(a.conflicts, "w") as f:
        for ric in sorted(conflict_lines):
            f.write(f"{ric}\t{conflict_lines[ric]}\n")
print(
    f"catalog RICs {len(seen)} ({multi_year} in several directories by year, latest year used), plan-only RICs {plan_only}, purchase-catalog-only RICs {len(third)}, conflicts {conflicts}, map lines {len(out)}",
    file=sys.stderr,
)
