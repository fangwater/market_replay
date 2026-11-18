#!/usr/bin/env python3
"""Per product coverage of the delivered L2 (catalog l2_directories), written as JSON.

usage: l2_coverage.py --out coverage.json [--years 2024,2025,2026]

For every product with TAS or L2 in the window: first/last day with L2, and the UTC months of the window that no
delivered RIC's [first_day, last_day] range touches ('months_without_l2'; a range is an upper bound of the days with
data, so a month listed here certainly has no L2, a month not listed may still have days missing). Products with TAS
but no L2 at all are listed under 'tas_without_l2'. Read-only.
"""
import argparse, collections, json, sqlite3

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--years", default="2024,2025,2026")
a = ap.parse_args()
years = [int(y) for y in a.years.split(",")]
c = sqlite3.connect("file:/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite?mode=ro&immutable=1", uri=True)
prod = {}
for ric, p in c.execute("select ric, product from contracts"):
    prod.setdefault(ric, p)
months = [(y, m) for y in years for m in range(1, 13) if (y, m) <= (2026, 11)]
rng = collections.defaultdict(list)
for ric, y, fd, ld in c.execute("select ric, utc_year, first_day, last_day from l2_directories where utc_year in (%s)" % a.years):
    rng[prod.get(ric, "?")].append((fd, ld))
tas = collections.defaultdict(set)
for p, y in c.execute("select product, utc_year from directories where utc_year in (%s)" % a.years):
    tas[p].add(y)
out = {"years": years, "products": {}, "tas_without_l2": {}}
for p in sorted(set(rng) | set(tas)):
    r = rng.get(p, [])
    if not r:
        out["tas_without_l2"][p] = sorted(tas[p])
        continue
    cov = set()
    for fd, ld in r:
        y, m = int(fd[:4]), int(fd[4:6])
        while (y, m) <= (int(ld[:4]), int(ld[4:6])):
            cov.add((y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    first_tas = min(tas[p]) if tas.get(p) else None
    out["products"][p] = {"first_day": min(fd for fd, _ in r), "last_day": max(ld for _, ld in r), "rics": len(r),
                          "months_without_l2": ["%d-%02d" % ym for ym in months if ym not in cov and (first_tas is None or ym[0] >= first_tas)]}
json.dump(out, open(a.out, "w"), indent=1, ensure_ascii=False)
print("products with L2", len(out["products"]), "TAS without any L2", len(out["tas_without_l2"]))
