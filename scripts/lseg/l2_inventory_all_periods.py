#!/usr/bin/env python3
"""Inventory of every Normalized LL2 package (main + remote supplements) per year, against TAS.

For each UTC year: the main package (local Report), the supplement packages (remote Reports), the RICs with
Count > 0, which supplement RICs are not in the main package, and which products have TAS data in that year
but no L2 in any package (the catalog's `contracts` gives TAS first/last trade UTC per RIC).
Read-only; writes only the JSON given with --out.
usage: l2_inventory_all_periods.py --out inventory_all_periods.json [--from 2010 --to 2026]
"""
import argparse, collections, csv, gzip, io, json, os, re, sqlite3, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--from", dest="y0", type=int, default=2010)
ap.add_argument("--to", dest="y1", type=int, default=2026)
a = ap.parse_args()

LOCAL = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalisedLL2"
REMOTE = "http://10.61.20.12/public/users/u158/public/lseg_data/future_supplement/normalisedLL2/"
CAT = "file:/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite?mode=ro&immutable=1"
op = urllib.request.build_opener(urllib.request.ProxyHandler({}))

con = sqlite3.connect(CAT, uri=True)
ric2prod = {}
tas_years = collections.defaultdict(set)  # year -> products with TAS trades in that UTC year
for product, ric, ft, lt in con.execute("select product, ric, first_trade_utc, last_trade_utc from contracts"):
    ric2prod.setdefault(ric, product)
    if ft and lt:
        for y in range(int(ft[:4]), int(lt[:4]) + 1):
            tas_years[y].add(product)


def report_rows(raw):
    return list(csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(raw)), encoding="utf-8-sig")))


def with_data(rows):
    return {r["#RIC"] for r in rows if int(r.get("Count") or 0) > 0}


def period_year(name):
    m = re.search(r"_ll2_(\d{4})-(\d\d)-(\d\d)_(\d{4})-", name)
    return int(m.group(1)) if m else None


def gb_of_dir(path):
    return sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path) if f.startswith("merged-Data")) / 1e9


main = {}
for d in sorted(os.listdir(LOCAL)):
    y = period_year(d)
    p = os.path.join(LOCAL, d)
    if y is None or not os.path.isdir(p) or not os.path.exists(os.path.join(p, "merged-Report.csv.gz")):
        continue
    rows = report_rows(open(os.path.join(p, "merged-Report.csv.gz"), "rb").read())
    key = ("supplement-71" if "_futures_" not in d else "main", y)
    main[(d, y)] = {"job": d, "year": y, "gb": round(gb_of_dir(p), 1), "rics_in_report": len(rows), "rics": with_data(rows),
                    "kind": "main" if "_futures_market_depth" in d else "shanghai-supplement"}

supp = []
h = op.open(REMOTE, timeout=60).read().decode("utf-8", "replace")
for d in sorted(x for x in re.findall(r'href="([^"?]+/)"', h) if x not in ("../", "/")):
    y = period_year(d)
    if y is None:
        continue
    try:
        raw = op.open(REMOTE + d + "merged-Report.csv.gz", timeout=60).read()
        h2 = op.open(REMOTE + d, timeout=60).read().decode("utf-8", "replace")
    except Exception as e:
        supp.append({"job": d, "year": y, "error": str(e)[:80]})
        continue
    gb = 0.0
    for line in h2.splitlines():
        m = re.search(r'href="merged-Data[^"]*">[^<]*</a>\s+\S+ \S+\s+([\d.]+)([KMGT])', line)
        if m:
            gb += float(m.group(1)) * {"K": 1e-6, "M": 1e-3, "G": 1, "T": 1e3}[m.group(2)]
    rows = report_rows(raw)
    supp.append({"job": d, "year": y, "gb": round(gb, 1), "rics_in_report": len(rows), "rics": with_data(rows)})

years = {}
for y in range(a.y0, a.y1 + 1):
    mains = [v for (d, yy), v in main.items() if yy == y]
    main_rics = set().union(*[v["rics"] for v in mains if v["kind"] == "main"]) if mains else set()
    all_main_rics = set().union(*[v["rics"] for v in mains]) if mains else set()
    sup = [s for s in supp if s["year"] == y and "rics" in s]
    covered = set(all_main_rics)
    pk = []
    for v in mains:
        pk.append({"job": v["job"], "kind": v["kind"], "gb": v["gb"], "rics_with_data": len(v["rics"]),
                   "products": len({ric2prod.get(r, "?") for r in v["rics"]})})
    for s in sup:
        new = s["rics"] - all_main_rics
        covered |= s["rics"]
        pk.append({"job": s["job"], "kind": "supplement", "gb": s["gb"], "rics_with_data": len(s["rics"]),
                   "new_vs_main": len(new), "new_products": sorted({ric2prod.get(r, "?") for r in new} - {"?"})[:40],
                   "unmapped_new_rics": sum(1 for r in new if r not in ric2prod)})
    l2_products = {ric2prod[r] for r in covered if r in ric2prod}
    tas = tas_years.get(y, set())
    years[y] = {"packages": pk, "rics_with_data_total": len(covered), "l2_products_mapped": len(l2_products),
                "tas_products": len(tas), "tas_products_without_l2": len(tas - l2_products),
                "tas_without_l2_sample": sorted(tas - l2_products)[:60]}
json.dump(years, open(a.out, "w"), indent=1, ensure_ascii=False)
print("year  main_GB  main_RICs  suppl(GB/new RICs)           L2 products  TAS products  TAS-without-L2")
for y, v in years.items():
    m = [p for p in v["packages"] if p["kind"] == "main"]
    s = [p for p in v["packages"] if p["kind"] != "main"]
    print("%d  %7.0f  %9d  %-28s %11d  %12d  %14d" % (
        y, sum(p["gb"] for p in m), sum(p["rics_with_data"] for p in m),
        ", ".join("%.0fG/%d" % (p["gb"], p.get("new_vs_main", 0)) for p in s) or "-",
        v["l2_products_mapped"], v["tas_products"], v["tas_products_without_l2"]))
