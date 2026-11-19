#!/usr/bin/env python3
"""Inventory of the remote LSEG normalisedLL2 supplement packages (read-only).

usage: l2_supplement_inventory.py --work <dir> [--periods 2024,2025,2026]

Lists http://10.61.20.12/public/users/u158/public/lseg_data/future_supplement/normalisedLL2/, downloads the
small merged-Report.csv.gz of every job (cached under <work>/reports), and for the selected periods says what
each package adds compared with the L2 data we hold: RICs with Count>0, RICs already delivered for the same
UTC year (catalog table l2_directories) or present in the main period package of that year (its own Report),
new RICs, new products and exchange mapping status. Writes <work>/inventory.json and prints a table.
"""
import argparse, collections, csv, gzip, io, json, os, re, sqlite3, urllib.request

BASE = "http://10.61.20.12/public/users/u158/public/lseg_data/future_supplement/normalisedLL2/"
CATALOG = "file:/mnt/nfs/30.100_cta_data/lseg/meta/catalog.sqlite?mode=ro&immutable=1"
MAIN = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalisedLL2/shanghai_evolution_futures_market_depth_ric_list_0_ll2_%s_%s/merged-Report.csv.gz"
PLAN = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/tas_status_20261109/plan.json"
RIC_CATALOG = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_exchange_groups_20261109/ric_catalog.csv"

ap = argparse.ArgumentParser()
ap.add_argument("--work", required=True)
ap.add_argument("--periods", default="2024,2025,2026")
a = ap.parse_args()
want = set(a.periods.split(","))
os.makedirs(os.path.join(a.work, "reports"), exist_ok=True)
op = urllib.request.build_opener(urllib.request.ProxyHandler({}))

listing = op.open(BASE, timeout=60).read().decode("utf-8", "replace")
dirs = sorted(x for x in re.findall(r'href="([^"?]+/)"', listing) if x not in ("../", "/"))
jobs = []
for d in dirs:
    html = op.open(BASE + d, timeout=60).read().decode("utf-8", "replace")
    files = {}
    for line in html.splitlines():
        m = re.search(r'href="([^"?]+)">[^<]*</a>\s+(\S+ \S+)\s+([\d.]+[KMGT]?)\s*$', line.strip())
        if m and not m.group(1).endswith("/"):
            files[m.group(1)] = {"modified": m.group(2), "size": m.group(3)}
    jobs.append({"dir": d, "files": files})
    rp = os.path.join(a.work, "reports", d.strip("/") + ".Report.csv.gz")
    if "merged-Report.csv.gz" in files and not os.path.exists(rp):
        with open(rp + ".tmp", "wb") as f:
            f.write(op.open(BASE + d + "merged-Report.csv.gz", timeout=120).read())
        os.replace(rp + ".tmp", rp)


def report(path):
    with gzip.open(path, "rt", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def ric_set(rows):
    return {r["#RIC"] for r in rows if int(r.get("Count") or 0) > 0}


con = sqlite3.connect(CATALOG, uri=True)
ric2prod = dict(con.execute("select ric, product from directories"))
ric2dir = {}
for ric, d in con.execute("select ric, directory from directories"):
    ric2dir.setdefault(ric, d.split("/lseg/")[1].split("/")[0])
delivered = collections.defaultdict(set)  # UTC year -> RICs with delivered L2
for ric, y in con.execute("select ric, utc_year from l2_directories"):
    delivered[int(y)].add(ric)
plan = json.load(open(PLAN))["exchanges"]
cat_ex = {}
with open(RIC_CATALOG, newline="", encoding="utf-8-sig") as f:
    for r in csv.DictReader(f):
        cat_ex[r["ric"]] = r["exchange"]

out = []
others = collections.Counter()
for j in jobs:
    d = j["dir"]
    m = re.search(r"ll2_(\d{4})-(\d\d-\d\d)_(\d{4})-(\d\d-\d\d)", d)
    year = m.group(1)
    fam = re.sub(r"_ll2_.*", "", d)
    if year not in want:
        others[(fam, year)] += 1
        continue
    rp = os.path.join(a.work, "reports", d.strip("/") + ".Report.csv.gz")
    rows = report(rp)
    rics = ric_set(rows)
    main_path = MAIN % ("%s-01-01" % year, "%d-01-01" % (int(year) + 1)) if year != "2026" else MAIN % ("2026-01-01", "2026-06-01")
    main_rows = {r["#RIC"]: r for r in report(main_path)} if os.path.exists(main_path) else {}
    main = {k for k, r in main_rows.items() if int(r.get("Count") or 0) > 0}
    dates = [r for r in rows if int(r.get("Count") or 0) > 0]
    first = min((r["Start"] for r in dates), default="")
    last = max((r["End"] for r in dates), default="")
    prods = collections.Counter(ric2prod.get(r) or "?" for r in rics)
    unmapped = sorted(r for r in rics if r not in ric2prod)
    in_main = rics & main
    same = more = fewer = wider = 0
    for r in rows:
        k = r["#RIC"]
        if k not in in_main:
            continue
        a_, b_ = int(r.get("Count") or 0), int(main_rows[k].get("Count") or 0)
        same += a_ == b_
        more += a_ > b_
        fewer += a_ < b_
        wider += (r["Start"] < main_rows[k]["Start"]) or (r["End"] > main_rows[k]["End"])
    in_deliv = rics & delivered.get(int(year), set())
    new = rics - main - delivered.get(int(year), set())
    mapped_ex = lambda r: ric2dir.get(r) or plan.get(r)
    os.makedirs(os.path.join(a.work, "only_rics"), exist_ok=True)
    with open(os.path.join(a.work, "only_rics", d.strip("/") + ".txt"), "w") as f:  # input of `lseg_l2_slice deliver --only-rics`
        f.write("".join(r + "\n" for r in sorted(new)))
    third = [r for r in new if not mapped_ex(r) and r in cat_ex]
    none = [r for r in new if not mapped_ex(r) and r not in cat_ex]
    data = [k for k in j["files"] if k.startswith("merged-Data")]
    out.append({
        "job": d.strip("/"), "family": fam, "year": year, "data_files": {k: j["files"][k]["size"] for k in data},
        "rics_in_report": len(rows), "rics_with_data": len(rics), "first": first[:10], "last": last[:10],
        "in_main_package": len(in_main), "overlap_same_count": same, "overlap_supplement_more_rows": more,
        "overlap_supplement_fewer_rows": fewer, "overlap_supplement_wider_range": wider, "already_delivered_same_year": len(in_deliv), "new_rics": len(new),
        "new_products": sorted({ric2prod.get(r) or "?" for r in new}),
        "rics_without_catalog_product": len(unmapped), "new_rics_mapped_catalog_or_plan": len(new) - len(third) - len(none),
        "new_rics_only_in_ric_catalog": len(third), "new_rics_in_no_source": len(none),
        "sample_new": sorted(new)[:6], "sample_in_no_source": sorted(none)[:6],
    })
json.dump({"jobs": out, "other_periods": {"%s %s" % k: v for k, v in others.items()}},
          open(os.path.join(a.work, "inventory.json"), "w"), indent=1)
for o in out:
    print("%-58s data=%s rics=%d new=%d inmain=%d deliv=%d prods=%s nosrc=%d" % (
        o["job"][:58], ",".join(o["data_files"].values()), o["rics_with_data"], o["new_rics"],
        o["in_main_package"], o["already_delivered_same_year"], ",".join(o["new_products"])[:60], o["new_rics_in_no_source"]))
for o in out:
    print("  %-46s %s in_main=%d same=%d supp_more=%d supp_fewer=%d wider=%d | new_only_in_ric_catalog=%d no_source=%d" % (
        o["family"][:30] + " " + o["year"], o["first"] + ".." + o["last"], o["in_main_package"], o["overlap_same_count"],
        o["overlap_supplement_more_rows"], o["overlap_supplement_fewer_rows"], o["overlap_supplement_wider_range"],
        o["new_rics_only_in_ric_catalog"], o["new_rics_in_no_source"]))
print("other periods (not downloaded):", dict(others))
