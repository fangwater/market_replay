#!/usr/bin/env python3
"""Builds lseg/meta/l2_gaps.sqlite: RIC-years that have TAS data but no (or only partial) L2 250 ms data.

usage: l2_gaps_build.py --out <local.sqlite> --reports <dir>... [--packages-json f] [--problems tsv]
                        [--years 2024,2025,2026] [--threads 8] [--no-gate]
       l2_gaps_build.py --publish <local.sqlite> [--replace]

Inputs (all read-only):
  catalog.sqlite  `directories` (RIC-years that have TAS directories), `l2_directories` (what is delivered; run
                  `lseg_catalog l2-sync` first), `contracts` (product)
  <reports>       merged-Report.csv.gz of every L2 package we inspected (one file per package; the file name,
                  without .Report.csv.gz, is the package name); a *.Report.csv.gz that lists a RIC with Count 0
                  means the vendor package holds no data for it
  NFS walk        one listdir of every TAS RIC-year directory that has no L2 (bounded by --threads, default 8,
                  gated on the server job queue like the other jobs); for RIC-years that have L2 but fewer
                  L2 days than 0.8 * TAS days, each day directory is listed to count the days that really hold
                  trade.parquet or quote.parquet

tas_days   = day directories of the RIC-year (a day directory exists only when the delivery tool published at least
             one file for it); for 'partial' candidates recounted as day directories with trade.parquet or
             quote.parquet. TAS days after 2026-11-12 (the last L2 day) are not counted.
kind       none    = no L2 file for the RIC-year; partial = l2_days < 0.8 * tas_days
reason     not_in_any_package | report_count_zero | outside_package_window | unmapped_exchange | overlap_conflict
           | in_package_not_delivered (listed with data, covered, but nothing delivered: a defect to look at)
Publishing: a new file is hard-linked into place (never overwritten); --replace swaps an existing l2_gaps.sqlite
by an atomic rename, so a reader sees either the old or the new complete file.
"""
import argparse, collections, concurrent.futures as cf, csv, datetime, glob, gzip, json, os, sqlite3, sys, time, urllib.request

ROOT = "/mnt/nfs/30.100_cta_data/lseg"
CATALOG = "file:%s/meta/catalog.sqlite?mode=ro&immutable=1" % ROOT
LAST_L2_DAY = "20261112"
THRESHOLD = 0.8
SLACK_DAYS = 7

ap = argparse.ArgumentParser()
ap.add_argument("--out")
ap.add_argument("--reports", nargs="*", default=[])
ap.add_argument("--packages-json")
ap.add_argument("--inventory-json", help="l2_inventory_all_periods.json (per year: packages with GB, kind, RIC counts)")
ap.add_argument("--main-glob", default="/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalisedLL2/*/merged-Report.csv.gz")
ap.add_argument("--problems")
ap.add_argument("--years", default="2024,2025,2026")
ap.add_argument("--threads", type=int, default=8)
ap.add_argument("--ric-catalog", default="/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/artifacts/lseg_exchange_groups_20261109/ric_catalog.csv",
                help="purchase catalog (routed_product, source_layers per RIC): tells whether an L2 layer was ever ordered")
ap.add_argument("--no-gate", action="store_true")
ap.add_argument("--publish")
ap.add_argument("--replace", action="store_true")
a = ap.parse_args()

if a.publish:
    dst = ROOT + "/meta/l2_gaps.sqlite"
    con = sqlite3.connect("file:%s?mode=ro" % a.publish, uri=True)
    assert con.execute("pragma integrity_check").fetchone()[0] == "ok"
    n = con.execute("select count(*) from l2_unfillable").fetchone()[0]
    con.close()
    tmp = "%s.%d.tmp" % (dst, os.getpid())
    with open(a.publish, "rb") as f, open(tmp, "wb") as g:
        while True:
            b = f.read(1 << 24)
            if not b:
                break
            g.write(b)
        g.flush()
        os.fsync(g.fileno())
    try:
        if a.replace:
            os.rename(tmp, dst)  # atomic: readers keep the old inode or see the new complete file
        else:
            os.link(tmp, dst)  # fails if the file exists: never overwrites
            os.unlink(tmp)
    except Exception:
        os.path.exists(tmp) and os.unlink(tmp)
        raise
    print("published", dst, "rows", n)
    sys.exit(0)

assert a.out, "--out required"
years = [int(y) for y in a.years.split(",")]


def gate():
    if a.no_gate:
        return
    st = json.load(urllib.request.build_opener(urllib.request.ProxyHandler({})).open("http://127.0.0.1:18313/api/admin/status", timeout=10))
    now = datetime.datetime.fromisoformat(st["now"].replace("Z", "+00:00"))
    for j in st["jobs"]:
        if j.get("running"):
            sys.exit("gate: job %s is running" % j["name"])
        nr = j.get("next_run")
        if nr and 0 <= (datetime.datetime.fromisoformat(nr.replace("Z", "+00:00")) - now).total_seconds() < 600:
            sys.exit("gate: job %s starts within 10 minutes" % j["name"])
    if st.get("queue"):
        sys.exit("gate: queue not empty")


gate()
cat = sqlite3.connect(CATALOG, uri=True)
prod_ex = {}
for ric, prod, ex in cat.execute("select ric, product, exchange from contracts"):
    prod_ex.setdefault(ric, (prod, ex))
l2 = {(r, y): (fd, ld, d) for r, y, fd, ld, d in cat.execute("select ric, utc_year, first_day, last_day, days from l2_directories")}
tas_rows = [(r, p, y, d) for _, p, r, y, d in cat.execute(
    "select source_year, product, ric, utc_year, directory from directories where utc_year in (%s)" % ",".join(map(str, years)))]
# a RIC-year can appear under two source_years: keep one row per (ric, utc_year)
tas = {}
for r, p, y, d in tas_rows:
    tas.setdefault((r, y), (p, d))
print("TAS RIC-years", len(tas), "L2 RIC-years", sum(1 for k in l2 if k[1] in years), file=sys.stderr)

# reports -------------------------------------------------------------------
reports = {}  # package -> {ric: (count, start, end)}
paths = []
for d in a.reports:
    paths += sorted(glob.glob(os.path.join(d, "*.Report.csv.gz")))
paths += sorted(glob.glob(a.main_glob))  # the local main packages: name = their directory
for p in paths:
    name = os.path.basename(p)[: -len(".Report.csv.gz")] if p.endswith(".Report.csv.gz") else os.path.basename(os.path.dirname(p))
    rows = {}
    with gzip.open(p, "rt", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows[r["#RIC"]] = (int(r.get("Count") or 0), r["Start"][:10], r["End"][:10])
    reports[name] = rows


# purchase catalog: which source layers (tas, summarised, normalisedLL2, rawLL2, ...) exist per RIC / per product
layers_of, prod_layers = {}, collections.defaultdict(set)
if os.path.exists(a.ric_catalog):
    with open(a.ric_catalog, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ls = set(r["source_layers"].split("|"))
            layers_of[r["ric"]] = ls
            prod_layers[r["routed_product"]] |= ls
HAS_L2 = {"normalisedLL2", "rawLL2"}


def cause_of(ric, prod, reason, ev):
    """The reason in plain words (what is missing and why), for the person reading the table."""
    if reason == "not_in_any_package":
        ls = layers_of.get(ric) if ric else prod_layers.get(prod)
        if ls is None:
            return "No L2 data: no L2 package lists it and the purchase catalog has no entry for it (TAS only)."
        if not (ls & HAS_L2):
            return ("No L2 data: L2 was never ordered for it. The purchase catalog has only %s for this %s, no normalisedLL2 / rawLL2 layer, "
                    "so LSEG never exported L2 for it (a new LSEG L2 job would be needed)." % ("+".join(sorted(ls)), "RIC" if ric else "product"))
        if ev.startswith("TAS only after 2026-06-01") or "daily L2 files of that period have no Report" in ev:
            return ("No L2 data: it only trades after 2026-06-01, where L2 comes from the daily files; those hold only the near contracts "
                    "of the product and this RIC is not one of them.")
        return "Unexpected: the purchase catalog lists an L2 layer for it but no downloaded package does; check the package list."
    if reason == "report_count_zero":
        return "No L2 data: the LSEG package lists the RIC but with Count 0 (the vendor has no L2 messages for it in the period)."
    if reason == "outside_package_window":
        return "Partly missing: the LSEG package covers %s only; the rest of the TAS period is outside it." % ev.split("Start..End ")[-1].split(", TAS")[0]
    if reason == "in_package_not_delivered":
        return "Defect: a package holds data for it but nothing was delivered; to be fixed."
    return reason


def pkg_years(name):
    import re
    m = re.search(r"_(\d{4})-(\d\d-\d\d)_(\d{4})-(\d\d-\d\d)", name)
    return (m.group(1) + m.group(2).replace("-", ""), m.group(3) + m.group(4).replace("-", "")) if m else ("", "")


def pkgs_for(year):
    out = []
    for n in reports:
        s, e = pkg_years(n)
        if s[:4] == str(year):
            out.append(n)
    return sorted(out)


problems = {}
if a.problems and os.path.exists(a.problems):
    for line in open(a.problems):
        parts = line.rstrip("\n").split("\t")
        if len(parts) >= 3:
            problems[(parts[0], int(parts[1]))] = (parts[2], parts[3] if len(parts) > 3 else "")


# NFS walk ------------------------------------------------------------------
def days_of(d):
    try:
        return sorted(x for x in os.listdir(d) if len(x) == 8 and x.isdigit() and x <= LAST_L2_DAY)
    except FileNotFoundError:
        return []


def real_days(d, days):
    out = []
    for x in days:
        try:
            names = set(os.listdir(os.path.join(d, x)))
        except FileNotFoundError:
            continue
        if "trade.parquet" in names or "quote.parquet" in names:
            out.append(x)
    return out


def work(item):
    (ric, y), (prod, d) = item
    days = days_of(d)
    if not days:
        return (ric, y), None
    have = l2.get((ric, y))
    if have and have[2] >= THRESHOLD * len(days):
        return (ric, y), days  # not a candidate: no exact recount needed
    if have:
        days = real_days(d, days)
    return (ric, y), days


t0 = time.time()
walked = {}
with cf.ThreadPoolExecutor(max_workers=min(a.threads, 8)) as ex:
    for k, days in ex.map(work, tas.items(), chunksize=64):
        walked[k] = days
print("walked in %.0fs" % (time.time() - t0), file=sys.stderr)


def d8(x):
    return "%s-%s-%s" % (x[:4], x[4:6], x[6:])


def reason_for(ric, y, tfirst, tlast):
    names = pkgs_for(y)
    found = [(n, reports[n][ric]) for n in names if ric in reports[n]]
    ev = "checked %d packages for %d; " % (len(names), y)
    if y == 2026 and tfirst >= "2026-06-01":
        ev += "TAS only after 2026-06-01, the daily L2 files of that period have no Report; "
    if (ric, y) in problems:
        return problems[(ric, y)][0], ev + problems[(ric, y)][1]
    if not found:
        return "not_in_any_package", ev + "RIC absent from every Report"
    pos = [(n, v) for n, v in found if v[0] > 0]
    if not pos:
        n, v = found[0]
        return "report_count_zero", ev + "%s lists it with Count 0" % n
    s = min(v[1] for _, v in pos)
    e = max(v[2] for _, v in pos)
    lo = (datetime.date.fromisoformat(tfirst) + datetime.timedelta(days=SLACK_DAYS)).isoformat()
    hi = (datetime.date.fromisoformat(tlast) - datetime.timedelta(days=SLACK_DAYS)).isoformat()
    if s > lo or e < hi:
        return "outside_package_window", ev + "%s Start..End %s..%s, TAS %s..%s" % (pos[0][0], s, e, tfirst, tlast)
    return "in_package_not_delivered", ev + "%s Count=%d %s..%s" % (pos[0][0], pos[0][1][0], s, e)


now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
checked = sorted(reports)
rows = []
for (ric, y), days in sorted(walked.items()):
    if not days:
        continue
    prod, ex_ = prod_ex.get(ric, ("?", "?"))
    exdir = tas[(ric, y)][1].split("/lseg/")[1].split("/")[0]
    have = l2.get((ric, y))
    if have is None:
        kind = "none"
    elif have[2] < THRESHOLD * len(days):
        kind = "partial"
    else:
        continue
    tf, tl = d8(days[0]), d8(days[-1])
    rs, ev = reason_for(ric, y, tf, tl)
    rows.append((ric, y, prod, exdir, tf, tl, len(days), have[2] if have else 0,
                 d8(have[0]) if have else None, d8(have[1]) if have else None, kind, rs, ev))

if os.path.exists(a.out):
    os.unlink(a.out)
db = sqlite3.connect(a.out)
db.executescript("""
CREATE TABLE l2_unfillable (
 ric TEXT, utc_year INTEGER NOT NULL, product TEXT NOT NULL, exchange_dir TEXT,
 tas_first_day TEXT, tas_last_day TEXT, tas_days INTEGER, l2_days INTEGER,
 l2_first_day TEXT, l2_last_day TEXT, kind TEXT NOT NULL, reason TEXT NOT NULL, evidence TEXT NOT NULL,
 checked_packages TEXT NOT NULL, checked_utc TEXT NOT NULL, n_rics INTEGER,
 cause TEXT NOT NULL DEFAULT '', purchase_layers TEXT);
CREATE UNIQUE INDEX l2_unfillable_ric ON l2_unfillable(ric, utc_year) WHERE ric IS NOT NULL;
CREATE UNIQUE INDEX l2_unfillable_prod ON l2_unfillable(product, utc_year) WHERE ric IS NULL;
CREATE TABLE l2_package_inventory (
 package TEXT PRIMARY KEY, period TEXT, kind TEXT, gb REAL, rics_with_data INTEGER, products INTEGER,
 washed_utc TEXT, delivered_rics_days INTEGER, note TEXT);
CREATE TABLE l2_year_summary (
 utc_year INTEGER PRIMARY KEY, main_gb REAL, main_rics INTEGER, supplement_gb REAL, l2_rics_total INTEGER,
 l2_products INTEGER, tas_products INTEGER, tas_products_without_l2 INTEGER, note TEXT);
CREATE TABLE conventions (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE l2_raw_versions (
 ric TEXT NOT NULL, utc_year INTEGER NOT NULL, directory TEXT NOT NULL, first_day TEXT NOT NULL,
 last_day TEXT NOT NULL, days INTEGER NOT NULL, product TEXT, PRIMARY KEY(ric, utc_year));
CREATE VIEW l2_unfillable_products AS
 SELECT product, utc_year, SUM(COALESCE(n_rics, 1)) rics, kind, GROUP_CONCAT(DISTINCT reason) reasons
 FROM l2_unfillable GROUP BY product, utc_year, kind;
""")
for r in rows:
    ls = layers_of.get(r[0])
    db.execute("insert into l2_unfillable values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)",
               r[:2] + (r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], json.dumps(pkgs_for(r[1])), now,
                        cause_of(r[0], r[2], r[11], r[12]), "|".join(sorted(ls)) if ls else None))
# RIC-years whose L2 files were built from Raw LL2 (catalog l2_directories.source = 'raw'; older catalogs have no column)
if any(r[1] == "source" for r in cat.execute("pragma table_info(l2_directories)")):
    prod_of = {r: p for r, p in cat.execute("select ric, product from contracts")}
    for r, y, d, fd, ld, n in cat.execute(
            "select ric, utc_year, directory, first_day, last_day, days from l2_directories where source='raw'"):
        db.execute("insert into l2_raw_versions values (?,?,?,?,?,?,?)", (r, y, d, fd, ld, n, prod_of.get(r)))
inv = json.load(open(a.packages_json)) if a.packages_json else {}
allp = json.load(open(a.inventory_json)) if a.inventory_json else {}
meta = {p["job"].strip("/"): p for y in allp.values() for p in y["packages"]}
for n in checked:
    rr = reports[n]
    rics = [k for k, v in rr.items() if v[0] > 0]
    pr = {prod_ex.get(k, ("?",))[0] for k in rics}
    inf = inv.get(n, {})
    pre = pkg_years(n)[0][:4] < "2024"
    note = "empty: no RIC with data" if not rics else ("not washed: inventory only (before 2024)" if pre else None)
    db.execute("insert into l2_package_inventory values (?,?,?,?,?,?,?,?,?)",
               (n, "%s..%s" % pkg_years(n), meta.get(n, {}).get("kind"), meta.get(n, {}).get("gb"), len(rics), len(pr),
                None if pre else inf.get("washed_utc"), 0 if pre else inf.get("delivered_rics_days"), note))

# products (and their TAS RIC counts) per UTC year before the window: nothing was washed there, so a product is
# 'not_washed' when some package holds L2 for it and 'not_in_any_package' when none does
early = collections.defaultdict(lambda: collections.defaultdict(set))
for _, prod, ric, y, d in cat.execute("select source_year, product, ric, utc_year, directory from directories where utc_year < %d" % min(years)):
    early[y][prod].add(ric)
for y in sorted(early):
    names = pkgs_for(y)
    prod_pkgs = collections.defaultdict(lambda: collections.defaultdict(int))
    for n in names:
        for ric, v in reports[n].items():
            if v[0] > 0:
                prod_pkgs[prod_ex.get(ric, ("?",))[0]][n] += 1
    for prod, rs in sorted(early[y].items()):
        if prod in prod_pkgs:
            kind, rs_ = "not_washed", "washed_nothing_before_2024"
            ev = "L2 exists in " + "; ".join("%s (%d RICs)" % (n[:60], c) for n, c in sorted(prod_pkgs[prod].items()))
        else:
            kind, rs_ = "not_in_any_package", "not_in_any_package"
            ev = "no RIC of the product has data in any of the %d packages of the year" % len(names)
        cause = ("Not washed yet: an L2 package of that year holds the product (inventory only for years before 2024)." if kind == "not_washed"
                 else cause_of(None, prod, "not_in_any_package", ev))
        pl = prod_layers.get(prod)
        db.execute("insert into l2_unfillable values (NULL,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?,?,?,?,?,?,?)",
                   (y, prod, kind, rs_, ev, json.dumps(names), now, len(rs), cause, "|".join(sorted(pl)) if pl else None))

# per-year summary: packages/GB/RICs from the all-periods inventory; for the window years the products without L2
# are recomputed from what is delivered (l2_directories); the inventory's number (no package at all) stays in the note
for y in sorted(int(k) for k in allp):
    x = allp[str(y)]
    mg = sum(p["gb"] for p in x["packages"] if p["kind"] == "main")
    sg = sum(p["gb"] for p in x["packages"] if p["kind"] != "main")
    twl, note = x["tas_products_without_l2"], None
    if y in years:
        tp = collections.defaultdict(set)
        for (ric, yy), (pr_, _) in tas.items():
            if yy == y:
                tp[pr_].add(ric)
        dl = {prod_ex.get(r, ("?",))[0] for (r, yy) in l2 if yy == y}
        twl = sum(1 for pr_ in tp if pr_ not in dl)
        note = ("products with a TAS RIC directory in this UTC year and no delivered L2 RIC (delivered L2 products: %d). The inventory's %d counted per "
                "TAS source year and 'no package lists the product with data', so the two numbers differ (e.g. products whose package RICs have Count 0)"
                % (len(dl), x["tas_products_without_l2"]))
    if x["tas_products"] == 0:
        note = "TAS has no data in the catalog for this year (tas_products=0): nothing to compare against"
    elif y < min(years):
        note = "inventory only, nothing washed; tas_products_without_l2 = no package has the product"
    db.execute("insert into l2_year_summary values (?,?,?,?,?,?,?,?,?)",
               (y, round(mg, 1), sum(p["rics_with_data"] for p in x["packages"] if p["kind"] == "main"), round(sg, 1),
                x["rics_with_data_total"], x["l2_products_mapped"], x["tas_products"], twl, note))
conv = {
    "built_utc": now, "years": a.years, "tas_source": "catalog.sqlite directories (RIC-years with TAS directories) + day directories listed on NFS",
    "l2_source": "catalog.sqlite l2_directories (lseg_catalog l2-sync); last L2 day %s" % LAST_L2_DAY,
    "tas_days": "day directories of the RIC-year up to the last L2 day; for partial candidates only days holding trade.parquet or quote.parquet",
    "partial_threshold": "l2_days < %.1f * tas_days" % THRESHOLD, "window_slack_days": str(SLACK_DAYS),
    "reasons": "not_in_any_package | report_count_zero | outside_package_window | unmapped_exchange | overlap_conflict | in_package_not_delivered",
    "daily_2026_note": "L2 from 2026-06-01 comes from daily files that have no Report here; 2026 reasons only know the package Reports up to 2026-05-30",
    "cause": "plain-language reason per row; purchase_layers = source layers of the RIC/product in the purchase catalog (artifacts/lseg_exchange_groups_20261109/ric_catalog.csv): no normalisedLL2/rawLL2 there means L2 was never ordered",
    "l2_raw_versions": "RIC-years whose L2 files are built from Raw LL2 (Legacy Level 2 slot updates, book rebuilt by us, see docs/lseg/l2_raw.md), not from Normalized LL2; every other L2 RIC-year is Normalized. Same information: catalog.sqlite l2_directories.source",
    "query": "select product, rics, kind, reasons from l2_unfillable_products where utc_year=2025 order by product",
}
db.executemany("insert into conventions values (?,?)", conv.items())
db.commit()
print("rows", len(rows), file=sys.stderr)
for k, y, n in db.execute("select kind, utc_year, count(*) from l2_unfillable group by 1,2"):
    print("  %s %s %d" % (k, y, n), file=sys.stderr)
db.close()
