#!/usr/bin/env python3
"""Final report of the L2 backfill (period packages + daily files washed into RocksDB).

usage: l2_backfill_report.py --root <runtime/l2_backfill> --db-root </mnt/hdd-raid5-72t/fanghaizhou/lseg_data/l2_db> [--out-dir <dir>]

Reads what the run itself wrote (backfill.log = one JSON line per washed unit, progress.json,
source_gaps.json) plus the independent checks (spotcheck.jsonl, boundary_*.jsonl) and writes
report.json and report.md. Nothing is invented: a figure the run did not record is not reported.
Complete means: progress state finished, units_done == units_total, no errors, every unit washed once,
all spot checks ok.
"""
import argparse, collections, json, os, subprocess, sys, ast

ap = argparse.ArgumentParser()
ap.add_argument("--root", required=True)
ap.add_argument("--db-root", required=True)
ap.add_argument("--out-dir")
a = ap.parse_args()
out = a.out_dir or a.root


def jl(path):
    rows = []
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line.startswith("{"):
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    return rows


def du(path):
    if not os.path.exists(path):
        return None
    return int(subprocess.run(["du", "-sb", path], capture_output=True, text=True).stdout.split()[0])


def iso(ns):
    import datetime
    return datetime.datetime.fromtimestamp(ns / 1e9, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


progress = json.load(open(os.path.join(a.root, "progress.json")))
gaps = json.load(open(os.path.join(a.root, "source_gaps.json")))
units = {}
dups = 0
for r in jl(os.path.join(a.root, "backfill.log")):
    if r.get("status") != "washed":
        continue
    if r["unit"] in units:
        dups += 1
    units[r["unit"]] = r

stream_ids = [u for u, r in units.items() if "rics_total_in_db" in r]
daily = {u: r for u, r in units.items() if u not in stream_ids}

inputs = {}
for u in sorted(stream_ids):
    r = units[u]
    inputs[u] = {
        "kind": "period package (one stream database)",
        "source_rows_counted_by_pipeline": r["updates"],
        "rics": r["rics"], "rics_in_db": r["rics_total_in_db"], "skipped_rics": r["skipped_rics"],
        "first_ts": iso(r["first_ts"]), "last_ts": iso(r["last_ts"]),
        "gz_bytes": r["input_gz_bytes"], "key_bytes": r["key_bytes"], "value_bytes": r["value_bytes"],
        "sst_bytes": r["sst_bytes"], "sst_over_gz": round(r["sst_bytes"] / r["input_gz_bytes"], 3),
        "updates_by_month": r["months"], "seconds": r["seconds"],
    }

by_month = collections.defaultdict(lambda: {"units": 0, "units_with_data": 0, "source_rows": 0, "gz_bytes": 0,
                                            "sst_bytes": 0, "rics": 0, "first_ts": None, "last_ts": None})
for r in daily.values():
    m = by_month[r["day"][:7]]
    m["units"] += 1
    m["gz_bytes"] += r["input_gz_bytes"]
    m["sst_bytes"] += r["sst_bytes"]
    m["source_rows"] += r["updates"]
    m["rics"] += r["rics"]
    if r["updates"]:
        m["units_with_data"] += 1
        m["first_ts"] = r["first_ts"] if m["first_ts"] is None else min(m["first_ts"], r["first_ts"])
        m["last_ts"] = r["last_ts"] if m["last_ts"] is None else max(m["last_ts"], r["last_ts"])
daily_months = {}
for k, m in sorted(by_month.items()):
    daily_months[k] = dict(m, first_ts=iso(m["first_ts"]) if m["first_ts"] else None,
                           last_ts=iso(m["last_ts"]) if m["last_ts"] else None,
                           sst_over_gz=round(m["sst_bytes"] / m["gz_bytes"], 3) if m["gz_bytes"] else None)

# seams: no overlap between consecutive inputs (global first/last timestamps)
seams = {}
if "p2025" in inputs and "p2026h1" in inputs:
    seams["2025/2026 year boundary"] = {
        "p2025_last": inputs["p2025"]["last_ts"], "p2026h1_first": inputs["p2026h1"]["first_ts"],
        "overlap": inputs["p2025"]["last_ts"] >= inputs["p2026h1"]["first_ts"]}
d0601 = [r for u, r in daily.items() if r["day"] == "2026-06-01" and r["updates"]]
if "p2026h1" in inputs and d0601:
    first = min(r["first_ts"] for r in d0601)
    seams["period package / daily seam (2026-06-01)"] = {
        "p2026h1_last": inputs["p2026h1"]["last_ts"], "daily_2026-06-01_first": iso(first),
        "overlap": units["p2026h1"]["last_ts"] >= first}

spot = []
for r in jl(os.path.join(a.root, "spotcheck.jsonl")):
    v = r.get("verify")
    v = ast.literal_eval(v) if isinstance(v, str) else v
    spot.append({"ric": r["ric"], "day": r["day"], "source": os.path.basename(r["sources"][0]),
                 "source_rows": r["source_rows"], "slice_rows": v["parquet_rows"],
                 "mismatch": v["mismatch"], "ok": r["ok"] and v["ok"]})
boundary = []
for f in sorted(os.listdir(a.root)):
    if f.startswith("boundary_") and f.endswith(".jsonl"):
        for r in jl(os.path.join(a.root, f)):
            v = r.get("verify")
            v = ast.literal_eval(v) if isinstance(v, str) else v
            boundary.append({"ric": r["ric"], "day": r["day"], "sources": [os.path.basename(s) for s in r["sources"]],
                             "source_rows": r["source_rows"], "slice_rows": v["parquet_rows"],
                             "mismatch": v["mismatch"], "ok": r["ok"] and v["ok"]})

# only the databases of this report's inputs (other stream databases, e.g. a 2024 run in progress, are not counted)
stream_bytes = {s: du(os.path.join(a.db_root, "_stream", s)) for s in sorted(stream_ids)}
daily_days = sorted({r["day"].replace("-", "") for r in daily.values()})
daily_bytes = sum(du(os.path.join(a.db_root, d)) or 0 for d in daily_days)
db = {"root": a.db_root, "total_bytes": sum(v or 0 for v in stream_bytes.values()) + daily_bytes,
      "stream_bytes": stream_bytes, "daily_bytes": daily_bytes}
zero_units = sum(1 for r in daily.values() if r["updates"] == 0)
complete = (progress["state"] in ("finished", "done") and progress["units_done"] == progress["units_total"]
            and not progress["errors"] and len(units) == progress["units_total"] and dups == 0
            and spot and all(s["ok"] for s in spot))

report = {
    "complete": bool(complete),
    "run": progress.get("run"),
    "state": progress["state"], "units_done": progress["units_done"], "units_total": progress["units_total"],
    "units_washed_in_log": len(units), "units_logged_twice": dups, "errors": progress["errors"],
    "gz_gb_total": progress["gz_gb_total"],
    "inputs": inputs,
    "daily_by_month": daily_months,
    "daily_units_without_updates (weekend/holiday/empty placeholders)": zero_units,
    "source_gaps": {"missing_files": gaps.get("missing_files"), "zero_byte_placeholders": gaps.get("zero_byte_placeholders"),
                    "still_downloading": gaps.get("still_downloading"),
                    "missing_by_day": dict(collections.Counter(d for _, d in gaps.get("missing", [])).most_common(12))},
    "seams": seams,
    "database": db,
    "independent_spot_checks": spot,
    "boundary_checks (RIC continuing across two package parts)": boundary,
    "not_recorded_by_the_run": [
        "an independent count of update records read back from the databases (checked instead: sampled RIC-days reproduce the source slice value for value)",
        "counts of identical-row duplicates between the 2025 package and its supplement (the packages are stored in separate databases; no conflict was reported: errors is empty)",
    ],
}
os.makedirs(out, exist_ok=True)
name = "report" if complete else "report.incomplete"  # report.json exists only for a complete run
json.dump(report, open(os.path.join(out, name + ".json"), "w"), indent=1, ensure_ascii=False)

gb = lambda b: "%.1f GB" % (b / 1e9)
md = ["# L2 backfill 2025 + 2026 (RocksDB wash)", "",
      "complete: **%s**; units %d/%d; errors %d; source %.0f GB gz; database %s" % (
          report["complete"], progress["units_done"], progress["units_total"], len(progress["errors"]),
          progress["gz_gb_total"], gb(db["total_bytes"] or 0)), "",
      "| input | source rows | RICs | first | last | gz | database | db/gz |", "|---|---|---|---|---|---|---|---|"]
for k, v in inputs.items():
    md.append("| %s | %d | %d | %s | %s | %s | %s | %.2f |" % (
        k, v["source_rows_counted_by_pipeline"], v["rics"], v["first_ts"][:10], v["last_ts"][:10],
        gb(v["gz_bytes"]), gb(v["sst_bytes"]), v["sst_over_gz"]))
md += ["", "| daily month | units | source rows | gz | database | db/gz |", "|---|---|---|---|---|---|"]
for k, v in daily_months.items():
    md.append("| %s | %d (%d with data) | %d | %s | %s | %s |" % (
        k, v["units"], v["units_with_data"], v["source_rows"], gb(v["gz_bytes"]), gb(v["sst_bytes"]), v["sst_over_gz"]))
md += ["", "seams: " + "; ".join("%s: overlap=%s" % (k, v["overlap"]) for k, v in seams.items()),
       "", "spot checks: %d/%d ok; boundary checks: %d/%d ok" % (
           sum(s["ok"] for s in spot), len(spot), sum(b["ok"] for b in boundary), len(boundary)),
       "", "source gaps: %s missing files, %s zero-byte placeholders" % (gaps.get("missing_files"), gaps.get("zero_byte_placeholders")),
       "", "not recorded by the run:"] + ["- " + s for s in report["not_recorded_by_the_run"]]
open(os.path.join(out, name + ".md"), "w").write("\n".join(md) + "\n")
print("complete" if complete else "INCOMPLETE", "->", os.path.join(out, name + ".json"))
sys.exit(0 if complete else 3)
