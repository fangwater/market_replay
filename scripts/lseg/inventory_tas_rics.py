#!/usr/bin/env python3
"""Inventory TAS source RICs; source Count is not a stored trade/quote count."""

import argparse
import csv
import gzip
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET


MONTHS = "FGHJKMNQUVXZ"
PREPROCESS = Path("/home/fanghaizhou/preprocess")


def routed_product(ric):
    """Match cme_tas_replay::product::parse_product, without claiming identity."""
    if not ric or not ric.isascii() or any(not 33 <= ord(c) <= 126 for c in ric):
        raise ValueError(f"non-printable RIC: {ric!r}")
    if ric.startswith("."):
        if len(ric) == 1:
            raise ValueError("empty index RIC")
        return ric[1:]
    stem = ric.split("^", 1)[0]
    match = re.fullmatch(rf"(.+)[{MONTHS}][0-9]+", stem)
    return match[1] if match else stem


def encode_product(product):
    return "".join(c if c.isascii() and (c.isalnum() or c == "_") else f"%{ord(c):02X}"
                   for c in product)


def shape(ric):
    stem = ric.split("^", 1)[0]
    if stem.startswith("."):
        return "index", "", ""
    if stem.endswith("TOT"):
        return "product_total", "", ""
    if re.fullmatch(r"Cc(?:[1-9]|1[0-7])", stem):
        return "corn_continuation_alias", "", ""
    if stem.startswith("NGLND"):
        return "nbp_gas_family_requires_specific_mapping", "", ""
    match = re.fullmatch(rf"(.+)([{MONTHS}])([0-9]{{1,2}})", stem)
    if match:
        return "month_year_pattern_not_identity_certification", match[2], match[3]
    return "other_requires_specific_mapping", "", ""


def write_csv(path, fields, rows):
    with path.open("x", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_vendor_mapping(path):
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(path) as archive:
        strings = ["".join(node.itertext()) for node in ET.fromstring(
            archive.read("xl/sharedStrings.xml")).findall("s:si", ns)]
        result = []
        for row in ET.fromstring(archive.read("xl/worksheets/sheet1.xml")).findall(
                "s:sheetData/s:row", ns)[1:]:
            cells = {}
            for cell in row.findall("s:c", ns):
                value = cell.find("s:v", ns)
                text = value.text if value is not None else ""
                cells[re.sub(r"[0-9]", "", cell.attrib["r"])] = (
                    strings[int(text)] if cell.get("t") == "s" else text)
            result.append(dict(zip(
                ("cn_name", "purchase_ticker", "exchange", "english_name", "category", "subcategory", "chain_ric"),
                (cells.get(key, "").strip() for key in "ABCDEFG"))))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=Path(
        "/mnt/hdd-raid5-72t/liang_torch/lseg_data/future/normalised"))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    reports = sorted(args.source_root.glob("*/merged-Report.csv.gz"))
    if not reports:
        raise ValueError("no TAS source reports found")
    records, period_stats, inputs = [], [], []
    by_ric = defaultdict(set)
    for report in reports:
        period = report.parent.name[-21:]
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}-[0-9]{2}-[0-9]{2}", period):
            raise ValueError(f"unrecognised source period: {report}")
        with gzip.open(report, "rt", newline="") as file:
            rows = list(csv.DictReader(file))
        active = [row for row in rows if int(row["Count"]) > 0]
        inputs.append({"path": str(report), "sha256": hashlib.sha256(report.read_bytes()).hexdigest()})
        period_stats.append({"period": period, "report_rics": len(rows),
                             "active_source_rics": len(active),
                             "source_event_rows": sum(int(row["Count"]) for row in rows)})
        for row in active:
            ric = row["#RIC"]
            product = routed_product(ric)
            category, month, year_token = shape(ric)
            records.append(dict(period=period, period_year=int(period[:4]), ric=ric,
                                routed_product=product,
                                product_cf=f"p:{period[:4]}:{encode_product(product)}",
                                ric_shape=category, month_code=month, year_token=year_token,
                                source_status=row["Status"], source_rows=int(row["Count"]),
                                source_start=row["Start"], source_end=row["End"], report_path=str(report)))
            by_ric[ric].add(period)

    mapping_path = PREPROCESS / "database/lseg_future_supplement_20261012/Futues_List_RIC_Mapping.xlsx"
    description_path = PREPROCESS / "database/lseg_tas_ll2_missing_2025_2026_20261011/tas_2025_descriptions.json"
    stored_path = PREPROCESS / "database/lseg_tas_ll2_matching_20261011/contracts.csv"
    mapping = read_vendor_mapping(mapping_path)
    descriptions = json.loads(description_path.read_text())
    changes = [dict(year=row["year"], ric=row["ric"], **change)
               for row in descriptions["rows"] if re.fullmatch(r"Cc[0-9]+", row["ric"])
               for change in row.get("descriptions", [])]
    with stored_path.open(newline="") as file:
        stored = [{key: row[key] for key in ("year", "product_cf", "ric", "is_index", "first_trade_ns", "last_trade_ns")}
                  for row in csv.DictReader(file)]
    for path in (mapping_path, description_path, stored_path):
        inputs.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})

    args.output_dir.mkdir(parents=True, exist_ok=False)
    write_csv(args.output_dir / "source_ric_by_period.csv", list(records[0]), records)
    write_csv(args.output_dir / "source_periods.csv", list(period_stats[0]), period_stats)
    write_csv(args.output_dir / "vendor_chain_mapping.csv", list(mapping[0]), mapping)
    write_csv(args.output_dir / "corn_alias_description_changes.csv", ["year", "ric", "ts_ns", "old", "new"], changes)
    write_csv(args.output_dir / "stored_trade_rics_2024_2025.csv", list(stored[0]), stored)
    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "scope": "source Report metadata plus existing 2026-10-11 database audit; no new database scan",
        "source_periods": len(period_stats), "active_source_ric_period_pairs": len(records),
        "distinct_active_source_ric_strings": len(by_ric),
        "source_event_rows": sum(row["source_event_rows"] for row in period_stats),
        "shape_counts_are_source_ric_period_pairs": dict(Counter(row["ric_shape"] for row in records)),
        "existing_stored_trade_audit_by_year": dict(Counter(row["year"] for row in stored)),
        "corn_alias_description_changes": len(changes),
        "notes": ["Source Count includes all event types; it is not a Trade or aggregated Quote count.",
                  "A month/year-shaped code does not certify contract identity or exchange.",
                  "A one-digit year must not be expanded without reference identity evidence.",
                  "Routing CF names are not an authoritative instrument taxonomy.",
                  "Preserve original RIC, case and source period when exporting."],
        "inputs": inputs,
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in summary.items() if key not in ("inputs", "notes")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
