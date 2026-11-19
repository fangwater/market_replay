#!/usr/bin/env python3
"""Publish a coherent domestic minute HFQ archive after cn-daily."""

import argparse
import csv
import ctypes
from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib
from collections import defaultdict


PROVENANCE = Path("_provenance/hfq")
MANIFEST = PROVENANCE / "generation.json"
AT_FDCWD = -100
RENAME_EXCHANGE = 2


def exchange_directories(left, right):
    rename = ctypes.CDLL(None, use_errno=True).renameat2
    rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    if rename(AT_FDCWD, os.fsencode(left), AT_FDCWD, os.fsencode(right), RENAME_EXCHANGE):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(right))


def parquet_days(directory):
    return {
        path.stem for path in directory.glob("*.parquet")
        if len(path.stem) == 8 and path.stem.isdecimal()
    }


def read_roll(directory, through, baseline):
    selections = defaultdict(dict)
    adjustments = defaultdict(list)
    for path in sorted(directory.glob("*_dominant.csv")):
        exchange = path.name.removesuffix("_dominant.csv")
        if not (baseline / exchange).is_dir():
            continue
        with path.open() as rows:
            seen_end = max_date = ""
            for row in csv.DictReader(rows):
                day = row["trad_day"].replace("-", "")
                max_date = max(day, max_date)
                if day > through:
                    break
                seen_end = max(day, seen_end)
                if row["instrument_id"]:
                    selections[exchange, row["product_id"]][day] = row["instrument_id"]
            if max_date != through or seen_end != through:
                raise ValueError(f"{path.name} is not at cn-daily watermark {through}")
        path = directory / f"{exchange}_adjustment_factor.csv"
        with path.open() as rows:
            for row in csv.DictReader(rows):
                day = row["effective_trading_day"].replace("-", "")
                if day > through:
                    raise ValueError(f"{path.name} has a roll beyond the archive watermark")
                adjustments[exchange, row["product_id"]].append(
                    (day, row["adjustment_value"], row["skipped"])
                )
    return selections, adjustments


def next_basis(directory, exchange, product, through, last):
    with (directory / f"{exchange}_dominant.csv").open() as rows:
        selection = next((row for row in csv.DictReader(rows)
                          if row["product_id"] == product and row["trad_day"].replace("-", "") == through), {})
    following = selection.get("scheduled_next")
    if not following or following == last:
        return {}
    with (directory / f"{exchange}_eod.csv").open() as rows:
        prices = {row["instrument_id"]: float(row["close"])
                  for row in csv.DictReader(rows) if row["product_id"] == product
                  and row["trad_day"].replace("-", "") == through
                  and row["instrument_id"] in (last, following) and row["close"]}
    if any(not (0 < prices.get(contract, 0) < 1e100) for contract in (last, following)):
        return {"scheduled_next": following}
    return {"scheduled_next": following, "next_gap": prices[following] - prices[last]}


def build(root, stage, roll, raw, through, exporter, workers, staged, rebuild):
    selected, adjustments = read_roll(roll, through, raw)
    old_manifest = root / MANIFEST
    old_through = ""
    old_markets = {}
    old_selected, old_adjustments = {}, {}
    if old_manifest.is_file():
        previous = json.loads(old_manifest.read_text())
        old_through = previous["through"].replace("-", "")
        old_markets = previous["markets"]
        if old_through > through:
            raise ValueError("cn-daily watermark is behind the published HFQ generation")
        old_selected, old_adjustments = read_roll(root / PROVENANCE / "roll", old_through, raw)
    expected = {}
    known_gaps = {}
    groups = defaultdict(list)
    for (exchange, product), dates in sorted(selected.items()):
        market = exchange, product
        available = parquet_days(raw / exchange / product)
        if not available:
            continue
        first = min(available)
        skipped = max((day for day, _, skip in adjustments[market] if skip == "1"), default="")
        original = root / exchange / product
        existing = parquet_days(original)
        selected_days = {day for day in dates if first <= day and skipped <= day}
        missing_raw = selected_days - available
        previous_market = old_markets.get(f"{exchange}/{product}", {})
        gaps = set(previous_market.get("source_gaps", []))
        last_existing = max(existing, default="")
        if not old_markets and staged:
            gaps = {day for day in selected_days - parquet_days(stage / exchange / product)
                    if day <= last_existing}
        if missing_raw - gaps:
            raise ValueError(f"missing new raw source for {exchange}/{product}: {sorted(missing_raw)}")
        days = selected_days & available
        if not existing <= days:
            raise ValueError(f"new generation would lose history for {exchange}/{product}: "
                             f"{sorted(existing - days)[:5]}")
        if not days:
            continue
        expected[market] = days
        known_gaps[market] = gaps
        if staged:
            continue
        carry = set()
        if (not rebuild and old_selected.get(market) == {d: dates[d] for d in dates if d <= old_through}
                and old_adjustments.get(market, []) == adjustments[market]):
            carry = existing
        if carry:
            destination = stage / exchange / product
            destination.mkdir(parents=True)
            for day in sorted(carry):
                name = f"{day}.parquet"
                os.link(original / name, destination / name)
        if missing := days - carry - (gaps if carry else set()):
            groups[min(missing)].append(product)
    if not expected:
        raise ValueError("archive has no selected products")
    known = {
        (directory.parent.name, directory.name) for directory in root.glob("*/*")
        if directory.is_dir() and parquet_days(directory)
    }
    if known - expected.keys():
        raise ValueError(f"new archive would lose products: {sorted(known - expected.keys())}")
    for start, products in sorted(groups.items()):
        subprocess.run([
            exporter, "--baseline-only", "--in-root", raw, "--roll-root", roll,
            "--out-root", stage, "--start", start, "--end", through,
            "--workers", str(workers), "--product", ",".join(products),
        ], check=True)
    markets = {}
    for (exchange, product), days in expected.items():
        actual = parquet_days(stage / exchange / product)
        gaps = known_gaps[exchange, product]
        if actual - days or days - actual - gaps or not actual or max(actual) != max(days):
            raise ValueError(f"{exchange}/{product} staged coverage mismatch: "
                             f"missing {sorted(days - actual)[:5]}, extra {sorted(actual - days)[:5]}")
        latest = max(days)
        markets[f"{exchange}/{product}"] = {
            "first_day": min(days), "last_day": latest, "last_contract": selected[exchange, product][latest],
            "files": len(actual),
            "source_gaps": sorted(gaps - actual),
        }
        if latest == through:
            markets[f"{exchange}/{product}"].update(
                next_basis(roll, exchange, product, through, selected[exchange, product][latest])
            )
    return markets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--exporter", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--staged-root", type=Path)
    parser.add_argument("--roll-snapshot", type=Path)
    args = parser.parse_args()
    if bool(args.staged_root) != bool(args.roll_snapshot) or args.workers < 1:
        parser.error("provide both staged inputs together, with a positive worker count")
    cfg = tomllib.loads(args.config.read_text())
    raw = Path(cfg["baseline_root"])
    root = raw.with_name("baseline_data_1min_hfq")
    state = Path(cfg["state_file"])
    through_date = date.fromisoformat(json.loads(state.read_text())["last_done"])
    through = through_date.strftime("%Y%m%d")
    with (root.parent / ".cn-hfq.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tempfile.TemporaryDirectory(prefix="cn-hfq-roll-") as temp:
            snapshot = Path(temp)
            roll = args.roll_snapshot or Path(cfg["roll_root"]) / "result"
            for suffix in ("*_dominant.csv", "*_adjustment_factor.csv", "*_eod.csv"):
                for path in sorted(roll.glob(suffix)):
                    shutil.copyfile(path, snapshot / path.name)
            digest = hashlib.sha256()
            for path in sorted(snapshot.iterdir()):
                digest.update(path.name.encode())
                digest.update(path.read_bytes())
            roll_sha256 = digest.hexdigest()
            if (root / MANIFEST).is_file() and not args.staged_root and not args.rebuild:
                if json.loads((root / MANIFEST).read_text())["roll_sha256"] == roll_sha256:
                    print(f"HFQ already current through {through_date}")
                    return
            generation = hashlib.sha256(digest.digest() + os.urandom(32)).hexdigest()
            stage = args.staged_root or Path(tempfile.mkdtemp(prefix=".cn-hfq-", dir=root.parent))
            if stage.resolve().parent != root.parent.resolve() or stage.resolve() in (root, raw):
                raise ValueError("HFQ stage must be a separate sibling of the archive")
            had_generation = (root / MANIFEST).is_file()
            try:
                acl = subprocess.check_output(["getfacl", "-cp", root])
                subprocess.run(["setfacl", "--set-file=-", stage], input=acl, check=True)
                markets = build(root, stage, snapshot, raw, through, args.exporter, args.workers,
                                args.staged_root is not None, args.rebuild)
                if date.fromisoformat(json.loads(state.read_text())["last_done"]) != through_date:
                    raise ValueError("cn-daily advanced during the HFQ export")
                provenance = stage / PROVENANCE
                if (root / "_provenance").is_dir():
                    shutil.copytree(root / "_provenance", stage / "_provenance",
                                    dirs_exist_ok=True, ignore=shutil.ignore_patterns("hfq"))
                provenance.mkdir(parents=True, exist_ok=True)
                shutil.copytree(snapshot, provenance / "roll")
                (stage / MANIFEST).write_text(json.dumps({
                    "through": through_date.isoformat(), "generation": generation,
                    "roll_sha256": roll_sha256, "markets": markets,
                }, indent=2) + "\n")
                exchange_directories(root, stage)
            except Exception:
                if args.staged_root is None:
                    shutil.rmtree(stage)
                raise
            if had_generation:
                shutil.rmtree(stage)
            else:
                print(f"Previous archive retained at {stage}")
            print(f"HFQ published through {through_date}: {len(markets)} products, "
                  f"{sum(m['files'] for m in markets.values())} files, generation {generation}")


if __name__ == "__main__":
    main()
