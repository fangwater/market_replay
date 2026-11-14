#!/usr/bin/env python3
"""One-way migration of legacy SQLite snapshots into incremental PostgreSQL rows."""

import argparse
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cn_futures_ref import storage
from cn_futures_ref.models import DB_COLUMNS


def read_source(path):
    connection = sqlite3.connect(str(path))
    connection.row_factory = sqlite3.Row
    try:
        snapshots = [dict(row) for row in connection.execute("""
            SELECT id, snapshot_date, fetched_at_utc, source_url
            FROM snapshots
            ORDER BY snapshot_date, fetched_at_utc, id
        """).fetchall()]
        for snapshot in snapshots:
            snapshot["records"] = [dict(row) for row in connection.execute(
                "SELECT %s FROM instrument_snapshots WHERE snapshot_id = ? "
                "ORDER BY venue, product_id, instrument_id" % ",".join(DB_COLUMNS),
                (snapshot["id"],),
            ).fetchall()]
        overrides = [dict(row) for row in connection.execute("""
            SELECT venue, product_id, instrument_id, effective_from, effective_to,
                   field_name, field_value, source_url, note, created_at_utc
            FROM official_overrides ORDER BY id
        """).fetchall()]
    finally:
        connection.close()
    return snapshots, overrides


def migrate(args):
    sqlite_path = Path(args.sqlite).resolve()
    if not sqlite_path.is_file():
        raise RuntimeError("SQLite file not found: %s" % sqlite_path)
    snapshots, overrides = read_source(sqlite_path)
    if args.dry_run:
        print("legacy snapshots: %d" % len(snapshots))
        print("legacy instrument rows: %d" % sum(len(item["records"]) for item in snapshots))
        print("legacy overrides: %d" % len(overrides))
        return

    target = storage.connect(args.database_url)
    try:
        existing = target.execute(
            "SELECT COUNT(*) AS count FROM %s" % storage.VERSIONS
        ).fetchone()["count"]
        if existing:
            raise RuntimeError(
                "PostgreSQL incremental target already contains %d rows; migration stopped" % existing
            )
        inserted, confirmed = 0, 0
        for snapshot in snapshots:
            result = storage.merge_records(
                target, snapshot["records"], snapshot["snapshot_date"],
                snapshot["fetched_at_utc"], snapshot["source_url"],
            )
            inserted += result["inserted"]
            confirmed += result["confirmed"]
        for override in overrides:
            storage.upsert_override(target, override)
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        target.close()
    print("migrated incremental versions: %d" % inserted)
    print("confirmed unchanged rows: %d" % confirmed)
    print("migrated overrides: %d" % len(overrides))


def main():
    parser = argparse.ArgumentParser(
        description="Migrate legacy SQLite snapshots into incremental PostgreSQL records"
    )
    parser.add_argument("--sqlite", default="cn_futures_ref.db")
    parser.add_argument(
        "--database-url", default=os.environ.get("CN_FUTURES_REF_DATABASE_URL")
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.database_url:
        parser.error("--database-url or CN_FUTURES_REF_DATABASE_URL is required")
    try:
        migrate(args)
    except Exception as error:
        print("error: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
