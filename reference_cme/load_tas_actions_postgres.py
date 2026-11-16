from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import tempfile

from tas_action_mapping import MAPPING_VERSION, action_mappings


PROJECT_ROOT = Path(__file__).resolve().parent
PRODUCTS = ("ES", "NQ", "RTY", "YM", "GC", "CL")
DEFAULT_ROCKSDB = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products"
)
DEFAULT_EXPORTER = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/mkt_signal/release/"
    "cme_tas_export_status"
)
DEFAULT_PSQL = Path("/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql")
DEFAULT_PG_SOCKET = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/postgresql/domestic_futures/16/run"
)
DEFAULT_DDL = PROJECT_ROOT / "database/cme_tas_action_events.sql"
EXPORT_HEADER = "product\tric\tts_utc_ns\texch_hms_ns\tqualifiers"


@dataclass(frozen=True, order=True)
class Partition:
    year: int
    product: str
    period: str


def epoch_ns(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000_000


def parse_partition_output(output: str) -> list[Partition]:
    lines = output.splitlines()
    if not lines or lines[0] != "year\tproduct\tperiod":
        raise RuntimeError("unexpected TAS partition-list header")
    partitions: list[Partition] = []
    for line in lines[1:]:
        if not line:
            continue
        raw_year, product, period = line.split("\t", 2)
        if product not in PRODUCTS:
            raise RuntimeError(f"unexpected TAS product in partition list: {product}")
        year = int(raw_year)
        if not period.startswith(f"{year:04}-") or "_" not in period:
            raise RuntimeError(f"unexpected TAS period for year {year}: {period}")
        partitions.append(Partition(year, product, period))
    return sorted(set(partitions))


def discover_partitions(
    exporter: Path, rocksdb: Path, start_year: int | None, end_year: int | None
) -> list[Partition]:
    command = [
        str(exporter),
        "--rocksdb-dir",
        str(rocksdb),
        "--list-partitions",
    ]
    for product in PRODUCTS:
        command.extend(("--product", product))
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    partitions = parse_partition_output(result.stdout)
    return [
        partition
        for partition in partitions
        if (start_year is None or partition.year >= start_year)
        and (end_year is None or partition.year <= end_year)
    ]


def scan_to_csv(
    exporter: Path,
    rocksdb: Path,
    partitions: list[Partition],
    destination: Path,
) -> dict[str, int]:
    by_year: dict[int, list[Partition]] = defaultdict(list)
    for partition in partitions:
        by_year[partition.year].append(partition)

    counts: Counter[str] = Counter()
    with destination.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        for year, year_partitions in sorted(by_year.items()):
            periods = {partition.period for partition in year_partitions}
            if len(periods) != 1:
                raise RuntimeError(f"multiple TAS periods found for year {year}: {periods}")
            raw_start, raw_end = periods.pop().split("_", 1)
            year_start = datetime.fromisoformat(raw_start).replace(tzinfo=timezone.utc)
            year_end = datetime.fromisoformat(raw_end).replace(tzinfo=timezone.utc)
            products = sorted(partition.product for partition in year_partitions)
            command = [
                str(exporter),
                "--rocksdb-dir",
                str(rocksdb),
                "--year",
                str(year),
                "--start-ns",
                str(epoch_ns(year_start)),
                "--end-ns",
                str(epoch_ns(year_end)),
                "--phase-actions-only",
            ]
            for product in products:
                command.extend(("--product", product))
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if process.stdout is None or process.stderr is None:
                raise RuntimeError("failed to capture TAS exporter output")
            header = process.stdout.readline().rstrip("\n")
            if header != EXPORT_HEADER:
                process.kill()
                raise RuntimeError(f"unexpected TAS exporter header: {header!r}")
            for line in process.stdout:
                product, ric, raw_ns, _, qualifiers = line.rstrip("\n").split(
                    "\t", 4
                )
                mappings = action_mappings(qualifiers)
                if mappings:
                    counts["mapped_status_rows"] += 1
                else:
                    counts["filtered_unmapped_rows"] += 1
                timestamp_ns = int(raw_ns)
                second_epoch = timestamp_ns // 1_000_000_000
                for mapping in mappings:
                    writer.writerow(
                        (
                            year,
                            product,
                            ric,
                            timestamp_ns,
                            second_epoch,
                            mapping.action,
                            mapping.is_inferred,
                            mapping.rule,
                            MAPPING_VERSION,
                            qualifiers,
                        )
                    )
                    counts["staged_action_rows"] += 1
            stderr = process.stderr.read()
            return_code = process.wait()
            if return_code:
                raise RuntimeError(
                    f"TAS exporter failed for year {year}: {stderr.strip()}"
                )
            exporter_counts = re.findall(
                r"product=(\w+) year=(\d+) scanned=(\d+) exported=(\d+) in_range=(\d+)",
                stderr,
            )
            if len(exporter_counts) != len(products):
                raise RuntimeError(
                    f"missing TAS exporter counts for year {year}: {stderr.strip()}"
                )
            counts["raw_status_rows"] += sum(
                int(in_range) for _, _, _, _, in_range in exporter_counts
            )
            counts["years_scanned"] += 1
            print(
                f"scanned year={year} products={','.join(sorted(products))} "
                f"raw_rows={counts['raw_status_rows']:,} "
                f"action_rows={counts['staged_action_rows']:,}",
                flush=True,
            )
    return dict(counts)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def psql_command(args: argparse.Namespace) -> list[str]:
    return [
        str(args.psql),
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        str(args.pg_socket),
        "-p",
        str(args.pg_port),
        "-U",
        args.pg_user,
        "-d",
        args.pg_database,
    ]


def load_postgres(
    args: argparse.Namespace,
    csv_path: Path,
    partitions: list[Partition],
    counts: dict[str, int],
) -> int:
    subprocess.run(psql_command(args) + ["-f", str(args.ddl)], check=True)
    products_sql = "ARRAY[" + ",".join(sql_literal(item) for item in PRODUCTS) + "]"
    available_years = sorted({partition.year for partition in partitions})
    periods = sorted({partition.period for partition in partitions})
    years_sql = "ARRAY[" + ",".join(str(year) for year in available_years) + "]"
    periods_sql = "ARRAY[" + ",".join(sql_literal(item) for item in periods) + "]"
    script = rf"""
BEGIN;
SET LOCAL TIME ZONE 'UTC';
CREATE TEMP TABLE cme_tas_action_stage (
    source_year smallint NOT NULL,
    product text NOT NULL,
    ric text NOT NULL,
    event_ts_ns bigint NOT NULL,
    event_second_epoch bigint NOT NULL,
    action text NOT NULL,
    is_inferred boolean NOT NULL,
    mapping_rule text NOT NULL,
    mapping_version text NOT NULL,
    qualifiers text NOT NULL
) ON COMMIT DROP;
\copy cme_tas_action_stage FROM {sql_literal(str(csv_path))} WITH (FORMAT csv)

TRUNCATE TABLE public.cme_tas_action_events;
INSERT INTO public.cme_tas_action_events (
    product, ric, source_year, action, event_second_utc, event_ts_utc,
    event_ts_ns, is_inferred, mapping_rule, mapping_version, qualifiers,
    source_event_count
)
SELECT
    product,
    ric,
    source_year,
    action,
    to_timestamp(event_second_epoch::double precision),
    to_timestamp((min(event_ts_ns) / 1000000000)::double precision)
        + ((min(event_ts_ns) % 1000000000) / 1000)::double precision
            * interval '1 microsecond',
    min(event_ts_ns),
    is_inferred,
    (array_agg(mapping_rule ORDER BY event_ts_ns, mapping_rule))[1],
    min(mapping_version),
    (array_agg(qualifiers ORDER BY event_ts_ns, qualifiers))[1],
    count(*)::integer
FROM cme_tas_action_stage
GROUP BY source_year, product, ric, action, event_second_epoch, is_inferred;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.cme_tas_action_events) THEN
        RAISE EXCEPTION 'refusing to commit an empty TAS action table';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.cme_tas_action_events
        WHERE is_inferred
            OR (action = 'NEW_PRICE_INDICATION' AND mapping_rule <> 'TRD_TYPE=15')
            OR (
                action = 'TRADING'
                AND mapping_rule <> 'TRD_TYPE=17;INST_PHASE=T'
            )
    ) THEN
        RAISE EXCEPTION 'direct TAS action mapping invariant failed';
    END IF;
END
$$;

TRUNCATE TABLE public.cme_tas_action_load_metadata;
INSERT INTO public.cme_tas_action_load_metadata (
    singleton_id, source_rocksdb, products, available_years, periods,
    start_year, end_year,
    partition_count, raw_status_rows, mapped_status_rows, staged_action_rows,
    stored_event_rows, mapping_version
)
SELECT
    1,
    {sql_literal(str(args.rocksdb))},
    {products_sql},
    {years_sql},
    {periods_sql},
    {min(available_years)},
    {max(available_years)},
    {len(partitions)},
    {counts.get('raw_status_rows', 0)},
    {counts.get('mapped_status_rows', 0)},
    {counts.get('staged_action_rows', 0)},
    count(*),
    {sql_literal(MAPPING_VERSION)}
FROM public.cme_tas_action_events;

ANALYZE public.cme_tas_action_events;
COMMIT;
"""
    subprocess.run(psql_command(args), input=script, text=True, check=True)
    result = subprocess.run(
        psql_command(args)
        + [
            "-Atc",
            "SELECT stored_event_rows FROM public.cme_tas_action_load_metadata",
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    return int(result.stdout.strip())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize CME TAS phase events and atomically load PostgreSQL"
    )
    parser.add_argument("--rocksdb", type=Path, default=DEFAULT_ROCKSDB)
    parser.add_argument("--exporter", type=Path, default=DEFAULT_EXPORTER)
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="first TAS year to materialize (default: Databento overlap start 2020)",
    )
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--write-pg", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--psql", type=Path, default=DEFAULT_PSQL)
    parser.add_argument("--pg-socket", type=Path, default=DEFAULT_PG_SOCKET)
    parser.add_argument("--pg-port", type=int, default=5433)
    parser.add_argument("--pg-user", default="u171")
    parser.add_argument("--pg-database", default="market_metadata")
    parser.add_argument("--ddl", type=Path, default=DEFAULT_DDL)
    parser.add_argument("--temp-dir", type=Path)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.write_pg != args.replace:
        parser.error("PostgreSQL replacement requires both --write-pg and --replace")
    for path, label in (
        (args.rocksdb, "RocksDB"),
        (args.exporter, "TAS exporter"),
    ):
        if not path.exists():
            parser.error(f"{label} does not exist: {path}")
    if args.write_pg and not args.psql.exists():
        parser.error(f"psql does not exist: {args.psql}")

    partitions = discover_partitions(
        args.exporter, args.rocksdb, args.start_year, args.end_year
    )
    if not partitions:
        raise RuntimeError("no selected TAS product partitions found")
    print(
        f"discovered partitions={len(partitions)} "
        f"years={','.join(str(year) for year in sorted({item.year for item in partitions}))}",
        flush=True,
    )

    temp_dir = str(args.temp_dir) if args.temp_dir else None
    with tempfile.NamedTemporaryFile(
        prefix="cme_tas_actions_", suffix=".csv", dir=temp_dir, delete=False
    ) as temporary:
        csv_path = Path(temporary.name)
    try:
        counts = scan_to_csv(args.exporter, args.rocksdb, partitions, csv_path)
        print(
            "scan complete "
            f"raw_status_rows={counts.get('raw_status_rows', 0):,} "
            f"mapped_status_rows={counts.get('mapped_status_rows', 0):,} "
            f"staged_action_rows={counts.get('staged_action_rows', 0):,}",
            flush=True,
        )
        if args.write_pg:
            stored_rows = load_postgres(args, csv_path, partitions, counts)
            print(f"PostgreSQL load complete stored_event_rows={stored_rows:,}")
        else:
            print("dry run only; pass --write-pg --replace to replace PostgreSQL data")
    finally:
        csv_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
