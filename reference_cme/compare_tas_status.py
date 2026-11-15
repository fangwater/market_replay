from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
from typing import Iterable

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent
PRODUCTS = ("ES", "NQ", "RTY", "YM", "GC", "CL")
EQUITY_PRODUCTS = frozenset(("ES", "NQ", "RTY", "YM"))
MONTH_CODES = "FGHJKMNQUVXZ"
DEFAULT_STATUS_PATH = (
    PROJECT_ROOT / "parsed/core_six_status_2020_present/status_outrights.parquet"
)
DEFAULT_DEFINITION_PATH = (
    PROJECT_ROOT / "parsed/core_six_definition_2020_present/definition_outrights.parquet"
)
DEFAULT_STATUS_SUMMARY_PATH = DEFAULT_STATUS_PATH.with_name("summary.json")
DEFAULT_TAS_DB = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products"
)
DEFAULT_TAS_EXPORTER = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/cargo-target/mkt_signal/release/cme_tas_export_status"
)
DEFAULT_UNPARSED_LOG = Path(
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/logs/cme_tas_replay_all_unparsed.log"
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "reports"
DEFAULT_TAS_END = "2026-06-01"
QUALIFIER_PATTERN = re.compile(r"(?:^|;)([^;]*)\[([^\]]+)\]")
TAS_RIC_PATTERN = re.compile(
    rf"^({'|'.join(PRODUCTS)})([{MONTH_CODES}])(\d{{1,2}})(?:\^.*)?$"
)
UNPARSED_RIC_PATTERN = re.compile(
    rf"#RIC=({'|'.join(PRODUCTS)})[{MONTH_CODES}]\d{{1,2}}(?:\^\S*)?(?:\s|$)"
)
SESSION_CATEGORIES = ("close", "pre_open", "open")
CATEGORY_LABELS = {
    "close": "收盘",
    "pre_open": "预开盘",
    "no_cancel": "禁止撤单",
    "open": "进入交易",
    "new_price": "新价格指示时刻",
    "implied_on": "隐含撮合开启",
    "implied_off": "隐含撮合关闭",
    "halt_or_unavailable": "停盘/不可交易",
}


@dataclass
class EventIndex:
    family: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    contract: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    any_family: dict[str, set[int]] = field(default_factory=lambda: defaultdict(set))
    any_contract: dict[str, set[int]] = field(default_factory=lambda: defaultdict(set))
    exception_family: dict[str, set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    exception_contract: dict[str, set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    scheduled_family: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    scheduled_contract: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )


def utc_datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def epoch_ns(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000_000


def qualifier_fields(qualifiers: str) -> dict[str, set[str]]:
    fields: dict[str, set[str]] = defaultdict(set)
    for raw_value, name in QUALIFIER_PATTERN.findall(qualifiers):
        value = raw_value.strip().strip('"').strip()
        fields[name].add(value)
    return fields


def tas_categories(qualifiers: str) -> set[str]:
    fields = qualifier_fields(qualifiers)
    trade_types = fields.get("TRD_TYPE", set())
    phases = fields.get("INST_PHASE", set())
    order_states = fields.get("ORD_ENT_ST", set())
    security_states = fields.get("SECUR_ST", set())
    categories: set[str] = set()
    if "4" in trade_types and "C" in phases:
        categories.add("close")
    if "21" in trade_types and "O" in phases:
        categories.add("pre_open")
    if "N" in order_states:
        categories.add("no_cancel")
    if "15" in trade_types or ("17" in trade_types and "T" in phases):
        categories.add("open")
    if "5" in security_states:
        categories.add("implied_on")
    if "6" in security_states:
        categories.add("implied_off")
    halt_reasons = fields.get("HALT_REASN", set())
    if any(value not in ("", "0") for value in halt_reasons):
        categories.add("halt_or_unavailable")
    return categories


def meaningful_qualifiers(qualifiers: str) -> bool:
    return any(
        value
        for values in qualifier_fields(qualifiers).values()
        for value in values
    )


def tas_contract_key(product: str, ric: str, event_year: int) -> str | None:
    match = TAS_RIC_PATTERN.fullmatch(ric)
    if not match or match.group(1) != product:
        return None
    month = match.group(2)
    digits = match.group(3)
    if len(digits) == 2:
        maturity_year = 2000 + int(digits)
    else:
        digit = int(digits)
        maturity_year = next(
            year
            for year in range(event_year - 1, event_year + 10)
            if year % 10 == digit
        )
    return f"{product}:{month}:{maturity_year}"


def databento_categories(
    action: str, reason: str, trading_event: str
) -> tuple[set[str], bool]:
    categories: set[str] = set()
    if action == "CLOSE":
        categories.add("close")
    if action == "PRE_OPEN" and trading_event != "NO_CANCEL":
        categories.add("pre_open")
    if trading_event == "NO_CANCEL":
        categories.add("no_cancel")
    if action == "TRADING":
        categories.add("open")
    if action == "NEW_PRICE_INDICATION":
        categories.add("new_price")
    if trading_event == "IMPLIED_MATCHING_ON":
        categories.add("implied_on")
    if trading_event == "IMPLIED_MATCHING_OFF":
        categories.add("implied_off")
    if action in ("HALT", "NOT_AVAILABLE_FOR_TRADING"):
        categories.add("halt_or_unavailable")
    is_exception = reason not in ("SCHEDULED", "NONE", "INSTRUMENT_EXPIRATION")
    return categories, is_exception


def match_count(source: Iterable[int], target: Iterable[int], tolerance: int) -> int:
    left = sorted(source)
    right = sorted(target)
    source_index = 0
    target_index = 0
    matches = 0
    while source_index < len(left) and target_index < len(right):
        source_value = left[source_index]
        target_value = right[target_index]
        if abs(source_value - target_value) <= tolerance:
            matches += 1
            source_index += 1
            target_index += 1
        elif source_value < target_value - tolerance:
            source_index += 1
        else:
            target_index += 1
    return matches


def unmatched_targets(
    source: Iterable[int], target: Iterable[int], tolerance: int, limit: int = 5
) -> list[int]:
    source_values = sorted(source)
    missing: list[int] = []
    for value in sorted(target):
        position = bisect_left(source_values, value - tolerance)
        if position >= len(source_values) or source_values[position] > value + tolerance:
            missing.append(value)
            if len(missing) >= limit:
                break
    return missing


def seconds_in_range(values: Iterable[int], start: int, end: int) -> set[int]:
    return {value for value in values if start <= value < end}


def comparison_metric(
    source: dict[object, set[int]],
    target: dict[object, set[int]],
    keys: Iterable[object],
    tolerance: int,
    start: int | None = None,
    end: int | None = None,
) -> dict[str, float | int]:
    source_events = 0
    target_events = 0
    exact_matches = 0
    tolerance_matches = 0
    for key in keys:
        left = source.get(key, set())
        right = target.get(key, set())
        if start is not None and end is not None:
            left = seconds_in_range(left, start, end)
            right = seconds_in_range(right, start, end)
        source_events += len(left)
        target_events += len(right)
        exact_matches += match_count(left, right, 0)
        tolerance_matches += match_count(left, right, tolerance)
    return {
        "tasEvents": source_events,
        "databentoEvents": target_events,
        "exactMatches": exact_matches,
        "matchesWithinTolerance": tolerance_matches,
        "exactRecallPct": round(100 * exact_matches / target_events, 4)
        if target_events
        else 100.0,
        "recallWithinTolerancePct": round(100 * tolerance_matches / target_events, 4)
        if target_events
        else 100.0,
    }


def build_definition_map(path: Path) -> dict[tuple[int, str], str]:
    connection = duckdb.connect(":memory:")
    rows = connection.execute(
        """
        SELECT instrument_id, raw_symbol, asset, maturity_year
        FROM read_parquet(?)
        GROUP BY ALL
        """,
        [str(path)],
    ).fetchall()
    connection.close()
    result: dict[tuple[int, str], str] = {}
    for instrument_id, symbol, product, maturity_year in rows:
        key = (int(instrument_id), str(symbol))
        canonical = f"{product}:{str(symbol)[len(product)]}:{int(maturity_year)}"
        previous = result.setdefault(key, canonical)
        if previous != canonical:
            raise RuntimeError(f"ambiguous definition identity for {key}: {previous}, {canonical}")
    return result


def load_tas(
    exporter: Path,
    rocksdb_dir: Path,
    product_starts: dict[str, datetime],
    end: datetime,
) -> tuple[EventIndex, dict[str, dict[str, int]], dict[str, int]]:
    index = EventIndex()
    scan_counts: dict[str, dict[str, int]] = {}
    diagnostics = Counter()
    for year in range(min(value.year for value in product_starts.values()), end.year + 1):
        year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
        year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        range_end = min(year_end, end)
        products = [
            product
            for product in PRODUCTS
            if max(year_start, product_starts[product]) < range_end
        ]
        if not products:
            continue
        range_start = min(max(year_start, product_starts[product]) for product in products)
        command = [
            str(exporter),
            "--rocksdb-dir",
            str(rocksdb_dir),
            "--year",
            str(year),
            "--start-ns",
            str(epoch_ns(range_start)),
            "--end-ns",
            str(epoch_ns(range_end)),
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
        if header != "product\tric\tts_utc_ns\texch_hms_ns\tqualifiers":
            process.kill()
            raise RuntimeError(f"unexpected TAS exporter header: {header!r}")
        for line in process.stdout:
            product, ric, raw_ns, _, qualifiers = line.rstrip("\n").split("\t", 4)
            second = int(raw_ns) // 1_000_000_000
            contract = tas_contract_key(
                product, ric, datetime.fromtimestamp(second, timezone.utc).year
            )
            diagnostics["rows"] += 1
            if contract is None:
                diagnostics["unmappedContracts"] += 1
            meaningful = meaningful_qualifiers(qualifiers)
            if meaningful:
                index.any_family[product].add(second)
                if contract:
                    index.any_contract[contract].add(second)
            else:
                diagnostics["shadowRows"] += 1
            for category in tas_categories(qualifiers):
                index.family[product, category].add(second)
                if contract:
                    index.contract[contract, category].add(second)
        stderr = process.stderr.read()
        return_code = process.wait()
        if return_code:
            raise RuntimeError(f"TAS exporter failed for {year}: {stderr.strip()}")
        for match in re.finditer(
            r"product=(\w+) year=(\d+) scanned=(\d+) exported=(\d+)", stderr
        ):
            product, scan_year, scanned, exported = match.groups()
            scan_counts[f"{scan_year}:{product}"] = {
                "scanned": int(scanned),
                "exported": int(exported),
            }
    return index, scan_counts, dict(diagnostics)


def load_databento(
    status_path: Path,
    definition_path: Path,
    product_starts: dict[str, datetime],
    end: datetime,
) -> tuple[EventIndex, dict[str, int]]:
    definitions = build_definition_map(definition_path)
    index = EventIndex()
    diagnostics = Counter()
    connection = duckdb.connect(":memory:")
    equity_start = min(product_starts[product] for product in EQUITY_PRODUCTS)
    commodity_start = min(product_starts[product] for product in ("GC", "CL"))
    cursor = connection.execute(
        """
        SELECT product, instrument_id, symbol, epoch_ns(ts_event) // 1000000000,
               action_name, reason_name, trading_event_name,
               ts_recv = date_trunc('day', ts_recv) AS is_midnight_snapshot
        FROM read_parquet(?)
        WHERE (
                (product IN ('ES', 'NQ', 'RTY', 'YM') AND ts_recv >= ?)
             OR (product IN ('GC', 'CL') AND ts_recv >= ?)
              )
          AND ts_recv < ?
        ORDER BY ts_recv, product, symbol
        """,
        [str(status_path), equity_start, commodity_start, end],
    )
    while rows := cursor.fetchmany(100_000):
        for (
            product,
            instrument_id,
            symbol,
            second,
            action,
            reason,
            trading_event,
            is_midnight_snapshot,
        ) in rows:
            diagnostics["rows"] += 1
            if is_midnight_snapshot:
                diagnostics["midnightSnapshotsExcluded"] += 1
                continue
            contract = definitions.get((int(instrument_id), str(symbol)))
            if contract is None:
                diagnostics["unmappedContracts"] += 1
            categories, is_exception = databento_categories(
                str(action), str(reason), str(trading_event)
            )
            second = int(second)
            index.any_family[str(product)].add(second)
            if contract:
                index.any_contract[contract].add(second)
            if is_exception:
                index.exception_family[str(product)].add(second)
                if contract:
                    index.exception_contract[contract].add(second)
            for category in categories:
                index.family[str(product), category].add(second)
                if contract:
                    index.contract[contract, category].add(second)
                if reason == "SCHEDULED":
                    index.scheduled_family[str(product), category].add(second)
                    if contract:
                        index.scheduled_contract[contract, category].add(second)
    connection.close()
    return index, dict(diagnostics)


def category_metric(
    tas: EventIndex,
    databento: EventIndex,
    product: str | None,
    category: str,
    tolerance: int,
    scheduled: bool,
    level: str,
) -> dict[str, float | int]:
    source_category = "open" if category == "new_price" else category
    source = tas.family if level == "family" else tas.contract
    target_index = (
        databento.scheduled_family
        if scheduled and level == "family"
        else databento.scheduled_contract
        if scheduled
        else databento.family
        if level == "family"
        else databento.contract
    )
    if level == "family":
        products = (product,) if product else PRODUCTS
        source_sets = {(item, category): source.get((item, source_category), set()) for item in products}
        target_sets = {(item, category): target_index.get((item, category), set()) for item in products}
        keys: list[object] = [(item, category) for item in products]
    else:
        target_keys = [
            key
            for key in target_index
            if key[1] == category
            and (product is None or str(key[0]).startswith(f"{product}:"))
        ]
        source_sets = {
            (key[0], category): source.get((key[0], source_category), set())
            for key in target_keys
        }
        target_sets = {(key[0], category): target_index[key] for key in target_keys}
        keys = list(target_sets)
    return comparison_metric(source_sets, target_sets, keys, tolerance)


def presence_metric(
    tas: EventIndex,
    databento: EventIndex,
    product: str | None,
    tolerance: int,
    exception_only: bool = False,
    level: str = "family",
    start: int | None = None,
    end: int | None = None,
) -> dict[str, float | int]:
    source = tas.any_family if level == "family" else tas.any_contract
    if exception_only:
        target = (
            databento.exception_family
            if level == "family"
            else databento.exception_contract
        )
    else:
        target = databento.any_family if level == "family" else databento.any_contract
    if level == "family":
        keys: list[object] = [product] if product else list(PRODUCTS)
    else:
        keys = [
            key
            for key in target
            if product is None or str(key).startswith(f"{product}:")
        ]
    return comparison_metric(source, target, keys, tolerance, start, end)


def sum_metrics(metrics: Iterable[dict[str, float | int]]) -> dict[str, float | int]:
    metrics = list(metrics)
    result = {
        key: sum(int(metric[key]) for metric in metrics)
        for key in (
            "tasEvents",
            "databentoEvents",
            "exactMatches",
            "matchesWithinTolerance",
        )
    }
    denominator = result["databentoEvents"]
    result["exactRecallPct"] = (
        round(100 * result["exactMatches"] / denominator, 4) if denominator else 100.0
    )
    result["recallWithinTolerancePct"] = (
        round(100 * result["matchesWithinTolerance"] / denominator, 4)
        if denominator
        else 100.0
    )
    return result


def known_unparsed_statuses(path: Path) -> dict[str, int]:
    counts = Counter()
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", errors="replace") as source:
        for line in source:
            if "period=2025-01-01_2026-01-01" not in line or "reason=status Qualifiers" not in line:
                continue
            match = UNPARSED_RIC_PATTERN.search(line)
            if match:
                counts[match.group(1)] += 1
    return dict(sorted(counts.items()))


def iso_second(value: int) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def missing_samples(
    tas: EventIndex,
    databento: EventIndex,
    category: str,
    tolerance: int,
    scheduled: bool,
) -> dict[str, list[str]]:
    source_category = "open" if category == "new_price" else category
    target = databento.scheduled_family if scheduled else databento.family
    result: dict[str, list[str]] = {}
    for product in PRODUCTS:
        missing = unmatched_targets(
            tas.family.get((product, source_category), set()),
            target.get((product, category), set()),
            tolerance,
        )
        if missing:
            result[product] = [iso_second(value) for value in missing]
    return result


def build_report(
    tas: EventIndex,
    databento: EventIndex,
    product_starts: dict[str, datetime],
    end: datetime,
    tolerance: int,
    tas_scans: dict[str, dict[str, int]],
    tas_diagnostics: dict[str, int],
    databento_diagnostics: dict[str, int],
    unparsed: dict[str, int],
) -> dict[str, object]:
    scheduled_family: dict[str, dict[str, object]] = {}
    all_family: dict[str, dict[str, object]] = {}
    scheduled_contract: dict[str, dict[str, object]] = {}
    by_product: dict[str, object] = {}
    for category in CATEGORY_LABELS:
        scheduled_family[category] = category_metric(
            tas, databento, None, category, tolerance, True, "family"
        )
        all_family[category] = category_metric(
            tas, databento, None, category, tolerance, False, "family"
        )
        scheduled_contract[category] = category_metric(
            tas, databento, None, category, tolerance, True, "contract"
        )
    for product in PRODUCTS:
        product_categories = {
            category: category_metric(
                tas, databento, product, category, tolerance, True, "family"
            )
            for category in CATEGORY_LABELS
        }
        by_product[product] = {
            "coverageStart": product_starts[product].date().isoformat(),
            "coverageEndExclusive": end.date().isoformat(),
            "scheduledFamily": product_categories,
            "coreSession": sum_metrics(
                product_categories[category] for category in SESSION_CATEGORIES
            ),
            "allStatusTimepointPresence": presence_metric(
                tas, databento, product, tolerance
            ),
            "exceptionTimepointPresence": presence_metric(
                tas, databento, product, tolerance, exception_only=True
            ),
        }

    by_year: dict[str, object] = {}
    for year in range(min(value.year for value in product_starts.values()), end.year + 1):
        start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
        stop = int(min(datetime(year + 1, 1, 1, tzinfo=timezone.utc), end).timestamp())
        if start >= stop:
            continue
        active_products = [
            product
            for product in PRODUCTS
            if product_starts[product].timestamp() < stop
        ]
        by_year[str(year)] = {
            "products": active_products,
            "allStatusTimepointPresence": comparison_metric(
                tas.any_family,
                databento.any_family,
                active_products,
                tolerance,
                start,
                stop,
            ),
        }

    core_session = sum_metrics(
        scheduled_family[category] for category in SESSION_CATEGORIES
    )
    all_timepoints = presence_metric(tas, databento, None, tolerance)
    contract_timepoints = presence_metric(
        tas, databento, None, tolerance, level="contract"
    )
    exception_timepoints = presence_metric(
        tas, databento, None, tolerance, exception_only=True
    )
    report: dict[str, object] = {
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "method": {
            "toleranceSeconds": tolerance,
            "timeBasis": "Databento ts_event versus LSEG Date-Time, compared in UTC",
            "deduplication": "unique event seconds per product/category or canonical contract/category",
            "contractNormalization": "Databento instrument_id + maturity_year; LSEG one- or two-digit RIC maturity year",
            "databentoStatusFields": {
                "action": [
                    "NONE",
                    "PRE_OPEN",
                    "TRADING",
                    "NEW_PRICE_INDICATION",
                    "CLOSE",
                    "NOT_AVAILABLE_FOR_TRADING",
                    "HALT",
                ],
                "tradingEvent": [
                    "NONE",
                    "CHANGE_TRADING_SESSION",
                    "NO_CANCEL",
                    "IMPLIED_MATCHING_ON",
                    "IMPLIED_MATCHING_OFF",
                ],
                "reason": [
                    "NONE",
                    "SCHEDULED",
                    "MARKET_EVENT",
                    "SURVEILLANCE_INTERVENTION",
                    "RECOVERY_IN_PROCESS",
                    "INSTRUMENT_EXPIRATION",
                ],
                "stateFlags": ["is_trading", "is_quoting"],
            },
            "tasQualifierMapping": {
                "4[TRD_TYPE] + C[INST_PHASE]": "CLOSE",
                "21[TRD_TYPE] + O[INST_PHASE]": "PRE_OPEN",
                "N[ORD_ENT_ST]": "NO_CANCEL",
                "15[TRD_TYPE] or 17[TRD_TYPE] + T[INST_PHASE]": "TRADING/open boundary; also used to infer NEW_PRICE_INDICATION time",
                "5[SECUR_ST]": "IMPLIED_MATCHING_ON heuristic",
                "6[SECUR_ST]": "IMPLIED_MATCHING_OFF heuristic",
                "nonzero HALT_REASN": "subset of HALT/NOT_AVAILABLE_FOR_TRADING",
            },
            "excludedDatabentoMidnightSnapshots": databento_diagnostics.get(
                "midnightSnapshotsExcluded", 0
            ),
        },
        "coverage": {
            product: {
                "start": product_starts[product].isoformat().replace("+00:00", "Z"),
                "endExclusive": end.isoformat().replace("+00:00", "Z"),
            }
            for product in PRODUCTS
        },
        "summary": {
            "coreScheduledSessionFamily": core_session,
            "scheduledNoCancelFamily": scheduled_family["no_cancel"],
            "scheduledNewPriceTimeFamily": scheduled_family["new_price"],
            "scheduledImpliedMatchingFamily": sum_metrics(
                (
                    scheduled_family["implied_on"],
                    scheduled_family["implied_off"],
                )
            ),
            "allStatusTimepointPresenceFamily": all_timepoints,
            "allStatusTimepointPresenceContract": contract_timepoints,
            "exceptionTimepointPresenceFamily": exception_timepoints,
        },
        "scheduledFamilyByCategory": scheduled_family,
        "allReasonFamilyByCategory": all_family,
        "scheduledContractByCategory": scheduled_contract,
        "byProduct": by_product,
        "byYear": by_year,
        "knownTasUnparsedStatusRows2025": {
            "byProduct": unparsed,
            "total": sum(unparsed.values()),
        },
        "diagnostics": {
            "tas": tas_diagnostics,
            "databento": databento_diagnostics,
            "tasScans": tas_scans,
        },
        "missingSamples": {
            category: missing_samples(
                tas, databento, category, tolerance, category != "halt_or_unavailable"
            )
            for category in CATEGORY_LABELS
        },
        "limitations": [
            "TAS Mkt. Condition is a vendor qualifier stream, not the Databento status schema.",
            "An LSEG open transition identifies the open second but cannot separate Databento NEW_PRICE_INDICATION from TRADING.",
            "SECUR_ST 5/6 to implied matching on/off is an observed heuristic and is not complete across products or years.",
            "HALT_REASN identifies only a subset of Databento HALT/NOT_AVAILABLE_FOR_TRADING events and does not recover Databento reason taxonomy.",
            "Event-time presence only proves a TAS status exists near a Databento event; it does not prove field-for-field semantic identity.",
        ],
    }
    return report


def format_metric(metric: dict[str, object]) -> str:
    return (
        f"{metric['matchesWithinTolerance']:,}/{metric['databentoEvents']:,} "
        f"({metric['recallWithinTolerancePct']:.2f}%)"
    )


def render_markdown(report: dict[str, object]) -> str:
    summary = report["summary"]
    method = report["method"]
    lines = [
        "# TAS `Mkt. Condition` 与 Databento `status` 比较",
        "",
        f"生成时间：`{report['generatedAt']}`。比较容差：`±{method['toleranceSeconds']} 秒`。",
        "Databento 使用交易所 `ts_event`，TAS 使用 LSEG `Date-Time`；已排除 "
        f"`{method['excludedDatabentoMidnightSnapshots']:,}` 条 UTC 午夜状态快照。",
        "",
        "## 结论",
        "",
        "| 能力 | 可还原 | 结论 |",
        "| --- | ---: | --- |",
        f"| 核心计划交易时段（收盘/预开盘/开盘） | {format_metric(summary['coreScheduledSessionFamily'])} | 可用于核验交易时段边界 |",
        f"| 计划内禁止撤单阶段 | {format_metric(summary['scheduledNoCancelFamily'])} | 部分产品缺失，不可作为统一规则 |",
        f"| 新价格指示发生时刻 | {format_metric(summary['scheduledNewPriceTimeFamily'])} | 时刻可推断，但无法与 TRADING 分成两个独立事件 |",
        f"| 隐含撮合开关 | {format_metric(summary['scheduledImpliedMatchingFamily'])} | 仅部分可由 SECUR_ST 5/6 推断 |",
        f"| 全部状态事件时点（品种级，仅证明附近有状态） | {format_metric(summary['allStatusTimepointPresenceFamily'])} | 不能代表字段语义完全一致 |",
        f"| 全部状态事件时点（合约级） | {format_metric(summary['allStatusTimepointPresenceContract'])} | 同时要求到期合约身份一致 |",
        f"| 非计划事件时点（品种级） | {format_metric(summary['exceptionTimepointPresenceFamily'])} | 不足以完整重放停牌原因和恢复状态 |",
        "",
        "## 重叠范围",
        "",
        "- `ES/NQ/RTY/YM`：2024-01-01 至 2026-06-01（右开）。",
        "- `GC/CL`：2020-01-01 至 2026-06-01（右开）。",
        "- 合约代码按到期年月归一，例如 Databento `ESH4` 对应 LSEG `ESH24`。",
        "",
        "## Databento 中有哪些状态事件",
        "",
        "- `action`：`PRE_OPEN`（预开盘）、`TRADING`（交易）、`NEW_PRICE_INDICATION`（新价格指示）、`CLOSE`（收盘）、`NOT_AVAILABLE_FOR_TRADING`（不可交易）、`HALT`（停盘）；`NONE` 表示主 action 不变。",
        "- `trading_event`：`CHANGE_TRADING_SESSION`（交易时段切换）、`NO_CANCEL`（禁止撤单）、`IMPLIED_MATCHING_ON/OFF`（隐含撮合开/关）和 `NONE`。",
        "- `reason`：`SCHEDULED`、`MARKET_EVENT`、`SURVEILLANCE_INTERVENTION`、`RECOVERY_IN_PROCESS`、`INSTRUMENT_EXPIRATION` 和 `NONE`。",
        "- 状态位：`is_trading`、`is_quoting`；短卖限制在这六个期货品种中没有可用取值。",
        "",
        "## TAS 如何对应",
        "",
        "| TAS `Qualifiers` | Databento 事件 | 映射性质 |",
        "| --- | --- | --- |",
        "| `4[TRD_TYPE]` + `C[INST_PHASE]` | `CLOSE` | 直接对应 |",
        "| `21[TRD_TYPE]` + `O[INST_PHASE]` | `PRE_OPEN` | 直接对应 |",
        "| `N[ORD_ENT_ST]` | `NO_CANCEL` | 直接对应，但指数品种覆盖不全 |",
        "| `15[TRD_TYPE]`，或 `17[TRD_TYPE]` + `T[INST_PHASE]` | `TRADING` / `NEW_PRICE_INDICATION` 时点 | 只能还原共同开盘时点，不能拆成两个事件 |",
        "| `5/6[SECUR_ST]` | `IMPLIED_MATCHING_ON/OFF` | 经验映射，覆盖不全 |",
        "| 非零 `HALT_REASN` | `HALT` / `NOT_AVAILABLE_FOR_TRADING` | 只能命中一部分，原因码不等价 |",
        "",
        "## 按品种",
        "",
        "| 品种 | 核心计划时段 | 禁止撤单 | 全状态时点 | 非计划事件时点 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for product in PRODUCTS:
        item = report["byProduct"][product]
        lines.append(
            f"| {product} | {format_metric(item['coreSession'])} | "
            f"{format_metric(item['scheduledFamily']['no_cancel'])} | "
            f"{format_metric(item['allStatusTimepointPresence'])} | "
            f"{format_metric(item['exceptionTimepointPresence'])} |"
        )
    lines.extend(
        [
            "",
            "## 计划事件明细",
            "",
            "| 事件 | 品种级 | 合约级 |",
            "| --- | ---: | ---: |",
        ]
    )
    for category, label in CATEGORY_LABELS.items():
        lines.append(
            f"| {label} | {format_metric(report['scheduledFamilyByCategory'][category])} | "
            f"{format_metric(report['scheduledContractByCategory'][category])} |"
        )
    unparsed = report["knownTasUnparsedStatusRows2025"]
    by_product = ", ".join(
        f"{product}={count:,}" for product, count in unparsed["byProduct"].items()
    )
    lines.extend(
        [
            "",
            "## 已知缺口",
            "",
            f"- 2025 TAS replay 有 `{unparsed['total']:,}` 条六品种超长 `Mkt. Condition` 未写入 RocksDB（{by_product}）。",
            "- `ES/NQ/RTY` 的 TAS 状态通常没有每日 `NO_CANCEL`；`YM` 也只覆盖部分年份。",
            "- TAS 没有 Databento 的结构化 `reason`，无法可靠区分计划、市场事件、监控干预、恢复和到期。",
            "- 比较按绝对 UTC 时间完成；TAS 的 Chicago 17:00 `TradDay` 标签不能直接与 Databento UTC 接收日期比较。",
            "",
            "## 使用边界",
            "",
            "TAS 足以高覆盖还原六品种的计划交易时段边界，但不能替代完整 Databento 状态流。"
            "回测开市掩码继续以 `cme_globex_daily_trading_intervals` 为准；TAS 状态用于旁证，Databento 用于结构化状态和异常事件核验。",
            "",
            "完整机器可读指标、逐年结果、诊断计数和缺失样本见 `tas_status_comparison.json`。",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare TAS Mkt. Condition events with Databento status records."
    )
    parser.add_argument("--status-parquet", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--definition-parquet", type=Path, default=DEFAULT_DEFINITION_PATH)
    parser.add_argument("--status-summary", type=Path, default=DEFAULT_STATUS_SUMMARY_PATH)
    parser.add_argument("--tas-rocksdb", type=Path, default=DEFAULT_TAS_DB)
    parser.add_argument("--tas-exporter", type=Path, default=DEFAULT_TAS_EXPORTER)
    parser.add_argument("--tas-end", default=DEFAULT_TAS_END)
    parser.add_argument("--unparsed-log", type=Path, default=DEFAULT_UNPARSED_LOG)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--tolerance-seconds", type=int, default=2)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.tolerance_seconds < 0:
        raise SystemExit("--tolerance-seconds must be non-negative")
    for path in (
        args.status_parquet,
        args.definition_parquet,
        args.status_summary,
        args.tas_rocksdb,
        args.tas_exporter,
    ):
        if not path.exists():
            raise SystemExit(f"required input does not exist: {path}")
    status_summary = json.loads(args.status_summary.read_text(encoding="utf-8"))
    product_starts = {
        product: utc_datetime(
            status_summary["coverage"][
                "equity" if product in EQUITY_PRODUCTS else "commodity"
            ]["ts_recv_min"]
        )
        for product in PRODUCTS
    }
    end = utc_datetime(args.tas_end)
    tas, tas_scans, tas_diagnostics = load_tas(
        args.tas_exporter, args.tas_rocksdb, product_starts, end
    )
    databento, databento_diagnostics = load_databento(
        args.status_parquet, args.definition_parquet, product_starts, end
    )
    report = build_report(
        tas,
        databento,
        product_starts,
        end,
        args.tolerance_seconds,
        tas_scans,
        tas_diagnostics,
        databento_diagnostics,
        known_unparsed_statuses(args.unparsed_log),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "tas_status_comparison.json"
    markdown_path = args.output_dir / "tas_status_comparison.md"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    print(markdown_path)
    print(json_path)


if __name__ == "__main__":
    main()
