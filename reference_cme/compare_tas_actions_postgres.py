from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from typing import Iterable

import duckdb

from compare_tas_status import build_definition_map, match_count, tas_contract_key


PROJECT_ROOT = Path(__file__).resolve().parent
PRODUCTS = ("ES", "NQ", "RTY", "YM", "GC", "CL")
EQUITY_PRODUCTS = frozenset(("ES", "NQ", "RTY", "YM"))
ACTIONS = ("CLOSE", "PRE_OPEN", "TRADING", "NEW_PRICE_INDICATION")
ACTION_LABELS = {
    "CLOSE": "收盘",
    "PRE_OPEN": "预开盘",
    "TRADING": "交易",
    "NEW_PRICE_INDICATION": "新价格指示",
}
DEFAULT_STATUS = (
    PROJECT_ROOT / "parsed/core_six_status_2020_present/status_outrights.parquet"
)
DEFAULT_DEFINITION = (
    PROJECT_ROOT / "parsed/core_six_definition_2020_present/definition_outrights.parquet"
)
DEFAULT_PSQL = Path("/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql")
DEFAULT_PG_SOCKET = Path(
    "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run"
)
DEFAULT_OUTPUT_PREFIX = PROJECT_ROOT / "reports/tas_action_pg_comparison"


@dataclass
class ActionIndex:
    family: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    contract: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )


def align_contract_actions(
    direct: dict[tuple[str, str], set[int]], cluster_seconds: int = 5
) -> dict[tuple[str, str], set[int]]:
    aligned: dict[tuple[str, str], set[int]] = defaultdict(set)
    entities = {entity for entity, _ in direct}
    for entity in entities:
        for action in ("CLOSE", "PRE_OPEN"):
            aligned[entity, action].update(direct.get((entity, action), set()))
        open_points = sorted(
            direct.get((entity, "NEW_PRICE_INDICATION"), set())
            | direct.get((entity, "TRADING"), set())
        )
        cluster_start: int | None = None
        for point in open_points:
            if cluster_start is None or point - cluster_start > cluster_seconds:
                if cluster_start is not None:
                    aligned[entity, "NEW_PRICE_INDICATION"].add(cluster_start)
                    aligned[entity, "TRADING"].add(cluster_start)
                cluster_start = point
        if cluster_start is not None:
            aligned[entity, "NEW_PRICE_INDICATION"].add(cluster_start)
            aligned[entity, "TRADING"].add(cluster_start)
    return aligned


def utc_datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


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


def postgres_where(equity_start: datetime, commodity_start: datetime, end: datetime) -> str:
    equity = equity_start.isoformat()
    commodity = commodity_start.isoformat()
    stop = end.isoformat()
    return f"""
event_second_utc < TIMESTAMPTZ '{stop}'
AND (
    (product IN ('ES', 'NQ', 'RTY', 'YM')
        AND event_second_utc >= TIMESTAMPTZ '{equity}')
    OR (product IN ('GC', 'CL')
        AND event_second_utc >= TIMESTAMPTZ '{commodity}')
)
"""


def load_postgres(
    args: argparse.Namespace,
    equity_start: datetime,
    commodity_start: datetime,
    end: datetime,
) -> tuple[ActionIndex, dict[str, object]]:
    where = postgres_where(equity_start, commodity_start, end)
    query = f"""
COPY (
    SELECT product, ric, EXTRACT(EPOCH FROM event_second_utc)::bigint, action
    FROM public.cme_tas_action_events
    WHERE {where}
    ORDER BY event_second_utc, product, ric, action
) TO STDOUT WITH (FORMAT text)
"""
    process = subprocess.Popen(
        psql_command(args) + ["-q", "-c", query],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("failed to capture PostgreSQL action rows")
    index = ActionIndex()
    diagnostics = Counter()
    for line in process.stdout:
        product, ric, raw_second, action = line.rstrip("\n").split("\t", 3)
        second = int(raw_second)
        diagnostics["rows"] += 1
        contract = tas_contract_key(
            product, ric, datetime.fromtimestamp(second, timezone.utc).year
        )
        if contract is None:
            diagnostics["unmappedContracts"] += 1
        else:
            index.contract[contract, action].add(second)
    stderr = process.stderr.read()
    return_code = process.wait()
    if return_code:
        raise RuntimeError(f"PostgreSQL TAS read failed: {stderr.strip()}")
    index.contract = align_contract_actions(index.contract)

    alignment_query = f"""
COPY (
    SELECT product, EXTRACT(EPOCH FROM event_second_utc)::bigint, action
    FROM public.cme_tas_action_alignment
    WHERE {where}
    ORDER BY event_second_utc, product, action
) TO STDOUT WITH (FORMAT text)
"""
    alignment = subprocess.run(
        psql_command(args) + ["-q", "-c", alignment_query],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in alignment.stdout.splitlines():
        product, raw_second, action = line.split("\t", 2)
        index.family[product, action].add(int(raw_second))
        diagnostics["familyAlignmentRows"] += 1

    metadata_query = """
SELECT json_build_object(
    'sourceRocksdb', source_rocksdb,
    'products', products,
    'availableYears', available_years,
    'periods', periods,
    'startYear', start_year,
    'endYear', end_year,
    'partitionCount', partition_count,
    'rawStatusRows', raw_status_rows,
    'mappedStatusRows', mapped_status_rows,
    'stagedActionRows', staged_action_rows,
    'storedEventRows', stored_event_rows,
    'mappingVersion', mapping_version,
    'loadedAt', loaded_at
)::text
FROM public.cme_tas_action_load_metadata
"""
    result = subprocess.run(
        psql_command(args) + ["-At", "-c", metadata_query],
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(result.stdout)
    metadata["comparisonRead"] = dict(diagnostics)
    return index, metadata


def load_databento(
    status_path: Path,
    definition_path: Path,
    equity_start: datetime,
    commodity_start: datetime,
    end: datetime,
) -> tuple[ActionIndex, ActionIndex, dict[str, int]]:
    definitions = build_definition_map(definition_path)
    all_events = ActionIndex()
    scheduled_events = ActionIndex()
    diagnostics = Counter()
    connection = duckdb.connect(":memory:")
    cursor = connection.execute(
        """
        SELECT product, instrument_id, symbol,
               epoch_ns(ts_event) // 1000000000 AS event_second,
               action_name, reason_name, trading_event_name,
               ts_recv = date_trunc('day', ts_recv) AS is_midnight_snapshot
        FROM read_parquet(?)
        WHERE action_name IN (
                'CLOSE', 'PRE_OPEN', 'TRADING', 'NEW_PRICE_INDICATION'
              )
          AND (
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
            if action == "PRE_OPEN" and trading_event == "NO_CANCEL":
                diagnostics["ancillaryActionRowsExcluded"] += 1
                continue
            product = str(product)
            action = str(action)
            second = int(second)
            contract = definitions.get((int(instrument_id), str(symbol)))
            all_events.family[product, action].add(second)
            if contract is None:
                diagnostics["unmappedContracts"] += 1
            else:
                all_events.contract[contract, action].add(second)
            if reason == "SCHEDULED":
                scheduled_events.family[product, action].add(second)
                if contract:
                    scheduled_events.contract[contract, action].add(second)
    connection.close()
    return all_events, scheduled_events, dict(diagnostics)


def ratio(numerator: int, denominator: int) -> float:
    return round(100 * numerator / denominator, 4) if denominator else 100.0


def event_metric(
    source: dict[tuple[str, str], set[int]],
    target: dict[tuple[str, str], set[int]],
    keys: Iterable[tuple[str, str]],
    tolerance: int,
) -> dict[str, float | int]:
    tas_events = 0
    databento_events = 0
    exact_matches = 0
    tolerance_matches = 0
    for key in set(keys):
        tas_values = source.get(key, set())
        databento_values = target.get(key, set())
        tas_events += len(tas_values)
        databento_events += len(databento_values)
        exact_matches += match_count(tas_values, databento_values, 0)
        tolerance_matches += match_count(tas_values, databento_values, tolerance)
    exact_precision = ratio(exact_matches, tas_events)
    exact_recall = ratio(exact_matches, databento_events)
    tolerance_precision = ratio(tolerance_matches, tas_events)
    tolerance_recall = ratio(tolerance_matches, databento_events)

    def f1(precision: float, recall: float) -> float:
        return round(2 * precision * recall / (precision + recall), 4) if precision + recall else 0.0

    return {
        "tasEvents": tas_events,
        "databentoEvents": databento_events,
        "exactMatches": exact_matches,
        "exactPrecisionPct": exact_precision,
        "exactRecallPct": exact_recall,
        "exactF1Pct": f1(exact_precision, exact_recall),
        "matchesWithinTolerance": tolerance_matches,
        "precisionWithinTolerancePct": tolerance_precision,
        "recallWithinTolerancePct": tolerance_recall,
        "f1WithinTolerancePct": f1(tolerance_precision, tolerance_recall),
    }


def action_metric(
    tas: ActionIndex,
    databento: ActionIndex,
    action: str,
    tolerance: int,
    level: str,
    product: str | None = None,
) -> dict[str, float | int]:
    source = tas.family if level == "family" else tas.contract
    target = databento.family if level == "family" else databento.contract
    if level == "family":
        products = (product,) if product else PRODUCTS
        keys = [(item, action) for item in products]
    else:
        keys = {
            key
            for key in source.keys() | target.keys()
            if key[1] == action
            and (product is None or key[0].startswith(f"{product}:"))
        }
    return event_metric(source, target, keys, tolerance)


def sum_metrics(metrics: Iterable[dict[str, float | int]]) -> dict[str, float | int]:
    values = list(metrics)
    tas_events = sum(int(item["tasEvents"]) for item in values)
    databento_events = sum(int(item["databentoEvents"]) for item in values)
    exact = sum(int(item["exactMatches"]) for item in values)
    matched = sum(int(item["matchesWithinTolerance"]) for item in values)
    exact_precision = ratio(exact, tas_events)
    exact_recall = ratio(exact, databento_events)
    precision = ratio(matched, tas_events)
    recall = ratio(matched, databento_events)

    def f1(left: float, right: float) -> float:
        return round(2 * left * right / (left + right), 4) if left + right else 0.0

    return {
        "tasEvents": tas_events,
        "databentoEvents": databento_events,
        "exactMatches": exact,
        "exactPrecisionPct": exact_precision,
        "exactRecallPct": exact_recall,
        "exactF1Pct": f1(exact_precision, exact_recall),
        "matchesWithinTolerance": matched,
        "precisionWithinTolerancePct": precision,
        "recallWithinTolerancePct": recall,
        "f1WithinTolerancePct": f1(precision, recall),
    }


def build_report(
    tas: ActionIndex,
    all_databento: ActionIndex,
    scheduled_databento: ActionIndex,
    tolerance: int,
    equity_start: datetime,
    commodity_start: datetime,
    end: datetime,
    load_metadata: dict[str, object],
    databento_diagnostics: dict[str, int],
) -> dict[str, object]:
    all_family = {
        action: action_metric(tas, all_databento, action, tolerance, "family")
        for action in ACTIONS
    }
    scheduled_family = {
        action: action_metric(
            tas, scheduled_databento, action, tolerance, "family"
        )
        for action in ACTIONS
    }
    all_contract = {
        action: action_metric(tas, all_databento, action, tolerance, "contract")
        for action in ACTIONS
    }
    by_product = {
        product: {
            action: action_metric(
                tas, all_databento, action, tolerance, "family", product
            )
            for action in ACTIONS
        }
        for product in PRODUCTS
    }
    by_product_summary = {
        product: sum_metrics(by_product[product].values()) for product in PRODUCTS
    }
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "method": {
            "toleranceSeconds": tolerance,
            "timeBasis": "Databento ts_event versus TAS event timestamp, UTC seconds",
            "databentoMidnightSnapshotsExcluded": databento_diagnostics.get(
                "midnightSnapshotsExcluded", 0
            ),
            "familyDeduplication": "unique product/action/event second",
            "contractDeduplication": "unique canonical contract/action/event second after clustering direct CME 15/17 states within five seconds",
            "newPriceSemantics": "CME status 15 is direct NEW_PRICE_INDICATION; a clustered status 17 is an explicit fallback when 15 is absent",
            "tradingSemantics": "CME status 17 is direct TRADING; the aligned boundary uses clustered status 15 as an explicit fallback when 17 is absent",
            "stageTransitionFilter": "excludes PRE_OPEN rows whose trading_event is NO_CANCEL because they carry the current action without a new phase boundary",
        },
        "coverage": {
            "equityStart": equity_start.isoformat(),
            "commodityStart": commodity_start.isoformat(),
            "endExclusive": end.isoformat(),
        },
        "summary": {
            "allReasonFamily": sum_metrics(all_family.values()),
            "scheduledFamily": sum_metrics(scheduled_family.values()),
            "allReasonContract": sum_metrics(all_contract.values()),
        },
        "allReasonFamilyByAction": all_family,
        "scheduledFamilyByAction": scheduled_family,
        "allReasonContractByAction": all_contract,
        "allReasonFamilyByProduct": by_product,
        "allReasonFamilyProductSummary": by_product_summary,
        "tasPostgresLoad": load_metadata,
        "databentoDiagnostics": databento_diagnostics,
        "limitations": [
            "The alignment layer emits both open actions at the first timestamp in a five-second CME 15/17 cluster; cme_tas_action_events retains only direct per-RIC evidence.",
            "The PostgreSQL TAS table is observational audit data and does not replace the authoritative CME calendar interval table.",
            "Scheduled-only precision treats non-scheduled TAS phase observations as extras; all-reason precision is the primary precision measure.",
        ],
    }


def format_pair(metric: dict[str, object], prefix: str) -> str:
    return (
        f"{metric[prefix + 'PrecisionPct']:.2f}% / "
        f"{metric[prefix + 'RecallPct']:.2f}%"
    )


def format_tolerance_pair(metric: dict[str, object]) -> str:
    return (
        f"{metric['precisionWithinTolerancePct']:.2f}% / "
        f"{metric['recallWithinTolerancePct']:.2f}%"
    )


def render_markdown(report: dict[str, object]) -> str:
    tolerance = report["method"]["toleranceSeconds"]
    summary = report["summary"]
    lines = [
        "# PostgreSQL TAS action 与 Databento 对照",
        "",
        f"生成时间：`{report['generatedAt']}`。PG 数据由 TAS RocksDB 全量 RIC 归一化后反读。",
        f"对照窗口：股指从 `{report['coverage']['equityStart']}`、商品从 "
        f"`{report['coverage']['commodityStart']}` 至 `{report['coverage']['endExclusive']}`（右开）。",
        "",
        "## 汇总",
        "",
        "| 粒度 | TAS 事件 | Databento 事件 | exact P/R | "
        f"±{tolerance}s P/R |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for label, key in (
        ("品种级，全部 reason", "allReasonFamily"),
        ("品种级，仅 SCHEDULED 目标", "scheduledFamily"),
        ("合约级，全部 reason", "allReasonContract"),
    ):
        metric = summary[key]
        lines.append(
            f"| {label} | {metric['tasEvents']:,} | {metric['databentoEvents']:,} | "
            f"{format_pair(metric, 'exact')} | "
            f"{format_tolerance_pair(metric)} |"
        )

    lines.extend(
        [
            "",
            "## 品种级 action",
            "",
            "| action | TAS 事件 | Databento 事件 | exact P/R | "
            f"±{tolerance}s P/R |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for action in ACTIONS:
        metric = report["allReasonFamilyByAction"][action]
        lines.append(
            f"| {action}（{ACTION_LABELS[action]}） | {metric['tasEvents']:,} | "
            f"{metric['databentoEvents']:,} | {format_pair(metric, 'exact')} | "
            f"{format_tolerance_pair(metric)} |"
        )
    lines.extend(
        [
            "",
            "## 六品种汇总",
            "",
            "| product | TAS 事件 | Databento 事件 | exact P/R | "
            f"±{tolerance}s P/R |",
            "| --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for product in PRODUCTS:
        metric = report["allReasonFamilyProductSummary"][product]
        lines.append(
            f"| {product} | {metric['tasEvents']:,} | "
            f"{metric['databentoEvents']:,} | {format_pair(metric, 'exact')} | "
            f"{format_tolerance_pair(metric)} |"
        )
    lines.extend(
        [
            "",
            "逐 RIC 原始映射中，CME 状态 15 直接对应 `NEW_PRICE_INDICATION`，17/T "
            "直接对应 `TRADING`。对齐视图将 5 秒内的 15/17 聚成一次开盘边界；"
            "缺少一侧时才把另一侧标记为推断。",
            "",
            "PG 表 `public.cme_tas_action_events` 是逐 RIC 观测；"
            "`public.cme_tas_action_consensus` 是直接事件汇总，"
            "`public.cme_tas_action_alignment` 是用于 Databento 对照的阶段边界。"
            "它们用于 replay 审计，不替代官方交易日历表。",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read TAS actions from PostgreSQL and compare them with Databento"
    )
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    parser.add_argument("--definition", type=Path, default=DEFAULT_DEFINITION)
    parser.add_argument("--equity-start", default="2024-01-01T00:00:00Z")
    parser.add_argument("--commodity-start", default="2020-01-01T00:00:00Z")
    parser.add_argument("--end", default="2026-06-01T00:00:00Z")
    parser.add_argument("--tolerance-seconds", type=int, default=2)
    parser.add_argument("--output-prefix", type=Path, default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--psql", type=Path, default=DEFAULT_PSQL)
    parser.add_argument("--pg-socket", type=Path, default=DEFAULT_PG_SOCKET)
    parser.add_argument("--pg-port", type=int, default=5433)
    parser.add_argument("--pg-user", default="u171")
    parser.add_argument("--pg-database", default="market_metadata")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    equity_start = utc_datetime(args.equity_start)
    commodity_start = utc_datetime(args.commodity_start)
    end = utc_datetime(args.end)
    if max(equity_start, commodity_start) >= end:
        raise ValueError("coverage start must be before end")

    tas, load_metadata = load_postgres(
        args, equity_start, commodity_start, end
    )
    all_databento, scheduled_databento, diagnostics = load_databento(
        args.status, args.definition, equity_start, commodity_start, end
    )
    report = build_report(
        tas,
        all_databento,
        scheduled_databento,
        args.tolerance_seconds,
        equity_start,
        commodity_start,
        end,
        load_metadata,
        diagnostics,
    )
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    json_path = args.output_prefix.with_suffix(".json")
    markdown_path = args.output_prefix.with_suffix(".md")
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    print(f"wrote {markdown_path}")
    print(f"wrote {json_path}")


if __name__ == "__main__":
    main()
