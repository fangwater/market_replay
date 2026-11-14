"""Command-line operations for reviewed message-fee rules and manual refreshes."""

import argparse
import json
import os
import sys
from pathlib import Path

from . import services, storage
from .models import DEFAULT_SESSION_SOURCE_URL, DataError


def _database_url():
    value = os.environ.get("CN_FUTURES_REF_DATABASE_URL")
    if not value:
        raise DataError("CN_FUTURES_REF_DATABASE_URL is required")
    return value


def _parser():
    parser = argparse.ArgumentParser(description="中国期货参考数据工具")
    commands = parser.add_subparsers(dest="command", required=True)

    import_rules = commands.add_parser(
        "import-message-rules", help="导入并版本化申报费规则 JSON"
    )
    import_rules.add_argument("rules", type=Path, help="规则 JSON 文件")
    import_rules.add_argument(
        "--verified", action="store_true", help="将未指定审核状态的规则标记为已审核"
    )

    calculate = commands.add_parser(
        "message-fee", help="根据订单事件 CSV 和已审核规则计算每日申报费"
    )
    calculate.add_argument("--orders", type=Path, required=True, help="订单事件 CSV")

    refresh = commands.add_parser(
        "refresh-announcements", help="手动拉取公告并调用 Codex 分析"
    )
    refresh.add_argument("--pages", type=int, default=5, help="每个交易所扫描页数")
    refresh.add_argument("--per-source", type=int, default=8, help="每个交易所最多拉取数")
    refresh.add_argument("--analysis-limit", type=int, default=20, help="本次最多分析数")
    refresh.add_argument("--timeout", type=int, default=12, help="单次交易所请求超时秒数")
    refresh.add_argument("--retries", type=int, default=0, help="单次交易所请求重试次数")

    sessions = commands.add_parser(
        "sync-trading-sessions", help="从 OpenCTP 增量同步连续竞价时段"
    )
    sessions.add_argument(
        "--source-url", default=None, help="OpenCTP times 接口地址"
    )
    sessions.add_argument("--source-date", help="观测日期，格式 YYYY-MM-DD")
    sessions.add_argument("--timeout", type=int, default=30, help="请求超时秒数")
    sessions.add_argument("--retries", type=int, default=2, help="请求重试次数")
    return parser


def main(argv=None):
    args = _parser().parse_args(argv)
    try:
        database = _database_url()
        if args.command == "sync-trading-sessions":
            result = services.sync_continuous_sessions(
                database,
                args.source_url or os.environ.get(
                    "CN_FUTURES_REF_SESSION_SOURCE_URL", DEFAULT_SESSION_SOURCE_URL
                ),
                source_date=args.source_date,
                timeout=max(1, args.timeout), retries=max(0, args.retries),
            )
        elif args.command == "refresh-announcements":
            result = services.refresh_announcements(
                database, page_count=max(1, args.pages),
                per_source_limit=max(1, args.per_source),
                analysis_limit=max(1, args.analysis_limit),
                timeout=max(5, args.timeout), retries=max(0, args.retries),
            )
        elif args.command == "import-message-rules":
            text = args.rules.read_text(encoding="utf-8-sig")
            connection = storage.connect(database)
            try:
                result = services.import_message_fee_rules(
                    connection, text, "VERIFIED" if args.verified else "PENDING"
                )
            finally:
                connection.close()
        else:
            text = args.orders.read_text(encoding="utf-8-sig")
            connection = storage.connect(database)
            try:
                result = services.calculate_message_fees(connection, text)
            finally:
                connection.close()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (DataError, OSError, ValueError) as error:
        print("错误：%s" % error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
