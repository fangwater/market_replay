"""只读访问指定 PostgreSQL 实例中已有的 CME 参考表，不执行迁移。"""

import json
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = (
    "postgresql:///market_metadata?host=%2Fmnt%2Fnvme-raid0-28t%2Fpostgresql%2F"
    "domestic_futures%2F16%2Frun&port=5433&user=u171"
)


def _json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def query(sql, params):
    # 固定 DSN，不能由客户端或环境变量切换实例；在连接启动时强制只读。
    with psycopg.connect(
        DATABASE_URL, row_factory=dict_row, connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=20000 -c timezone=UTC",
    ) as connection:
        rows = connection.execute(sql, params).fetchall()
    return json.loads(json.dumps(rows, default=_json_value))


def sessions(product, start_date, end_date):
    return query(
        """SELECT product, exchange, trade_date, segment_index, open_utc, close_utc,
                  duration_seconds, schedule_regimes, exception_names, confidence,
                  source_urls, snapshot_ids
           FROM public.cme_six_product_trading_sessions
           WHERE product = %s AND trade_date BETWEEN %s AND %s
           ORDER BY trade_date, segment_index""",
        (product, start_date, end_date),
    )


def commissions(product, exchange):
    return query(
        """SELECT schedule_version, association_code, instrument_currency,
                  instrument_name_zh, exchange, ib_exchange_option,
                  commission_amount, commission_currency, charge_basis, charge_side,
                  fee_scope, historical_exact, effective_from, effective_to,
                  source_received_on, source_reference, notes
           FROM public.overseas_futures_broker_commission_reference
           WHERE (%s::text IS NULL OR association_code = %s)
             AND (%s::text IS NULL OR exchange = %s)
           ORDER BY association_code, schedule_version, ib_exchange_option""",
        (product, product, exchange, exchange),
    )
