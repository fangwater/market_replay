BEGIN;

CREATE TABLE IF NOT EXISTS public.overseas_futures_broker_commission_reference (
    schedule_version text NOT NULL,
    association_code text NOT NULL,
    instrument_currency text NOT NULL,
    instrument_name_zh text NOT NULL,
    exchange text NOT NULL,
    ib_exchange_option text NOT NULL,
    commission_amount numeric(18, 8) NOT NULL,
    commission_currency text NOT NULL,
    charge_basis text NOT NULL DEFAULT 'PER_CONTRACT',
    charge_side text NOT NULL DEFAULT 'UNSPECIFIED',
    fee_scope text NOT NULL DEFAULT 'BROKER_REFERENCE',
    historical_exact boolean NOT NULL DEFAULT false,
    effective_from date,
    effective_to date,
    source_received_on date NOT NULL,
    source_reference text NOT NULL,
    notes text NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (schedule_version, association_code, ib_exchange_option),
    CHECK (instrument_currency ~ '^[A-Z]{3}$'),
    CHECK (commission_currency ~ '^[A-Z]{3}$'),
    CHECK (commission_amount >= 0),
    CHECK (charge_basis = 'PER_CONTRACT'),
    CHECK (charge_side IN ('UNSPECIFIED', 'PER_SIDE', 'ROUND_TURN')),
    CHECK (fee_scope IN ('BROKER_REFERENCE', 'ACCOUNT_EXACT')),
    CHECK (NOT historical_exact OR fee_scope = 'ACCOUNT_EXACT'),
    CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_from <= effective_to)
);

CREATE INDEX IF NOT EXISTS overseas_futures_broker_commission_lookup_idx
    ON public.overseas_futures_broker_commission_reference
       (association_code, exchange, effective_from, effective_to);

WITH source_rows (
    association_code,
    instrument_currency,
    instrument_name_zh,
    exchange,
    ib_exchange_option,
    commission_amount,
    commission_currency
) AS (
    VALUES
        ('ZL', 'USD', '豆油期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZC', 'USD', '玉米期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZS', 'USD', '大豆期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZW', 'USD', '小麦期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZM', 'USD', '豆粕期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZF', 'USD', '5年期美国中期国债期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZT', 'USD', '2年期美国中期国债期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('ZN', 'USD', '10年期美国中期国债期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('YM', 'USD', '迷你道琼斯工业平均指数期货', 'CBOT', 'CBOT', 0.85, 'USD'),
        ('GC', 'USD', '黄金期货', 'COMEX', 'COMEX', 0.85, 'USD'),
        ('HG', 'USD', '铜期货', 'COMEX', 'COMEX', 0.85, 'USD'),
        ('QO', 'USD', '迷你黄金期货', 'COMEX', 'COMEX', 0.85, 'USD'),
        ('SI(Micro Contracts)', 'USD', '白银期货', 'COMEX', 'COMEX', 0.25, 'USD'),
        ('MGC', 'USD', '微型黄金期货', 'COMEX', 'COMEX', 0.25, 'USD'),
        ('BZ', 'USD', '布伦特原油期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('CL', 'USD', '低硫轻质原油期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('NG', 'USD', '天然气期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('QG', 'USD', '迷你天然气期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('RB', 'USD', '汽油期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('HO', 'USD', '取暖油期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('PA', 'USD', '钯金期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('PL', 'USD', '铂金期货', 'NYMEX', 'NYMEX', 0.85, 'USD'),
        ('NKD', 'USD', '美元计价日经225指数期货', 'CME', 'CME-USD', 0.85, 'USD'),
        ('BRR', 'USD', 'CF比特币美元参考汇率期货', 'CME', 'CME-USD', 0.85, 'USD'),
        ('BTCEURRR', 'EUR', 'CF比特币欧元参考汇率期货', 'CME', 'CME-USD', 0.85, 'USD'),
        ('GF', 'USD', '育肥用牛期货', 'CME', 'CME-USD', 0.85, 'USD'),
        ('HE', 'USD', '瘦肉猪', 'CME', 'CME-USD', 0.85, 'USD'),
        ('LE', 'USD', '活牛期货', 'CME', 'CME-USD', 0.85, 'USD'),
        ('VIX', 'USD', '芝加哥期权交易所SPX波动率指数', 'CBOE', 'CME-USD', 0.85, 'USD'),
        ('CT', 'USD', '棉花期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('CC', 'USD', '可可期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('KC', 'USD', '咖啡期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('SB', 'USD', '11号糖期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('RS', 'USD', '油菜籽期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('DX', 'USD', '美元指数期货', 'NYBOT', 'NYBOT', 0.85, 'USD'),
        ('W', 'USD', '白糖期货', 'ICE', 'ICEEUSOFT - USD', 2.40, 'USD'),
        ('ECF', 'EUR', '欧盟碳排放配额期货', 'ICE', 'ICE', 6.00, 'EUR'),
        ('TFM', 'EUR', 'Dutch TTF Natural Gas Futures', 'ICE', 'INDEX', 3.00, 'EUR'),
        ('GOIL', 'USD', '轻柴油期货', 'IPE', 'IPE', 2.40, 'USD'),
        ('COIL', 'USD', '布伦特原油期货', 'IPE', 'IPE', 2.40, 'USD'),
        ('CA', 'USD', '铜期货', 'LME', 'LMEOTC', 7.30, 'USD'),
        ('AH', 'USD', '铝期货', 'LME', 'LMEOTC', 7.30, 'USD'),
        ('ZSLME', 'USD', '锌期货', 'LME', 'LMEOTC', 7.30, 'USD'),
        ('NI', 'USD', '镍期货', 'LME', 'LMEOTC', 7.30, 'USD'),
        ('PB', 'USD', '铅期货', 'LME', 'LMEOTC', 7.30, 'USD'),
        ('DAX', 'EUR', '德国DAX40指数微型期货', 'EUREX', 'EUREX - EUR', 0.40, 'EUR'),
        ('GBL', 'EUR', '长期欧元债券（10年期债券）', 'EUREX', 'EUREX - EUR', 2.00, 'EUR'),
        ('GBS', 'EUR', '短期欧元债券（2年期债券）', 'EUREX', 'EUREX - EUR', 2.00, 'EUR'),
        ('GBM', 'EUR', '中期欧元债券（5年期债券）', 'EUREX', 'EUREX - EUR', 2.00, 'EUR'),
        ('CY', 'USD', '新交所在岸人民币（CNY）/美元期货', 'SGX', 'SGX', 1.80, 'USD'),
        ('JGB', 'JPY', '日本政府债券10年期期货', 'OSE', 'CME-JPY', 500.00, 'JPY')
)
INSERT INTO public.overseas_futures_broker_commission_reference (
    schedule_version,
    association_code,
    instrument_currency,
    instrument_name_zh,
    exchange,
    ib_exchange_option,
    commission_amount,
    commission_currency,
    charge_basis,
    charge_side,
    fee_scope,
    historical_exact,
    effective_from,
    effective_to,
    source_received_on,
    source_reference,
    notes
)
SELECT
    'USER_PROVIDED_BROKER_COMMISSION_V1',
    association_code,
    instrument_currency,
    instrument_name_zh,
    exchange,
    ib_exchange_option,
    commission_amount,
    commission_currency,
    'PER_CONTRACT',
    'UNSPECIFIED',
    'BROKER_REFERENCE',
    false,
    NULL,
    NULL,
    DATE '2026-09-09',
    'User-provided broker commission schedule',
    'Source provides no effective dates or charge-side semantics.'
FROM source_rows
ON CONFLICT (schedule_version, association_code, ib_exchange_option) DO UPDATE
SET instrument_currency = EXCLUDED.instrument_currency,
    instrument_name_zh = EXCLUDED.instrument_name_zh,
    exchange = EXCLUDED.exchange,
    commission_amount = EXCLUDED.commission_amount,
    commission_currency = EXCLUDED.commission_currency,
    charge_basis = EXCLUDED.charge_basis,
    charge_side = EXCLUDED.charge_side,
    fee_scope = EXCLUDED.fee_scope,
    historical_exact = EXCLUDED.historical_exact,
    effective_from = EXCLUDED.effective_from,
    effective_to = EXCLUDED.effective_to,
    source_received_on = EXCLUDED.source_received_on,
    source_reference = EXCLUDED.source_reference,
    notes = EXCLUDED.notes,
    loaded_at = clock_timestamp();

COMMENT ON TABLE public.overseas_futures_broker_commission_reference IS
    'User-provided broker commission references. Not an exchange fee schedule and not historically exact unless account-specific dated evidence is loaded.';
COMMENT ON COLUMN public.overseas_futures_broker_commission_reference.charge_side IS
    'The source states only an amount per contract; it does not state whether the charge is per side or round turn.';

COMMIT;
