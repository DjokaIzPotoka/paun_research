-- Every public/get_instruments response, one row per instrument per fetch.
-- Timestamps stay as Deribit's raw epoch milliseconds: BTC-PERPETUAL's
-- expiration_timestamp is year 3000, outside DateTime64's range.
CREATE TABLE IF NOT EXISTS instruments
(
    fetched_at           DateTime64(3, 'UTC'),
    symbol               LowCardinality(String),
    instrument_id        UInt64,
    kind                 LowCardinality(String),
    option_type          LowCardinality(Nullable(String)),
    strike               Nullable(Float64),
    expiration_timestamp Int64,
    creation_timestamp   Int64,
    is_active            Bool,
    settlement_currency  LowCardinality(Nullable(String)),
    settlement_period    LowCardinality(String),
    contract_size        Float64,
    tick_size            Float64,
    min_trade_amount     Float64,
    schema_version       UInt16
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (symbol, fetched_at)
SETTINGS non_replicated_deduplication_window = 1000
