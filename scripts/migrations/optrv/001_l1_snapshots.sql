-- One row per instrument per full minute in which it had at least one message.
-- snapshot_ts is the END of the minute window (as-of time), so quote_age_ms >= 0.
-- Prices are exactly as Deribit sends them (BTC for inverse options).
-- ReplacingMergeTree on (symbol, snapshot_ts) makes reloads idempotent; read with FINAL.
CREATE TABLE IF NOT EXISTS l1_snapshots
(
    snapshot_ts            DateTime64(3, 'UTC'),
    recv_ts                DateTime64(6, 'UTC'),
    venue_ts               DateTime64(3, 'UTC'),
    quote_age_ms           Int64,
    symbol                 LowCardinality(String),
    kind                   LowCardinality(String),
    strike                 Nullable(Float64),
    expiration             Nullable(DateTime64(3, 'UTC')),
    option_type            LowCardinality(Nullable(String)),
    bid_price              Nullable(Float64),
    bid_amount             Nullable(Float64),
    ask_price              Nullable(Float64),
    ask_amount             Nullable(Float64),
    bid_iv                 Nullable(Float64),
    ask_iv                 Nullable(Float64),
    mark_iv                Nullable(Float64),
    mark_price             Nullable(Float64),
    underlying_price       Nullable(Float64),
    index_price            Nullable(Float64),
    open_interest          Nullable(Float64),
    delta                  Nullable(Float64),
    gamma                  Nullable(Float64),
    vega                   Nullable(Float64),
    theta                  Nullable(Float64),
    interest_rate          Nullable(Float64),
    funding_8h             Nullable(Float64),
    current_funding        Nullable(Float64),
    meta_mismatch          Bool,
    instruments_fetched_at Nullable(DateTime64(3, 'UTC')),
    schema_version         UInt16
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (symbol, snapshot_ts)
SETTINGS non_replicated_deduplication_window = 1000
