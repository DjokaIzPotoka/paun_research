-- Periods when the collector was not listening (connection_lost, stale_connection,
-- process_restart, subscribe_rejected, sink_write_failed) or listened but could not
-- write rows (missing_metadata). Lets "no quote" be told apart from "not listening".
CREATE TABLE IF NOT EXISTS collector_gaps
(
    gap_start            DateTime64(3, 'UTC'),
    gap_end              DateTime64(3, 'UTC'),
    reason               LowCardinality(String),
    affected_instruments UInt32,
    detail               String,
    recorded_at          DateTime64(3, 'UTC'),
    schema_version       UInt16
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(gap_start)
ORDER BY (gap_start, reason, gap_end)
SETTINGS non_replicated_deduplication_window = 1000
