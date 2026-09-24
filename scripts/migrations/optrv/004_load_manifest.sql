-- Parquet files already loaded, keyed by content hash. Lives next to the data so a
-- wiped database cannot disagree with a manifest kept somewhere else.
CREATE TABLE IF NOT EXISTS load_manifest
(
    sha256      String,
    table_name  LowCardinality(String),
    source_path String,
    rows        UInt64,
    loaded_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (sha256, table_name)
