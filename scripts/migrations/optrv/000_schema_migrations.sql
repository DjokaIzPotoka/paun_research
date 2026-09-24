CREATE TABLE IF NOT EXISTS schema_migrations
(
    version    UInt32,
    name       String,
    applied_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY version
