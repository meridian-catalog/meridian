-- Streaming smoke: bounded datagen source -> Iceberg sink in streaming
-- mode. The Iceberg sink commits on checkpoints, so this exercises the
-- checkpoint-commit path against Meridian (unlike the batch INSERT,
-- which commits once at the end of the job).
-- Run with: sql-client.sh -i /opt/sql/00_catalog.sql -f /opt/sql/20_streaming_smoke.sql
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '2s';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.dml-sync' = 'true';

CREATE TEMPORARY TABLE gen (
  id BIGINT,
  name STRING,
  `value` DOUBLE,
  ts TIMESTAMP(6)
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '25',
  'number-of-rows' = '50',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '1000',
  'fields.id.end' = '1049',
  'fields.name.length' = '8'
);

USE CATALOG mrd;
USE flink_ns;

INSERT INTO events SELECT id, name, `value`, ts FROM default_catalog.default_database.gen;

-- Read back in batch mode for a single-row result instead of a changelog.
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS row_count_after_streaming FROM events;
