-- Batch smoke: namespace + table DDL, bounded INSERT, read back.
-- Run with: sql-client.sh -i /opt/sql/00_catalog.sql -f /opt/sql/10_batch_smoke.sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
-- Make INSERT block until the job finishes so the SELECTs below see the data.
SET 'table.dml-sync' = 'true';

USE CATALOG mrd;

CREATE DATABASE IF NOT EXISTS flink_ns;
USE flink_ns;

CREATE TABLE IF NOT EXISTS events (
  id BIGINT,
  name STRING,
  `value` DOUBLE,  -- VALUE is reserved in Flink SQL
  ts TIMESTAMP(6)
);

INSERT INTO events VALUES
  (1, 'alpha', 1.5, TIMESTAMP '2026-01-01 00:00:00'),
  (2, 'beta',  3.0, TIMESTAMP '2026-01-01 00:00:01'),
  (3, 'gamma', 4.5, TIMESTAMP '2026-01-01 00:00:02');

SELECT COUNT(*) AS row_count FROM events;

SELECT * FROM events ORDER BY id;
