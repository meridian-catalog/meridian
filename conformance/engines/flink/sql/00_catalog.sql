-- Shared catalog definition, sourced by the smoke scripts via -i.
--
-- The URI points at host.docker.internal because Meridian runs on the
-- Docker host; the S3 settings mirror what Meridian vends for the
-- warehouse (MinIO on localhost:9000 — reachable because the Flink
-- containers run with host networking; see docker-compose.yml).
-- Credentials are the fixed local-dev MinIO credentials; Meridian never
-- vends credentials, so the engine must configure them client-side.
CREATE CATALOG mrd WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://host.docker.internal:8181/iceberg',
  'warehouse' = 'flink_smoke',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://localhost:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'meridian',
  's3.secret-access-key' = 'meridian123',
  'client.region' = 'us-east-1'
);
