// Wire types for the Meridian API surfaces the console consumes.
//
// Every shape here was checked against the running server's route handlers
// (crates/meridian-server/src/routes/*) and live responses. Fields the server
// may omit are typed optional; nothing is invented.

// ---------------------------------------------------------------------------
// Error envelope (IRC + management API share this shape)
// ---------------------------------------------------------------------------

export interface ErrorEnvelope {
  error: {
    message: string;
    type: string;
    code: number;
  };
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

export interface HealthResponse {
  status: string;
  checks: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Config (IRC)
// ---------------------------------------------------------------------------

export interface ConfigResponse {
  defaults: Record<string, string>;
  overrides: Record<string, string>;
  endpoints: string[];
  "idempotency-key-lifetime"?: string;
}

// ---------------------------------------------------------------------------
// Warehouses (management)
// ---------------------------------------------------------------------------

export interface Warehouse {
  id: string;
  name: string;
  storage_root: string;
  storage_options: Record<string, string>;
  created_at: string;
  updated_at: string;
}

export interface ListWarehousesResponse {
  warehouses: Warehouse[];
}

// ---------------------------------------------------------------------------
// Principals (management)
// ---------------------------------------------------------------------------

export interface Principal {
  id: string;
  kind: string; // "user" | "service"
  subject: string;
  issuer: string;
  display_name: string | null;
  created_at: string;
}

export interface ListPrincipalsResponse {
  principals: Principal[];
}

// ---------------------------------------------------------------------------
// Namespaces / tables / views (IRC)
// ---------------------------------------------------------------------------

export interface ListNamespacesResponse {
  namespaces: string[][];
  "next-page-token"?: string | null;
}

export interface NamespaceResponse {
  namespace: string[];
  properties: Record<string, string>;
}

export interface TableIdentifier {
  namespace: string[];
  name: string;
}

export interface ListTablesResponse {
  identifiers: TableIdentifier[];
  "next-page-token"?: string | null;
}

// LoadTableResult (Iceberg spec). We only type the fields the console renders.
export interface IcebergField {
  id: number;
  name: string;
  required: boolean;
  type: IcebergType;
  doc?: string;
}

export type IcebergType =
  | string
  | {
      type: "struct" | "list" | "map";
      fields?: IcebergField[];
      element?: IcebergType;
      "element-id"?: number;
      "element-required"?: boolean;
      key?: IcebergType;
      value?: IcebergType;
      "key-id"?: number;
      "value-id"?: number;
      "value-required"?: boolean;
    };

export interface IcebergSchema {
  "schema-id"?: number;
  type?: string;
  fields: IcebergField[];
  "identifier-field-ids"?: number[];
}

export interface IcebergSnapshot {
  "snapshot-id": number;
  "parent-snapshot-id"?: number;
  "sequence-number"?: number;
  "timestamp-ms": number;
  "manifest-list"?: string;
  summary?: Record<string, string>;
  "schema-id"?: number;
}

export interface TableMetadata {
  "table-uuid": string;
  location: string;
  "format-version": number;
  "last-updated-ms"?: number;
  "current-schema-id"?: number;
  schemas?: IcebergSchema[];
  "current-snapshot-id"?: number;
  snapshots?: IcebergSnapshot[];
  "partition-specs"?: unknown[];
  "sort-orders"?: unknown[];
  properties?: Record<string, string>;
  refs?: Record<string, unknown>;
}

export interface LoadTableResult {
  metadata: TableMetadata;
  "metadata-location"?: string;
  config?: Record<string, string>;
}

// LoadViewResult (Iceberg spec).
export interface ViewRepresentation {
  type: string; // "sql"
  sql: string;
  dialect: string;
}

export interface ViewVersion {
  "version-id": number;
  "schema-id"?: number;
  "timestamp-ms"?: number;
  representations: ViewRepresentation[];
  "default-catalog"?: string;
  "default-namespace"?: string[];
  summary?: Record<string, string>;
}

export interface ViewMetadata {
  "view-uuid": string;
  location: string;
  "format-version"?: number;
  "current-version-id"?: number;
  versions?: ViewVersion[];
  schemas?: IcebergSchema[];
  properties?: Record<string, string>;
}

export interface LoadViewResult {
  metadata: ViewMetadata;
  "metadata-location"?: string;
  config?: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Search (management)
// ---------------------------------------------------------------------------

export interface SearchResult {
  type: "table" | "view" | "namespace";
  id: string;
  name: string;
  namespace: string[];
  warehouse: string;
  rank: number;
  snippet: string;
}

export interface SearchResponse {
  results: SearchResult[];
  next_page_token?: string;
}

// ---------------------------------------------------------------------------
// RBAC (management)
// ---------------------------------------------------------------------------

export interface Role {
  id: string;
  name: string;
  description: string | null;
  built_in: boolean;
  created_at: string;
}

export interface ListRolesResponse {
  roles: Role[];
}

export interface Grant {
  id: string;
  privilege: string;
  role: string | null;
  principal_id: string | null;
  securable_type: string;
  securable_id: string;
  granted_by: string;
  created_at: string;
}

export interface ListGrantsResponse {
  grants: Grant[];
}

export interface Permission {
  privilege: string;
  securable_type: string;
  securable_id: string;
  via: string; // "direct" | "role:<name>"
}

export interface PermissionsResponse {
  principal_id: string;
  roles: string[];
  permissions: Permission[];
}

export interface SecurableSelector {
  type: "warehouse" | "namespace" | "table" | "view";
  warehouse: string;
  namespace?: string[];
  table?: string;
  view?: string;
}

export interface CreateGrantRequest {
  privilege: string;
  role?: string;
  principal_id?: string;
  securable: SecurableSelector;
}

// ---------------------------------------------------------------------------
// Audit (management)
// ---------------------------------------------------------------------------

export interface AuditEntry {
  seq: number;
  id: string;
  workspace_id: string | null;
  occurred_at: string;
  principal: string;
  action: string;
  resource: string;
  details: unknown;
  prev_hash: string | null;
  hash: string;
}

export interface AuditQueryResponse {
  entries: AuditEntry[];
  next_cursor?: number;
}

export interface VerifyChainResponse {
  entries_checked: number;
  valid: boolean;
  broken_at?: number;
  error?: string;
}

export interface AuditQueryParams {
  principal?: string;
  action?: string;
  resource?: string;
  workspace?: string;
  from?: string;
  to?: string;
  before?: number;
  limit?: number;
}

// ---------------------------------------------------------------------------
// Events + webhooks (management)
// ---------------------------------------------------------------------------

// CloudEvents 1.0 JSON. Only the envelope fields the console renders are typed;
// the rest of the object is preserved.
export interface CloudEvent {
  id: string;
  type: string;
  source?: string;
  subject?: string;
  time?: string;
  data?: unknown;
  [key: string]: unknown;
}

export interface FeedResponse {
  events: CloudEvent[];
  next_cursor: string;
}

export interface Webhook {
  id: string;
  url: string;
  event_types: string[];
  created_at: string;
  updated_at: string;
}

export interface ListWebhooksResponse {
  webhooks: Webhook[];
}

export interface CreateWebhookRequest {
  url: string;
  event_types: string[];
  secret: string;
}

export interface Delivery {
  event_id: string;
  event_type: string;
  status: string; // "pending" | "delivered" | "dead"
  attempts: number;
  last_status: number | null;
  last_error: string | null;
  next_attempt_at: string;
  updated_at: string;
}

export interface ListDeliveriesResponse {
  deliveries: Delivery[];
}

// The wire vocabulary for grant creation (matches meridian_store::rbac).
export const PRIVILEGES = [
  "MANAGE_WAREHOUSE",
  "CREATE_NAMESPACE",
  "LIST_NAMESPACES",
  "MANAGE_NAMESPACE",
  "CREATE_TABLE",
  "LIST_TABLES",
  "CREATE_VIEW",
  "READ",
  "WRITE",
  "COMMIT",
  "DROP",
] as const;

export const SECURABLE_TYPES = [
  "warehouse",
  "namespace",
  "table",
  "view",
] as const;

// ---- maintenance (Pillar C) -----------------------------------------------

export interface HealthMetrics {
  total_bytes: number;
  data_file_count: number;
  small_file_ratio: number;
  avg_file_bytes: number;
  median_file_bytes: number;
  delete_debt_ratio: number;
  delete_file_count: number;
  manifest_count: number;
  avg_manifest_entries: number;
  partition_skew: number;
  snapshot_count: number;
  oldest_snapshot_ms: number | null;
  metadata_json_bytes: number;
  file_size_histogram: Record<string, number>;
}

export interface Recommendation {
  action: string;
  reason: string;
  impact: number;
}

export interface TableHealth {
  table_id: string;
  table_ident: string;
  snapshot_id: number | null;
  score: number;
  metrics: HealthMetrics;
  recommendations: Recommendation[];
  computed_at: string;
}

export interface HealthHistoryResponse {
  history: TableHealth[];
}

export interface WorstTable {
  table_id: string;
  table_ident: string;
  namespace: string[];
  name: string;
  score: number;
  small_file_ratio: number;
  snapshot_count: number;
  data_file_count: number;
}

export interface WarehouseHealthSummary {
  warehouse: string;
  tables_scored: number;
  avg_score: number;
  min_score: number;
  healthy_count: number;
  degraded_count: number;
  unhealthy_count: number;
  total_bytes: number;
  total_data_files: number;
  worst_tables: WorstTable[];
}

export interface MaintenanceJob {
  id: string;
  table_id: string;
  job_type: string;
  state: string;
  policy_id: string | null;
  spec: unknown;
  created_by: string;
  claimed_by: string | null;
  attempts: number;
  error: unknown | null;
  result: unknown | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface ListJobsResponse {
  jobs: MaintenanceJob[];
}

export interface MaintenancePolicy {
  id: string;
  scope: string;
  scope_id: string;
  scope_label: string;
  target_file_size_bytes: number;
  min_input_files: number;
  snapshot_retention_count: number;
  snapshot_retention_age_ms: number;
  max_staleness_ms: number | null;
  schedule: string | null;
  window_start: string | null;
  window_end: string | null;
  cost_cap_usd_month: number | null;
  exclusions: unknown;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface ListPoliciesResponse {
  policies: MaintenancePolicy[];
}

export interface SavingsRow {
  id: string;
  job_id: string;
  table_id: string;
  table_ident: string;
  period: string;
  bytes_before: number;
  bytes_after: number;
  files_before: number;
  files_after: number;
  bytes_saved: number;
  files_removed: number;
  est_get_requests_saved: number;
  methodology: string;
  created_at: string;
}

export interface ListSavingsResponse {
  savings: SavingsRow[];
}

export interface SavingsRollupPeriod {
  period: string;
  job_count: number;
  bytes_saved: number;
  files_removed: number;
  est_get_requests_saved: number;
}

export interface SavingsRollupResponse {
  rollup: SavingsRollupPeriod[];
}

export interface TriggerJobRequest {
  warehouse: string;
  namespace: string;
  table: string;
  job_type?: string;
  dry_run?: boolean;
}

// ---- federation (Pillar B): mirrors + sprawl -----------------------------

export interface Mirror {
  id: string;
  name: string;
  kind: string;
  endpoint: string;
  remote_catalog: string | null;
  config: Record<string, string>;
  enabled: boolean;
  sync_interval_s: number;
  last_synced_at: string | null;
  last_sync_status: string | null;
  last_sync_detail: string | null;
  asset_count: number;
  created_at: string;
  updated_at: string;
}

export interface ListMirrorsResponse {
  mirrors: Mirror[];
}

export interface SyncRun {
  id: string;
  status: string;
  assets_seen: number;
  detail: string | null;
  started_at: string;
  finished_at: string | null;
}

export interface MirrorSyncStatus {
  mirror: Mirror;
  history: SyncRun[];
}

export interface CreateMirrorRequest {
  name: string;
  kind: string;
  endpoint: string;
  remote_catalog?: string;
  config?: Record<string, string>;
  enabled?: boolean;
  sync_interval_s?: number;
}

export interface SprawlSource {
  source_type: string;
  source_id: string;
  name: string;
  kind: string;
  asset_count: number;
  last_synced_at: string | null;
}

export interface SprawlDuplicate {
  storage_location: string;
  source_count: number;
  sources: string[];
}

export interface SprawlStaleMirror {
  mirror_id: string;
  name: string;
  last_synced_at: string | null;
  age_seconds: number | null;
  sync_interval_s: number;
}

export interface SprawlHealth {
  tables_scored: number;
  avg_score: number;
  unhealthy_count: number;
  degraded_count: number;
  healthy_count: number;
  total_bytes: number;
}

export interface SprawlSummary {
  stale_threshold_s: number;
  source_count: number;
  warehouse_count: number;
  mirror_count: number;
  total_assets: number;
  sources: SprawlSource[];
  duplicates: SprawlDuplicate[];
  duplicate_count: number;
  duplicates_truncated: boolean;
  stale_mirrors: SprawlStaleMirror[];
  ownership_gaps: number;
  owned_mirror_assets: number;
  health: SprawlHealth;
}

// ---------------------------------------------------------------------------
// Governance (Pillar D): tags, policies, bindings, and analytics
// ---------------------------------------------------------------------------

export interface GovTag {
  id: string;
  key: string;
  value: string;
  rendered: string;
  description: string | null;
  created_at: string;
}

export interface ListTagsResponse {
  tags: GovTag[];
}

export interface CreateTagRequest {
  key: string;
  value: string;
  description?: string;
}

export interface AssignmentTarget {
  securable_type: "table" | "namespace" | "column";
  warehouse: string;
  namespace: string;
  table?: string;
  column?: string;
}

export interface AssignTagRequest {
  tag_id: string;
  target: AssignmentTarget;
  source?: "manual" | "classifier";
  confidence?: number;
  approved?: boolean;
}

export interface GovAssignment {
  id: string;
  tag_id: string;
  securable_type: string;
  securable_id: string;
  column_name: string | null;
  source: string;
  approved: boolean;
  created_at: string;
}

export type GovPolicyKind = "row_filter" | "column_mask" | "abac";

export interface GovPolicy {
  id: string;
  name: string;
  kind: GovPolicyKind;
  version: number;
  enabled: boolean;
  // The typed AbacRule definition (shape depends on kind).
  definition: unknown;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface ListGovPoliciesResponse {
  policies: GovPolicy[];
}

export interface CreateGovPolicyRequest {
  name: string;
  kind: GovPolicyKind;
  definition: unknown;
}

export interface BindPolicyRequest {
  target_type: "tag" | "table" | "namespace";
  tag_id?: string;
  warehouse?: string;
  namespace?: string;
  table?: string;
}

export interface GovBinding {
  id: string;
  policy_id: string;
  target_type: string;
  target_id: string;
  bound_by: string;
  created_at: string;
}

export interface EffectivePolicyResponse {
  principal: string;
  table: string;
  purpose: string | null;
  denied: boolean;
  reason: string;
  applied_policies: string[];
  row_filter: unknown | null;
  masked_columns: string[];
}

export interface DriftAlert {
  table_id: string;
  column: string;
  tag: string;
  issue: string;
}

export interface DriftResponse {
  warehouse: string;
  alert_count: number;
  alerts: DriftAlert[];
}
