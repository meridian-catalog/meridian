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
