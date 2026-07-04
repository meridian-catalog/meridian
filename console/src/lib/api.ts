// Typed API client for Meridian.
//
// Pure browser-side client: it talks only to the configured Meridian base URL
// (NEXT_PUBLIC_MERIDIAN_URL) and the standard IRC + /api/v2 endpoints. No
// server-side secrets, no Next.js API routes, no backdoors. An optional bearer
// token (for oidc-mode servers) is held in session memory only — see auth.ts.

import type {
  AssignTagRequest,
  AuditQueryParams,
  AuditQueryResponse,
  BindPolicyRequest,
  ConfigResponse,
  CreateGovPolicyRequest,
  CreateGrantRequest,
  CreateMirrorRequest,
  CreateMonitorRequest,
  CreateTagRequest,
  CreateWebhookRequest,
  DriftResponse,
  EffectivePolicyResponse,
  FeedResponse,
  Incident,
  ListIncidentsResponse,
  ListMonitorResultsResponse,
  ListMonitorsResponse,
  Monitor,
  QualityScoreResponse,
  GovAssignment,
  GovBinding,
  GovPolicy,
  GovTag,
  Grant,
  ListGovPoliciesResponse,
  ListTagsResponse,
  HealthHistoryResponse,
  HealthResponse,
  ListDeliveriesResponse,
  ListGrantsResponse,
  ListJobsResponse,
  ListMirrorsResponse,
  ListNamespacesResponse,
  ListPoliciesResponse,
  ListPrincipalsResponse,
  ListRolesResponse,
  ListSavingsResponse,
  ListTablesResponse,
  ListWarehousesResponse,
  ListWebhooksResponse,
  LoadTableResult,
  LoadViewResult,
  MaintenanceJob,
  Mirror,
  MirrorSyncStatus,
  NamespaceResponse,
  PermissionsResponse,
  SavingsRollupResponse,
  SearchResponse,
  SprawlSummary,
  TableHealth,
  TriggerJobRequest,
  VerifyChainResponse,
  WarehouseHealthSummary,
  Webhook,
} from "./types";

// The 0x1F unit separator the IRC uses to encode multi-level namespaces in a
// single path segment. Each level is percent-encoded, then joined with %1F.
const NS_SEPARATOR = "\x1f";

/** Base URL of the server, from the public env var (default localhost:8181). */
export function baseUrl(): string {
  const raw = process.env.NEXT_PUBLIC_MERIDIAN_URL ?? "http://localhost:8181";
  return raw.replace(/\/+$/, "");
}

/**
 * An error carrying the parsed IRC error envelope. `message` is the
 * server-provided message when available, so it can be surfaced verbatim in a
 * toast; `type` and `status` add machine-readable context.
 */
export class ApiError extends Error {
  readonly type: string;
  readonly status: number;

  constructor(message: string, type: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.type = type;
    this.status = status;
  }
}

/** In-session bearer token accessor, injected by the auth layer. */
let tokenProvider: () => string | null = () => null;

export function setTokenProvider(fn: () => string | null): void {
  tokenProvider = fn;
}

interface RequestOptions {
  method?: string;
  body?: unknown;
  // For endpoints that return 204 No Content.
  expectNoContent?: boolean;
  signal?: AbortSignal;
}

async function request<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const url = `${baseUrl()}${path}`;
  const headers: Record<string, string> = { Accept: "application/json" };
  const token = tokenProvider();
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";

  let res: Response;
  try {
    res = await fetch(url, {
      method: opts.method ?? "GET",
      headers,
      body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
      signal: opts.signal,
      cache: "no-store",
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") throw err;
    // Network-level failure (server down, CORS, DNS). No envelope to parse.
    throw new ApiError(
      `Could not reach the Meridian server at ${baseUrl()}. Is it running, and does NEXT_PUBLIC_MERIDIAN_URL point at it?`,
      "NetworkError",
      0,
    );
  }

  if (!res.ok) {
    throw await toApiError(res);
  }

  if (opts.expectNoContent || res.status === 204) {
    return undefined as T;
  }

  const text = await res.text();
  if (!text) return undefined as T;
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ApiError(
      "Server returned a non-JSON response.",
      "MalformedResponse",
      res.status,
    );
  }
}

/** Parses a non-2xx response into an ApiError, preferring the IRC envelope. */
async function toApiError(res: Response): Promise<ApiError> {
  let message = `Request failed with status ${res.status}`;
  let type = "HttpError";
  try {
    const body = await res.json();
    if (body && typeof body === "object" && "error" in body) {
      const env = (body as { error?: { message?: string; type?: string; code?: number } }).error;
      if (env?.message) message = env.message;
      if (env?.type) type = env.type;
    }
  } catch {
    // No JSON body; fall back to the status-derived message.
  }
  if (res.status === 401) {
    type = type === "HttpError" ? "NotAuthorizedException" : type;
    if (message.startsWith("Request failed")) {
      message =
        "Unauthorized. This server requires a bearer token — set one in the top bar.";
    }
  }
  if (res.status === 403 && message.startsWith("Request failed")) {
    message = "Forbidden. Your token lacks management access for this resource.";
  }
  return new ApiError(message, type, res.status);
}

/** Encodes a namespace level array into a single IRC path segment. */
export function encodeNamespace(levels: string[]): string {
  return levels.map((l) => encodeURIComponent(l)).join(encodeURIComponent(NS_SEPARATOR));
}

function qs(params: Record<string, string | number | undefined>): string {
  const entries = Object.entries(params).filter(
    ([, v]) => v !== undefined && v !== "",
  );
  if (entries.length === 0) return "";
  const sp = new URLSearchParams();
  for (const [k, v] of entries) sp.set(k, String(v));
  return `?${sp.toString()}`;
}

export const api = {
  // ---- health / config -------------------------------------------------
  health: () => request<HealthResponse>("/healthz"),
  config: () => request<ConfigResponse>("/v1/config"),

  // ---- warehouses ------------------------------------------------------
  listWarehouses: () => request<ListWarehousesResponse>("/api/v2/warehouses"),

  // ---- principals ------------------------------------------------------
  listPrincipals: () => request<ListPrincipalsResponse>("/api/v2/principals"),

  // ---- catalog (IRC) ---------------------------------------------------
  listNamespaces: (warehouse: string, parent?: string[]) =>
    request<ListNamespacesResponse>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces${
        parent && parent.length ? `?parent=${encodeNamespace(parent)}` : ""
      }`,
    ),

  loadNamespace: (warehouse: string, ns: string[]) =>
    request<NamespaceResponse>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}`,
    ),

  listTables: (warehouse: string, ns: string[]) =>
    request<ListTablesResponse>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/tables`,
    ),

  listViews: (warehouse: string, ns: string[]) =>
    request<ListTablesResponse>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/views`,
    ),

  loadTable: (warehouse: string, ns: string[], table: string) =>
    request<LoadTableResult>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/tables/${encodeURIComponent(table)}?snapshots=all`,
    ),

  loadView: (warehouse: string, ns: string[], view: string) =>
    request<LoadViewResult>(
      `/v1/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/views/${encodeURIComponent(view)}`,
    ),

  // ---- search ----------------------------------------------------------
  search: (params: {
    q: string;
    type?: string;
    warehouse?: string;
    namespace?: string;
    limit?: number;
    page_token?: string;
  }) => request<SearchResponse>(`/api/v2/search${qs(params)}`),

  // ---- governance ------------------------------------------------------
  listRoles: () => request<ListRolesResponse>("/api/v2/roles"),
  listGrants: () => request<ListGrantsResponse>("/api/v2/grants"),
  createGrant: (body: CreateGrantRequest) =>
    request<Grant>("/api/v2/grants", { method: "POST", body }),
  deleteGrant: (id: string) =>
    request<void>(`/api/v2/grants/${encodeURIComponent(id)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  permissions: (principalId: string) =>
    request<PermissionsResponse>(
      `/api/v2/permissions?principal=${encodeURIComponent(principalId)}`,
    ),

  // ---- audit -----------------------------------------------------------
  audit: (params: AuditQueryParams) =>
    request<AuditQueryResponse>(
      `/api/v2/audit${qs({
        principal: params.principal,
        action: params.action,
        resource: params.resource,
        workspace: params.workspace,
        from: params.from,
        to: params.to,
        before: params.before,
        limit: params.limit,
      })}`,
    ),
  verifyAudit: () => request<VerifyChainResponse>("/api/v2/audit/verify"),

  // ---- events + webhooks ----------------------------------------------
  events: (params: {
    after?: string;
    types?: string;
    limit?: number;
    order?: "asc" | "desc";
  }) => request<FeedResponse>(`/api/v2/events${qs(params)}`),
  listWebhooks: () => request<ListWebhooksResponse>("/api/v2/webhooks"),
  createWebhook: (body: CreateWebhookRequest) =>
    request<Webhook>("/api/v2/webhooks", { method: "POST", body }),
  deleteWebhook: (id: string) =>
    request<void>(`/api/v2/webhooks/${encodeURIComponent(id)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  webhookDeliveries: (id: string, status?: string) =>
    request<ListDeliveriesResponse>(
      `/api/v2/webhooks/${encodeURIComponent(id)}/deliveries${qs({ status })}`,
    ),

  // ---- maintenance (Pillar C) -----------------------------------------
  warehouseHealthSummary: (warehouse: string) =>
    request<WarehouseHealthSummary>(
      `/api/v2/warehouses/${encodeURIComponent(warehouse)}/health-summary`,
    ),
  tableHealth: (warehouse: string, ns: string[], table: string) =>
    request<TableHealth>(
      `/api/v2/warehouses/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/tables/${encodeURIComponent(table)}/health`,
    ),
  tableHealthHistory: (warehouse: string, ns: string[], table: string, limit?: number) =>
    request<HealthHistoryResponse>(
      `/api/v2/warehouses/${encodeURIComponent(warehouse)}/namespaces/${encodeNamespace(ns)}/tables/${encodeURIComponent(table)}/health/history${qs({ limit })}`,
    ),
  listJobs: (params: { state?: string; table_id?: string; limit?: number }) =>
    request<ListJobsResponse>(`/api/v2/maintenance/jobs${qs(params)}`),
  getJob: (id: string) =>
    request<MaintenanceJob>(`/api/v2/maintenance/jobs/${encodeURIComponent(id)}`),
  triggerJob: (body: TriggerJobRequest) =>
    request<MaintenanceJob>("/api/v2/maintenance/jobs", { method: "POST", body }),
  cancelJob: (id: string) =>
    request<MaintenanceJob>(`/api/v2/maintenance/jobs/${encodeURIComponent(id)}/cancel`, {
      method: "POST",
    }),
  listPolicies: () => request<ListPoliciesResponse>("/api/v2/maintenance/policies"),
  listSavings: (params: { table_id?: string; limit?: number }) =>
    request<ListSavingsResponse>(`/api/v2/maintenance/savings${qs(params)}`),
  savingsRollup: (months?: number) =>
    request<SavingsRollupResponse>(`/api/v2/maintenance/savings/rollup${qs({ months })}`),

  // ---- federation (Pillar B) ------------------------------------------
  listMirrors: () => request<ListMirrorsResponse>("/api/v2/mirrors"),
  createMirror: (body: CreateMirrorRequest) =>
    request<Mirror>("/api/v2/mirrors", { method: "POST", body }),
  deleteMirror: (name: string) =>
    request<void>(`/api/v2/mirrors/${encodeURIComponent(name)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  syncMirror: (name: string) =>
    request<{ id: string; status: string }>(
      `/api/v2/mirrors/${encodeURIComponent(name)}/sync`,
      { method: "POST" },
    ),
  mirrorSyncStatus: (name: string) =>
    request<MirrorSyncStatus>(
      `/api/v2/mirrors/${encodeURIComponent(name)}/sync`,
    ),
  sprawl: (staleThresholdS?: number) =>
    request<SprawlSummary>(
      `/api/v2/federation/sprawl${qs({ stale_threshold_s: staleThresholdS })}`,
    ),

  // ---- governance (Pillar D) ------------------------------------------
  govListTags: () => request<ListTagsResponse>("/api/v2/governance/tags"),
  govCreateTag: (body: CreateTagRequest) =>
    request<GovTag>("/api/v2/governance/tags", { method: "POST", body }),
  govDeleteTag: (id: string) =>
    request<void>(`/api/v2/governance/tags/${encodeURIComponent(id)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  govAssignTag: (body: AssignTagRequest) =>
    request<GovAssignment>("/api/v2/governance/tags/assignments", {
      method: "POST",
      body,
    }),
  govListPolicies: () =>
    request<ListGovPoliciesResponse>("/api/v2/governance/policies"),
  govCreatePolicy: (body: CreateGovPolicyRequest) =>
    request<GovPolicy>("/api/v2/governance/policies", { method: "POST", body }),
  govDeletePolicy: (id: string) =>
    request<void>(`/api/v2/governance/policies/${encodeURIComponent(id)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  govBindPolicy: (id: string, body: BindPolicyRequest) =>
    request<GovBinding>(
      `/api/v2/governance/policies/${encodeURIComponent(id)}/bindings`,
      { method: "POST", body },
    ),
  govEffectivePolicy: (params: {
    principal: string;
    warehouse: string;
    namespace: string;
    table: string;
    purpose?: string;
  }) =>
    request<EffectivePolicyResponse>(
      `/api/v2/governance/effective-policy${qs(params)}`,
    ),
  govDrift: (warehouse: string) =>
    request<DriftResponse>(`/api/v2/governance/drift${qs({ warehouse })}`),

  // ---- data quality (Pillar E) ----------------------------------------
  listMonitors: () =>
    request<ListMonitorsResponse>("/api/v2/quality/monitors"),
  createMonitor: (body: CreateMonitorRequest) =>
    request<Monitor>("/api/v2/quality/monitors", { method: "POST", body }),
  setMonitorEnabled: (id: string, enabled: boolean) =>
    request<Monitor>(`/api/v2/quality/monitors/${encodeURIComponent(id)}`, {
      method: "PATCH",
      body: { enabled },
    }),
  deleteMonitor: (id: string) =>
    request<void>(`/api/v2/quality/monitors/${encodeURIComponent(id)}`, {
      method: "DELETE",
      expectNoContent: true,
    }),
  listMonitorResults: (params: { monitor_id?: string; table_id?: string; limit?: number }) =>
    request<ListMonitorResultsResponse>(
      `/api/v2/quality/monitors/results${qs(params)}`,
    ),
  listIncidents: (params: {
    table_id?: string;
    status?: string;
    live?: boolean;
    limit?: number;
  }) =>
    request<ListIncidentsResponse>(
      `/api/v2/quality/incidents${qs({
        table_id: params.table_id,
        status: params.status,
        live: params.live ? "true" : undefined,
        limit: params.limit,
      })}`,
    ),
  ackIncident: (id: string) =>
    request<Incident>(`/api/v2/quality/incidents/${encodeURIComponent(id)}/ack`, {
      method: "POST",
    }),
  resolveIncident: (id: string) =>
    request<Incident>(
      `/api/v2/quality/incidents/${encodeURIComponent(id)}/resolve`,
      { method: "POST" },
    ),
  tableQualityScore: (warehouse: string, ns: string[], table: string) =>
    request<QualityScoreResponse>(
      `/api/v2/quality/tables/${encodeURIComponent(warehouse)}/${encodeNamespace(ns)}/${encodeURIComponent(table)}/score`,
    ),
};

export type Api = typeof api;
