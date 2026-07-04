"use client";

import { useState } from "react";
import { Activity, AlertTriangle, Gauge, Plus } from "lucide-react";
import { api } from "@/lib/api";
import { timeAgo } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync } from "@/components/states";
import { useToast } from "@/components/toast";
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Label,
  Select,
} from "@/components/ui/primitives";
import {
  MONITOR_KINDS,
  MONITOR_SEVERITIES,
  type CreateMonitorRequest,
  type Incident,
  type QualityScoreResponse,
} from "@/lib/types";

export default function QualityPage() {
  const monitors = useAsync(() => api.listMonitors(), []);
  const incidents = useAsync(() => api.listIncidents({ limit: 100 }), []);

  return (
    <div>
      <PageHeader
        title="Data quality"
        description="Zero-scan monitors, incidents, and per-table trust scores. Monitors are evaluated from the commit stream — no data scan."
      />

      <div className="space-y-4">
        {/* Incidents first — the thing an operator opens this page to see. */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-muted-foreground" />{" "}
              Incidents
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Async state={incidents} loadingLabel="Loading incidents…">
              {(data) =>
                data.incidents.length === 0 ? (
                  <p className="py-4 text-sm text-muted-foreground">
                    No incidents. Every monitored table is healthy.
                  </p>
                ) : (
                  <IncidentTable
                    incidents={data.incidents}
                    onChanged={() => {
                      incidents.reload();
                    }}
                  />
                )
              }
            </Async>
          </CardContent>
        </Card>

        {/* Monitors + create form. */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-4 w-4 text-muted-foreground" /> Monitors
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <CreateMonitorForm onCreated={() => monitors.reload()} />
            <Async state={monitors} loadingLabel="Loading monitors…">
              {(data) =>
                data.monitors.length === 0 ? (
                  <p className="py-4 text-sm text-muted-foreground">
                    No monitors yet. Create one above to start watching a table
                    or namespace.
                  </p>
                ) : (
                  <MonitorTable
                    monitors={data.monitors}
                    onChanged={() => monitors.reload()}
                  />
                )
              }
            </Async>
          </CardContent>
        </Card>

        {/* Per-table quality score lookup. */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Gauge className="h-4 w-4 text-muted-foreground" /> Trust score
            </CardTitle>
          </CardHeader>
          <CardContent>
            <QualityScoreLookup />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

type BadgeVariant =
  | "default"
  | "secondary"
  | "success"
  | "warning"
  | "danger"
  | "outline";

function severityVariant(severity: string): BadgeVariant {
  switch (severity) {
    case "high":
      return "danger";
    case "medium":
      return "warning";
    default:
      return "secondary";
  }
}

function statusVariant(status: string): BadgeVariant {
  switch (status) {
    case "open":
      return "danger";
    case "acknowledged":
      return "warning";
    default:
      return "success";
  }
}

function IncidentTable({
  incidents,
  onChanged,
}: {
  incidents: Incident[];
  onChanged: () => void;
}) {
  const toast = useToast();
  const [busy, setBusy] = useState<string | null>(null);

  async function act(id: string, action: "ack" | "resolve") {
    setBusy(id);
    try {
      if (action === "ack") {
        await api.ackIncident(id);
        toast.success("Incident acknowledged");
      } else {
        await api.resolveIncident(id);
        toast.success("Incident resolved");
      }
      onChanged();
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Action failed", e2.message);
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
            <th className="py-2 pr-4 font-medium">Status</th>
            <th className="py-2 pr-4 font-medium">Severity</th>
            <th className="py-2 pr-4 font-medium">Table</th>
            <th className="py-2 pr-4 font-medium">What</th>
            <th className="py-2 pr-4 font-medium">Owner</th>
            <th className="py-2 pr-4 font-medium">Downstream</th>
            <th className="py-2 pr-4 font-medium">Seen</th>
            <th className="py-2 font-medium">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {incidents.map((i) => (
            <tr key={i.id}>
              <td className="py-2 pr-4">
                <Badge variant={statusVariant(i.status)}>{i.status}</Badge>
              </td>
              <td className="py-2 pr-4">
                <Badge variant={severityVariant(i.severity)}>
                  {i.severity}
                </Badge>
              </td>
              <td className="py-2 pr-4 font-mono text-xs">{i.table_ident}</td>
              <td className="py-2 pr-4 text-muted-foreground">
                <div>{i.title}</div>
                <div className="text-xs opacity-70">{i.detail}</div>
              </td>
              <td className="py-2 pr-4 text-xs">{i.owner ?? "—"}</td>
              <td className="py-2 pr-4 text-xs">
                {i.blast_radius.length === 0 ? (
                  <span className="text-muted-foreground">none</span>
                ) : (
                  <span title={i.blast_radius.map((a) => a.ident ?? a.table_id).join(", ")}>
                    {i.blast_radius.length} asset
                    {i.blast_radius.length === 1 ? "" : "s"}
                  </span>
                )}
              </td>
              <td className="py-2 pr-4 text-xs text-muted-foreground">
                {timeAgo(i.last_seen_at)}
                {i.occurrence_count > 1 ? ` ×${i.occurrence_count}` : ""}
              </td>
              <td className="py-2">
                <div className="flex gap-2">
                  {i.status === "open" && (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy === i.id}
                      onClick={() => act(i.id, "ack")}
                    >
                      Ack
                    </Button>
                  )}
                  {i.status !== "resolved" && (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy === i.id}
                      onClick={() => act(i.id, "resolve")}
                    >
                      Resolve
                    </Button>
                  )}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MonitorTable({
  monitors,
  onChanged,
}: {
  monitors: import("@/lib/types").Monitor[];
  onChanged: () => void;
}) {
  const toast = useToast();
  const [busy, setBusy] = useState<string | null>(null);

  async function toggle(id: string, enabled: boolean) {
    setBusy(id);
    try {
      await api.setMonitorEnabled(id, enabled);
      onChanged();
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Update failed", e2.message);
    } finally {
      setBusy(null);
    }
  }

  async function remove(id: string) {
    setBusy(id);
    try {
      await api.deleteMonitor(id);
      toast.success("Monitor deleted");
      onChanged();
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Delete failed", e2.message);
    } finally {
      setBusy(null);
    }
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
            <th className="py-2 pr-4 font-medium">Name</th>
            <th className="py-2 pr-4 font-medium">Kind</th>
            <th className="py-2 pr-4 font-medium">Bound to</th>
            <th className="py-2 pr-4 font-medium">Severity</th>
            <th className="py-2 pr-4 font-medium">Enabled</th>
            <th className="py-2 font-medium">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {monitors.map((m) => (
            <tr key={m.id}>
              <td className="py-2 pr-4 font-mono text-xs">{m.name}</td>
              <td className="py-2 pr-4">
                <Badge variant="outline">{m.kind}</Badge>
              </td>
              <td className="py-2 pr-4 text-xs text-muted-foreground">
                {m.bound_to}
              </td>
              <td className="py-2 pr-4">
                <Badge variant={severityVariant(m.severity)}>
                  {m.severity}
                </Badge>
              </td>
              <td className="py-2 pr-4">
                <Badge variant={m.enabled ? "default" : "secondary"}>
                  {m.enabled ? "on" : "off"}
                </Badge>
              </td>
              <td className="py-2">
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={busy === m.id}
                    onClick={() => toggle(m.id, !m.enabled)}
                  >
                    {m.enabled ? "Disable" : "Enable"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={busy === m.id}
                    onClick={() => remove(m.id)}
                  >
                    Delete
                  </Button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CreateMonitorForm({ onCreated }: { onCreated: () => void }) {
  const toast = useToast();
  const [name, setName] = useState("");
  const [warehouse, setWarehouse] = useState("");
  const [boundTo, setBoundTo] = useState<"table" | "namespace">("table");
  const [namespace, setNamespace] = useState("");
  const [table, setTable] = useState("");
  const [kind, setKind] = useState<string>(MONITOR_KINDS[0]);
  const [severity, setSeverity] = useState<string>("medium");
  const [submitting, setSubmitting] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim() || !warehouse.trim() || !namespace.trim()) {
      toast.error("Name, warehouse and namespace are required");
      return;
    }
    if (boundTo === "table" && !table.trim()) {
      toast.error("A table binding needs a table name");
      return;
    }
    const body: CreateMonitorRequest = {
      name: name.trim(),
      warehouse: warehouse.trim(),
      bound_to: boundTo,
      namespace: namespace.trim(),
      kind,
      severity,
      ...(boundTo === "table" ? { table: table.trim() } : {}),
    };
    setSubmitting(true);
    try {
      await api.createMonitor(body);
      toast.success("Monitor created", `${kind} on ${boundTo}`);
      setName("");
      setTable("");
      onCreated();
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Create failed", e2.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form
      onSubmit={submit}
      className="rounded-md border border-border bg-muted/20 p-4"
    >
      <p className="mb-3 flex items-center gap-2 text-sm font-medium">
        <Plus className="h-4 w-4" /> Create monitor
      </p>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <div>
          <Label className="mb-1 block text-xs">Name</Label>
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="orders-volume"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Kind</Label>
          <Select value={kind} onChange={(e) => setKind(e.target.value)}>
            {MONITOR_KINDS.map((k) => (
              <option key={k} value={k}>
                {k}
              </option>
            ))}
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">Severity</Label>
          <Select
            value={severity}
            onChange={(e) => setSeverity(e.target.value)}
          >
            {MONITOR_SEVERITIES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">Warehouse</Label>
          <Input
            value={warehouse}
            onChange={(e) => setWarehouse(e.target.value)}
            placeholder="warehouse name"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Bind to</Label>
          <Select
            value={boundTo}
            onChange={(e) =>
              setBoundTo(e.target.value as "table" | "namespace")
            }
          >
            <option value="table">table</option>
            <option value="namespace">namespace</option>
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">Namespace (dotted)</Label>
          <Input
            value={namespace}
            onChange={(e) => setNamespace(e.target.value)}
            placeholder="analytics.reporting"
          />
        </div>
        {boundTo === "table" && (
          <div>
            <Label className="mb-1 block text-xs">Table name</Label>
            <Input
              value={table}
              onChange={(e) => setTable(e.target.value)}
              placeholder="orders"
            />
          </div>
        )}
      </div>
      <div className="mt-4 flex justify-end">
        <Button type="submit" disabled={submitting}>
          {submitting ? "Creating…" : "Create monitor"}
        </Button>
      </div>
    </form>
  );
}

function scoreColor(score: number): string {
  if (score >= 80) return "text-emerald-600 dark:text-emerald-400";
  if (score >= 60) return "text-amber-600 dark:text-amber-400";
  return "text-destructive";
}

function QualityScoreLookup() {
  const toast = useToast();
  const [warehouse, setWarehouse] = useState("");
  const [namespace, setNamespace] = useState("");
  const [table, setTable] = useState("");
  const [result, setResult] = useState<QualityScoreResponse | null>(null);
  const [loading, setLoading] = useState(false);

  async function lookup(e: React.FormEvent) {
    e.preventDefault();
    if (!warehouse.trim() || !namespace.trim() || !table.trim()) {
      toast.error("Warehouse, namespace and table are required");
      return;
    }
    setLoading(true);
    try {
      const levels = namespace.split(".").map((s) => s.trim()).filter(Boolean);
      const res = await api.tableQualityScore(
        warehouse.trim(),
        levels,
        table.trim(),
      );
      setResult(res);
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Lookup failed", e2.message);
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <form onSubmit={lookup} className="flex flex-wrap items-end gap-2">
        <div>
          <Label className="mb-1 block text-xs">Warehouse</Label>
          <Input
            value={warehouse}
            onChange={(e) => setWarehouse(e.target.value)}
            placeholder="warehouse"
            className="max-w-[10rem]"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Namespace</Label>
          <Input
            value={namespace}
            onChange={(e) => setNamespace(e.target.value)}
            placeholder="analytics"
            className="max-w-[12rem]"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Table</Label>
          <Input
            value={table}
            onChange={(e) => setTable(e.target.value)}
            placeholder="orders"
            className="max-w-[10rem]"
          />
        </div>
        <Button type="submit" disabled={loading}>
          {loading ? "Scoring…" : "Score"}
        </Button>
      </form>

      {result && (
        <div className="mt-4 rounded-md border border-border p-4">
          <div className="flex items-baseline gap-3">
            <span className={`text-3xl font-semibold ${scoreColor(result.score)}`}>
              {result.score}
            </span>
            <span className="text-lg text-muted-foreground">
              / 100 · grade {result.grade}
            </span>
            <span className="ml-auto font-mono text-xs text-muted-foreground">
              {result.ident}
            </span>
          </div>
          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-5">
            {(
              [
                ["monitors", result.components.monitors],
                ["contract", result.components.contract],
                ["ownership", result.components.ownership],
                ["docs", result.components.docs],
                ["freshness", result.components.freshness],
              ] as const
            ).map(([label, value]) => (
              <div key={label}>
                <div className="text-xs uppercase tracking-wide text-muted-foreground">
                  {label}
                </div>
                <div className="mt-1 h-2 w-full rounded bg-muted">
                  <div
                    className="h-2 rounded bg-primary"
                    style={{ width: `${Math.round(value * 100)}%` }}
                  />
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {Math.round(value * 100)}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
