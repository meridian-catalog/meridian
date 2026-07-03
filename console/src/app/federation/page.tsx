"use client";

// The Federation page (Pillar B): the catalogs Meridian knows about beyond its
// own warehouses — external catalogs registered as *mirrors* — plus the
// cross-catalog *sprawl* dashboard. Everything here is real data from the
// /api/v2/mirrors and /api/v2/federation/sprawl surfaces.

import { useState } from "react";
import {
  Boxes,
  Copy,
  GitCompareArrows,
  Layers,
  RefreshCw,
  Trash2,
  UserX,
  Clock,
} from "lucide-react";
import { api } from "@/lib/api";
import { fmtBytes, fmtCount, fmtTime, timeAgo } from "@/lib/utils";
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
import type { Mirror, SprawlSummary } from "@/lib/types";

export default function FederationPage() {
  return (
    <div>
      <PageHeader
        title="Federation"
        description="External catalogs Meridian tracks (mirrors) and the cross-catalog sprawl dashboard: asset counts per source, duplicate storage locations, and sync staleness."
      />
      <div className="space-y-4">
        <SprawlSection />
        <MirrorsSection />
      </div>
    </div>
  );
}

// ---- sprawl dashboard ------------------------------------------------------

function SprawlSection() {
  const sprawl = useAsync(() => api.sprawl(), []);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <GitCompareArrows className="h-4 w-4 text-muted-foreground" /> Sprawl
          overview
          <Button
            variant="ghost"
            size="sm"
            className="ml-auto"
            onClick={() => sprawl.reload()}
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Async state={sprawl} loadingLabel="Computing sprawl…">
          {(s) => <SprawlBody summary={s} />}
        </Async>
      </CardContent>
    </Card>
  );
}

function SprawlBody({ summary }: { summary: SprawlSummary }) {
  return (
    <div className="space-y-5">
      {/* Top-line stats */}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat
          label="Sources"
          value={fmtCount(summary.source_count)}
          hint={`${summary.warehouse_count} warehouses · ${summary.mirror_count} mirrors`}
        />
        <Stat label="Total assets" value={fmtCount(summary.total_assets)} />
        <Stat
          label="Duplicate locations"
          value={fmtCount(summary.duplicate_count)}
          tone={summary.duplicate_count > 0 ? "warning" : undefined}
        />
        <Stat
          label="Stale mirrors"
          value={fmtCount(summary.stale_mirrors.length)}
          tone={summary.stale_mirrors.length > 0 ? "warning" : undefined}
        />
      </div>

      {/* Per-source asset counts */}
      <div>
        <p className="mb-2 flex items-center gap-1.5 text-xs uppercase tracking-wide text-muted-foreground">
          <Layers className="h-3.5 w-3.5" /> Assets per source
        </p>
        {summary.sources.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            No sources yet. Register a warehouse or a mirror.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                  <th className="py-2 pr-4 font-medium">Source</th>
                  <th className="py-2 pr-4 font-medium">Type</th>
                  <th className="py-2 pr-4 font-medium">Kind</th>
                  <th className="py-2 pr-4 font-medium">Assets</th>
                  <th className="py-2 font-medium">Last synced</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {summary.sources.map((src) => (
                  <tr key={`${src.source_type}:${src.source_id}`}>
                    <td className="py-2 pr-4 font-mono text-xs">{src.name}</td>
                    <td className="py-2 pr-4">
                      <Badge
                        variant={
                          src.source_type === "warehouse"
                            ? "secondary"
                            : "outline"
                        }
                      >
                        {src.source_type}
                      </Badge>
                    </td>
                    <td className="py-2 pr-4 text-muted-foreground">
                      {src.kind}
                    </td>
                    <td className="py-2 pr-4 font-mono tabular-nums">
                      {fmtCount(src.asset_count)}
                    </td>
                    <td className="py-2 text-xs text-muted-foreground">
                      {src.source_type === "warehouse"
                        ? "live"
                        : src.last_synced_at
                          ? timeAgo(src.last_synced_at)
                          : "never"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Duplicate storage locations */}
      {summary.duplicates.length > 0 && (
        <div>
          <p className="mb-2 flex items-center gap-1.5 text-xs uppercase tracking-wide text-muted-foreground">
            <Copy className="h-3.5 w-3.5" /> Duplicate storage locations
          </p>
          <p className="mb-2 text-xs text-muted-foreground">
            The same physical location registered by more than one catalog — a
            zero-copy overlap worth reconciling.
          </p>
          <div className="space-y-1.5">
            {summary.duplicates.map((d) => (
              <div
                key={d.storage_location}
                className="rounded-md border border-amber-500/20 bg-amber-500/5 px-3 py-2 text-sm"
              >
                <p className="truncate font-mono text-xs">
                  {d.storage_location}
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {d.source_count} sources: {d.sources.join(", ")}
                </p>
              </div>
            ))}
          </div>
          {summary.duplicates_truncated && (
            <p className="mt-2 text-xs text-muted-foreground">
              Showing {summary.duplicates.length} of {summary.duplicate_count}{" "}
              duplicated locations.
            </p>
          )}
        </div>
      )}

      {/* Stale mirrors */}
      {summary.stale_mirrors.length > 0 && (
        <div>
          <p className="mb-2 flex items-center gap-1.5 text-xs uppercase tracking-wide text-muted-foreground">
            <Clock className="h-3.5 w-3.5" /> Stale mirrors
          </p>
          <div className="flex flex-wrap gap-2">
            {summary.stale_mirrors.map((m) => (
              <Badge key={m.mirror_id} variant="warning">
                {m.name} ·{" "}
                {m.last_synced_at ? timeAgo(m.last_synced_at) : "never synced"}
              </Badge>
            ))}
          </div>
        </div>
      )}

      {/* Ownership + health roll-up */}
      <div className="grid gap-3 sm:grid-cols-2">
        <div className="rounded-md border border-border bg-muted/20 p-3">
          <p className="flex items-center gap-1.5 text-xs uppercase tracking-wide text-muted-foreground">
            <UserX className="h-3.5 w-3.5" /> Ownership
          </p>
          <p className="mt-1 text-sm">
            <span className="font-semibold">
              {fmtCount(summary.ownership_gaps)}
            </span>{" "}
            mirror asset(s) with no known owner
            {summary.owned_mirror_assets + summary.ownership_gaps > 0 && (
              <span className="text-muted-foreground">
                {" "}
                · {fmtCount(summary.owned_mirror_assets)} owned
              </span>
            )}
          </p>
        </div>
        <div className="rounded-md border border-border bg-muted/20 p-3">
          <p className="flex items-center gap-1.5 text-xs uppercase tracking-wide text-muted-foreground">
            <Boxes className="h-3.5 w-3.5" /> Native health roll-up
          </p>
          {summary.health.tables_scored === 0 ? (
            <p className="mt-1 text-sm text-muted-foreground">
              No native tables scored yet.
            </p>
          ) : (
            <p className="mt-1 text-sm">
              <span className="font-semibold">
                {summary.health.avg_score.toFixed(0)}
              </span>{" "}
              avg score across {fmtCount(summary.health.tables_scored)} tables ·{" "}
              {fmtBytes(summary.health.total_bytes)}
              <span className="ml-1 text-xs text-muted-foreground">
                ({summary.health.healthy_count} healthy /{" "}
                {summary.health.degraded_count} degraded /{" "}
                {summary.health.unhealthy_count} unhealthy)
              </span>
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

// ---- mirrors list + create form -------------------------------------------

function MirrorsSection() {
  const mirrors = useAsync(() => api.listMirrors(), []);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <GitCompareArrows className="h-4 w-4 text-muted-foreground" /> Mirrors
          <Button
            variant="ghost"
            size="sm"
            className="ml-auto"
            onClick={() => mirrors.reload()}
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <CreateMirrorForm onCreated={() => mirrors.reload()} />
        <Async state={mirrors} loadingLabel="Loading mirrors…">
          {(data) =>
            data.mirrors.length === 0 ? (
              <p className="py-2 text-sm text-muted-foreground">
                No mirrors registered. Register one above to track an external
                Iceberg REST or Glue catalog.
              </p>
            ) : (
              <MirrorsTable
                mirrors={data.mirrors}
                onChanged={() => mirrors.reload()}
              />
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function MirrorsTable({
  mirrors,
  onChanged,
}: {
  mirrors: Mirror[];
  onChanged: () => void;
}) {
  const toast = useToast();
  const [busy, setBusy] = useState<string | null>(null);

  async function sync(name: string) {
    setBusy(name);
    try {
      const run = await api.syncMirror(name);
      toast.success("Sync requested", `${name} (${run.status})`);
      onChanged();
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Sync failed", e.message);
    } finally {
      setBusy(null);
    }
  }

  async function remove(name: string) {
    setBusy(name);
    try {
      await api.deleteMirror(name);
      toast.success("Mirror deleted", name);
      onChanged();
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Delete failed", e.message);
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
            <th className="py-2 pr-4 font-medium">Endpoint</th>
            <th className="py-2 pr-4 font-medium">Enabled</th>
            <th className="py-2 pr-4 font-medium">Assets</th>
            <th className="py-2 pr-4 font-medium">Sync status</th>
            <th className="py-2 font-medium"></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {mirrors.map((m) => (
            <tr key={m.id}>
              <td className="py-2 pr-4 font-mono text-xs">{m.name}</td>
              <td className="py-2 pr-4">
                <Badge variant="outline">{m.kind}</Badge>
              </td>
              <td className="py-2 pr-4 max-w-[16rem] truncate font-mono text-xs text-muted-foreground">
                {m.endpoint}
              </td>
              <td className="py-2 pr-4">
                {m.enabled ? (
                  <Badge variant="success">enabled</Badge>
                ) : (
                  <Badge variant="secondary">disabled</Badge>
                )}
              </td>
              <td className="py-2 pr-4 font-mono tabular-nums">
                {fmtCount(m.asset_count)}
              </td>
              <td className="py-2 pr-4">
                <SyncStatus mirror={m} />
              </td>
              <td className="py-2 text-right">
                <div className="flex justify-end gap-1">
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy === m.name || !m.enabled}
                    onClick={() => sync(m.name)}
                    title={
                      m.enabled ? "Sync now" : "Enable the mirror to sync it"
                    }
                  >
                    <RefreshCw className="h-3.5 w-3.5" /> Sync
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy === m.name}
                    onClick={() => remove(m.name)}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
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

function SyncStatus({ mirror }: { mirror: Mirror }) {
  if (!mirror.last_sync_status) {
    return <span className="text-xs text-muted-foreground">never synced</span>;
  }
  const variant =
    mirror.last_sync_status === "ok"
      ? "success"
      : mirror.last_sync_status === "error"
        ? "danger"
        : "default";
  return (
    <span className="flex items-center gap-1.5">
      <Badge variant={variant}>{mirror.last_sync_status}</Badge>
      <span className="text-xs text-muted-foreground">
        {fmtTime(mirror.last_synced_at)}
      </span>
    </span>
  );
}

function CreateMirrorForm({ onCreated }: { onCreated: () => void }) {
  const toast = useToast();
  const [name, setName] = useState("");
  const [kind, setKind] = useState("iceberg-rest");
  const [endpoint, setEndpoint] = useState("");
  const [remoteCatalog, setRemoteCatalog] = useState("");
  const [intervalS, setIntervalS] = useState("3600");
  const [submitting, setSubmitting] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    try {
      await api.createMirror({
        name: name.trim(),
        kind,
        endpoint: endpoint.trim(),
        remote_catalog: remoteCatalog.trim() || undefined,
        sync_interval_s: Number(intervalS) || 3600,
      });
      toast.success("Mirror created", name.trim());
      setName("");
      setEndpoint("");
      setRemoteCatalog("");
      onCreated();
    } catch (err) {
      const ex = err instanceof Error ? err : new Error(String(err));
      toast.error("Create failed", ex.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form
      onSubmit={submit}
      className="rounded-md border border-border bg-muted/10 p-3"
    >
      <p className="mb-3 text-xs uppercase tracking-wide text-muted-foreground">
        Register a mirror
      </p>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <div>
          <Label className="mb-1 block text-xs">Name</Label>
          <Input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="prod-polaris"
            required
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Kind</Label>
          <Select value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="iceberg-rest">iceberg-rest</option>
            <option value="glue">glue</option>
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">
            {kind === "glue" ? "AWS region" : "Endpoint URI"}
          </Label>
          <Input
            value={endpoint}
            onChange={(e) => setEndpoint(e.target.value)}
            placeholder={
              kind === "glue" ? "us-east-1" : "http://host/api/catalog"
            }
            required
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Remote catalog</Label>
          <Input
            value={remoteCatalog}
            onChange={(e) => setRemoteCatalog(e.target.value)}
            placeholder="optional"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Sync interval (s)</Label>
          <Input
            type="number"
            min={1}
            value={intervalS}
            onChange={(e) => setIntervalS(e.target.value)}
          />
        </div>
      </div>
      <div className="mt-3 flex justify-end">
        <Button type="submit" size="sm" disabled={submitting}>
          {submitting ? "Registering…" : "Register mirror"}
        </Button>
      </div>
    </form>
  );
}

// ---- small shared UI -------------------------------------------------------

function Stat({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "warning" | "danger";
}) {
  const toneClass =
    tone === "warning"
      ? "text-amber-400"
      : tone === "danger"
        ? "text-destructive"
        : "text-foreground";
  return (
    <div className="rounded-md border border-border bg-muted/20 p-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={`mt-0.5 text-lg font-semibold ${toneClass}`}>{value}</p>
      {hint && <p className="mt-0.5 text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}
