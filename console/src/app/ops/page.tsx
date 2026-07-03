"use client";

// The Ops page (spec §8.6 console IA: "Ops (jobs, policies, savings ledger)").
// Everything here is real data from the /api/v2 maintenance surface — the
// fleet health summary, the savings ledger, the per-table health detail
// (score, file histogram, recommendations), and the maintenance job queue.

import { useState } from "react";
import {
  Database,
  Gauge,
  PiggyBank,
  ListChecks,
  RefreshCw,
  Play,
  FileWarning,
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
} from "@/components/ui/primitives";
import type {
  TableHealth,
  WarehouseHealthSummary,
  WorstTable,
} from "@/lib/types";

export default function OpsPage() {
  const warehouses = useAsync(() => api.listWarehouses(), []);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div>
      <PageHeader
        title="Ops"
        description="Autonomous table maintenance: fleet health, the savings ledger, and the job queue."
      />
      <div className="grid gap-4 lg:grid-cols-[240px_1fr]">
        {/* Warehouse picker */}
        <Card className="h-fit">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">
              Warehouses
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <Async state={warehouses}>
              {(w) => {
                // Default-select the first warehouse once loaded.
                if (selected === null && w.warehouses.length > 0) {
                  setSelected(w.warehouses[0].name);
                }
                return w.warehouses.length === 0 ? (
                  <p className="py-4 text-sm text-muted-foreground">
                    No warehouses registered.
                  </p>
                ) : (
                  <ul className="space-y-0.5">
                    {w.warehouses.map((wh) => (
                      <li key={wh.id}>
                        <button
                          onClick={() => setSelected(wh.name)}
                          className={`flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left text-sm transition-colors ${
                            selected === wh.name
                              ? "bg-accent text-accent-foreground"
                              : "hover:bg-accent/50"
                          }`}
                        >
                          <Database className="h-4 w-4 shrink-0 text-muted-foreground" />
                          <span className="truncate font-mono text-xs">
                            {wh.name}
                          </span>
                        </button>
                      </li>
                    ))}
                  </ul>
                );
              }}
            </Async>
          </CardContent>
        </Card>

        {/* Right column */}
        <div className="space-y-4">
          {selected ? (
            <WarehouseOps warehouse={selected} />
          ) : (
            <Card>
              <CardContent className="py-12 text-center text-sm text-muted-foreground">
                Select a warehouse to view its maintenance overview.
              </CardContent>
            </Card>
          )}
          <SavingsSection />
          <JobsSection />
        </div>
      </div>
    </div>
  );
}

// ---- Fleet health for one warehouse ---------------------------------------

function WarehouseOps({ warehouse }: { warehouse: string }) {
  const summary = useAsync(
    () => api.warehouseHealthSummary(warehouse),
    [warehouse],
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Gauge className="h-4 w-4 text-muted-foreground" /> Fleet health —{" "}
          <span className="font-mono text-sm">{warehouse}</span>
          <Button
            variant="ghost"
            size="sm"
            className="ml-auto"
            onClick={() => summary.reload()}
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Async state={summary} loadingLabel="Loading fleet health…">
          {(s) =>
            s.tables_scored === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No health has been computed for any table in this warehouse yet.
                Health is computed on demand (open a table below) or by the
                background reconciler.
              </p>
            ) : (
              <FleetSummary summary={s} warehouse={warehouse} />
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function FleetSummary({
  summary,
  warehouse,
}: {
  summary: WarehouseHealthSummary;
  warehouse: string;
}) {
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat label="Tables scored" value={fmtCount(summary.tables_scored)} />
        <Stat
          label="Avg score"
          value={summary.avg_score.toFixed(0)}
          tone={scoreTone(Math.round(summary.avg_score))}
        />
        <Stat label="Total data" value={fmtBytes(summary.total_bytes)} />
        <Stat
          label="Data files"
          value={fmtCount(summary.total_data_files)}
        />
      </div>

      {/* Health distribution bar */}
      <div>
        <p className="mb-1.5 text-xs uppercase tracking-wide text-muted-foreground">
          Health distribution
        </p>
        <HealthBar
          healthy={summary.healthy_count}
          degraded={summary.degraded_count}
          unhealthy={summary.unhealthy_count}
        />
        <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground">
          <LegendDot className="bg-emerald-500" label={`Healthy (${summary.healthy_count})`} />
          <LegendDot className="bg-amber-500" label={`Degraded (${summary.degraded_count})`} />
          <LegendDot className="bg-destructive" label={`Unhealthy (${summary.unhealthy_count})`} />
        </div>
      </div>

      {/* Worst tables */}
      {summary.worst_tables.length > 0 && (
        <div>
          <p className="mb-2 text-xs uppercase tracking-wide text-muted-foreground">
            Tables needing attention
          </p>
          <div className="space-y-1.5">
            {summary.worst_tables.map((t) => (
              <TableHealthRow key={t.table_id} warehouse={warehouse} table={t} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ---- Per-table health detail (expandable) ---------------------------------

function TableHealthRow({
  warehouse,
  table,
}: {
  warehouse: string;
  table: WorstTable;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-border">
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-3 px-3 py-2 text-left text-sm hover:bg-accent/40"
      >
        <ScoreBadge score={table.score} />
        <span className="truncate font-mono text-xs">{table.table_ident}</span>
        <span className="ml-auto shrink-0 text-xs text-muted-foreground">
          {fmtCount(table.data_file_count)} files ·{" "}
          {(table.small_file_ratio * 100).toFixed(0)}% small ·{" "}
          {table.snapshot_count} snapshots
        </span>
      </button>
      {open && (
        <div className="border-t border-border p-3">
          <TableHealthDetail
            warehouse={warehouse}
            namespace={table.namespace}
            name={table.name}
            ident={table.table_ident}
          />
        </div>
      )}
    </div>
  );
}

function TableHealthDetail({
  warehouse,
  namespace,
  name,
  ident,
}: {
  warehouse: string;
  namespace: string[];
  name: string;
  ident: string;
}) {
  const toast = useToast();
  const health = useAsync(
    () => api.tableHealth(warehouse, namespace, name),
    [warehouse, ident],
  );
  const [triggering, setTriggering] = useState<string | null>(null);

  async function trigger(jobType: "compaction" | "expire_snapshots") {
    setTriggering(jobType);
    try {
      const job = await api.triggerJob({
        warehouse,
        namespace: namespace.join("."),
        table: name,
        job_type: jobType,
      });
      toast.success(
        "Job enqueued",
        `${jobType} on ${ident} (${(job as { id: string }).id})`,
      );
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Trigger failed", e.message);
    } finally {
      setTriggering(null);
    }
  }

  return (
    <Async state={health} loadingLabel="Computing health…">
      {(h) => (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3">
            <ScoreBadge score={h.score} large />
            <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
              <span>{fmtBytes(h.metrics.total_bytes)}</span>
              <span>{fmtCount(h.metrics.data_file_count)} data files</span>
              <span>avg {fmtBytes(h.metrics.avg_file_bytes)}</span>
              <span>{h.metrics.snapshot_count} snapshots</span>
              {h.metrics.delete_file_count > 0 && (
                <span>{h.metrics.delete_file_count} delete files</span>
              )}
            </div>
            <div className="ml-auto flex gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={triggering !== null}
                onClick={() => trigger("compaction")}
              >
                <Play className="h-3.5 w-3.5" />
                {triggering === "compaction" ? "Enqueuing…" : "Compact"}
              </Button>
              <Button
                size="sm"
                variant="ghost"
                disabled={triggering !== null}
                onClick={() => trigger("expire_snapshots")}
              >
                {triggering === "expire_snapshots" ? "Enqueuing…" : "Expire snapshots"}
              </Button>
            </div>
          </div>

          <FileHistogram histogram={h.metrics.file_size_histogram} />

          {h.recommendations.length > 0 ? (
            <div>
              <p className="mb-1.5 text-xs uppercase tracking-wide text-muted-foreground">
                Recommendations
              </p>
              <ul className="space-y-1.5">
                {h.recommendations.map((r, i) => (
                  <li
                    key={i}
                    className="flex items-start gap-2 rounded-md bg-muted/30 px-3 py-2 text-sm"
                  >
                    <FileWarning className="mt-0.5 h-4 w-4 shrink-0 text-amber-400" />
                    <span>
                      <Badge variant="outline" className="mr-2">
                        {r.action}
                      </Badge>
                      <span className="text-muted-foreground">{r.reason}</span>
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">
              No recommendations — this table is healthy.
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            Computed {timeAgo(h.computed_at)} · snapshot{" "}
            {h.snapshot_id ?? "—"}
          </p>
        </div>
      )}
    </Async>
  );
}

// ---- File-size histogram ---------------------------------------------------

function FileHistogram({
  histogram,
}: {
  histogram: Record<string, number>;
}) {
  // Keys are prefixed with a sort index (e.g. "0:<1MiB"); strip it for display
  // but sort by it so buckets read smallest→largest.
  const entries = Object.entries(histogram).sort(([a], [b]) =>
    a.localeCompare(b),
  );
  const max = Math.max(1, ...entries.map(([, v]) => v));
  if (entries.length === 0) return null;
  return (
    <div>
      <p className="mb-1.5 text-xs uppercase tracking-wide text-muted-foreground">
        File-size distribution
      </p>
      <div className="space-y-1">
        {entries.map(([key, count]) => {
          const label = key.includes(":") ? key.split(":").slice(1).join(":") : key;
          const isSmall = key.startsWith("0") || key.startsWith("1");
          return (
            <div key={key} className="flex items-center gap-2 text-xs">
              <span className="w-24 shrink-0 text-right font-mono text-muted-foreground">
                {label}
              </span>
              <div className="h-4 flex-1 overflow-hidden rounded bg-muted/40">
                <div
                  className={`h-full ${isSmall ? "bg-amber-500/70" : "bg-emerald-500/60"}`}
                  style={{ width: `${(count / max) * 100}%` }}
                />
              </div>
              <span className="w-12 shrink-0 font-mono tabular-nums text-muted-foreground">
                {fmtCount(count)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---- Savings ledger --------------------------------------------------------

function SavingsSection() {
  const rollup = useAsync(() => api.savingsRollup(12), []);
  const ledger = useAsync(() => api.listSavings({ limit: 20 }), []);

  const totalBytes = (rollup.data?.rollup ?? []).reduce(
    (acc, p) => acc + p.bytes_saved,
    0,
  );
  const totalFiles = (rollup.data?.rollup ?? []).reduce(
    (acc, p) => acc + p.files_removed,
    0,
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <PiggyBank className="h-4 w-4 text-muted-foreground" /> Savings ledger
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <Async state={rollup} loadingLabel="Loading savings…">
          {(r) =>
            r.rollup.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No savings recorded yet. Once maintenance jobs commit, Meridian
                records exactly what each one saved here.
              </p>
            ) : (
              <>
                <div className="rounded-md border border-emerald-500/20 bg-emerald-500/5 p-4">
                  <p className="text-sm text-muted-foreground">
                    Meridian saved
                  </p>
                  <p className="text-2xl font-semibold text-emerald-400">
                    {fmtBytes(totalBytes)} · {fmtCount(totalFiles)} files
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    across the last {r.rollup.length} month
                    {r.rollup.length === 1 ? "" : "s"}, from verified before/after
                    commit metrics.
                  </p>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                        <th className="py-2 pr-4 font-medium">Month</th>
                        <th className="py-2 pr-4 font-medium">Jobs</th>
                        <th className="py-2 pr-4 font-medium">Bytes saved</th>
                        <th className="py-2 pr-4 font-medium">Files removed</th>
                        <th className="py-2 font-medium">GETs saved / scan</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {r.rollup.map((p) => (
                        <tr key={p.period}>
                          <td className="py-2 pr-4 font-mono text-xs">
                            {p.period}
                          </td>
                          <td className="py-2 pr-4">{fmtCount(p.job_count)}</td>
                          <td className="py-2 pr-4 font-mono tabular-nums">
                            {fmtBytes(p.bytes_saved)}
                          </td>
                          <td className="py-2 pr-4 font-mono tabular-nums">
                            {fmtCount(p.files_removed)}
                          </td>
                          <td className="py-2 font-mono tabular-nums text-muted-foreground">
                            {fmtCount(p.est_get_requests_saved)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )
          }
        </Async>

        {/* Recent per-job receipts */}
        {ledger.data && ledger.data.savings.length > 0 && (
          <div>
            <p className="mb-2 text-xs uppercase tracking-wide text-muted-foreground">
              Recent job receipts
            </p>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                    <th className="py-2 pr-4 font-medium">Table</th>
                    <th className="py-2 pr-4 font-medium">Files</th>
                    <th className="py-2 pr-4 font-medium">Saved</th>
                    <th className="py-2 font-medium">When</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {ledger.data.savings.map((s) => (
                    <tr key={s.id}>
                      <td className="py-2 pr-4 font-mono text-xs">
                        {s.table_ident}
                      </td>
                      <td className="py-2 pr-4 tabular-nums">
                        {fmtCount(s.files_before)} → {fmtCount(s.files_after)}
                      </td>
                      <td className="py-2 pr-4 font-mono tabular-nums">
                        {fmtBytes(s.bytes_saved)}
                      </td>
                      <td className="py-2 text-xs text-muted-foreground">
                        {timeAgo(s.created_at)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ---- Jobs list -------------------------------------------------------------

function JobsSection() {
  const toast = useToast();
  const jobs = useAsync(() => api.listJobs({ limit: 50 }), []);

  async function cancel(id: string) {
    try {
      await api.cancelJob(id);
      toast.success("Job cancelled", id);
      jobs.reload();
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Cancel failed", e.message);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ListChecks className="h-4 w-4 text-muted-foreground" /> Maintenance
          jobs
          <Button
            variant="ghost"
            size="sm"
            className="ml-auto"
            onClick={() => jobs.reload()}
          >
            <RefreshCw className="h-3.5 w-3.5" /> Refresh
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Async state={jobs} loadingLabel="Loading jobs…">
          {(data) =>
            data.jobs.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No maintenance jobs yet. Trigger one from a table above, or let
                the reconciler enqueue them from policy violations.
              </p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                      <th className="py-2 pr-4 font-medium">Type</th>
                      <th className="py-2 pr-4 font-medium">State</th>
                      <th className="py-2 pr-4 font-medium">Result</th>
                      <th className="py-2 pr-4 font-medium">By</th>
                      <th className="py-2 pr-4 font-medium">Created</th>
                      <th className="py-2 font-medium"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {data.jobs.map((j) => (
                      <tr key={j.id}>
                        <td className="py-2 pr-4">
                          <Badge variant="outline">{j.job_type}</Badge>
                        </td>
                        <td className="py-2 pr-4">
                          <JobStateBadge state={j.state} />
                        </td>
                        <td className="py-2 pr-4 text-xs text-muted-foreground">
                          <JobResult result={j.result} error={j.error} />
                        </td>
                        <td className="py-2 pr-4 font-mono text-xs text-muted-foreground">
                          {j.created_by}
                        </td>
                        <td className="py-2 pr-4 text-xs text-muted-foreground">
                          {fmtTime(j.created_at)}
                        </td>
                        <td className="py-2 text-right">
                          {(j.state === "queued" || j.state === "running") && (
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => cancel(j.id)}
                            >
                              Cancel
                            </Button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function JobResult({
  result,
  error,
}: {
  result: unknown | null;
  error: unknown | null;
}) {
  if (error && typeof error === "object" && "error" in error) {
    return (
      <span className="text-destructive">
        {String((error as { error: unknown }).error)}
      </span>
    );
  }
  if (result && typeof result === "object") {
    const r = result as {
      outcome?: string;
      files_before?: number;
      files_after?: number;
      bytes_saved?: number;
      reason?: string;
    };
    if (r.outcome === "committed") {
      return (
        <span>
          {fmtCount(r.files_before ?? 0)} → {fmtCount(r.files_after ?? 0)} files,{" "}
          {fmtBytes(r.bytes_saved ?? 0)} saved
        </span>
      );
    }
    if (r.outcome === "noop") {
      return <span>{r.reason ?? "no-op"}</span>;
    }
  }
  return <span>—</span>;
}

// ---- small shared UI -------------------------------------------------------

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "success" | "warning" | "danger";
}) {
  const toneClass =
    tone === "success"
      ? "text-emerald-400"
      : tone === "warning"
        ? "text-amber-400"
        : tone === "danger"
          ? "text-destructive"
          : "text-foreground";
  return (
    <div className="rounded-md border border-border bg-muted/20 p-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={`mt-0.5 text-lg font-semibold ${toneClass}`}>{value}</p>
    </div>
  );
}

function HealthBar({
  healthy,
  degraded,
  unhealthy,
}: {
  healthy: number;
  degraded: number;
  unhealthy: number;
}) {
  const total = Math.max(1, healthy + degraded + unhealthy);
  const pct = (n: number) => `${(n / total) * 100}%`;
  return (
    <div className="flex h-3 w-full overflow-hidden rounded-full bg-muted">
      <div className="bg-emerald-500" style={{ width: pct(healthy) }} />
      <div className="bg-amber-500" style={{ width: pct(degraded) }} />
      <div className="bg-destructive" style={{ width: pct(unhealthy) }} />
    </div>
  );
}

function LegendDot({ className, label }: { className: string; label: string }) {
  return (
    <span className="flex items-center gap-1.5">
      <span className={`h-2 w-2 rounded-full ${className}`} />
      {label}
    </span>
  );
}

function ScoreBadge({ score, large }: { score: number; large?: boolean }) {
  const tone = scoreTone(score);
  const variant =
    tone === "success" ? "success" : tone === "warning" ? "warning" : "danger";
  return (
    <Badge variant={variant} className={large ? "text-base" : ""}>
      {score}
    </Badge>
  );
}

function JobStateBadge({ state }: { state: string }) {
  const variant =
    state === "succeeded"
      ? "success"
      : state === "failed"
        ? "danger"
        : state === "running"
          ? "default"
          : state === "cancelled"
            ? "outline"
            : "secondary";
  return <Badge variant={variant}>{state}</Badge>;
}

function scoreTone(score: number): "success" | "warning" | "danger" {
  if (score >= 80) return "success";
  if (score >= 50) return "warning";
  return "danger";
}
