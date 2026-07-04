"use client";

// The Workbench page (Pillar L, L-F1/L-F3; spec §8.6 console IA).
//
// A governed SQL editor over the catalog's assets: write a SELECT, pick the
// warehouse (and an optional default namespace for bare table names), run it on
// the built-in small-scan executor, and see the rows — governed by the same
// Pillar-D row/column policies the agent gateway and scan planner enforce, so
// the result shows only what the caller's grants permit (masked values, filtered
// rows). Every result carries provenance (the tables + snapshots read and the
// policies applied). Query history and saved queries live alongside; the
// notebook-handoff snippet generator (L-F3) opens a table in PyIceberg/Daft/
// Pandas with scoped, vended credentials.
//
// Everything here is real data from the /api/v2/workbench surface.

import { useState } from "react";
import {
  Play,
  Save,
  Trash2,
  Clock,
  Bookmark,
  Table2,
  ShieldCheck,
  Code2,
  AlertTriangle,
} from "lucide-react";
import { api } from "@/lib/api";
import { ApiError } from "@/lib/api";
import { fmtBytes, fmtCount, timeAgo } from "@/lib/utils";
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
} from "@/components/ui/primitives";
import type {
  SavedQuery,
  SnippetResponse,
  WorkbenchQueryResponse,
} from "@/lib/types";

export default function WorkbenchPage() {
  const toast = useToast();
  const [sql, setSql] = useState("SELECT 1 AS example");
  const [warehouse, setWarehouse] = useState("");
  const [namespace, setNamespace] = useState("");
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<WorkbenchQueryResponse | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);

  const warehouses = useAsync(() => api.listWarehouses(), []);
  const saved = useAsync(() => api.listSavedQueries(), []);
  const history = useAsync(() => api.workbenchHistory(30), []);

  async function run() {
    if (!sql.trim()) return;
    setRunning(true);
    setQueryError(null);
    try {
      const res = await api.runWorkbenchQuery({
        sql,
        warehouse: warehouse || undefined,
        namespace: namespace || undefined,
      });
      setResult(res);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setQueryError(msg);
      setResult(null);
    } finally {
      setRunning(false);
      history.reload();
    }
  }

  async function saveCurrent() {
    const name = window.prompt("Save this query as:");
    if (!name) return;
    try {
      await api.saveQuery({
        name,
        sql,
        warehouse: warehouse || undefined,
        namespace: namespace || undefined,
      });
      toast.success("Query saved", name);
      saved.reload();
    } catch (e) {
      toast.error(
        "Save failed",
        e instanceof ApiError ? e.message : String(e),
      );
    }
  }

  function loadQuery(q: { sql: string; warehouse: string | null; namespace?: string[] }) {
    setSql(q.sql);
    setWarehouse(q.warehouse ?? "");
    setNamespace(q.namespace && q.namespace.length ? q.namespace.join(".") : "");
  }

  return (
    <div>
      <PageHeader
        title="Workbench"
        description="Run governed SQL over your catalog on the built-in executor — row/column policies enforced, provenance on every result. Small scans only; large queries route to a registered engine."
        actions={
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={saveCurrent} disabled={running}>
              <Save className="mr-1.5 h-4 w-4" />
              Save
            </Button>
            <Button onClick={run} disabled={running || !sql.trim()}>
              <Play className="mr-1.5 h-4 w-4" />
              {running ? "Running…" : "Run"}
            </Button>
          </div>
        }
      />

      <div className="grid gap-6 lg:grid-cols-[1fr_320px]">
        {/* Editor + results */}
        <div className="flex flex-col gap-4">
          <Card>
            <CardContent className="pt-5">
              <div className="mb-3 grid gap-3 sm:grid-cols-2">
                <div>
                  <Label htmlFor="wh">Warehouse</Label>
                  <select
                    id="wh"
                    className="mt-1 flex h-9 w-full rounded-md border border-border bg-background px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    value={warehouse}
                    onChange={(e) => setWarehouse(e.target.value)}
                  >
                    <option value="">(none — table-free query)</option>
                    {warehouses.data?.warehouses?.map((w) => (
                      <option key={w.name} value={w.name}>
                        {w.name}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <Label htmlFor="ns">Default namespace (optional)</Label>
                  <Input
                    id="ns"
                    className="mt-1"
                    placeholder="e.g. sales.eu — for bare table names"
                    value={namespace}
                    onChange={(e) => setNamespace(e.target.value)}
                  />
                </div>
              </div>
              <textarea
                className="min-h-[180px] w-full rounded-md border border-border bg-background px-3 py-2 font-mono text-sm shadow-sm placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                spellCheck={false}
                value={sql}
                onChange={(e) => setSql(e.target.value)}
                onKeyDown={(e) => {
                  // Cmd/Ctrl+Enter runs the query.
                  if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
                    e.preventDefault();
                    void run();
                  }
                }}
                placeholder="SELECT * FROM namespace.table WHERE …"
              />
              <p className="mt-2 text-xs text-muted-foreground">
                Governed: only rows and columns your grants permit are returned.
                Press ⌘/Ctrl+Enter to run.
              </p>
            </CardContent>
          </Card>

          {queryError && (
            <Card className="border-destructive/40">
              <CardContent className="flex items-start gap-2 pt-5 text-sm">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-destructive" />
                <span className="text-destructive">{queryError}</span>
              </CardContent>
            </Card>
          )}

          {result && <ResultView result={result} />}
        </div>

        {/* Sidebar: saved queries + history */}
        <div className="flex flex-col gap-4">
          <SnippetCard warehouse={warehouse} />

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm">
                <Bookmark className="h-4 w-4" />
                Saved queries
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Async state={saved}>
                {(data) =>
                  data.saved_queries.length === 0 ? (
                    <p className="text-sm text-muted-foreground">
                      No saved queries yet.
                    </p>
                  ) : (
                    <ul className="flex flex-col gap-1">
                      {data.saved_queries.map((q) => (
                        <SavedRow
                          key={q.id}
                          q={q}
                          onLoad={() => loadQuery(q)}
                          onDelete={async () => {
                            try {
                              await api.deleteSavedQuery(q.id);
                              toast.success("Deleted", q.name);
                              saved.reload();
                            } catch (e) {
                              toast.error(
                                "Delete failed",
                                e instanceof ApiError ? e.message : String(e),
                              );
                            }
                          }}
                        />
                      ))}
                    </ul>
                  )
                }
              </Async>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm">
                <Clock className="h-4 w-4" />
                History
              </CardTitle>
            </CardHeader>
            <CardContent>
              <Async state={history}>
                {(data) =>
                  data.history.length === 0 ? (
                    <p className="text-sm text-muted-foreground">
                      No queries run yet.
                    </p>
                  ) : (
                    <ul className="flex flex-col gap-1">
                      {data.history.map((h) => (
                        <li key={h.id}>
                          <button
                            className="w-full rounded-md px-2 py-1.5 text-left text-xs hover:bg-muted"
                            onClick={() => {
                              setSql(h.sql);
                              setWarehouse(h.warehouse ?? "");
                            }}
                            title={h.sql}
                          >
                            <div className="flex items-center justify-between gap-2">
                              <span className="truncate font-mono">
                                {h.sql}
                              </span>
                              <HistoryBadge status={h.status} />
                            </div>
                            <div className="mt-0.5 text-[11px] text-muted-foreground">
                              {timeAgo(h.created_at)}
                              {h.status === "ok" &&
                                h.row_count != null &&
                                ` · ${fmtCount(h.row_count)} rows`}
                            </div>
                          </button>
                        </li>
                      ))}
                    </ul>
                  )
                }
              </Async>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

function ResultView({ result }: { result: WorkbenchQueryResponse }) {
  const { columns, rows, provenance } = result;
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2 text-sm">
          <Table2 className="h-4 w-4" />
          {fmtCount(result.row_count)} row{result.row_count === 1 ? "" : "s"}
          {result.truncated && (
            <Badge variant="warning" className="ml-1">
              truncated
            </Badge>
          )}
        </CardTitle>
        <span className="text-xs text-muted-foreground">
          {result.duration_ms} ms · {fmtBytes(result.bytes_scanned)} scanned
        </span>
      </CardHeader>
      <CardContent>
        {/* Provenance: what was read + what policy applied (H-F3/D-F2). */}
        <div className="mb-3 flex flex-wrap items-center gap-2 text-xs">
          <ShieldCheck className="h-3.5 w-3.5 text-muted-foreground" />
          {provenance.tables.length === 0 ? (
            <span className="text-muted-foreground">no tables read</span>
          ) : (
            provenance.tables.map((t) => (
              <Badge key={t.name} variant="secondary">
                {t.name}
                {t.snapshot_id != null && ` @${t.snapshot_id}`}
              </Badge>
            ))
          )}
          {provenance.masked_columns.length > 0 && (
            <span className="text-muted-foreground">
              · masked: {provenance.masked_columns.join(", ")}
            </span>
          )}
          {(provenance.row_filter_policies.length > 0 ||
            provenance.column_mask_policies.length > 0) && (
            <span className="text-muted-foreground">· policies applied</span>
          )}
        </div>

        {columns.length === 0 ? (
          <p className="text-sm text-muted-foreground">No columns.</p>
        ) : (
          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/40">
                  {columns.map((c) => (
                    <th
                      key={c.name}
                      className="whitespace-nowrap px-3 py-2 text-left font-medium"
                    >
                      {c.name}
                      <span className="ml-1 font-normal text-muted-foreground">
                        {c.data_type}
                      </span>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => (
                  <tr key={i} className="border-b border-border last:border-0">
                    {columns.map((c) => (
                      <td
                        key={c.name}
                        className="whitespace-nowrap px-3 py-1.5 font-mono text-xs"
                      >
                        {renderCell(row[c.name])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function renderCell(value: unknown): string {
  if (value === null || value === undefined) return "∅";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function SavedRow({
  q,
  onLoad,
  onDelete,
}: {
  q: SavedQuery;
  onLoad: () => void;
  onDelete: () => void;
}) {
  return (
    <li className="group flex items-center gap-1">
      <button
        className="flex-1 truncate rounded-md px-2 py-1.5 text-left text-sm hover:bg-muted"
        onClick={onLoad}
        title={q.sql}
      >
        {q.name}
      </button>
      <button
        className="rounded-md p-1.5 text-muted-foreground opacity-0 hover:bg-muted hover:text-destructive group-hover:opacity-100"
        onClick={onDelete}
        aria-label={`Delete ${q.name}`}
      >
        <Trash2 className="h-3.5 w-3.5" />
      </button>
    </li>
  );
}

function HistoryBadge({ status }: { status: "ok" | "error" | "denied" }) {
  const variant =
    status === "ok" ? "success" : status === "denied" ? "warning" : "danger";
  return (
    <Badge variant={variant} className="shrink-0 text-[10px]">
      {status}
    </Badge>
  );
}

// The notebook-handoff snippet generator (L-F3): pick a table, get connection
// snippets for PyIceberg/Daft/Pandas that vend scoped creds at connect time.
function SnippetCard({ warehouse }: { warehouse: string }) {
  const toast = useToast();
  const [namespace, setNamespace] = useState("");
  const [table, setTable] = useState("");
  const [snippet, setSnippet] = useState<SnippetResponse | null>(null);
  const [tab, setTab] = useState<"pyiceberg" | "daft" | "pandas">("pyiceberg");

  async function generate() {
    if (!warehouse || !namespace || !table) {
      toast.error("Fill in warehouse, namespace, and table first");
      return;
    }
    try {
      const res = await api.workbenchSnippet({ warehouse, namespace, table });
      setSnippet(res);
    } catch (e) {
      toast.error(
        "Could not generate snippet",
        e instanceof ApiError ? e.message : String(e),
      );
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm">
          <Code2 className="h-4 w-4" />
          Open in a notebook
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid gap-2">
          <Input
            placeholder="namespace (e.g. sales.eu)"
            value={namespace}
            onChange={(e) => setNamespace(e.target.value)}
          />
          <Input
            placeholder="table"
            value={table}
            onChange={(e) => setTable(e.target.value)}
          />
          <Button variant="outline" onClick={generate} disabled={!warehouse}>
            Generate snippet
          </Button>
        </div>
        {snippet && (
          <div className="mt-3">
            <div className="mb-2 flex gap-1">
              {(["pyiceberg", "daft", "pandas"] as const).map((t) => (
                <button
                  key={t}
                  className={`rounded-md px-2 py-1 text-xs ${
                    tab === t
                      ? "bg-foreground text-background"
                      : "bg-muted text-muted-foreground hover:text-foreground"
                  }`}
                  onClick={() => setTab(t)}
                >
                  {t}
                </button>
              ))}
            </div>
            <pre className="max-h-56 overflow-auto rounded-md border border-border bg-muted/40 p-2 text-[11px] leading-relaxed">
              {snippet.snippets[tab]}
            </pre>
            <p className="mt-1 text-[11px] text-muted-foreground">
              {snippet.note}
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
