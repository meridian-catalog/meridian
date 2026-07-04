"use client";

import { useState } from "react";
import {
  ArrowRightLeft,
  BookText,
  Boxes,
  Gauge,
  Play,
  Plus,
  Trash2,
} from "lucide-react";
import { api } from "@/lib/api";
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
  CERTIFICATIONS,
  SQL_DIALECTS,
  type Certification,
  type CompileMetricResponse,
  type Metric,
  type TranspileResponse,
} from "@/lib/types";

// A transpile/compile status maps to a badge tone: verified is success,
// best_effort is a caveat (warning), unsupported is a hard no (danger). This is
// the honest status machine surfaced verbatim — never dressed up.
function statusVariant(
  status: string,
): "success" | "warning" | "danger" | "secondary" {
  if (status === "verified") return "success";
  if (status === "best_effort") return "warning";
  if (status === "unsupported") return "danger";
  return "secondary";
}

function certVariant(
  c: Certification,
): "success" | "secondary" | "warning" {
  if (c === "certified") return "success";
  if (c === "deprecated") return "warning";
  return "secondary";
}

export default function SemanticsPage() {
  return (
    <div>
      <PageHeader
        title="Semantics"
        description="Metrics, business glossary, and certified data products — meaning next to the data. Plus the universal-view transpiler that lets one view read in every engine's dialect."
      />
      <div className="space-y-4">
        <TranspilerCard />
        <MetricsCard />
        <GlossaryCard />
        <ProductsCard />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Universal-view transpiler (G-F1) — the demo magnet
// ---------------------------------------------------------------------------

function TranspilerCard() {
  const toast = useToast();
  const [sql, setSql] = useState("SELECT DATE_ADD(d, 7) FROM t");
  const [from, setFrom] = useState("spark");
  const [to, setTo] = useState("trino");
  const [result, setResult] = useState<TranspileResponse | null>(null);
  const [busy, setBusy] = useState(false);

  async function run() {
    setBusy(true);
    try {
      setResult(await api.transpile(sql, from, to));
    } catch (err) {
      toast.error("Transpile failed", (err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ArrowRightLeft className="h-4 w-4 text-muted-foreground" /> Universal
          view transpiler
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-muted-foreground">
          Deterministic SQLGlot translation via the sidecar. Every result is
          labelled <span className="font-mono">verified</span>,{" "}
          <span className="font-mono">best_effort</span>, or{" "}
          <span className="font-mono">unsupported</span> — validated by
          parse-back, never guessed.
        </p>
        <div>
          <Label htmlFor="transpile-sql">SQL</Label>
          <textarea
            id="transpile-sql"
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            rows={3}
            className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2 font-mono text-sm"
          />
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <Label htmlFor="from-dialect">From</Label>
            <Select
              id="from-dialect"
              value={from}
              onChange={(e) => setFrom(e.target.value)}
              className="mt-1"
            >
              {SQL_DIALECTS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </Select>
          </div>
          <div>
            <Label htmlFor="to-dialect">To</Label>
            <Select
              id="to-dialect"
              value={to}
              onChange={(e) => setTo(e.target.value)}
              className="mt-1"
            >
              {SQL_DIALECTS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </Select>
          </div>
          <Button onClick={run} disabled={busy || !sql.trim()}>
            <Play className="h-3.5 w-3.5" /> Translate
          </Button>
        </div>
        {result && (
          <div className="rounded-md border border-border bg-muted/40 p-3">
            <div className="mb-2 flex items-center gap-2">
              <Badge variant={statusVariant(result.status)}>
                {result.status}
              </Badge>
              <span className="text-xs text-muted-foreground">
                {result.from_dialect} → {result.to_dialect}
              </span>
            </div>
            {result.sql ? (
              <pre className="overflow-x-auto whitespace-pre-wrap font-mono text-sm text-foreground">
                {result.sql}
              </pre>
            ) : (
              <p className="text-sm text-muted-foreground">
                No translation available for this construct.
              </p>
            )}
            {result.diagnostics.length > 0 && (
              <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
                {result.diagnostics.map((d, i) => (
                  <li key={i}>
                    <span className="font-mono">[{d.severity}]</span> {d.code}:{" "}
                    {d.message}
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Metrics (G-F2)
// ---------------------------------------------------------------------------

function MetricsCard() {
  const toast = useToast();
  const metrics = useAsync(() => api.listMetrics(), []);
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState("");
  const [source, setSource] = useState("");
  const [expression, setExpression] = useState("");
  const [dialect, setDialect] = useState("trino");
  const [certification, setCertification] = useState<Certification>("draft");

  async function create() {
    try {
      await api.createMetric({
        name,
        source,
        expression,
        dialect,
        certification,
      });
      toast.success("Metric created", name);
      setName("");
      setSource("");
      setExpression("");
      setShowForm(false);
      metrics.reload();
    } catch (err) {
      toast.error("Create failed", (err as Error).message);
    }
  }

  async function remove(id: string, mname: string) {
    try {
      await api.deleteMetric(id);
      toast.success("Metric deleted", mname);
      metrics.reload();
    } catch (err) {
      toast.error("Delete failed", (err as Error).message);
    }
  }

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Gauge className="h-4 w-4 text-muted-foreground" /> Metrics
        </CardTitle>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowForm((s) => !s)}
        >
          <Plus className="h-3.5 w-3.5" /> New metric
        </Button>
      </CardHeader>
      <CardContent>
        {showForm && (
          <div className="mb-4 space-y-3 rounded-md border border-border p-3">
            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <Label htmlFor="m-name">Name</Label>
                <Input
                  id="m-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="revenue"
                  className="mt-1"
                />
              </div>
              <div>
                <Label htmlFor="m-source">Source</Label>
                <Input
                  id="m-source"
                  value={source}
                  onChange={(e) => setSource(e.target.value)}
                  placeholder="analytics.sales"
                  className="mt-1"
                />
              </div>
            </div>
            <div>
              <Label htmlFor="m-expr">Measure expression</Label>
              <Input
                id="m-expr"
                value={expression}
                onChange={(e) => setExpression(e.target.value)}
                placeholder="SUM(amount)"
                className="mt-1 font-mono"
              />
            </div>
            <div className="flex flex-wrap items-end gap-3">
              <div>
                <Label htmlFor="m-dialect">Dialect</Label>
                <Select
                  id="m-dialect"
                  value={dialect}
                  onChange={(e) => setDialect(e.target.value)}
                  className="mt-1"
                >
                  {SQL_DIALECTS.map((d) => (
                    <option key={d} value={d}>
                      {d}
                    </option>
                  ))}
                </Select>
              </div>
              <div>
                <Label htmlFor="m-cert">Certification</Label>
                <Select
                  id="m-cert"
                  value={certification}
                  onChange={(e) =>
                    setCertification(e.target.value as Certification)
                  }
                  className="mt-1"
                >
                  {CERTIFICATIONS.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </Select>
              </div>
              <Button
                onClick={create}
                disabled={!name.trim() || !source.trim() || !expression.trim()}
              >
                Create
              </Button>
            </div>
          </div>
        )}
        <Async state={metrics} loadingLabel="Loading metrics…">
          {(data) =>
            data.metrics.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No metrics yet. A metric compiles deterministically to any
                engine&apos;s SQL.
              </p>
            ) : (
              <div className="space-y-2">
                {data.metrics.map((m) => (
                  <MetricRow key={m.id} metric={m} onDelete={remove} />
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function MetricRow({
  metric,
  onDelete,
}: {
  metric: Metric;
  onDelete: (id: string, name: string) => void;
}) {
  const toast = useToast();
  const [engine, setEngine] = useState("trino");
  const [compiled, setCompiled] = useState<CompileMetricResponse | null>(null);

  async function compile() {
    try {
      setCompiled(await api.compileMetric(metric.id, engine));
    } catch (err) {
      toast.error("Compile failed", (err as Error).message);
    }
  }

  return (
    <div className="rounded-md border border-border p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="font-medium">{metric.name}</span>
          <Badge variant={certVariant(metric.certification)}>
            {metric.certification}
          </Badge>
          <span className="font-mono text-xs text-muted-foreground">
            {metric.expression}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <Select
            value={engine}
            onChange={(e) => setEngine(e.target.value)}
            className="h-8"
            aria-label="Compile target engine"
          >
            {SQL_DIALECTS.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </Select>
          <Button size="sm" variant="outline" onClick={compile}>
            <Play className="h-3.5 w-3.5" /> Compile
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onDelete(metric.id, metric.name)}
            aria-label={`Delete ${metric.name}`}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
      <div className="mt-1 text-xs text-muted-foreground">
        source <span className="font-mono">{metric.source}</span>
        {metric.grain ? ` · grain: ${metric.grain}` : ""}
      </div>
      {compiled && (
        <div className="mt-2 rounded-md border border-border bg-muted/40 p-2">
          <div className="mb-1 flex items-center gap-2">
            <Badge variant={statusVariant(compiled.status)}>
              {compiled.status}
            </Badge>
            <span className="text-xs text-muted-foreground">
              {compiled.engine}
            </span>
          </div>
          {compiled.sql ? (
            <pre className="overflow-x-auto whitespace-pre-wrap font-mono text-xs text-foreground">
              {compiled.sql}
            </pre>
          ) : (
            <p className="text-xs text-muted-foreground">
              Not compilable for this engine.
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Glossary (G-F3)
// ---------------------------------------------------------------------------

function GlossaryCard() {
  const toast = useToast();
  const terms = useAsync(() => api.listTerms(), []);
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState("");
  const [definition, setDefinition] = useState("");
  const [certification, setCertification] = useState<Certification>("draft");

  async function create() {
    try {
      await api.createTerm({ name, definition, certification });
      toast.success("Term created", name);
      setName("");
      setDefinition("");
      setShowForm(false);
      terms.reload();
    } catch (err) {
      toast.error("Create failed", (err as Error).message);
    }
  }

  async function remove(id: string, tname: string) {
    try {
      await api.deleteTerm(id);
      toast.success("Term deleted", tname);
      terms.reload();
    } catch (err) {
      toast.error("Delete failed", (err as Error).message);
    }
  }

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <BookText className="h-4 w-4 text-muted-foreground" /> Business
          glossary
        </CardTitle>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowForm((s) => !s)}
        >
          <Plus className="h-3.5 w-3.5" /> New term
        </Button>
      </CardHeader>
      <CardContent>
        {showForm && (
          <div className="mb-4 space-y-3 rounded-md border border-border p-3">
            <div>
              <Label htmlFor="t-name">Term</Label>
              <Input
                id="t-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Net Revenue"
                className="mt-1"
              />
            </div>
            <div>
              <Label htmlFor="t-def">Definition</Label>
              <textarea
                id="t-def"
                value={definition}
                onChange={(e) => setDefinition(e.target.value)}
                rows={2}
                placeholder="Recognized revenue, net of refunds."
                className="mt-1 w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
              />
            </div>
            <div className="flex items-end gap-3">
              <div>
                <Label htmlFor="t-cert">Certification</Label>
                <Select
                  id="t-cert"
                  value={certification}
                  onChange={(e) =>
                    setCertification(e.target.value as Certification)
                  }
                  className="mt-1"
                >
                  {CERTIFICATIONS.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </Select>
              </div>
              <Button
                onClick={create}
                disabled={!name.trim() || !definition.trim()}
              >
                Create
              </Button>
            </div>
          </div>
        )}
        <Async state={terms} loadingLabel="Loading glossary…">
          {(data) =>
            data.terms.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No terms yet. Define business vocabulary once; link it to the
                assets that mean it.
              </p>
            ) : (
              <div className="space-y-2">
                {data.terms.map((t) => (
                  <div
                    key={t.id}
                    className="flex items-start justify-between gap-2 rounded-md border border-border p-3"
                  >
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{t.name}</span>
                        <Badge variant={certVariant(t.certification)}>
                          {t.certification}
                        </Badge>
                      </div>
                      <p className="mt-1 text-sm text-muted-foreground">
                        {t.definition}
                      </p>
                    </div>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => remove(t.id, t.name)}
                      aria-label={`Delete ${t.name}`}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Data products (G-F4)
// ---------------------------------------------------------------------------

function ProductsCard() {
  const toast = useToast();
  const products = useAsync(() => api.listProducts(), []);
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [sla, setSla] = useState("");
  const [certification, setCertification] = useState<Certification>("draft");

  async function create() {
    try {
      await api.createProduct({ name, description, sla, certification });
      toast.success("Product created", name);
      setName("");
      setDescription("");
      setSla("");
      setShowForm(false);
      products.reload();
    } catch (err) {
      toast.error("Create failed", (err as Error).message);
    }
  }

  async function remove(id: string, pname: string) {
    try {
      await api.deleteProduct(id);
      toast.success("Product deleted", pname);
      products.reload();
    } catch (err) {
      toast.error("Delete failed", (err as Error).message);
    }
  }

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Boxes className="h-4 w-4 text-muted-foreground" /> Data products
        </CardTitle>
        <Button
          size="sm"
          variant="outline"
          onClick={() => setShowForm((s) => !s)}
        >
          <Plus className="h-3.5 w-3.5" /> New product
        </Button>
      </CardHeader>
      <CardContent>
        {showForm && (
          <div className="mb-4 space-y-3 rounded-md border border-border p-3">
            <div className="grid gap-3 sm:grid-cols-2">
              <div>
                <Label htmlFor="p-name">Name</Label>
                <Input
                  id="p-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="sales_360"
                  className="mt-1"
                />
              </div>
              <div>
                <Label htmlFor="p-sla">SLA</Label>
                <Input
                  id="p-sla"
                  value={sla}
                  onChange={(e) => setSla(e.target.value)}
                  placeholder="99.9% freshness within 1h"
                  className="mt-1"
                />
              </div>
            </div>
            <div>
              <Label htmlFor="p-desc">Description</Label>
              <Input
                id="p-desc"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="The certified sales view."
                className="mt-1"
              />
            </div>
            <div className="flex items-end gap-3">
              <div>
                <Label htmlFor="p-cert">Certification</Label>
                <Select
                  id="p-cert"
                  value={certification}
                  onChange={(e) =>
                    setCertification(e.target.value as Certification)
                  }
                  className="mt-1"
                >
                  {CERTIFICATIONS.map((c) => (
                    <option key={c} value={c}>
                      {c}
                    </option>
                  ))}
                </Select>
              </div>
              <Button onClick={create} disabled={!name.trim()}>
                Create
              </Button>
            </div>
          </div>
        )}
        <Async state={products} loadingLabel="Loading products…">
          {(data) =>
            data.products.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No data products yet. A product bundles tables, views, metrics,
                and contracts into one certified unit of consumption.
              </p>
            ) : (
              <div className="space-y-2">
                {data.products.map((p) => (
                  <div
                    key={p.id}
                    className="flex items-start justify-between gap-2 rounded-md border border-border p-3"
                  >
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{p.name}</span>
                        <Badge variant={certVariant(p.certification)}>
                          {p.certification}
                        </Badge>
                      </div>
                      {p.description && (
                        <p className="mt-1 text-sm text-muted-foreground">
                          {p.description}
                        </p>
                      )}
                      {p.sla && (
                        <p className="mt-0.5 text-xs text-muted-foreground">
                          SLA: {p.sla}
                        </p>
                      )}
                    </div>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => remove(p.id, p.name)}
                      aria-label={`Delete ${p.name}`}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}
