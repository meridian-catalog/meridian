"use client";

import { useState } from "react";
import Link from "next/link";
import {
  ChevronRight,
  ChevronDown,
  Database,
  Folder,
  Table2,
  Eye,
} from "lucide-react";
import { api } from "@/lib/api";
import { nsPath, encodeNsParam } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync, ErrorState, LoadingState } from "@/components/states";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/primitives";

export default function CatalogPage() {
  const warehouses = useAsync(() => api.listWarehouses(), []);
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div>
      <PageHeader
        title="Catalog"
        description="Warehouses, namespaces, tables, and views."
      />
      <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">
              Warehouses
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <Async state={warehouses}>
              {(w) =>
                w.warehouses.length === 0 ? (
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
                )
              }
            </Async>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm text-muted-foreground">
              {selected ? (
                <span className="font-mono text-foreground">{selected}</span>
              ) : (
                "Select a warehouse"
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            {selected ? (
              <NamespaceTree warehouse={selected} />
            ) : (
              <p className="py-8 text-center text-sm text-muted-foreground">
                Pick a warehouse on the left to explore its namespaces.
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function NamespaceTree({ warehouse }: { warehouse: string }) {
  const roots = useAsync(
    () => api.listNamespaces(warehouse),
    [warehouse],
  );
  return (
    <Async state={roots} loadingLabel="Loading namespaces…">
      {(data) =>
        data.namespaces.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            No namespaces in this warehouse.
          </p>
        ) : (
          <ul className="space-y-0.5">
            {data.namespaces.map((levels) => (
              <NamespaceNode
                key={levels.join("")}
                warehouse={warehouse}
                levels={levels}
                depth={0}
              />
            ))}
          </ul>
        )
      }
    </Async>
  );
}

function NamespaceNode({
  warehouse,
  levels,
  depth,
}: {
  warehouse: string;
  levels: string[];
  depth: number;
}) {
  const [open, setOpen] = useState(depth === 0);
  const label = levels[levels.length - 1];
  return (
    <li>
      <button
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center gap-1.5 rounded-md px-2 py-1.5 text-left text-sm hover:bg-accent/50"
        style={{ paddingLeft: `${depth * 16 + 8}px` }}
      >
        {open ? (
          <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" />
        ) : (
          <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />
        )}
        <Folder className="h-4 w-4 text-amber-400/80" />
        <span className="font-medium">{label}</span>
        <span className="ml-1 truncate text-xs text-muted-foreground">
          {nsPath(levels)}
        </span>
      </button>
      {open && (
        <NamespaceChildren
          warehouse={warehouse}
          levels={levels}
          depth={depth + 1}
        />
      )}
    </li>
  );
}

function NamespaceChildren({
  warehouse,
  levels,
  depth,
}: {
  warehouse: string;
  levels: string[];
  depth: number;
}) {
  const children = useAsync(
    () => api.listNamespaces(warehouse, levels),
    [warehouse, levels.join("")],
  );
  const tables = useAsync(
    () => api.listTables(warehouse, levels),
    [warehouse, levels.join("")],
  );
  const views = useAsync(
    () => api.listViews(warehouse, levels),
    [warehouse, levels.join("")],
  );

  if (children.loading || tables.loading || views.loading) {
    return <LoadingState label="Loading…" />;
  }
  const err = children.error || tables.error || views.error;
  if (err) {
    return (
      <div style={{ paddingLeft: `${depth * 16 + 8}px` }}>
        <ErrorState
          error={err}
          onRetry={() => {
            children.reload();
            tables.reload();
            views.reload();
          }}
        />
      </div>
    );
  }

  const childNs = children.data?.namespaces ?? [];
  const tbls = tables.data?.identifiers ?? [];
  const vws = views.data?.identifiers ?? [];
  const empty = childNs.length === 0 && tbls.length === 0 && vws.length === 0;

  return (
    <ul className="space-y-0.5">
      {childNs.map((cl) => (
        <NamespaceNode
          key={cl.join("")}
          warehouse={warehouse}
          levels={cl}
          depth={depth}
        />
      ))}
      {tbls.map((t) => (
        <li key={`t-${t.name}`}>
          <Link
            href={`/catalog/${encodeURIComponent(warehouse)}/${encodeNsParam(levels)}/table/${encodeURIComponent(t.name)}`}
            className="flex items-center gap-1.5 rounded-md px-2 py-1.5 text-sm hover:bg-accent/50"
            style={{ paddingLeft: `${depth * 16 + 26}px` }}
          >
            <Table2 className="h-4 w-4 text-sky-400/80" />
            <span>{t.name}</span>
          </Link>
        </li>
      ))}
      {vws.map((v) => (
        <li key={`v-${v.name}`}>
          <Link
            href={`/catalog/${encodeURIComponent(warehouse)}/${encodeNsParam(levels)}/view/${encodeURIComponent(v.name)}`}
            className="flex items-center gap-1.5 rounded-md px-2 py-1.5 text-sm hover:bg-accent/50"
            style={{ paddingLeft: `${depth * 16 + 26}px` }}
          >
            <Eye className="h-4 w-4 text-violet-400/80" />
            <span>{v.name}</span>
          </Link>
        </li>
      ))}
      {empty && (
        <li
          className="px-2 py-1.5 text-xs text-muted-foreground"
          style={{ paddingLeft: `${depth * 16 + 26}px` }}
        >
          empty
        </li>
      )}
    </ul>
  );
}
