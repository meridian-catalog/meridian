"use client";

import { useState, useCallback, Fragment } from "react";
import Link from "next/link";
import { Search as SearchIcon, Table2, Eye, Folder } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { encodeNsParam, nsPath } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { useToast } from "@/components/toast";
import {
  Badge,
  Button,
  Card,
  CardContent,
  Input,
} from "@/components/ui/primitives";
import type { SearchResult } from "@/lib/types";

export default function SearchPage() {
  const toast = useToast();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<SearchResult[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [lastRun, setLastRun] = useState("");

  const run = useCallback(
    async (q: string) => {
      const trimmed = q.trim();
      if (!trimmed) return;
      setLoading(true);
      setError(null);
      setLastRun(trimmed);
      try {
        const res = await api.search({ q: trimmed, limit: 50 });
        setResults(res.results);
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        setError(e);
        toast.error("Search failed", e.message);
      } finally {
        setLoading(false);
      }
    },
    [toast],
  );

  return (
    <div>
      <PageHeader
        title="Search"
        description="Full-text search across tables, views, and namespaces."
      />

      <form
        onSubmit={(e) => {
          e.preventDefault();
          run(query);
        }}
        className="mb-6 flex gap-2"
      >
        <div className="relative flex-1">
          <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search assets by name, path, or column…"
            className="pl-9"
            autoFocus
          />
        </div>
        <Button type="submit" disabled={loading || !query.trim()}>
          Search
        </Button>
      </form>

      {loading ? (
        <LoadingState label="Searching…" />
      ) : error ? (
        <ErrorState error={error} onRetry={() => run(lastRun)} />
      ) : results === null ? (
        <EmptyState
          title="Search the catalog"
          detail="Type a query above to find tables, views, and namespaces."
        />
      ) : results.length === 0 ? (
        <EmptyState
          title="No matches"
          detail={`Nothing matched “${lastRun}”.`}
        />
      ) : (
        <Card>
          <CardContent className="p-0">
            <ul className="divide-y divide-border">
              {results.map((r) => (
                <ResultRow key={`${r.type}-${r.id}`} result={r} />
              ))}
            </ul>
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function resultHref(r: SearchResult): string {
  const wh = encodeURIComponent(r.warehouse);
  if (r.type === "namespace") {
    return "/catalog";
  }
  const ns = encodeNsParam(r.namespace);
  const kind = r.type === "view" ? "view" : "table";
  return `/catalog/${wh}/${ns}/${kind}/${encodeURIComponent(r.name)}`;
}

function typeMeta(type: SearchResult["type"]) {
  switch (type) {
    case "table":
      return {
        icon: <Table2 className="h-4 w-4 text-sky-400/80" />,
        variant: "default" as const,
      };
    case "view":
      return {
        icon: <Eye className="h-4 w-4 text-violet-400/80" />,
        variant: "secondary" as const,
      };
    default:
      return {
        icon: <Folder className="h-4 w-4 text-amber-400/80" />,
        variant: "outline" as const,
      };
  }
}

function ResultRow({ result }: { result: SearchResult }) {
  const meta = typeMeta(result.type);
  const path =
    result.type === "namespace"
      ? `${result.warehouse} · ${nsPath(result.namespace)}`
      : `${result.warehouse} · ${nsPath(result.namespace)}`;

  const content = (
    <div className="flex items-start gap-3 px-4 py-3 transition-colors hover:bg-accent/40">
      <div className="mt-0.5 shrink-0">{meta.icon}</div>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-medium">{result.name}</span>
          <Badge variant={meta.variant}>{result.type}</Badge>
        </div>
        <p className="mt-0.5 truncate font-mono text-xs text-muted-foreground">
          {path}
        </p>
        {result.snippet && (
          <p className="mt-1 text-sm text-muted-foreground">
            <Highlight text={result.snippet} />
          </p>
        )}
      </div>
    </div>
  );

  if (result.type === "namespace") {
    // Namespaces have no dedicated detail page; link to the catalog browser.
    return (
      <li>
        <Link href={resultHref(result)}>{content}</Link>
      </li>
    );
  }
  return (
    <li>
      <Link href={resultHref(result)}>{content}</Link>
    </li>
  );
}

/**
 * Renders the server's `ts_headline` snippet. Matches are wrapped in `**…**`;
 * we split on that delimiter and bold the odd segments. Rendering as React text
 * nodes (never dangerouslySetInnerHTML) keeps it XSS-safe.
 */
function Highlight({ text }: { text: string }) {
  const parts = text.split("**");
  return (
    <>
      {parts.map((part, i) =>
        i % 2 === 1 ? (
          <mark
            key={i}
            className="rounded bg-primary/20 px-0.5 text-foreground"
          >
            {part}
          </mark>
        ) : (
          <Fragment key={i}>{part}</Fragment>
        ),
      )}
    </>
  );
}
