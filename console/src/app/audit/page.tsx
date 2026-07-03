"use client";

import { useState, useCallback, useEffect } from "react";
import {
  ShieldCheck,
  ShieldAlert,
  ChevronRight,
  ChevronDown,
} from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { fmtTime } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { useToast } from "@/components/toast";
import {
  Badge,
  Button,
  Card,
  CardContent,
  Input,
  Label,
} from "@/components/ui/primitives";
import type {
  AuditEntry,
  AuditQueryParams,
  VerifyChainResponse,
} from "@/lib/types";

const PAGE_SIZE = 50;

export default function AuditPage() {
  const toast = useToast();
  const [filters, setFilters] = useState<AuditQueryParams>({});
  const [entries, setEntries] = useState<AuditEntry[] | null>(null);
  const [cursor, setCursor] = useState<number | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [verify, setVerify] = useState<VerifyChainResponse | null>(null);
  const [verifying, setVerifying] = useState(false);

  const load = useCallback(
    async (params: AuditQueryParams, append: boolean) => {
      if (append) setLoadingMore(true);
      else {
        setLoading(true);
        setError(null);
      }
      try {
        const res = await api.audit({ ...params, limit: PAGE_SIZE });
        setEntries((prev) =>
          append && prev ? [...prev, ...res.entries] : res.entries,
        );
        setCursor(res.next_cursor);
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        if (append) toast.error("Load more failed", e.message);
        else setError(e);
      } finally {
        setLoading(false);
        setLoadingMore(false);
      }
    },
    [toast],
  );

  // Initial load.
  useEffect(() => {
    load({}, false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function runVerify() {
    setVerifying(true);
    try {
      const res = await api.verifyAudit();
      setVerify(res);
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Verification failed", e.message);
    } finally {
      setVerifying(false);
    }
  }

  function applyFilters(e: React.FormEvent) {
    e.preventDefault();
    load(filters, false);
  }

  function clearFilters() {
    setFilters({});
    load({}, false);
  }

  return (
    <div>
      <PageHeader
        title="Audit"
        description="Tamper-evident log of management actions."
        actions={
          <Button
            variant="outline"
            size="sm"
            onClick={runVerify}
            disabled={verifying}
          >
            <ShieldCheck className="h-3.5 w-3.5" />
            {verifying ? "Verifying…" : "Verify chain"}
          </Button>
        }
      />

      {verify && (
        <div
          className={`mb-4 flex items-center gap-2 rounded-md border px-4 py-3 text-sm ${
            verify.valid
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-400"
              : "border-destructive/40 bg-destructive/10 text-destructive"
          }`}
        >
          {verify.valid ? (
            <ShieldCheck className="h-4 w-4" />
          ) : (
            <ShieldAlert className="h-4 w-4" />
          )}
          <span className="font-medium">
            {verify.valid ? "Chain intact" : "Chain broken"}
          </span>
          <span className="opacity-80">
            {verify.entries_checked} entries checked
            {verify.broken_at !== undefined
              ? ` · broke at seq ${verify.broken_at}`
              : ""}
            {verify.error ? ` · ${verify.error}` : ""}
          </span>
        </div>
      )}

      {/* Filters */}
      <Card className="mb-4">
        <CardContent className="p-4">
          <form onSubmit={applyFilters}>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              <div>
                <Label className="mb-1 block text-xs">Principal</Label>
                <Input
                  value={filters.principal ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, principal: e.target.value }))
                  }
                  placeholder="principal"
                />
              </div>
              <div>
                <Label className="mb-1 block text-xs">Action</Label>
                <Input
                  value={filters.action ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, action: e.target.value }))
                  }
                  placeholder="grant.create or grant.*"
                />
              </div>
              <div>
                <Label className="mb-1 block text-xs">Resource</Label>
                <Input
                  value={filters.resource ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, resource: e.target.value }))
                  }
                  placeholder="resource"
                />
              </div>
              <div>
                <Label className="mb-1 block text-xs">From</Label>
                <Input
                  type="datetime-local"
                  value={filters.from ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, from: e.target.value }))
                  }
                />
              </div>
              <div>
                <Label className="mb-1 block text-xs">To</Label>
                <Input
                  type="datetime-local"
                  value={filters.to ?? ""}
                  onChange={(e) =>
                    setFilters((f) => ({ ...f, to: e.target.value }))
                  }
                />
              </div>
            </div>
            <div className="mt-3 flex justify-end gap-2">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={clearFilters}
              >
                Clear
              </Button>
              <Button type="submit" size="sm">
                Apply filters
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>

      {loading ? (
        <LoadingState label="Loading audit log…" />
      ) : error ? (
        <ErrorState error={error} onRetry={() => load(filters, false)} />
      ) : !entries || entries.length === 0 ? (
        <EmptyState
          title="No audit entries"
          detail="No entries matched. Management actions will appear here."
        />
      ) : (
        <Card>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                    <th className="py-2 pl-4 pr-4 font-medium">Seq</th>
                    <th className="py-2 pr-4 font-medium">Occurred</th>
                    <th className="py-2 pr-4 font-medium">Principal</th>
                    <th className="py-2 pr-4 font-medium">Action</th>
                    <th className="py-2 pr-4 font-medium">Resource</th>
                    <th className="py-2 pr-4 font-medium">Details</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {entries.map((entry) => (
                    <AuditRow key={entry.seq} entry={entry} />
                  ))}
                </tbody>
              </table>
            </div>
            {cursor !== undefined && (
              <div className="flex justify-center border-t border-border p-3">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={loadingMore}
                  onClick={() => load({ ...filters, before: cursor }, true)}
                >
                  {loadingMore ? "Loading…" : "Load more"}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}

function AuditRow({ entry }: { entry: AuditEntry }) {
  const [open, setOpen] = useState(false);
  const hasDetails =
    entry.details !== null &&
    entry.details !== undefined &&
    !(typeof entry.details === "object" &&
      Object.keys(entry.details as object).length === 0);

  return (
    <>
      <tr className="align-top">
        <td className="py-2 pl-4 pr-4 font-mono text-xs">{entry.seq}</td>
        <td className="py-2 pr-4 text-xs text-muted-foreground">
          {fmtTime(entry.occurred_at)}
        </td>
        <td className="py-2 pr-4 font-mono text-xs">{entry.principal}</td>
        <td className="py-2 pr-4">
          <Badge variant="outline">{entry.action}</Badge>
        </td>
        <td className="py-2 pr-4 font-mono text-xs text-muted-foreground break-all">
          {entry.resource}
        </td>
        <td className="py-2 pr-4">
          {hasDetails ? (
            <button
              onClick={() => setOpen((o) => !o)}
              className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
            >
              {open ? (
                <ChevronDown className="h-3.5 w-3.5" />
              ) : (
                <ChevronRight className="h-3.5 w-3.5" />
              )}
              {open ? "hide" : "view"}
            </button>
          ) : (
            <span className="text-xs text-muted-foreground">—</span>
          )}
        </td>
      </tr>
      {open && hasDetails && (
        <tr>
          <td colSpan={6} className="px-4 pb-3">
            <pre className="overflow-x-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs">
              {JSON.stringify(entry.details, null, 2)}
            </pre>
          </td>
        </tr>
      )}
    </>
  );
}
