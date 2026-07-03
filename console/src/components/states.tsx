"use client";

// Loading / empty / error state building blocks, plus a `useAsync` hook that
// tracks the three states around a fetch and re-runs when its deps change.

import { useCallback, useEffect, useState } from "react";
import { Loader2, AlertTriangle, Inbox, RefreshCw } from "lucide-react";
import { ApiError } from "@/lib/api";
import { Button } from "./ui/primitives";

export interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: ApiError | Error | null;
  reload: () => void;
}

/**
 * Runs `fn` on mount and whenever `deps` change, exposing loading/error/data.
 * Aborts an in-flight request when deps change or the component unmounts.
 */
export function useAsync<T>(
  fn: () => Promise<T>,
  deps: unknown[],
): AsyncState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [nonce, setNonce] = useState(0);

  const reload = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    fn()
      .then((result) => {
        if (active) {
          setData(result);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!active) return;
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError(err instanceof Error ? err : new Error(String(err)));
        setLoading(false);
      });
    return () => {
      active = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  return { data, loading, error, reload };
}

export function LoadingState({ label = "Loading…" }: { label?: string }) {
  return (
    <div className="flex items-center justify-center gap-2 py-12 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" />
      {label}
    </div>
  );
}

export function EmptyState({
  title,
  detail,
}: {
  title: string;
  detail?: string;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-12 text-center">
      <Inbox className="h-8 w-8 text-muted-foreground/60" />
      <p className="text-sm font-medium text-foreground">{title}</p>
      {detail && <p className="max-w-md text-sm text-muted-foreground">{detail}</p>}
    </div>
  );
}

export function ErrorState({
  error,
  onRetry,
}: {
  error: ApiError | Error;
  onRetry?: () => void;
}) {
  const type = error instanceof ApiError ? error.type : "Error";
  const status = error instanceof ApiError ? error.status : undefined;
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12 text-center">
      <AlertTriangle className="h-8 w-8 text-destructive" />
      <div>
        <p className="text-sm font-medium text-destructive">
          {type}
          {status ? ` (${status})` : ""}
        </p>
        <p className="mt-1 max-w-md break-words text-sm text-muted-foreground">
          {error.message}
        </p>
      </div>
      {onRetry && (
        <Button variant="outline" size="sm" onClick={onRetry}>
          <RefreshCw className="h-3.5 w-3.5" />
          Retry
        </Button>
      )}
    </div>
  );
}

/**
 * Renders the right state for an AsyncState: loading spinner, error card, or
 * the children (given the loaded, non-null data).
 */
export function Async<T>({
  state,
  children,
  loadingLabel,
}: {
  state: AsyncState<T>;
  children: (data: T) => React.ReactNode;
  loadingLabel?: string;
}) {
  if (state.loading && state.data === null) {
    return <LoadingState label={loadingLabel} />;
  }
  if (state.error) {
    return <ErrorState error={state.error} onRetry={state.reload} />;
  }
  if (state.data === null) {
    return <LoadingState label={loadingLabel} />;
  }
  return <>{children(state.data)}</>;
}
