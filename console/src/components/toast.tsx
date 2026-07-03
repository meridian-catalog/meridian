"use client";

// Minimal toast system. Errors surfaced here carry the server's IRC envelope
// message verbatim (the API client puts it on ApiError.message).

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
} from "react";
import { cn } from "@/lib/utils";

type ToastKind = "error" | "success" | "info";

interface Toast {
  id: number;
  kind: ToastKind;
  title: string;
  detail?: string;
}

interface ToastContextValue {
  toast: (t: Omit<Toast, "id">) => void;
  error: (title: string, detail?: string) => void;
  success: (title: string, detail?: string) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

let nextId = 1;

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const remove = useCallback((id: number) => {
    setToasts((ts) => ts.filter((t) => t.id !== id));
  }, []);

  const toast = useCallback(
    (t: Omit<Toast, "id">) => {
      const id = nextId++;
      setToasts((ts) => [...ts, { ...t, id }]);
      window.setTimeout(() => remove(id), t.kind === "error" ? 8000 : 4000);
    },
    [remove],
  );

  const value = useMemo<ToastContextValue>(
    () => ({
      toast,
      error: (title, detail) => toast({ kind: "error", title, detail }),
      success: (title, detail) => toast({ kind: "success", title, detail }),
    }),
    [toast],
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="pointer-events-none fixed bottom-4 right-4 z-50 flex w-full max-w-sm flex-col gap-2">
        {toasts.map((t) => (
          <div
            key={t.id}
            className={cn(
              "pointer-events-auto rounded-md border px-4 py-3 text-sm shadow-lg",
              t.kind === "error" &&
                "border-destructive/40 bg-destructive/10 text-destructive",
              t.kind === "success" &&
                "border-emerald-500/40 bg-emerald-500/10 text-emerald-400",
              t.kind === "info" && "border-border bg-card text-foreground",
            )}
            role="alert"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="font-medium">{t.title}</p>
                {t.detail && (
                  <p className="mt-1 break-words text-xs opacity-80">
                    {t.detail}
                  </p>
                )}
              </div>
              <button
                onClick={() => remove(t.id)}
                className="shrink-0 opacity-60 hover:opacity-100"
                aria-label="Dismiss"
              >
                ×
              </button>
            </div>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}
