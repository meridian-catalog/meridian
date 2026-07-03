"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { Radio, Webhook as WebhookIcon, ChevronRight, ChevronDown } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { fmtTime, timeAgo } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync, ErrorState, LoadingState } from "@/components/states";
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
import type { CloudEvent, Delivery, Webhook } from "@/lib/types";

const POLL_MS = 5000;
const MAX_EVENTS = 100;

export default function EventsPage() {
  return (
    <div>
      <PageHeader
        title="Events"
        description="Live catalog activity feed and webhook subscriptions."
      />
      <div className="space-y-4">
        <EventsFeed />
        <WebhooksSection />
      </div>
    </div>
  );
}

function EventsFeed() {
  const [events, setEvents] = useState<CloudEvent[]>([]);
  const [initialLoading, setInitialLoading] = useState(true);
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [live, setLive] = useState(true);
  const cursorRef = useRef<string | undefined>(undefined);
  const seenRef = useRef<Set<string>>(new Set());

  const initializedRef = useRef(false);

  const poll = useCallback(async () => {
    try {
      // First load: pull the most recent events (newest-first) so the feed
      // opens on current activity, then seed the cursor at the newest id so
      // later polls tail only genuinely new events forward.
      const initial = !initializedRef.current;
      const res = initial
        ? await api.events({ order: "desc", limit: 50 })
        : await api.events({ after: cursorRef.current, limit: 50 });
      if (initial) {
        initializedRef.current = true;
        cursorRef.current = res.events[0]?.id ?? cursorRef.current;
      } else {
        cursorRef.current = res.next_cursor;
      }
      setError(null);
      if (res.events.length > 0) {
        setEvents((prev) => {
          const fresh = res.events.filter((e) => !seenRef.current.has(e.id));
          for (const e of fresh) seenRef.current.add(e.id);
          if (fresh.length === 0) return prev;
          // The desc initial page is already newest-first; ascending tail
          // pages are oldest-first, so reverse those before prepending.
          const ordered = initial ? fresh : fresh.reverse();
          return [...ordered, ...prev].slice(0, MAX_EVENTS);
        });
      }
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setInitialLoading(false);
    }
  }, []);

  useEffect(() => {
    poll();
    if (!live) return;
    const id = window.setInterval(poll, POLL_MS);
    return () => window.clearInterval(id);
  }, [poll, live]);

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Radio className="h-4 w-4 text-muted-foreground" /> Live feed
          {live && (
            <span className="flex items-center gap-1 text-xs font-normal text-emerald-400">
              <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-emerald-400" />
              polling
            </span>
          )}
        </CardTitle>
        <Button
          variant="outline"
          size="sm"
          onClick={() => setLive((l) => !l)}
        >
          {live ? "Pause" : "Resume"}
        </Button>
      </CardHeader>
      <CardContent>
        {initialLoading ? (
          <LoadingState label="Loading events…" />
        ) : error && events.length === 0 ? (
          <ErrorState error={error} onRetry={poll} />
        ) : events.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            No events yet. Catalog mutations will appear here.
          </p>
        ) : (
          <ul className="divide-y divide-border">
            {events.map((ev) => (
              <EventRow key={ev.id} ev={ev} />
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function EventRow({ ev }: { ev: CloudEvent }) {
  const [open, setOpen] = useState(false);
  const short = ev.type?.replace(/^com\.meridian\./, "") ?? "event";
  const hasData = ev.data !== null && ev.data !== undefined;
  return (
    <li className="py-2.5 text-sm">
      <div className="flex items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-3">
          {hasData ? (
            <button
              onClick={() => setOpen((o) => !o)}
              className="text-muted-foreground"
              aria-label={open ? "Collapse" : "Expand"}
            >
              {open ? (
                <ChevronDown className="h-3.5 w-3.5" />
              ) : (
                <ChevronRight className="h-3.5 w-3.5" />
              )}
            </button>
          ) : (
            <span className="inline-block w-3.5" />
          )}
          <Badge variant="outline" className="font-mono">
            {short}
          </Badge>
          <span className="truncate text-muted-foreground">
            {ev.subject ?? ev.source ?? ""}
          </span>
        </div>
        <span
          className="shrink-0 text-xs text-muted-foreground"
          title={fmtTime(ev.time)}
        >
          {timeAgo(ev.time)}
        </span>
      </div>
      {open && hasData && (
        <pre className="mt-2 ml-7 overflow-x-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs">
          {JSON.stringify(ev.data, null, 2)}
        </pre>
      )}
    </li>
  );
}

function WebhooksSection() {
  const webhooks = useAsync(() => api.listWebhooks(), []);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <WebhookIcon className="h-4 w-4 text-muted-foreground" /> Webhooks
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <CreateWebhookForm onCreated={() => webhooks.reload()} />
        <Async state={webhooks} loadingLabel="Loading webhooks…">
          {(data) =>
            data.webhooks.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">
                No webhooks configured.
              </p>
            ) : (
              <ul className="space-y-3">
                {data.webhooks.map((wh) => (
                  <WebhookCard
                    key={wh.id}
                    webhook={wh}
                    onDeleted={() => webhooks.reload()}
                  />
                ))}
              </ul>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function CreateWebhookForm({ onCreated }: { onCreated: () => void }) {
  const toast = useToast();
  const [url, setUrl] = useState("");
  const [types, setTypes] = useState("");
  const [secret, setSecret] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!url.trim()) {
      toast.error("URL required");
      return;
    }
    const eventTypes = types
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);
    setSubmitting(true);
    try {
      await api.createWebhook({
        url: url.trim(),
        event_types: eventTypes,
        secret: secret.trim(),
      });
      toast.success("Webhook created", url.trim());
      setUrl("");
      setTypes("");
      setSecret("");
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
      <p className="mb-3 text-sm font-medium">Add webhook</p>
      <div className="grid gap-3 sm:grid-cols-3">
        <div>
          <Label className="mb-1 block text-xs">URL</Label>
          <Input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="https://example.com/hook"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">
            Event types (comma-separated, blank = all)
          </Label>
          <Input
            value={types}
            onChange={(e) => setTypes(e.target.value)}
            placeholder="table.created, table.updated"
          />
        </div>
        <div>
          <Label className="mb-1 block text-xs">Secret</Label>
          <Input
            type="password"
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
            placeholder="signing secret"
          />
        </div>
      </div>
      <div className="mt-4 flex justify-end">
        <Button type="submit" disabled={submitting}>
          {submitting ? "Creating…" : "Create webhook"}
        </Button>
      </div>
    </form>
  );
}

function WebhookCard({
  webhook,
  onDeleted,
}: {
  webhook: Webhook;
  onDeleted: () => void;
}) {
  const toast = useToast();
  const [showDeliveries, setShowDeliveries] = useState(false);
  const [deleting, setDeleting] = useState(false);

  async function remove() {
    setDeleting(true);
    try {
      await api.deleteWebhook(webhook.id);
      toast.success("Webhook deleted");
      onDeleted();
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      toast.error("Delete failed", e.message);
      setDeleting(false);
    }
  }

  return (
    <li className="rounded-md border border-border p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="min-w-0">
          <p className="truncate font-mono text-sm">{webhook.url}</p>
          <div className="mt-1 flex flex-wrap gap-1">
            {webhook.event_types.length === 0 ? (
              <Badge variant="outline">all events</Badge>
            ) : (
              webhook.event_types.map((t) => (
                <Badge key={t} variant="secondary" className="font-mono">
                  {t}
                </Badge>
              ))
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowDeliveries((s) => !s)}
          >
            {showDeliveries ? "Hide deliveries" : "Deliveries"}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={remove}
            disabled={deleting}
          >
            Delete
          </Button>
        </div>
      </div>
      {showDeliveries && <DeliveryHistory webhookId={webhook.id} />}
    </li>
  );
}

function DeliveryHistory({ webhookId }: { webhookId: string }) {
  const deliveries = useAsync(
    () => api.webhookDeliveries(webhookId),
    [webhookId],
  );
  return (
    <div className="mt-3 border-t border-border pt-3">
      <Async state={deliveries} loadingLabel="Loading deliveries…">
        {(data) =>
          data.deliveries.length === 0 ? (
            <p className="text-sm text-muted-foreground">No deliveries yet.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                    <th className="py-2 pr-4 font-medium">Event</th>
                    <th className="py-2 pr-4 font-medium">Status</th>
                    <th className="py-2 pr-4 font-medium">Attempts</th>
                    <th className="py-2 pr-4 font-medium">Last code</th>
                    <th className="py-2 font-medium">Updated</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {data.deliveries.map((d) => (
                    <DeliveryRow key={d.event_id} delivery={d} />
                  ))}
                </tbody>
              </table>
            </div>
          )
        }
      </Async>
    </div>
  );
}

function statusVariant(status: string): "success" | "warning" | "danger" {
  if (status === "delivered") return "success";
  if (status === "dead") return "danger";
  return "warning";
}

function DeliveryRow({ delivery }: { delivery: Delivery }) {
  return (
    <tr className="align-top">
      <td className="py-2 pr-4 font-mono text-xs">
        <div className="truncate">{delivery.event_type}</div>
        <div className="text-[11px] text-muted-foreground">
          {delivery.event_id}
        </div>
      </td>
      <td className="py-2 pr-4">
        <Badge variant={statusVariant(delivery.status)}>
          {delivery.status}
        </Badge>
      </td>
      <td className="py-2 pr-4 font-mono text-xs">{delivery.attempts}</td>
      <td className="py-2 pr-4 font-mono text-xs">
        {delivery.last_status ?? "—"}
        {delivery.last_error && (
          <span
            className="ml-1 text-destructive"
            title={delivery.last_error}
          >
            !
          </span>
        )}
      </td>
      <td className="py-2 text-xs text-muted-foreground">
        {fmtTime(delivery.updated_at)}
      </td>
    </tr>
  );
}
