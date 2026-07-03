"use client";

import Link from "next/link";
import { CheckCircle2, XCircle, Database, Activity } from "lucide-react";
import { api } from "@/lib/api";
import { fmtTime, timeAgo } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync } from "@/components/states";
import {
  Badge,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/primitives";
import type { CloudEvent } from "@/lib/types";

export default function OverviewPage() {
  const health = useAsync(() => api.health(), []);
  const warehouses = useAsync(() => api.listWarehouses(), []);
  const events = useAsync(() => api.events({ limit: 10, order: "desc" }), []);

  return (
    <div>
      <PageHeader
        title="Overview"
        description="Server health, catalog size, and the latest activity."
      />

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {/* Health */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm text-muted-foreground">
              <Activity className="h-4 w-4" /> Server health
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Async state={health}>
              {(h) => (
                <div>
                  <div className="flex items-center gap-2">
                    {h.status === "ok" ? (
                      <CheckCircle2 className="h-5 w-5 text-emerald-400" />
                    ) : (
                      <XCircle className="h-5 w-5 text-destructive" />
                    )}
                    <span className="text-lg font-semibold capitalize">
                      {h.status}
                    </span>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {Object.entries(h.checks).map(([k, v]) => (
                      <Badge
                        key={k}
                        variant={v === "ok" ? "success" : "danger"}
                      >
                        {k}: {v}
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </Async>
          </CardContent>
        </Card>

        {/* Warehouse count */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm text-muted-foreground">
              <Database className="h-4 w-4" /> Warehouses
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Async state={warehouses}>
              {(w) => (
                <div>
                  <p className="text-3xl font-semibold">
                    {w.warehouses.length}
                  </p>
                  <Link
                    href="/catalog"
                    className="mt-2 inline-block text-sm text-primary hover:underline"
                  >
                    Browse catalog →
                  </Link>
                </div>
              )}
            </Async>
          </CardContent>
        </Card>

        {/* Recent events count preview */}
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-sm text-muted-foreground">
              <Activity className="h-4 w-4" /> Recent events
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Async state={events}>
              {(feed) => (
                <div>
                  <p className="text-3xl font-semibold">
                    {feed.events.length}
                    <span className="ml-1 text-base font-normal text-muted-foreground">
                      shown
                    </span>
                  </p>
                  <Link
                    href="/events"
                    className="mt-2 inline-block text-sm text-primary hover:underline"
                  >
                    Open events feed →
                  </Link>
                </div>
              )}
            </Async>
          </CardContent>
        </Card>
      </div>

      {/* Recent events list */}
      <Card className="mt-4">
        <CardHeader>
          <CardTitle>Latest activity</CardTitle>
        </CardHeader>
        <CardContent>
          <Async state={events} loadingLabel="Loading events…">
            {(feed) =>
              feed.events.length === 0 ? (
                <p className="py-6 text-center text-sm text-muted-foreground">
                  No events yet. Catalog mutations will appear here.
                </p>
              ) : (
                <ul className="divide-y divide-border">
                  {feed.events.map((ev) => (
                    <EventRow key={ev.id} ev={ev} />
                  ))}
                </ul>
              )
            }
          </Async>
        </CardContent>
      </Card>
    </div>
  );
}

function EventRow({ ev }: { ev: CloudEvent }) {
  const short = ev.type?.replace(/^com\.meridian\./, "") ?? "event";
  return (
    <li className="flex items-center justify-between gap-4 py-2.5 text-sm">
      <div className="flex min-w-0 items-center gap-3">
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
    </li>
  );
}
